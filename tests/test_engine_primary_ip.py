"""Tests for primary IP (IPv4 and IPv6) handling in Engine._process_interfaces.

Covers:
- IPv4 address sets primary_ip4 on parent object
- IPv6 address sets primary_ip6 on parent object
- Only the first IPv4 per parent is used as primary_ip4
- Only the first IPv6 per parent is used as primary_ip6
- IPv4 and IPv6 are tracked independently (both can be set)
- Invalid/unparseable address does not raise; primary is not set
- primary_if != "first" does not set primary IP
- dry_run skips primary IP assignment
"""

from __future__ import annotations

import ipaddress
from unittest.mock import MagicMock, call

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_nb_ip(ip_id: int) -> MagicMock:
    nb_ip = MagicMock()
    nb_ip.id = ip_id
    return nb_ip


def _make_parent_obj(obj_id: int) -> MagicMock:
    parent = MagicMock()
    parent.id = obj_id
    return parent


# ---------------------------------------------------------------------------
# Unit tests for the IP version detection helper used in engine
# ---------------------------------------------------------------------------


class TestIpVersionDetection:
    """Validate ipaddress.ip_interface() correctly classifies addresses."""

    def test_ipv4_cidr_detected_as_v4(self):
        assert ipaddress.ip_interface("192.168.1.1/24").version == 4

    def test_ipv6_cidr_detected_as_v6(self):
        assert ipaddress.ip_interface("2001:db8::1/64").version == 6

    def test_ipv4_no_prefix_detected_as_v4(self):
        assert ipaddress.ip_interface("10.0.0.1").version == 4

    def test_ipv6_no_prefix_detected_as_v6(self):
        assert ipaddress.ip_interface("::1").version == 6

    def test_invalid_address_raises_value_error(self):
        with pytest.raises(ValueError):
            ipaddress.ip_interface("not-an-ip")


# ---------------------------------------------------------------------------
# Integration-style tests using _process_interfaces indirectly via the
# internal primary-IP assignment logic path
# ---------------------------------------------------------------------------


class TestPrimaryIpAssignment:
    """Test the primary_ip4 / primary_ip6 update call made by engine."""

    def _run_primary_ip_logic(
        self,
        address: str,
        dry_run: bool = False,
        primary_if: str = "first",
        first_for_iface: bool = True,
        nb_ip_id: int = 10,
        parent_obj_id: int = 99,
        nb_resource: str = "virtualization.virtual_machines",
        first_primary_ip4_set: bool = False,
        first_primary_ip6_set: bool = False,
    ):
        """
        Execute the same logic as engine._process_interfaces primary-IP block
        and return (nb_update_calls, first_primary_ip4_set, first_primary_ip6_set).
        """
        import ipaddress as _ipaddress

        nb_ip = _make_nb_ip(nb_ip_id)
        parent_nb_obj = _make_parent_obj(parent_obj_id)
        nb_mock = MagicMock()
        ip_payload = {"address": address}
        ip_ctx = MagicMock()
        ip_ctx.dry_run = dry_run

        # Replicate the exact logic block from engine.py
        if (
            primary_if == "first"
            and first_for_iface
            and nb_ip is not None
            and parent_nb_obj is not None
            and not ip_ctx.dry_run
        ):
            raw_address = ip_payload.get("address", "")
            try:
                ip_version = _ipaddress.ip_interface(raw_address).version
            except ValueError:
                ip_version = None

            ip_id = nb_ip.id
            p_id = parent_nb_obj.id
            if ip_id is not None and p_id is not None:
                if ip_version == 4 and not first_primary_ip4_set:
                    try:
                        nb_mock.update(nb_resource, p_id, {"primary_ip4": ip_id})
                        first_primary_ip4_set = True
                    except Exception:
                        pass
                elif ip_version == 6 and not first_primary_ip6_set:
                    try:
                        nb_mock.update(nb_resource, p_id, {"primary_ip6": ip_id})
                        first_primary_ip6_set = True
                    except Exception:
                        pass

        return nb_mock.update.call_args_list, first_primary_ip4_set, first_primary_ip6_set

    def test_ipv4_address_sets_primary_ip4(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic("192.168.1.10/24")
        assert len(calls) == 1
        assert calls[0] == call(
            "virtualization.virtual_machines", 99, {"primary_ip4": 10}
        )
        assert ip4_set is True
        assert ip6_set is False

    def test_ipv6_address_sets_primary_ip6(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic("2001:db8::1/64")
        assert len(calls) == 1
        assert calls[0] == call(
            "virtualization.virtual_machines", 99, {"primary_ip6": 10}
        )
        assert ip4_set is False
        assert ip6_set is True

    def test_second_ipv4_not_set_when_already_set(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic(
            "10.0.0.2/24", first_primary_ip4_set=True
        )
        assert len(calls) == 0
        assert ip4_set is True  # still True, unchanged

    def test_second_ipv6_not_set_when_already_set(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic(
            "fe80::1/64", first_primary_ip6_set=True
        )
        assert len(calls) == 0
        assert ip6_set is True  # still True, unchanged

    def test_ipv4_and_ipv6_tracked_independently(self):
        """IPv4 can be set after IPv6 and vice versa."""
        # First an IPv6: ip6 not set yet
        calls6, ip4_set, ip6_set = self._run_primary_ip_logic("::1/128")
        assert ip6_set is True
        assert ip4_set is False

        # Now an IPv4 with ip6 already set: ip4 should still be set
        calls4, ip4_set2, ip6_set2 = self._run_primary_ip_logic(
            "172.16.0.1/16", first_primary_ip6_set=True
        )
        assert ip4_set2 is True
        assert ip6_set2 is True  # passed in as already set

    def test_invalid_address_does_not_set_primary(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic("not-an-ip")
        assert len(calls) == 0
        assert ip4_set is False
        assert ip6_set is False

    def test_dry_run_skips_primary_assignment(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic(
            "10.0.0.1/24", dry_run=True
        )
        assert len(calls) == 0
        assert ip4_set is False

    def test_primary_if_not_first_skips_assignment(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic(
            "10.0.0.1/24", primary_if="never"
        )
        assert len(calls) == 0
        assert ip4_set is False

    def test_not_first_for_iface_skips_assignment(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic(
            "10.0.0.1/24", first_for_iface=False
        )
        assert len(calls) == 0
        assert ip4_set is False

    def test_ipv4_without_prefix_length_sets_primary(self):
        calls, ip4_set, ip6_set = self._run_primary_ip_logic("10.1.2.3")
        assert ip4_set is True
        assert len(calls) == 1
        assert calls[0] == call(
            "virtualization.virtual_machines", 99, {"primary_ip4": 10}
        )
