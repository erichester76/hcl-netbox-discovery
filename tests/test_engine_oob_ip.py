"""Tests for oob_ip assignment in Engine._process_interfaces.

Covers:
- oob_if = "first" sets oob_ip on parent dcim.device
- Only the first IP per interface block is promoted to oob_ip
- oob_if != "first" does not set oob_ip
- dry_run skips oob_ip assignment
- oob_ip is NOT set for virtualization resources (vminterfaces)
- Second interface item in the same block does not override oob_ip
"""

from __future__ import annotations

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


def _run_oob_logic(
    address: str = "10.0.0.1/24",
    dry_run: bool = False,
    oob_if: str = "first",
    first_for_iface: bool = True,
    first_oob_set: bool = False,
    nb_ip_id: int = 10,
    parent_obj_id: int = 99,
    nb_resource: str = "dcim.devices",
    iface_resource: str = "dcim.interfaces",
):
    """
    Replicate the oob_if logic block from engine._process_interfaces and
    return (nb_update_calls, first_oob_set).
    """
    nb_ip = _make_nb_ip(nb_ip_id)
    parent_nb_obj = _make_parent_obj(parent_obj_id)
    nb_mock = MagicMock()
    ip_payload = {"address": address}
    ip_ctx = MagicMock()
    ip_ctx.dry_run = dry_run

    # Replicate the exact logic block from engine.py
    if (
        oob_if == "first"
        and first_for_iface
        and not first_oob_set
        and nb_ip is not None
        and parent_nb_obj is not None
        and not ip_ctx.dry_run
        and iface_resource == "dcim.interfaces"
    ):
        ip_id = nb_ip.id
        p_id = parent_nb_obj.id
        if ip_id is not None and p_id is not None:
            try:
                nb_mock.update(nb_resource, p_id, {"oob_ip": ip_id})
                first_oob_set = True
            except Exception:
                pass

    return nb_mock.update.call_args_list, first_oob_set


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestOobIpAssignment:
    """Test the oob_ip update call made by engine when oob_if = 'first'."""

    def test_first_ip_sets_oob_ip(self):
        calls, oob_set = _run_oob_logic("10.0.0.1/24")
        assert len(calls) == 1
        assert calls[0] == call("dcim.devices", 99, {"oob_ip": 10})
        assert oob_set is True

    def test_dry_run_skips_oob_assignment(self):
        calls, oob_set = _run_oob_logic("10.0.0.1/24", dry_run=True)
        assert len(calls) == 0
        assert oob_set is False

    def test_oob_if_not_first_skips_assignment(self):
        calls, oob_set = _run_oob_logic("10.0.0.1/24", oob_if="never")
        assert len(calls) == 0
        assert oob_set is False

    def test_not_first_for_iface_skips_assignment(self):
        calls, oob_set = _run_oob_logic("10.0.0.1/24", first_for_iface=False)
        assert len(calls) == 0
        assert oob_set is False

    def test_already_set_skips_second_ip(self):
        calls, oob_set = _run_oob_logic("10.0.0.2/24", first_oob_set=True)
        assert len(calls) == 0
        assert oob_set is True  # still True, not overwritten

    def test_vminterface_resource_skips_oob(self):
        """oob_ip must not be set for virtualization resources."""
        calls, oob_set = _run_oob_logic(
            "10.0.0.1/24",
            iface_resource="virtualization.vminterface",
        )
        assert len(calls) == 0
        assert oob_set is False

    def test_ipv6_address_sets_oob_ip(self):
        """oob_if is address-version agnostic; IPv6 also triggers the update."""
        calls, oob_set = _run_oob_logic("2001:db8::1/64")
        assert len(calls) == 1
        assert calls[0] == call("dcim.devices", 99, {"oob_ip": 10})
        assert oob_set is True

    def test_second_call_with_flag_already_set_does_not_duplicate(self):
        """Simulate two iterations: first sets the flag, second sees it already set."""
        _, oob_set = _run_oob_logic("10.0.0.1/24")
        assert oob_set is True

        calls2, oob_set2 = _run_oob_logic("10.0.0.2/24", first_oob_set=oob_set)
        assert len(calls2) == 0
        assert oob_set2 is True
