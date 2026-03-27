"""Tests for the LDAP source adapter (collector/sources/ldap.py).

All ldap3 calls are mocked — no real LDAP/AD server is required.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.ldap import (
    LDAPSource,
    _attr,
    _format_description,
    _is_ap,
    _is_static,
)


# ---------------------------------------------------------------------------
# _is_ap()
# ---------------------------------------------------------------------------


class TestIsAP:
    @pytest.mark.parametrize(
        "description, expected",
        [
            ("Office-AP-101 Wireless", True),
            ("Lobby-WAP controller", True),
            ("Desktop-PC-042", False),
            ("Laptop Device", False),
            ("KAPTAN-ap-201", True),
            ("", False),
        ],
    )
    def test_is_ap(self, description, expected):
        assert _is_ap(description) == expected


# ---------------------------------------------------------------------------
# _format_description()
# ---------------------------------------------------------------------------


class TestFormatDescription:
    def test_static_lease_uses_description_directly(self):
        result = _format_description("cn=jsmith,ou=people,dc=example,dc=com", "John's Laptop", "Static")
        assert result == "John's Laptop"

    def test_dhcp_lease_prepends_upn(self):
        result = _format_description("cn=jsmith,ou=people,dc=clemson,dc=edu", "Gaming PC", "Registered")
        assert result == "JSMITH@CLEMSON.EDU: Gaming PC"

    def test_description_truncated_to_64_chars(self):
        long_desc = "X" * 100
        result = _format_description("", long_desc, "Static")
        assert len(result) == 64

    def test_strips_connected_to_prefix(self):
        result = _format_description("", "Connected to switch-01", "Static")
        assert not result.startswith("Connected to")

    def test_newlines_replaced_with_space(self):
        result = _format_description("", "line1\nline2", "Static")
        assert "\n" not in result

    def test_unknown_upn_for_no_dn_match(self):
        result = _format_description("invalid-dn", "My device", "Registered")
        assert "INVALID-DN" in result

    def test_double_spaces_collapsed(self):
        result = _format_description("", "word1  word2", "Static")
        assert "  " not in result


# ---------------------------------------------------------------------------
# _attr()
# ---------------------------------------------------------------------------


class TestAttr:
    def test_returns_string_value(self):
        entry = SimpleNamespace(DirXMLjnsuDHCPAddress="10.1.2.3")
        assert _attr(entry, "DirXMLjnsuDHCPAddress") == "10.1.2.3"

    def test_returns_empty_for_missing_attr(self):
        entry = SimpleNamespace()
        assert _attr(entry, "DirXMLjnsuDHCPAddress") == ""

    def test_returns_empty_for_none_attr(self):
        entry = SimpleNamespace(DirXMLjnsuDHCPAddress=None)
        assert _attr(entry, "DirXMLjnsuDHCPAddress") == ""


# ---------------------------------------------------------------------------
# _is_static()
# ---------------------------------------------------------------------------


class TestIsStatic:
    def test_static_with_non_empty_value(self):
        entry = SimpleNamespace(DirXMLjnsuStaticAddrs=["10.1.2.3"])
        assert _is_static(entry) is True

    def test_not_static_when_no_attr(self):
        entry = SimpleNamespace()
        assert _is_static(entry) is False

    def test_not_static_when_empty_list(self):
        entry = SimpleNamespace(DirXMLjnsuStaticAddrs=[])
        assert _is_static(entry) is False

    def test_not_static_when_none(self):
        entry = SimpleNamespace(DirXMLjnsuStaticAddrs=None)
        assert _is_static(entry) is False


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestLDAPConnect:
    def test_connect_creates_connection(self, ldap_config):
        fake_conn = MagicMock()
        fake_server = MagicMock()

        with patch("ldap3.Server", return_value=fake_server):
            with patch("ldap3.Connection", return_value=fake_conn) as lc:
                src = LDAPSource()
                src.connect(ldap_config)
                lc.assert_called_once()

        assert src._conn is fake_conn

    def test_connect_raises_if_no_url(self, ldap_config):
        ldap_config.url = ""
        src = LDAPSource()
        with patch("ldap3.Server", return_value=MagicMock()):
            with patch("ldap3.Connection", return_value=MagicMock()):
                with pytest.raises(ValueError, match="url"):
                    src.connect(ldap_config)

    def test_connect_raises_if_ldap3_missing(self, ldap_config):
        src = LDAPSource()
        with patch.dict("sys.modules", {"ldap3": None}):
            with pytest.raises((RuntimeError, ImportError)):
                src.connect(ldap_config)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestLDAPGetObjects:
    def _connected_source(self, ldap_config) -> LDAPSource:
        src = LDAPSource()
        src._conn = MagicMock()
        src._config = ldap_config
        return src

    def test_raises_without_connect(self):
        src = LDAPSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("dhcp_leases")

    def test_raises_for_unknown_collection(self, ldap_config):
        src = self._connected_source(ldap_config)
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("users")

    def test_requires_search_base(self, ldap_config):
        ldap_config.extra["search_base"] = ""
        src = self._connected_source(ldap_config)
        with pytest.raises(ValueError, match="search_base"):
            src.get_objects("dhcp_leases")

    def test_returns_normalised_records(self, ldap_config):
        src = self._connected_source(ldap_config)

        entry = SimpleNamespace(
            DirXMLjnsuDHCPAddress="10.1.2.3",
            DirXMLjnsuDeviceName="laptop-01",
            DirXMLjnsuHWAddress="aa:bb:cc:dd:ee:ff",
            DirXMLjnsuDescription="Staff Laptop",
            DirXMLjnsuUserDN="cn=jsmith,ou=people,dc=clemson,dc=edu",
            DirXMLJnsuDisabled=None,
            DirXMLjnsuStaticAddrs=None,
        )
        src._conn.entries = [entry]

        result = src.get_objects("dhcp_leases")

        assert len(result) == 1
        r = result[0]
        assert r["address"] == "10.1.2.3"
        assert r["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert r["device_name"] == "laptop-01"
        assert r["status"] == "dhcp"
        assert r["lease_type"] == "Registered"

    def test_skips_ap_entries(self, ldap_config):
        src = self._connected_source(ldap_config)

        entry = SimpleNamespace(
            DirXMLjnsuDHCPAddress="10.1.2.4",
            DirXMLjnsuDeviceName="ap-floor-2",
            DirXMLjnsuHWAddress="",
            DirXMLjnsuDescription="Floor-2-AP-Wireless",
            DirXMLjnsuUserDN="",
            DirXMLJnsuDisabled=None,
            DirXMLjnsuStaticAddrs=None,
        )
        src._conn.entries = [entry]

        result = src.get_objects("dhcp_leases")
        assert result == []

    def test_skips_entries_without_ip(self, ldap_config):
        src = self._connected_source(ldap_config)

        entry = SimpleNamespace(
            DirXMLjnsuDHCPAddress="",
            DirXMLjnsuDeviceName="device-01",
            DirXMLjnsuHWAddress="",
            DirXMLjnsuDescription="",
            DirXMLjnsuUserDN="",
            DirXMLJnsuDisabled=None,
            DirXMLjnsuStaticAddrs=None,
        )
        src._conn.entries = [entry]

        result = src.get_objects("dhcp_leases")
        assert result == []

    def test_prefix_length_appended(self, ldap_config):
        ldap_config.extra["default_prefix_length"] = "24"
        src = self._connected_source(ldap_config)

        entry = SimpleNamespace(
            DirXMLjnsuDHCPAddress="10.1.2.5",
            DirXMLjnsuDeviceName="device",
            DirXMLjnsuHWAddress="",
            DirXMLjnsuDescription="Test",
            DirXMLjnsuUserDN="",
            DirXMLJnsuDisabled=None,
            DirXMLjnsuStaticAddrs=None,
        )
        src._conn.entries = [entry]

        result = src.get_objects("dhcp_leases")
        assert result[0]["address"] == "10.1.2.5/24"

    def test_static_lease_status(self, ldap_config):
        src = self._connected_source(ldap_config)

        entry = SimpleNamespace(
            DirXMLjnsuDHCPAddress="10.1.2.6",
            DirXMLjnsuDeviceName="static-device",
            DirXMLjnsuHWAddress="",
            DirXMLjnsuDescription="Static PC",
            DirXMLjnsuUserDN="",
            DirXMLJnsuDisabled=None,
            DirXMLjnsuStaticAddrs=["10.1.2.6"],
        )
        src._conn.entries = [entry]

        result = src.get_objects("dhcp_leases")
        assert result[0]["status"] == "active"
        assert result[0]["lease_type"] == "Static"


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestLDAPClose:
    def test_close_calls_unbind(self, ldap_config):
        src = LDAPSource()
        conn = MagicMock()
        src._conn = conn
        src.close()
        conn.unbind.assert_called_once()
        assert src._conn is None

    def test_close_handles_unbind_error(self):
        src = LDAPSource()
        conn = MagicMock()
        conn.unbind.side_effect = Exception("oops")
        src._conn = conn
        src.close()  # should not raise
        assert src._conn is None

    def test_close_noop_when_not_connected(self):
        src = LDAPSource()
        src.close()  # should not raise
