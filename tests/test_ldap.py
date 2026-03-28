"""Tests for the generic LDAP source adapter (collector/sources/ldap.py).

All ldap3 calls are mocked — no real LDAP/AD server is required.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.ldap import (
    LDAPSource,
    _entry_to_dict,
)


# ---------------------------------------------------------------------------
# _entry_to_dict()
# ---------------------------------------------------------------------------


class TestEntryToDict:
    def test_single_value_returned_as_string(self):
        entry = SimpleNamespace(cn="johndoe", mail="john@example.com")
        result = _entry_to_dict(entry)
        assert result["cn"] == "johndoe"
        assert result["mail"] == "john@example.com"

    def test_none_value_returned_as_empty_string(self):
        entry = SimpleNamespace(cn=None)
        result = _entry_to_dict(entry)
        assert result["cn"] == ""

    def test_list_single_item_returned_as_string(self):
        entry = SimpleNamespace(cn=["johndoe"])
        result = _entry_to_dict(entry)
        assert result["cn"] == "johndoe"

    def test_list_multi_item_returned_as_list(self):
        entry = SimpleNamespace(memberOf=["cn=GroupA,dc=ex,dc=com", "cn=GroupB,dc=ex,dc=com"])
        result = _entry_to_dict(entry)
        assert result["memberOf"] == ["cn=GroupA,dc=ex,dc=com", "cn=GroupB,dc=ex,dc=com"]

    def test_empty_list_returned_as_empty_string(self):
        entry = SimpleNamespace(memberOf=[])
        result = _entry_to_dict(entry)
        assert result["memberOf"] == ""

    def test_entry_attributes_used_when_present(self):
        entry = SimpleNamespace(cn="alice", mail="alice@example.com")
        # Restrict to only 'cn' via entry_attributes
        entry.entry_attributes = ["cn"]
        result = _entry_to_dict(entry)
        assert "cn" in result
        assert "mail" not in result

    def test_ldap3_attribute_with_values_property(self):
        """Simulate a real ldap3 Attribute object that exposes .values."""
        attr = MagicMock()
        attr.values = ["10.0.0.1"]
        entry = MagicMock()
        entry.entry_attributes = ["ipAddress"]
        entry.ipAddress = attr
        result = _entry_to_dict(entry)
        assert result["ipAddress"] == "10.0.0.1"

    def test_ldap3_multi_value_attribute(self):
        attr = MagicMock()
        attr.values = ["addr1", "addr2"]
        entry = MagicMock()
        entry.entry_attributes = ["aliases"]
        entry.aliases = attr
        result = _entry_to_dict(entry)
        assert result["aliases"] == ["addr1", "addr2"]

    def test_exception_in_attribute_returns_empty_string(self):
        entry = MagicMock()
        entry.entry_attributes = ["badAttr"]
        type(entry).badAttr = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
        result = _entry_to_dict(entry)
        assert result["badAttr"] == ""


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
            src.get_objects("users")

    def test_accepts_any_collection_name(self, ldap_config):
        src = self._connected_source(ldap_config)
        src._conn.entries = []
        # Any name should be accepted (no ValueError)
        result = src.get_objects("users")
        assert result == []
        result2 = src.get_objects("dhcp_leases")
        assert result2 == []
        result3 = src.get_objects("ip_registrations")
        assert result3 == []

    def test_requires_search_base(self, ldap_config):
        ldap_config.extra["search_base"] = ""
        src = self._connected_source(ldap_config)
        with pytest.raises(ValueError, match="search_base"):
            src.get_objects("objects")

    def test_returns_raw_attribute_dicts(self, ldap_config):
        src = self._connected_source(ldap_config)

        entry = SimpleNamespace(
            cn="johndoe",
            mail="john@example.com",
            telephoneNumber="+1-555-1234",
        )
        src._conn.entries = [entry]

        result = src.get_objects("users")

        assert len(result) == 1
        r = result[0]
        assert r["cn"] == "johndoe"
        assert r["mail"] == "john@example.com"
        assert r["telephoneNumber"] == "+1-555-1234"

    def test_configured_attributes_passed_to_search(self, ldap_config):
        ldap_config.extra["attributes"] = "cn,mail"
        src = self._connected_source(ldap_config)
        src._conn.entries = []

        src.get_objects("users")

        call_kwargs = src._conn.search.call_args
        assert call_kwargs is not None
        passed_attrs = call_kwargs.kwargs.get("attributes") or call_kwargs.args[2]
        assert "cn" in passed_attrs
        assert "mail" in passed_attrs

    def test_no_attributes_config_uses_wildcard(self, ldap_config):
        ldap_config.extra.pop("attributes", None)
        src = self._connected_source(ldap_config)
        src._conn.entries = []

        src.get_objects("users")

        call_kwargs = src._conn.search.call_args
        passed_attrs = call_kwargs.kwargs.get("attributes") or call_kwargs.args[2]
        assert passed_attrs == ["*"]

    def test_multi_value_attribute_returned_as_list(self, ldap_config):
        src = self._connected_source(ldap_config)
        entry = SimpleNamespace(
            cn="jsmith",
            memberOf=["cn=GroupA,dc=ex,dc=com", "cn=GroupB,dc=ex,dc=com"],
        )
        src._conn.entries = [entry]

        result = src.get_objects("users")

        assert result[0]["memberOf"] == ["cn=GroupA,dc=ex,dc=com", "cn=GroupB,dc=ex,dc=com"]

    def test_absent_attribute_returned_as_empty_string(self, ldap_config):
        src = self._connected_source(ldap_config)
        entry = SimpleNamespace(cn="alice", mail=None)
        src._conn.entries = [entry]

        result = src.get_objects("users")
        assert result[0]["mail"] == ""

    def test_search_filter_from_extra(self, ldap_config):
        ldap_config.extra["search_filter"] = "(cn=test*)"
        src = self._connected_source(ldap_config)
        src._conn.entries = []

        src.get_objects("users")

        call_kwargs = src._conn.search.call_args
        passed_filter = call_kwargs.kwargs.get("search_filter") or call_kwargs.args[1]
        assert passed_filter == "(cn=test*)"

    def test_empty_result_set(self, ldap_config):
        src = self._connected_source(ldap_config)
        src._conn.entries = []

        result = src.get_objects("users")
        assert result == []


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
