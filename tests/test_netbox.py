"""Tests for the NetBox source adapter (collector/sources/netbox.py).

All pynetbox calls are mocked — no real NetBox server is required.

Created: 2026-03-30
Author: GitHub Copilot
Last Changed: GitHub Copilot Issue: #(netbox-source-type)
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.netbox import (
    NetBoxSource,
    _normalise_tags,
    _record_to_dict,
)


# ---------------------------------------------------------------------------
# _record_to_dict()
# ---------------------------------------------------------------------------


class TestRecordToDict:
    def test_plain_dict_passthrough(self):
        record = {"id": 1, "name": "router-01", "status": "active"}
        result = _record_to_dict(record)
        assert result == {"id": 1, "name": "router-01", "status": "active"}

    def test_none_returns_empty_dict(self):
        assert _record_to_dict(None) == {}

    def test_nested_foreign_key_converted_to_dict(self):
        site = {"id": 5, "name": "HQ", "slug": "hq"}
        record = {"id": 1, "name": "router-01", "site": site}
        result = _record_to_dict(record)
        assert result["site"] == {"id": 5, "name": "HQ", "slug": "hq"}

    def test_none_value_preserved(self):
        record = {"id": 1, "tenant": None}
        result = _record_to_dict(record)
        assert result["tenant"] is None

    def test_tags_normalised_to_slug_list(self):
        record = {
            "id": 1,
            "tags": [
                {"id": 1, "name": "prod", "slug": "prod"},
                {"id": 2, "name": "core", "slug": "core"},
            ],
        }
        result = _record_to_dict(record)
        assert result["tags"] == ["prod", "core"]

    def test_custom_fields_kept_as_dict(self):
        record = {"id": 1, "custom_fields": {"asset_tag": "ASSET-001"}}
        result = _record_to_dict(record)
        assert result["custom_fields"] == {"asset_tag": "ASSET-001"}

    def test_list_of_plain_values_preserved(self):
        record = {"id": 1, "ip_addresses": ["10.0.0.1/24", "10.0.0.2/24"]}
        result = _record_to_dict(record)
        assert result["ip_addresses"] == ["10.0.0.1/24", "10.0.0.2/24"]

    def test_list_of_nested_records_recursed(self):
        record = {
            "id": 1,
            "local_context_data": [{"key": "val"}],
        }
        result = _record_to_dict(record)
        assert result["local_context_data"] == [{"key": "val"}]

    def test_pynetbox_record_like_object(self):
        """Simulate a pynetbox Record (dict-like with .items())."""
        class FakeRecord(dict):
            pass

        rec = FakeRecord({"id": 10, "name": "sw-01", "tags": []})
        result = _record_to_dict(rec)
        assert result["id"] == 10
        assert result["name"] == "sw-01"
        assert result["tags"] == []


# ---------------------------------------------------------------------------
# _normalise_tags()
# ---------------------------------------------------------------------------


class TestNormaliseTags:
    def test_empty_list(self):
        assert _normalise_tags([]) == []

    def test_none_returns_empty(self):
        assert _normalise_tags(None) == []

    def test_dict_tags_use_slug(self):
        tags = [{"id": 1, "name": "production", "slug": "production"}]
        assert _normalise_tags(tags) == ["production"]

    def test_dict_tag_falls_back_to_name(self):
        tags = [{"name": "staging"}]
        assert _normalise_tags(tags) == ["staging"]

    def test_object_with_slug_attribute(self):
        tag = SimpleNamespace(slug="prod", name="Production")
        assert _normalise_tags([tag]) == ["prod"]

    def test_object_with_name_only(self):
        tag = MagicMock(spec=["name"])
        tag.name = "staging"
        assert _normalise_tags([tag]) == ["staging"]

    def test_plain_string_tag(self):
        assert _normalise_tags(["prod"]) == ["prod"]


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestNetBoxConnect:
    def test_connect_creates_nb_client(self, netbox_config):
        fake_nb = MagicMock()
        with patch("pynetbox.api", return_value=fake_nb) as mock_api:
            src = NetBoxSource()
            src.connect(netbox_config)
            mock_api.assert_called_once_with(
                "https://source-netbox.example.com",
                token="source-api-token",
            )
        assert src._nb is fake_nb

    def test_connect_raises_if_no_url(self, netbox_config):
        netbox_config.url = ""
        src = NetBoxSource()
        with patch("pynetbox.api", return_value=MagicMock()):
            with pytest.raises(ValueError, match="url"):
                src.connect(netbox_config)

    def test_connect_raises_if_no_token(self, netbox_config):
        netbox_config.password = ""
        src = NetBoxSource()
        with patch("pynetbox.api", return_value=MagicMock()):
            with pytest.raises(ValueError, match="password"):
                src.connect(netbox_config)

    def test_connect_raises_if_pynetbox_missing(self, netbox_config):
        src = NetBoxSource()
        with patch.dict("sys.modules", {"pynetbox": None}):
            with pytest.raises((RuntimeError, ImportError)):
                src.connect(netbox_config)

    def test_connect_disables_ssl_verification(self, netbox_config):
        netbox_config.verify_ssl = False
        fake_nb = MagicMock()
        fake_session = MagicMock()
        with patch("pynetbox.api", return_value=fake_nb):
            with patch("requests.Session", return_value=fake_session):
                src = NetBoxSource()
                src.connect(netbox_config)
        assert fake_session.verify is False
        assert fake_nb.http_session is fake_session


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestNetBoxGetObjects:
    def _connected_source(self, netbox_config) -> NetBoxSource:
        src = NetBoxSource()
        src._nb = MagicMock()
        src._config = netbox_config
        return src

    def test_raises_without_connect(self):
        src = NetBoxSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("dcim.devices")

    def test_fetches_all_records_from_collection(self, netbox_config):
        src = self._connected_source(netbox_config)
        fake_device = {"id": 1, "name": "router-01", "tags": []}
        src._nb.dcim.devices.all.return_value = [fake_device]

        result = src.get_objects("dcim.devices")

        src._nb.dcim.devices.all.assert_called_once()
        assert len(result) == 1
        assert result[0]["name"] == "router-01"

    def test_uses_filter_when_extra_filters_set(self, netbox_config):
        netbox_config.extra["filters"] = '{"site": "hq"}'
        src = self._connected_source(netbox_config)
        fake_device = {"id": 1, "name": "router-01", "tags": []}
        src._nb.dcim.devices.filter.return_value = [fake_device]

        result = src.get_objects("dcim.devices")

        src._nb.dcim.devices.filter.assert_called_once_with(site="hq")
        assert len(result) == 1

    def test_raises_on_invalid_collection_format(self, netbox_config):
        src = self._connected_source(netbox_config)
        with pytest.raises(ValueError, match="two-part dotted path"):
            src.get_objects("devices")

    def test_raises_on_unknown_app(self, netbox_config):
        src = self._connected_source(netbox_config)
        # Use a spec-restricted MagicMock so attribute access raises AttributeError
        # for apps that are not listed in the spec
        src._nb = MagicMock(spec=["dcim", "ipam"])
        with pytest.raises((ValueError, AttributeError)):
            src.get_objects("nonexistent.devices")

    def test_hyphenated_collection_normalised(self, netbox_config):
        src = self._connected_source(netbox_config)
        src._nb.dcim.device_types.all.return_value = []
        # Both forms should resolve to the same endpoint
        result = src.get_objects("dcim.device-types")
        assert result == []

    def test_returns_list_of_dicts(self, netbox_config):
        src = self._connected_source(netbox_config)
        records = [
            {"id": 1, "name": "sw-01", "tags": []},
            {"id": 2, "name": "sw-02", "tags": []},
        ]
        src._nb.dcim.devices.all.return_value = records

        result = src.get_objects("dcim.devices")

        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)
        assert result[0]["name"] == "sw-01"
        assert result[1]["name"] == "sw-02"

    def test_empty_collection_returns_empty_list(self, netbox_config):
        src = self._connected_source(netbox_config)
        src._nb.ipam.prefixes.all.return_value = []

        result = src.get_objects("ipam.prefixes")
        assert result == []

    def test_filters_parsed_from_dict_extra(self, netbox_config):
        netbox_config.extra["filters"] = {"status": "active"}
        src = self._connected_source(netbox_config)
        src._nb.dcim.devices.filter.return_value = []

        src.get_objects("dcim.devices")

        src._nb.dcim.devices.filter.assert_called_once_with(status="active")

    def test_invalid_filter_json_falls_back_to_all(self, netbox_config):
        netbox_config.extra["filters"] = "not-valid-json"
        src = self._connected_source(netbox_config)
        src._nb.dcim.devices.all.return_value = []

        result = src.get_objects("dcim.devices")

        src._nb.dcim.devices.all.assert_called_once()
        assert result == []


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestNetBoxClose:
    def test_close_clears_nb_client(self, netbox_config):
        src = NetBoxSource()
        src._nb = MagicMock()
        src._config = netbox_config
        src.close()
        assert src._nb is None
        assert src._config is None

    def test_close_noop_when_not_connected(self):
        src = NetBoxSource()
        src.close()  # should not raise
