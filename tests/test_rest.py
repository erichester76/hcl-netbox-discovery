"""Tests for the generic REST source adapter (collector/sources/rest.py).

All HTTP calls are mocked via responses/requests-mock so that no real server
is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from collector.config import CollectionConfig, SourceConfig
from collector.sources.rest import RestSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session(json_responses: dict) -> MagicMock:
    """Return a mock requests.Session whose GET returns pre-defined JSON responses."""
    session = MagicMock(spec=requests.Session)
    session.verify = True
    session.headers = {}

    def get_side_effect(url, timeout=30):
        resp = MagicMock()
        resp.json.return_value = json_responses.get(url, {})
        resp.raise_for_status.return_value = None
        return resp

    session.get.side_effect = get_side_effect
    return session


# ---------------------------------------------------------------------------
# _normalise_url()
# ---------------------------------------------------------------------------


class TestNormaliseUrl:
    def test_adds_https_scheme(self):
        assert RestSource._normalise_url("api.example.com") == "https://api.example.com"

    def test_preserves_http_scheme(self):
        assert RestSource._normalise_url("http://api.example.com") == "http://api.example.com"

    def test_strips_trailing_slash(self):
        assert RestSource._normalise_url("https://api.example.com/") == "https://api.example.com"

    def test_strips_multiple_trailing_slashes(self):
        assert RestSource._normalise_url("https://api.example.com///") == "https://api.example.com"

    def test_strips_whitespace(self):
        assert RestSource._normalise_url("  https://api.example.com  ") == "https://api.example.com"


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestRestConnect:
    def test_connect_basic_auth(self, rest_config):
        with patch("requests.Session") as MockSession:
            instance = MockSession.return_value
            instance.headers = {}
            src = RestSource()
            src.connect(rest_config)
            assert instance.auth == (rest_config.username, rest_config.password)

    def test_connect_bearer_auth(self, rest_config):
        rest_config.extra["auth"] = "bearer"
        with patch("requests.Session") as MockSession:
            instance = MockSession.return_value
            instance.headers = {}
            src = RestSource()
            src.connect(rest_config)
            assert instance.headers.get("Authorization") == f"Bearer {rest_config.password}"

    def test_connect_header_auth(self, rest_config):
        rest_config.extra["auth"] = "header"
        rest_config.extra["auth_header"] = "X-Api-Key"
        with patch("requests.Session") as MockSession:
            instance = MockSession.return_value
            instance.headers = {}
            src = RestSource()
            src.connect(rest_config)
            assert instance.headers.get("X-Api-Key") == rest_config.password

    def test_connect_raises_for_unknown_auth_scheme(self, rest_config):
        rest_config.extra["auth"] = "oauth2"
        with patch("requests.Session"):
            src = RestSource()
            with pytest.raises(ValueError, match="unknown auth scheme"):
                src.connect(rest_config)

    def test_connect_raises_when_no_collections(self, rest_config):
        rest_config.collections = {}
        with patch("requests.Session"):
            src = RestSource()
            with pytest.raises(ValueError, match="collection"):
                src.connect(rest_config)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestRestGetObjects:
    def _connected_source(self, rest_config) -> RestSource:
        src = RestSource()
        src._session = MagicMock()
        src._base_url = "https://api.example.com"
        src._collections = rest_config.collections
        return src

    def test_raises_without_connect(self):
        src = RestSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("nodes")

    def test_raises_for_unknown_collection(self, rest_config):
        src = self._connected_source(rest_config)
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("chassis")

    def test_returns_list_from_list_key(self, rest_config):
        src = self._connected_source(rest_config)
        src._base_url = "https://api.example.com"

        resp = MagicMock()
        resp.json.return_value = {
            "nodeList": [
                {"uuid": "node-1", "name": "blade-01"},
                {"uuid": "node-2", "name": "blade-02"},
            ]
        }
        resp.raise_for_status.return_value = None
        src._session.get.return_value = resp

        # Disable detail enrichment for this test
        rest_config.collections["nodes"].detail_endpoint = ""
        result = src.get_objects("nodes")

        assert len(result) == 2
        assert result[0]["uuid"] == "node-1"

    def test_returns_list_response_directly(self, rest_config):
        col = CollectionConfig(name="items", endpoint="/items")
        rest_config.collections["items"] = col
        src = self._connected_source(rest_config)

        resp = MagicMock()
        resp.json.return_value = [{"id": 1}, {"id": 2}]
        resp.raise_for_status.return_value = None
        src._session.get.return_value = resp

        result = src.get_objects("items")
        assert result == [{"id": 1}, {"id": 2}]

    def test_falls_back_to_results_key(self, rest_config):
        col = CollectionConfig(name="data", endpoint="/data")
        rest_config.collections["data"] = col
        src = self._connected_source(rest_config)

        resp = MagicMock()
        resp.json.return_value = {"results": [{"id": 1}]}
        resp.raise_for_status.return_value = None
        src._session.get.return_value = resp

        result = src.get_objects("data")
        assert result == [{"id": 1}]


# ---------------------------------------------------------------------------
# _fetch_list()
# ---------------------------------------------------------------------------


class TestRestFetchList:
    def test_dict_with_list_key(self):
        src = RestSource()
        src._session = MagicMock()
        src._base_url = "https://api.example.com"

        resp = MagicMock()
        resp.json.return_value = {"nodeList": [{"name": "n1"}, {"name": "n2"}]}
        resp.raise_for_status.return_value = None
        src._session.get.return_value = resp

        col = CollectionConfig(name="nodes", endpoint="/nodes", list_key="nodeList")
        result = src._fetch_list(col)
        assert result == [{"name": "n1"}, {"name": "n2"}]

    def test_empty_list_response(self):
        src = RestSource()
        src._session = MagicMock()
        src._base_url = "https://api.example.com"

        resp = MagicMock()
        resp.json.return_value = []
        resp.raise_for_status.return_value = None
        src._session.get.return_value = resp

        col = CollectionConfig(name="nodes", endpoint="/nodes")
        result = src._fetch_list(col)
        assert result == []


# ---------------------------------------------------------------------------
# _enrich_with_detail()
# ---------------------------------------------------------------------------


class TestRestEnrichWithDetail:
    def test_merges_detail_fields(self):
        src = RestSource()
        src._session = MagicMock()
        src._base_url = "https://api.example.com"

        def get_side_effect(url, timeout=30):
            resp = MagicMock()
            if "/nodes/node-1" in url:
                resp.json.return_value = {"uuid": "node-1", "serial": "SN001", "extra": "data"}
            resp.raise_for_status.return_value = None
            return resp

        src._session.get.side_effect = get_side_effect

        col = CollectionConfig(
            name="nodes",
            endpoint="/nodes",
            list_key="nodeList",
            detail_endpoint="/nodes/{uuid}",
            detail_id_field="uuid",
        )
        items = [{"uuid": "node-1", "name": "blade-01"}]
        result = src._enrich_with_detail(items, col)

        assert result[0]["serial"] == "SN001"
        assert result[0]["name"] == "blade-01"

    def test_keeps_item_when_detail_fetch_fails(self):
        src = RestSource()
        src._session = MagicMock()
        src._base_url = "https://api.example.com"

        def get_side_effect(url, timeout=30):
            resp = MagicMock()
            resp.raise_for_status.side_effect = Exception("404 not found")
            return resp

        src._session.get.side_effect = get_side_effect

        col = CollectionConfig(
            name="nodes",
            endpoint="/nodes",
            detail_endpoint="/nodes/{uuid}",
            detail_id_field="uuid",
        )
        items = [{"uuid": "node-99", "name": "blade-99"}]
        result = src._enrich_with_detail(items, col)

        assert result == items  # unchanged

    def test_skips_enrichment_for_item_without_id(self):
        src = RestSource()
        src._session = MagicMock()
        src._base_url = "https://api.example.com"

        col = CollectionConfig(
            name="nodes",
            endpoint="/nodes",
            detail_endpoint="/nodes/{uuid}",
            detail_id_field="uuid",
        )
        items = [{"name": "no-uuid-item"}]  # no 'uuid' key
        result = src._enrich_with_detail(items, col)

        src._session.get.assert_not_called()
        assert result == items


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestRestClose:
    def test_close_closes_session(self, rest_config):
        src = RestSource()
        session = MagicMock()
        src._session = session
        src.close()
        session.close.assert_called_once()
        assert src._session is None

    def test_close_handles_error(self):
        src = RestSource()
        session = MagicMock()
        session.close.side_effect = Exception("oops")
        src._session = session
        src.close()  # should not raise
        assert src._session is None

    def test_close_noop_when_not_connected(self):
        src = RestSource()
        src.close()  # should not raise
