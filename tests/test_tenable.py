"""Tests for the Tenable One / Nessus source adapter (collector/sources/tenable.py).

All HTTP calls are mocked — no real Tenable or Nessus instance is required.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.tenable import (
    TenableSource,
    _extract_list,
    _first,
    _safe_get,
    _severity_label,
)

# ---------------------------------------------------------------------------
# Pure-function helpers
# ---------------------------------------------------------------------------


class TestSeverityLabel:
    @pytest.mark.parametrize(
        "level, expected",
        [
            (0, "info"),
            (1, "low"),
            (2, "medium"),
            (3, "high"),
            (4, "critical"),
            (99, "info"),
            ("3", "high"),
            (None, "info"),
            ("x", "info"),
        ],
    )
    def test_severity_label(self, level, expected):
        assert _severity_label(level) == expected


class TestFirst:
    def test_returns_first_element(self):
        assert _first(["a", "b", "c"]) == "a"

    def test_returns_default_for_empty_list(self):
        assert _first([], default="none") == "none"

    def test_returns_default_for_none(self):
        assert _first(None) == ""

    def test_returns_default_for_non_list(self):
        assert _first("string") == ""


class TestSafeGet:
    def test_dict_access(self):
        assert _safe_get({"key": "val"}, "key") == "val"

    def test_dict_missing_key(self):
        assert _safe_get({}, "key", "default") == "default"

    def test_object_access(self):
        obj = SimpleNamespace(name="host01")
        assert _safe_get(obj, "name") == "host01"

    def test_object_missing_attr(self):
        obj = SimpleNamespace()
        assert _safe_get(obj, "missing", 42) == 42


class TestExtractList:
    def test_list_passthrough(self):
        data = [{"a": 1}, {"b": 2}]
        assert _extract_list(data, ("items",)) is data

    def test_dict_with_matching_key(self):
        data = {"assets": [{"id": "x"}], "total": 1}
        result = _extract_list(data, ("assets", "items"))
        assert result == [{"id": "x"}]

    def test_dict_second_key_matches(self):
        data = {"items": [1, 2, 3]}
        result = _extract_list(data, ("assets", "items"))
        assert result == [1, 2, 3]

    def test_empty_dict(self):
        assert _extract_list({}, ("assets",)) == []

    def test_non_list_non_dict(self):
        assert _extract_list(None, ("x",)) == []
        assert _extract_list(42, ("x",)) == []


# ---------------------------------------------------------------------------
# connect() – Tenable.io
# ---------------------------------------------------------------------------


class TestTenableConnect:
    def _make_session(self):
        session = MagicMock()
        session.headers = {}
        session.verify = True
        return session

    def test_tenable_io_sets_api_keys_header(self, tenable_config):
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            session = self._make_session()
            MockSession.return_value = session
            src.connect(tenable_config)
        assert "X-ApiKeys" in session.headers
        assert "accessKey=tenable-access-key" in session.headers["X-ApiKeys"]
        assert "secretKey=tenable-secret-key" in session.headers["X-ApiKeys"]

    def test_connect_prepends_https_if_missing(self, tenable_config):
        tenable_config.url = "cloud.tenable.com"
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            session = self._make_session()
            MockSession.return_value = session
            src.connect(tenable_config)
        assert src._base_url.startswith("https://")

    def test_connect_default_url_when_empty(self, tenable_config):
        tenable_config.url = ""
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            session = self._make_session()
            MockSession.return_value = session
            src.connect(tenable_config)
        assert src._base_url == "https://cloud.tenable.com"

    def test_connect_stores_date_range(self, tenable_config):
        tenable_config.extra["date_range"] = "60"
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            MockSession.return_value = self._make_session()
            src.connect(tenable_config)
        assert src._date_range == 60

    def test_connect_raises_if_credentials_missing(self, tenable_config):
        tenable_config.username = ""
        tenable_config.password = ""
        src = TenableSource()
        with pytest.raises(RuntimeError, match="username and password are required"):
            src.connect(tenable_config)

    def test_include_asset_details_flag_true(self, tenable_config):
        tenable_config.extra["include_asset_details"] = "true"
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            MockSession.return_value = self._make_session()
            src.connect(tenable_config)
        assert src._include_asset_details is True

    def test_include_asset_details_flag_false(self, tenable_config):
        tenable_config.extra["include_asset_details"] = "false"
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            MockSession.return_value = self._make_session()
            src.connect(tenable_config)
        assert src._include_asset_details is False


# ---------------------------------------------------------------------------
# connect() – Nessus on-prem
# ---------------------------------------------------------------------------


class TestNessusConnect:
    def _make_session(self, token: str = "nessus-token") -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"token": token}
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.post.return_value = mock_resp
        session.headers = {}
        session.verify = False
        return session

    def test_nessus_sets_cookie_token(self, nessus_config):
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            session = self._make_session()
            MockSession.return_value = session
            src.connect(nessus_config)
        assert "X-Cookie" in session.headers
        assert session.headers["X-Cookie"] == "token=nessus-token"

    def test_nessus_raises_if_no_token_returned(self, nessus_config):
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            session = MagicMock()
            session.headers = {}
            mock_resp = MagicMock()
            mock_resp.json.return_value = {}  # no token key
            mock_resp.raise_for_status = MagicMock()
            session.post.return_value = mock_resp
            MockSession.return_value = session
            with pytest.raises(RuntimeError, match="no token was returned"):
                src.connect(nessus_config)

    def test_nessus_sets_platform_flag(self, nessus_config):
        src = TenableSource()
        with patch("collector.sources.tenable.requests.Session") as MockSession:
            session = self._make_session()
            MockSession.return_value = session
            src.connect(nessus_config)
        assert src._platform == "nessus"


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestGetObjects:
    def _connected_source(self) -> TenableSource:
        src = TenableSource()
        src._session = MagicMock()
        src._base_url = "https://cloud.tenable.com"
        src._date_range = 30
        src._platform = "tenable"
        src._include_asset_details = False
        return src

    def test_raises_without_connect(self):
        src = TenableSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("assets")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("unknown_thing")

    def test_get_assets_returns_normalised_dicts(self):
        src = self._connected_source()

        raw_asset = {
            "id":                "asset-uuid-001",
            "has_agent":         False,
            "last_seen":         "2024-01-15T12:00:00.000Z",
            "ipv4":              ["10.0.0.1"],
            "ipv6":              [],
            "fqdn":              ["host01.example.com"],
            "netbios_name":      ["HOST01"],
            "operating_system":  ["Windows Server 2019"],
            "mac_address":       ["aa:bb:cc:dd:ee:ff"],
            "system_type":       ["General Purpose"],
            "sources":           [{"name": "NESSUS_SCAN"}],
            "severities": [
                {"level": 4, "count": 2, "name": "Critical"},
                {"level": 3, "count": 5, "name": "High"},
            ],
            "acr_score":         7,
            "exposure_score":    600,
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"assets": [raw_asset], "total": 1}
        mock_resp.raise_for_status = MagicMock()
        src._session.get.return_value = mock_resp

        result = src.get_objects("assets")

        assert len(result) == 1
        d = result[0]
        assert d["id"] == "asset-uuid-001"
        assert d["name"] == "host01"
        assert d["fqdn"] == "host01.example.com"
        assert d["ip_address"] == "10.0.0.1"
        assert d["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert d["os"] == "Windows Server 2019"
        assert d["status"] == "active"
        assert d["critical_vulns"] == 2
        assert d["high_vulns"] == 5
        assert d["acr_score"] == 7
        assert d["exposure_score"] == 600

    def test_get_assets_uses_ip_when_no_fqdn(self):
        src = self._connected_source()

        raw_asset = {
            "id":               "asset-uuid-002",
            "ipv4":             ["192.168.1.50"],
            "fqdn":             [],
            "netbios_name":     [],
            "operating_system": [],
            "mac_address":      [],
            "system_type":      [],
            "sources":          [],
            "severities":       [],
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"assets": [raw_asset]}
        mock_resp.raise_for_status = MagicMock()
        src._session.get.return_value = mock_resp

        result = src.get_objects("assets")
        assert result[0]["name"] == "192.168.1.50"
        assert result[0]["ip_address"] == "192.168.1.50"

    def test_get_assets_raises_on_error(self):
        src = self._connected_source()
        src._session.get.side_effect = Exception("connection refused")
        with pytest.raises(Exception, match="connection refused"):
            src.get_objects("assets")

    def test_get_vulnerabilities_returns_normalised_dicts(self):
        src = self._connected_source()

        raw_vuln = {
            "plugin_id":     21745,
            "plugin_name":   "Authentication Failure - Local Checks Not Run",
            "plugin_family": "Settings",
            "count":         12,
            "vulnerability_state": "open",
            "cve":           ["CVE-2023-1234"],
            "risk_factor":   "High",
            "cvss_base_score": 7.5,
            "description":   "A test vulnerability description.",
            "solution":      "Apply vendor patch.",
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"vulnerabilities": [raw_vuln]}
        mock_resp.raise_for_status = MagicMock()
        src._session.get.return_value = mock_resp

        result = src.get_objects("vulnerabilities")

        assert len(result) == 1
        v = result[0]
        assert v["plugin_id"] == 21745
        assert v["cve_id"] == "CVE-2023-1234"
        assert v["cve_ids"] == ["CVE-2023-1234"]
        assert v["name"] == "Authentication Failure - Local Checks Not Run"
        assert v["severity"] == "high"
        assert v["cvss_score"] == 7.5
        assert v["count"] == 12
        assert v["state"] == "open"
        assert v["solution"] == "Apply vendor patch."

    def test_get_vulnerabilities_raises_on_error(self):
        src = self._connected_source()
        src._session.get.side_effect = Exception("timeout")
        with pytest.raises(Exception, match="timeout"):
            src.get_objects("vulnerabilities")

    def test_get_findings_warns_when_details_disabled(self, caplog):
        src = self._connected_source()
        src._include_asset_details = False
        with caplog.at_level(logging.WARNING):
            result = src.get_objects("findings")
        assert result == []
        assert "include_asset_details" in caplog.text

    def test_get_findings_returns_asset_vuln_pairs(self):
        src = self._connected_source()
        src._include_asset_details = True

        raw_asset = {
            "id":               "asset-uuid-003",
            "ipv4":             ["10.10.10.1"],
            "fqdn":             ["server01.corp.com"],
            "netbios_name":     [],
            "operating_system": ["Linux"],
            "mac_address":      [],
            "system_type":      ["Server"],
            "sources":          [],
            "severities":       [],
        }

        raw_vuln = {
            "plugin_id":     10150,
            "plugin_name":   "Windows NetBIOS / SMB Remote Host Information Disclosure",
            "plugin_family": "Windows",
            "count":         1,
            "vulnerability_state": "open",
            "cve":           ["CVE-2021-9999"],
            "risk_factor":   "Medium",
            "cvss_base_score": 5.0,
        }

        asset_list_resp = MagicMock()
        asset_list_resp.raise_for_status = MagicMock()
        asset_list_resp.json.return_value = {"assets": [raw_asset]}

        asset_vuln_resp = MagicMock()
        asset_vuln_resp.raise_for_status = MagicMock()
        asset_vuln_resp.json.return_value = {"vulnerabilities": [raw_vuln]}

        src._session.get.side_effect = [asset_list_resp, asset_vuln_resp]

        result = src.get_objects("findings")

        assert len(result) == 1
        f = result[0]
        assert f["asset_id"] == "asset-uuid-003"
        assert f["cve_id"] == "CVE-2021-9999"
        assert f["name"] == "server01"
        assert f["severity"] == "medium"

    def test_get_findings_raises_on_asset_fetch_error(self):
        src = self._connected_source()
        src._include_asset_details = True
        src._session.get.side_effect = Exception("asset fetch failed")

        with pytest.raises(Exception, match="asset fetch failed"):
            src.get_objects("findings")

    def test_get_findings_raises_on_asset_vulnerability_fetch_error(self):
        src = self._connected_source()
        src._include_asset_details = True

        raw_asset = {
            "id": "asset-uuid-004",
            "ipv4": ["10.10.10.2"],
            "fqdn": ["server02.corp.com"],
            "netbios_name": [],
            "operating_system": ["Linux"],
            "mac_address": [],
            "system_type": ["Server"],
            "sources": [],
            "severities": [],
        }

        asset_list_resp = MagicMock()
        asset_list_resp.raise_for_status = MagicMock()
        asset_list_resp.json.return_value = {"assets": [raw_asset]}

        src._session.get.side_effect = [asset_list_resp, Exception("detail fetch failed")]

        with pytest.raises(Exception, match="detail fetch failed"):
            src.get_objects("findings")


# ---------------------------------------------------------------------------
# _enrich_asset() edge cases
# ---------------------------------------------------------------------------


class TestEnrichAsset:
    def test_fqdn_short_name_truncated_to_64(self):
        src = TenableSource()
        long_fqdn = "a" * 100 + ".example.com"
        raw = {
            "id": "x", "ipv4": [], "ipv6": [], "fqdn": [long_fqdn],
            "netbios_name": [], "operating_system": [], "mac_address": [],
            "system_type": [], "sources": [], "severities": [],
        }
        result = src._enrich_asset(raw)
        assert len(result["name"]) <= 64

    def test_empty_asset_produces_unknown_name(self):
        src = TenableSource()
        raw = {
            "id": "", "ipv4": [], "ipv6": [], "fqdn": [],
            "netbios_name": [], "operating_system": [], "mac_address": [],
            "system_type": [], "sources": [], "severities": [],
        }
        result = src._enrich_asset(raw)
        assert result["name"] == "Unknown"

    def test_mac_address_uppercased(self):
        src = TenableSource()
        raw = {
            "id": "y", "ipv4": [], "ipv6": [], "fqdn": ["host.example.com"],
            "netbios_name": [], "operating_system": [], "mac_address": ["aa:bb:cc:dd:ee:ff"],
            "system_type": [], "sources": [], "severities": [],
        }
        result = src._enrich_asset(raw)
        assert result["mac_address"] == "AA:BB:CC:DD:EE:FF"

    def test_no_mac_gives_empty_string(self):
        src = TenableSource()
        raw = {
            "id": "z", "ipv4": [], "ipv6": [], "fqdn": ["host.example.com"],
            "netbios_name": [], "operating_system": [], "mac_address": [],
            "system_type": [], "sources": [], "severities": [],
        }
        result = src._enrich_asset(raw)
        assert result["mac_address"] == ""

    def test_severity_counts_aggregated(self):
        src = TenableSource()
        raw = {
            "id": "sev-test", "ipv4": [], "ipv6": [], "fqdn": [],
            "netbios_name": [], "operating_system": [], "mac_address": [],
            "system_type": [], "sources": [],
            "severities": [
                {"level": 4, "count": 3, "name": "Critical"},
                {"level": 3, "count": 7, "name": "High"},
                {"level": 2, "count": 12, "name": "Medium"},
                {"level": 1, "count": 5,  "name": "Low"},
            ],
        }
        result = src._enrich_asset(raw)
        assert result["critical_vulns"] == 3
        assert result["high_vulns"] == 7
        assert result["medium_vulns"] == 12
        assert result["low_vulns"] == 5


# ---------------------------------------------------------------------------
# _enrich_vulnerability() edge cases
# ---------------------------------------------------------------------------


class TestEnrichVulnerability:
    def test_cve_id_falls_back_to_plugin_id(self):
        src = TenableSource()
        raw = {
            "plugin_id": 99999, "plugin_name": "Some Check",
            "plugin_family": "General", "count": 1,
            "vulnerability_state": "open",
        }
        result = src._enrich_vulnerability(raw)
        assert result["cve_id"] == "NESSUS-99999"
        assert result["cve_ids"] == []

    def test_severity_derived_from_counts_when_no_risk_factor(self):
        src = TenableSource()
        raw = {
            "plugin_id": 12345, "plugin_name": "Test", "plugin_family": "X",
            "count": 2, "vulnerability_state": "open",
            "counts_by_severity": [
                {"level": 3, "count": 2, "name": "High"},
            ],
        }
        result = src._enrich_vulnerability(raw)
        assert result["severity"] == "high"

    def test_severity_from_risk_factor_string(self):
        src = TenableSource()
        raw = {
            "plugin_id": 11111, "plugin_name": "Test", "plugin_family": "X",
            "count": 1, "vulnerability_state": "open",
            "risk_factor": "Critical",
        }
        result = src._enrich_vulnerability(raw)
        assert result["severity"] == "critical"

    def test_risk_factor_none_maps_to_info(self):
        src = TenableSource()
        raw = {
            "plugin_id": 22222, "plugin_name": "Informational", "plugin_family": "X",
            "count": 1, "vulnerability_state": "open",
            "risk_factor": "None",
        }
        result = src._enrich_vulnerability(raw)
        assert result["severity"] == "info"

    def test_cve_string_wrapped_in_list(self):
        src = TenableSource()
        raw = {
            "plugin_id": 33333, "plugin_name": "Test", "plugin_family": "X",
            "count": 1, "vulnerability_state": "open",
            "cve": "CVE-2020-1234",
            "risk_factor": "Medium",
        }
        result = src._enrich_vulnerability(raw)
        assert result["cve_id"] == "CVE-2020-1234"
        assert result["cve_ids"] == ["CVE-2020-1234"]

    def test_description_falls_back_to_synopsis(self):
        src = TenableSource()
        raw = {
            "plugin_id": 44444, "plugin_name": "Test", "plugin_family": "X",
            "count": 1, "vulnerability_state": "open",
            "risk_factor": "Low",
            "synopsis": "A brief synopsis.",
        }
        result = src._enrich_vulnerability(raw)
        assert result["description"] == "A brief synopsis."


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestClose:
    def test_close_clears_session(self):
        src = TenableSource()
        src._session = MagicMock()
        src._platform = "tenable"
        src.close()
        assert src._session is None

    def test_close_safe_when_not_connected(self):
        src = TenableSource()
        src.close()  # should not raise
        assert src._session is None

    def test_close_sends_delete_for_nessus(self):
        src = TenableSource()
        mock_session = MagicMock()
        src._session = mock_session
        src._platform = "nessus"
        src._base_url = "https://nessus.example.com:8834"
        src.close()
        # DELETE /session should have been called once before the session was cleared.
        mock_session.delete.assert_called_once()
        assert src._session is None
