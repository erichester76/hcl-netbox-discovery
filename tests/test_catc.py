"""Tests for the Cisco Catalyst Center source adapter (collector/sources/catc.py).

All DNAC SDK calls are mocked — no real Catalyst Center is required.
The dnacentersdk package is an optional runtime dep; this module injects a
lightweight fake module into sys.modules so the adapter can be tested without
installing the SDK.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Inject a fake dnacentersdk module so adapter imports succeed without the SDK.
# ---------------------------------------------------------------------------
if "dnacentersdk" not in sys.modules:
    _fake_sdk = ModuleType("dnacentersdk")
    _fake_sdk_api = ModuleType("dnacentersdk.api")
    _fake_sdk_api.DNACenterAPI = MagicMock()
    _fake_sdk.api = _fake_sdk_api
    sys.modules["dnacentersdk"] = _fake_sdk
    sys.modules["dnacentersdk.api"] = _fake_sdk_api

from collector.sources.catc import (  # noqa: E402
    CatalystCenterSource,
    _hierarchy_part,
    _normalize_model,
    _safe_get,
)


# ---------------------------------------------------------------------------
# _normalize_model()
# ---------------------------------------------------------------------------


class TestNormalizeModel:
    @pytest.mark.parametrize(
        "platform_id, expected",
        [
            ("WS-C3560-48PS-S", "Catalyst 3560-48PS-S"),
            ("C9300-48P-K9", "Catalyst 9300-48P"),
            ("C9200L-24P-4G-A", "Catalyst 9200L-24P-4G-A"),
            ("IE-3300-8T2S-A", "Catalyst IE 3300-8T2S-A"),
            ("AIR-AP3802I-E-K9", "Catalyst 3802I-E"),
            ("AIR-CAP3702I-A-K9", "Catalyst 3702I-A"),
            ("", "Unknown"),
            ("UNKNOWN-DEVICE", "UNKNOWN-DEVICE"),
        ],
    )
    def test_normalize_model(self, platform_id, expected):
        assert _normalize_model(platform_id) == expected

    def test_multiple_models_keeps_first(self):
        # Comma-separated: keeps only the part before the first comma
        result = _normalize_model("C9300-48P-K9,C9300-24P-K9")
        assert "," not in result
        assert result.startswith("Catalyst")


# ---------------------------------------------------------------------------
# _hierarchy_part()
# ---------------------------------------------------------------------------


class TestHierarchyPart:
    def test_level_0_is_global(self):
        assert _hierarchy_part("Global/US/Southeast/Clemson/Library", 0) == "Global"

    def test_level_3_is_site(self):
        assert _hierarchy_part("Global/US/Southeast/Clemson/Library", 3) == "Clemson"

    def test_level_4_is_building(self):
        assert _hierarchy_part("Global/US/Southeast/Clemson/Library", 4) == "Library"

    def test_missing_level_returns_empty(self):
        assert _hierarchy_part("Global/US", 5) == ""

    def test_empty_hierarchy_returns_empty(self):
        assert _hierarchy_part("", 0) == ""

    def test_leading_slash_ignored(self):
        assert _hierarchy_part("/Global/US/Southeast", 0) == "Global"


# ---------------------------------------------------------------------------
# _safe_get()
# ---------------------------------------------------------------------------


class TestSafeGet:
    def test_dict_access(self):
        assert _safe_get({"key": "val"}, "key") == "val"

    def test_dict_missing_key_returns_default(self):
        assert _safe_get({}, "key", "default") == "default"

    def test_object_access(self):
        obj = SimpleNamespace(name="switch-01")
        assert _safe_get(obj, "name") == "switch-01"

    def test_object_missing_attr_returns_default(self):
        obj = SimpleNamespace()
        assert _safe_get(obj, "missing", 42) == 42

    def test_none_default(self):
        assert _safe_get({}, "key") is None


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestCatalystConnect:
    def test_connect_creates_dnac_client(self, catc_config):
        fake_api = MagicMock()
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=fake_api)

        src = CatalystCenterSource()
        src.connect(catc_config)

        _fake_dnac_api.DNACenterAPI.assert_called_once_with(
            base_url=catc_config.url,
            username=catc_config.username,
            password=catc_config.password,
            verify=catc_config.verify_ssl,
        )
        assert src._client is fake_api

    def test_connect_prepends_https_if_missing(self, catc_config):
        catc_config.url = "catc.example.com"  # no scheme
        fake_api = MagicMock()
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=fake_api)

        src = CatalystCenterSource()
        src.connect(catc_config)

        call_kwargs = _fake_dnac_api.DNACenterAPI.call_args[1]
        assert call_kwargs["base_url"].startswith("https://")

    def test_connect_raises_if_sdk_missing(self, catc_config):
        src = CatalystCenterSource()
        with patch.dict("sys.modules", {"dnacentersdk": None, "dnacentersdk.api": None}):
            with pytest.raises((RuntimeError, ImportError)):
                src.connect(catc_config)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestCatalystGetObjects:
    def _connected_source(self) -> CatalystCenterSource:
        src = CatalystCenterSource()
        src._client = MagicMock()
        return src

    def test_raises_without_connect(self):
        src = CatalystCenterSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("devices")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("switches")

    def test_get_devices_returns_enriched_dicts(self):
        src = self._connected_source()

        site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])

        src._client.sites.get_site.return_value = SimpleNamespace(response=[site])
        src._client.sites.get_membership.return_value = membership

        result = src.get_objects("devices")

        assert len(result) == 1
        d = result[0]
        assert d["name"] == "switch-01"
        assert d["model"] == "Catalyst 9300-48P"
        assert d["manufacturer"] == "Cisco"
        assert d["serial"] == "FOC12345678"
        assert d["status"] == "active"
        assert d["site_name"] == "Clemson"

    def test_deduplicates_by_serial(self):
        src = self._connected_source()

        site1 = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/SE/CU")
        site2 = SimpleNamespace(id="site-2", siteNameHierarchy="Global/US/SE/CU")
        device = SimpleNamespace(
            hostname="switch-01",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="SERIAL001",
            reachabilityStatus="Reachable",
            family="Switches",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])

        def get_site_side_effect(**kwargs):
            offset = kwargs.get("offset", 1)
            if offset == 1:
                return SimpleNamespace(response=[site1, site2])
            return SimpleNamespace(response=[])

        src._client.sites.get_site.side_effect = get_site_side_effect
        src._client.sites.get_membership.return_value = membership

        result = src.get_objects("devices")
        # Device with same serial should appear only once
        assert len(result) == 1

    def test_offline_device(self):
        src = self._connected_source()

        site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/SE/CU")
        device = SimpleNamespace(
            hostname="router-01",
            platformId="ISR4431/K9",
            role="BORDER ROUTER",
            softwareType="IOS-XE",
            softwareVersion="16.12.4",
            serialNumber="FGL12345678",
            reachabilityStatus="Unreachable",
            family="Routers",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])

        src._client.sites.get_site.return_value = SimpleNamespace(response=[site])
        src._client.sites.get_membership.return_value = membership

        result = src.get_objects("devices")
        assert result[0]["status"] == "offline"


# ---------------------------------------------------------------------------
# _enrich_device()
# ---------------------------------------------------------------------------


class TestCatalystEnrichDevice:
    def test_hostname_stripped_to_64_chars(self):
        src = CatalystCenterSource()
        long_hostname = "a" * 100 + ".example.com"
        device = SimpleNamespace(
            hostname=long_hostname,
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC123",
            reachabilityStatus="Reachable",
            family="Switches",
        )
        result = src._enrich_device(device, "Global/US/SE/CU")
        assert len(result["name"]) <= 64

    def test_empty_hostname_becomes_unknown(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="",
            platformId="",
            role="",
            softwareType="",
            softwareVersion="",
            serialNumber="",
            reachabilityStatus="",
            family="",
        )
        result = src._enrich_device(device, "")
        assert result["name"] == "Unknown"

    def test_role_formatted_as_title_case(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="sw-01",
            platformId="C9300",
            role="DISTRIBUTION_LAYER",
            softwareType="IOS-XE",
            softwareVersion="17.6",
            serialNumber="SN001",
            reachabilityStatus="Reachable",
            family="Switches",
        )
        result = src._enrich_device(device, "Global/US/SE/CU")
        assert result["role"] == "Distribution Layer"

    def test_serial_uppercased(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="sw-01",
            platformId="C9300",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6",
            serialNumber="foc12345",
            reachabilityStatus="Reachable",
            family="Switches",
        )
        result = src._enrich_device(device, "Global/US/SE/CU")
        assert result["serial"] == "FOC12345"


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestCatalystClose:
    def test_close_clears_client(self):
        src = CatalystCenterSource()
        src._client = MagicMock()
        src.close()
        assert src._client is None
