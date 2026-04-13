"""Tests for the Cisco Catalyst Center source adapter (collector/sources/catc.py).

All DNAC SDK calls are mocked — no real Catalyst Center is required.
The dnacentersdk package is an optional runtime dep; this module injects a
lightweight fake module into sys.modules so the adapter can be tested without
installing the SDK.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, call, patch

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
    _mask_to_prefix,
    _normalize_iface_type,
    _normalize_model,
    _normalize_module_profile,
    _normalize_module_status,
    _parse_speed_mbps,
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

    def test_level_2_is_site(self):
        assert _hierarchy_part("Global/US/Southeast/Clemson/Library", 2) == "Southeast"

    def test_level_3_is_building(self):
        assert _hierarchy_part("Global/US/Southeast/Clemson/Library", 3) == "Clemson"

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
            wait_on_rate_limit=True,
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

    @pytest.mark.parametrize("username,password", [
        ("", "secret"),
        ("admin", ""),
        ("", ""),
    ])
    def test_connect_raises_if_credentials_missing(self, catc_config, username, password):
        catc_config.username = username
        catc_config.password = password
        src = CatalystCenterSource()
        with pytest.raises(RuntimeError, match="CATC_USER"):
            src.connect(catc_config)

    def test_connect_defaults_site_assignment_strategy_to_auto(self, catc_config):
        fake_api = MagicMock()
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=fake_api)

        src = CatalystCenterSource()
        src.connect(catc_config)

        assert src._site_assignment_strategy == "auto"

    def test_connect_accepts_membership_site_assignment_strategy(self, catc_config):
        catc_config.extra = {
            "fetch_interfaces": "false",
            "site_assignment_strategy": "membership",
        }
        fake_api = MagicMock()
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=fake_api)

        src = CatalystCenterSource()
        src.connect(catc_config)

        assert src._site_assignment_strategy == "membership"

    def test_connect_invalid_site_assignment_strategy_defaults_to_auto(self, catc_config, caplog):
        catc_config.extra = {
            "fetch_interfaces": "false",
            "site_assignment_strategy": "bogus",
        }
        fake_api = MagicMock()
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=fake_api)

        src = CatalystCenterSource()
        with caplog.at_level("WARNING"):
            src.connect(catc_config)

        assert src._site_assignment_strategy == "auto"
        assert "unsupported site_assignment_strategy" in caplog.text


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestCatalystGetObjects:
    def _connected_source(self) -> CatalystCenterSource:
        src = CatalystCenterSource()
        src._client = MagicMock()
        src._client.site_design = None
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
        assert d["site_name"] == "Southeast"

    def test_get_devices_maps_building_segment_to_location(self):
        src = self._connected_source()

        site = SimpleNamespace(
            id="site-1",
            siteNameHierarchy="Global/US/Southeast/Watt/First Floor",
        )
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

        assert result[0]["site_name"] == "Southeast"
        assert result[0]["location_name"] == "Watt"

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

    def test_hierarchy_labels_are_title_cased(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="ap-01",
            platformId="AIR-CAP2702E-B-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6",
            serialNumber="sn001",
            reachabilityStatus="Reachable",
            family="Unified AP",
        )
        result = src._enrich_device(device, "Global/US/Southeast/watt/first floor")
        assert result["site_name"] == "Southeast"
        assert result["location_name"] == "Watt"


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestCatalystClose:
    def test_close_clears_client(self):
        src = CatalystCenterSource()
        src._client = MagicMock()
        src.close()
        assert src._client is None


# ---------------------------------------------------------------------------
# _normalize_iface_type()
# ---------------------------------------------------------------------------


class TestNormalizeIfaceType:
    @pytest.mark.parametrize(
        "raw_type, expected",
        [
            ("Physical",     "1000base-t"),
            ("Management",   "1000base-t"),
            ("Virtual",      "virtual"),
            ("SVI",          "virtual"),
            ("Loopback",     "virtual"),
            ("Port-Channel", "lag"),
            ("Tunnel",       "virtual"),
            ("NVE",          "virtual"),
            ("Unknown",      "other"),
            ("",             "other"),
        ],
    )
    def test_normalize_iface_type(self, raw_type, expected):
        assert _normalize_iface_type(raw_type) == expected


# ---------------------------------------------------------------------------
# _parse_speed_mbps()
# ---------------------------------------------------------------------------


class TestParseSpeedMbps:
    @pytest.mark.parametrize(
        "speed_str, expected",
        [
            ("1000000000", 1000),     # 1 Gbps in bps
            ("100000000",  100),      # 100 Mbps in bps
            ("10000000",   10),       # 10 Mbps in bps
            ("1 Gbps",     1000),
            ("10Gbps",     10000),
            ("100 Mbps",   100),
            ("1000M",      1000),
            ("0",          None),
            ("",           None),
            ("unknown",    None),
        ],
    )
    def test_parse_speed(self, speed_str, expected):
        assert _parse_speed_mbps(speed_str) == expected


# ---------------------------------------------------------------------------
# _mask_to_prefix()
# ---------------------------------------------------------------------------


class TestMaskToPrefix:
    @pytest.mark.parametrize(
        "mask, expected",
        [
            ("255.255.255.0",   24),
            ("255.255.0.0",     16),
            ("255.0.0.0",        8),
            ("255.255.255.255", 32),
            ("0.0.0.0",          0),
            ("",               None),
            ("not-a-mask",     None),
            ("255.255.255",    None),  # only 3 octets
        ],
    )
    def test_mask_to_prefix(self, mask, expected):
        assert _mask_to_prefix(mask) == expected


# ---------------------------------------------------------------------------
# connect() — fetch_interfaces option
# ---------------------------------------------------------------------------


class TestCatalystConnectFetchInterfaces:
    def test_fetch_interfaces_true_by_default(self, catc_config):
        catc_config.extra = {}
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=MagicMock())

        src = CatalystCenterSource()
        src.connect(catc_config)
        assert src._fetch_interfaces is True

    def test_fetch_interfaces_enabled_via_extra(self, catc_config):
        catc_config.extra = {"fetch_interfaces": "true"}
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=MagicMock())

        src = CatalystCenterSource()
        src.connect(catc_config)
        assert src._fetch_interfaces is True

    def test_fetch_interfaces_false_explicit(self, catc_config):
        catc_config.extra = {"fetch_interfaces": "false"}
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=MagicMock())

        src = CatalystCenterSource()
        src.connect(catc_config)
        assert src._fetch_interfaces is False

    def test_fetch_modules_false_by_default(self, catc_config):
        catc_config.extra = {}
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=MagicMock())

        src = CatalystCenterSource()
        src.connect(catc_config)
        assert src._fetch_modules is False

    def test_fetch_modules_enabled_via_extra(self, catc_config):
        catc_config.extra = {"fetch_modules": "true"}
        _fake_dnac_api = sys.modules["dnacentersdk.api"]
        _fake_dnac_api.DNACenterAPI = MagicMock(return_value=MagicMock())

        src = CatalystCenterSource()
        src.connect(catc_config)
        assert src._fetch_modules is True


# ---------------------------------------------------------------------------
# _enrich_device() — management IP and device ID
# ---------------------------------------------------------------------------


class TestCatalystEnrichDeviceExtended:
    def test_management_ip_included(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="sw-01",
            platformId="C9300",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6",
            serialNumber="SN001",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        result = src._enrich_device(device, "Global/US/SE/CU")
        assert result["ip_address"] == "10.0.0.1"
        assert result["managementIpAddress"] == "10.0.0.1"

    def test_device_id_included(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="sw-01",
            platformId="C9300",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6",
            serialNumber="SN001",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="abc-123-uuid",
        )
        result = src._enrich_device(device, "Global/US/SE/CU")
        assert result["deviceId"] == "abc-123-uuid"

    def test_missing_management_ip_returns_empty_string(self):
        src = CatalystCenterSource()
        device = SimpleNamespace(
            hostname="sw-01",
            platformId="C9300",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6",
            serialNumber="SN001",
            reachabilityStatus="Reachable",
            family="Switches",
        )
        result = src._enrich_device(device, "Global/US/SE/CU")
        assert result["ip_address"] == ""


# ---------------------------------------------------------------------------
# _enrich_interface()
# ---------------------------------------------------------------------------


class TestCatalystEnrichInterface:
    def _make_src(self) -> CatalystCenterSource:
        src = CatalystCenterSource()
        src._client = MagicMock()
        return src

    def test_physical_interface_enriched(self):
        src = self._make_src()
        iface = {
            "portName":      "GigabitEthernet1/0/1",
            "interfaceType": "Physical",
            "adminStatus":   "UP",
            "operStatus":    "up",
            "description":   "Uplink to core",
            "macAddress":    "aa:bb:cc:dd:ee:ff",
            "ipv4Address":   "10.0.0.1",
            "ipv4Mask":      "255.255.255.0",
            "speed":         "1000000000",
        }
        result = src._enrich_interface(iface)
        assert result["name"] == "GigabitEthernet1/0/1"
        assert result["type"] == "1000base-t"
        assert result["enabled"] is True
        assert result["description"] == "Uplink to core"
        assert result["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert result["ip_address"] == "10.0.0.1/24"
        assert result["speed"] == 1000

    def test_loopback_interface_type(self):
        src = self._make_src()
        iface = {
            "portName":      "Loopback0",
            "interfaceType": "Loopback",
            "adminStatus":   "UP",
            "operStatus":    "up",
            "description":   "",
            "macAddress":    "",
            "ipv4Address":   "192.168.1.1",
            "ipv4Mask":      "255.255.255.255",
            "speed":         "",
        }
        result = src._enrich_interface(iface)
        assert result["type"] == "virtual"
        assert result["ip_address"] == "192.168.1.1/32"
        assert result["speed"] is None

    def test_admin_down_interface(self):
        src = self._make_src()
        iface = {
            "portName":      "GigabitEthernet1/0/2",
            "interfaceType": "Physical",
            "adminStatus":   "DOWN",
            "operStatus":    "down",
            "description":   "",
            "macAddress":    "",
            "ipv4Address":   "",
            "ipv4Mask":      "",
            "speed":         "1000000000",
        }
        result = src._enrich_interface(iface)
        assert result["enabled"] is False
        assert result["ip_address"] == ""

    def test_interface_without_ip(self):
        src = self._make_src()
        iface = {
            "portName":      "GigabitEthernet1/0/3",
            "interfaceType": "Physical",
            "adminStatus":   "UP",
            "operStatus":    "up",
            "description":   "",
            "macAddress":    "11:22:33:44:55:66",
            "ipv4Address":   "",
            "ipv4Mask":      "",
            "speed":         "100000000",
        }
        result = src._enrich_interface(iface)
        assert result["ip_address"] == ""
        assert result["mac_address"] == "11:22:33:44:55:66"

    def test_unknown_interface_type_defaults_to_other(self):
        src = self._make_src()
        iface = {
            "portName":      "Wlan0",
            "interfaceType": "Wireless",
            "adminStatus":   "UP",
            "operStatus":    "up",
            "description":   "",
            "macAddress":    "",
            "ipv4Address":   "",
            "ipv4Mask":      "",
            "speed":         "",
        }
        result = src._enrich_interface(iface)
        assert result["type"] == "other"


# ---------------------------------------------------------------------------
# _fetch_device_interfaces()
# ---------------------------------------------------------------------------


class TestCatalystFetchDeviceInterfaces:
    def _connected_source(self) -> CatalystCenterSource:
        src = CatalystCenterSource()
        src._client = MagicMock()
        src._client.site_design = None
        return src

    def test_returns_enriched_interfaces(self):
        src = self._connected_source()
        raw_iface = {
            "portName":      "GigabitEthernet1/0/1",
            "interfaceType": "Physical",
            "adminStatus":   "UP",
            "operStatus":    "up",
            "description":   "",
            "macAddress":    "aa:bb:cc:dd:ee:ff",
            "ipv4Address":   "",
            "ipv4Mask":      "",
            "speed":         "1000000000",
        }
        src._client.devices.get_interface_info_by_id.return_value = SimpleNamespace(
            response=[raw_iface]
        )

        result = src._fetch_device_interfaces("device-uuid-1")
        assert len(result) == 1
        assert result[0]["name"] == "GigabitEthernet1/0/1"

    def test_returns_empty_list_on_api_error(self):
        src = self._connected_source()
        src._client.devices.get_interface_info_by_id.side_effect = Exception("API error")

        result = src._fetch_device_interfaces("device-uuid-1")
        assert result == []


# ---------------------------------------------------------------------------
# module normalization and fetch
# ---------------------------------------------------------------------------


class TestNormalizeModuleHelpers:
    @pytest.mark.parametrize(
        "name, vendor_equipment_type, expected",
        [
            ("Power Supply 1", "", "Power supply"),
            ("FAN1", "", "Fan"),
            ("Line Card 1", "", "Line card"),
            ("Supervisor Engine", "", "Supervisor"),
            ("Module 1", "C9300-NM-8X", "Module"),
        ],
    )
    def test_normalize_module_profile(self, name, vendor_equipment_type, expected):
        assert _normalize_module_profile(name, vendor_equipment_type) == expected

    @pytest.mark.parametrize(
        "state, expected",
        [
            ("OK", "active"),
            ("up", "active"),
            ("DOWN", "offline"),
            ("failed", "offline"),
            ("warning", "active"),
            ("degraded", "active"),
            ("missing", "offline"),
            ("mystery", "active"),
            ("", "active"),
        ],
    )
    def test_normalize_module_status(self, state, expected):
        assert _normalize_module_status(state) == expected


class TestCatalystModules:
    def _connected_source(self) -> CatalystCenterSource:
        src = CatalystCenterSource()
        src._client = MagicMock()
        src._client.site_design = None
        return src

    def test_enrich_module(self):
        src = self._connected_source()
        module = {
            "name": "Power Supply 1",
            "vendorEquipmentType": "PWR-C1-715WAC-P",
            "partNumber": "341-100215-01",
            "serialNumber": "LIT1234ABC",
            "manufacturer": "Cisco",
            "moduleIndex": 1,
            "operationalStateCode": "OK",
            "description": "Primary power supply",
        }

        result = src._enrich_module(module)

        assert result is not None
        assert result["bay_name"] == "Power Supply 1"
        assert result["profile"] == "Power supply"
        assert result["model"] == "PWR-C1-715WAC-P"
        assert result["serial"] == "LIT1234ABC"
        assert result["status"] == "active"
        assert result["attributes"]["part_number"] == "341-100215-01"

    def test_fetch_device_modules_returns_normalized_modules(self):
        src = self._connected_source()
        raw_module = {
            "name": "Fan 1",
            "vendorEquipmentType": "FAN-T1",
            "partNumber": "FAN-T1",
            "serialNumber": "FAN123",
            "manufacturer": "Cisco",
            "moduleIndex": 2,
            "operationalStateCode": "OK",
        }
        src._client.devices.get_modules.return_value = SimpleNamespace(response=[raw_module])

        result = src._fetch_device_modules("device-uuid-1")

        assert len(result) == 1
        assert result[0]["profile"] == "Fan"
        src._client.devices.get_modules.assert_called_once_with(
            deviceId="device-uuid-1",
            offset=1,
            limit=500,
        )

    def test_fetch_device_modules_paginates_until_short_page(self):
        src = self._connected_source()
        first_page = [{"name": f"Module {idx}", "vendorEquipmentType": "MOD", "moduleIndex": idx} for idx in range(1, 501)]
        second_page = [{"name": "Module 501", "vendorEquipmentType": "MOD", "moduleIndex": 501}]
        src._client.devices.get_modules.side_effect = [
            SimpleNamespace(response=first_page),
            SimpleNamespace(response=second_page),
        ]

        result = src._fetch_device_modules("device-uuid-1")

        assert len(result) == 501
        assert src._client.devices.get_modules.call_args_list == [
            call(deviceId="device-uuid-1", offset=1, limit=500),
            call(deviceId="device-uuid-1", offset=501, limit=500),
        ]
    def test_get_objects_fetches_modules_when_enabled(self):
        src = self._connected_source()
        src._fetch_interfaces = False
        src._fetch_modules = True

        site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])
        raw_module = {
            "name": "Power Supply 1",
            "vendorEquipmentType": "PWR-C1-715WAC-P",
            "partNumber": "341-100215-01",
            "serialNumber": "LIT1234ABC",
            "manufacturer": "Cisco",
            "moduleIndex": 1,
            "operationalStateCode": "OK",
        }

        src._client.sites.get_site.return_value = SimpleNamespace(response=[site])
        src._client.sites.get_membership.return_value = membership
        src._client.devices.get_modules.return_value = SimpleNamespace(response=[raw_module])

        result = src.get_objects("devices")

        assert len(result) == 1
        assert "modules" in result[0]
        assert len(result[0]["modules"]) == 1
        assert result[0]["modules"][0]["bay_name"] == "Power Supply 1"

    def test_returns_empty_list_when_response_not_list(self):
        src = self._connected_source()
        src._client.devices.get_interface_info_by_id.return_value = SimpleNamespace(
            response=None
        )

        result = src._fetch_device_interfaces("device-uuid-1")
        assert result == []


# ---------------------------------------------------------------------------
# get_objects() with fetch_interfaces enabled
# ---------------------------------------------------------------------------


class TestCatalystGetObjectsWithInterfaces:
    def test_interfaces_fetched_when_enabled(self):
        src = CatalystCenterSource()
        src._client = MagicMock()
        src._client.site_design = None
        src._fetch_interfaces = True

        site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])

        raw_iface = {
            "portName":      "GigabitEthernet1/0/1",
            "interfaceType": "Physical",
            "adminStatus":   "UP",
            "operStatus":    "up",
            "description":   "Uplink",
            "macAddress":    "aa:bb:cc:dd:ee:ff",
            "ipv4Address":   "",
            "ipv4Mask":      "",
            "speed":         "1000000000",
        }
        src._client.sites.get_site.return_value = SimpleNamespace(response=[site])
        src._client.sites.get_membership.return_value = membership
        src._client.devices.get_interface_info_by_id.return_value = SimpleNamespace(
            response=[raw_iface]
        )

        result = src.get_objects("devices")

        assert len(result) == 1
        assert "interfaces" in result[0]
        assert len(result[0]["interfaces"]) == 1
        assert result[0]["interfaces"][0]["name"] == "GigabitEthernet1/0/1"

    def test_unified_ap_gets_mgmt0_radio0_with_management_ip(self):
        src = CatalystCenterSource()
        src._client = MagicMock()
        src._client.site_design = None
        src._fetch_interfaces = True

        site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="ap-01",
            platformId="C9120AXI-B",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.9.3",
            serialNumber="FCW12345678",
            reachabilityStatus="Reachable",
            family="Unified AP",
            managementIpAddress="10.0.53.115",
            macAddress="70:7d:b9:33:47:c0",
            apEthernetMacAddress="38:90:a5:f9:3d:cc",
            id="ap-device-uuid-1",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])

        src._client.sites.get_site.return_value = SimpleNamespace(response=[site])
        src._client.sites.get_membership.return_value = membership

        result = src.get_objects("devices")

        assert len(result) == 1
        ap = result[0]
        assert "interfaces" in ap
        names = {iface["name"] for iface in ap["interfaces"]}
        assert names == {"mgmt0", "radio0"}

        mgmt0 = next(iface for iface in ap["interfaces"] if iface["name"] == "mgmt0")
        radio0 = next(iface for iface in ap["interfaces"] if iface["name"] == "radio0")

        assert mgmt0["ip_address"] == "10.0.53.115/32"
        assert mgmt0["mgmt_only"] is True
        assert radio0["ip_address"] == ""
        assert radio0["mac_address"] == "38:90:A5:F9:3D:CC"
        src._client.devices.get_interface_info_by_id.assert_not_called()

    def test_interfaces_not_fetched_when_disabled(self):
        src = CatalystCenterSource()
        src._client = MagicMock()
        src._client.site_design = None
        src._fetch_interfaces = False

        site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        member = SimpleNamespace(response=[device])
        membership = SimpleNamespace(device=[member])

        src._client.sites.get_site.return_value = SimpleNamespace(response=[site])
        src._client.sites.get_membership.return_value = membership

        result = src.get_objects("devices")

        assert len(result) == 1
        assert "interfaces" not in result[0]
        src._client.devices.get_interface_info_by_id.assert_not_called()


# ---------------------------------------------------------------------------
# bulk site assignments and 429 handling
# ---------------------------------------------------------------------------


class TestCatalystBulkAssignments:
    def _connected_source(self) -> CatalystCenterSource:
        src = CatalystCenterSource()
        src._client = MagicMock()
        return src

    def test_get_devices_prefers_bulk_site_assignment_join(self):
        src = self._connected_source()

        root_site = SimpleNamespace(id="area-1", siteNameHierarchy="Global/US")
        child_site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        assignment = SimpleNamespace(
            deviceId="device-uuid-1",
            siteId="site-1",
            siteNameHierarchy="Global/US/Southeast/Clemson",
        )

        src._client.sites.get_site.side_effect = [
            SimpleNamespace(response=[root_site, child_site]),
            SimpleNamespace(response=[]),
        ]
        src._client.devices.get_device_list.side_effect = [
            SimpleNamespace(response=[device]),
            SimpleNamespace(response=[]),
        ]
        src._client.site_design.get_site_assigned_network_devices.return_value = SimpleNamespace(
            response=[assignment]
        )

        result = src.get_objects("devices")

        assert len(result) == 1
        assert result[0]["deviceId"] == "device-uuid-1"
        assert result[0]["site_name"] == "Southeast"
        src._client.site_design.get_site_assigned_network_devices.assert_called_once_with(
            site_id="area-1",
            offset=1,
            limit=500,
        )
        src._client.sites.get_membership.assert_not_called()

    def test_get_devices_prefers_membership_when_configured(self):
        src = self._connected_source()
        src._site_assignment_strategy = "membership"

        root_site = SimpleNamespace(id="area-1", siteNameHierarchy="Global/US")
        child_site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        membership = SimpleNamespace(device=[SimpleNamespace(response=[device])])

        src._client.sites.get_site.side_effect = [
            SimpleNamespace(response=[root_site, child_site]),
            SimpleNamespace(response=[]),
        ]
        src._client.sites.get_membership.side_effect = [
            SimpleNamespace(device=[]),
            membership,
        ]

        result = src.get_objects("devices")

        assert len(result) == 1
        assert result[0]["deviceId"] == "device-uuid-1"
        src._client.site_design.get_site_assigned_network_devices.assert_not_called()

    def test_get_devices_membership_first_falls_back_to_bulk(self):
        src = self._connected_source()
        src._site_assignment_strategy = "membership"

        root_site = SimpleNamespace(id="area-1", siteNameHierarchy="Global/US")
        child_site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        assignment = {
            "deviceId": "device-uuid-1",
            "siteId": "site-1",
            "siteNameHierarchy": "Global/US/Southeast/Clemson",
        }

        src._client.sites.get_site.side_effect = [
            SimpleNamespace(response=[root_site, child_site]),
            SimpleNamespace(response=[]),
        ]
        src._client.sites.get_membership.side_effect = [
            SimpleNamespace(device=[]),
            SimpleNamespace(device=[]),
        ]
        src._client.devices.get_device_list.side_effect = [
            SimpleNamespace(response=[device]),
            SimpleNamespace(response=[]),
        ]
        src._client.site_design.get_site_assigned_network_devices.return_value = SimpleNamespace(
            response={"response": [assignment]}
        )

        result = src.get_objects("devices")

        assert len(result) == 1
        assert result[0]["deviceId"] == "device-uuid-1"
        assert src._client.sites.get_membership.called
        src._client.site_design.get_site_assigned_network_devices.assert_called_once()

    def test_bulk_site_assignment_uses_site_lookup_when_hierarchy_missing(self):
        src = self._connected_source()

        root_site = SimpleNamespace(id="area-1", siteNameHierarchy="Global/US")
        child_site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        assignment = {
            "deviceId": "device-uuid-1",
            "siteId": "site-1",
        }

        src._client.sites.get_site.side_effect = [
            SimpleNamespace(response=[root_site, child_site]),
            SimpleNamespace(response=[]),
        ]
        src._client.devices.get_device_list.side_effect = [
            SimpleNamespace(response=[device]),
            SimpleNamespace(response=[]),
        ]
        src._client.site_design.get_site_assigned_network_devices.return_value = SimpleNamespace(
            response={"response": [assignment]}
        )

        result = src.get_objects("devices")

        assert len(result) == 1
        assert result[0]["deviceId"] == "device-uuid-1"
        assert result[0]["site_name"] == "Southeast"

    def test_bulk_site_assignment_parses_nested_device_and_site_shapes(self):
        src = self._connected_source()

        root_site = SimpleNamespace(id="area-1", siteNameHierarchy="Global/US")
        child_site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        assignment = {
            "device": {"id": "device-uuid-1"},
            "site": {"id": "site-1"},
        }

        src._client.sites.get_site.side_effect = [
            SimpleNamespace(response=[root_site, child_site]),
            SimpleNamespace(response=[]),
        ]
        src._client.devices.get_device_list.side_effect = [
            SimpleNamespace(response=[device]),
            SimpleNamespace(response=[]),
        ]
        src._client.site_design.get_site_assigned_network_devices.return_value = SimpleNamespace(
            response=[assignment]
        )

        result = src.get_objects("devices")

        assert len(result) == 1
        assert result[0]["deviceId"] == "device-uuid-1"
        assert result[0]["site_name"] == "Southeast"

    def test_select_assignment_roots_uses_shallowest_non_global_hierarchy(self):
        src = self._connected_source()
        sites = [
            SimpleNamespace(id="global", siteNameHierarchy="Global"),
            SimpleNamespace(id="area-1", siteNameHierarchy="Global/US"),
            SimpleNamespace(id="area-2", siteNameHierarchy="Global/CA"),
            SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson"),
        ]

        roots = src._select_assignment_roots(sites)

        assert [site.id for site in roots] == ["area-1", "area-2"]

    def test_select_assignment_root_groups_orders_by_depth(self):
        src = self._connected_source()
        sites = [
            SimpleNamespace(id="global", siteNameHierarchy="Global"),
            SimpleNamespace(id="area-1", siteNameHierarchy="Global/US"),
            SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson"),
            SimpleNamespace(id="building-1", siteNameHierarchy="Global/US/Southeast/Clemson/Tillman"),
        ]

        groups = src._select_assignment_root_groups(sites)

        assert [(depth, [site.id for site in roots]) for depth, roots in groups] == [
            (2, ["area-1"]),
            (4, ["site-1"]),
            (5, ["building-1"]),
        ]

    def test_get_devices_bulk_assignment_retries_deeper_roots_when_shallow_roots_empty(self):
        src = self._connected_source()

        area_site = SimpleNamespace(id="area-1", siteNameHierarchy="Global/US")
        child_site = SimpleNamespace(id="site-1", siteNameHierarchy="Global/US/Southeast/Clemson")
        device = SimpleNamespace(
            hostname="switch-01.clemson.edu",
            platformId="C9300-48P-K9",
            role="ACCESS",
            softwareType="IOS-XE",
            softwareVersion="17.6.4",
            serialNumber="FOC12345678",
            reachabilityStatus="Reachable",
            family="Switches",
            managementIpAddress="10.0.0.1",
            id="device-uuid-1",
        )
        assignment = SimpleNamespace(
            deviceId="device-uuid-1",
            siteId="site-1",
            siteNameHierarchy="Global/US/Southeast/Clemson",
        )

        src._client.sites.get_site.side_effect = [
            SimpleNamespace(response=[area_site, child_site]),
            SimpleNamespace(response=[]),
        ]
        src._client.devices.get_device_list.side_effect = [
            SimpleNamespace(response=[device]),
            SimpleNamespace(response=[]),
        ]
        src._client.site_design.get_site_assigned_network_devices.side_effect = [
            SimpleNamespace(response=[]),
            SimpleNamespace(response=[assignment]),
        ]

        result = src.get_objects("devices")

        assert len(result) == 1
        assert result[0]["deviceId"] == "device-uuid-1"
        assert result[0]["site_name"] == "Southeast"
        assert result[0]["location_name"] == "Clemson"
        assert src._client.site_design.get_site_assigned_network_devices.call_args_list == [
            call(site_id="area-1", offset=1, limit=500),
            call(site_id="site-1", offset=1, limit=500),
        ]
        src._client.sites.get_membership.assert_not_called()

    def test_rate_limit_backoff_retries_429_and_uses_retry_after(self):
        src = self._connected_source()
        src._rate_limit_retry_attempts = 3
        src._rate_limit_retry_initial_delay = 1.0
        src._rate_limit_retry_max_delay = 30.0
        src._rate_limit_retry_jitter = 0.0

        class RateLimitError(Exception):
            def __init__(self):
                self.status_code = 429
                self.response = SimpleNamespace(headers={"Retry-After": "7"})

        fn = MagicMock(side_effect=[RateLimitError(), "ok"])

        with patch("collector.sources.catc.time.sleep") as mock_sleep:
            result = src._call_with_rate_limit_backoff(fn, "test operation", site_id="site-1")

        assert result == "ok"
        assert fn.call_count == 2
        mock_sleep.assert_called_once_with(7.0)
