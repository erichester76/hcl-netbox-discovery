"""Tests for the Cisco Nexus Dashboard source adapter (collector/sources/nexus.py).

All HTTP calls are mocked — no real Nexus Dashboard / NDFC is required.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.nexus import (
    NexusDashboardSource,
    _derive_interface_name_details,
    _flatten_interface_payload,
    _infer_iface_type_from_speed,
    _is_interface_enabled,
    _normalize_iface_type,
    _normalize_model,
    _normalize_port_channel_name,
    _normalize_vpc_name,
    _parse_speed_mbps,
    _safe_get,
)

# ---------------------------------------------------------------------------
# _normalize_model()
# ---------------------------------------------------------------------------


class TestNormalizeModel:
    @pytest.mark.parametrize(
        "model, expected",
        [
            ("N9K-C93180YC-EX",  "Nexus 93180YC-EX"),
            ("N9K-C9336C-FX2",   "Nexus 9336C-FX2"),
            ("N9K-SUP-A",        "Nexus 9000 SUP-A"),
            ("N7K-C7010",        "Nexus 7000 C7010"),
            ("N77-C7706",        "Nexus 7700 C7706"),
            ("N5K-C5596UP",      "Nexus 5000 C5596UP"),
            ("N56-C5672UP",      "Nexus 5600 C5672UP"),
            ("N3K-C3172PQ",      "Nexus 3172PQ"),
            ("N3K-C3048TP-1GE",  "Nexus 3048TP-1GE"),
            ("N2K-C2248TP-E-1GE","Nexus 2000 C2248TP-E-1GE"),
            ("",                 "Unknown"),
            ("UNKNOWN-DEVICE",   "UNKNOWN-DEVICE"),
        ],
    )
    def test_normalize_model(self, model, expected):
        assert _normalize_model(model) == expected


# ---------------------------------------------------------------------------
# _normalize_iface_type()
# ---------------------------------------------------------------------------


class TestNormalizeIfaceType:
    @pytest.mark.parametrize(
        "raw_type, if_name, speed_mbps, expected",
        [
            ("INTERFACE_ETHERNET",     "",                None,    "1000base-t"),
            ("INTERFACE_ETHERNET",     "Ethernet1/1",     100000,  "100gbase-x-qsfp28"),
            ("INTERFACE_ETHERNET",     "Ethernet1/1",     40000,   "40gbase-x-qsfpp"),
            ("INTERFACE_ETHERNET",     "Ethernet1/1",     25000,   "25gbase-x-sfp28"),
            ("INTERFACE_MANAGEMENT",   "",                None,    "1000base-t"),
            ("INTERFACE_PORT_CHANNEL", "",                None,    "lag"),
            ("INTERFACE_LOOPBACK",     "",                None,    "virtual"),
            ("INTERFACE_VLAN",         "",                None,    "virtual"),
            ("INTERFACE_NVE",          "",                None,    "virtual"),
            ("eth",                    "",                None,    "1000base-t"),
            ("port-channel",           "",                None,    "lag"),
            ("loopback",               "",                None,    "virtual"),
            ("UNKNOWN_TYPE",           "Ethernet1/1",     None,    "1000base-t"),
            ("",                       "port-channel10",  None,    "lag"),
            ("",                       "loopback0",       None,    "virtual"),
            ("",                       "vpc101",          None,    "virtual"),
            ("",                       "",                None,    "other"),
        ],
    )
    def test_normalize_iface_type(self, raw_type, if_name, speed_mbps, expected):
        assert _normalize_iface_type(raw_type, if_name, speed_mbps) == expected

    @pytest.mark.parametrize(
        ("speed_mbps", "expected"),
        [
            (100000, "100gbase-x-qsfp28"),
            (40000, "40gbase-x-qsfpp"),
            (25000, "25gbase-x-sfp28"),
            (10000, "10gbase-x-sfpp"),
            (1000, "1000base-t"),
            (100, "100base-tx"),
            (None, "other"),
        ],
    )
    def test_infer_iface_type_from_speed(self, speed_mbps, expected):
        assert _infer_iface_type_from_speed(speed_mbps) == expected


class TestNormalizeEnabled:
    @pytest.mark.parametrize(
        ("admin_state", "oper_status", "expected"),
        [
            ("up", "", True),
            ("UP", "", True),
            ("admin-up", "", True),
            ("ADMIN_STATE_UP", "", True),
            ("enabled", "", True),
            ("", "up", True),
            ("down", "", False),
            ("disabled", "", False),
            ("ADMIN_STATE_DOWN", "", False),
            ("", "", False),
        ],
    )
    def test_is_interface_enabled(self, admin_state, oper_status, expected):
        assert _is_interface_enabled(admin_state, oper_status) is expected


class TestDeriveInterfaceNameDetails:
    def test_returns_first_non_empty_candidate_and_source(self):
        name, source, candidates = _derive_interface_name_details(
            {
                "ifName": "",
                "name": "",
                "interfaceName": "Ethernet1/10",
                "portName": "Ethernet1/11",
                "displayName": "Display Ethernet1/12",
                "shortName": "Et1/13",
            }
        )

        assert name == "Ethernet1/10"
        assert source == "interfaceName"
        assert candidates["ifName"] == ""
        assert candidates["interfaceName"] == "Ethernet1/10"
        assert candidates["portName"] == "Ethernet1/11"


class TestFlattenInterfacePayload:
    def test_flattens_wrapped_interface_dicts(self):
        payload = [
            {
                "interfaces": {
                    "ifName": "Ethernet1/1",
                    "ifType": "INTERFACE_ETHERNET",
                },
                "policy": "default",
            }
        ]

        assert _flatten_interface_payload(payload) == [
            {"ifName": "Ethernet1/1", "ifType": "INTERFACE_ETHERNET"}
        ]

    def test_flattens_wrapped_interface_lists(self):
        payload = [
            {
                "interfaces": [
                    {"ifName": "Ethernet1/1"},
                    {"ifName": "Ethernet1/2"},
                ],
                "policy": "default",
            }
        ]

        assert _flatten_interface_payload(payload) == [
            {"ifName": "Ethernet1/1"},
            {"ifName": "Ethernet1/2"},
        ]


class TestInterfaceRelationshipNames:
    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            ("15", "port-channel15"),
            ("Po15", "port-channel15"),
            ("Port-Channel 15", "port-channel15"),
            ("sys/intf/aggr-[po15]", "port-channel15"),
            ("vpc101", ""),
            ("", ""),
        ],
    )
    def test_normalize_port_channel_name(self, raw_value, expected):
        assert _normalize_port_channel_name(raw_value) == expected

    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            ("1", "vpc1"),
            ("vpc1", "vpc1"),
            ("vPC 100", "vpc100"),
            ("port-channel15", ""),
            ("", ""),
        ],
    )
    def test_normalize_vpc_name(self, raw_value, expected):
        assert _normalize_vpc_name(raw_value) == expected


# ---------------------------------------------------------------------------
# _parse_speed_mbps()
# ---------------------------------------------------------------------------


class TestParseSpeedMbps:
    @pytest.mark.parametrize(
        "speed_str, expected",
        [
            ("10G",        10000),
            ("10GBPS",     10000),
            ("10Gbps",     10000),
            ("1G",         1000),
            ("100G",       100000),
            ("1000 Mbps",  1000),
            ("1000M",      1000),
            ("1000",       1000),
            ("25000000000", 25000),
            ("100",        100),
            ("",           None),
            ("unknown",    None),
        ],
    )
    def test_parse_speed_mbps(self, speed_str, expected):
        assert _parse_speed_mbps(speed_str) == expected


# ---------------------------------------------------------------------------
# _safe_get()
# ---------------------------------------------------------------------------


class TestSafeGet:
    def test_dict_access(self):
        assert _safe_get({"key": "val"}, "key") == "val"

    def test_dict_missing_key_returns_default(self):
        assert _safe_get({}, "key", "default") == "default"

    def test_object_access(self):
        obj = SimpleNamespace(name="nx-leaf-01")
        assert _safe_get(obj, "name") == "nx-leaf-01"

    def test_object_missing_attr_returns_default(self):
        obj = SimpleNamespace()
        assert _safe_get(obj, "missing", 42) == 42

    def test_none_default(self):
        assert _safe_get({}, "key") is None


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestNexusConnect:
    def _make_mock_session(self, token: str = "test-jwt-token") -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"token": token}
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.post.return_value = mock_resp
        session.headers = {}
        return session

    def test_connect_sets_auth_token(self, nexus_config):
        src = NexusDashboardSource()
        with patch("collector.sources.nexus.requests.Session") as MockSession:
            session = self._make_mock_session()
            MockSession.return_value = session
            src.connect(nexus_config)
        assert src._session is session
        assert "X-Auth-Token" in session.headers

    def test_connect_prepends_https_if_missing(self, nexus_config):
        nexus_config.url = "ndfc.example.com"
        src = NexusDashboardSource()
        with patch("collector.sources.nexus.requests.Session") as MockSession:
            session = self._make_mock_session()
            MockSession.return_value = session
            src.connect(nexus_config)
        assert src._base_url.startswith("https://")

    def test_connect_raises_if_credentials_missing(self, nexus_config):
        nexus_config.username = ""
        nexus_config.password = ""
        src = NexusDashboardSource()
        with pytest.raises(RuntimeError, match="NDFC_USER"):
            src.connect(nexus_config)

    def test_fetch_interfaces_flag_true(self, nexus_config):
        nexus_config.extra = {"fetch_interfaces": "true"}
        src = NexusDashboardSource()
        with patch("collector.sources.nexus.requests.Session") as MockSession:
            session = self._make_mock_session()
            MockSession.return_value = session
            src.connect(nexus_config)
        assert src._fetch_interfaces is True

    def test_fetch_interfaces_flag_false(self, nexus_config):
        nexus_config.extra = {"fetch_interfaces": "false"}
        src = NexusDashboardSource()
        with patch("collector.sources.nexus.requests.Session") as MockSession:
            session = self._make_mock_session()
            MockSession.return_value = session
            src.connect(nexus_config)
        assert src._fetch_interfaces is False

    def test_fetch_modules_flag_true(self, nexus_config):
        nexus_config.extra = {"fetch_modules": "true"}
        src = NexusDashboardSource()
        with patch("collector.sources.nexus.requests.Session") as MockSession:
            session = self._make_mock_session()
            MockSession.return_value = session
            src.connect(nexus_config)
        assert src._fetch_modules is True

    def test_connect_raises_if_all_auth_endpoints_fail(self, nexus_config):
        src = NexusDashboardSource()
        with patch("collector.sources.nexus.requests.Session") as MockSession:
            session = MagicMock()
            session.headers = {}
            session.post.side_effect = Exception("connection refused")
            MockSession.return_value = session
            with pytest.raises(RuntimeError, match="Failed to authenticate"):
                src.connect(nexus_config)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestNexusGetObjects:
    def _connected_source(self) -> NexusDashboardSource:
        src = NexusDashboardSource()
        src._session = MagicMock()
        return src

    def test_raises_without_connect(self):
        src = NexusDashboardSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("switches")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("routers")

    def test_get_switches_returns_enriched_dicts(self):
        src = self._connected_source()

        raw_switch = {
            "hostName":    "nx-leaf-01.example.com",
            "model":       "N9K-C93180YC-EX",
            "serialNumber": "SAL1234567X",
            "release":     "9.3(7)",
            "fabricName":  "MyFabric",
            "switchRole":  "leaf",
            "ipAddress":   "10.0.0.1",
            "status":      "alive",
            "systemMode":  "Normal",
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = [raw_switch]
        mock_resp.raise_for_status = MagicMock()
        src._session.get.return_value = mock_resp

        result = src.get_objects("switches")

        assert len(result) == 1
        d = result[0]
        assert d["name"] == "nx-leaf-01"
        assert d["model"] == "Nexus 93180YC-EX"
        assert d["manufacturer"] == "Cisco"
        assert d["serial"] == "SAL1234567X"
        assert d["status"] == "active"
        assert d["fabric_name"] == "MyFabric"
        assert d["site_name"] == "MyFabric"
        assert d["role"] == "Leaf"
        assert d["platform_name"] == "NX-OS 9.3(7)"

    def test_get_switches_offline_when_unreachable(self):
        src = self._connected_source()

        raw_switch = {
            "hostName":    "nx-spine-01",
            "model":       "N9K-C9336C-FX2",
            "serialNumber": "FGE12345678",
            "release":     "9.3(8)",
            "fabricName":  "SpineFabric",
            "switchRole":  "spine",
            "ipAddress":   "10.0.0.2",
            "status":      "unreachable",
            "systemMode":  "Normal",
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = [raw_switch]
        mock_resp.raise_for_status = MagicMock()
        src._session.get.return_value = mock_resp

        result = src.get_objects("switches")
        assert result[0]["status"] == "offline"

    def test_get_switches_handles_dict_wrapped_response(self):
        src = self._connected_source()

        raw_switch = {
            "hostName":    "nx-leaf-02",
            "model":       "N3K-C3172PQ",
            "serialNumber": "ABC1234567",
            "release":     "7.0(3)I7(9)",
            "fabricName":  "LabFabric",
            "switchRole":  "leaf",
            "ipAddress":   "10.0.0.3",
            "status":      "alive",
            "systemMode":  "Normal",
        }

        # Wrapped in {"switches": [...]}
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"switches": [raw_switch]}
        mock_resp.raise_for_status = MagicMock()
        src._session.get.return_value = mock_resp

        result = src.get_objects("switches")
        assert len(result) == 1
        assert result[0]["name"] == "nx-leaf-02"

    def test_get_switches_with_interfaces_fetched(self):
        src = self._connected_source()
        src._fetch_interfaces = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        iface_resp = MagicMock()
        iface_resp.raise_for_status = MagicMock()
        iface_resp.json.return_value = [
            {
                "ifName":     "Ethernet1/1",
                "ifType":     "INTERFACE_ETHERNET",
                "adminState": "up",
                "operStatus": "up",
                "ifDescr":    "uplink",
                "macAddress": "00:1b:44:11:3a:b7",
                "ipAddress":  "",
                "speedStr":   "Auto",
            }
        ]

        analyze_iface_resp = MagicMock()
        analyze_iface_resp.raise_for_status = MagicMock()
        analyze_iface_resp.json.return_value = {
            "interfaces": [
                {
                    "interfaceName": "Ethernet1/1",
                    "interfaceType": "ethernet",
                    "physicalInterface": True,
                    "speed": "100Gb",
                    "ip": "10.127.117.32/24",
                    "channelId": 500,
                    "adminStatus": "up",
                    "operationalStatus": "up",
                    "description": "Connected to host-20",
                }
            ]
        }

        detail_resp = MagicMock()
        detail_resp.raise_for_status = MagicMock()
        detail_resp.json.return_value = [
            {
                "interfaceName": "Ethernet1/1",
                "interfaceType": "ethernet",
                "operData": {
                    "adminStatus": "up",
                    "operationalStatus": "up",
                    "operDescription": "XCVR present",
                    "speed": "100Gb",
                },
                "channelId": 500,
            }
        ]

        def side_effect(url, params=None, **kwargs):
            if url.endswith("/inventory/allswitches"):
                return switch_resp
            if url.endswith("/lan-fabric/rest/interface"):
                assert params == {"serialNumber": "SAL9876543"}
                return iface_resp
            if url.endswith("/api/v1/analyze/interfaces"):
                assert params == {"fabricName": "ProdFabric", "switchId": "SAL9876543"}
                return analyze_iface_resp
            if url.endswith("/lan-fabric/rest/interface/detail/filter"):
                assert params == {"serialNumber": "SAL9876543"}
                return detail_resp
            raise AssertionError(f"unexpected URL {url!r}")

        src._session.get.side_effect = side_effect

        result = src.get_objects("switches")
        assert len(result) == 1
        assert "interfaces" in result[0]
        iface = result[0]["interfaces"][0]
        assert iface["name"] == "Ethernet1/1"
        assert iface["type"] == "100gbase-x-qsfp28"
        assert iface["enabled"] is True
        assert iface["mac_address"] == "00:1B:44:11:3A:B7"
        assert iface["speed"] == 100000
        assert iface["ip_address"] == "10.127.117.32/24"
        assert iface["lag_name"] == "port-channel500"

    def test_get_switches_with_modules_fetched(self):
        src = self._connected_source()
        src._fetch_modules = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "switchDbID": 22530,
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        module_resp = MagicMock()
        module_resp.raise_for_status = MagicMock()
        module_resp.json.return_value = {
            "modules": [
                {
                    "name": "PowerSupply-1",
                    "modelName": "NXA-PAC-500W-PI",
                    "serialNumber": "LIT22505EAA",
                    "type": "chassis",
                    "operStatus": "offEnvPower",
                    "slot": "1",
                    "moduleType": ["MI FPGA", "IO FPGA"],
                    "moduleVersion": ["0x10", "0x17"],
                    "hardwareRevision": "V03",
                    "softwareRevision": "10.4(2)",
                    "assetId": "73-18235-04",
                },
                {
                    "name": "Fan-1",
                    "modelName": "N9K-C9504-FAN",
                    "serialNumber": "FAN123",
                    "type": "chassis",
                    "operStatus": "ok",
                    "slot": "2",
                },
            ]
        }

        interface_resp = MagicMock()
        interface_resp.raise_for_status = MagicMock()
        interface_resp.json.return_value = []

        def side_effect(url, **kwargs):
            if url.endswith("/inventory/allswitches"):
                return switch_resp
            if url.endswith("/dashboard/switch/interface?switchId=22530"):
                return interface_resp
            if url.endswith("/dashboard/switch/module?switchId=22530"):
                return module_resp
            raise AssertionError(f"unexpected URL {url!r}")

        src._session.get.side_effect = side_effect

        result = src.get_objects("switches")

        modules = result[0]["modules"]
        assert len(modules) == 2
        assert modules[0]["profile"] == "Power supply"
        assert modules[0]["bay_name"] == "PowerSupply-1"
        assert modules[0]["position"] == "1"
        assert modules[0]["model"] == "NXA-PAC-500W-PI"
        assert modules[0]["serial"] == "LIT22505EAA"
        assert modules[1]["profile"] == "Fan"

    def test_get_switches_without_modules_key_absent(self):
        src = self._connected_source()
        src._fetch_modules = False

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        src._session.get.return_value = switch_resp

        result = src.get_objects("switches")

        assert "modules" not in result[0]

    def test_get_switches_with_wrapped_interfaces_fetched(self):
        src = self._connected_source()
        src._fetch_interfaces = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        iface_resp = MagicMock()
        iface_resp.raise_for_status = MagicMock()
        iface_resp.json.return_value = [
            {
                "interfaces": {
                    "ifName": "mgmt0",
                    "ifType": "INTERFACE_MANAGEMENT",
                    "adminState": "up",
                    "operStatus": "up",
                    "ifDescr": "management",
                    "ipAddress": "10.0.0.4/24",
                },
                "policy": "default",
            }
        ]

        analyze_iface_resp = MagicMock()
        analyze_iface_resp.raise_for_status = MagicMock()
        analyze_iface_resp.json.return_value = {"interfaces": []}

        detail_resp = MagicMock()
        detail_resp.raise_for_status = MagicMock()
        detail_resp.json.return_value = []

        def side_effect(url, params=None, **kwargs):
            if url.endswith("/inventory/allswitches"):
                return switch_resp
            if url.endswith("/lan-fabric/rest/interface"):
                return iface_resp
            if url.endswith("/api/v1/analyze/interfaces"):
                return analyze_iface_resp
            if url.endswith("/lan-fabric/rest/interface/detail/filter"):
                return detail_resp
            raise AssertionError(f"unexpected URL {url!r}")

        src._session.get.side_effect = side_effect

        result = src.get_objects("switches")

        assert result[0]["interfaces"][0]["name"] == "mgmt0"
        assert result[0]["interfaces"][0]["mgmt_only"] is True

    def test_get_switches_includes_interfaces_found_only_in_analyze_or_detail(self):
        src = self._connected_source()
        src._fetch_interfaces = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        iface_resp = MagicMock()
        iface_resp.raise_for_status = MagicMock()
        iface_resp.json.return_value = []

        analyze_iface_resp = MagicMock()
        analyze_iface_resp.raise_for_status = MagicMock()
        analyze_iface_resp.json.return_value = {
            "interfaces": [
                {
                    "interfaceName": "Ethernet1/10",
                    "interfaceType": "ethernet",
                    "physicalInterface": True,
                    "speed": "25Gb",
                    "ip": "10.10.10.1/31",
                    "channelId": 7,
                    "adminStatus": "up",
                    "operationalStatus": "up",
                    "description": "analyze-only",
                }
            ]
        }

        detail_resp = MagicMock()
        detail_resp.raise_for_status = MagicMock()
        detail_resp.json.return_value = [
            {
                "interfaceName": "loopback100",
                "interfaceType": "loopback",
                "operData": {
                    "adminStatus": "up",
                    "operationalStatus": "up",
                    "operDescription": "detail-only",
                    "speed": "",
                },
            }
        ]

        def side_effect(url, params=None, **kwargs):
            if url.endswith("/inventory/allswitches"):
                return switch_resp
            if url.endswith("/lan-fabric/rest/interface"):
                return iface_resp
            if url.endswith("/api/v1/analyze/interfaces"):
                return analyze_iface_resp
            if url.endswith("/lan-fabric/rest/interface/detail/filter"):
                return detail_resp
            raise AssertionError(f"unexpected URL {url!r}")

        src._session.get.side_effect = side_effect

        result = src.get_objects("switches")

        interfaces = {iface["name"]: iface for iface in result[0]["interfaces"]}
        assert interfaces["Ethernet1/10"]["type"] == "25gbase-x-sfp28"
        assert interfaces["Ethernet1/10"]["speed"] == 25000
        assert interfaces["Ethernet1/10"]["ip_address"] == "10.10.10.1/31"
        assert interfaces["Ethernet1/10"]["lag_name"] == "port-channel7"
        assert interfaces["loopback100"]["type"] == "virtual"
        assert interfaces["loopback100"]["description"] == "detail-only"

    def test_get_switches_interface_fetch_error_returns_empty(self):
        src = self._connected_source()
        src._fetch_interfaces = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-04",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL0001111",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.5",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        def side_effect(url, params=None, **kwargs):
            if url.endswith("/inventory/allswitches"):
                return switch_resp
            if url.endswith("/lan-fabric/rest/interface"):
                raise Exception("timeout")
            return MagicMock(json=MagicMock(return_value={"interfaces": []}), raise_for_status=MagicMock())

        src._session.get.side_effect = side_effect

        result = src.get_objects("switches")
        assert result[0]["interfaces"] == []

    def test_get_switches_without_interfaces_key_absent(self):
        """When fetch_interfaces is False, no 'interfaces' key should be added."""
        src = self._connected_source()
        src._fetch_interfaces = False

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = [
            {
                "hostName": "nx-leaf-05",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL0002222",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.6",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]
        src._session.get.return_value = mock_resp

        result = src.get_objects("switches")
        assert "interfaces" not in result[0]

    def test_get_switches_logs_empty_module_shape_at_debug(self, caplog):
        src = self._connected_source()
        src._fetch_interfaces = False

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = [
            {
                "hostName": "nx-leaf-05",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL0002222",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.6",
                "status": "alive",
                "systemMode": "Normal",
                "modules": [],
            }
        ]
        src._session.get.return_value = mock_resp

        with caplog.at_level(logging.DEBUG, logger="collector.sources.nexus"):
            src.get_objects("switches")

        assert "NDFC switch modules list empty" in caplog.text

    def test_get_switches_suppresses_duplicate_interface_ips_across_switches(self):
        src = self._connected_source()
        src._fetch_interfaces = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            },
            {
                "hostName": "nx-leaf-04",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876544",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.5",
                "status": "alive",
                "systemMode": "Normal",
            },
        ]

        iface_resp_one = MagicMock()
        iface_resp_one.raise_for_status = MagicMock()
        iface_resp_one.json.return_value = [
            {
                "ifName": "Vlan3600",
                "ifType": "INTERFACE_VLAN",
                "adminState": "up",
                "ipAddress": "10.100.7.1/32",
            }
        ]

        iface_resp_two = MagicMock()
        iface_resp_two.raise_for_status = MagicMock()
        iface_resp_two.json.return_value = [
            {
                "ifName": "Vlan3600",
                "ifType": "INTERFACE_VLAN",
                "adminState": "up",
                "ipAddress": "10.100.7.1/32",
            }
        ]

        empty_analyze = MagicMock()
        empty_analyze.raise_for_status = MagicMock()
        empty_analyze.json.return_value = {"interfaces": []}

        empty_detail = MagicMock()
        empty_detail.raise_for_status = MagicMock()
        empty_detail.json.return_value = []

        def side_effect(url, params=None, **kwargs):
            if url.endswith("/inventory/allswitches"):
                return switch_resp
            if url.endswith("/lan-fabric/rest/interface") and params == {"serialNumber": "SAL9876543"}:
                return iface_resp_one
            if url.endswith("/lan-fabric/rest/interface") and params == {"serialNumber": "SAL9876544"}:
                return iface_resp_two
            if url.endswith("/api/v1/analyze/interfaces"):
                return empty_analyze
            if url.endswith("/lan-fabric/rest/interface/detail/filter"):
                return empty_detail
            raise AssertionError(f"unexpected URL {url!r}")

        src._session.get.side_effect = side_effect

        result = src.get_objects("switches")

        assert result[0]["interfaces"][0]["ip_address"] == ""
        assert result[1]["interfaces"][0]["ip_address"] == ""
        assert result[0]["interfaces"][0]["duplicate_ip_address"] == "10.100.7.1/32"
        assert result[1]["interfaces"][0]["duplicate_ip_address"] == "10.100.7.1/32"


# ---------------------------------------------------------------------------
# _enrich_switch() edge cases
# ---------------------------------------------------------------------------


class TestNexusEnrichSwitch:
    def test_hostname_stripped_of_domain_and_truncated(self):
        src = NexusDashboardSource()
        long_hostname = "a" * 100 + ".example.com"
        sw = {
            "hostName": long_hostname,
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "9.3(7)",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert len(result["name"]) <= 64

    def test_empty_hostname_becomes_unknown(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "",
            "model": "",
            "serialNumber": "",
            "release": "",
            "fabricName": "",
            "switchRole": "",
            "ipAddress": "",
            "status": "",
            "systemMode": "",
        }
        result = src._enrich_switch(sw)
        assert result["name"] == "Unknown"

    def test_name_falls_back_to_switch_name(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "",
            "switchName": "nx-border-01.example.com",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "9.3(7)",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["name"] == "nx-border-01"

    def test_site_name_falls_back_to_site_name(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "9.3(7)",
            "fabricName": "",
            "siteName": "Clemson-DC",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["site_name"] == "Clemson-DC"

    def test_site_name_falls_back_to_hierarchy_tail(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "9.3(7)",
            "fabricName": "",
            "siteNameHierarchy": "Global/Campus/Fabric-A",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["site_name"] == "Fabric-A"

    def test_raw_ip_address_remains_passthrough_when_management_ip_falls_back(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "9.3(7)",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "mgmtAddress": "10.0.0.10",
            "status": "alive",
            "systemMode": "Normal",
        }

        result = src._enrich_switch(sw)

        assert result["ip_address"] == "10.0.0.10"
        assert result["ipAddress"] == "10.0.0.1"

    def test_role_formatted_as_title_case(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-spine-01",
            "model": "N9K-C9336C-FX2",
            "serialNumber": "SAL999",
            "release": "9.3(7)",
            "fabricName": "SpineFabric",
            "switchRole": "border_gateway",
            "ipAddress": "10.0.0.9",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["role"] == "Border Gateway"

    def test_serial_uppercased(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "sal1234567",
            "release": "9.3(7)",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["serial"] == "SAL1234567"

    def test_platform_name_includes_release(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "9.3(7)",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["platform_name"] == "NX-OS 9.3(7)"

    def test_empty_release_gives_nxos_platform(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "release": "",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }
        result = src._enrich_switch(sw)
        assert result["platform_name"] == "NX-OS"

    def test_switch_db_id_passthrough_preserved(self):
        src = NexusDashboardSource()
        sw = {
            "hostName": "nx-leaf-01",
            "model": "N9K-C93180YC-EX",
            "serialNumber": "SAL123",
            "switchDbID": 22530,
            "release": "9.3(7)",
            "fabricName": "Fabric1",
            "switchRole": "leaf",
            "ipAddress": "10.0.0.1",
            "status": "alive",
            "systemMode": "Normal",
        }

        result = src._enrich_switch(sw)

        assert result["switch_db_id"] == 22530
        assert result["switchDbID"] == 22530


class TestNexusEnrichModule:
    def test_power_supply_module_is_normalized(self):
        src = NexusDashboardSource()
        module = {
            "name": "PowerSupply-1",
            "modelName": "NXA-PAC-500W-PI",
            "serialNumber": "LIT22505EAA",
            "type": "chassis",
            "operStatus": "offEnvPower",
            "slot": "1",
            "moduleType": ["MI FPGA", "IO FPGA"],
            "moduleVersion": ["0x10", "0x17"],
            "hardwareRevision": "V03",
            "softwareRevision": "10.4(2)",
            "assetId": "73-18235-04",
        }

        result = src._enrich_module(module)

        assert result is not None
        assert result["profile"] == "Power supply"
        assert result["bay_name"] == "PowerSupply-1"
        assert result["position"] == "1"
        assert result["model"] == "NXA-PAC-500W-PI"
        assert result["serial"] == "LIT22505EAA"
        assert result["manufacturer"] == "Cisco"
        assert result["module_type"] == "MI FPGA, IO FPGA"
        assert result["module_version"] == "0x10, 0x17"

    def test_fan_module_is_normalized(self):
        src = NexusDashboardSource()
        module = {
            "name": "Fan-1",
            "modelName": "N9K-C9504-FAN",
            "serialNumber": "FAN123",
            "type": "chassis",
            "operStatus": "ok",
            "slot": "2",
        }

        result = src._enrich_module(module)

        assert result is not None
        assert result["profile"] == "Fan"
        assert result["bay_name"] == "Fan-1"

    def test_transceiver_module_is_normalized(self):
        src = NexusDashboardSource()
        module = {
            "name": "QSFP-1",
            "modelName": "QSFP-100G-SR4-S",
            "serialNumber": "QSFP123",
            "type": "transceiver",
            "operStatus": "ok",
            "slot": "Ethernet1/49",
        }

        result = src._enrich_module(module)

        assert result is not None
        assert result["profile"] == "Transceiver"
        assert result["position"] == "Ethernet1/49"

    def test_unsupported_module_is_skipped(self):
        src = NexusDashboardSource()

        result = src._enrich_module(
            {
                "name": "Supervisor",
                "modelName": "SUP-A",
                "serialNumber": "SUP123",
                "type": "chassis",
                "slot": "3",
            }
        )

        assert result is None


# ---------------------------------------------------------------------------
# _enrich_interface()
# ---------------------------------------------------------------------------


class TestNexusEnrichInterface:
    def test_basic_ethernet_interface(self):
        src = NexusDashboardSource()
        iface = {
            "ifName":     "Ethernet1/1",
            "ifType":     "INTERFACE_ETHERNET",
            "adminState": "up",
            "operStatus": "up",
            "ifDescr":    "to-spine-01",
            "macAddress": "aa:bb:cc:dd:ee:ff",
            "ipAddress":  "",
            "speedStr":   "25G",
        }
        result = src._enrich_interface(iface)
        assert result["name"] == "Ethernet1/1"
        assert result["type"] == "25gbase-x-sfp28"
        assert result["enabled"] is True
        assert result["description"] == "to-spine-01"
        assert result["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert result["speed"] == 25000

    def test_port_channel_interface(self):
        src = NexusDashboardSource()
        iface = {
            "ifName":     "port-channel100",
            "ifType":     "INTERFACE_PORT_CHANNEL",
            "adminState": "up",
            "operStatus": "up",
            "ifDescr":    "",
            "macAddress": "",
            "ipAddress":  "",
            "speedStr":   "",
        }
        result = src._enrich_interface(iface)
        assert result["type"] == "lag"

    def test_loopback_interface(self):
        src = NexusDashboardSource()
        iface = {
            "ifName":     "loopback0",
            "ifType":     "INTERFACE_LOOPBACK",
            "adminState": "up",
            "operStatus": "up",
            "ifDescr":    "router-id",
            "macAddress": "",
            "ipAddress":  "10.255.0.1/32",
            "speedStr":   "",
        }
        result = src._enrich_interface(iface)
        assert result["type"] == "virtual"
        assert result["ip_address"] == "10.255.0.1/32"
        assert result["mgmt_only"] is False

    def test_management_interface_sets_mgmt_only(self):
        src = NexusDashboardSource()
        iface = {
            "ifName":     "mgmt0",
            "ifType":     "INTERFACE_MANAGEMENT",
            "adminState": "up",
            "operStatus": "up",
            "ifDescr":    "oob management",
            "macAddress": "aa:bb:cc:dd:ee:ff",
            "ipAddress":  "10.0.0.10/24",
            "speedStr":   "1G",
        }
        result = src._enrich_interface(iface)
        assert result["type"] == "1000base-t"
        assert result["mgmt_only"] is True

    def test_management_interface_can_use_switch_ip_when_interface_ip_missing(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "mgmt0",
            "nvPairs": {
                "ifType": "INTERFACE_MANAGEMENT",
                "adminState": "up",
            },
        }

        result = src._enrich_interface(iface, switch_ip_address="10.19.237.183")

        assert result["mgmt_only"] is True
        assert result["ip_address"] == "10.19.237.183/32"

    def test_routed_interface_bare_ip_uses_prefix_from_nv_pairs(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/33",
            "nvPairs": {
                "ifType": "INTERFACE_ETHERNET",
                "adminState": "up",
                "ip": "10.100.7.1",
                "prefix": "32",
            },
        }

        result = src._enrich_interface(iface)

        assert result["ip_address"] == "10.100.7.1/32"

    def test_routed_interface_bare_ip_defaults_to_host_prefix(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "loopback254",
            "nvPairs": {
                "ifType": "INTERFACE_LOOPBACK",
                "adminState": "up",
                "ip": "10.100.8.22",
            },
        }

        result = src._enrich_interface(iface)

        assert result["ip_address"] == "10.100.8.22/32"

    def test_interface_type_and_enabled_can_fall_back_from_name_and_admin_variants(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "port-channel500",
            "nvPairs": {
                "adminStatus": "ADMIN_STATE_UP",
            },
        }

        result = src._enrich_interface(iface)

        assert result["type"] == "lag"
        assert result["enabled"] is True

    def test_member_interface_derives_lag_and_vpc_names_from_nv_pairs(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/11",
            "nvPairs": {
                "ifType": "INTERFACE_ETHERNET",
                "adminState": "up",
                "channelGroup": "Port-Channel 15",
                "vpcId": "101",
            },
        }

        result = src._enrich_interface(iface)

        assert result["lag_name"] == "port-channel15"
        assert result["vpc_name"] == "vpc101"

    def test_member_interface_derives_lag_name_from_poid(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/35",
            "nvPairs": {
                "ifType": "INTERFACE_ETHERNET",
                "poid": "500",
            },
        }

        result = src._enrich_interface(iface)

        assert result["lag_name"] == "port-channel500"

    def test_member_interface_derives_lag_name_from_primaryintf(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/37",
            "nvPairs": {
                "ifType": "INTERFACE_ETHERNET",
                "primaryIntf": "port-channel500",
            },
        }

        result = src._enrich_interface(iface)

        assert result["lag_name"] == "port-channel500"

    def test_port_channel_interface_does_not_self_assign_lag_name(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "port-channel500",
            "nvPairs": {
                "ifType": "INTERFACE_PORT_CHANNEL",
                "poid": "500",
                "primaryIntf": "port-channel500",
            },
        }

        result = src._enrich_interface(iface)

        assert result["type"] == "lag"
        assert result["lag_name"] == ""

    def test_interface_speed_can_use_extended_nvpair_keys(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/1",
            "ifType": "INTERFACE_ETHERNET",
            "nvPairs": {
                "adminStatus": "ADMIN_STATE_UP",
                "ifSpeed": "100000000000",
            },
        }

        result = src._enrich_interface(iface)

        assert result["speed"] == 100000
        assert result["type"] == "100gbase-x-qsfp28"
        assert result["enabled"] is True

    def test_interface_speed_can_use_admin_and_oper_speed_variants(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/2",
            "ifType": "INTERFACE_ETHERNET",
            "nvPairs": {
                "adminState": "up",
                "adminSpeed": "40000000000",
                "operSpeedStr": "40000000000",
            },
        }

        result = src._enrich_interface(iface)

        assert result["speed"] == 40000
        assert result["type"] == "40gbase-x-qsfpp"
        assert result["enabled"] is True

    def test_interface_speed_prefers_numeric_admin_speed_over_auto_placeholder(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/3",
            "ifType": "INTERFACE_ETHERNET",
            "nvPairs": {
                "adminState": "up",
                "speed": "Auto",
                "adminSpeed": "100000000000",
            },
        }

        result = src._enrich_interface(iface)

        assert result["speed"] == 100000
        assert result["type"] == "100gbase-x-qsfp28"
        assert result["enabled"] is True

    def test_interface_speed_can_use_analyze_and_detail_records_when_legacy_speed_is_auto(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/3",
            "ifType": "INTERFACE_ETHERNET",
            "nvPairs": {
                "adminState": "up",
                "speed": "Auto",
            },
        }
        analyze_iface = {
            "interfaceName": "Ethernet1/3",
            "speed": "100Gb",
            "channelId": 500,
        }
        detail_iface = {
            "interfaceName": "Ethernet1/3",
            "operData": {
                "adminStatus": "up",
                "speed": "100Gb",
            },
        }

        result = src._enrich_interface(iface, analyze_iface=analyze_iface, detail_iface=detail_iface)

        assert result["speed"] == 100000
        assert result["type"] == "100gbase-x-qsfp28"
        assert result["enabled"] is True
        assert result["lag_name"] == "port-channel500"

    def test_interface_detail_state_overrides_legacy_and_analyze_state(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/7",
            "ifType": "INTERFACE_ETHERNET",
            "adminState": "down",
            "operStatus": "down",
        }
        analyze_iface = {
            "interfaceName": "Ethernet1/7",
            "adminStatus": "down",
            "operationalStatus": "down",
        }
        detail_iface = {
            "interfaceName": "Ethernet1/7",
            "operData": {
                "adminStatus": "up",
                "operationalStatus": "up",
            },
        }

        result = src._enrich_interface(iface, analyze_iface=analyze_iface, detail_iface=detail_iface)

        assert result["enabled"] is True

    def test_detail_and_analyze_speed_override_auto_placeholders(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/8",
            "ifType": "INTERFACE_ETHERNET",
            "speed": "Auto",
            "adminSpeed": "Auto",
            "operSpeed": "Auto",
            "nvPairs": {
                "adminState": "up",
            },
        }
        analyze_iface = {
            "interfaceName": "Ethernet1/8",
            "adminSpeed": "100000000000",
        }
        detail_iface = {
            "interfaceName": "Ethernet1/8",
            "operData": {
                "speed": "100Gb",
            },
        }

        result = src._enrich_interface(iface, analyze_iface=analyze_iface, detail_iface=detail_iface)

        assert result["speed"] == 100000
        assert result["type"] == "100gbase-x-qsfp28"

    def test_interface_speed_can_fall_back_to_bandwidth_when_speed_is_auto(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "port-channel28",
            "nvPairs": {
                "ifType": "INTERFACE_PORT_CHANNEL",
                "adminState": "up",
                "speed": "Auto",
                "bandwidth": "40000000",
            },
        }

        result = src._enrich_interface(iface)

        assert result["speed"] == 40000
        assert result["type"] == "lag"

    def test_interface_bandwidth_is_not_parsed_as_generic_speed_string(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "port-channel29",
            "nvPairs": {
                "ifType": "INTERFACE_PORT_CHANNEL",
                "bandwidth": "40000000",
            },
        }

        result = src._enrich_interface(iface)

        assert result["speed"] == 40000
        assert result["type"] == "lag"

    def test_vpc_interface_does_not_emit_parent_lag_from_port_channel_id(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "vpc101",
            "nvPairs": {
                "ifType": "INTERFACE_ST",
                "portChannelId": "15",
                "vpcId": "101",
            },
        }

        result = src._enrich_interface(iface)

        assert result["lag_name"] == ""
        assert result["vpc_name"] == "vpc101"

    @pytest.mark.parametrize("pcid_key", ["peer1Pcid", "peer2Pcid"])
    def test_vpc_interface_does_not_emit_parent_lag_from_peer_pcid(self, pcid_key):
        src = NexusDashboardSource()
        iface = {
            "ifName": "vpc28",
            "nvPairs": {
                "ifType": "INTERFACE_ST",
                pcid_key: "28",
                "vpcId": "28",
            },
        }

        result = src._enrich_interface(iface)

        assert result["lag_name"] == ""
        assert result["vpc_name"] == "vpc28"

    def test_nvpair_speed_sets_speed_and_physical_type(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "Ethernet1/49",
            "nvPairs": {
                "ifType": "INTERFACE_ETHERNET",
                "adminState": "up",
                "speed": "100000000000",
            },
        }

        result = src._enrich_interface(iface)

        assert result["speed"] == 100000
        assert result["type"] == "100gbase-x-qsfp28"

    @pytest.mark.parametrize(
        ("iface", "expected_name"),
        [
            (
                {
                    "ifName":        "",
                    "name":          "Ethernet1/49",
                    "interfaceName": "ignored-because-name-already-set",
                    "portName":      "ignored-port-name",
                    "displayName":   "ignored-display-name",
                    "shortName":     "ignored-short-name",
                },
                "Ethernet1/49",
            ),
            (
                {
                    "ifName":        "",
                    "name":          "",
                    "interfaceName": "Ethernet1/50",
                    "portName":      "ignored-port-name",
                    "displayName":   "ignored-display-name",
                    "shortName":     "ignored-short-name",
                },
                "Ethernet1/50",
            ),
            (
                {
                    "ifName":        "",
                    "name":          "",
                    "interfaceName": "",
                    "portName":      "Ethernet1/51",
                    "displayName":   "ignored-display-name",
                    "shortName":     "ignored-short-name",
                },
                "Ethernet1/51",
            ),
            (
                {
                    "ifName":        "",
                    "name":          "",
                    "interfaceName": "",
                    "portName":      "",
                    "displayName":   "Ethernet1/52",
                    "shortName":     "ignored-short-name",
                },
                "Ethernet1/52",
            ),
            (
                {
                    "ifName":        "",
                    "name":          "",
                    "interfaceName": "",
                    "portName":      "",
                    "displayName":   "",
                    "shortName":     "Ethernet1/53",
                },
                "Ethernet1/53",
            ),
        ],
    )
    def test_interface_name_falls_back_when_ifname_missing(self, iface, expected_name):
        src = NexusDashboardSource()
        iface = {
            "ifType":        "INTERFACE_ETHERNET",
            "adminState":    "up",
            "operStatus":    "up",
            "ifDescr":       "uplink",
            "macAddress":    "",
            "ipAddress":     "",
            "speedStr":      "10G",
            **iface,
        }
        result = src._enrich_interface(iface)
        assert result["name"] == expected_name
        assert result["type"] == "10gbase-x-sfpp"
        assert result["enabled"] is True

    def test_admin_down_interface_not_enabled(self):
        src = NexusDashboardSource()
        iface = {
            "ifName":     "Ethernet1/48",
            "ifType":     "INTERFACE_ETHERNET",
            "adminState": "down",
            "operStatus": "down",
            "ifDescr":    "",
            "macAddress": "",
            "ipAddress":  "",
            "speedStr":   "1G",
        }
        result = src._enrich_interface(iface)
        assert result["enabled"] is False

    def test_get_switches_orders_lags_before_member_interfaces(self):
        src = NexusDashboardSource()
        src._session = MagicMock()
        src._fetch_interfaces = True

        switch_resp = MagicMock()
        switch_resp.raise_for_status = MagicMock()
        switch_resp.json.return_value = [
            {
                "hostName": "nx-leaf-03",
                "model": "N9K-C93180YC-EX",
                "serialNumber": "SAL9876543",
                "release": "9.3(7)",
                "fabricName": "ProdFabric",
                "switchRole": "leaf",
                "ipAddress": "10.0.0.4",
                "status": "alive",
                "systemMode": "Normal",
            }
        ]

        iface_resp = MagicMock()
        iface_resp.raise_for_status = MagicMock()
        iface_resp.json.return_value = [
            {
                "ifName": "Ethernet1/1",
                "nvPairs": {
                    "ifType": "INTERFACE_ETHERNET",
                    "adminState": "up",
                    "channelGroup": "15",
                },
            },
            {
                "ifName": "port-channel15",
                "nvPairs": {
                    "ifType": "INTERFACE_PORT_CHANNEL",
                    "adminState": "up",
                },
            },
        ]

        src._session.get.side_effect = [switch_resp, iface_resp]

        result = src.get_objects("switches")

        assert [iface["name"] for iface in result[0]["interfaces"]] == [
            "port-channel15",
            "Ethernet1/1",
        ]
        assert result[0]["interfaces"][1]["lag_name"] == "port-channel15"


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestNexusClose:
    def test_close_clears_session(self):
        src = NexusDashboardSource()
        src._session = MagicMock()
        src.close()
        assert src._session is None

    def test_close_is_safe_when_not_connected(self):
        src = NexusDashboardSource()
        src.close()  # should not raise
        assert src._session is None
