"""Tests for the Cisco Nexus Dashboard source adapter (collector/sources/nexus.py).

All HTTP calls are mocked — no real Nexus Dashboard / NDFC is required.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.nexus import (
    NexusDashboardSource,
    _derive_interface_name_details,
    _flatten_interface_payload,
    _flatten_nv_pairs,
    _normalize_iface_type,
    _normalize_model,
    _nvpair_get,
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
        "raw_type, expected",
        [
            ("INTERFACE_ETHERNET",     "1000base-t"),
            ("INTERFACE_MANAGEMENT",   "1000base-t"),
            ("INTERFACE_PORT_CHANNEL", "lag"),
            ("INTERFACE_LOOPBACK",     "virtual"),
            ("INTERFACE_VLAN",         "virtual"),
            ("INTERFACE_NVE",          "virtual"),
            ("eth",                    "1000base-t"),
            ("port-channel",           "lag"),
            ("loopback",               "virtual"),
            ("UNKNOWN_TYPE",           "other"),
            ("",                       "other"),
        ],
    )
    def test_normalize_iface_type(self, raw_type, expected):
        assert _normalize_iface_type(raw_type) == expected


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


class TestNvPairsHelpers:
    def test_flattens_dict_nv_pairs(self):
        nv_pairs = {
            "ifType": "INTERFACE_LOOPBACK",
            "speedStr": "10G",
            "ipAddress": "10.0.0.1/32",
        }

        assert _flatten_nv_pairs(nv_pairs) == {
            "iftype": "INTERFACE_LOOPBACK",
            "speedstr": "10G",
            "ipaddress": "10.0.0.1/32",
        }

    def test_flattens_list_style_nv_pairs(self):
        iface = {
            "nvPairs": [
                {"key": "ifType", "value": "INTERFACE_PORT_CHANNEL"},
                {"key": "speedStr", "value": "100G"},
            ]
        }

        assert _nvpair_get(iface, "ifType") == "INTERFACE_PORT_CHANNEL"
        assert _nvpair_get(iface, "speedStr") == "100G"


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
                "speedStr":   "10G",
            }
        ]

        src._session.get.side_effect = [switch_resp, iface_resp]

        result = src.get_objects("switches")
        assert len(result) == 1
        assert "interfaces" in result[0]
        iface = result[0]["interfaces"][0]
        assert iface["name"] == "Ethernet1/1"
        assert iface["type"] == "1000base-t"
        assert iface["enabled"] is True
        assert iface["mac_address"] == "00:1B:44:11:3A:B7"
        assert iface["speed"] == 10000

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

        src._session.get.side_effect = [switch_resp, iface_resp]

        result = src.get_objects("switches")

        assert result[0]["interfaces"][0]["name"] == "mgmt0"
        assert result[0]["interfaces"][0]["mgmt_only"] is True

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

        def side_effect(url, **kwargs):
            if "interface" in url:
                raise Exception("timeout")
            return switch_resp

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
        assert result["type"] == "1000base-t"
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

    def test_interface_uses_nv_pairs_when_top_level_fields_missing(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "loopback0",
            "nvPairs": {
                "ifType": "INTERFACE_LOOPBACK",
                "adminState": "up",
                "operStatus": "up",
                "ifDescr": "router-id",
                "ipAddress": "10.255.0.1/32",
                "speedStr": "",
            },
        }

        result = src._enrich_interface(iface)

        assert result["type"] == "virtual"
        assert result["enabled"] is True
        assert result["description"] == "router-id"
        assert result["ip_address"] == "10.255.0.1/32"

    def test_management_interface_falls_back_to_switch_ip(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "mgmt0",
            "nvPairs": {
                "ifType": "INTERFACE_MANAGEMENT",
                "adminState": "up",
                "operStatus": "up",
            },
        }

        result = src._enrich_interface(iface, switch_ip_address="10.19.237.183")

        assert result["mgmt_only"] is True
        assert result["ip_address"] == "10.19.237.183"

    def test_port_channel_uses_nv_pairs_type(self):
        src = NexusDashboardSource()
        iface = {
            "ifName": "port-channel500",
            "nvPairs": {
                "ifType": "INTERFACE_PORT_CHANNEL",
                "adminState": "up",
            },
        }

        result = src._enrich_interface(iface)

        assert result["type"] == "lag"

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
        assert result["type"] == "1000base-t"
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
