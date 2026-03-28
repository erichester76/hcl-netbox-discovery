"""Tests for the F5 BIG-IP iControl REST source adapter (collector/sources/f5.py).

All HTTP calls are mocked — no real BIG-IP is required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collector.sources.f5 import (
    F5Source,
    _map_platform_id,
    _normalize_iface_type,
    _parse_media_speed_mbps,
    _safe_get,
    _strip_partition,
)

# ---------------------------------------------------------------------------
# _normalize_iface_type()
# ---------------------------------------------------------------------------


class TestNormalizeIfaceType:
    @pytest.mark.parametrize(
        "media, speed, expected",
        [
            ("100000SR4-FD", None,   "100gbase-x-qsfp28"),
            ("40000SR4-FD",  None,   "40gbase-x-qsfpp"),
            ("25000SR-FD",   None,   "25gbase-x-sfp28"),
            ("10000SR-FD",   None,   "10gbase-x-sfpp"),
            ("10000T-FD",    None,   "10gbase-t"),
            ("1000T-FD",     None,   "1000base-t"),
            ("1000SX-FD",    None,   "1000base-x-sfp"),
            ("100TX-FD",     None,   "100base-tx"),
            ("100FX-FD",     None,   "100base-fx"),
            # Speed fallback when media is empty
            ("",             10000,  "10gbase-x-sfpp"),
            ("",             1000,   "1000base-t"),
            ("",             100,    "100base-tx"),
            ("",             None,   "other"),
            # Unrecognised media with no speed
            ("UNKNOWN-FD",   None,   "other"),
        ],
    )
    def test_normalize_iface_type(self, media, speed, expected):
        assert _normalize_iface_type(media, speed) == expected


# ---------------------------------------------------------------------------
# _parse_media_speed_mbps()
# ---------------------------------------------------------------------------


class TestParseMediaSpeedMbps:
    @pytest.mark.parametrize(
        "media_str, expected",
        [
            ("10000SR-FD",   10000),
            ("1000T-FD",     1000),
            ("100TX-FD",     100),
            ("40000SR4-FD",  40000),
            ("100000SR4-FD", 100000),
            ("25000SR-FD",   25000),
            ("",             None),
            ("unknown",      None),
        ],
    )
    def test_parse_media_speed_mbps(self, media_str, expected):
        assert _parse_media_speed_mbps(media_str) == expected


# ---------------------------------------------------------------------------
# _map_platform_id()
# ---------------------------------------------------------------------------


class TestMapPlatformId:
    @pytest.mark.parametrize(
        "platform_id, expected",
        [
            ("Z100",   "BIG-IP 2000"),
            ("Z101G",  "BIG-IP i7800"),
            ("A109",   "BIG-IP Virtual Edition"),
            ("Z99",    "BIG-IP Virtual Edition"),
            ("ZXXX",   "BIG-IP ZXXX"),   # unknown ID gets a generic prefix
            ("",       ""),
        ],
    )
    def test_map_platform_id(self, platform_id, expected):
        assert _map_platform_id(platform_id) == expected


# ---------------------------------------------------------------------------
# _strip_partition()
# ---------------------------------------------------------------------------


class TestStripPartition:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/Common/vs_http",   "vs_http"),
            ("/Partition/pool_1", "pool_1"),
            ("vs_http",           "vs_http"),
            ("/only_one",         "only_one"),
        ],
    )
    def test_strip_partition(self, path, expected):
        assert _strip_partition(path) == expected


# ---------------------------------------------------------------------------
# _safe_get()
# ---------------------------------------------------------------------------


class TestSafeGet:
    def test_dict_access(self):
        assert _safe_get({"key": "val"}, "key") == "val"

    def test_dict_missing_key_returns_default(self):
        assert _safe_get({}, "key", "default") == "default"

    def test_object_access(self):
        class Obj:
            name = "bigip1"
        assert _safe_get(Obj(), "name") == "bigip1"

    def test_missing_attr_returns_none(self):
        assert _safe_get(object(), "missing") is None


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestF5Connect:
    def _make_token_session(self, token: str = "test-token") -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"token": {"token": token}}
        mock_resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.post.return_value = mock_resp
        session.headers = {}
        return session

    def test_connect_sets_auth_token(self, f5_config):
        src = F5Source()
        with patch("collector.sources.f5.requests.Session") as MockSession:
            session = self._make_token_session()
            MockSession.return_value = session
            src.connect(f5_config)
        assert src._session is session
        assert "X-F5-Auth-Token" in session.headers
        assert session.headers["X-F5-Auth-Token"] == "test-token"

    def test_connect_prepends_https_if_missing(self, f5_config):
        f5_config.url = "bigip.example.com"
        src = F5Source()
        with patch("collector.sources.f5.requests.Session") as MockSession:
            session = self._make_token_session()
            MockSession.return_value = session
            src.connect(f5_config)
        assert src._base_url.startswith("https://")

    def test_connect_raises_if_credentials_missing(self, f5_config):
        f5_config.username = ""
        f5_config.password = ""
        src = F5Source()
        with pytest.raises(RuntimeError, match="F5_USER"):
            src.connect(f5_config)

    def test_fetch_interfaces_flag_true(self, f5_config):
        f5_config.extra = {"fetch_interfaces": "true"}
        src = F5Source()
        with patch("collector.sources.f5.requests.Session") as MockSession:
            MockSession.return_value = self._make_token_session()
            src.connect(f5_config)
        assert src._fetch_interfaces is True

    def test_fetch_interfaces_flag_false(self, f5_config):
        f5_config.extra = {"fetch_interfaces": "false"}
        src = F5Source()
        with patch("collector.sources.f5.requests.Session") as MockSession:
            MockSession.return_value = self._make_token_session()
            src.connect(f5_config)
        assert src._fetch_interfaces is False

    def test_connect_falls_back_to_basic_auth_on_token_failure(self, f5_config):
        """When the token endpoint fails the session should use basic auth."""
        src = F5Source()
        with patch("collector.sources.f5.requests.Session") as MockSession:
            session = MagicMock()
            session.headers = {}
            session.post.side_effect = Exception("connection refused")
            MockSession.return_value = session
            src.connect(f5_config)
        assert session.auth == (f5_config.username, f5_config.password)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestF5GetObjects:
    def _connected_source(self) -> F5Source:
        src = F5Source()
        src._session = MagicMock()
        return src

    def test_raises_without_connect(self):
        src = F5Source()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("devices")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("pools")

    def test_get_devices_returns_single_enriched_dict(self):
        src = self._connected_source()

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "identified-devices" in path:
                resp.json.return_value = {
                    "hostname":   "bigip1.example.com",
                    "product":    "BIG-IP",
                    "platformId": "Z101G",
                    "chassisId":  "f5-abc1-0001",
                }
            elif "/version" in path:
                resp.json.return_value = {
                    "entries": {
                        "https://localhost/mgmt/tm/sys/version/0": {
                            "nestedStats": {
                                "entries": {
                                    "Version": {"description": "16.1.3"},
                                    "Product": {"description": "BIG-IP"},
                                }
                            }
                        }
                    }
                }
            elif "management-ip" in path:
                resp.json.return_value = {
                    "items": [{"name": "192.168.1.10/24"}]
                }
            elif "/hardware" in path:
                resp.json.return_value = {
                    "entries": {
                        "https://localhost/mgmt/tm/sys/hardware/0": {
                            "nestedStats": {
                                "entries": {
                                    "Chassis": {
                                        "nestedStats": {
                                            "entries": {
                                                "bigipChassisSerialNum": {
                                                    "description": "chs000001"
                                                }
                                            }
                                        }
                                    },
                                    "Platform": {
                                        "nestedStats": {
                                            "entries": {
                                                "Model": {
                                                    "description": "BIG-IP i7800"
                                                }
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    }
                }
            else:
                resp.json.return_value = {}
            return resp

        src._session.get.side_effect = mock_get

        result = src.get_objects("devices")

        assert len(result) == 1
        d = result[0]
        assert d["name"] == "bigip1"
        assert d["model"] == "BIG-IP i7800"
        assert d["manufacturer"] == "F5 Networks"
        assert d["serial"] == "CHS000001"
        assert d["status"] == "active"
        assert d["platform_name"] == "BIG-IP 16.1.3"
        assert d["mgmt_address"] == "192.168.1.10"

    def test_get_devices_uses_chassis_id_as_serial_fallback(self):
        """When hardware endpoint fails, chassisId is used as serial."""
        src = self._connected_source()

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "identified-devices" in path:
                resp.json.return_value = {
                    "hostname":   "bigip2.lab",
                    "product":    "BIG-IP",
                    "platformId": "A109",
                    "chassisId":  "f5-ve-chassis-001",
                }
            elif "/hardware" in path:
                raise Exception("not available on VE")
            elif "/version" in path:
                resp.json.return_value = {}
            elif "management-ip" in path:
                resp.json.return_value = {"items": []}
            else:
                resp.json.return_value = {}
            return resp

        src._session.get.side_effect = mock_get

        result = src.get_objects("devices")
        d = result[0]
        assert d["serial"] == "F5-VE-CHASSIS-001"
        assert d["model"] == "BIG-IP Virtual Edition"

    def test_get_devices_without_interfaces_key_absent(self):
        """When fetch_interfaces is False no 'interfaces' key should appear."""
        src = self._connected_source()
        src._fetch_interfaces = False

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {}
            return resp

        src._session.get.side_effect = mock_get

        result = src.get_objects("devices")
        assert "interfaces" not in result[0]

    def test_get_devices_with_interfaces_key_present(self):
        """When fetch_interfaces is True 'interfaces' key should appear."""
        src = self._connected_source()
        src._fetch_interfaces = True

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "net/interface" in path:
                resp.json.return_value = {
                    "items": [
                        {
                            "name":        "1.1",
                            "macAddress":  "00:1b:17:00:aa:bb",
                            "mediaActive": "10000SR-FD",
                            "mtu":         9198,
                            "enabled":     True,
                        }
                    ]
                }
            elif "net/self" in path:
                resp.json.return_value = {"items": []}
            else:
                resp.json.return_value = {}
            return resp

        src._session.get.side_effect = mock_get

        result = src.get_objects("devices")
        assert "interfaces" in result[0]
        assert len(result[0]["interfaces"]) == 1
        iface = result[0]["interfaces"][0]
        assert iface["name"] == "1.1"
        assert iface["type"] == "10gbase-x-sfpp"
        assert iface["mac_address"] == "00:1B:17:00:AA:BB"

    def test_get_virtual_servers(self):
        src = self._connected_source()

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {
                "items": [
                    {
                        "name":        "vs_http",
                        "fullPath":    "/Common/vs_http",
                        "partition":   "Common",
                        "destination": "/Common/10.0.0.100:80",
                        "pool":        "/Common/pool_http",
                        "ipProtocol":  "tcp",
                        "enabled":     True,
                    }
                ]
            }
            return resp

        src._session.get.side_effect = mock_get

        result = src.get_objects("virtual_servers")
        assert len(result) == 1
        vs = result[0]
        assert vs["name"] == "vs_http"
        assert vs["destination"] == "10.0.0.100:80"
        assert vs["pool"] == "pool_http"
        assert vs["protocol"] == "tcp"
        assert vs["status"] == "active"
        assert vs["partition"] == "Common"

    def test_get_virtual_servers_disabled(self):
        src = self._connected_source()

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {
                "items": [
                    {
                        "name":        "vs_disabled",
                        "fullPath":    "/Common/vs_disabled",
                        "partition":   "Common",
                        "destination": "/Common/10.0.0.200:443",
                        "pool":        "",
                        "ipProtocol":  "tcp",
                        "disabled":    True,
                    }
                ]
            }
            return resp

        src._session.get.side_effect = mock_get

        result = src.get_objects("virtual_servers")
        assert result[0]["status"] == "offline"


# ---------------------------------------------------------------------------
# _enrich_interface() edge cases
# ---------------------------------------------------------------------------


class TestF5EnrichInterface:
    def test_mgmt_interface_type_override(self):
        src = F5Source()
        iface = {"name": "mgmt", "macAddress": "00:0c:29:ab:cd:ef", "enabled": True}
        result = src._enrich_interface(iface)
        assert result["type"] == "1000base-t"

    def test_loopback_interface_type_override(self):
        src = F5Source()
        iface = {"name": "lo0", "enabled": True}
        result = src._enrich_interface(iface)
        assert result["type"] == "virtual"

    def test_mac_address_uppercased(self):
        src = F5Source()
        iface = {"name": "1.1", "macAddress": "aa:bb:cc:dd:ee:ff", "enabled": True}
        result = src._enrich_interface(iface)
        assert result["mac_address"] == "AA:BB:CC:DD:EE:FF"

    def test_disabled_interface(self):
        src = F5Source()
        iface = {"name": "1.2", "enabled": False, "mediaActive": "1000T-FD"}
        result = src._enrich_interface(iface)
        assert result["enabled"] is False

    def test_speed_extracted_from_media(self):
        src = F5Source()
        iface = {"name": "1.3", "enabled": True, "mediaActive": "10000SR-FD"}
        result = src._enrich_interface(iface)
        assert result["speed"] == 10000

    def test_missing_mac_returns_empty_string(self):
        src = F5Source()
        iface = {"name": "1.4", "enabled": True}
        result = src._enrich_interface(iface)
        assert result["mac_address"] == ""


# ---------------------------------------------------------------------------
# Self-IP mapping
# ---------------------------------------------------------------------------


class TestF5SelfIPs:
    def test_self_ips_attached_to_interfaces(self):
        src = F5Source()
        src._session = MagicMock()

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "net/interface" in path:
                resp.json.return_value = {
                    "items": [{"name": "internal", "enabled": True}]
                }
            elif "net/self" in path:
                resp.json.return_value = {
                    "items": [
                        {"address": "10.1.0.1/24", "vlan": "/Common/internal"}
                    ]
                }
            else:
                resp.json.return_value = {}
            return resp

        src._session.get.side_effect = mock_get
        interfaces = src._get_interfaces()

        assert len(interfaces) == 1
        assert interfaces[0]["ip_addresses"] == [
            {"address": "10.1.0.1/24", "status": "active"}
        ]

    def test_self_ip_fetch_failure_returns_empty_mapping(self):
        src = F5Source()
        src._session = MagicMock()

        def mock_get(path, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "net/interface" in path:
                resp.json.return_value = {"items": [{"name": "external", "enabled": True}]}
            elif "net/self" in path:
                raise Exception("permission denied")
            else:
                resp.json.return_value = {}
            return resp

        src._session.get.side_effect = mock_get
        interfaces = src._get_interfaces()

        assert interfaces[0]["ip_addresses"] == []


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestF5Close:
    def test_close_clears_session(self):
        src = F5Source()
        mock_session = MagicMock()
        src._session = mock_session
        src.close()
        mock_session.close.assert_called_once()
        assert src._session is None

    def test_close_is_safe_when_not_connected(self):
        src = F5Source()
        src.close()
        assert src._session is None
