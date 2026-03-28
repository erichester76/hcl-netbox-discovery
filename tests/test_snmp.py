"""Tests for the SNMP source adapter (collector/sources/snmp.py).

All pysnmp calls are mocked — no real SNMP agent is required.
"""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from collector.config import SourceConfig
from collector.sources.snmp import (
    SNMPSource,
    _if_type_to_netbox,
    _ip_suffix,
    _mac_bytes_to_str,
    _netmask_to_prefixlen,
    _parse_juniper_descr,
    _rows_by_index,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def snmp_config():
    """Minimal SNMPv2c SourceConfig for tests."""
    return SourceConfig(
        api_type="snmp",
        url="192.168.1.1,192.168.1.2",
        username="public",
        password="",
        verify_ssl=False,
        extra={
            "version": "2c",
            "port": "161",
            "timeout": "5",
            "retries": "1",
        },
    )


@pytest.fixture()
def snmp_v3_config():
    """SNMPv3 SourceConfig."""
    return SourceConfig(
        api_type="snmp",
        url="10.0.0.1",
        username="snmpv3user",
        password="authpassword",
        verify_ssl=False,
        extra={
            "version": "3",
            "auth_protocol": "sha",
            "priv_protocol": "aes",
            "priv_password": "privpassword",
        },
    )


@pytest.fixture()
def connected_source(snmp_config):
    """Return an SNMPSource with connect() already called (pysnmp mocked)."""
    src = SNMPSource()
    with patch("pysnmp.hlapi.v3arch.asyncio"):
        src.connect(snmp_config)
    return src


# ---------------------------------------------------------------------------
# _netmask_to_prefixlen()
# ---------------------------------------------------------------------------


class TestNetmaskToPrefixlen:
    @pytest.mark.parametrize(
        "mask,expected",
        [
            ("255.255.255.0", 24),
            ("255.255.0.0", 16),
            ("255.255.255.128", 25),
            ("255.255.255.252", 30),
            ("0.0.0.0", 0),
            ("255.255.255.255", 32),
            ("invalid", 32),  # fallback
        ],
    )
    def test_conversion(self, mask, expected):
        assert _netmask_to_prefixlen(mask) == expected


# ---------------------------------------------------------------------------
# _mac_bytes_to_str()
# ---------------------------------------------------------------------------


class TestMacBytesToStr:
    def test_hex_prefixed(self):
        assert _mac_bytes_to_str("0x001122aabbcc") == "00:11:22:AA:BB:CC"

    def test_colon_separated_already(self):
        assert _mac_bytes_to_str("00:11:22:aa:bb:cc") == "00:11:22:AA:BB:CC"

    def test_hyphen_separated(self):
        assert _mac_bytes_to_str("00-11-22-aa-bb-cc") == "00:11:22:AA:BB:CC"

    def test_plain_hex(self):
        assert _mac_bytes_to_str("001122AABBCC") == "00:11:22:AA:BB:CC"

    def test_empty_returns_empty(self):
        assert _mac_bytes_to_str("") == ""

    def test_invalid_returns_empty(self):
        assert _mac_bytes_to_str("not-a-mac") == ""

    def test_uppercase_output(self):
        result = _mac_bytes_to_str("aabbccddeeff")
        assert result == result.upper()


# ---------------------------------------------------------------------------
# _if_type_to_netbox()
# ---------------------------------------------------------------------------


class TestIfTypeToNetbox:
    def test_juniper_ge_interface(self):
        assert _if_type_to_netbox(6, "ge-0/0/0") == "1000base-t"

    def test_juniper_xe_interface(self):
        assert _if_type_to_netbox(6, "xe-0/0/0") == "10gbase-x-sfpp"

    def test_juniper_et_interface(self):
        assert _if_type_to_netbox(6, "et-0/0/0") == "100gbase-x-cfp"

    def test_juniper_ae_lag(self):
        assert _if_type_to_netbox(6, "ae0") == "lag"

    def test_juniper_reth_lag(self):
        assert _if_type_to_netbox(6, "reth0") == "lag"

    def test_juniper_loopback(self):
        assert _if_type_to_netbox(24, "lo0") == "virtual"

    def test_juniper_irb(self):
        assert _if_type_to_netbox(53, "irb.10") == "virtual"

    def test_juniper_fxp_mgmt(self):
        assert _if_type_to_netbox(6, "fxp0") == "1000base-t"

    def test_generic_lag_via_iftype(self):
        # No matching name prefix — falls back to ifType 161
        assert _if_type_to_netbox(161, "bond0") == "lag"

    def test_generic_virtual_via_iftype(self):
        assert _if_type_to_netbox(24, "loopback0") == "virtual"

    def test_unknown_defaults_to_other(self):
        assert _if_type_to_netbox(999, "unknownif") == "other"

    def test_case_insensitive_name(self):
        assert _if_type_to_netbox(6, "GE-0/0/0") == "1000base-t"


# ---------------------------------------------------------------------------
# _parse_juniper_descr()
# ---------------------------------------------------------------------------


class TestParseJuniperDescr:
    def test_mx240(self):
        descr = (
            "Juniper Networks, Inc. mx240 internet router, "
            "kernel JUNOS 18.1R3-S9.6 #0 SMP"
        )
        model, ver = _parse_juniper_descr(descr)
        assert model == "mx240"
        assert ver == "18.1R3-S9.6"

    def test_ex4300(self):
        descr = (
            "Juniper Networks, Inc. ex4300-48t Ethernet Switch, "
            "kernel JUNOS 20.4R3.8"
        )
        model, ver = _parse_juniper_descr(descr)
        assert model == "ex4300-48t"
        assert ver == "20.4R3.8"

    def test_srx300(self):
        descr = (
            "Juniper Networks, Inc. srx300 internet router, "
            "kernel JUNOS 21.2R3-S2.5"
        )
        model, ver = _parse_juniper_descr(descr)
        assert model == "srx300"
        assert ver == "21.2R3-S2.5"

    def test_non_juniper_returns_empty(self):
        model, ver = _parse_juniper_descr("Cisco IOS Software, Version 15.6")
        assert model == ""
        assert ver == ""

    def test_empty_string(self):
        assert _parse_juniper_descr("") == ("", "")


# ---------------------------------------------------------------------------
# _rows_by_index()
# ---------------------------------------------------------------------------


class TestRowsByIndex:
    def test_extracts_last_oid_component(self):
        rows = [
            ("1.3.6.1.2.1.2.2.1.2.1", "ge-0/0/0"),
            ("1.3.6.1.2.1.2.2.1.2.2", "ge-0/0/1"),
            ("1.3.6.1.2.1.2.2.1.2.10", "lo0"),
        ]
        result = _rows_by_index(rows)
        assert result == {1: "ge-0/0/0", 2: "ge-0/0/1", 10: "lo0"}

    def test_ignores_malformed_oid(self):
        rows = [("invalid", "value"), ("1.3.6.1.2.1.2.2.1.2.5", "good")]
        result = _rows_by_index(rows)
        assert 5 in result
        assert result[5] == "good"


# ---------------------------------------------------------------------------
# _ip_suffix()
# ---------------------------------------------------------------------------


class TestIpSuffix:
    def test_extracts_ip_from_oid(self):
        oid = "1.3.6.1.2.1.4.20.1.1.10.1.2.3"
        base = "1.3.6.1.2.1.4.20.1.1"
        assert _ip_suffix(oid, base) == "10.1.2.3"

    def test_returns_empty_for_mismatch(self):
        assert _ip_suffix("1.3.6.1.2.1.5.0", "1.3.6.1.2.1.4.20.1.1") == ""


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestSNMPConnect:
    def test_connect_stores_hosts(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)
        assert src._hosts == ["192.168.1.1", "192.168.1.2"]

    def test_connect_sets_community(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)
        assert src._community == "public"

    def test_connect_sets_port_and_timeout(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)
        assert src._port == 161
        assert src._timeout == 5
        assert src._retries == 1

    def test_connect_v3_stores_credentials(self, snmp_v3_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_v3_config)
        assert src._version == "3"
        assert src._v3_user == "snmpv3user"
        assert src._v3_auth_pass == "authpassword"
        assert src._v3_priv_pass == "privpassword"
        assert src._v3_auth_proto == "sha"
        assert src._v3_priv_proto == "aes"

    def test_connect_raises_if_no_url(self, snmp_config):
        snmp_config.url = ""
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            with pytest.raises(ValueError, match="url"):
                src.connect(snmp_config)

    def test_connect_raises_if_pysnmp_missing(self, snmp_config):
        src = SNMPSource()
        with patch.dict(sys.modules, {"pysnmp": None, "pysnmp.hlapi.v3arch.asyncio": None}):
            with pytest.raises((RuntimeError, ImportError)):
                src.connect(snmp_config)

    def test_connect_trims_whitespace_from_hosts(self):
        cfg = SourceConfig(
            api_type="snmp",
            url="  10.0.0.1 , 10.0.0.2  ",
            username="public",
        )
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(cfg)
        assert src._hosts == ["10.0.0.1", "10.0.0.2"]


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestSNMPGetObjects:
    def test_raises_without_connect(self):
        src = SNMPSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("devices")

    def test_raises_for_unknown_collection(self, connected_source):
        with pytest.raises(ValueError, match="unknown collection"):
            connected_source.get_objects("interfaces")

    def test_devices_collection_calls_asyncio_run(self, connected_source):
        mock_result = [{"host": "192.168.1.1", "name": "router-01"}]
        with patch("asyncio.run", return_value=mock_result) as mock_run:
            result = connected_source.get_objects("devices")
        mock_run.assert_called_once()
        assert result == mock_result


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestSNMPClose:
    def test_close_clears_config(self, connected_source):
        connected_source.close()
        assert connected_source._config is None

    def test_close_noop_when_not_connected(self):
        src = SNMPSource()
        src.close()  # should not raise


# ---------------------------------------------------------------------------
# _build_auth()
# ---------------------------------------------------------------------------


class TestBuildAuth:
    def _make_source(self, version, community="public"):
        src = SNMPSource()
        src._version = version
        src._community = community
        return src

    def test_v2c_returns_community_data(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)

        mock_community_data = MagicMock()
        mock_usm_data = MagicMock()
        mock_hlapi = MagicMock()
        mock_hlapi.CommunityData.return_value = mock_community_data
        mock_hlapi.UsmUserData.return_value = mock_usm_data

        with patch.dict(
            "sys.modules", {"pysnmp.hlapi.v3arch.asyncio": mock_hlapi}
        ):
            result = src._build_auth()

        mock_hlapi.CommunityData.assert_called_once_with("public", mpModel=1)
        assert result is mock_community_data

    def test_v1_uses_mpmodel_0(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)
        src._version = "1"

        mock_hlapi = MagicMock()
        with patch.dict("sys.modules", {"pysnmp.hlapi.v3arch.asyncio": mock_hlapi}):
            src._build_auth()
        mock_hlapi.CommunityData.assert_called_once_with("public", mpModel=0)

    def test_v3_returns_usm_user_data(self, snmp_v3_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_v3_config)

        mock_hlapi = MagicMock()
        mock_usm = MagicMock()
        mock_hlapi.UsmUserData.return_value = mock_usm

        with patch.dict("sys.modules", {"pysnmp.hlapi.v3arch.asyncio": mock_hlapi}):
            result = src._build_auth()

        mock_hlapi.UsmUserData.assert_called_once()
        assert result is mock_usm


# ---------------------------------------------------------------------------
# Async collection helpers (tested via asyncio.run)
# ---------------------------------------------------------------------------


SAMPLE_DEVICE = {
    "host": "10.0.0.1",
    "name": "router-mx240",
    "description": "Juniper Networks, Inc. mx240 internet router, kernel JUNOS 20.4R3",
    "location": "DC-1",
    "contact": "noc@example.com",
    "serial": "AB1234",
    "model": "mx240",
    "os_version": "20.4R3",
    "platform": "Junos 20.4R3",
    "manufacturer": "Juniper Networks",
    "interfaces": [
        {
            "index": 1,
            "name": "ge-0/0/0",
            "label": "uplink",
            "type": "1000base-t",
            "mac_address": "AA:BB:CC:DD:EE:01",
            "admin_status": "up",
            "oper_status": "up",
            "speed": 1000,
            "mtu": 1514,
            "ip_addresses": [
                {"address": "10.0.0.1/24", "family": 4, "status": "active", "if_index": 1}
            ],
        }
    ],
}


class TestCollectAllHosts:
    def test_returns_device_list(self, connected_source):
        async def _fake_collect_host(host):
            return SAMPLE_DEVICE.copy()

        connected_source._collect_host = _fake_collect_host
        result = asyncio.run(connected_source._collect_all_hosts())
        assert len(result) == 2  # two hosts in fixture

    def test_skips_failed_hosts(self, connected_source):
        call_count = {"n": 0}

        async def _fake_collect_host(host):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("timeout")
            return SAMPLE_DEVICE.copy()

        connected_source._collect_host = _fake_collect_host
        result = asyncio.run(connected_source._collect_all_hosts())
        assert len(result) == 1

    def test_skips_none_results(self, connected_source):
        async def _fake_collect_host(host):
            return None

        connected_source._collect_host = _fake_collect_host
        result = asyncio.run(connected_source._collect_all_hosts())
        assert result == []


class TestCollectHost:
    def _make_source_with_mocks(self, snmp_config, sys_info, jnx_info=None, ifaces=None, ips=None):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)
        src._hosts = ["10.0.0.1"]

        async def _mock_get_multi(host, oids):
            if "1.3.6.1.4.1.2636.3.1.2.0" in oids:
                return jnx_info or {}
            return sys_info

        async def _mock_collect_interfaces(host):
            return ifaces or []

        async def _mock_collect_ip_addresses(host):
            return ips or []

        src._snmp_get_multi = _mock_get_multi
        src._collect_interfaces = _mock_collect_interfaces
        src._collect_ip_addresses = _mock_collect_ip_addresses
        return src

    def test_juniper_device_identified(self, snmp_config):
        sys_info = {
            "1.3.6.1.2.1.1.1.0": (
                "Juniper Networks, Inc. mx240 internet router, kernel JUNOS 20.4R3"
            ),
            "1.3.6.1.2.1.1.2.0": "1.3.6.1.4.1.2636.1.1.1.2.57",  # Juniper OID
            "1.3.6.1.2.1.1.4.0": "noc@example.com",
            "1.3.6.1.2.1.1.5.0": "router-01",
            "1.3.6.1.2.1.1.6.0": "DC-1",
        }
        jnx_info = {
            "1.3.6.1.4.1.2636.3.1.2.0": "Juniper MX240",
            "1.3.6.1.4.1.2636.3.1.3.0": "AB1234",
        }
        src = self._make_source_with_mocks(snmp_config, sys_info, jnx_info)
        result = asyncio.run(src._collect_host("10.0.0.1"))

        assert result is not None
        assert result["manufacturer"] == "Juniper Networks"
        assert result["serial"] == "AB1234"
        assert result["os_version"] == "20.4R3"
        assert result["name"] == "router-01"
        assert result["location"] == "DC-1"

    def test_non_juniper_device(self, snmp_config):
        sys_info = {
            "1.3.6.1.2.1.1.1.0": "Cisco IOS Software",
            "1.3.6.1.2.1.1.2.0": "1.3.6.1.4.1.9.1.1",  # Cisco OID
            "1.3.6.1.2.1.1.4.0": "",
            "1.3.6.1.2.1.1.5.0": "cisco-router",
            "1.3.6.1.2.1.1.6.0": "",
        }
        src = self._make_source_with_mocks(snmp_config, sys_info)
        result = asyncio.run(src._collect_host("10.0.0.1"))

        assert result is not None
        assert result["manufacturer"] == ""
        assert result["serial"] == ""

    def test_returns_none_on_get_failure(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)

        async def _failing_get(host, oids):
            raise RuntimeError("No SNMP response")

        src._snmp_get_multi = _failing_get
        result = asyncio.run(src._collect_host("10.0.0.1"))
        assert result is None

    def test_host_fallback_when_sysname_empty(self, snmp_config):
        sys_info = {
            "1.3.6.1.2.1.1.1.0": "",
            "1.3.6.1.2.1.1.2.0": "1.3.6.1.4.1.9.1.1",
            "1.3.6.1.2.1.1.4.0": "",
            "1.3.6.1.2.1.1.5.0": "",  # empty sysName
            "1.3.6.1.2.1.1.6.0": "",
        }
        src = self._make_source_with_mocks(snmp_config, sys_info)
        result = asyncio.run(src._collect_host("10.0.0.1"))
        assert result["name"] == "10.0.0.1"

    def test_ip_addresses_attached_to_interfaces(self, snmp_config):
        sys_info = {
            "1.3.6.1.2.1.1.1.0": "",
            "1.3.6.1.2.1.1.2.0": "1.3.6.1.4.1.9",
            "1.3.6.1.2.1.1.4.0": "",
            "1.3.6.1.2.1.1.5.0": "sw-01",
            "1.3.6.1.2.1.1.6.0": "",
        }
        ifaces = [
            {"index": 1, "name": "ge-0/0/0", "ip_addresses": []},
            {"index": 2, "name": "ge-0/0/1", "ip_addresses": []},
        ]
        ips = [
            {"address": "10.1.1.1/24", "family": 4, "status": "active", "if_index": 1},
            {"address": "10.1.2.1/24", "family": 4, "status": "active", "if_index": 2},
        ]
        src = self._make_source_with_mocks(snmp_config, sys_info, ifaces=ifaces, ips=ips)
        result = asyncio.run(src._collect_host("10.0.0.1"))

        iface_map = {i["name"]: i for i in result["interfaces"]}
        assert iface_map["ge-0/0/0"]["ip_addresses"][0]["address"] == "10.1.1.1/24"
        assert iface_map["ge-0/0/1"]["ip_addresses"][0]["address"] == "10.1.2.1/24"


class TestCollectInterfaces:
    def _make_source(self, snmp_config, walk_responses):
        """Build an SNMPSource whose _snmp_walk returns preset data."""
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)
        src._hosts = ["10.0.0.1"]

        async def _mock_walk(host, base_oid):
            return walk_responses.get(base_oid, [])

        src._snmp_walk = _mock_walk
        return src

    def test_builds_interface_list(self, snmp_config):
        from collector.sources.snmp import (
            _OID_IF_ADMIN_STATUS,
            _OID_IF_ALIAS,
            _OID_IF_DESCR,
            _OID_IF_HIGH_SPEED,
            _OID_IF_MTU,
            _OID_IF_NAME,
            _OID_IF_OPER_STATUS,
            _OID_IF_PHYS_ADDR,
            _OID_IF_SPEED,
            _OID_IF_TYPE,
        )

        walk_resp = {
            _OID_IF_DESCR:        [("1.3.6.1.2.1.2.2.1.2.1", "ge-0/0/0")],
            _OID_IF_TYPE:         [("1.3.6.1.2.1.2.2.1.3.1", "6")],
            _OID_IF_MTU:          [("1.3.6.1.2.1.2.2.1.4.1", "1514")],
            _OID_IF_SPEED:        [("1.3.6.1.2.1.2.2.1.5.1", "1000000000")],
            _OID_IF_PHYS_ADDR:    [("1.3.6.1.2.1.2.2.1.6.1", "0xaabbccddeeff")],
            _OID_IF_ADMIN_STATUS: [("1.3.6.1.2.1.2.2.1.7.1", "1")],
            _OID_IF_OPER_STATUS:  [("1.3.6.1.2.1.2.2.1.8.1", "1")],
            _OID_IF_NAME:         [("1.3.6.1.2.1.31.1.1.1.1.1", "ge-0/0/0")],
            _OID_IF_ALIAS:        [("1.3.6.1.2.1.31.1.1.1.18.1", "uplink-to-core")],
            _OID_IF_HIGH_SPEED:   [("1.3.6.1.2.1.31.1.1.1.15.1", "1000")],
        }
        src = self._make_source(snmp_config, walk_resp)
        ifaces = asyncio.run(src._collect_interfaces("10.0.0.1"))

        assert len(ifaces) == 1
        iface = ifaces[0]
        assert iface["index"] == 1
        assert iface["name"] == "ge-0/0/0"
        assert iface["label"] == "uplink-to-core"
        assert iface["type"] == "1000base-t"
        assert iface["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert iface["admin_status"] == "up"
        assert iface["oper_status"] == "up"
        assert iface["speed"] == 1000
        assert iface["mtu"] == 1514

    def test_speed_falls_back_to_ifspeed(self, snmp_config):
        from collector.sources.snmp import (
            _OID_IF_ADMIN_STATUS,
            _OID_IF_ALIAS,
            _OID_IF_DESCR,
            _OID_IF_HIGH_SPEED,
            _OID_IF_MTU,
            _OID_IF_NAME,
            _OID_IF_OPER_STATUS,
            _OID_IF_PHYS_ADDR,
            _OID_IF_SPEED,
            _OID_IF_TYPE,
        )

        walk_resp = {
            _OID_IF_DESCR:        [("1.3.6.1.2.1.2.2.1.2.3", "lo0")],
            _OID_IF_TYPE:         [("1.3.6.1.2.1.2.2.1.3.3", "24")],
            _OID_IF_MTU:          [("1.3.6.1.2.1.2.2.1.4.3", "65535")],
            _OID_IF_SPEED:        [("1.3.6.1.2.1.2.2.1.5.3", "4294967295")],  # max 32-bit
            _OID_IF_PHYS_ADDR:    [],
            _OID_IF_ADMIN_STATUS: [("1.3.6.1.2.1.2.2.1.7.3", "1")],
            _OID_IF_OPER_STATUS:  [("1.3.6.1.2.1.2.2.1.8.3", "1")],
            _OID_IF_NAME:         [],  # no ifXTable
            _OID_IF_ALIAS:        [],
            _OID_IF_HIGH_SPEED:   [],  # no ifHighSpeed → use ifSpeed fallback
        }
        src = self._make_source(snmp_config, walk_resp)
        ifaces = asyncio.run(src._collect_interfaces("10.0.0.1"))

        assert len(ifaces) == 1
        assert ifaces[0]["name"] == "lo0"
        # ifSpeed 4294967295 bps / 1e6 = 4294 Mbps
        assert ifaces[0]["speed"] == 4294967295 // 1_000_000

    def test_returns_empty_on_walk_failure(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)

        async def _failing_walk(host, base_oid):
            raise RuntimeError("timeout")

        src._snmp_walk = _failing_walk
        ifaces = asyncio.run(src._collect_interfaces("10.0.0.1"))
        assert ifaces == []


class TestCollectIpAddresses:
    def _make_source(self, snmp_config, walk_responses):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)

        async def _mock_walk(host, base_oid):
            return walk_responses.get(base_oid, [])

        src._snmp_walk = _mock_walk
        return src

    def test_builds_ip_list(self, snmp_config):
        from collector.sources.snmp import (
            _OID_IP_ADDR,
            _OID_IP_IF_IDX,
            _OID_IP_NETMASK,
        )

        walk_resp = {
            _OID_IP_ADDR:    [("1.3.6.1.2.1.4.20.1.1.10.1.1.1", "10.1.1.1")],
            _OID_IP_IF_IDX:  [("1.3.6.1.2.1.4.20.1.2.10.1.1.1", "5")],
            _OID_IP_NETMASK: [("1.3.6.1.2.1.4.20.1.3.10.1.1.1", "255.255.255.0")],
        }
        src = self._make_source(snmp_config, walk_resp)
        ips = asyncio.run(src._collect_ip_addresses("10.0.0.1"))

        assert len(ips) == 1
        assert ips[0]["address"] == "10.1.1.1/24"
        assert ips[0]["family"] == 4
        assert ips[0]["status"] == "active"
        assert ips[0]["if_index"] == 5

    def test_returns_empty_on_walk_failure(self, snmp_config):
        src = SNMPSource()
        with patch("pysnmp.hlapi.v3arch.asyncio"):
            src.connect(snmp_config)

        async def _failing_walk(host, base_oid):
            raise RuntimeError("network error")

        src._snmp_walk = _failing_walk
        ips = asyncio.run(src._collect_ip_addresses("10.0.0.1"))
        assert ips == []

    def test_multiple_ips_different_interfaces(self, snmp_config):
        from collector.sources.snmp import (
            _OID_IP_ADDR,
            _OID_IP_IF_IDX,
            _OID_IP_NETMASK,
        )

        walk_resp = {
            _OID_IP_ADDR: [
                ("1.3.6.1.2.1.4.20.1.1.10.1.1.1", "10.1.1.1"),
                ("1.3.6.1.2.1.4.20.1.1.192.168.0.1", "192.168.0.1"),
            ],
            _OID_IP_IF_IDX: [
                ("1.3.6.1.2.1.4.20.1.2.10.1.1.1", "1"),
                ("1.3.6.1.2.1.4.20.1.2.192.168.0.1", "2"),
            ],
            _OID_IP_NETMASK: [
                ("1.3.6.1.2.1.4.20.1.3.10.1.1.1", "255.255.255.0"),
                ("1.3.6.1.2.1.4.20.1.3.192.168.0.1", "255.255.255.252"),
            ],
        }
        src = self._make_source(snmp_config, walk_resp)
        ips = asyncio.run(src._collect_ip_addresses("10.0.0.1"))

        addresses = {ip["address"] for ip in ips}
        assert "10.1.1.1/24" in addresses
        assert "192.168.0.1/30" in addresses
