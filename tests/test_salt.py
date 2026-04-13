"""Tests for the Salt grains artifact source adapter."""

from __future__ import annotations

import json

import pytest

from collector.sources.salt import (
    SaltSource,
    _clean_ip_list,
    _infer_iface_type,
    _iter_records,
    _normalise_host,
)


class TestCleanIpList:
    def test_filters_loopback_and_invalid_values(self):
        result = _clean_ip_list(["10.0.0.5", "", "127.0.0.1", "not-an-ip", "::1"])
        assert result == ["10.0.0.5"]


class TestInferIfaceType:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("bond0", "lag"),
            ("br0", "bridge"),
            ("eth0", "1000base-t"),
            ("lo", "virtual"),
            ("wlan0", "ieee802.11a"),
            ("ib0", "infiniband"),
            ("xe-0/0/0", "other"),
        ],
    )
    def test_maps_common_prefixes(self, name, expected):
        assert _infer_iface_type(name) == expected


class TestIterRecords:
    def test_accepts_return_dict_shape(self):
        payload = {"return": {"minion-1": {"host": "web-01"}}}
        assert _iter_records(payload) == [("minion-1", {"host": "web-01"})]

    def test_accepts_return_list_shape(self):
        payload = {"return": [{"minion-1": {"host": "web-01"}}]}
        assert _iter_records(payload) == [("minion-1", {"host": "web-01"})]

    def test_accepts_explicit_host_records(self):
        payload = [{"id": "minion-1", "grains": {"host": "web-01"}}]
        assert _iter_records(payload) == [("minion-1", payload[0])]


class TestNormaliseHost:
    def test_builds_host_and_interface_shape(self):
        record = {
            "grains": {
                "host": "web-01",
                "fqdn": "web-01.example.com",
                "os": "Ubuntu",
                "os_family": "Debian",
                "osrelease": "24.04",
                "kernelrelease": "6.8.0",
                "serialnumber": "ABC123",
                "productname": "PowerEdge R650",
                "manufacturer": "Dell Inc.",
                "virtual": "kvm",
                "ipv4": ["10.0.0.5", "127.0.0.1"],
                "ipv6": ["2001:db8::5", "::1"],
                "ip_interfaces": {
                    "eth0": ["10.0.0.5", "fe80::1"],
                    "lo": ["127.0.0.1"],
                },
                "hwaddr_interfaces": {
                    "eth0": "00:11:22:33:44:55",
                    "lo": "",
                },
            }
        }

        host = _normalise_host("minion-1", record)

        assert host["name"] == "web-01"
        assert host["hostname"] == "web-01.example.com"
        assert host["platform"] == "Ubuntu 24.04"
        assert host["serial"] == "ABC123"
        assert host["manufacturer"] == "Dell Inc."
        assert host["ip_addresses"] == [
            {"address": "10.0.0.5/32", "family": 4, "status": "active"},
            {"address": "fe80::1/128", "family": 6, "status": "active"},
            {"address": "2001:db8::5/128", "family": 6, "status": "active"},
        ]
        assert host["interfaces"][0]["name"] == "eth0"
        assert host["interfaces"][0]["mac_address"] == "00:11:22:33:44:55"
        assert host["interfaces"][0]["ip_addresses"][0]["address"] == "10.0.0.5/32"


class TestSaltSource:
    def test_connect_requires_artifact_path(self, salt_config):
        salt_config.url = ""
        salt_config.extra = {}

        source = SaltSource()

        with pytest.raises(ValueError, match="artifact_path"):
            source.connect(salt_config)

    def test_reads_hosts_collection_from_artifact(self, tmp_path, salt_config):
        artifact = tmp_path / "salt-grains.json"
        artifact.write_text(
            json.dumps(
                {
                    "return": {
                        "minion-1": {
                            "host": "web-01",
                            "fqdn": "web-01.example.com",
                            "os": "Ubuntu",
                            "osrelease": "24.04",
                            "ip_interfaces": {"eth0": ["10.0.0.5"]},
                            "hwaddr_interfaces": {"eth0": "00:11:22:33:44:55"},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        salt_config.extra["artifact_path"] = str(artifact)
        salt_config.url = ""

        source = SaltSource()
        source.connect(salt_config)
        hosts = source.get_objects("hosts")

        assert len(hosts) == 1
        assert hosts[0]["name"] == "web-01"
        assert hosts[0]["source_system"] == "salt"
        assert hosts[0]["interfaces"][0]["name"] == "eth0"

    def test_rejects_unknown_collection(self, tmp_path, salt_config):
        artifact = tmp_path / "salt-grains.json"
        artifact.write_text(json.dumps({"return": {}}), encoding="utf-8")
        salt_config.extra["artifact_path"] = str(artifact)
        salt_config.url = ""

        source = SaltSource()
        source.connect(salt_config)

        with pytest.raises(ValueError, match="Supported: \\['hosts'\\]"):
            source.get_objects("devices")
