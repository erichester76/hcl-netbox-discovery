"""Tests for the Ansible facts artifact source adapter."""

from __future__ import annotations

import json

import pytest

from collector.sources.ansible import (
    AnsibleSource,
    _extract_interface_ips,
    _infer_iface_type,
    _interface_var_name,
    _iter_hostvars,
    _normalise_host,
)


class TestInterfaceVarName:
    def test_normalises_hyphens(self):
        assert _interface_var_name("eth0.100") == "ansible_eth0.100"
        assert _interface_var_name("bond-0") == "ansible_bond_0"


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


class TestExtractInterfaceIps:
    def test_reads_ipv4_and_ipv6(self):
        details = {
            "ipv4": {"address": "10.0.0.5"},
            "ipv6": [{"address": "2001:db8::5"}, {"address": "::1"}],
        }
        assert _extract_interface_ips(details) == ["10.0.0.5", "2001:db8::5"]


class TestIterHostvars:
    def test_accepts_meta_hostvars_shape(self):
        payload = {"_meta": {"hostvars": {"web-01": {"ansible_hostname": "web-01"}}}}
        assert _iter_hostvars(payload) == [("web-01", {"ansible_hostname": "web-01"})]

    def test_accepts_list_shape(self):
        payload = [{"inventory_hostname": "web-01", "ansible_hostname": "web-01"}]
        assert _iter_hostvars(payload) == [("web-01", payload[0])]


class TestNormaliseHost:
    def test_builds_host_and_interface_shape(self):
        record = {
            "ansible_facts": {
                "ansible_hostname": "web-01",
                "ansible_fqdn": "web-01.example.com",
                "ansible_distribution": "Ubuntu",
                "ansible_distribution_version": "24.04",
                "ansible_os_family": "Debian",
                "ansible_kernel": "6.8.0",
                "ansible_product_serial": "ABC123",
                "ansible_product_name": "PowerEdge R650",
                "ansible_system_vendor": "Dell Inc.",
                "ansible_virtualization_type": "kvm",
                "ansible_all_ipv4_addresses": ["10.0.0.5", "127.0.0.1"],
                "ansible_all_ipv6_addresses": ["2001:db8::5", "::1"],
                "ansible_interfaces": ["eth0", "lo"],
                "ansible_eth0": {
                    "active": True,
                    "macaddress": "00:11:22:33:44:55",
                    "ipv4": {"address": "10.0.0.5"},
                    "ipv6": [{"address": "fe80::1"}],
                },
                "ansible_lo": {
                    "active": True,
                    "ipv4": {"address": "127.0.0.1"},
                },
            }
        }

        host = _normalise_host("web-01", record)

        assert host["name"] == "web-01"
        assert host["hostname"] == "web-01"
        assert host["fqdn"] == "web-01.example.com"
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


class TestAnsibleSource:
    def test_connect_requires_artifact_path(self, ansible_config):
        ansible_config.url = ""
        ansible_config.extra = {}

        source = AnsibleSource()

        with pytest.raises(ValueError, match="artifact_path"):
            source.connect(ansible_config)

    def test_reads_hosts_collection_from_inventory_export(self, tmp_path, ansible_config):
        artifact = tmp_path / "ansible-hostvars.json"
        artifact.write_text(
            json.dumps(
                {
                    "_meta": {
                        "hostvars": {
                            "web-01": {
                                "ansible_facts": {
                                    "ansible_hostname": "web-01",
                                    "ansible_fqdn": "web-01.example.com",
                                    "ansible_distribution": "Ubuntu",
                                    "ansible_distribution_version": "24.04",
                                    "ansible_interfaces": ["eth0"],
                                    "ansible_eth0": {
                                        "macaddress": "00:11:22:33:44:55",
                                        "ipv4": {"address": "10.0.0.5"},
                                    },
                                }
                            }
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        ansible_config.extra["artifact_path"] = str(artifact)
        ansible_config.url = ""

        source = AnsibleSource()
        source.connect(ansible_config)
        hosts = source.get_objects("hosts")

        assert len(hosts) == 1
        assert hosts[0]["name"] == "web-01"
        assert hosts[0]["source_system"] == "ansible"
        assert hosts[0]["interfaces"][0]["name"] == "eth0"

    def test_reads_directory_fact_cache(self, tmp_path, ansible_config):
        artifact_dir = tmp_path / "facts"
        artifact_dir.mkdir()
        (artifact_dir / "web-01.json").write_text(
            json.dumps(
                {
                    "inventory_hostname": "web-01",
                    "ansible_facts": {
                        "ansible_hostname": "web-01",
                        "ansible_interfaces": [],
                    },
                }
            ),
            encoding="utf-8",
        )
        ansible_config.extra["artifact_path"] = str(artifact_dir)
        ansible_config.url = ""

        source = AnsibleSource()
        source.connect(ansible_config)

        hosts = source.get_objects("hosts")
        assert [host["name"] for host in hosts] == ["web-01"]
