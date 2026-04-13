"""Tests for the Salt grains artifact source adapter."""

from __future__ import annotations

import json

import pytest
import requests

from collector.sources.salt import (
    SaltSource,
    _clean_ip_list,
    _infer_iface_type,
    _iter_records,
    _normalise_host,
)


class _FakeResponse:
    def __init__(self, payload, status_code=200, json_error=False):
        self._payload = payload
        self.status_code = status_code
        self._json_error = json_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        if self._json_error:
            raise ValueError("bad json")
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.verify = True
        self.headers = {}
        self.requests = []
        self.closed = False

    def post(self, url, **kwargs):
        self.requests.append({"url": url, **kwargs})
        if not self._responses:
            raise AssertionError("No fake response queued for session.post()")
        return self._responses.pop(0)

    def close(self):
        self.closed = True


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

    def test_tolerates_non_dict_interface_grains(self):
        host = _normalise_host(
            "minion-1",
            {
                "grains": {
                    "host": "web-01",
                    "ip_interfaces": None,
                    "hwaddr_interfaces": ["bad-shape"],
                }
            },
        )

        assert host["interfaces"] == []


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

    def test_reports_invalid_json_with_artifact_path(self, tmp_path, salt_config):
        artifact = tmp_path / "salt-grains.json"
        artifact.write_text("{not-json", encoding="utf-8")
        salt_config.extra["artifact_path"] = str(artifact)
        salt_config.url = ""

        source = SaltSource()
        source.connect(salt_config)

        with pytest.raises(ValueError, match="contains invalid JSON"):
            source.get_objects("hosts")

    def test_reads_hosts_collection_from_salt_master(self, monkeypatch, salt_config):
        fake_session = _FakeSession(
            [
                _FakeResponse({"return": [{"token": "salt-token"}]}),
                _FakeResponse(
                    {
                        "return": [
                            {
                                "minion-1": {
                                    "host": "web-01",
                                    "fqdn": "web-01.example.com",
                                    "os": "Ubuntu",
                                    "osrelease": "24.04",
                                    "ip_interfaces": {"eth0": ["10.0.0.5"]},
                                    "hwaddr_interfaces": {
                                        "eth0": "00:11:22:33:44:55",
                                    },
                                }
                            }
                        ]
                    }
                ),
            ]
        )
        monkeypatch.setattr(
            "collector.sources.salt.requests.Session",
            lambda: fake_session,
        )
        salt_config.url = "https://salt-master.example.com"
        salt_config.username = "salt-user"
        salt_config.password = "salt-pass"
        salt_config.extra = {
            "mode": "master",
            "eauth": "pam",
            "target": "G@roles:web",
            "expr_form": "compound",
        }

        source = SaltSource()
        source.connect(salt_config)
        hosts = source.get_objects("hosts")

        assert hosts[0]["name"] == "web-01"
        assert hosts[0]["source_system"] == "salt"
        assert fake_session.headers["X-Auth-Token"] == "salt-token"
        assert fake_session.requests[0]["url"].endswith("/login")
        assert fake_session.requests[0]["json"]["username"] == "salt-user"
        assert fake_session.requests[1]["url"].endswith("/run")
        assert fake_session.requests[1]["json"] == [
            {
                "client": "local",
                "tgt": "G@roles:web",
                "fun": "grains.items",
                "expr_form": "compound",
            }
        ]

    def test_supports_token_auth_for_salt_master(self, monkeypatch, salt_config):
        fake_session = _FakeSession(
            [
                _FakeResponse(
                    {
                        "return": {
                            "minion-1": {
                                "host": "web-01",
                                "ip_interfaces": {"eth0": ["10.0.0.5"]},
                            }
                        }
                    }
                )
            ]
        )
        monkeypatch.setattr(
            "collector.sources.salt.requests.Session",
            lambda: fake_session,
        )
        salt_config.url = "https://salt-master.example.com"
        salt_config.extra = {
            "mode": "master",
            "auth_type": "token",
            "auth_token": "preissued-token",
        }

        source = SaltSource()
        source.connect(salt_config)
        hosts = source.get_objects("hosts")

        assert len(hosts) == 1
        assert fake_session.headers["X-Auth-Token"] == "preissued-token"
        assert len(fake_session.requests) == 1
        assert fake_session.requests[0]["url"].endswith("/run")

    def test_raises_when_salt_login_response_has_no_token(
        self, monkeypatch, salt_config
    ):
        fake_session = _FakeSession([_FakeResponse({"return": [{"user": "salt-user"}]})])
        monkeypatch.setattr(
            "collector.sources.salt.requests.Session",
            lambda: fake_session,
        )
        salt_config.url = "https://salt-master.example.com"
        salt_config.username = "salt-user"
        salt_config.password = "salt-pass"
        salt_config.extra = {"mode": "master"}

        source = SaltSource()

        with pytest.raises(ValueError, match="did not include a token"):
            source.connect(salt_config)
