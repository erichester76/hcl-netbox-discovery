"""Tests for the Prometheus node-exporter source adapter
(collector/sources/prometheus.py).

All HTTP calls are mocked — no real Prometheus server is required.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from collector.sources.prometheus import (
    PrometheusSource,
    _clean_dmi,
    _host_from_instance,
    _infer_iface_type,
    _short_name,
)


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

def _make_query_response(results: list) -> MagicMock:
    """Return a mock requests.Response for a successful Prometheus query."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {
        "status": "success",
        "data": {"resultType": "vector", "result": results},
    }
    return resp


def _uname_series(instance: str, nodename: str, release: str = "5.15.0",
                  machine: str = "x86_64", sysname: str = "Linux",
                  job: str = "node") -> dict:
    return {
        "metric": {
            "instance": instance,
            "job": job,
            "nodename": nodename,
            "release": release,
            "machine": machine,
            "sysname": sysname,
        },
        "value": [1700000000, "1"],
    }


def _dmi_series(instance: str, sys_vendor: str = "Dell Inc.",
                product_name: str = "PowerEdge R640",
                product_serial: str = "SVC1234") -> dict:
    return {
        "metric": {
            "instance": instance,
            "sys_vendor": sys_vendor,
            "product_name": product_name,
            "product_serial": product_serial,
        },
        "value": [1700000000, "1"],
    }


def _mem_series(instance: str, bytes_val: float) -> dict:
    return {
        "metric": {"instance": instance},
        "value": [1700000000, str(bytes_val)],
    }


def _cpu_series(instance: str, count: int) -> dict:
    return {
        "metric": {"instance": instance},
        "value": [1700000000, str(count)],
    }


def _iface_series(instance: str, device: str, operstate: str = "up",
                  duplex: str = "full") -> dict:
    return {
        "metric": {
            "instance": instance,
            "device": device,
            "operstate": operstate,
            "duplex": duplex,
        },
        "value": [1700000000, "1"],
    }


def _speed_series(instance: str, device: str, bytes_per_sec: float) -> dict:
    return {
        "metric": {"instance": instance, "device": device},
        "value": [1700000000, str(bytes_per_sec)],
    }


def _mac_series(instance: str, device: str, address: str) -> dict:
    return {
        "metric": {"instance": instance, "device": device, "address": address},
        "value": [1700000000, "1"],
    }


# ---------------------------------------------------------------------------
# _host_from_instance()
# ---------------------------------------------------------------------------


class TestHostFromInstance:
    @pytest.mark.parametrize(
        "instance, expected",
        [
            ("host.example.com:9100", "host.example.com"),
            ("10.0.0.1:9100",         "10.0.0.1"),
            ("[::1]:9100",             "::1"),
            ("[2001:db8::1]:9100",     "2001:db8::1"),
            ("hostname",               "hostname"),
            ("",                       ""),
        ],
    )
    def test_host_from_instance(self, instance, expected):
        assert _host_from_instance(instance) == expected


# ---------------------------------------------------------------------------
# _short_name()
# ---------------------------------------------------------------------------


class TestShortName:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("web-01.example.com",     "web-01"),
            ("web-01.example.com:9100","web-01"),
            ("web-01",                 "web-01"),
            ("10.0.0.1:9100",          "10.0.0.1"),
            ("",                       "Unknown"),
        ],
    )
    def test_short_name(self, name, expected):
        assert _short_name(name) == expected

    def test_truncated_to_64_chars(self):
        long_name = "a" * 100 + ".example.com"
        assert len(_short_name(long_name)) <= 64


# ---------------------------------------------------------------------------
# _infer_iface_type()
# ---------------------------------------------------------------------------


class TestInferIfaceType:
    @pytest.mark.parametrize(
        "name, expected",
        [
            ("eth0",      "1000base-t"),
            ("eth1",      "1000base-t"),
            ("ens192",    "1000base-t"),
            ("enp3s0",    "1000base-t"),
            ("em1",       "1000base-t"),
            ("bond0",     "lag"),
            ("team0",     "lag"),
            ("lo",        "virtual"),
            ("dummy0",    "virtual"),
            ("virbr0",    "virtual"),
            ("docker0",   "virtual"),
            ("br0",       "bridge"),
            ("br-abc123", "bridge"),
            ("veth0",     "virtual"),
            ("tun0",      "virtual"),
            ("tap0",      "virtual"),
            ("wlan0",     "ieee802.11a"),
            ("wlp2s0",    "ieee802.11a"),
            ("ib0",       "infiniband"),
            ("unknown99", "other"),
        ],
    )
    def test_infer_iface_type(self, name, expected):
        assert _infer_iface_type(name) == expected


# ---------------------------------------------------------------------------
# _clean_dmi()
# ---------------------------------------------------------------------------


class TestCleanDmi:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("Dell Inc.",                    "Dell Inc."),
            ("  Dell Inc.  ",               "Dell Inc."),
            ("To Be Filled By O.E.M.",       ""),
            ("Default string",               ""),
            ("Not Specified",                ""),
            ("Unknown",                      ""),
            ("System Product Name",          ""),
            ("",                             ""),
        ],
    )
    def test_clean_dmi(self, value, expected):
        assert _clean_dmi(value) == expected


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestPrometheusConnect:
    def _make_healthy_session(self) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        session = MagicMock()
        session.get.return_value = resp
        session.headers = {}
        return session

    def test_connect_succeeds_with_healthy_endpoint(self, prometheus_config):
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = self._make_healthy_session()
            MockSession.return_value = session
            src.connect(prometheus_config)
        assert src._session is session

    def test_connect_prepends_http_if_missing(self, prometheus_config):
        prometheus_config.url = "prometheus.example.com:9090"
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = self._make_healthy_session()
            MockSession.return_value = session
            src.connect(prometheus_config)
        assert src._base_url.startswith("http://")

    def test_connect_keeps_https_scheme(self, prometheus_config):
        prometheus_config.url = "https://prometheus.example.com"
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = self._make_healthy_session()
            MockSession.return_value = session
            src.connect(prometheus_config)
        assert src._base_url.startswith("https://")

    def test_connect_sets_basic_auth_when_credentials_provided(self, prometheus_config):
        prometheus_config.username = "user"
        prometheus_config.password = "pass"
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = self._make_healthy_session()
            MockSession.return_value = session
            src.connect(prometheus_config)
        assert session.auth == ("user", "pass")

    def test_connect_no_auth_when_credentials_empty(self, prometheus_config):
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = self._make_healthy_session()
            MockSession.return_value = session
            src.connect(prometheus_config)
        assert not hasattr(session, "auth") or session.auth != ("", "")

    def test_fetch_interfaces_flag_true(self, prometheus_config):
        prometheus_config.extra = {"fetch_interfaces": "true"}
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            MockSession.return_value = self._make_healthy_session()
            src.connect(prometheus_config)
        assert src._fetch_interfaces is True

    def test_fetch_interfaces_flag_false(self, prometheus_config):
        prometheus_config.extra = {"fetch_interfaces": "false"}
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            MockSession.return_value = self._make_healthy_session()
            src.connect(prometheus_config)
        assert src._fetch_interfaces is False

    def test_connect_falls_back_to_api_query_when_healthy_unavailable(
        self, prometheus_config
    ):
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = MagicMock()
            session.headers = {}

            healthy_resp = MagicMock()
            healthy_resp.raise_for_status.side_effect = Exception("404")

            api_resp = MagicMock()
            api_resp.raise_for_status = MagicMock()
            api_resp.json.return_value = {
                "status": "success",
                "data": {"resultType": "scalar", "result": []},
            }

            session.get.side_effect = [healthy_resp, api_resp]
            MockSession.return_value = session
            src.connect(prometheus_config)  # should not raise

    def test_connect_raises_when_all_endpoints_fail(self, prometheus_config):
        src = PrometheusSource()
        with patch("collector.sources.prometheus.requests.Session") as MockSession:
            session = MagicMock()
            session.headers = {}
            session.get.side_effect = Exception("connection refused")
            MockSession.return_value = session
            with pytest.raises(RuntimeError, match="Failed to connect to Prometheus"):
                src.connect(prometheus_config)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestPrometheusGetObjects:
    def _connected_source(self, fetch_interfaces: bool = False) -> PrometheusSource:
        src = PrometheusSource()
        src._session = MagicMock()
        src._base_url = "http://prometheus.example.com:9090"
        src._fetch_interfaces = fetch_interfaces
        return src

    def test_raises_without_connect(self):
        src = PrometheusSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("nodes")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("switches")

    def test_get_nodes_returns_enriched_dicts(self):
        src = self._connected_source()

        uname = _uname_series(
            "web-01.example.com:9100", "web-01.example.com",
            release="5.15.0-91-generic", machine="x86_64",
        )
        dmi = _dmi_series("web-01.example.com:9100")
        mem = _mem_series("web-01.example.com:9100", 8 * 1024 ** 3)  # 8 GiB
        cpu = _cpu_series("web-01.example.com:9100", 4)

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_dmi_info" in query:
                return _make_query_response([dmi])
            if "node_memory_MemTotal_bytes" in query:
                return _make_query_response([mem])
            if "node_cpu_seconds_total" in query:
                return _make_query_response([cpu])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")

        assert len(result) == 1
        node = result[0]
        assert node["name"] == "web-01"
        assert node["hostname"] == "web-01.example.com"
        assert node["instance"] == "web-01.example.com:9100"
        assert node["host"] == "web-01.example.com"
        assert node["os"] == "Linux"
        assert node["kernel"] == "5.15.0-91-generic"
        assert node["machine"] == "x86_64"
        assert node["manufacturer"] == "Dell Inc."
        assert node["model"] == "PowerEdge R640"
        assert node["serial"] == "SVC1234"
        assert node["memory_mb"] == 8192
        assert node["cpu_count"] == 4
        assert node["platform"] == "Linux x86_64"
        assert node["status"] == "active"
        assert node["interfaces"] == []

    def test_get_nodes_returns_empty_when_no_uname_info(self):
        src = self._connected_source()

        def side_effect(url, **kwargs):
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert result == []

    def test_get_nodes_handles_missing_dmi_info(self):
        src = self._connected_source()

        uname = _uname_series("db-01:9100", "db-01")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_dmi_info" in query:
                raise Exception("metric not found")
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert len(result) == 1
        node = result[0]
        assert node["manufacturer"] == ""
        assert node["model"] == ""
        assert node["serial"] == ""

    def test_get_nodes_handles_missing_memory_info(self):
        src = self._connected_source()

        uname = _uname_series("db-01:9100", "db-01")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_memory_MemTotal_bytes" in query:
                raise Exception("metric not found")
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert result[0]["memory_mb"] is None

    def test_get_nodes_handles_missing_cpu_info(self):
        src = self._connected_source()

        uname = _uname_series("db-01:9100", "db-01")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_cpu_seconds_total" in query:
                raise Exception("metric not found")
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert result[0]["cpu_count"] is None

    def test_get_nodes_with_interfaces(self):
        src = self._connected_source(fetch_interfaces=True)

        uname = _uname_series("app-01:9100", "app-01")
        iface = _iface_series("app-01:9100", "eth0", operstate="up", duplex="full")
        mac = _mac_series("app-01:9100", "eth0", "aa:bb:cc:dd:ee:ff")
        speed = _speed_series("app-01:9100", "eth0", 125_000_000)  # 1 Gbps in bytes/s

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_network_info" in query:
                return _make_query_response([iface])
            if "node_network_address_info" in query:
                return _make_query_response([mac])
            if "node_network_speed_bytes" in query:
                return _make_query_response([speed])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert len(result) == 1
        interfaces = result[0]["interfaces"]
        assert len(interfaces) == 1
        iface_dict = interfaces[0]
        assert iface_dict["name"] == "eth0"
        assert iface_dict["type"] == "1000base-t"
        assert iface_dict["enabled"] is True
        assert iface_dict["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert iface_dict["speed"] == 1000  # 1 Gbps = 1000 Mbps

    def test_get_nodes_interface_fetch_error_returns_empty_list(self):
        src = self._connected_source(fetch_interfaces=True)

        uname = _uname_series("app-02:9100", "app-02")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_network_info" in query:
                raise Exception("timeout")
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert result[0]["interfaces"] == []

    def test_get_nodes_multiple_instances(self):
        src = self._connected_source()

        uname1 = _uname_series("host-01:9100", "host-01")
        uname2 = _uname_series("host-02:9100", "host-02")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname1, uname2])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        assert len(result) == 2
        names = {node["name"] for node in result}
        assert names == {"host-01", "host-02"}

    def test_get_nodes_no_interfaces_when_flag_false(self):
        src = self._connected_source(fetch_interfaces=False)

        uname = _uname_series("app-03:9100", "app-03")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        # interfaces key is present but empty when fetch_interfaces is False
        assert result[0]["interfaces"] == []

    def test_get_nodes_dmi_filler_strings_cleaned(self):
        src = self._connected_source()

        uname = _uname_series("vm-01:9100", "vm-01")
        dmi = _dmi_series(
            "vm-01:9100",
            sys_vendor="To Be Filled By O.E.M.",
            product_name="System Product Name",
            product_serial="Default string",
        )

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_uname_info" in query:
                return _make_query_response([uname])
            if "node_dmi_info" in query:
                return _make_query_response([dmi])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src.get_objects("nodes")
        node = result[0]
        assert node["manufacturer"] == ""
        assert node["model"] == ""
        assert node["serial"] == ""


# ---------------------------------------------------------------------------
# _enrich_node() edge cases
# ---------------------------------------------------------------------------


class TestPrometheusEnrichNode:
    def _src(self) -> PrometheusSource:
        src = PrometheusSource()
        return src

    def test_empty_nodename_falls_back_to_instance(self):
        src = self._src()
        node = src._enrich_node(
            "10.0.0.1:9100",
            {"nodename": "", "release": "", "machine": "", "sysname": "Linux"},
            {},
            {},
            {},
            {},
        )
        assert node["name"] == "10.0.0.1"

    def test_platform_includes_architecture(self):
        src = self._src()
        node = src._enrich_node(
            "host:9100",
            {"nodename": "host", "release": "5.15.0", "machine": "x86_64", "sysname": "Linux"},
            {},
            {},
            {},
            {},
        )
        assert node["platform"] == "Linux x86_64"

    def test_platform_without_machine(self):
        src = self._src()
        node = src._enrich_node(
            "host:9100",
            {"nodename": "host", "release": "5.15.0", "machine": "", "sysname": "Linux"},
            {},
            {},
            {},
            {},
        )
        assert node["platform"] == "Linux"

    def test_memory_mb_calculated_correctly(self):
        src = self._src()
        node = src._enrich_node(
            "host:9100",
            {"nodename": "host", "release": "", "machine": "", "sysname": "Linux"},
            {},
            {"host:9100": 16 * 1024 ** 3},  # 16 GiB in bytes
            {},
            {},
        )
        assert node["memory_mb"] == 16384

    def test_cpu_count_integer(self):
        src = self._src()
        node = src._enrich_node(
            "host:9100",
            {"nodename": "host", "release": "", "machine": "", "sysname": "Linux"},
            {},
            {},
            {"host:9100": 8.0},
            {},
        )
        assert node["cpu_count"] == 8

    def test_status_always_active(self):
        src = self._src()
        node = src._enrich_node(
            "host:9100",
            {"nodename": "host", "release": "", "machine": "", "sysname": "Linux"},
            {},
            {},
            {},
            {},
        )
        assert node["status"] == "active"


# ---------------------------------------------------------------------------
# _fetch_interface_info()
# ---------------------------------------------------------------------------


class TestPrometheusFetchInterfaceInfo:
    def _src(self) -> PrometheusSource:
        src = PrometheusSource()
        src._session = MagicMock()
        src._base_url = "http://prometheus.example.com:9090"
        return src

    def test_interface_grouped_by_instance(self):
        src = self._src()

        iface1 = _iface_series("host-01:9100", "eth0", operstate="up")
        iface2 = _iface_series("host-02:9100", "eth0", operstate="down")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_network_info" in query:
                return _make_query_response([iface1, iface2])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src._fetch_interface_info()
        assert "host-01:9100" in result
        assert "host-02:9100" in result
        assert result["host-01:9100"][0]["enabled"] is True
        assert result["host-02:9100"][0]["enabled"] is False

    def test_speed_converted_to_mbps(self):
        src = self._src()

        iface = _iface_series("host-01:9100", "eth0")
        speed = _speed_series("host-01:9100", "eth0", 1_250_000_000)  # 10 Gbps

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_network_info" in query:
                return _make_query_response([iface])
            if "node_network_speed_bytes" in query:
                return _make_query_response([speed])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src._fetch_interface_info()
        assert result["host-01:9100"][0]["speed"] == 10000  # 10 Gbps = 10000 Mbps

    def test_mac_uppercased(self):
        src = self._src()

        iface = _iface_series("host-01:9100", "eth0")
        mac = _mac_series("host-01:9100", "eth0", "aa:bb:cc:dd:ee:ff")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_network_info" in query:
                return _make_query_response([iface])
            if "node_network_address_info" in query:
                return _make_query_response([mac])
            if "node_network_speed_bytes" in query:
                return _make_query_response([])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src._fetch_interface_info()
        assert result["host-01:9100"][0]["mac_address"] == "AA:BB:CC:DD:EE:FF"

    def test_missing_speed_and_mac_gracefully_handled(self):
        src = self._src()

        iface = _iface_series("host-01:9100", "eth0")

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_network_info" in query:
                return _make_query_response([iface])
            if "node_network_address_info" in query:
                raise Exception("no data")
            if "node_network_speed_bytes" in query:
                raise Exception("no data")
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src._fetch_interface_info()
        iface_dict = result["host-01:9100"][0]
        assert iface_dict["mac_address"] == ""
        assert iface_dict["speed"] is None

    def test_items_without_device_label_skipped(self):
        src = self._src()

        # Series without a device label
        bad_item = {
            "metric": {"instance": "host-01:9100"},
            "value": [1700000000, "1"],
        }

        def side_effect(url, **kwargs):
            query = kwargs.get("params", {}).get("query", "")
            if "node_network_info" in query:
                return _make_query_response([bad_item])
            return _make_query_response([])

        src._session.get.side_effect = side_effect

        result = src._fetch_interface_info()
        assert result == {}


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestPrometheusClose:
    def test_close_clears_session(self):
        src = PrometheusSource()
        src._session = MagicMock()
        src.close()
        assert src._session is None

    def test_close_is_safe_when_not_connected(self):
        src = PrometheusSource()
        src.close()  # should not raise
        assert src._session is None

    def test_close_handles_session_error(self):
        src = PrometheusSource()
        mock_session = MagicMock()
        mock_session.close.side_effect = Exception("network error")
        src._session = mock_session
        src.close()  # should not raise
        assert src._session is None


# ---------------------------------------------------------------------------
# _query() error handling
# ---------------------------------------------------------------------------


class TestPrometheusQuery:
    def _src(self) -> PrometheusSource:
        src = PrometheusSource()
        src._session = MagicMock()
        src._base_url = "http://prometheus.example.com:9090"
        return src

    def test_query_raises_on_prometheus_error_status(self):
        src = self._src()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"status": "error", "error": "bad query"}
        src._session.get.return_value = resp

        with pytest.raises(RuntimeError, match="bad query"):
            src._query("invalid{query")

    def test_query_returns_empty_list_on_no_results(self):
        src = self._src()
        resp = _make_query_response([])
        src._session.get.return_value = resp

        result = src._query("node_uname_info")
        assert result == []
