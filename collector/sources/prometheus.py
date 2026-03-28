"""Prometheus node-exporter data source adapter.

Connects to a Prometheus server and discovers Linux hosts that are being
scraped by node_exporter.  Returns device and interface information as
plain Python dicts.

Supported collections
---------------------
``"nodes"`` — all Linux nodes discovered via ``node_uname_info`` metrics,
              enriched with DMI hardware info, memory, CPU count, and optional
              network interface data.

Each returned node dict includes both normalised convenience fields and
the original Prometheus label values:

Normalised fields
  name           Short hostname (nodename from node_uname_info, falls back
                 to the instance label without port)
  hostname       Full nodename as reported by the OS
  instance       Prometheus instance label (host:port)
  host           Hostname portion of the instance label
  os             Operating system name (e.g. ``"Linux"``)
  kernel         Kernel release string (e.g. ``"5.15.0-91-generic"``)
  machine        CPU architecture (e.g. ``"x86_64"``)
  manufacturer   Hardware vendor (from node_dmi_info sys_vendor)
  model          Product model (from node_dmi_info product_name)
  serial         Product serial number (from node_dmi_info product_serial)
  memory_mb      Total RAM in megabytes (from node_memory_MemTotal_bytes)
  cpu_count      Number of logical CPUs (from node_cpu_seconds_total)
  platform       Platform string, e.g. ``"Linux x86_64"``
  status         Always ``"active"`` for discovered nodes
  job            Prometheus scrape job name
  interfaces     List of interface dicts (see below)

Interface dict fields
  name           Interface name (e.g. ``"eth0"``, ``"ens192"``)
  type           NetBox-compatible interface type slug
  enabled        ``True`` if operstate is ``"up"``
  mac_address    MAC address in upper-case colon notation, or ``""``
  speed          Speed in Mbps (from node_network_speed_bytes), or ``None``
  operstate      Raw operstate string from node_network_info
  duplex         Duplex mode string from node_network_info
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import requests

from .base import DataSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Interface type inference from device name
# ---------------------------------------------------------------------------

#: Each tuple is (regex pattern, NetBox interface type slug).
#: Patterns are matched case-insensitively in order.
_IFACE_TYPE_PATTERNS: list[tuple[str, str]] = [
    (r"^eth\d",    "1000base-t"),
    (r"^ens\d",    "1000base-t"),
    (r"^enp\d",    "1000base-t"),
    (r"^em\d",     "1000base-t"),
    (r"^bond\d",   "lag"),
    (r"^team\d",   "lag"),
    (r"^lo$",      "virtual"),
    (r"^dummy",    "virtual"),
    (r"^virbr",    "virtual"),
    (r"^docker",   "virtual"),
    (r"^br-",      "bridge"),
    (r"^br\d",     "bridge"),
    (r"^veth",     "virtual"),
    (r"^tun\d",    "virtual"),
    (r"^tap\d",    "virtual"),
    (r"^wlan\d",   "ieee802.11a"),
    (r"^wlp\d",    "ieee802.11a"),
    (r"^wlx",      "ieee802.11a"),
    (r"^ib\d",     "infiniband"),
    (r"^ib",       "infiniband"),
]

#: DMI strings that carry no useful information and should be treated as empty.
_DMI_FILLERS: frozenset[str] = frozenset({
    "to be filled by o.e.m.",
    "default string",
    "not specified",
    "unknown",
    "system product name",
    "all series",
    "none",
    "n/a",
})


def _infer_iface_type(name: str) -> str:
    """Infer a NetBox-compatible interface type slug from the device *name*."""
    for pattern, iface_type in _IFACE_TYPE_PATTERNS:
        if re.match(pattern, name, re.IGNORECASE):
            return iface_type
    return "other"


# ---------------------------------------------------------------------------
# PrometheusSource
# ---------------------------------------------------------------------------


class PrometheusSource(DataSource):
    """Prometheus HTTP API-backed source adapter for node-exporter discovery."""

    def __init__(self) -> None:
        self._session: Optional[requests.Session] = None
        self._base_url: str = ""
        self._fetch_interfaces: bool = True

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to Prometheus using settings from *config*."""
        url = config.url
        if not url.startswith(("http://", "https://")):
            url = f"http://{url}"
        self._base_url = url.rstrip("/")

        verify_ssl = config.verify_ssl
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        extra = config.extra or {}
        self._fetch_interfaces = (
            str(extra.get("fetch_interfaces", "true")).lower() == "true"
        )

        self._session = requests.Session()
        self._session.verify = verify_ssl
        self._session.headers.update({"Accept": "application/json"})

        if config.username and config.password:
            self._session.auth = (config.username, config.password)

        logger.info("Connecting to Prometheus: %s", self._base_url)
        self._check_connectivity()
        logger.info("Prometheus connection established: %s", config.url)

    def get_objects(self, collection: str) -> list:
        """Return a flat list of dicts for *collection*."""
        if self._session is None:
            raise RuntimeError("PrometheusSource: connect() has not been called")

        collectors = {
            "nodes": self._get_nodes,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"PrometheusSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Release the HTTP session."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception as exc:
                logger.debug("PrometheusSource session close error: %s", exc)
            finally:
                self._session = None

    # ------------------------------------------------------------------
    # Connectivity check
    # ------------------------------------------------------------------

    def _check_connectivity(self) -> None:
        """Verify that the Prometheus server is reachable.

        First tries the ``/-/healthy`` endpoint (standard Prometheus health
        check), then falls back to a trivial instant query if that endpoint
        is not available.
        """
        try:
            resp = self._session.get(  # type: ignore[union-attr]
                f"{self._base_url}/-/healthy", timeout=10
            )
            resp.raise_for_status()
            return
        except Exception:
            pass

        try:
            resp = self._session.get(  # type: ignore[union-attr]
                f"{self._base_url}/api/v1/query",
                params={"query": "1"},
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to connect to Prometheus at {self._base_url}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Prometheus query helpers
    # ------------------------------------------------------------------

    def _query(self, promql: str) -> list[dict]:
        """Execute an instant PromQL query and return the result list."""
        url = f"{self._base_url}/api/v1/query"
        logger.debug("Prometheus query: %s", promql)
        resp = self._session.get(  # type: ignore[union-attr]
            url, params={"query": promql}, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "success":
            raise RuntimeError(
                f"Prometheus query failed: {data.get('error', 'unknown error')}"
            )
        return data.get("data", {}).get("result", [])

    def _query_labels(self, promql: str) -> dict[str, dict]:
        """Return a mapping of ``instance`` → label-dict for the given *promql*.

        Only the first series per instance is kept; subsequent series for the
        same instance are silently ignored.
        """
        out: dict[str, dict] = {}
        for item in self._query(promql):
            metric = item.get("metric", {})
            instance = metric.get("instance", "")
            if instance and instance not in out:
                out[instance] = metric
        return out

    def _query_value(self, promql: str) -> dict[str, Any]:
        """Return a mapping of ``instance`` → scalar float for the given *promql*."""
        out: dict[str, Any] = {}
        for item in self._query(promql):
            metric = item.get("metric", {})
            instance = metric.get("instance", "")
            value = item.get("value", [])
            if instance and len(value) >= 2:
                try:
                    out[instance] = float(value[1])
                except (ValueError, TypeError):
                    out[instance] = None
        return out

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_nodes(self) -> list[dict]:
        """Fetch all nodes from Prometheus node_exporter metrics."""
        # node_uname_info is required — it provides the list of instances.
        uname_map = self._query_labels("node_uname_info")
        if not uname_map:
            logger.warning(
                "PrometheusSource: no node_uname_info metrics found. "
                "Is node_exporter running and being scraped by Prometheus?"
            )
            return []

        # Optional enrichment queries — failures are logged and silently ignored.
        dmi_map: dict[str, dict] = {}
        mem_map: dict[str, Any] = {}
        cpu_map: dict[str, Any] = {}
        iface_map: dict[str, list[dict]] = {}

        try:
            dmi_map = self._query_labels("node_dmi_info")
        except Exception as exc:
            logger.debug("PrometheusSource: DMI info not available: %s", exc)

        try:
            mem_map = self._query_value("node_memory_MemTotal_bytes")
        except Exception as exc:
            logger.debug("PrometheusSource: memory info not available: %s", exc)

        try:
            cpu_map = self._query_value(
                "count without(cpu, mode)(node_cpu_seconds_total{mode='idle'})"
            )
        except Exception as exc:
            logger.debug("PrometheusSource: CPU count not available: %s", exc)

        if self._fetch_interfaces:
            try:
                iface_map = self._fetch_interface_info()
            except Exception as exc:
                logger.warning(
                    "PrometheusSource: interface info not available: %s", exc
                )

        nodes: list[dict] = []
        for instance, uname in uname_map.items():
            dmi = dmi_map.get(instance, {})
            node = self._enrich_node(instance, uname, dmi, mem_map, cpu_map, iface_map)
            nodes.append(node)

        logger.debug("PrometheusSource: returning %d nodes", len(nodes))
        return nodes

    def _fetch_interface_info(self) -> dict[str, list[dict]]:
        """Return a mapping of ``instance`` → list of interface dicts.

        Fetches ``node_network_info`` for base interface metadata,
        ``node_network_address_info`` for MAC addresses, and
        ``node_network_speed_bytes`` for link speed.
        """
        iface_results = self._query("node_network_info")

        # MAC addresses from node_network_address_info (best-effort)
        mac_map: dict[tuple[str, str], str] = {}
        try:
            for item in self._query("node_network_address_info"):
                metric = item.get("metric", {})
                instance = metric.get("instance", "")
                device = metric.get("device", "")
                mac = metric.get("address", "")
                if instance and device and mac:
                    key = (instance, device)
                    if key not in mac_map:
                        mac_map[key] = mac.upper()
        except Exception as exc:
            logger.debug(
                "PrometheusSource: node_network_address_info not available: %s", exc
            )

        # Speed in Mbps from node_network_speed_bytes (best-effort)
        speed_map: dict[tuple[str, str], Optional[int]] = {}
        try:
            for item in self._query("node_network_speed_bytes"):
                metric = item.get("metric", {})
                instance = metric.get("instance", "")
                device = metric.get("device", "")
                value = item.get("value", [])
                if instance and device and len(value) >= 2:
                    try:
                        # node_network_speed_bytes is bytes/sec → convert to Mbps
                        speed_map[(instance, device)] = int(
                            float(value[1]) * 8 / 1_000_000
                        )
                    except (ValueError, TypeError):
                        pass
        except Exception as exc:
            logger.debug(
                "PrometheusSource: node_network_speed_bytes not available: %s", exc
            )

        # Group interfaces by instance
        out: dict[str, list[dict]] = {}
        for item in iface_results:
            metric = item.get("metric", {})
            instance = metric.get("instance", "")
            device = metric.get("device", "")
            if not instance or not device:
                continue
            operstate = metric.get("operstate", "unknown")
            iface = {
                "name":        device,
                "type":        _infer_iface_type(device),
                "enabled":     operstate.lower() == "up",
                "mac_address": mac_map.get((instance, device), ""),
                "speed":       speed_map.get((instance, device)),
                "operstate":   operstate,
                "duplex":      metric.get("duplex", ""),
            }
            out.setdefault(instance, []).append(iface)

        return out

    def _enrich_node(
        self,
        instance: str,
        uname: dict,
        dmi: dict,
        mem_map: dict,
        cpu_map: dict,
        iface_map: dict,
    ) -> dict:
        """Return a normalised dict for a single Prometheus node_exporter host."""
        nodename = uname.get("nodename", "")
        kernel   = uname.get("release", "")
        machine  = uname.get("machine", "")
        sysname  = uname.get("sysname", "Linux")
        job      = uname.get("job", "")

        short_name = _short_name(nodename or instance)

        manufacturer = _clean_dmi(dmi.get("sys_vendor", ""))
        model        = _clean_dmi(dmi.get("product_name", ""))
        serial       = dmi.get("product_serial", "")
        # Treat serial filler strings as empty
        if serial.strip().lower() in _DMI_FILLERS:
            serial = ""

        memory_bytes = mem_map.get(instance)
        memory_mb    = int(memory_bytes / (1024 * 1024)) if memory_bytes else None

        cpu_val   = cpu_map.get(instance)
        cpu_count = int(cpu_val) if cpu_val is not None else None

        platform = f"{sysname} {machine}".strip() if machine else sysname

        return {
            # --- normalised convenience fields ---
            "name":         short_name,
            "hostname":     nodename,
            "instance":     instance,
            "host":         _host_from_instance(instance),
            "os":           sysname,
            "kernel":       kernel,
            "machine":      machine,
            "manufacturer": manufacturer,
            "model":        model,
            "serial":       serial,
            "memory_mb":    memory_mb,
            "cpu_count":    cpu_count,
            "platform":     platform,
            "status":       "active",
            "job":          job,
            # --- interfaces (empty list when fetch_interfaces is False) ---
            "interfaces":   iface_map.get(instance, []),
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _host_from_instance(instance: str) -> str:
    """Return the hostname/IP portion of a Prometheus instance label.

    Strips the port number so that ``"host.example.com:9100"`` returns
    ``"host.example.com"``.  IPv6 bracket notation is also handled:
    ``"[::1]:9100"`` returns ``"::1"``.
    """
    if not instance:
        return ""
    # IPv6 bracketed form: [addr]:port
    m = re.match(r"^\[([^\]]+)\](?::\d+)?$", instance)
    if m:
        return m.group(1)
    # host:port
    if ":" in instance:
        return instance.rsplit(":", 1)[0]
    return instance


def _short_name(name: str) -> str:
    """Return the short hostname from *name*, max 64 chars.

    For hostnames, strips the domain portion (e.g. ``"web-01.example.com"``
    → ``"web-01"``).  For raw IP addresses the full address is kept as-is
    since splitting on ``.`` would truncate to the first octet.
    """
    if not name:
        return "Unknown"
    host = _host_from_instance(name)
    # Preserve IP addresses as-is (avoid splitting 10.0.0.1 → "10")
    if re.match(r"^\d{1,3}(?:\.\d{1,3}){3}$", host) or ":" in host:
        return host[:64] or "Unknown"
    short = host.split(".")[0]
    return short[:64] or "Unknown"


def _clean_dmi(value: str) -> str:
    """Return *value* cleaned of common DMI filler strings."""
    if not value:
        return ""
    if value.strip().lower() in _DMI_FILLERS:
        return ""
    return value.strip()
