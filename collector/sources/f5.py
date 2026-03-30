"""F5 BIG-IP iControl REST API data source adapter.

Uses the iControl REST API to connect to an F5 BIG-IP device and return
device inventory, network interfaces, self IPs, and virtual servers as plain
Python dicts.

Supported collections
---------------------
``"devices"``         – The BIG-IP appliance itself, optionally including
                        embedded interface and self-IP lists when
                        ``fetch_interfaces`` is enabled.
``"virtual_servers"`` – LTM virtual servers (VIPs) configured on the device.

Device dict fields
------------------
Normalised fields
  name            Hostname (domain stripped, max 64 chars)
  model           Platform model name (e.g. ``"BIG-IP i7800"``)
  manufacturer    Always ``"F5 Networks"``
  serial          Chassis serial number (upper-cased)
  status          Always ``"active"``
  mgmt_address    Management IP address (without prefix length)
  platform_name   ``"BIG-IP {version}"`` (e.g. ``"BIG-IP 16.1.3"``)
  interfaces      List of interface dicts (when fetch_interfaces enabled)

Raw passthrough fields
  hostname, platformId, chassisId, product, version

Interface dict fields (when fetch_interfaces enabled)
  name            Interface name (e.g. ``"1.1"`` or ``"mgmt"``)
  type            NetBox-compatible interface type slug
  enabled         ``True`` if the interface is up
  mac_address     MAC address (upper-cased)
  description     Interface description
  mtu             MTU value (integer)
  speed           Speed in Mbps (integer or ``None``)
  ip_addresses    List of self-IP dicts assigned to this interface
  mediaActive     Raw F5 media type string (passthrough)

Self-IP dict fields (nested inside interface)
  address         IP address with prefix length (e.g. ``"10.0.0.1/24"``)
  status          Always ``"active"``

Virtual server dict fields
  name            Virtual server name
  full_name       Fully-qualified path (e.g. ``"/Common/vs_http"``)
  destination     Destination ``"IP:port"`` (partition prefix stripped)
  pool            Associated pool name (partition prefix stripped)
  protocol        IP protocol (e.g. ``"tcp"``)
  status          ``"active"`` or ``"offline"``
  description     Description
  partition       Partition name (e.g. ``"Common"``)
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests

from .base import DataSource
from .utils import close_http_session, disable_ssl_warnings, safe_get

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Interface type helpers
# ---------------------------------------------------------------------------

# Media-type prefix → NetBox interface type slug.
# The F5 mediaActive string (e.g. "10000SR-FD") is upper-cased and the
# "-FD"/"-HD" duplex suffix is stripped before matching.
_MEDIA_TO_IFACE_TYPE: list[tuple[str, str]] = [
    ("100000",  "100gbase-x-qsfp28"),
    ("40000",   "40gbase-x-qsfpp"),
    ("25000",   "25gbase-x-sfp28"),
    ("10000T",  "10gbase-t"),
    ("10000",   "10gbase-x-sfpp"),
    ("1000SX",  "1000base-x-sfp"),
    ("1000LX",  "1000base-x-sfp"),
    ("1000T",   "1000base-t"),
    ("100TX",   "100base-tx"),
    ("100FX",   "100base-fx"),
]

# Speed thresholds (Mbps) → NetBox interface type slug (fallback when media
# type cannot be matched).
_SPEED_TO_IFACE_TYPE: list[tuple[int, str]] = [
    (100_000, "100gbase-x-qsfp28"),
    (40_000,  "40gbase-x-qsfpp"),
    (25_000,  "25gbase-x-sfp28"),
    (10_000,  "10gbase-x-sfpp"),
    (1_000,   "1000base-t"),
    (100,     "100base-tx"),
]

# F5 internal platform ID → human-readable model string.
_PLATFORM_ID_MAP: dict[str, str] = {
    "Z100":  "BIG-IP 2000",
    "Z101":  "BIG-IP 4000",
    "Z102":  "BIG-IP 5000",
    "Z147":  "BIG-IP i2600",
    "Z100X": "BIG-IP i4600",
    "Z107":  "BIG-IP i5600",
    "Z100G": "BIG-IP i5800",
    "Z101G": "BIG-IP i7800",
    "Z116":  "BIG-IP i10800",
    "Z117":  "BIG-IP i11800",
    "Z120":  "BIG-IP i15800",
    "A109":  "BIG-IP Virtual Edition",
    "Z99":   "BIG-IP Virtual Edition",
}


def _normalize_iface_type(media_active: str, speed_mbps: int | None = None) -> str:
    """Return a NetBox interface type slug for *media_active*.

    Falls back to a speed-based lookup when *media_active* cannot be matched,
    and returns ``"other"`` when neither input yields a result.
    """
    if media_active:
        base = re.sub(r"[-](FD|HD)$", "", media_active.strip().upper())
        for prefix, slug in _MEDIA_TO_IFACE_TYPE:
            if base.startswith(prefix):
                return slug
    if speed_mbps:
        for threshold, slug in _SPEED_TO_IFACE_TYPE:
            if speed_mbps >= threshold:
                return slug
    return "other"


def _parse_media_speed_mbps(media_str: str) -> int | None:
    """Extract speed in Mbps from an F5 media type string.

    Examples: ``"10000SR-FD"`` → ``10000``, ``"1000T-FD"`` → ``1000``.
    Returns ``None`` when the string cannot be parsed.
    """
    if not media_str:
        return None
    m = re.match(r"(\d+)", media_str.strip())
    if m:
        return int(m.group(1))
    return None


def _map_platform_id(platform_id: str) -> str:
    """Return a human-readable model name for *platform_id*.

    Returns an empty string when *platform_id* is falsy so that callers can
    easily fall back to other sources.
    """
    if not platform_id:
        return ""
    return _PLATFORM_ID_MAP.get(platform_id, f"BIG-IP {platform_id}")


def _strip_partition(path: str) -> str:
    """Strip a leading F5 partition prefix from *path*.

    ``"/Common/vs_http"`` → ``"vs_http"``
    ``"vs_http"`` → ``"vs_http"``
    """
    if path.startswith("/"):
        parts = path.lstrip("/").split("/", 1)
        return parts[-1] if len(parts) > 1 else path.lstrip("/")
    return path


# ---------------------------------------------------------------------------
# F5Source
# ---------------------------------------------------------------------------


class F5Source(DataSource):
    """F5 BIG-IP iControl REST API-backed source adapter."""

    def __init__(self) -> None:
        self._session: requests.Session | None = None
        self._base_url: str = ""
        self._fetch_interfaces: bool = False

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to F5 BIG-IP using settings from *config*."""
        url = config.url
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        self._base_url = url.rstrip("/")

        verify_ssl = config.verify_ssl
        if not verify_ssl:
            disable_ssl_warnings()

        if not config.username or not config.password:
            raise RuntimeError(
                "F5 authentication requires both username and password. "
                "Ensure F5_USER and F5_PASS environment variables are set."
            )

        extra = config.extra or {}
        self._fetch_interfaces = str(extra.get("fetch_interfaces", "false")).lower() == "true"

        self._session = requests.Session()
        self._session.verify = verify_ssl
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })

        logger.info("Connecting to F5 BIG-IP: %s", self._base_url)
        self._authenticate(config.username, config.password)
        logger.info("F5 BIG-IP connection established: %s", config.url)

    def get_objects(self, collection: str) -> list:
        """Return a flat list of dicts for *collection*."""
        if self._session is None:
            raise RuntimeError("F5Source: connect() has not been called")

        collectors = {
            "devices":         self._get_devices,
            "virtual_servers": self._get_virtual_servers,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"F5Source: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Release the HTTP session."""
        self._session = close_http_session(self._session, "F5Source")

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _authenticate(self, username: str, password: str) -> None:
        """Obtain a session token and attach it to the session headers.

        Tries the iControl REST token endpoint first; falls back to HTTP Basic
        auth when token auth is unavailable (e.g. older BIG-IP versions).
        """
        try:
            resp = self._session.post(  # type: ignore[union-attr]
                f"{self._base_url}/mgmt/shared/authn/login",
                json={
                    "username":          username,
                    "password":          password,
                    "loginProviderName": "tmos",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            token_data = data.get("token", {})
            token = (
                token_data.get("token")
                if isinstance(token_data, dict)
                else token_data
            )
            if token and isinstance(token, str):
                self._session.headers["X-F5-Auth-Token"] = token  # type: ignore[union-attr]
                logger.debug("F5: authenticated via iControl REST token")
                return
        except Exception as exc:
            logger.debug("F5: token auth failed, falling back to basic auth: %s", exc)

        # Basic auth fallback — remove Content-Type so GETs stay clean
        self._session.auth = (username, password)  # type: ignore[union-attr]
        logger.debug("F5: using basic auth")

    # ------------------------------------------------------------------
    # Low-level GET helper
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        """Perform an authenticated GET and return parsed JSON."""
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        logger.debug("F5 GET %s", url)
        resp = self._session.get(url, timeout=30)  # type: ignore[union-attr]
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Device collection
    # ------------------------------------------------------------------

    def _get_devices(self) -> list[dict]:
        """Return the BIG-IP device as a single-element normalised list."""
        device_info = self._fetch_device_info()
        version     = self._fetch_version()
        mgmt_ip     = self._fetch_mgmt_ip()
        hw_info     = self._fetch_hardware_info()

        hostname = device_info.get("hostname", "") or ""
        name     = (hostname.split(".")[0] if hostname else "")[:64] or "Unknown"

        # Prefer hardware-derived serial/model; fall back to device-info fields
        serial = hw_info.get("serial") or device_info.get("chassisId", "")
        model  = (
            hw_info.get("model")
            or _map_platform_id(device_info.get("platformId", ""))
        )

        device: dict[str, Any] = {
            # --- normalised convenience fields ---
            "name":          name,
            "model":         model or "BIG-IP",
            "manufacturer":  "F5 Networks",
            "serial":        serial.upper() if serial else "",
            "status":        "active",
            "mgmt_address":  mgmt_ip,
            "platform_name": f"BIG-IP {version}".strip() if version else "BIG-IP",
            # --- passthrough raw fields ---
            "hostname":   hostname,
            "platformId": device_info.get("platformId", ""),
            "chassisId":  device_info.get("chassisId", ""),
            "product":    device_info.get("product", "BIG-IP"),
            "version":    version,
        }

        if self._fetch_interfaces:
            device["interfaces"] = self._get_interfaces()

        return [device]

    def _fetch_device_info(self) -> dict:
        """Fetch basic device info, with a fallback to global-settings."""
        try:
            return self._get("/mgmt/shared/identified-devices/config/device-info")
        except Exception as exc:
            logger.debug("F5: identified-devices endpoint failed: %s", exc)

        try:
            gs = self._get("/mgmt/tm/sys/global-settings")
            return {"hostname": gs.get("hostname", ""), "product": "BIG-IP"}
        except Exception as exc2:
            logger.debug("F5: global-settings endpoint also failed: %s", exc2)
            return {}

    def _fetch_version(self) -> str:
        """Fetch the BIG-IP software version string from sys/version stats."""
        try:
            data    = self._get("/mgmt/tm/sys/version")
            entries = data.get("entries", {})
            for url_data in entries.values():
                nested = url_data.get("nestedStats", {}).get("entries", {})
                ver    = nested.get("Version", {})
                if isinstance(ver, dict) and ver.get("description"):
                    return ver["description"]
        except Exception as exc:
            logger.debug("F5: failed to fetch version: %s", exc)
        return ""

    def _fetch_mgmt_ip(self) -> str:
        """Fetch the management IP address (without prefix length)."""
        try:
            data  = self._get("/mgmt/tm/sys/management-ip")
            items = data.get("items", [])
            if items and isinstance(items, list):
                addr = items[0].get("name", "")
                return addr.split("/")[0] if "/" in addr else addr
        except Exception as exc:
            logger.debug("F5: failed to fetch management IP: %s", exc)
        return ""

    def _fetch_hardware_info(self) -> dict:
        """Fetch chassis serial number and platform model from hardware stats.

        Returns a dict with ``"serial"`` and ``"model"`` keys, which may be
        empty strings when the hardware endpoint is unavailable or the device
        is a Virtual Edition.
        """
        result: dict[str, str] = {"serial": "", "model": ""}
        try:
            data    = self._get("/mgmt/tm/sys/hardware")
            entries = data.get("entries", {})
            for url_data in entries.values():
                if not isinstance(url_data, dict):
                    continue
                nested = url_data.get("nestedStats", {}).get("entries", {})

                # Chassis serial
                chassis = nested.get("Chassis", {}).get("nestedStats", {}).get("entries", {})
                serial_entry = chassis.get("bigipChassisSerialNum", {})
                if isinstance(serial_entry, dict) and serial_entry.get("description"):
                    result["serial"] = serial_entry["description"]

                # Platform model
                platform = nested.get("Platform", {}).get("nestedStats", {}).get("entries", {})
                model_entry = platform.get("Model", {})
                if isinstance(model_entry, dict) and model_entry.get("description"):
                    result["model"] = model_entry["description"]

                if result["serial"] or result["model"]:
                    break  # first valid stats entry is sufficient
        except Exception as exc:
            logger.debug("F5: failed to fetch hardware info: %s", exc)
        return result

    # ------------------------------------------------------------------
    # Interface / self-IP fetchers
    # ------------------------------------------------------------------

    def _get_interfaces(self) -> list[dict]:
        """Fetch and normalise all network interfaces with self-IP assignment."""
        try:
            data  = self._get("/mgmt/tm/net/interface")
            items = data.get("items", [])
            if not isinstance(items, list):
                items = []
        except Exception as exc:
            logger.warning("F5: failed to fetch interfaces: %s", exc)
            return []

        self_ips = self._fetch_self_ips_by_vlan()

        interfaces = []
        for iface in items:
            if not isinstance(iface, dict):
                continue
            enriched = self._enrich_interface(iface)
            # Attach any self IPs whose VLAN name matches the interface name.
            # F5 VLAN names are user-defined so this is a best-effort match.
            enriched["ip_addresses"] = self_ips.get(iface.get("name", ""), [])
            interfaces.append(enriched)

        return interfaces

    def _fetch_self_ips_by_vlan(self) -> dict[str, list[dict]]:
        """Return a mapping of VLAN interface name → list of self-IP dicts."""
        result: dict[str, list[dict]] = {}
        try:
            data  = self._get("/mgmt/tm/net/self")
            items = data.get("items", [])
            if not isinstance(items, list):
                return result
            for item in items:
                if not isinstance(item, dict):
                    continue
                address = item.get("address", "") or ""
                vlan    = item.get("vlan", "") or ""
                # Strip partition prefix from the VLAN reference
                vlan_name = _strip_partition(vlan)
                if address and vlan_name:
                    result.setdefault(vlan_name, []).append({
                        "address": address,
                        "status":  "active",
                    })
        except Exception as exc:
            logger.debug("F5: failed to fetch self IPs: %s", exc)
        return result

    def _enrich_interface(self, iface: dict) -> dict:
        """Return a normalised dict for a single F5 interface record."""
        name         = iface.get("name", "") or ""
        mac          = iface.get("macAddress") or iface.get("macAddr") or ""
        media_active = iface.get("mediaActive", "") or ""
        mtu          = iface.get("mtu", 0) or 0
        enabled      = iface.get("enabled", True)
        description  = iface.get("description", "") or ""

        speed      = _parse_media_speed_mbps(media_active)
        iface_type = _normalize_iface_type(media_active, speed)

        # Override type for well-known special interfaces
        if name == "mgmt":
            iface_type = "1000base-t"
        elif name.startswith("lo"):
            iface_type = "virtual"

        return {
            # --- normalised convenience fields ---
            "name":         name,
            "type":         iface_type,
            "enabled":      bool(enabled),
            "mac_address":  mac.upper() if mac else "",
            "description":  description,
            "mtu":          mtu,
            "speed":        speed,
            # --- passthrough ---
            "mediaActive":  media_active,
        }

    # ------------------------------------------------------------------
    # Virtual server collection
    # ------------------------------------------------------------------

    def _get_virtual_servers(self) -> list[dict]:
        """Fetch and normalise LTM virtual servers."""
        try:
            data  = self._get("/mgmt/tm/ltm/virtual")
            items = data.get("items", [])
            if not isinstance(items, list):
                items = []
            return [self._enrich_virtual_server(vs) for vs in items if isinstance(vs, dict)]
        except Exception as exc:
            logger.warning("F5: failed to fetch virtual servers: %s", exc)
            return []

    def _enrich_virtual_server(self, vs: dict) -> dict:
        """Return a normalised dict for a single virtual server record."""
        name      = vs.get("name", "") or ""
        full_name = vs.get("fullPath", name)
        partition = vs.get("partition", "Common") or "Common"

        destination = vs.get("destination", "") or ""
        destination = _strip_partition(destination)

        pool = vs.get("pool", "") or ""
        pool = _strip_partition(pool)

        disabled = vs.get("disabled", False)
        status   = "offline" if disabled else "active"

        return {
            "name":        name,
            "full_name":   full_name,
            "destination": destination,
            "pool":        pool,
            "protocol":    vs.get("ipProtocol", "tcp") or "tcp",
            "status":      status,
            "description": vs.get("description", "") or "",
            "partition":   partition,
        }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


# Module-level alias so existing call sites are unchanged.
_safe_get = safe_get
