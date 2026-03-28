"""Cisco Nexus Dashboard Fabric Controller (NDFC) data source adapter.

Uses the NDFC REST API to connect to Cisco Nexus Dashboard and return
fabric switch inventory as plain Python dicts.

Supported collections
---------------------
``"switches"`` – all managed Nexus switches across all fabrics, optionally
                 including embedded interface lists when ``fetch_interfaces``
                 is enabled in the source config.

Each returned switch dict includes both normalised convenience fields and
the original NDFC response attributes:

Normalised fields
  name              Hostname (domain stripped, truncated to 64 chars)
  model             Platform model with Nexus prefix normalisation applied
  manufacturer      Always ``"Cisco"``
  role              Switch role in title-case (e.g. ``"leaf"`` → ``"Leaf"``)
  platform_name     ``"NX-OS {release}"``
  serial            Uppercase serial number
  fabric_name       Fabric name the switch belongs to
  ip_address        Management IP address
  status            ``"active"`` if alive, otherwise ``"offline"``
  interfaces        List of normalised interface dicts (when fetch_interfaces
                    is enabled)

Raw fields (passthrough from NDFC)
  hostName, ipAddress, rawModel, serialNumber, release, fabricName,
  switchRole, rawStatus, systemMode

Interface dict fields (when fetch_interfaces is enabled)
  name              Interface name (e.g. ``"Ethernet1/1"``)
  type              NetBox-compatible interface type string
  enabled           ``True`` if admin state is up
  description       Interface description
  mac_address       MAC address (upper-cased)
  speed             Speed in Mbps (integer)
  ip_address        IP address with prefix length (e.g. ``"10.0.0.1/24"``)
  ifName, ifType, adminState, operStatus (raw passthrough)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import requests

from .base import DataSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Nexus model normalisation
# ---------------------------------------------------------------------------

# Each tuple is (pattern, replacement).  Patterns are applied in order so
# that, for example, "N9K-C93180YC-EX" → "Nexus 93180YC-EX".
_MODEL_REPLACEMENTS: list[tuple[str, str]] = [
    (r"^N9K-C",  "Nexus "),       # N9K-C93180YC-EX → Nexus 93180YC-EX
    (r"^N9K-",   "Nexus 9000 "),  # N9K-SUP-A → Nexus 9000 SUP-A
    (r"^N77-",   "Nexus 7700 "),
    (r"^N7K-",   "Nexus 7000 "),
    (r"^N56-",   "Nexus 5600 "),
    (r"^N5K-",   "Nexus 5000 "),
    (r"^N3K-C",  "Nexus "),       # N3K-C3172PQ → Nexus 3172PQ
    (r"^N3K-",   "Nexus 3000 "),
    (r"^N2K-",   "Nexus 2000 "),
]


def _normalize_model(model: str) -> str:
    """Return a normalised Cisco Nexus model string from *model*."""
    if not model:
        return "Unknown"
    result = model.strip()
    for pattern, repl in _MODEL_REPLACEMENTS:
        new = re.sub(pattern, repl, result)
        if new != result:
            result = new
    return result.strip()


# ---------------------------------------------------------------------------
# Interface type mapping (NDFC → NetBox)
# ---------------------------------------------------------------------------

# Maps NDFC interface type strings to NetBox interface type slugs.
# Used to populate the ``type`` field on the normalised interface dict.
_IFACE_TYPE_MAP: dict[str, str] = {
    "INTERFACE_ETHERNET":    "1000base-t",
    "INTERFACE_MANAGEMENT":  "1000base-t",
    "INTERFACE_PORT_CHANNEL": "lag",
    "INTERFACE_LOOPBACK":    "virtual",
    "INTERFACE_VLAN":        "virtual",
    "INTERFACE_NVE":         "virtual",
    "INTERFACE_ST":          "virtual",
    "eth":                   "1000base-t",
    "port-channel":          "lag",
    "loopback":              "virtual",
    "vlan":                  "virtual",
    "mgmt":                  "1000base-t",
    "nve":                   "virtual",
}


def _normalize_iface_type(raw_type: str) -> str:
    """Return a NetBox-compatible interface type slug for *raw_type*."""
    if not raw_type:
        return "other"
    return _IFACE_TYPE_MAP.get(raw_type, "other")


# ---------------------------------------------------------------------------
# NexusDashboardSource
# ---------------------------------------------------------------------------


class NexusDashboardSource(DataSource):
    """NDFC REST API-backed source adapter for Cisco Nexus Dashboard."""

    #: NDFC application path prefix (Nexus Dashboard 2.x / NDFC 12.x)
    _API_BASE = "/appcenter/cisco/ndfc/api/v1"

    def __init__(self) -> None:
        self._session: Optional[requests.Session] = None
        self._base_url: str = ""
        self._fetch_interfaces: bool = False
        self._switches: list[dict] = []  # cached after _get_switches()

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to Nexus Dashboard using settings from *config*."""
        url = config.url
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"
        self._base_url = url.rstrip("/")

        verify_ssl = config.verify_ssl
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if not config.username or not config.password:
            raise RuntimeError(
                "Nexus Dashboard authentication requires both username and password. "
                "Ensure NDFC_USER and NDFC_PASS environment variables are set."
            )

        extra = config.extra or {}
        self._fetch_interfaces = str(extra.get("fetch_interfaces", "false")).lower() == "true"

        self._session = requests.Session()
        self._session.verify = verify_ssl
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })

        logger.info("Connecting to Nexus Dashboard: %s", self._base_url)
        self._authenticate(config.username, config.password)
        logger.info("Nexus Dashboard connection established: %s", config.url)

    def get_objects(self, collection: str) -> list:
        """Return a flat list of dicts for *collection*."""
        if self._session is None:
            raise RuntimeError("NexusDashboardSource: connect() has not been called")

        collectors = {
            "switches": self._get_switches,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"NexusDashboardSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Release the HTTP session."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception as exc:
                logger.debug("NexusDashboardSource session close error: %s", exc)
            finally:
                self._session = None

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _authenticate(self, username: str, password: str) -> None:
        """Obtain a session token from NDFC and attach it to the session headers.

        Tries the Nexus Dashboard platform login endpoint first, then falls
        back to the older NDFC-direct token endpoint.  Both paths return a
        token that is added as the ``X-Auth-Token`` request header.
        """
        auth_attempts = [
            # Nexus Dashboard 2.x / NDFC 12.x platform login
            (
                "/login",
                {"userName": username, "userPasswd": password, "domain": "DefaultAuth"},
                "token",
            ),
            # Older NDFC direct token endpoint
            (
                f"{self._API_BASE}/auth/token",
                {"domain": "LOCAL", "userName": username, "userPasswd": password},
                "jwttoken",
            ),
        ]

        last_error: Optional[Exception] = None
        for path, payload, token_key in auth_attempts:
            try:
                url = self._base_url + path
                resp = self._session.post(url, json=payload, timeout=30)  # type: ignore[union-attr]
                resp.raise_for_status()
                data = resp.json()
                token = data.get(token_key) or data.get("token") or data.get("jwttoken")
                if token:
                    self._session.headers["X-Auth-Token"] = token  # type: ignore[union-attr]
                    logger.debug("NDFC authenticated via %s", path)
                    return
            except Exception as exc:
                last_error = exc
                logger.debug("NDFC auth attempt via %s failed: %s", path, exc)

        raise RuntimeError(
            f"Failed to authenticate with Nexus Dashboard at {self._base_url}. "
            f"Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        """Perform an authenticated GET and return parsed JSON."""
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        logger.debug("NDFC GET %s", url)
        resp = self._session.get(url, timeout=30)  # type: ignore[union-attr]
        resp.raise_for_status()
        return resp.json()

    def _get_switches(self) -> list[dict]:
        """Fetch all switches across all NDFC-managed fabrics."""
        data = self._get(
            f"{self._API_BASE}/lan-fabric/rest/inventory/allswitches"
        )

        # Normalise response shape — some NDFC versions wrap in a key.
        if isinstance(data, dict):
            for key in ("switches", "items", "data"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                data = list(data.values()) if data else []

        if not isinstance(data, list):
            data = []

        switches: list[dict] = []
        for raw in data:
            enriched = self._enrich_switch(raw)
            if self._fetch_interfaces:
                serial = enriched.get("serialNumber", "")
                if serial:
                    enriched["interfaces"] = self._fetch_switch_interfaces(serial)
                else:
                    enriched["interfaces"] = []
            switches.append(enriched)

        self._switches = switches
        logger.debug("NDFC: returning %d switches", len(switches))
        return switches

    def _fetch_switch_interfaces(self, serial: str) -> list[dict]:
        """Return a list of normalised interface dicts for the given switch *serial*."""
        try:
            data = self._get(
                f"{self._API_BASE}/lan-fabric/rest/interface"
                f"?serialNumber={serial}"
            )
        except Exception as exc:
            logger.warning(
                "NDFC: failed to fetch interfaces for serial %s: %s", serial, exc
            )
            return []

        if not isinstance(data, list):
            if isinstance(data, dict):
                for key in ("interfaces", "items", "data"):
                    if key in data and isinstance(data[key], list):
                        data = data[key]
                        break
                else:
                    data = []
            else:
                data = []

        return [self._enrich_interface(iface) for iface in data if isinstance(iface, dict)]

    def _enrich_switch(self, switch: Any) -> dict:
        """Return a normalised dict for a single NDFC switch record."""
        hostname    = _safe_get(switch, "hostName", "") or ""
        model       = _safe_get(switch, "model", "") or ""
        serial      = _safe_get(switch, "serialNumber", "") or ""
        release     = _safe_get(switch, "release", "") or ""
        fabric_name = _safe_get(switch, "fabricName", "") or ""
        switch_role = _safe_get(switch, "switchRole", "") or ""
        ip_address  = _safe_get(switch, "ipAddress", "") or ""
        raw_status  = _safe_get(switch, "status", "") or ""
        system_mode = _safe_get(switch, "systemMode", "") or ""

        name = (hostname.split(".")[0] if hostname else "")[:64] or "Unknown"

        # NDFC uses "alive" / "unreachable" / "inactive" for status.
        status = "active" if raw_status.lower() in ("alive", "ok") else "offline"

        return {
            # --- normalised convenience fields ---
            "name":         name,
            "model":        _normalize_model(model),
            "manufacturer": "Cisco",
            "role":         switch_role.replace("_", " ").title() if switch_role else "Network Device",
            "platform_name": f"NX-OS {release}".strip() if release else "NX-OS",
            "serial":       serial.upper() if serial else "",
            "fabric_name":  fabric_name,
            "ip_address":   ip_address,
            "status":       status,
            # --- passthrough raw fields ---
            "hostName":     hostname,
            "rawModel":     model,
            "serialNumber": serial,
            "release":      release,
            "fabricName":   fabric_name,
            "switchRole":   switch_role,
            "ipAddress":    ip_address,
            "rawStatus":    raw_status,
            "systemMode":   system_mode,
        }

    def _enrich_interface(self, iface: dict) -> dict:
        """Return a normalised dict for a single NDFC interface record."""
        if_name     = _safe_get(iface, "ifName", "") or ""
        if_type     = _safe_get(iface, "ifType", "") or ""
        admin_state = _safe_get(iface, "adminState", "") or ""
        oper_status = _safe_get(iface, "operStatus", "") or ""
        description = _safe_get(iface, "ifDescr", "") or _safe_get(iface, "description", "") or ""
        mac_address = _safe_get(iface, "macAddress", "") or ""
        ip_address  = _safe_get(iface, "ipAddress", "") or ""
        speed_str   = _safe_get(iface, "speedStr", "") or _safe_get(iface, "speed", "") or ""

        return {
            # --- normalised convenience fields ---
            "name":        if_name,
            "type":        _normalize_iface_type(if_type),
            "enabled":     admin_state.lower() == "up",
            "description": description,
            "mac_address": mac_address.upper() if mac_address else "",
            "ip_address":  ip_address,
            "speed":       _parse_speed_mbps(speed_str),
            # --- passthrough raw fields ---
            "ifName":      if_name,
            "ifType":      if_type,
            "adminState":  admin_state,
            "operStatus":  oper_status,
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Return obj[key] (dict) or getattr(obj, key) (object) or *default*."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _parse_speed_mbps(speed_str: str) -> Optional[int]:
    """Parse a speed string like ``"10G"`` or ``"1000 Mbps"`` into Mbps.

    Returns ``None`` when the speed cannot be parsed.
    """
    if not speed_str:
        return None
    s = str(speed_str).strip().upper().replace(" ", "")
    # "10GBPS" / "10G" / "10GBIT"
    m = re.match(r"(\d+(?:\.\d+)?)\s*(?:G(?:BPS|BIT|B)?)", s)
    if m:
        return int(float(m.group(1)) * 1000)
    # "1000MBPS" / "1000M"
    m = re.match(r"(\d+(?:\.\d+)?)\s*(?:M(?:BPS|BIT|B)?)", s)
    if m:
        return int(float(m.group(1)))
    # bare integer → assume Mbps
    m = re.match(r"^(\d+)$", s)
    if m:
        return int(m.group(1))
    return None
