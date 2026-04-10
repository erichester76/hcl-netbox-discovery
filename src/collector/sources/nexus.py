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
  name              Best-available switch name (domain stripped, truncated to 64 chars)
  model             Platform model with Nexus prefix normalisation applied
  manufacturer      Always ``"Cisco"``
  role              Switch role in title-case (e.g. ``"leaf"`` → ``"Leaf"``)
  platform_name     ``"NX-OS {release}"``
  serial            Uppercase serial number
  fabric_name       Fabric name the switch belongs to
  site_name         Best-available site/fabric label for NetBox site mapping
  ip_address        Management IP address
  status            ``"active"`` if alive, otherwise ``"offline"``
  interfaces        List of normalised interface dicts (when fetch_interfaces
                    is enabled)

Raw fields (passthrough from NDFC)
  hostName, switchName, deviceName, logicalName, siteName,
  siteNameHierarchy, ipAddress, rawModel, serialNumber, release,
  fabricName, switchRole, rawStatus, systemMode

Interface dict fields (when fetch_interfaces is enabled)
  name              Interface name (e.g. ``"Ethernet1/1"``)
  type              NetBox-compatible interface type string
  enabled           ``True`` if admin state is up
  description       Interface description
  mgmt_only         ``True`` for management interfaces
  mac_address       MAC address (upper-cased)
  speed             Speed in Mbps (integer)
  ip_address        IP address with prefix length (e.g. ``"10.0.0.1/24"``)
  ifName, ifType, adminState, operStatus (raw passthrough)
"""

from __future__ import annotations

import logging
import re
from typing import Any

import requests

from .base import DataSource
from .utils import close_http_session, disable_ssl_warnings, parse_speed_mbps, safe_get

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


def _last_hierarchy_part(value: str) -> str:
    """Return the last non-empty segment of a slash-separated hierarchy string."""
    if not value:
        return ""
    parts = [part.strip() for part in str(value).split("/") if part and part.strip()]
    return parts[-1] if parts else ""


def _first_non_empty(obj: Any, *keys: str) -> str:
    """Return the first non-empty string-like field from *obj* for *keys*."""
    for key in keys:
        value = _safe_get(obj, key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _derive_switch_name(switch: Any) -> str:
    """Return the best-available switch name from common NDFC inventory fields."""
    raw_name = _first_non_empty(
        switch,
        "hostName",
        "switchName",
        "deviceName",
        "logicalName",
        "sysName",
        "name",
    )
    if not raw_name:
        return "Unknown"
    return raw_name.split(".")[0][:64] or "Unknown"


def _derive_site_name(switch: Any) -> str:
    """Return the best-available site/fabric label from common NDFC fields."""
    site_name = _first_non_empty(
        switch,
        "fabricName",
        "fabric",
        "siteName",
        "site",
        "podName",
        "networkName",
    )
    if site_name:
        return site_name
    return _last_hierarchy_part(
        _first_non_empty(switch, "siteNameHierarchy", "fabricNameHierarchy", "sitePath")
    )


def _derive_interface_name(iface: Any) -> str:
    """Return the best-available interface name from common NDFC fields."""
    return _first_non_empty(
        iface,
        "ifName",
        "name",
        "interfaceName",
        "portName",
        "displayName",
        "shortName",
    )


def _derive_interface_name_details(iface: Any) -> tuple[str, str | None, dict[str, str]]:
    """Return the interface name, source field, and raw candidate values."""
    candidates: dict[str, str] = {}
    for key in ("ifName", "name", "interfaceName", "portName", "displayName", "shortName"):
        value = _safe_get(iface, key)
        candidates[key] = "" if value is None else str(value).strip()

    for key, value in candidates.items():
        if value:
            return value, key, candidates

    return "", None, candidates


def _debug_interface_normalization(
    serial: str,
    raw_iface: Any,
    normalized_iface: dict,
    *,
    name_source: str | None,
    name_candidates: dict[str, str],
) -> None:
    """Emit detailed DEBUG logs for interfaces using fallback or empty names."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    normalized_name = normalized_iface.get("name", "")
    if name_source == "ifName" and normalized_name:
        return

    logger.debug(
        "NDFC interface normalization serial=%s normalized_name=%r name_source=%s "
        "candidates=%s ifType=%r adminState=%r operStatus=%r ipAddress=%r description=%r raw_keys=%s",
        serial,
        normalized_name,
        name_source or "none",
        name_candidates,
        _safe_get(raw_iface, "ifType", "") or "",
        _safe_get(raw_iface, "adminState", "") or "",
        _safe_get(raw_iface, "operStatus", "") or "",
        _safe_get(raw_iface, "ipAddress", "") or "",
        _safe_get(raw_iface, "ifDescr", "") or _safe_get(raw_iface, "description", "") or "",
        sorted(raw_iface.keys()) if isinstance(raw_iface, dict) else [],
    )


def _debug_interface_fetch_summary(
    serial: str, interfaces: list[dict], fetched_count: int | None = None
) -> None:
    """Emit a DEBUG summary of interface normalization results for one switch."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    fetched = fetched_count if fetched_count is not None else len(interfaces)
    named = sum(1 for iface in interfaces if iface.get("name"))
    blank = sum(1 for iface in interfaces if not iface.get("name"))
    mgmt_only = sum(1 for iface in interfaces if iface.get("mgmt_only"))

    logger.debug(
        "NDFC interface normalization summary serial=%s fetched=%d named=%d blank=%d mgmt_only=%d",
        serial,
        fetched,
        named,
        blank,
        mgmt_only,
    )


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
        self._session: requests.Session | None = None
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
            disable_ssl_warnings()

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
        self._session = close_http_session(self._session, "NexusDashboardSource")

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

        last_error: Exception | None = None
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

        if data and isinstance(data[0], dict):
            first = data[0]
            preview_keys = [
                "hostName",
                "switchName",
                "deviceName",
                "logicalName",
                "sysName",
                "name",
                "fabricName",
                "fabric",
                "siteName",
                "site",
                "siteNameHierarchy",
                "podName",
                "networkName",
                "ipAddress",
                "serialNumber",
            ]
            preview = {
                key: value
                for key in preview_keys
                if key in first and (value := first.get(key)) not in (None, "")
            }
            logger.debug(
                "NDFC first switch payload keys=%s preview=%s",
                sorted(first.keys()),
                preview,
            )

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

        if (
            logger.isEnabledFor(logging.DEBUG)
            and data
            and isinstance(data[0], dict)
        ):
            first = data[0]
            preview_keys = [
                "ifName",
                "name",
                "interfaceName",
                "portName",
                "displayName",
                "shortName",
                "ifType",
                "adminState",
                "operStatus",
                "ipAddress",
            ]
            preview = {
                key: value
                for key in preview_keys
                if key in first and (value := first.get(key)) not in (None, "")
            }
            logger.debug(
                "NDFC first interface payload serial=%s keys=%s preview=%s",
                serial,
                sorted(first.keys()),
                preview,
            )

        debug_enabled = logger.isEnabledFor(logging.DEBUG)
        interfaces: list[dict] = []
        for iface in data:
            if not isinstance(iface, dict):
                continue

            enriched = self._enrich_interface(iface)
            interfaces.append(enriched)
            if debug_enabled:
                _, name_source, name_candidates = _derive_interface_name_details(iface)
                _debug_interface_normalization(
                    serial,
                    iface,
                    enriched,
                    name_source=name_source,
                    name_candidates=name_candidates,
                )

        _debug_interface_fetch_summary(serial, interfaces, fetched_count=len(data))
        return interfaces

    def _enrich_switch(self, switch: Any) -> dict:
        """Return a normalised dict for a single NDFC switch record."""
        hostname    = _first_non_empty(switch, "hostName")
        model       = _safe_get(switch, "model", "") or ""
        serial      = _safe_get(switch, "serialNumber", "") or ""
        release     = _safe_get(switch, "release", "") or ""
        fabric_name = _safe_get(switch, "fabricName", "") or ""
        site_name   = _derive_site_name(switch)
        switch_role = _safe_get(switch, "switchRole", "") or ""
        ip_address  = _safe_get(switch, "ipAddress", "") or ""
        raw_status  = _safe_get(switch, "status", "") or ""
        system_mode = _safe_get(switch, "systemMode", "") or ""

        name = _derive_switch_name(switch)

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
            "site_name":    site_name,
            "ip_address":   ip_address,
            "status":       status,
            # --- passthrough raw fields ---
            "hostName":     hostname,
            "switchName":   _safe_get(switch, "switchName", "") or "",
            "deviceName":   _safe_get(switch, "deviceName", "") or "",
            "logicalName":  _safe_get(switch, "logicalName", "") or "",
            "siteName":     _safe_get(switch, "siteName", "") or "",
            "siteNameHierarchy": _safe_get(switch, "siteNameHierarchy", "") or "",
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
        name        = _derive_interface_name(iface)
        if_type     = _safe_get(iface, "ifType", "") or ""
        admin_state = _safe_get(iface, "adminState", "") or ""
        oper_status = _safe_get(iface, "operStatus", "") or ""
        description = _safe_get(iface, "ifDescr", "") or _safe_get(iface, "description", "") or ""
        mac_address = _safe_get(iface, "macAddress", "") or ""
        ip_address  = _safe_get(iface, "ipAddress", "") or ""
        speed_str   = _safe_get(iface, "speedStr", "") or _safe_get(iface, "speed", "") or ""
        mgmt_only   = if_type in {"INTERFACE_MANAGEMENT", "mgmt"} or name.lower().startswith("mgmt")

        return {
            # --- normalised convenience fields ---
            "name":        name,
            "type":        _normalize_iface_type(if_type),
            "enabled":     admin_state.lower() == "up",
            "description": description,
            "mgmt_only":   mgmt_only,
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

# Module-level aliases so existing call sites are unchanged.
_safe_get = safe_get


def _parse_speed_mbps(speed_str: str) -> int | None:
    """Delegate to shared helper, treating bare integers as already-Mbps (Nexus)."""
    return parse_speed_mbps(speed_str)
