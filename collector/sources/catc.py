"""Cisco Catalyst Center (CATC) data source adapter.

Uses the ``dnacentersdk`` Python SDK to connect to Cisco Catalyst Center and
return network device inventory as plain Python dicts.

Supported collections
---------------------
``"devices"`` – all managed network devices, enriched with site hierarchy data
                and optionally with embedded interface lists when
                ``fetch_interfaces`` is enabled in the source config.

Each returned dict includes both normalised convenience fields and the original
Catalyst Center response attributes:

Normalised fields
  name              Hostname with domain stripped and truncated to 64 chars
  model             Platform ID with Cisco model prefix normalisation applied
  manufacturer      Always ``"Cisco"``
  role              Device role in title-case (e.g. ``"ACCESS"`` → ``"Access"``)
  platform_name     ``"{softwareType} {softwareVersion}"``
  serial            Uppercase serial number
  ip_address        Management IP address (empty string if absent)
  site_name         Site name extracted from ``siteNameHierarchy`` (level 3)
  location_name     Building/location from hierarchy (level 4; empty string if absent)
  status            ``"active"`` if Reachable, otherwise ``"offline"``
  interfaces        List of normalised interface dicts (when fetch_interfaces enabled)

Raw fields (passthrough from DNAC)
  hostname, platformId, softwareType, softwareVersion, serialNumber,
  reachabilityStatus, family, siteNameHierarchy, rawRole,
  managementIpAddress, deviceId

Interface dict fields (when fetch_interfaces is enabled)
  name              Interface name (e.g. ``"GigabitEthernet1/0/1"``)
  type              NetBox-compatible interface type string
  enabled           ``True`` if admin status is UP
  description       Interface description
  mac_address       MAC address (upper-cased, colon-separated)
  ip_address        IPv4 address with prefix length (e.g. ``"10.0.0.1/24"``)
                    or empty string
  speed             Speed in Mbps (integer or ``None``)
  portName, interfaceType, adminStatus, operStatus (raw passthrough)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cisco model normalisation (mirrors transformer.py in the archive)
# ---------------------------------------------------------------------------

# Each tuple is (pattern, replacement).  All matching patterns are applied
# in order so that, for example, "C9300-48P-K9" → "Catalyst 9300-48P" and
# "WS-C3560-48PS-S" → "Catalyst 3560-48PS-S".
_MODEL_REPLACEMENTS: list[tuple[str, str]] = [
    (r"^WS-C",          "Catalyst "),   # Catalyst classic form
    (r"^IE-",           "Catalyst IE "),
    (r"^AIR-AP",        "Catalyst "),   # Access points
    (r"^AIR-CAP",       "Catalyst "),
    (r"^C(?=[0-9])",    "Catalyst "),   # C9300, C9200, etc.
    (r"-K9$",           ""),            # Crypto suffix
    (r",.*$",           ""),            # Multiple models: keep first
]


# ---------------------------------------------------------------------------
# DNAC interface type → NetBox interface type slug
# ---------------------------------------------------------------------------

# Maps DNAC ``interfaceType`` strings to NetBox interface type slugs.
_IFACE_TYPE_MAP: dict[str, str] = {
    "Physical":                    "1000base-t",
    "Management":                  "1000base-t",
    "Virtual":                     "virtual",
    "SVI":                         "virtual",
    "Loopback":                    "virtual",
    "Port-Channel":                "lag",
    "Tunnel":                      "virtual",
    "NVE":                         "virtual",
}

# Speed string suffix → multiplier to convert to Mbps.
_SPEED_SUFFIX_MAP: list[tuple[str, int]] = [
    ("gbps", 1000),
    ("g",    1000),
    ("mbps", 1),
    ("m",    1),
    ("kbps", 0),   # rounded to 0 Mbps — effectively too slow to matter
    ("k",    0),
]


def _normalize_model(platform_id: str) -> str:
    """Return a normalised Cisco model string from *platform_id*."""
    if not platform_id:
        return "Unknown"
    model = platform_id.strip()
    for pattern, repl in _MODEL_REPLACEMENTS:
        new = re.sub(pattern, repl, model)
        if new != model:
            model = new
    return model.strip()


def _normalize_iface_type(raw_type: str) -> str:
    """Return a NetBox-compatible interface type slug for *raw_type*."""
    if not raw_type:
        return "other"
    return _IFACE_TYPE_MAP.get(raw_type, "other")


def _parse_speed_mbps(speed_str: str) -> Optional[int]:
    """Parse a DNAC speed string into Mbps.

    DNAC may return speed as:
      * a numeric string of bits-per-second (e.g. ``"1000000000"`` for 1 Gbps)
      * a human-readable string like ``"1 Gbps"`` or ``"100 Mbps"``

    Returns ``None`` when the value cannot be parsed.
    """
    if not speed_str:
        return None
    s = str(speed_str).strip()

    # Pure numeric → assume bits per second
    if s.isdigit():
        bps = int(s)
        if bps == 0:
            return None
        return max(1, bps // 1_000_000)  # bps → Mbps

    # Human-readable suffix matching
    lower = s.lower().replace(" ", "")
    m = re.match(r"(\d+(?:\.\d+)?)(.*)", lower)
    if m:
        value = float(m.group(1))
        suffix = m.group(2)
        for key, multiplier in _SPEED_SUFFIX_MAP:
            if suffix.startswith(key):
                return int(value * multiplier)

    return None


def _mask_to_prefix(mask: str) -> Optional[int]:
    """Convert a dotted-quad subnet mask to a prefix length.

    Returns ``None`` when *mask* is empty or invalid.
    """
    if not mask:
        return None
    try:
        octets = mask.split(".")
        if len(octets) != 4:
            return None
        bits = sum(bin(int(o)).count("1") for o in octets)
        return bits
    except (ValueError, AttributeError):
        return None


def _hierarchy_part(hierarchy: str, level: int) -> str:
    """Return the segment at *level* from a ``/``-separated site hierarchy.

    Level 0 is the root (Global), 1 is the country/area, 2 is the region,
    3 is the site, 4 is the building/location.
    """
    if not hierarchy:
        return ""
    parts = [p for p in hierarchy.split("/") if p]
    return parts[level] if len(parts) > level else ""


# ---------------------------------------------------------------------------
# CatalystCenterSource
# ---------------------------------------------------------------------------

class CatalystCenterSource(DataSource):
    """dnacentersdk-backed source adapter for Cisco Catalyst Center."""

    def __init__(self) -> None:
        self._client: Optional[Any] = None
        self._config: Optional[Any] = None
        self._fetch_interfaces: bool = False

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to Catalyst Center using settings from *config*."""
        try:
            from dnacentersdk import api  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "dnacentersdk is required for the CatalystCenter source adapter. "
                "Install it with: pip install dnacentersdk"
            ) from exc

        self._config = config
        url = config.url
        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        if not config.username or not config.password:
            raise RuntimeError(
                "Catalyst Center authentication requires both username and password. "
                "Ensure CATC_USER and CATC_PASS environment variables are set."
            )

        extra = config.extra or {}
        self._fetch_interfaces = str(extra.get("fetch_interfaces", "false")).lower() == "true"

        logger.info("Connecting to Catalyst Center: %s", url)
        self._client = api.DNACenterAPI(
            base_url=url,
            username=config.username,
            password=config.password,
            verify=config.verify_ssl,
        )
        logger.info("Catalyst Center connection established: %s", config.url)

    def get_objects(self, collection: str) -> list:
        """Return a flat list of device dicts for *collection*."""
        if self._client is None:
            raise RuntimeError("CatalystCenterSource: connect() has not been called")

        collectors = {
            "devices": self._get_devices,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"CatalystCenterSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Release the SDK client (no persistent socket to close)."""
        self._client = None

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_devices(self) -> list[dict]:
        """Fetch all devices across all sites with site hierarchy attached."""
        sites = self._fetch_all_sites()
        logger.debug("CatalystCenter: fetched %d sites", len(sites))

        devices: list[dict] = []
        seen_serials: set[str] = set()

        for site in sites:
            site_id = _safe_get(site, "id")
            if not site_id:
                continue

            site_hierarchy = _safe_get(site, "siteNameHierarchy", "")

            try:
                membership = self._client.sites.get_membership(site_id=site_id)
            except Exception as exc:
                logger.debug("No membership for site %s: %s", site_id, exc)
                continue

            if not membership or not hasattr(membership, "device"):
                continue

            for members in (membership.device or []):
                if not members or not hasattr(members, "response"):
                    continue
                for device in (members.response or []):
                    serial = _safe_get(device, "serialNumber", "")
                    if serial and serial in seen_serials:
                        continue
                    if serial:
                        seen_serials.add(serial)
                    enriched = self._enrich_device(device, site_hierarchy)
                    if self._fetch_interfaces:
                        device_id = enriched.get("deviceId", "")
                        if device_id:
                            enriched["interfaces"] = self._fetch_device_interfaces(device_id)
                        else:
                            enriched["interfaces"] = []
                    devices.append(enriched)

        logger.debug("CatalystCenter: returning %d devices", len(devices))
        return devices

    def _fetch_all_sites(self) -> list:
        """Fetch all sites from Catalyst Center with pagination."""
        sites: list = []
        offset = 1
        limit = 500
        while True:
            try:
                resp = self._client.sites.get_site(offset=offset, limit=limit)
                batch = resp.response if hasattr(resp, "response") else []
                if not batch:
                    break
                sites.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
            except Exception as exc:
                logger.warning("Failed to fetch sites at offset %d: %s", offset, exc)
                break
        return sites

    def _enrich_device(self, device: Any, site_hierarchy: str) -> dict:
        """Return a normalised dict for a single Catalyst Center device record."""
        hostname          = _safe_get(device, "hostname", "") or ""
        platform_id       = _safe_get(device, "platformId", "") or ""
        role              = _safe_get(device, "role", "") or ""
        software_type     = _safe_get(device, "softwareType", "") or ""
        software_version  = _safe_get(device, "softwareVersion", "") or ""
        serial            = _safe_get(device, "serialNumber", "") or ""
        reachability      = _safe_get(device, "reachabilityStatus", "") or ""
        family            = _safe_get(device, "family", "") or ""
        mgmt_ip           = _safe_get(device, "managementIpAddress", "") or ""
        device_id         = _safe_get(device, "id", "") or ""

        name = (hostname.split(".")[0] if hostname else "")[:64] or "Unknown"

        return {
            # --- normalised convenience fields ---
            "name":          name,
            "model":         _normalize_model(platform_id),
            "manufacturer":  "Cisco",
            "role":          role.replace("_", " ").title() if role else "Network Device",
            "platform_name": f"{software_type.upper()} {software_version}".strip() or "Unknown",
            "serial":        serial.upper() if serial else "",
            "ip_address":    mgmt_ip,
            "site_name":     (_hierarchy_part(site_hierarchy, 3)
                              or _hierarchy_part(site_hierarchy, 2)
                              or "Unknown"),
            "location_name": _hierarchy_part(site_hierarchy, 4),
            "status":        "active" if "Reachable" in reachability else "offline",
            # --- passthrough raw fields ---
            "hostname":             hostname,
            "platformId":           platform_id,
            "softwareType":         software_type,
            "softwareVersion":      software_version,
            "serialNumber":         serial,
            "reachabilityStatus":   reachability,
            "family":               family,
            "siteNameHierarchy":    site_hierarchy,
            "rawRole":              role,
            "managementIpAddress":  mgmt_ip,
            "deviceId":             device_id,
        }

    def _fetch_device_interfaces(self, device_id: str) -> list[dict]:
        """Return a list of normalised interface dicts for *device_id*.

        Uses the DNAC ``devices.get_interface_info_by_id`` endpoint.  Returns
        an empty list on any API failure so that one unreachable device does
        not abort the entire collection run.
        """
        try:
            resp = self._client.devices.get_interface_info_by_id(device_id=device_id)
            raw_list = resp.response if hasattr(resp, "response") else []
        except Exception as exc:
            logger.warning(
                "CatalystCenter: failed to fetch interfaces for device %s: %s",
                device_id, exc,
            )
            return []

        if not isinstance(raw_list, list):
            raw_list = []

        return [self._enrich_interface(iface) for iface in raw_list]

    def _enrich_interface(self, iface: Any) -> dict:
        """Return a normalised dict for a single DNAC interface record."""
        port_name    = _safe_get(iface, "portName", "") or ""
        iface_type   = _safe_get(iface, "interfaceType", "") or ""
        admin_status = _safe_get(iface, "adminStatus", "") or ""
        oper_status  = _safe_get(iface, "operStatus", "") or ""
        description  = _safe_get(iface, "description", "") or ""
        mac_address  = _safe_get(iface, "macAddress", "") or ""
        ipv4_address = _safe_get(iface, "ipv4Address", "") or ""
        ipv4_mask    = _safe_get(iface, "ipv4Mask", "") or ""
        speed_raw    = _safe_get(iface, "speed", "") or ""

        # Build CIDR notation when both address and mask are present.
        ip_address = ""
        if ipv4_address:
            prefix = _mask_to_prefix(ipv4_mask)
            if prefix is not None:
                ip_address = f"{ipv4_address}/{prefix}"
            else:
                logger.debug(
                    "CatalystCenter: interface %s has IP %s but unparseable mask %r; "
                    "storing address without prefix length",
                    port_name, ipv4_address, ipv4_mask,
                )
                ip_address = ipv4_address

        return {
            # --- normalised convenience fields ---
            "name":        port_name,
            "type":        _normalize_iface_type(iface_type),
            "enabled":     admin_status.upper() == "UP",
            "description": description,
            "mac_address": mac_address.upper() if mac_address else "",
            "ip_address":  ip_address,
            "speed":       _parse_speed_mbps(str(speed_raw)) if speed_raw else None,
            # --- passthrough raw fields ---
            "portName":      port_name,
            "interfaceType": iface_type,
            "adminStatus":   admin_status,
            "operStatus":    oper_status,
        }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Return obj[key] (dict) or getattr(obj, key) (object) or *default*."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)