"""Cisco Catalyst Center (CATC) data source adapter.

Uses the ``dnacentersdk`` Python SDK to connect to Cisco Catalyst Center and
return network device inventory as plain Python dicts.

Supported collection
--------------------
``"devices"`` – all managed network devices, enriched with site hierarchy data.

Each returned dict includes both normalised convenience fields and the original
Catalyst Center response attributes:

Normalised fields
  name              Hostname with domain stripped and truncated to 64 chars
  model             Platform ID with Cisco model prefix normalisation applied
  manufacturer      Always ``"Cisco"``
  role              Device role in title-case (e.g. ``"ACCESS"`` → ``"Access"``)
  platform_name     ``"{softwareType} {softwareVersion}"``
  serial            Uppercase serial number
  site_name         Site name extracted from ``siteNameHierarchy`` (level 3)
  location_name     Building/location from hierarchy (level 4; empty string if absent)
  status            ``"active"`` if Reachable, otherwise ``"offline"``

Raw fields (passthrough from DNAC)
  hostname, platformId, softwareType, softwareVersion, serialNumber,
  reachabilityStatus, family, siteNameHierarchy, rawRole
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
                    devices.append(self._enrich_device(device, site_hierarchy))

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

        name = (hostname.split(".")[0] if hostname else "")[:64] or "Unknown"

        return {
            # --- normalised convenience fields ---
            "name":          name,
            "model":         _normalize_model(platform_id),
            "manufacturer":  "Cisco",
            "role":          role.replace("_", " ").title() if role else "Network Device",
            "platform_name": f"{software_type.upper()} {software_version}".strip() or "Unknown",
            "serial":        serial.upper() if serial else "",
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
        }


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Return obj[key] (dict) or getattr(obj, key) (object) or *default*."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)
