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
import random
import re
import time
from typing import Any

from .base import DataSource
from .utils import parse_speed_mbps, safe_get

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


def _parse_speed_mbps(speed_str: str) -> int | None:
    """Delegate to shared helper, treating bare integers as bits-per-second (DNAC)."""
    return parse_speed_mbps(speed_str, numeric_is_bps=True)


def _mask_to_prefix(mask: str) -> int | None:
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


def _hierarchy_depth(hierarchy: str) -> int:
    """Return the number of populated path segments in *hierarchy*."""
    if not hierarchy:
        return 0
    return len([part for part in hierarchy.split("/") if part])


def _coerce_bool(value: Any, default: bool) -> bool:
    """Return *value* interpreted as a boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _coerce_int(value: Any, default: int) -> int:
    """Return *value* converted to ``int`` or *default* on failure."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float) -> float:
    """Return *value* converted to ``float`` or *default* on failure."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _response_items(payload: Any) -> list[Any]:
    """Return a list from common Catalyst Center SDK response shapes."""
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload

    for key in ("response", "items", "devices", "networkDevices"):
        value = _safe_get(payload, key)
        if isinstance(value, list):
            return value
        if value is not None and value is not payload:
            nested = _response_items(value)
            if nested:
                return nested

    return []


def _payload_shape(payload: Any, depth: int = 0) -> str:
    """Return a compact description of a Catalyst Center SDK payload shape."""
    if payload is None:
        return "None"
    if isinstance(payload, list):
        if not payload:
            return "list(len=0)"
        if depth >= 2:
            return f"list(len={len(payload)})"
        return f"list(len={len(payload)}, first={_payload_shape(payload[0], depth + 1)})"
    if isinstance(payload, dict):
        keys = sorted(payload.keys())
        if depth >= 2:
            return f"dict(keys={keys})"
        nested = {key: _payload_shape(payload[key], depth + 1) for key in keys[:4]}
        return f"dict(keys={keys}, nested={nested})"

    attrs = []
    for key in ("response", "items", "devices", "networkDevices"):
        value = _safe_get(payload, key)
        if value is not None and value is not payload:
            attrs.append(f"{key}={_payload_shape(value, depth + 1)}")

    if attrs:
        return f"{type(payload).__name__}({', '.join(attrs)})"
    return type(payload).__name__


def _payload_preview(payload: Any) -> str:
    """Return a short one-line preview of a payload item for debug logging."""
    if payload is None:
        return "None"
    if isinstance(payload, dict):
        preview = {key: payload.get(key) for key in list(payload.keys())[:6]}
        return repr(preview)

    preview: dict[str, Any] = {}
    for key in ("deviceId", "siteId", "siteNameHierarchy", "response", "items"):
        value = _safe_get(payload, key)
        if value is not None:
            preview[key] = value
    return repr(preview or payload)
# ---------------------------------------------------------------------------
# CatalystCenterSource
# ---------------------------------------------------------------------------

class CatalystCenterSource(DataSource):
    """dnacentersdk-backed source adapter for Cisco Catalyst Center."""

    def __init__(self) -> None:
        self._client: Any | None = None
        self._config: Any | None = None
        self._fetch_interfaces: bool = False
        self._wait_on_rate_limit: bool = True
        self._rate_limit_retry_attempts: int = 3
        self._rate_limit_retry_initial_delay: float = 1.0
        self._rate_limit_retry_max_delay: float = 30.0
        self._rate_limit_retry_jitter: float = 0.5

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
        self._wait_on_rate_limit = _coerce_bool(extra.get("wait_on_rate_limit", "true"), True)
        self._rate_limit_retry_attempts = max(
            1,
            _coerce_int(extra.get("rate_limit_retry_attempts", "3"), 3),
        )
        self._rate_limit_retry_initial_delay = max(
            0.0,
            _coerce_float(extra.get("rate_limit_retry_initial_delay", "1.0"), 1.0),
        )
        self._rate_limit_retry_max_delay = max(
            self._rate_limit_retry_initial_delay,
            _coerce_float(extra.get("rate_limit_retry_max_delay", "30.0"), 30.0),
        )
        self._rate_limit_retry_jitter = max(
            0.0,
            _coerce_float(extra.get("rate_limit_retry_jitter", "0.5"), 0.5),
        )

        logger.info("Connecting to Catalyst Center: %s", url)
        self._client = api.DNACenterAPI(
            base_url=url,
            username=config.username,
            password=config.password,
            verify=config.verify_ssl,
            wait_on_rate_limit=self._wait_on_rate_limit,
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

        assignments = self._fetch_site_assignments(sites)
        if assignments is None:
            logger.info(
                "CatalystCenter: falling back to per-site membership walk because "
                "bulk site assignment lookup is unavailable"
            )
            return self._get_devices_via_site_membership(sites)

        raw_devices = self._fetch_all_devices()
        logger.debug(
            "CatalystCenter: fetched %d inventory devices and %d site assignments",
            len(raw_devices), len(assignments),
        )
        if raw_devices and not assignments:
            logger.warning(
                "CatalystCenter: bulk site assignment lookup returned no assignments "
                "for %d inventory devices; falling back to per-site membership walk",
                len(raw_devices),
            )
            return self._get_devices_via_site_membership(sites)

        devices: list[dict] = []
        seen_devices: set[str] = set()

        for device in raw_devices:
            device_id = _safe_get(device, "id", "") or ""
            site_hierarchy = assignments.get(device_id, "")
            if not site_hierarchy:
                continue

            enriched = self._enrich_device(device, site_hierarchy)
            dedupe_key = (
                enriched.get("serial")
                or device_id
                or f"{enriched.get('name', 'Unknown')}|{site_hierarchy}"
            )
            if dedupe_key in seen_devices:
                continue
            seen_devices.add(dedupe_key)

            if self._fetch_interfaces:
                if device_id:
                    enriched["interfaces"] = self._fetch_device_interfaces(device_id)
                else:
                    enriched["interfaces"] = []
            devices.append(enriched)

        logger.debug("CatalystCenter: returning %d devices", len(devices))
        return devices

    def _get_devices_via_site_membership(self, sites: list[Any]) -> list[dict]:
        """Fallback inventory path using the legacy per-site membership API."""
        devices: list[dict] = []
        seen_devices: set[str] = set()

        for site in sites:
            site_id = _safe_get(site, "id")
            if not site_id:
                continue

            site_hierarchy = _safe_get(site, "siteNameHierarchy", "")

            try:
                membership = self._call_with_rate_limit_backoff(
                    self._client.sites.get_membership,
                    f"site membership {site_id}",
                    site_id=site_id,
                )
            except Exception as exc:
                logger.debug("No membership for site %s: %s", site_id, exc)
                continue

            if not membership or not hasattr(membership, "device"):
                continue

            for members in (membership.device or []):
                if not members or not hasattr(members, "response"):
                    continue
                for device in (members.response or []):
                    device_id = _safe_get(device, "id", "") or ""
                    serial = _safe_get(device, "serialNumber", "") or ""
                    dedupe_key = serial or device_id or f"{_safe_get(device, 'hostname', '')}|{site_hierarchy}"
                    if dedupe_key in seen_devices:
                        continue
                    seen_devices.add(dedupe_key)
                    enriched = self._enrich_device(device, site_hierarchy)
                    if self._fetch_interfaces:
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
                resp = self._call_with_rate_limit_backoff(
                    self._client.sites.get_site,
                    f"site page offset={offset}",
                    offset=offset,
                    limit=limit,
                )
                batch = _response_items(resp)
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

    def _fetch_all_devices(self) -> list:
        """Fetch all Catalyst Center inventory devices with pagination."""
        devices: list = []
        offset = 1
        limit = 500
        while True:
            try:
                resp = self._call_with_rate_limit_backoff(
                    self._client.devices.get_device_list,
                    f"device list page offset={offset}",
                    offset=offset,
                    limit=limit,
                )
                batch = _response_items(resp)
                if not batch:
                    break
                devices.extend(batch)
                if len(batch) < limit:
                    break
                offset += limit
            except Exception as exc:
                logger.warning("Failed to fetch device list at offset %d: %s", offset, exc)
                break
        return devices

    def _fetch_site_assignments(self, sites: list[Any]) -> dict[str, str] | None:
        """Return ``device_id -> site hierarchy`` using subtree assignment APIs."""
        site_design = getattr(self._client, "site_design", None)
        if site_design is None:
            return None

        fetcher = (
            getattr(site_design, "get_site_assigned_network_devices", None)
            or getattr(site_design, "get_site_assigned_network_devices_v1", None)
        )
        if fetcher is None:
            return None

        roots = self._select_assignment_roots(sites)
        if not roots:
            return None

        assignments: dict[str, str] = {}
        for root in roots:
            root_id = _safe_get(root, "id", "") or ""
            if not root_id:
                continue

            offset = 1
            limit = 500
            while True:
                try:
                    resp = self._call_with_rate_limit_backoff(
                        fetcher,
                        f"site assignment root={root_id} offset={offset}",
                        site_id=root_id,
                        offset=offset,
                        limit=limit,
                    )
                except Exception as exc:
                    logger.warning(
                        "CatalystCenter: bulk site assignment fetch failed for root %s: %s",
                        root_id, exc,
                    )
                    return None

                batch = _response_items(resp)
                if not batch:
                    logger.debug(
                        "CatalystCenter: empty site assignment batch root=%s offset=%d "
                        "response_shape=%s",
                        root_id,
                        offset,
                        _payload_shape(resp),
                    )
                    break

                parsed_in_batch = 0
                for assignment in batch:
                    device_id = _safe_get(assignment, "deviceId", "") or ""
                    site_hierarchy = _safe_get(assignment, "siteNameHierarchy", "") or ""
                    if device_id and site_hierarchy:
                        assignments[device_id] = site_hierarchy
                        parsed_in_batch += 1

                if parsed_in_batch == 0:
                    logger.debug(
                        "CatalystCenter: unparsed site assignment batch root=%s offset=%d "
                        "response_shape=%s sample=%s",
                        root_id,
                        offset,
                        _payload_shape(resp),
                        _payload_preview(batch[0]),
                    )

                if len(batch) < limit:
                    break
                offset += limit

        return assignments

    def _select_assignment_roots(self, sites: list[Any]) -> list[Any]:
        """Return the smallest disjoint set of root sites for subtree membership queries."""
        candidates: list[tuple[int, Any]] = []
        for site in sites:
            site_id = _safe_get(site, "id", "") or ""
            hierarchy = _safe_get(site, "siteNameHierarchy", "") or ""
            depth = _hierarchy_depth(hierarchy)
            if not site_id or depth <= 1:
                continue
            candidates.append((depth, site))

        if not candidates:
            return []

        min_depth = min(depth for depth, _site in candidates)
        roots = [site for depth, site in candidates if depth == min_depth]
        logger.debug(
            "CatalystCenter: selected %d subtree assignment roots at hierarchy depth %d",
            len(roots), min_depth,
        )
        return roots

    def _call_with_rate_limit_backoff(self, fn: Any, operation: str, **kwargs: Any) -> Any:
        """Call *fn* and retry 429 failures with exponential backoff."""
        attempts = self._rate_limit_retry_attempts
        delay = self._rate_limit_retry_initial_delay

        for attempt in range(1, attempts + 1):
            try:
                return fn(**kwargs)
            except Exception as exc:
                status_code = self._extract_status_code(exc)
                if status_code != 429 or attempt >= attempts:
                    raise

                retry_after = self._extract_retry_after_seconds(exc)
                sleep_for = retry_after if retry_after is not None else delay
                if self._rate_limit_retry_jitter:
                    sleep_for += random.uniform(0.0, self._rate_limit_retry_jitter)

                logger.warning(
                    "CatalystCenter: 429 rate limit during %s; retrying in %.2fs "
                    "(attempt %d/%d)",
                    operation,
                    sleep_for,
                    attempt + 1,
                    attempts,
                )
                time.sleep(sleep_for)
                if retry_after is None:
                    delay = min(delay * 2, self._rate_limit_retry_max_delay)

    def _extract_status_code(self, exc: Exception) -> int | None:
        """Best-effort extraction of an HTTP status code from an SDK exception."""
        for attr in ("status_code", "status", "http_status"):
            value = getattr(exc, attr, None)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass

        response = getattr(exc, "response", None) or getattr(exc, "resp", None)
        for attr in ("status_code", "status"):
            value = getattr(response, attr, None)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass

        return None

    def _extract_retry_after_seconds(self, exc: Exception) -> float | None:
        """Return the HTTP ``Retry-After`` header, when present."""
        response = getattr(exc, "response", None) or getattr(exc, "resp", None)
        headers = getattr(response, "headers", None) or getattr(exc, "headers", None)
        if not headers:
            return None

        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after is None:
            return None
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            return None

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
            resp = self._call_with_rate_limit_backoff(
                self._client.devices.get_interface_info_by_id,
                f"interface inventory for device {device_id}",
                device_id=device_id,
            )
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


# Keep _safe_get as a module-level alias so existing call sites are unchanged.
_safe_get = safe_get
