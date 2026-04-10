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
  lag_name          Best-available parent LAG/port-channel interface name
  vpc_name          Best-available vPC interface name
  mgmt_only         ``True`` for management interfaces
  mac_address       MAC address (upper-cased)
  speed             Speed in Mbps (integer)
  ip_address        IP address with prefix length (e.g. ``"10.0.0.1/24"``)
  ifName, ifType, adminState, operStatus (raw passthrough)
"""

from __future__ import annotations

import ipaddress
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


def _debug_missing_lag_name(
    serial: str,
    raw_iface: Any,
    normalized_iface: dict[str, Any],
    *,
    nvpair_values: dict[str, str],
) -> None:
    """Emit DEBUG logs for interfaces that expose LAG hints but do not normalize."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    if normalized_iface.get("lag_name"):
        return
    normalized_name = str(normalized_iface.get("name", "") or "").lower()
    if normalized_iface.get("type") == "lag":
        return
    if normalized_name.startswith(("port-channel", "vpc", "mgmt", "vlan", "loopback", "nve")):
        return

    lag_candidates = {
        key: value for key in _LAG_CANDIDATE_KEYS if (value := _nvpair_get_from_flattened(nvpair_values, key))
    }
    if not lag_candidates:
        return

    logger.debug(
        "NDFC interface missing lag_name serial=%s name=%r ifType=%r description=%r "
        "lag_candidates=%s nvpair_keys=%s raw_keys=%s",
        serial,
        normalized_iface.get("name", ""),
        normalized_iface.get("ifType", ""),
        normalized_iface.get("description", ""),
        lag_candidates,
        sorted(nvpair_values.keys()),
        sorted(raw_iface.keys()) if isinstance(raw_iface, dict) else [],
    )


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


def _debug_switch_modules(switch: dict[str, Any]) -> None:
    """Emit a compact DEBUG preview of switch module payload shape."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    modules = switch.get("modules")
    if modules in (None, ""):
        logger.debug("NDFC switch modules absent type=%s", type(modules).__name__)
        return

    if not isinstance(modules, list):
        preview = sorted(modules.keys()) if isinstance(modules, dict) else str(modules)[:200]
        logger.debug(
            "NDFC switch modules non-list type=%s preview=%s",
            type(modules).__name__,
            preview,
        )
        return

    if not modules:
        logger.debug("NDFC switch modules list empty")
        return

    first_module = next((item for item in modules if isinstance(item, dict)), None)
    if not first_module:
        logger.debug(
            "NDFC switch modules list count=%d but no dict entries type=%s",
            len(modules),
            type(modules[0]).__name__,
        )
        return

    preview_keys = (
        "moduleName",
        "name",
        "model",
        "moduleType",
        "moduleIndex",
        "serialNumber",
        "speed",
        "portSpeed",
        "ifSpeed",
    )
    preview = {
        key: value
        for key in preview_keys
        if key in first_module and (value := first_module.get(key)) not in (None, "")
    }
    for nested_key in ("ports", "interfaces"):
        nested = first_module.get(nested_key)
        if isinstance(nested, list):
            preview[f"{nested_key}_count"] = len(nested)
            first_nested = next((item for item in nested if isinstance(item, dict)), None)
            if first_nested:
                preview[f"{nested_key}_keys"] = sorted(first_nested.keys())
                preview[f"{nested_key}_preview"] = {
                    key: value
                    for key in (
                        "ifName",
                        "name",
                        "portName",
                        "speed",
                        "portSpeed",
                        "ifSpeed",
                        "adminSpeed",
                        "operSpeed",
                        "bandwidth",
                        "mediaType",
                        "portType",
                    )
                    if key in first_nested and (value := first_nested.get(key)) not in (None, "")
                }

    logger.debug(
        "NDFC first switch modules count=%d first_module_keys=%s preview=%s",
        len(modules),
        sorted(first_module.keys()),
        preview,
    )


def _debug_dashboard_payload_shape(kind: str, switch_id: Any, payload: Any) -> None:
    """Emit a compact DEBUG preview of a dashboard endpoint payload shape."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    preview_keys = (
        "ifName",
        "name",
        "portName",
        "speed",
        "portSpeed",
        "ifSpeed",
        "adminSpeed",
        "operSpeed",
        "bandwidth",
        "mediaType",
        "portType",
        "moduleName",
        "moduleType",
        "model",
        "serialNumber",
        "productId",
        "partNumber",
        "status",
        "adminStatus",
        "operStatus",
    )

    def _preview_dict(obj: dict[str, Any]) -> dict[str, Any]:
        return {
            key: value
            for key in preview_keys
            if key in obj and (value := obj.get(key)) not in (None, "")
        }

    if isinstance(payload, list):
        first_item = next((item for item in payload if isinstance(item, dict)), None)
        if first_item:
            logger.debug(
                "NDFC dashboard %s switch_id=%s count=%d first_keys=%s preview=%s",
                kind,
                switch_id,
                len(payload),
                sorted(first_item.keys()),
                _preview_dict(first_item),
            )
            return
        logger.debug(
            "NDFC dashboard %s switch_id=%s list count=%d first_type=%s",
            kind,
            switch_id,
            len(payload),
            type(payload[0]).__name__ if payload else "none",
        )
        return

    if isinstance(payload, dict):
        logger.debug(
            "NDFC dashboard %s switch_id=%s dict keys=%s",
            kind,
            switch_id,
            sorted(payload.keys()),
        )
        for nested_key in ("logicalInterfaces", "moduleInfo", "fexDetails"):
            nested = payload.get(nested_key)
            if isinstance(nested, list):
                first_item = next((item for item in nested if isinstance(item, dict)), None)
                logger.debug(
                    "NDFC dashboard %s switch_id=%s %s count=%d first_keys=%s preview=%s",
                    kind,
                    switch_id,
                    nested_key,
                    len(nested),
                    sorted(first_item.keys()) if first_item else [],
                    _preview_dict(first_item) if first_item else {},
                )
            elif isinstance(nested, dict):
                flattened = _flatten_dashboard_grouped_records(nested, root_group=nested_key)
                first_flattened = flattened[0] if flattened else None
                logger.debug(
                    "NDFC dashboard %s switch_id=%s %s dict keys=%s flattened_count=%d first_group=%r first_keys=%s preview=%s",
                    kind,
                    switch_id,
                    nested_key,
                    sorted(nested.keys()),
                    len(flattened),
                    first_flattened.get("dashboard_group", "") if first_flattened else "",
                    sorted(first_flattened.keys()) if first_flattened else [],
                    _preview_dict(first_flattened) if first_flattened else {},
                )
            elif nested not in (None, ""):
                logger.debug(
                    "NDFC dashboard %s switch_id=%s %s type=%s preview=%r",
                    kind,
                    switch_id,
                    nested_key,
                    type(nested).__name__,
                    nested,
                )
        return

    logger.debug(
        "NDFC dashboard %s switch_id=%s type=%s preview=%r",
        kind,
        switch_id,
        type(payload).__name__,
        payload,
    )


_DASHBOARD_RECORD_HINT_KEYS = (
    "ifName",
    "name",
    "portName",
    "displayName",
    "shortName",
    "speed",
    "portSpeed",
    "ifSpeed",
    "adminSpeed",
    "operSpeed",
    "bandwidth",
    "mediaType",
    "portType",
    "moduleName",
    "moduleType",
    "model",
    "serialNumber",
    "productId",
    "partNumber",
)


def _flatten_dashboard_grouped_records(
    payload: Any, *, root_group: str = ""
) -> list[dict[str, Any]]:
    """Flatten grouped dashboard payloads into plain records with group metadata."""

    def _visit(value: Any, group_path: tuple[str, ...]) -> list[dict[str, Any]]:
        if isinstance(value, list):
            records: list[dict[str, Any]] = []
            for item in value:
                records.extend(_visit(item, group_path))
            return records

        if not isinstance(value, dict):
            return []

        if not value:
            return []

        looks_like_record = any(key in value for key in _DASHBOARD_RECORD_HINT_KEYS)
        if looks_like_record:
            record = dict(value)
            if group_path:
                record.setdefault("dashboard_group", "/".join(group_path))
            return [record]

        records: list[dict[str, Any]] = []
        for key, nested in value.items():
            records.extend(_visit(nested, (*group_path, str(key))))
        return records

    initial_path = (root_group,) if root_group else ()
    return _visit(payload, initial_path)


_LAG_CANDIDATE_KEYS = (
    "portChannelInterfaceDn",
    "portChannelInterface",
    "portChannelName",
    "portChannel",
    "portChannelId",
    "channelGroup",
    "channelGroupId",
    "aggregateInterface",
    "aggregateId",
    "bundleId",
    "memberOf",
    "interfaceGroup",
    "poId",
    "primaryIntf",
)

_VPC_LAG_CANDIDATE_KEYS = (
    "peer1Pcid",
    "peer2Pcid",
    "pcId",
    "pcid",
    "peer1PortChannelId",
    "peer2PortChannelId",
)

_SPEED_CANDIDATE_KEYS = (
    "speedStr",
    "speed",
    "portSpeed",
    "ethSpeed",
    "adminSpeed",
    "operSpeed",
    "adminSpeedStr",
    "operSpeedStr",
    "negotiatedSpeed",
    "actualSpeed",
    "ifSpeed",
    "interfaceSpeed",
    "speedValue",
    "linkSpeed",
)

_BANDWIDTH_CANDIDATE_KEYS = (
    "bandwidth",
    "bw",
)


def _flatten_interface_payload(payload: Any) -> list[dict]:
    """Return flat interface records from mixed NDFC wrapper payloads."""
    if isinstance(payload, list):
        flattened: list[dict] = []
        for item in payload:
            flattened.extend(_flatten_interface_payload(item))
        return flattened

    if not isinstance(payload, dict):
        return []

    nested = payload.get("interfaces")
    if isinstance(nested, list):
        flattened: list[dict] = []
        for item in nested:
            flattened.extend(_flatten_interface_payload(item))
        return flattened

    if isinstance(nested, dict):
        return _flatten_interface_payload(nested)

    return [payload]


def _normalize_nvpair_key(key: Any) -> str:
    """Return a canonical lowercase key for nvPair matching."""
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _flatten_nv_pairs(nv_pairs: Any) -> dict[str, str]:
    """Flatten common NDFC ``nvPairs`` shapes into a simple string map."""
    flattened: dict[str, str] = {}

    def _visit(value: Any) -> None:
        if isinstance(value, dict):
            pair_key = None
            pair_value = None
            for key_name in ("key", "name", "nvPairKey"):
                if key_name in value and value[key_name] not in (None, ""):
                    pair_key = value[key_name]
                    break
            for value_name in ("value", "nvPairValue"):
                if value_name in value and value[value_name] not in (None, ""):
                    pair_value = value[value_name]
                    break

            if pair_key is not None and pair_value is not None:
                flattened[_normalize_nvpair_key(pair_key)] = str(pair_value).strip()

            for nested_key, nested_value in value.items():
                if nested_key in {"key", "name", "nvPairKey", "value", "nvPairValue"}:
                    continue
                if isinstance(nested_value, (dict, list)):
                    _visit(nested_value)
                elif nested_value not in (None, ""):
                    flattened[_normalize_nvpair_key(nested_key)] = str(nested_value).strip()
            return

        if isinstance(value, list):
            for item in value:
                _visit(item)

    _visit(nv_pairs)
    return flattened


def _nvpair_get(iface: Any, *keys: str) -> str:
    """Return the first non-empty value for *keys* from ``iface['nvPairs']``."""
    nv_pairs = _safe_get(iface, "nvPairs")
    if nv_pairs in (None, "", []):
        return ""

    flattened = _flatten_nv_pairs(nv_pairs)
    for key in keys:
        value = flattened.get(_normalize_nvpair_key(key), "")
        if value:
            return value
    return ""


def _nvpair_get_from_flattened(flattened: dict[str, str], *keys: str) -> str:
    """Return the first non-empty value for *keys* from a flattened nvPair map."""
    for key in keys:
        value = flattened.get(_normalize_nvpair_key(key), "")
        if value:
            return value
    return ""


def _normalize_host_ip_prefix(address: str) -> str:
    """Return *address* with a host prefix when it is a bare IP literal."""
    if not address:
        return ""

    text = str(address).strip()
    if not text:
        return ""
    if "/" in text:
        return text

    try:
        parsed = ipaddress.ip_address(text)
    except ValueError:
        return text

    return f"{text}/32" if parsed.version == 4 else f"{text}/128"


def _normalize_ip_with_prefix(address: str, prefix: str = "") -> str:
    """Return *address* with an explicit prefix, preferring a provided mask when valid."""
    if not address:
        return ""

    text = str(address).strip()
    if not text:
        return ""
    if "/" in text:
        return text

    try:
        ip_obj = ipaddress.ip_address(text)
    except ValueError:
        return text

    prefix_text = str(prefix or "").strip()
    if prefix_text:
        prefix_text = prefix_text.lstrip("/")
        if prefix_text.isdigit():
            prefix_len = int(prefix_text)
            max_prefix = 32 if ip_obj.version == 4 else 128
            if 0 <= prefix_len <= max_prefix:
                return f"{text}/{prefix_len}"
        else:
            try:
                if ip_obj.version == 4:
                    prefix_len = ipaddress.IPv4Network(f"0.0.0.0/{prefix_text}").prefixlen
                else:
                    prefix_len = ipaddress.IPv6Network(f"::/{prefix_text}").prefixlen
            except ValueError:
                prefix_len = None
            if prefix_len is not None:
                return f"{text}/{prefix_len}"

    return f"{text}/32" if ip_obj.version == 4 else f"{text}/128"


def _extract_port_channel_number(value: str) -> str:
    """Return the trailing number from common port-channel labels."""
    if not value:
        return ""

    text = str(value).strip()
    numeric_match = re.fullmatch(r"\d+", text)
    if numeric_match:
        return text

    patterns = (
        r"port[\s_-]*channel[\s_-]*(\d+)",
        r"\bpo[\s_-]*(\d+)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)

    return ""


def _extract_vpc_number(value: str) -> str:
    """Return the trailing number from common vPC labels."""
    if not value:
        return ""

    text = str(value).strip()
    numeric_match = re.fullmatch(r"\d+", text)
    if numeric_match:
        return text

    match = re.search(r"\bvpc[\s_-]*(\d+)\b", text, re.IGNORECASE)
    return match.group(1) if match else ""


def _normalize_port_channel_name(value: Any) -> str:
    """Return a canonical NetBox port-channel interface name."""
    suffix = _extract_port_channel_number("" if value is None else str(value))
    return f"port-channel{suffix}" if suffix else ""


def _normalize_vpc_name(value: Any) -> str:
    """Return a canonical NetBox vPC interface name."""
    suffix = _extract_vpc_number("" if value is None else str(value))
    return f"vpc{suffix}" if suffix else ""


def _candidate_iface_values(
    iface: Any, *keys: str, nvpair_values: dict[str, str] | None = None
) -> list[str]:
    """Return unique non-empty values for interface candidate keys."""
    values: list[str] = []
    seen: set[str] = set()
    flattened = nvpair_values or {}
    for key in keys:
        for value in (_safe_get(iface, key), _nvpair_get_from_flattened(flattened, key)):
            if value in (None, ""):
                continue
            text = str(value).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(text)
    return values


def _derive_lag_name(iface: Any, *, nvpair_values: dict[str, str] | None = None) -> str:
    """Return the best-available parent LAG name for an interface item."""
    iface_name = _derive_interface_name(iface).strip().lower()
    if iface_name.startswith(("port-channel", "vpc")):
        return ""

    candidates = _candidate_iface_values(iface, *_LAG_CANDIDATE_KEYS, nvpair_values=nvpair_values)
    for value in candidates:
        normalized = _normalize_port_channel_name(value)
        if normalized:
            return normalized
    return ""


def _derive_vpc_name(iface: Any, *, nvpair_values: dict[str, str] | None = None) -> str:
    """Return the best-available vPC interface name for an interface item."""
    candidates = _candidate_iface_values(
        iface,
        "vpcInterface",
        "vpcInterfaceName",
        "vpcName",
        "vpcId",
        "vpc",
        "interfaceGroup",
        nvpair_values=nvpair_values,
    )
    for value in candidates:
        normalized = _normalize_vpc_name(value)
        if normalized:
            return normalized
    name = _derive_interface_name(iface)
    if name.lower().startswith("vpc"):
        return _normalize_vpc_name(name)
    return ""


def _debug_unparsed_speed(
    serial: str,
    raw_iface: Any,
    normalized_iface: dict[str, Any],
    *,
    nvpair_values: dict[str, str],
    speed_str: str,
) -> None:
    """Emit DEBUG logs when NDFC exposes speed-like fields we still cannot parse."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    if normalized_iface.get("speed") is not None:
        return

    speed_candidates = {
        key: value
        for key in _SPEED_CANDIDATE_KEYS
        if (value := _nvpair_get_from_flattened(nvpair_values, key))
    }
    if not speed_candidates and not speed_str:
        return

    logger.debug(
        "NDFC interface unparsed speed serial=%s name=%r normalized_type=%r raw_speed=%r "
        "top_level_speed=%r speed_candidates=%s bandwidth_candidates=%s nvpair_keys=%s",
        serial,
        normalized_iface.get("name", ""),
        normalized_iface.get("type", ""),
        speed_str,
        _safe_get(raw_iface, "speed", "") or _safe_get(raw_iface, "speedStr", "") or "",
        speed_candidates,
        {
            key: value
            for key in _BANDWIDTH_CANDIDATE_KEYS
            if (value := _nvpair_get_from_flattened(nvpair_values, key))
        },
        sorted(nvpair_values.keys()),
    )


def _derive_interface_speed_string(iface: Any, nvpair_values: dict[str, str]) -> str:
    """Return the first parseable speed string, skipping placeholder values like ``Auto``."""
    candidates = [
        *(_safe_get(iface, key, "") for key in _SPEED_CANDIDATE_KEYS),
        *(_nvpair_get_from_flattened(nvpair_values, key) for key in _SPEED_CANDIDATE_KEYS),
    ]
    fallback = ""
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text:
            continue
        if not fallback:
            fallback = text
        if text.lower() == "auto":
            continue
        if _parse_speed_mbps(text) is not None:
            return text
    return fallback


def _derive_interface_speed_mbps(iface: Any, nvpair_values: dict[str, str]) -> tuple[int | None, str]:
    """Return parsed interface speed in Mbps plus the raw source string used."""
    speed_str = _derive_interface_speed_string(iface, nvpair_values)
    speed_mbps = _parse_speed_mbps(speed_str)
    if speed_mbps is not None:
        return speed_mbps, speed_str

    bandwidth = _first_non_empty(iface, *_BANDWIDTH_CANDIDATE_KEYS) or _nvpair_get_from_flattened(
        nvpair_values, *_BANDWIDTH_CANDIDATE_KEYS
    )
    if bandwidth and str(bandwidth).strip().isdigit():
        # NDFC ``bandwidth`` values are reported in Kbps; convert to Mbps for NetBox.
        bandwidth_mbps = int(str(bandwidth).strip()) // 1000
        if bandwidth_mbps > 0:
            return bandwidth_mbps, str(bandwidth).strip()

    return None, speed_str


def _index_dashboard_interfaces(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return dashboard interface records keyed by normalized interface name."""
    indexed: dict[str, dict[str, Any]] = {}
    for record in records:
        name = _derive_interface_name(record).strip()
        if not name:
            continue
        indexed.setdefault(name.lower(), record)
    return indexed


def _merge_dashboard_interface(raw_iface: dict[str, Any], dashboard_iface: dict[str, Any] | None) -> dict[str, Any]:
    """Overlay dashboard interface fields without letting legacy placeholders win."""
    if not dashboard_iface:
        return raw_iface

    merged = dict(raw_iface)
    for key, value in dashboard_iface.items():
        current = merged.get(key)
        if key not in merged or current in (None, "", "Auto", "auto"):
            merged[key] = value
    return merged


def _suppress_duplicate_interface_ips(switches: list[dict[str, Any]]) -> None:
    """Clear duplicated interface IPs so shared addresses do not churn in NetBox."""
    addresses: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for switch in switches:
        switch_name = str(switch.get("name", "") or switch.get("serialNumber", "") or "unknown")
        for iface in switch.get("interfaces", []) or []:
            if not isinstance(iface, dict):
                continue
            address = str(iface.get("ip_address", "") or "").strip()
            if not address:
                continue
            addresses.setdefault(address, []).append((switch_name, iface))

    for address, refs in addresses.items():
        if len(refs) < 2:
            continue
        reference_labels = [f"{switch_name}:{iface.get('name', '')}" for switch_name, iface in refs]
        logger.warning(
            "NDFC duplicate interface IP suppressed address=%s references=%s",
            address,
            reference_labels,
        )
        for _, iface in refs:
            iface["duplicate_ip_address"] = address
            iface["ip_address"] = ""


def _interface_sort_key(iface: dict[str, Any]) -> tuple[int, str]:
    """Sort LAGs before dependent interfaces so same-device FK lookups resolve."""
    name = str(iface.get("name", "") or "").lower()
    lag_name = str(iface.get("lag_name", "") or "").lower()

    if name.startswith("port-channel"):
        bucket = 0
    elif name.startswith("vpc"):
        bucket = 1
    elif lag_name:
        bucket = 2
    else:
        bucket = 3

    return (bucket, name)


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


def _infer_iface_type_from_speed(speed_mbps: int | None) -> str:
    """Return a best-effort physical interface type slug from speed in Mbps."""
    if not speed_mbps:
        return "other"
    if speed_mbps >= 400000:
        return "400gbase-x-qsfpdd"
    if speed_mbps >= 100000:
        return "100gbase-x-qsfp28"
    if speed_mbps >= 40000:
        return "40gbase-x-qsfpp"
    if speed_mbps >= 25000:
        return "25gbase-x-sfp28"
    if speed_mbps >= 10000:
        return "10gbase-x-sfpp"
    if speed_mbps >= 1000:
        return "1000base-t"
    if speed_mbps >= 100:
        return "100base-tx"
    return "other"


def _infer_iface_type_from_name(if_name: str) -> str:
    """Return a best-effort NetBox interface type slug from *if_name*."""
    name = str(if_name or "").strip().lower()
    if not name:
        return "other"

    if name.startswith(("ethernet", "eth", "mgmt")):
        return "1000base-t"
    if name.startswith(("port-channel", "po")):
        return "lag"
    if name.startswith(("loopback", "vlan", "nve", "vpc")):
        return "virtual"
    return "other"


def _normalize_iface_type(raw_type: str, if_name: str = "", speed_mbps: int | None = None) -> str:
    """Return a NetBox-compatible interface type slug for *raw_type*."""
    normalized_raw_type = str(raw_type or "").strip()
    lower_raw_type = normalized_raw_type.lower()

    if lower_raw_type in {"interface_ethernet", "eth", "ethernet"}:
        inferred_from_speed = _infer_iface_type_from_speed(speed_mbps)
        if inferred_from_speed != "other":
            return inferred_from_speed

    if not raw_type:
        inferred_from_speed = _infer_iface_type_from_speed(speed_mbps)
        if inferred_from_speed != "other":
            return inferred_from_speed
        return _infer_iface_type_from_name(if_name)
    return _IFACE_TYPE_MAP.get(normalized_raw_type, _infer_iface_type_from_name(if_name))


def _normalize_physical_iface_type(raw_type: str, if_name: str, speed_mbps: int | None) -> str:
    """Prefer speed-specific NetBox physical types when NDFC only reports generic Ethernet."""
    normalized = _normalize_iface_type(raw_type, if_name, speed_mbps)
    if normalized != "1000base-t":
        return normalized

    name = str(if_name or "").strip().lower()
    if not name.startswith(("ethernet", "eth")):
        return normalized
    inferred_from_speed = _infer_iface_type_from_speed(speed_mbps)
    if inferred_from_speed != "other":
        return inferred_from_speed
    return normalized


def _is_interface_enabled(admin_state: str, oper_status: str = "") -> bool:
    """Return the best-effort enabled state for an interface."""
    candidates = [
        str(admin_state or "").strip().lower(),
        str(oper_status or "").strip().lower(),
    ]
    truthy = {"up", "enabled", "enable", "true", "1", "on", "admin-up", "link-up"}
    falsey = {"down", "disabled", "disable", "false", "0", "off", "admin-down", "link-down"}

    for value in candidates:
        if not value:
            continue
        if value in truthy:
            return True
        if value in falsey:
            return False
        if "up" in value and "down" not in value:
            return True
        if "down" in value and "up" not in value:
            return False

    return False


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
            _debug_switch_modules(first)

        switches: list[dict] = []
        for raw in data:
            enriched = self._enrich_switch(raw)
            enriched.update(self._fetch_dashboard_switch_details(enriched))
            if self._fetch_interfaces:
                serial = enriched.get("serialNumber", "")
                if serial:
                    enriched["interfaces"] = self._fetch_switch_interfaces(
                        serial,
                        switch_ip_address=enriched.get("ip_address", ""),
                        dashboard_logical_interfaces=enriched.get("dashboard_logical_interfaces", []),
                    )
                else:
                    enriched["interfaces"] = []
            switches.append(enriched)

        if self._fetch_interfaces:
            _suppress_duplicate_interface_ips(switches)

        self._switches = switches
        logger.debug("NDFC: returning %d switches", len(switches))
        return switches

    def _fetch_dashboard_switch_details(self, switch: dict[str, Any]) -> dict[str, Any]:
        """Fetch and flatten documented dashboard switch endpoints for one switch."""
        switch_db_id = _safe_get(switch, "switchDbID") or _safe_get(switch, "switch_db_id")
        details = {
            "dashboard_logical_interfaces": [],
            "dashboard_module_info": [],
            "dashboard_fex_details": [],
        }
        if switch_db_id in (None, ""):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("NDFC dashboard switch endpoints skipped: missing switchDbID")
            return details

        endpoints = {
            "switch/interface": f"{self._API_BASE}/lan-fabric/rest/dashboard/switch/interface?switchId={switch_db_id}",
            "switch/module": f"{self._API_BASE}/lan-fabric/rest/dashboard/switch/module?switchId={switch_db_id}",
        }
        for kind, path in endpoints.items():
            try:
                payload = self._get(path)
            except Exception as exc:
                logger.debug(
                    "NDFC dashboard %s switch_id=%s fetch failed: %s",
                    kind,
                    switch_db_id,
                    exc,
                )
                continue
            _debug_dashboard_payload_shape(kind, switch_db_id, payload)
            if kind == "switch/interface":
                details["dashboard_logical_interfaces"] = _flatten_dashboard_grouped_records(
                    _safe_get(payload, "logicalInterfaces", {}),
                    root_group="logicalInterfaces",
                )
            elif kind == "switch/module":
                details["dashboard_module_info"] = _flatten_dashboard_grouped_records(
                    _safe_get(payload, "moduleInfo", {}),
                    root_group="moduleInfo",
                )
                details["dashboard_fex_details"] = _flatten_dashboard_grouped_records(
                    _safe_get(payload, "fexDetails", {}),
                    root_group="fexDetails",
                )

        return details

    def _fetch_switch_interfaces(
        self,
        serial: str,
        switch_ip_address: str = "",
        dashboard_logical_interfaces: list[dict[str, Any]] | None = None,
    ) -> list[dict]:
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

        if isinstance(data, dict):
            for key in ("interfaces", "items", "data"):
                if key in data:
                    data = data[key]
                    break
            else:
                data = []

        data = _flatten_interface_payload(data)

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
                "portChannelId",
                "channelGroup",
                "vpcId",
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
        dashboard_index = _index_dashboard_interfaces(dashboard_logical_interfaces or [])
        interfaces: list[dict] = []
        for iface in data:
            if not isinstance(iface, dict):
                continue

            dashboard_iface = dashboard_index.get(_derive_interface_name(iface).strip().lower())
            enriched = self._enrich_interface(
                iface,
                switch_ip_address=switch_ip_address,
                dashboard_iface=dashboard_iface,
            )
            interfaces.append(enriched)
            if debug_enabled:
                nvpair_values = _flatten_nv_pairs(_safe_get(iface, "nvPairs"))
                merged_iface = _merge_dashboard_interface(iface, dashboard_iface)
                _, speed_str = _derive_interface_speed_mbps(merged_iface, nvpair_values)
                _, name_source, name_candidates = _derive_interface_name_details(iface)
                _debug_interface_normalization(
                    serial,
                    iface,
                    enriched,
                    name_source=name_source,
                    name_candidates=name_candidates,
                )
                _debug_missing_lag_name(
                    serial,
                    iface,
                    enriched,
                    nvpair_values=nvpair_values,
                )
                _debug_unparsed_speed(
                    serial,
                    iface,
                    enriched,
                    nvpair_values=nvpair_values,
                    speed_str=speed_str,
                )

        interfaces.sort(key=_interface_sort_key)
        _debug_interface_fetch_summary(serial, interfaces, fetched_count=len(data))
        return interfaces

    def _enrich_switch(self, switch: Any) -> dict:
        """Return a normalised dict for a single NDFC switch record."""
        hostname    = _first_non_empty(switch, "hostName")
        model       = _safe_get(switch, "model", "") or ""
        serial      = _safe_get(switch, "serialNumber", "") or ""
        switch_db_id = _safe_get(switch, "switchDbID", None)
        release     = _safe_get(switch, "release", "") or ""
        fabric_name = _safe_get(switch, "fabricName", "") or ""
        site_name   = _derive_site_name(switch)
        switch_role = _safe_get(switch, "switchRole", "") or ""
        ip_address  = _first_non_empty(switch, "mgmtAddress", "primaryIP", "ipAddress")
        raw_ip_address = _safe_get(switch, "ipAddress", "") or ""
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
            "switch_db_id": switch_db_id,
            "status":       status,
            "dashboard_logical_interfaces": [],
            "dashboard_module_info": [],
            "dashboard_fex_details": [],
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
            "ipAddress":    raw_ip_address,
            "mgmtAddress":  _safe_get(switch, "mgmtAddress", "") or "",
            "primaryIP":    _safe_get(switch, "primaryIP", "") or "",
            "rawStatus":    raw_status,
            "systemMode":   system_mode,
            "switchDbID":   switch_db_id,
        }

    def _enrich_interface(
        self,
        iface: dict,
        switch_ip_address: str = "",
        dashboard_iface: dict[str, Any] | None = None,
    ) -> dict:
        """Return a normalised dict for a single NDFC interface record."""
        nvpair_values = _flatten_nv_pairs(_safe_get(iface, "nvPairs"))
        source_iface = _merge_dashboard_interface(iface, dashboard_iface)

        def nv(*keys: str) -> str:
            return _nvpair_get_from_flattened(nvpair_values, *keys)

        if_name     = _safe_get(source_iface, "ifName", "") or ""
        name        = _derive_interface_name(source_iface)
        if_type     = (
            _first_non_empty(source_iface, "ifType", "interfaceType", "portType", "mediaType")
            or nv("ifType", "interfaceType", "portType", "mediaType")
        )
        admin_state = _first_non_empty(source_iface, "adminState", "adminStatus") or nv("adminState", "adminStatus")
        oper_status = _first_non_empty(source_iface, "operStatus", "operState", "operStatusStr") or nv(
            "operStatus", "operState", "operStatusStr"
        )
        description = (
            _safe_get(source_iface, "ifDescr", "")
            or _safe_get(source_iface, "description", "")
            or nv("ifDescr", "description", "desc", "portDescription")
        )
        mac_address = _safe_get(source_iface, "macAddress", "") or nv("macAddress", "mac")
        ip_address  = _safe_get(source_iface, "ipAddress", "") or nv("ipAddress", "primaryIpAddress", "primaryIP", "ip")
        ip_prefix   = nv("prefix", "prefixLength", "subnetPrefix", "mask", "subnetMask")
        speed_mbps, speed_str = _derive_interface_speed_mbps(source_iface, nvpair_values)
        lag_name    = _derive_lag_name(source_iface, nvpair_values=nvpair_values)
        vpc_name    = _derive_vpc_name(source_iface, nvpair_values=nvpair_values)
        mgmt_only   = if_type in {"INTERFACE_MANAGEMENT", "mgmt"} or name.lower().startswith("mgmt")
        if mgmt_only and not ip_address:
            ip_address = _normalize_host_ip_prefix(switch_ip_address)
        elif ip_address:
            ip_address = _normalize_ip_with_prefix(ip_address, ip_prefix)
        return {
            # --- normalised convenience fields ---
            "name":        name,
            "type":        _normalize_physical_iface_type(if_type, name, speed_mbps),
            "enabled":     _is_interface_enabled(admin_state, oper_status),
            "description": description,
            "lag_name":    lag_name,
            "vpc_name":    vpc_name,
            "mgmt_only":   mgmt_only,
            "mac_address": mac_address.upper() if mac_address else "",
            "ip_address":  ip_address,
            "speed":       speed_mbps,
            "duplicate_ip_address": "",
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
    """Delegate to shared helper for Nexus speed values.

    Bare integer strings below 1,000,000 are treated as already being in Mbps.
    Larger bare integers are treated as bits-per-second and converted to Mbps.
    """
    text = str(speed_str or "").strip()
    if text.isdigit() and int(text) >= 1_000_000:
        return parse_speed_mbps(text, numeric_is_bps=True)
    return parse_speed_mbps(text)
