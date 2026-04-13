"""Cisco Nexus Dashboard Fabric Controller (NDFC) data source adapter.

Uses the NDFC REST API to connect to Cisco Nexus Dashboard and return
fabric switch inventory as plain Python dicts.

Supported collections
---------------------
``"switches"`` – all managed Nexus switches across all fabrics, optionally
                 including embedded interface lists when ``fetch_interfaces``
                 is enabled in the source config.
``"shared_ips"`` – shared loopback/VARP addresses derived from duplicate
                   interface IP detection.
``"shared_fhrp_groups"`` – shared VLAN gateway groups derived from duplicate
                           VARP-style interface IPs.
``"shared_fhrp_assignments"`` – per-interface memberships for the derived
                                shared FHRP groups.
``"fabrics"`` – aggregated per-fabric topology records.
``"vpc_domains"`` – aggregated fabric-scoped vPC domain records.
``"vpc_peer_links"`` – aggregated fabric-scoped vPC peer-link records.
``"topology_custom_fields"`` – static custom-field definitions used by the
                               Nexus topology fallback mapping.
``"topology_custom_object_types"`` – static NetBox Custom Object type
                                     definitions used by the Nexus topology
                                     custom-object mapping.
``"topology_custom_object_type_fields"`` – static NetBox Custom Object type
                                           field definitions used by the
                                           Nexus topology custom-object
                                           mapping.

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
  modules           List of normalised module dicts (when ``fetch_modules`` is
                    enabled)

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
  speed             Speed in Kbps (integer, matching NetBox interface units)
  ip_address        IP address with prefix length (e.g. ``"10.0.0.1/24"``)
  ifName, ifType, adminState, operStatus (raw passthrough)

Module dict fields (when fetch_modules is enabled)
  profile           NetBox module type profile name (e.g. ``"Power supply"``)
  name              Installed module name
  bay_name          Stable module bay label on the device
  position          Best-effort slot / bay position string
  model             Module model / part identifier
  serial            Installed module serial number
  manufacturer      Always ``"Cisco"``
  status            Raw operational status
  description       Human-readable module description
"""

from __future__ import annotations

import ipaddress
import logging
import re
import zlib
from typing import Any
from urllib.parse import quote

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


def _sorted_unique(values: list[str]) -> list[str]:
    """Return sorted unique non-empty strings."""
    return sorted({value for value in values if value})


def _split_interface_refs(value: Any) -> list[str]:
    """Split one or more interface labels from common NDFC string fields."""
    if value in (None, ""):
        return []
    text = str(value).strip()
    if not text:
        return []
    return [part for part in re.split(r"[,\s]+", text) if part]


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


def _derive_vpc_domain_id(switch: Any) -> str:
    """Return the best-available vPC domain identifier for a switch."""
    return _first_non_empty(
        switch,
        "vpcDomain",
        "vpcDomainId",
        "vpcDomainID",
        "vpcDomainName",
    )


def _derive_vpc_role(switch: Any) -> str:
    """Return a normalized vPC control-plane role for a switch."""
    principal = _safe_get(switch, "principal", None)
    peer = _first_non_empty(switch, "peer", "peerName", "peerSerialNumber", "peerSwitchDbId")

    if isinstance(principal, bool):
        return "primary" if principal else ("secondary" if peer else "")

    text = str(principal or "").strip().lower()
    if text in {"true", "1", "yes", "on", "primary", "principal"}:
        return "primary"
    if text in {"false", "0", "no", "off", "secondary", "backup"}:
        return "secondary"
    if peer:
        return "secondary"
    return ""


def _derive_peer_link_interfaces(switch: Any) -> list[str]:
    """Return normalized peer-link interface names from switch inventory fields."""
    names: list[str] = []
    for raw in (_safe_get(switch, "sendIntf", None), _safe_get(switch, "recvIntf", None)):
        for part in _split_interface_refs(raw):
            normalized = _normalize_interface_name(part)
            if normalized:
                names.append(normalized)
    return _sorted_unique(names)


def _derive_vpc_peer_name(switch: Any) -> str:
    """Return the best-available peer switch label for a vPC switch."""
    return _first_non_empty(
        switch,
        "peer",
        "peerName",
        "peerSerialNumber",
        "peerSwitchDbId",
    )


def _module_profile(module: Any) -> str:
    """Return the NetBox module profile for a Nexus module payload."""
    haystack = " ".join(
        str(value or "")
        for value in (
            _safe_get(module, "name", ""),
            _safe_get(module, "modelName", ""),
            _safe_get(module, "type", ""),
            " ".join(str(item) for item in (_safe_get(module, "moduleType", []) or [])),
        )
    ).lower()

    if any(token in haystack for token in ("powersupply", "power supply", "psu", "nxa-pac", "pac-")):
        return "Power supply"
    if "fan" in haystack:
        return "Fan"
    if any(token in haystack for token in ("qsfp", "sfp", "transceiver", "optic")):
        return "Transceiver"
    return ""


def _module_bay_name(module: Any) -> str:
    """Return a stable bay label for a Nexus module payload."""
    for key in ("name", "slot", "serialNumber", "modelName"):
        value = _safe_get(module, key)
        if value not in (None, ""):
            text = str(value).strip()
            if text:
                if key == "slot":
                    return f"Slot {text}"
                return text
    return ""


def _module_position(module: Any) -> str:
    """Return a stable position string for a Nexus module payload."""
    value = _safe_get(module, "slot")
    if value in (None, ""):
        return ""
    return str(value).strip()


def _derive_interface_name(iface: Any) -> str:
    """Return the best-available interface name from common NDFC fields."""
    return _normalize_interface_name(
        _first_non_empty(
            iface,
            "ifName",
            "name",
            "interfaceName",
            "portName",
            "displayName",
            "shortName",
        )
    )


def _derive_interface_name_details(iface: Any) -> tuple[str, str | None, dict[str, str]]:
    """Return the interface name, source field, and normalized candidate values."""
    candidates: dict[str, str] = {}
    for key in ("ifName", "name", "interfaceName", "portName", "displayName", "shortName"):
        value = _safe_get(iface, key)
        candidates[key] = _normalize_interface_name("" if value is None else str(value).strip())

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


def _flatten_module_payload(payload: Any) -> list[dict[str, Any]]:
    """Return flat module records from mixed NDFC wrapper payloads."""
    if isinstance(payload, list):
        flattened: list[dict[str, Any]] = []
        for item in payload:
            flattened.extend(_flatten_module_payload(item))
        return flattened

    if not isinstance(payload, dict):
        return []

    if any(
        key in payload for key in ("modelName", "serialNumber", "moduleType", "slot", "name", "type")
    ):
        return [payload]

    flattened: list[dict[str, Any]] = []
    for key in (
        "modules",
        "items",
        "data",
        "DATA",
        "results",
        "result",
        "moduleInfo",
        "fexDetails",
    ):
        nested = payload.get(key)
        if isinstance(nested, list):
            for item in nested:
                flattened.extend(_flatten_module_payload(item))
        elif isinstance(nested, dict):
            flattened.extend(_flatten_module_payload(nested))

    if flattened:
        return flattened

    for value in payload.values():
        if isinstance(value, (dict, list)):
            flattened.extend(_flatten_module_payload(value))
    if flattened:
        return flattened

    return []


def _debug_module_fetch_payload(*, switch_db_id: Any, fabric_name: str, switch_id: str, payload: Any, flattened: list[dict[str, Any]]) -> None:
    """Emit DEBUG details for raw and flattened module payloads."""
    if not logger.isEnabledFor(logging.DEBUG):
        return

    if isinstance(payload, dict):
        raw_preview: Any = sorted(payload.keys())
    elif isinstance(payload, list):
        raw_preview = f"list[{len(payload)}]"
    else:
        raw_preview = type(payload).__name__

    sample = flattened[0] if flattened and isinstance(flattened[0], dict) else {}
    sample_preview = {
        key: value
        for key in ("name", "modelName", "serialNumber", "slot", "type", "moduleType")
        if key in sample and (value := sample.get(key)) not in (None, "", [])
    }

    logger.debug(
        "NDFC module payload switch_db_id=%s fabric=%s switch_id=%s raw_type=%s raw_preview=%s flattened_count=%d sample=%s",
        switch_db_id,
        fabric_name,
        switch_id,
        type(payload).__name__,
        raw_preview,
        len(flattened),
        sample_preview,
    )


def _normalize_interface_name(name: str) -> str:
    """Return a canonical interface name for mixed NDFC short/long forms."""
    text = str(name or "").strip()
    if not text:
        return ""

    lower = text.lower()
    patterns = (
        (r"^eth(?:ernet)?(\d.*)$", "Ethernet"),
        (r"^(?:po|port-channel)(\d.*)$", "port-channel"),
        (r"^(?:lo|loopback)(\d.*)$", "loopback"),
        (r"^vlan(\d.*)$", "Vlan"),
        (r"^mgmt(\d.*)$", "mgmt"),
        (r"^nve(\d.*)$", "nve"),
        (r"^vpc(\d.*)$", "vpc"),
    )
    for pattern, prefix in patterns:
        match = re.match(pattern, lower)
        if match:
            return f"{prefix}{match.group(1)}"

    return text


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


def _normalize_prefixed_ip_string(text: str) -> str:
    """Normalize an IP string that already includes a slash component."""
    address_text, prefix_text = text.split("/", 1)
    address_text = address_text.strip()
    prefix_text = prefix_text.strip().lstrip("/")
    if not address_text:
        return ""

    try:
        ip_obj = ipaddress.ip_address(address_text)
    except ValueError:
        return ""

    if not prefix_text:
        return _normalize_host_ip_prefix(address_text)

    if prefix_text.isdigit():
        prefix_len = int(prefix_text)
        max_prefix = 32 if ip_obj.version == 4 else 128
        if 0 <= prefix_len <= max_prefix:
            return f"{address_text}/{prefix_len}"
        return ""

    try:
        if ip_obj.version == 4:
            prefix_len = ipaddress.IPv4Network(f"0.0.0.0/{prefix_text}").prefixlen
        else:
            prefix_len = ipaddress.IPv6Network(f"::/{prefix_text}").prefixlen
    except ValueError:
        return ""

    return f"{address_text}/{prefix_len}"


def _normalize_ip_with_prefix(address: str, prefix: str = "") -> str:
    """Return *address* with an explicit prefix, preferring a provided mask when valid."""
    if not address:
        return ""

    text = str(address).strip()
    if not text:
        return ""
    if text.lower() == "use-link-local-only":
        return ""
    if "/" in text:
        return _normalize_prefixed_ip_string(text)

    try:
        ip_obj = ipaddress.ip_address(text)
    except ValueError:
        return ""

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


def _derive_vpc_parent_lag_name(
    iface: Any,
    *,
    nvpair_values: dict[str, str] | None = None,
    analyze_iface: Any | None = None,
    detail_iface: Any | None = None,
) -> str:
    """Return the parent port-channel metadata for a vPC interface."""
    if not _derive_vpc_name(iface, nvpair_values=nvpair_values):
        return ""

    candidates = _candidate_iface_values(
        iface,
        "peer1Pcid",
        "peer2Pcid",
        "peer1PcId",
        "peer2PcId",
        "portChannelId",
        "channelId",
        nvpair_values=nvpair_values,
    )
    for value in (
        _safe_get(detail_iface, "channelId", ""),
        _safe_get(analyze_iface, "channelId", ""),
    ):
        if value not in ("", None, 0, "0"):
            candidates.append(str(value))

    for value in candidates:
        normalized = _normalize_port_channel_name(value)
        if normalized:
            return normalized
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
        # NDFC ``bandwidth`` values are reported in Kbps; convert to Mbps for
        # intermediate Nexus parsing and interface-type inference.
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


def _is_blankish_speed(value: Any) -> bool:
    """Return ``True`` for empty or placeholder speed values."""
    text = str(value or "").strip().lower()
    return text in {"", "auto"}


def _suppress_duplicate_interface_ips(switches: list[dict[str, Any]]) -> None:
    """Clear duplicated interface IPs so shared addresses do not churn in NetBox."""
    addresses: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for switch in switches:
        for iface in switch.get("interfaces", []) or []:
            if not isinstance(iface, dict):
                continue
            address = str(iface.get("ip_address", "") or "").strip()
            if not address:
                continue
            addresses.setdefault(address, []).append((switch, iface))

    for address, refs in addresses.items():
        if len(refs) < 2:
            continue
        reference_labels = [
            f"{switch.get('name', '')}:{iface.get('name', '')}"
            for switch, iface in refs
        ]
        logger.warning(
            "NDFC duplicate interface IP suppressed address=%s references=%s",
            address,
            reference_labels,
        )
        for _, iface in refs:
            iface["duplicate_ip_address"] = address
            iface["ip_address"] = ""


def _collect_duplicate_ip_refs(
    switches: list[dict[str, Any]],
) -> dict[str, list[tuple[dict[str, Any], dict[str, Any]]]]:
    """Return duplicate interface IP references grouped by address."""
    addresses: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for switch in switches:
        for iface in switch.get("interfaces", []) or []:
            if not isinstance(iface, dict):
                continue
            duplicate_address = str(iface.get("duplicate_ip_address", "") or "").strip()
            if not duplicate_address:
                continue
            normalized_address = _normalize_ip_with_prefix(duplicate_address)
            if not normalized_address:
                continue
            addresses.setdefault(normalized_address, []).append((switch, iface))
    return addresses


def _shared_ip_kind(iface_name: str) -> str:
    """Return the normalized shared-address kind for *iface_name*."""
    lower = str(iface_name or "").lower()
    if lower.startswith("vlan"):
        return "varp"
    if lower.startswith("loopback"):
        return "anycast"
    return ""


def _derive_fhrp_group_id(iface_name: str, address: str) -> int:
    """Return a deterministic NetBox FHRP group ID for a shared VLAN VIP."""
    match = re.search(r"(\d+)$", str(iface_name or ""))
    if match:
        vlan_id = int(match.group(1))
        if 0 <= vlan_id <= 32767:
            return vlan_id
    return zlib.crc32(address.encode("utf-8")) % 32768


def _build_shared_ip_records(switches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return normalized standalone anycast IP records from duplicate interface IPs."""
    addresses = _collect_duplicate_ip_refs(switches)

    shared_records: list[dict[str, Any]] = []
    for address, refs in sorted(addresses.items()):
        kinds = {
            _shared_ip_kind(iface.get("name", ""))
            for _, iface in refs
        }
        kinds.discard("")
        if kinds != {"anycast"}:
            continue

        sorted_refs = sorted(
            refs,
            key=lambda ref: (
                str(ref[0].get("name", "") or ""),
                str(ref[1].get("name", "") or ""),
            ),
        )
        site_names = {
            str(switch.get("site_name", "") or "")
            for switch, _ in sorted_refs
            if str(switch.get("site_name", "") or "")
        }
        if len(site_names) > 1:
            continue

        first_switch, first_iface = sorted_refs[0]
        first_name = str(first_iface.get("name", "") or "")
        references = [
            f"{switch.get('name', '')}:{iface.get('name', '')}"
            for switch, iface in sorted_refs
        ]
        shared_records.append(
            {
                "address": address,
                "role": "anycast",
                "name": f"{first_name} anycast {address}",
                "site_name": str(first_switch.get("site_name", "") or ""),
                "references": references,
            }
        )

    return shared_records


def _build_shared_fhrp_groups(switches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return shared VLAN VIPs normalized as NetBox FHRP groups."""
    addresses = _collect_duplicate_ip_refs(switches)

    groups: list[dict[str, Any]] = []
    for address, refs in sorted(addresses.items()):
        kinds = {
            _shared_ip_kind(iface.get("name", ""))
            for _, iface in refs
        }
        kinds.discard("")
        if kinds != {"varp"}:
            continue

        sorted_refs = sorted(
            refs,
            key=lambda ref: (
                str(ref[0].get("name", "") or ""),
                str(ref[1].get("name", "") or ""),
            ),
        )
        first_switch, first_iface = sorted_refs[0]
        first_name = str(first_iface.get("name", "") or "")
        site_names = {
            str(switch.get("site_name", "") or "")
            for switch, _ in sorted_refs
            if str(switch.get("site_name", "") or "")
        }
        references = [
            f"{switch.get('name', '')}:{iface.get('name', '')}"
            for switch, iface in sorted_refs
        ]
        groups.append(
            {
                "address": address,
                "role": "vip",
                "protocol": "other",
                "group_name": f"{first_name} varp {address}",
                "group_id": _derive_fhrp_group_id(first_name, address),
                "site_name": next(iter(site_names)) if len(site_names) == 1 else "",
                "interface_name": first_name,
                "references": references,
            }
        )

    return groups


def _build_shared_fhrp_assignments(groups: list[dict[str, Any]], switches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return FHRP group assignment records for shared VLAN VIP interfaces."""
    group_by_address = {str(group.get("address", "") or ""): group for group in groups}
    assignments: list[dict[str, Any]] = []
    for switch in switches:
        switch_name = str(switch.get("name", "") or "")
        site_name = str(switch.get("site_name", "") or "")
        for iface in switch.get("interfaces", []) or []:
            if not isinstance(iface, dict):
                continue
            raw_duplicate_address = str(iface.get("duplicate_ip_address", "") or "").strip()
            duplicate_address = _normalize_ip_with_prefix(raw_duplicate_address) or raw_duplicate_address
            group = group_by_address.get(duplicate_address)
            if not group:
                continue
            iface_name = str(iface.get("name", "") or "")
            if _shared_ip_kind(iface_name) != "varp":
                continue
            assignments.append(
                {
                    "group_name": str(group.get("group_name", "") or ""),
                    "device_name": switch_name,
                    "site_name": site_name,
                    "interface_name": iface_name,
                    "priority": 100,
                }
            )

    assignments.sort(
        key=lambda item: (
            item.get("group_name", ""),
            item.get("device_name", ""),
            item.get("interface_name", ""),
        )
    )
    return assignments


def _build_fabric_records(switches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one record per Nexus fabric."""
    fabrics: dict[str, dict[str, Any]] = {}
    for switch in switches:
        fabric_name = str(switch.get("fabric_name", "") or "").strip()
        if not fabric_name:
            continue
        record = fabrics.setdefault(
            fabric_name,
            {
                "identifier": fabric_name,
                "fabric_name": fabric_name,
                "site_names": set(),
                "device_names": set(),
                "tenant_names": set(),
            },
        )
        site_name = str(switch.get("site_name", "") or "").strip()
        tenant_name = str(switch.get("tenant_name", "") or "").strip()
        device_name = str(switch.get("name", "") or "").strip()
        if site_name:
            record["site_names"].add(site_name)
        if device_name:
            record["device_names"].add(device_name)
        if tenant_name:
            record["tenant_names"].add(tenant_name)

    return [
        {
            **record,
            "site_names": sorted(record["site_names"]),
            "device_names": sorted(record["device_names"]),
            "tenant_names": sorted(record["tenant_names"]),
        }
        for _, record in sorted(fabrics.items())
    ]


def _build_vpc_domain_records(switches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one record per fabric-scoped vPC domain."""
    domains: dict[tuple[str, str], dict[str, Any]] = {}
    for switch in switches:
        fabric_name = str(switch.get("fabric_name", "") or "").strip()
        vpc_domain_id = str(switch.get("vpc_domain_id", "") or "").strip()
        if not fabric_name or not vpc_domain_id:
            continue

        identifier = f"{fabric_name}:{vpc_domain_id}"
        record = domains.setdefault(
            (fabric_name, vpc_domain_id),
            {
                "identifier": identifier,
                "fabric_name": fabric_name,
                "fabric_identifier": fabric_name,
                "vpc_domain_id": vpc_domain_id,
                "vpc_name": f"vpc{vpc_domain_id}",
                "primary_device_name": "",
                "secondary_device_name": "",
                "peer_device_names": set(),
                "member_lag_refs": set(),
                "vpc_interface_refs": set(),
                "tenant_names": set(),
                "vrf_names": set(),
            },
        )

        device_name = str(switch.get("name", "") or "").strip()
        role = str(switch.get("vpc_role", "") or "").strip().lower()
        tenant_name = str(switch.get("tenant_name", "") or "").strip()
        peer_name = str(switch.get("vpc_peer_name", "") or "").strip()
        if role == "primary" and device_name:
            record["primary_device_name"] = device_name
        elif role == "secondary" and device_name:
            record["secondary_device_name"] = device_name
        if device_name:
            record["peer_device_names"].add(device_name)
        if peer_name:
            record["peer_device_names"].add(peer_name)
        if tenant_name:
            record["tenant_names"].add(tenant_name)

        for iface in switch.get("interfaces", []) or []:
            if not isinstance(iface, dict):
                continue
            iface_name = str(iface.get("name", "") or "").strip()
            if not iface_name or not device_name:
                continue
            lag_name = str(iface.get("vpc_parent_lag_name", "") or "").strip()
            vpc_name = str(iface.get("vpc_name", "") or "").strip()
            vrf_name = str(iface.get("vrf_name", "") or "").strip()
            if lag_name:
                record["member_lag_refs"].add((device_name, lag_name))
            if vpc_name:
                record["vpc_interface_refs"].add((device_name, iface_name))
            if vrf_name:
                record["vrf_names"].add(vrf_name)

    results: list[dict[str, Any]] = []
    for _, record in sorted(domains.items()):
        results.append(
            {
                **record,
                "peer_device_names": sorted(record["peer_device_names"]),
                "member_lag_refs": [
                    {"device_name": device_name, "name": name}
                    for device_name, name in sorted(record["member_lag_refs"])
                ],
                "vpc_interface_refs": [
                    {"device_name": device_name, "name": name}
                    for device_name, name in sorted(record["vpc_interface_refs"])
                ],
                "tenant_names": sorted(record["tenant_names"]),
                "vrf_names": sorted(record["vrf_names"]),
            }
        )
    return results


def _build_vpc_peer_link_records(switches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return one record per fabric-scoped vPC peer-link."""
    links: dict[tuple[str, str], dict[str, Any]] = {}
    for switch in switches:
        fabric_name = str(switch.get("fabric_name", "") or "").strip()
        vpc_domain_id = str(switch.get("vpc_domain_id", "") or "").strip()
        if not fabric_name or not vpc_domain_id:
            continue
        peer_link_interfaces = switch.get("peer_link_interfaces", []) or []
        if not peer_link_interfaces:
            continue

        identifier = f"{fabric_name}:{vpc_domain_id}:peer-link"
        record = links.setdefault(
            (fabric_name, vpc_domain_id),
            {
                "identifier": identifier,
                "vpc_domain_identifier": f"{fabric_name}:{vpc_domain_id}",
                "fabric_name": fabric_name,
                "device_names": set(),
                "interface_refs": set(),
                "status_values": set(),
                "tenant_names": set(),
                "vrf_names": set(),
            },
        )

        device_name = str(switch.get("name", "") or "").strip()
        tenant_name = str(switch.get("tenant_name", "") or "").strip()
        status = str(switch.get("peer_link_status", "") or "").strip()
        if device_name:
            record["device_names"].add(device_name)
        if tenant_name:
            record["tenant_names"].add(tenant_name)
        if status:
            record["status_values"].add(status)
        peer_link_names = {str(name).strip() for name in peer_link_interfaces if str(name).strip()}
        for iface in switch.get("interfaces", []) or []:
            if not isinstance(iface, dict):
                continue
            iface_name = str(iface.get("name", "") or "").strip()
            if iface_name not in peer_link_names:
                continue
            vrf_name = str(iface.get("vrf_name", "") or "").strip()
            if vrf_name:
                record["vrf_names"].add(vrf_name)
        for iface_name in peer_link_interfaces:
            if device_name and iface_name:
                record["interface_refs"].add((device_name, iface_name))

    results: list[dict[str, Any]] = []
    for _, record in sorted(links.items()):
        status_values = sorted(record["status_values"])
        results.append(
            {
                **record,
                "fabric_identifier": record["fabric_name"],
                "device_names": sorted(record["device_names"]),
                "interface_refs": [
                    {"device_name": device_name, "name": name}
                    for device_name, name in sorted(record["interface_refs"])
                ],
                "status_values": status_values,
                "status": ", ".join(status_values),
                "tenant_names": sorted(record["tenant_names"]),
                "vrf_names": sorted(record["vrf_names"]),
            }
        )
    return results


def _build_topology_custom_field_records() -> list[dict[str, Any]]:
    """Return NetBox custom field definitions for Nexus topology fallback metadata."""
    return [
        {
            "name": "ndfc_fabric",
            "label": "NDFC Fabric",
            "group_name": "NDFC",
            "description": "NDFC fabric membership synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.device", "dcim.interface"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_vpc_domain",
            "label": "NDFC vPC Domain",
            "group_name": "NDFC",
            "description": "NDFC vPC domain identifier synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.device"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_vpc_role",
            "label": "NDFC vPC Role",
            "group_name": "NDFC",
            "description": "NDFC vPC control-plane role synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.device"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_vpc_peer",
            "label": "NDFC vPC Peer",
            "group_name": "NDFC",
            "description": "NDFC vPC peer switch synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.device"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_tenant",
            "label": "NDFC Tenant",
            "group_name": "NDFC",
            "description": "NDFC tenant metadata synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.device"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_vrf",
            "label": "NDFC VRF",
            "group_name": "NDFC",
            "description": "NDFC VRF metadata synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.interface"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_vpc",
            "label": "NDFC vPC",
            "group_name": "NDFC",
            "description": "NDFC vPC interface metadata synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.interface"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "name": "ndfc_vpc_parent_lag",
            "label": "NDFC vPC Parent LAG",
            "group_name": "NDFC",
            "description": "NDFC parent port-channel for a vPC interface synchronized by hcl-netbox-discovery.",
            "type": "text",
            "object_types": ["dcim.interface"],
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
    ]


def _build_topology_custom_object_type_records() -> list[dict[str, Any]]:
    """Return NetBox Custom Object type definitions for Nexus topology modeling."""
    return [
        {
            "name": "ndfc_fabrics",
            "slug": "ndfc-fabrics",
            "verbose_name": "NDFC Fabric",
            "verbose_name_plural": "NDFC Fabrics",
            "group_name": "NDFC",
            "description": "NDFC fabric topology objects synchronized by hcl-netbox-discovery.",
        },
        {
            "name": "ndfc_vpc_domains",
            "slug": "ndfc-vpc-domains",
            "verbose_name": "NDFC vPC Domain",
            "verbose_name_plural": "NDFC vPC Domains",
            "group_name": "NDFC",
            "description": "NDFC vPC domain topology objects synchronized by hcl-netbox-discovery.",
        },
        {
            "name": "ndfc_vpc_peer_links",
            "slug": "ndfc-vpc-peer-links",
            "verbose_name": "NDFC vPC Peer Link",
            "verbose_name_plural": "NDFC vPC Peer Links",
            "group_name": "NDFC",
            "description": "NDFC vPC peer-link topology objects synchronized by hcl-netbox-discovery.",
        },
    ]


def _build_topology_custom_object_type_field_records() -> list[dict[str, Any]]:
    """Return NetBox Custom Object type field definitions for Nexus topology modeling."""
    return [
        {
            "custom_object_type_slug": "ndfc-fabrics",
            "name": "identifier",
            "label": "Identifier",
            "description": "Stable fabric identifier.",
            "type": "text",
            "primary": True,
            "required": True,
            "unique": True,
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-fabrics",
            "name": "fabric_name",
            "label": "Fabric Name",
            "description": "NDFC fabric name.",
            "type": "text",
            "required": True,
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-fabrics",
            "name": "site_names",
            "label": "Site Names",
            "description": "NetBox site names associated with the fabric.",
            "type": "json",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-fabrics",
            "name": "tenant_names",
            "label": "Tenant Names",
            "description": "NDFC tenant names present in the fabric.",
            "type": "json",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-fabrics",
            "name": "devices",
            "label": "Devices",
            "description": "Devices participating in the fabric.",
            "type": "multiobject",
            "app_label": "dcim",
            "model": "device",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "identifier",
            "label": "Identifier",
            "description": "Stable fabric-scoped vPC domain identifier.",
            "type": "text",
            "primary": True,
            "required": True,
            "unique": True,
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "fabric_identifier",
            "label": "Fabric",
            "description": "Parent NDFC fabric object.",
            "type": "object",
            "app_label": "custom-objects",
            "model": "ndfc-fabrics",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "fabric_name",
            "label": "Fabric Name",
            "description": "NDFC fabric name.",
            "type": "text",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "vpc_domain_id",
            "label": "vPC Domain ID",
            "description": "NDFC vPC domain identifier.",
            "type": "text",
            "required": True,
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "vpc_name",
            "label": "vPC Name",
            "description": "Derived vPC domain name.",
            "type": "text",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "primary_device",
            "label": "Primary Device",
            "description": "Primary vPC peer device.",
            "type": "object",
            "app_label": "dcim",
            "model": "device",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "secondary_device",
            "label": "Secondary Device",
            "description": "Secondary vPC peer device.",
            "type": "object",
            "app_label": "dcim",
            "model": "device",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "peer_devices",
            "label": "Peer Devices",
            "description": "All devices participating in the vPC domain.",
            "type": "multiobject",
            "app_label": "dcim",
            "model": "device",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "member_lags",
            "label": "Member LAGs",
            "description": "Local port-channel interfaces participating in the vPC domain.",
            "type": "multiobject",
            "app_label": "dcim",
            "model": "interface",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "vpc_interfaces",
            "label": "vPC Interfaces",
            "description": "Logical vPC interfaces associated with the domain.",
            "type": "multiobject",
            "app_label": "dcim",
            "model": "interface",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "tenant_names",
            "label": "Tenant Names",
            "description": "NDFC tenant names present in the vPC domain.",
            "type": "json",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-domains",
            "name": "vrf_names",
            "label": "VRF Names",
            "description": "VRFs associated with the vPC domain.",
            "type": "json",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "identifier",
            "label": "Identifier",
            "description": "Stable fabric-scoped vPC peer-link identifier.",
            "type": "text",
            "primary": True,
            "required": True,
            "unique": True,
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "fabric_identifier",
            "label": "Fabric",
            "description": "Parent NDFC fabric object.",
            "type": "object",
            "app_label": "custom-objects",
            "model": "ndfc-fabrics",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "fabric_name",
            "label": "Fabric Name",
            "description": "NDFC fabric name.",
            "type": "text",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "vpc_domain_identifier",
            "label": "vPC Domain",
            "description": "Parent NDFC vPC domain object.",
            "type": "object",
            "app_label": "custom-objects",
            "model": "ndfc-vpc-domains",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "devices",
            "label": "Devices",
            "description": "Devices participating in the peer link.",
            "type": "multiobject",
            "app_label": "dcim",
            "model": "device",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "interfaces",
            "label": "Interfaces",
            "description": "Interfaces participating in the peer link.",
            "type": "multiobject",
            "app_label": "dcim",
            "model": "interface",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "status",
            "label": "Status",
            "description": "Observed peer-link status values.",
            "type": "text",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "tenant_names",
            "label": "Tenant Names",
            "description": "NDFC tenant names present on the peer link.",
            "type": "json",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
        {
            "custom_object_type_slug": "ndfc-vpc-peer-links",
            "name": "vrf_names",
            "label": "VRF Names",
            "description": "VRFs associated with the peer link.",
            "type": "json",
            "ui_visible": "if-set",
            "ui_editable": "no",
        },
    ]


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
    _ANALYZE_BASE = "/api/v1/analyze"

    def __init__(self) -> None:
        self._session: requests.Session | None = None
        self._base_url: str = ""
        self._fetch_interfaces: bool = False
        self._fetch_modules: bool = False
        self._switches: list[dict] = []  # cached after _get_switches()
        self._shared_ips: list[dict[str, Any]] = []
        self._shared_fhrp_groups: list[dict[str, Any]] = []
        self._shared_fhrp_assignments: list[dict[str, Any]] = []
        self._fabrics: list[dict[str, Any]] = []
        self._vpc_domains: list[dict[str, Any]] = []
        self._vpc_peer_links: list[dict[str, Any]] = []
        self._topology_custom_fields: list[dict[str, Any]] = []
        self._topology_custom_object_types: list[dict[str, Any]] = []
        self._topology_custom_object_type_fields: list[dict[str, Any]] = []
        self._analyze_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._detail_cache: dict[str, list[dict[str, Any]]] = {}

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
        self._fetch_modules = str(extra.get("fetch_modules", "false")).lower() == "true"

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
            "shared_ips": self._get_shared_ips,
            "shared_fhrp_groups": self._get_shared_fhrp_groups,
            "shared_fhrp_assignments": self._get_shared_fhrp_assignments,
            "fabrics": self._get_fabrics,
            "vpc_domains": self._get_vpc_domains,
            "vpc_peer_links": self._get_vpc_peer_links,
            "topology_custom_fields": self._get_topology_custom_fields,
            "topology_custom_object_types": self._get_topology_custom_object_types,
            "topology_custom_object_type_fields": self._get_topology_custom_object_type_fields,
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
        self._shared_ips = []
        self._shared_fhrp_groups = []
        self._shared_fhrp_assignments = []
        self._fabrics = []
        self._vpc_domains = []
        self._vpc_peer_links = []
        self._topology_custom_fields = []
        self._topology_custom_object_types = []
        self._topology_custom_object_type_fields = []

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

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """Perform an authenticated GET and return parsed JSON."""
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        logger.debug("NDFC GET %s", url)
        resp = self._session.get(url, params=params, timeout=30)  # type: ignore[union-attr]
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
            if self._fetch_interfaces:
                serial = enriched.get("serialNumber", "")
                fabric_name = enriched.get("fabricName", "")
                if serial:
                    enriched["interfaces"] = self._fetch_switch_interfaces(
                        serial,
                        fabric_name=fabric_name,
                        switch_ip_address=enriched.get("ip_address", ""),
                    )
                else:
                    enriched["interfaces"] = []
            if self._fetch_modules:
                switch_db_id = enriched.get("switchDbID") or enriched.get("switch_db_id")
                switch_id = (
                    enriched.get("serialNumber")
                    or enriched.get("serial")
                    or enriched.get("switchId")
                    or ""
                )
                enriched["modules"] = self._fetch_switch_modules(
                    switch_db_id=switch_db_id,
                    fabric_name=str(enriched.get("fabricName") or enriched.get("fabric_name") or ""),
                    switch_id=str(switch_id or ""),
                )
            switches.append(enriched)

        if self._fetch_interfaces:
            _suppress_duplicate_interface_ips(switches)
            self._shared_ips = _build_shared_ip_records(switches)
            self._shared_fhrp_groups = _build_shared_fhrp_groups(switches)
            self._shared_fhrp_assignments = _build_shared_fhrp_assignments(
                self._shared_fhrp_groups,
                switches,
            )
        else:
            self._shared_ips = []
            self._shared_fhrp_groups = []
            self._shared_fhrp_assignments = []
        self._fabrics = _build_fabric_records(switches)
        self._vpc_domains = _build_vpc_domain_records(switches)
        self._vpc_peer_links = _build_vpc_peer_link_records(switches)
        self._topology_custom_fields = _build_topology_custom_field_records()
        self._topology_custom_object_types = _build_topology_custom_object_type_records()
        self._topology_custom_object_type_fields = _build_topology_custom_object_type_field_records()

        self._switches = switches
        logger.debug("NDFC: returning %d switches", len(switches))
        return switches

    def _get_shared_ips(self) -> list[dict[str, Any]]:
        """Return standalone shared anycast IP records derived from switch interfaces."""
        if not self._switches:
            self._get_switches()
        return list(self._shared_ips)

    def _get_shared_fhrp_groups(self) -> list[dict[str, Any]]:
        """Return shared VARP VLAN VIPs normalized as NetBox FHRP groups."""
        if not self._switches:
            self._get_switches()
        return list(self._shared_fhrp_groups)

    def _get_shared_fhrp_assignments(self) -> list[dict[str, Any]]:
        """Return FHRP membership rows for interfaces participating in VARP."""
        if not self._switches:
            self._get_switches()
        return list(self._shared_fhrp_assignments)

    def _get_fabrics(self) -> list[dict[str, Any]]:
        """Return fabric records derived from switch inventory."""
        if not self._switches:
            self._get_switches()
        return list(self._fabrics)

    def _get_vpc_domains(self) -> list[dict[str, Any]]:
        """Return vPC domain records derived from switch and interface inventory."""
        if not self._switches:
            self._get_switches()
        return list(self._vpc_domains)

    def _get_vpc_peer_links(self) -> list[dict[str, Any]]:
        """Return vPC peer-link records derived from switch inventory."""
        if not self._switches:
            self._get_switches()
        return list(self._vpc_peer_links)

    def _get_topology_custom_fields(self) -> list[dict[str, Any]]:
        """Return NetBox custom field definitions for Nexus topology fallback metadata."""
        if not self._topology_custom_fields:
            self._topology_custom_fields = _build_topology_custom_field_records()
        return list(self._topology_custom_fields)

    def _get_topology_custom_object_types(self) -> list[dict[str, Any]]:
        """Return NetBox Custom Object type definitions for Nexus topology metadata."""
        if not self._topology_custom_object_types:
            self._topology_custom_object_types = _build_topology_custom_object_type_records()
        return list(self._topology_custom_object_types)

    def _get_topology_custom_object_type_fields(self) -> list[dict[str, Any]]:
        """Return NetBox Custom Object type-field definitions for Nexus topology metadata."""
        if not self._topology_custom_object_type_fields:
            self._topology_custom_object_type_fields = (
                _build_topology_custom_object_type_field_records()
            )
        return list(self._topology_custom_object_type_fields)

    def _fetch_switch_modules(
        self,
        switch_db_id: Any,
        fabric_name: str = "",
        switch_id: str = "",
    ) -> list[dict[str, Any]]:
        """Return normalized module records for one switch."""
        data = None

        if fabric_name and switch_id:
            try:
                data = self._get(
                    f"/api/v1/manage/fabrics/{quote(str(fabric_name), safe='')}"
                    f"/switches/{quote(str(switch_id), safe='')}/modules"
                )
            except Exception as exc:
                logger.debug(
                    "NDFC: manage module fetch failed for fabric %s switch %s: %s",
                    fabric_name,
                    switch_id,
                    exc,
                )

        if data is None and switch_db_id:
            try:
                data = self._get(
                    f"{self._API_BASE}/lan-fabric/rest/dashboard/switch/module?switchId={switch_db_id}"
                )
            except Exception as exc:
                logger.warning(
                    "NDFC: failed to fetch modules for switchDbID %s: %s",
                    switch_db_id,
                    exc,
                )
                return []

        modules = _flatten_module_payload(data)
        _debug_module_fetch_payload(
            switch_db_id=switch_db_id,
            fabric_name=fabric_name,
            switch_id=switch_id,
            payload=data,
            flattened=modules,
        )

        normalized: list[dict[str, Any]] = []
        for module in modules:
            if not isinstance(module, dict):
                continue
            enriched = self._enrich_module(module)
            if enriched:
                normalized.append(enriched)

        logger.debug(
            "NDFC normalized modules switch_db_id=%s fabric=%s switch_id=%s normalized_count=%d",
            switch_db_id,
            fabric_name,
            switch_id,
            len(normalized),
        )
        return normalized

    def _fetch_switch_interfaces(
        self,
        serial: str,
        fabric_name: str = "",
        switch_ip_address: str = "",
    ) -> list[dict]:
        """Return a list of normalised interface dicts for the given switch *serial*."""
        try:
            data = self._get(
                f"{self._API_BASE}/lan-fabric/rest/interface",
                params={"serialNumber": serial},
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
        deduped_data: list[dict[str, Any]] = []
        deduped_index: dict[str, int] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            normalized_name = _derive_interface_name(item).strip().lower()
            if normalized_name and normalized_name in deduped_index:
                idx = deduped_index[normalized_name]
                deduped_data[idx] = _merge_dashboard_interface(deduped_data[idx], item)
                continue
            if normalized_name:
                deduped_index[normalized_name] = len(deduped_data)
            deduped_data.append(item)
        data = deduped_data

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
        analyze_interfaces = self._get_analyze_interfaces(fabric_name, serial)
        detail_interfaces = self._get_interface_details(serial)
        analyze_index = _index_dashboard_interfaces(analyze_interfaces)
        detail_index = _index_dashboard_interfaces(detail_interfaces)
        interfaces: list[dict] = []
        seen_names: set[str] = set()
        for iface in data:
            if not isinstance(iface, dict):
                continue

            iface_name = _derive_interface_name(iface).strip().lower()
            analyze_iface = analyze_index.get(iface_name)
            detail_iface = detail_index.get(iface_name)
            enriched = self._enrich_interface(
                iface,
                switch_ip_address=switch_ip_address,
                analyze_iface=analyze_iface,
                detail_iface=detail_iface,
            )
            interfaces.append(enriched)
            if iface_name:
                seen_names.add(iface_name)
            if debug_enabled:
                nvpair_values = _flatten_nv_pairs(_safe_get(iface, "nvPairs"))
                merged_iface = _merge_dashboard_interface(
                    _merge_dashboard_interface(iface, analyze_iface),
                    detail_iface,
                )
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

        for extra_iface in analyze_interfaces:
            iface_name = _derive_interface_name(extra_iface).strip().lower()
            if not iface_name or iface_name in seen_names:
                continue
            interfaces.append(
                self._enrich_interface(
                    {},
                    switch_ip_address=switch_ip_address,
                    analyze_iface=extra_iface,
                    detail_iface=detail_index.get(iface_name),
                )
            )
            seen_names.add(iface_name)

        for extra_iface in detail_interfaces:
            iface_name = _derive_interface_name(extra_iface).strip().lower()
            if not iface_name or iface_name in seen_names:
                continue
            interfaces.append(
                self._enrich_interface(
                    {},
                    switch_ip_address=switch_ip_address,
                    analyze_iface=analyze_index.get(iface_name),
                    detail_iface=extra_iface,
                )
            )
            seen_names.add(iface_name)

        interfaces.sort(key=_interface_sort_key)
        _debug_interface_fetch_summary(serial, interfaces, fetched_count=len(data))
        return interfaces

    def _get_analyze_interfaces(self, fabric_name: str, switch_id: str) -> list[dict[str, Any]]:
        """Return Analyze interface records for one fabric/switch pair."""
        if not fabric_name or not switch_id:
            return []

        cache_key = (fabric_name, switch_id)
        if cache_key in self._analyze_cache:
            return self._analyze_cache[cache_key]

        try:
            data = self._get(
                f"{self._ANALYZE_BASE}/interfaces",
                params={"fabricName": fabric_name, "switchId": switch_id},
            )
        except Exception as exc:
            logger.warning(
                "Nexus Analyze interface enrichment failed for fabric %s switch %s: %s",
                fabric_name,
                switch_id,
                exc,
            )
            self._analyze_cache[cache_key] = []
            return []

        if isinstance(data, dict):
            data = data.get("interfaces", [])
        if not isinstance(data, list):
            data = []

        interfaces = [item for item in data if isinstance(item, dict)]
        self._analyze_cache[cache_key] = interfaces
        return interfaces

    def _get_interface_details(self, serial: str) -> list[dict[str, Any]]:
        """Return detailed NDFC interface records for one switch serial."""
        if not serial:
            return []

        if serial in self._detail_cache:
            return self._detail_cache[serial]

        try:
            data = self._get(
                f"{self._API_BASE}/lan-fabric/rest/interface/detail/filter",
                params={"serialNumber": serial},
            )
        except Exception as exc:
            logger.warning("NDFC interface detail enrichment failed for serial %s: %s", serial, exc)
            self._detail_cache[serial] = []
            return []

        if isinstance(data, dict):
            for key in ("interfaces", "items", "data"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                data = [data] if data.get("interfaceName") else []

        if not isinstance(data, list):
            data = []

        details = [item for item in data if isinstance(item, dict)]
        self._detail_cache[serial] = details
        return details

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
        tenant_name = _first_non_empty(switch, "hsTenantName", "tenantName", "tenant")
        vpc_domain_id = _derive_vpc_domain_id(switch)
        vpc_role = _derive_vpc_role(switch)
        vpc_peer_name = _derive_vpc_peer_name(switch)
        peer_link_interfaces = _derive_peer_link_interfaces(switch)
        peer_link_status = _first_non_empty(switch, "peerlinkState", "peerLinkState", "peerLinkStatus")

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
            "tenant_name":  tenant_name,
            "vpc_domain_id": vpc_domain_id,
            "vpc_role":     vpc_role,
            "vpc_peer_name": vpc_peer_name,
            "peer_link_interfaces": peer_link_interfaces,
            "peer_link_status": peer_link_status,
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
            "vpcDomain":    _safe_get(switch, "vpcDomain", "") or "",
            "peer":         _safe_get(switch, "peer", "") or "",
            "principal":    _safe_get(switch, "principal", ""),
            "sendIntf":     _safe_get(switch, "sendIntf", "") or "",
            "recvIntf":     _safe_get(switch, "recvIntf", "") or "",
            "peerlinkState": _safe_get(switch, "peerlinkState", "") or "",
            "hsTenantName": _safe_get(switch, "hsTenantName", "") or "",
            "switchDbID":   switch_db_id,
            "switch_db_id": switch_db_id,
        }

    def _enrich_module(self, module: dict[str, Any]) -> dict[str, Any] | None:
        """Return a normalized module dict or ``None`` when unsupported."""
        profile = _module_profile(module)
        if not profile:
            return None

        module_type_values = _safe_get(module, "moduleType", []) or []
        module_version_values = _safe_get(module, "moduleVersion", []) or []
        return {
            "profile": profile,
            "name": _first_non_empty(module, "name", "modelName", "serialNumber"),
            "bay_name": _module_bay_name(module),
            "position": _module_position(module),
            "model": _first_non_empty(module, "modelName", "name"),
            "serial": _first_non_empty(module, "serialNumber"),
            "manufacturer": "Cisco",
            "status": _first_non_empty(module, "operStatus"),
            "description": _first_non_empty(module, "name", "modelName"),
            "module_type": ", ".join(str(item) for item in module_type_values if str(item).strip()),
            "module_version": ", ".join(str(item) for item in module_version_values if str(item).strip()),
            "hardware_revision": _first_non_empty(module, "hardwareRevision"),
            "software_revision": _first_non_empty(module, "softwareRevision"),
            "asset_id": _first_non_empty(module, "assetId"),
            "raw_type": _first_non_empty(module, "type"),
        }

    def _enrich_interface(
        self,
        iface: dict,
        switch_ip_address: str = "",
        analyze_iface: dict[str, Any] | None = None,
        detail_iface: dict[str, Any] | None = None,
    ) -> dict:
        """Return a normalised dict for a single NDFC interface record."""
        nvpair_values = _flatten_nv_pairs(_safe_get(iface, "nvPairs"))
        analyze_iface = analyze_iface or {}
        detail_iface = detail_iface or {}
        detail_oper = _safe_get(detail_iface, "operData", {}) or {}
        source_iface = _merge_dashboard_interface(
            _merge_dashboard_interface(iface, analyze_iface),
            detail_iface,
        )

        def nv(*keys: str) -> str:
            return _nvpair_get_from_flattened(nvpair_values, *keys)

        if_name     = _first_non_empty(source_iface, "ifName", "interfaceName", "displayName", "name")
        name        = _derive_interface_name(source_iface)
        if_type     = (
            _first_non_empty(source_iface, "ifType", "interfaceType", "portType", "mediaType")
            or nv("ifType", "interfaceType", "portType", "mediaType")
        )
        admin_state = (
            _safe_get(detail_oper, "adminStatus", "")
            or _first_non_empty(source_iface, "adminState", "adminStatus")
            or nv("adminState", "adminStatus")
        )
        oper_status = (
            _safe_get(detail_oper, "operationalStatus", "")
            or _first_non_empty(source_iface, "operStatus", "operState", "operStatusStr", "operationalStatus")
            or nv("operStatus", "operState", "operStatusStr")
        )
        description = (
            _safe_get(detail_oper, "operDescription", "")
            or _safe_get(source_iface, "ifDescr", "")
            or _safe_get(source_iface, "description", "")
            or _safe_get(source_iface, "displayName", "")
            or nv("ifDescr", "description", "desc", "portDescription")
        )
        mac_address = (
            _first_non_empty(source_iface, "macAddress", "mac")
            or _first_non_empty(analyze_iface, "macAddress", "mac")
            or _first_non_empty(detail_oper, "macAddress", "mac")
            or nv("macAddress", "mac")
        )
        ip_address  = (
            _first_non_empty(source_iface, "ip", "ipv4Address", "ipAddress")
            or nv("ipAddress", "primaryIpAddress", "primaryIP", "ip")
        )
        ip_prefix   = nv("prefix", "prefixLength", "subnetPrefix", "mask", "subnetMask")
        speed_source = dict(source_iface)
        if _safe_get(detail_oper, "speed", "") and _is_blankish_speed(speed_source.get("speed")):
            speed_source["speed"] = _safe_get(detail_oper, "speed", "")
        if _safe_get(analyze_iface, "speed", "") and _is_blankish_speed(speed_source.get("speed")):
            speed_source["speed"] = _safe_get(analyze_iface, "speed", "")
        if _safe_get(analyze_iface, "adminSpeed", "") and _is_blankish_speed(speed_source.get("adminSpeed")):
            speed_source["adminSpeed"] = _safe_get(analyze_iface, "adminSpeed", "")
        if _safe_get(analyze_iface, "operSpeed", "") and _is_blankish_speed(speed_source.get("operSpeed")):
            speed_source["operSpeed"] = _safe_get(analyze_iface, "operSpeed", "")
        speed_mbps, _speed_str = _derive_interface_speed_mbps(speed_source, nvpair_values)
        mtu = (
            _parse_mtu(_safe_get(detail_oper, "mtu", None))
            or _parse_mtu(_safe_get(source_iface, "mtu", None))
            or _parse_mtu(nv("mtu"))
        )
        mode = _normalize_interface_mode(
            _first_non_empty(
                _safe_get(detail_iface, "configData", {}) or {},
                "mode",
            )
            or _first_non_empty(detail_oper, "mode")
            or _first_non_empty(
                analyze_iface,
                "operMode",
                "discoveredConfigMode",
                "intendedConfigMode",
                "portType",
            )
        )
        untagged_vlan_vid: int | None = None
        if mode == "access":
            access_vlan = _first_non_empty(
                _safe_get(_safe_get(detail_iface, "configData", {}) or {}, "networkOS", {}) or {},
                "accessVlan",
            ) or _first_non_empty(
                _safe_get(_safe_get(_safe_get(detail_iface, "configData", {}) or {}, "networkOS", {}) or {}, "policy", {}) or {},
                "accessVlan",
            )
            untagged_vlan_vid = _normalize_vlan_vid(access_vlan)
        if untagged_vlan_vid is None:
            native_vlan = _first_non_empty(source_iface, "nativeVlanId") or nv("nativeVlanId")
            untagged_vlan_vid = _normalize_vlan_vid(native_vlan)
        if mode == "tagged-all":
            tagged_vlan_vids = []
        else:
            tagged_vlan_vids = _parse_vlan_list(
                _first_non_empty(source_iface, "allowedVlans") or nv("allowedVlans")
            )
        if untagged_vlan_vid is not None:
            tagged_vlan_vids = [vid for vid in tagged_vlan_vids if vid != untagged_vlan_vid]
        lag_name    = _derive_lag_name(source_iface, nvpair_values=nvpair_values)
        vpc_name    = _derive_vpc_name(source_iface, nvpair_values=nvpair_values)
        vpc_parent_lag_name = _derive_vpc_parent_lag_name(
            source_iface,
            nvpair_values=nvpair_values,
            analyze_iface=analyze_iface,
            detail_iface=detail_iface,
        )
        channel_id  = _safe_get(detail_iface, "channelId", "") or _safe_get(analyze_iface, "channelId", "")
        if not lag_name and not vpc_name and channel_id not in ("", None, 0, "0"):
            lag_name = _normalize_port_channel_name(channel_id)
        mgmt_only   = if_type in {"INTERFACE_MANAGEMENT", "mgmt"} or name.lower().startswith("mgmt")
        if mgmt_only and not ip_address:
            ip_address = _normalize_host_ip_prefix(switch_ip_address)
        elif ip_address:
            ip_address = _normalize_ip_with_prefix(ip_address, ip_prefix)
        vrf_name = (
            _safe_get(detail_oper, "vrfName", "")
            or _first_non_empty(source_iface, "vrfName")
            or nv("vrfName", "vrf", "sourceVrf")
        )
        fabric_name = (
            _first_non_empty(source_iface, "fabricName", "fabricNameDisplay", "fabric")
            or nv("fabricName", "fabricNameDisplay", "fabric")
        )
        return {
            # --- normalised convenience fields ---
            "name":        name,
            "type":        _normalize_physical_iface_type(if_type, name, speed_mbps),
            "enabled":     _is_interface_enabled(admin_state, oper_status),
            "description": description,
            "lag_name":    lag_name,
            "vpc_name":    vpc_name,
            "vpc_parent_lag_name": vpc_parent_lag_name,
            "mgmt_only":   mgmt_only,
            "mac_address": mac_address.upper() if mac_address else "",
            "ip_address":  ip_address,
            "speed":       _netbox_speed_kbps(speed_mbps),
            "mtu":         mtu,
            "mode":        mode,
            "untagged_vlan_vid": untagged_vlan_vid,
            "tagged_vlan_vids": tagged_vlan_vids,
            "fabric_name": fabric_name,
            "vrf_name":    vrf_name,
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


def _netbox_speed_kbps(speed_mbps: int | None) -> int | None:
    """Convert Mbps into NetBox's expected Kbps unit for interface speeds."""
    if speed_mbps is None:
        return None
    return speed_mbps * 1000


def _parse_mtu(value: Any) -> int | None:
    """Return MTU as an integer when the input is parseable."""
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_vlan_list(value: Any) -> list[int]:
    """Expand comma/range VLAN strings like ``100,103,200-202`` into ints."""
    if value in (None, ""):
        return []
    result: list[int] = []
    seen: set[int] = set()
    for part in str(value).split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            try:
                start = int(start_text)
                end = int(end_text)
            except ValueError:
                continue
            if end < start:
                start, end = end, start
            for vid in range(start, end + 1):
                if 1 <= vid <= 4094 and vid not in seen:
                    result.append(vid)
                    seen.add(vid)
            continue
        try:
            vid = int(token)
        except ValueError:
            continue
        if 1 <= vid <= 4094 and vid not in seen:
            result.append(vid)
            seen.add(vid)
    return result


def _normalize_interface_mode(raw_mode: str | None) -> str | None:
    """Map NDFC switchport modes onto NetBox interface mode values."""
    value = str(raw_mode or "").strip().lower()
    if not value:
        return None
    if value == "access":
        return "access"
    if value in {"trunk", "tagged"}:
        return "tagged"
    if value in {"tagged-all", "tagged_all"}:
        return "tagged-all"
    return None


def _normalize_vlan_vid(value: Any) -> int | None:
    """Return a valid 802.1Q VLAN ID or ``None``."""
    if value in (None, ""):
        return None
    try:
        vid = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return vid if 1 <= vid <= 4094 else None
