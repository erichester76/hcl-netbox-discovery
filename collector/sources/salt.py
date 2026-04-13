"""Salt grains artifact data source adapter.

This MVP adapter reads exported Salt grains/artifact JSON and normalises it
into a common ``hosts`` collection shape suitable for HCL mapping.

Supported collection
--------------------
``"hosts"`` — returns one record per Salt minion. Each host dict contains a
nested ``interfaces`` list, and each interface contains ``ip_addresses``.

Supported artifact shapes
-------------------------
- ``{"minion-id": {...grains...}, ...}``
- ``{"return": {"minion-id": {...grains...}, ...}}``
- ``{"return": [{"minion-id": {...grains...}}, ...]}``
- ``[{"id": "minion-id", "grains": {...}}, ...]``

The adapter intentionally does not shell out to ``salt`` or ``salt-call``.
Operators should export the desired facts through their existing Salt workflow
and point ``artifact_path`` at the resulting JSON file.
"""

from __future__ import annotations

import ipaddress
import json
from pathlib import Path
from typing import Any

from .base import DataSource

_IFACE_TYPE_PATTERNS: list[tuple[str, str]] = [
    ("bond", "lag"),
    ("team", "lag"),
    ("br", "bridge"),
    ("lo", "virtual"),
    ("virbr", "virtual"),
    ("docker", "virtual"),
    ("veth", "virtual"),
    ("tun", "virtual"),
    ("tap", "virtual"),
    ("ib", "infiniband"),
    ("wl", "ieee802.11a"),
    ("en", "1000base-t"),
    ("eth", "1000base-t"),
]


def _short_name(value: str, fallback: str) -> str:
    """Return the host name stripped to the first DNS label."""
    text = (value or "").strip()
    if not text:
        return fallback
    return text.split(".", 1)[0][:64] or fallback


def _infer_iface_type(name: str) -> str:
    """Infer a NetBox interface type slug from a Salt interface name."""
    lowered = (name or "").lower()
    for prefix, iface_type in _IFACE_TYPE_PATTERNS:
        if lowered.startswith(prefix):
            return iface_type
    return "other"


def _is_ip_address(value: Any) -> bool:
    """Return ``True`` when *value* looks like an IPv4/IPv6 address."""
    text = str(value or "").strip()
    if not text:
        return False
    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


def _clean_ip_list(values: Any) -> list[str]:
    """Normalise a Salt IP list into a de-duplicated list of real addresses."""
    if not isinstance(values, list):
        return []
    cleaned: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not _is_ip_address(text):
            continue
        if text in {"127.0.0.1", "::1"}:
            continue
        if text not in cleaned:
            cleaned.append(text)
    return cleaned


def _ip_dicts(values: list[str]) -> list[dict[str, Any]]:
    """Convert IP strings into the nested shape used by mapping files."""
    result: list[dict[str, Any]] = []
    for value in values:
        family = 6 if ":" in value else 4
        suffix = "/128" if family == 6 else "/32"
        result.append(
            {
                "address": f"{value}{suffix}",
                "family": family,
                "status": "active",
            }
        )
    return result


def _iter_records(payload: Any) -> list[tuple[str, dict[str, Any]]]:
    """Flatten supported Salt artifact shapes into ``[(minion_id, record)]``."""
    if isinstance(payload, dict):
        returned = payload.get("return")
        if returned is not None:
            return _iter_records(returned)
        if "hosts" in payload and isinstance(payload["hosts"], list):
            return _iter_records(payload["hosts"])
        return [
            (str(minion_id), record)
            for minion_id, record in payload.items()
            if isinstance(record, dict)
        ]

    if isinstance(payload, list):
        flattened: list[tuple[str, dict[str, Any]]] = []
        for item in payload:
            if isinstance(item, dict) and "id" in item:
                flattened.append((str(item["id"]), item))
                continue
            if isinstance(item, dict):
                flattened.extend(_iter_records(item))
        return flattened

    return []


def _normalise_host(minion_id: str, record: dict[str, Any]) -> dict[str, Any]:
    """Return one normalised Salt host record."""
    grains = record.get("grains") if isinstance(record.get("grains"), dict) else record
    fqdn = str(grains.get("fqdn") or "")
    host = str(grains.get("host") or "")
    hostname = fqdn or host or minion_id
    name = _short_name(hostname, minion_id[:64] or "unknown")

    ip_interfaces = grains.get("ip_interfaces", {})
    hwaddr_interfaces = grains.get("hwaddr_interfaces", {})
    interface_names = set()
    if isinstance(ip_interfaces, dict):
        interface_names.update(str(name) for name in ip_interfaces)
    if isinstance(hwaddr_interfaces, dict):
        interface_names.update(str(name) for name in hwaddr_interfaces)

    interfaces: list[dict[str, Any]] = []
    all_ips: list[str] = []
    for iface_name in sorted(interface_names):
        iface_ips = _clean_ip_list(ip_interfaces.get(iface_name, []))
        all_ips.extend(ip for ip in iface_ips if ip not in all_ips)
        interfaces.append(
            {
                "name": iface_name,
                "type": _infer_iface_type(iface_name),
                "enabled": True,
                "mac_address": str(hwaddr_interfaces.get(iface_name) or ""),
                "ip_addresses": _ip_dicts(iface_ips),
            }
        )

    ipv4 = _clean_ip_list(grains.get("ipv4", []))
    ipv6 = _clean_ip_list(grains.get("ipv6", []))
    for address in ipv4 + ipv6:
        if address not in all_ips:
            all_ips.append(address)

    manufacturer = (
        grains.get("manufacturer")
        or grains.get("system_manufacturer")
        or grains.get("vendor")
        or ""
    )
    model = grains.get("productname") or grains.get("model") or ""
    serial = grains.get("serialnumber") or grains.get("serial") or ""
    os_name = grains.get("os") or grains.get("kernel") or ""
    os_release = grains.get("osrelease") or grains.get("kernelrelease") or ""

    return {
        "id": minion_id,
        "name": name,
        "hostname": hostname,
        "fqdn": fqdn,
        "os": os_name,
        "os_family": grains.get("os_family", ""),
        "osrelease": os_release,
        "kernel": grains.get("kernelrelease", ""),
        "virtual": grains.get("virtual", ""),
        "manufacturer": manufacturer,
        "model": model,
        "serial": serial,
        "platform": " ".join(part for part in [os_name, os_release] if part).strip(),
        "ipv4": ipv4,
        "ipv6": ipv6,
        "ip_addresses": _ip_dicts(all_ips),
        "mac_addresses": [iface["mac_address"] for iface in interfaces if iface["mac_address"]],
        "interfaces": interfaces,
        "status": "active",
        "source_system": "salt",
        "raw": record,
    }


class SaltSource(DataSource):
    """Artifact-backed Salt source adapter."""

    def __init__(self) -> None:
        self._config: Any | None = None
        self._artifact_path: Path | None = None

    def connect(self, config: Any) -> None:
        """Store the Salt config and validate the artifact path."""
        self._config = config
        extra = config.extra or {}
        artifact_path = str(extra.get("artifact_path") or config.url or "").strip()
        if not artifact_path:
            raise ValueError(
                "SaltSource requires source.url or source.artifact_path to point "
                "at an exported Salt JSON artifact"
            )
        self._artifact_path = Path(artifact_path)
        if not self._artifact_path.exists():
            raise FileNotFoundError(f"Salt artifact not found: {self._artifact_path}")

    def get_objects(self, collection: str) -> list:
        """Return the normalised host list for *collection*."""
        if self._artifact_path is None:
            raise RuntimeError("SaltSource: connect() has not been called")
        if collection.lower() != "hosts":
            raise ValueError(
                f"SaltSource: unknown collection {collection!r}. Supported: ['hosts']"
            )

        payload = json.loads(self._artifact_path.read_text(encoding="utf-8"))
        return [
            _normalise_host(minion_id, record)
            for minion_id, record in _iter_records(payload)
        ]

    def close(self) -> None:
        """Release file-backed state."""
        self._config = None
        self._artifact_path = None
