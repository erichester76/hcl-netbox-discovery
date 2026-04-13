"""Ansible facts artifact data source adapter.

This MVP adapter reads exported Ansible facts or fact-cache JSON and
normalises it into a common ``hosts`` collection shape suitable for HCL
mapping.

Supported collection
--------------------
``"hosts"`` — returns one record per Ansible host. Each host dict contains a
nested ``interfaces`` list, and each interface contains ``ip_addresses``.

Supported artifact shapes
-------------------------
- A directory of per-host JSON fact cache files
- ``{"_meta": {"hostvars": {...}}}`` inventory export JSON
- ``{"hosts": [{...}, ...]}``
- ``[{"inventory_hostname": "host", ...}, ...]``
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
    """Infer a NetBox interface type slug from an Ansible interface name."""
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
    """Normalise a candidate IP list into de-duplicated real addresses."""
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


def _interface_var_name(name: str) -> str:
    """Return the hostvar key used for an interface details record."""
    return f"ansible_{name.replace('-', '_')}"


def _extract_interface_ips(details: dict[str, Any]) -> list[str]:
    """Return all usable IPs from an Ansible interface detail record."""
    result: list[str] = []

    ipv4 = details.get("ipv4")
    if isinstance(ipv4, dict):
        address = ipv4.get("address")
        if _is_ip_address(address) and address not in {"127.0.0.1", "::1"}:
            result.append(str(address))

    ipv6 = details.get("ipv6")
    if isinstance(ipv6, list):
        for item in ipv6:
            if not isinstance(item, dict):
                continue
            address = item.get("address")
            if _is_ip_address(address) and address not in {"127.0.0.1", "::1"}:
                text = str(address)
                if text not in result:
                    result.append(text)

    return result


def _iter_hostvars(payload: Any) -> list[tuple[str, dict[str, Any]]]:
    """Flatten supported Ansible artifact shapes into ``[(host, hostvars)]``."""
    if isinstance(payload, dict):
        meta = payload.get("_meta", {})
        if isinstance(meta, dict) and isinstance(meta.get("hostvars"), dict):
            return [
                (str(host), vars_dict)
                for host, vars_dict in meta["hostvars"].items()
                if isinstance(vars_dict, dict)
            ]
        if isinstance(payload.get("hosts"), list):
            return _iter_hostvars(payload["hosts"])
        if "inventory_hostname" in payload or "ansible_facts" in payload:
            facts = payload.get("ansible_facts")
            if not isinstance(facts, dict):
                facts = {}
            host = str(
                payload.get("inventory_hostname")
                or payload.get("ansible_hostname")
                or facts.get("ansible_hostname")
                or facts.get("ansible_fqdn")
                or ""
            )
            if host:
                return [(host, payload)]
            if "ansible_facts" in payload:
                return []
        return [
            (str(host), vars_dict)
            for host, vars_dict in payload.items()
            if isinstance(vars_dict, dict)
        ]

    if isinstance(payload, list):
        result: list[tuple[str, dict[str, Any]]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            host = str(item.get("inventory_hostname") or item.get("ansible_hostname") or "")
            if host:
                result.append((host, item))
        return result

    return []


def _normalise_host(hostname: str, record: dict[str, Any]) -> dict[str, Any]:
    """Return one normalised Ansible host record."""
    facts = record.get("ansible_facts") if isinstance(record.get("ansible_facts"), dict) else record
    fqdn = str(facts.get("ansible_fqdn") or "")
    host = str(facts.get("ansible_hostname") or hostname or "")
    display_name = _short_name(fqdn or host, hostname[:64] or "unknown")

    interface_names = facts.get("ansible_interfaces", [])
    interfaces: list[dict[str, Any]] = []
    all_ips: list[str] = []
    if isinstance(interface_names, list):
        valid_names = {
            str(name).strip()
            for name in interface_names
            if str(name or "").strip()
        }
        for iface_name in sorted(valid_names):
            details = facts.get(_interface_var_name(iface_name), {})
            if not isinstance(details, dict):
                details = {}
            iface_ips = _extract_interface_ips(details)
            all_ips.extend(ip for ip in iface_ips if ip not in all_ips)
            interfaces.append(
                {
                    "name": iface_name,
                    "type": _infer_iface_type(iface_name),
                    "enabled": bool(details.get("active", True)),
                    "mac_address": str(details.get("macaddress") or ""),
                    "ip_addresses": _ip_dicts(iface_ips),
                }
            )

    ipv4 = _clean_ip_list(facts.get("ansible_all_ipv4_addresses", []))
    ipv6 = _clean_ip_list(facts.get("ansible_all_ipv6_addresses", []))
    for address in ipv4 + ipv6:
        if address not in all_ips:
            all_ips.append(address)

    os_name = facts.get("ansible_distribution") or facts.get("ansible_system") or ""
    os_release = (
        facts.get("ansible_distribution_version")
        or facts.get("ansible_kernel")
        or ""
    )

    return {
        "id": hostname,
        "name": display_name,
        "hostname": host or hostname,
        "fqdn": fqdn,
        "os": os_name,
        "os_family": facts.get("ansible_os_family", ""),
        "osrelease": os_release,
        "kernel": facts.get("ansible_kernel", ""),
        "virtual": facts.get("ansible_virtualization_type", ""),
        "manufacturer": facts.get("ansible_system_vendor", ""),
        "model": facts.get("ansible_product_name", ""),
        "serial": facts.get("ansible_product_serial", "") or facts.get("ansible_serial_number", ""),
        "platform": " ".join(part for part in [os_name, os_release] if part).strip(),
        "ipv4": ipv4,
        "ipv6": ipv6,
        "ip_addresses": _ip_dicts(all_ips),
        "mac_addresses": [iface["mac_address"] for iface in interfaces if iface["mac_address"]],
        "interfaces": interfaces,
        "status": "active",
        "source_system": "ansible",
        "raw": record,
    }


class AnsibleSource(DataSource):
    """Artifact-backed Ansible source adapter."""

    def __init__(self) -> None:
        self._config: Any | None = None
        self._artifact_path: Path | None = None

    def connect(self, config: Any) -> None:
        """Store the Ansible config and validate the artifact path."""
        self._config = config
        extra = config.extra or {}
        artifact_path = str(extra.get("artifact_path") or config.url or "").strip()
        if not artifact_path:
            raise ValueError(
                "AnsibleSource requires source.url or source.artifact_path to point "
                "at exported Ansible facts"
            )
        self._artifact_path = Path(artifact_path)
        if not self._artifact_path.exists():
            raise FileNotFoundError(f"Ansible artifact path not found: {self._artifact_path}")

    def get_objects(self, collection: str) -> list:
        """Return the normalised host list for *collection*."""
        if self._artifact_path is None:
            raise RuntimeError("AnsibleSource: connect() has not been called")
        if collection.lower() != "hosts":
            raise ValueError(
                f"AnsibleSource: unknown collection {collection!r}. Supported: ['hosts']"
            )

        records: list[tuple[str, dict[str, Any]]] = []
        if self._artifact_path.is_dir():
            for entry in sorted(self._artifact_path.glob("*.json")):
                payload = json.loads(entry.read_text(encoding="utf-8"))
                file_records = _iter_hostvars(payload)
                if file_records:
                    records.extend(file_records)
                    continue
                if isinstance(payload, dict):
                    fallback_host = entry.stem
                    records.append((fallback_host, payload))
        else:
            payload = json.loads(self._artifact_path.read_text(encoding="utf-8"))
            records.extend(_iter_hostvars(payload))

        return [_normalise_host(hostname, record) for hostname, record in records]

    def close(self) -> None:
        """Release file-backed state."""
        self._config = None
        self._artifact_path = None
