"""Parse HCL mapping files into validated Python dataclasses.

python-hcl2 output structure for labeled blocks:
  ``source "vmware" {...}``  → ``{"source": [{"vmware": {body}}]}``
  ``object "host" {...}``    → ``{"object": [{"host": {body}}]}``

Unlabeled blocks:
  ``netbox {...}``           → ``{"netbox": [{body}]}``
  ``interface {...}``        → ``{"interface": [{body}]}``
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

import hcl2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _eval_config_str(value: Any) -> Any:
    """Evaluate env() references in a config-level attribute value.

    If *value* is a plain string that contains an ``env(`` call, it is eval'd
    with only the ``env`` built-in in scope so that environment variables are
    resolved at parse time.  All other values are returned unchanged.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if "env(" in stripped:
        try:
            return eval(stripped, {"__builtins__": {}}, {"env": lambda k, d="": os.environ.get(k, d)})
        except Exception:
            return value
    return value


def _labeled_list(raw_list: list) -> list[tuple[str, dict]]:
    """Convert a labeled block list into ``[(label, body), ...]`` pairs."""
    result = []
    for item in raw_list:
        if isinstance(item, dict):
            for label, body in item.items():
                result.append((label, body if isinstance(body, dict) else {}))
    return result


def _unlabeled_list(raw_list: list) -> list[dict]:
    """Return an unlabeled block list as ``[body, ...]``."""
    return [item for item in raw_list if isinstance(item, dict)]


def _bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "on")
    return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class CollectionConfig:
    """Describes one REST collection inside a ``source {}`` block.

    Used by the generic ``RestSource`` adapter to map collection names to API
    endpoints without any per-source Python code.
    """

    name: str
    endpoint: str
    list_key: str = ""
    detail_endpoint: str = ""
    detail_id_field: str = "uuid"


@dataclass
class SourceConfig:
    api_type: str
    url: str
    username: str = ""
    password: str = ""
    verify_ssl: bool = True
    extra: dict = field(default_factory=dict)
    collections: dict = field(default_factory=dict)  # name → CollectionConfig


@dataclass
class NetBoxConfig:
    url: str
    token: str
    cache: str = "none"
    cache_url: str = ""
    rate_limit: float = 0.0


@dataclass
class CollectorOptions:
    max_workers: int = 4
    dry_run: bool = False
    sync_tag: str = ""
    regex_dir: str = "./regex"
    extra_flags: dict = field(default_factory=dict)


@dataclass
class FieldConfig:
    name: str
    value: Optional[str] = None
    type: str = "scalar"        # scalar | fk | tags
    resource: Optional[str] = None
    lookup: Optional[dict] = None
    ensure: bool = False


@dataclass
class PrerequisiteConfig:
    name: str
    method: str
    args: dict = field(default_factory=dict)
    optional: bool = False


@dataclass
class IpAddressConfig:
    source_items: str = ""
    primary_if: Optional[str] = None
    enabled_if: Optional[str] = None
    fields: list[FieldConfig] = field(default_factory=list)


@dataclass
class TaggedVlanConfig:
    source_items: str = ""
    netbox_resource: str = "ipam.vlans"
    lookup_by: list[str] = field(default_factory=lambda: ["vid"])
    enabled_if: Optional[str] = None
    fields: list[FieldConfig] = field(default_factory=list)


@dataclass
class InterfaceConfig:
    source_items: str = ""
    enabled_if: Optional[str] = None
    fields: list[FieldConfig] = field(default_factory=list)
    ip_addresses: list[IpAddressConfig] = field(default_factory=list)
    tagged_vlans: list[TaggedVlanConfig] = field(default_factory=list)


@dataclass
class InventoryItemConfig:
    source_items: str = ""
    role: Optional[str] = None
    dedupe_by: Optional[str] = None
    enabled_if: Optional[str] = None
    fields: list[FieldConfig] = field(default_factory=list)


@dataclass
class DiskConfig:
    source_items: str = ""
    enabled_if: Optional[str] = None
    fields: list[FieldConfig] = field(default_factory=list)


@dataclass
class ModuleConfig:
    """Describes one ``module {}`` block inside an ``object {}`` block.

    Each item in *source_items* becomes a NetBox Module installed in a
    ModuleBay on the parent device.  The engine calls ensure_module_bay (and
    optionally ensure_module_bay_template on the device type) then
    ensure_module_type before upserting the Module.

    Required fields (resolved from source data via ``field`` sub-blocks):
      bay_name     — the slot / bay label (e.g. "CPU Socket 1")
      model        — the module-type model string

    Optional fields:
      position     — numeric or string position passed to the bay
      serial       — module serial number
      manufacturer — manufacturer name (resolved to ID automatically)

    Optional attribute sub-blocks (resolved from source data via ``attribute``
    sub-blocks) are applied to the ModuleType record after the profile is
    assigned.  Attribute names must match the keys declared in the profile's
    JSON Schema in NetBox.
    """

    source_items: str = ""
    profile: Optional[str] = None   # module type profile name (informational)
    dedupe_by: Optional[str] = None
    enabled_if: Optional[str] = None
    fields: list[FieldConfig] = field(default_factory=list)
    attributes: list[FieldConfig] = field(default_factory=list)


@dataclass
class ObjectConfig:
    name: str
    source_collection: str
    netbox_resource: str
    lookup_by: list[str] = field(default_factory=lambda: ["name"])
    max_workers: Optional[int] = None
    prerequisites: list[PrerequisiteConfig] = field(default_factory=list)
    fields: list[FieldConfig] = field(default_factory=list)
    interfaces: list[InterfaceConfig] = field(default_factory=list)
    inventory_items: list[InventoryItemConfig] = field(default_factory=list)
    disks: list[DiskConfig] = field(default_factory=list)
    modules: list[ModuleConfig] = field(default_factory=list)


@dataclass
class CollectorConfig:
    source: SourceConfig
    netbox: NetBoxConfig
    collector: CollectorOptions
    objects: list[ObjectConfig] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Block parsers
# ---------------------------------------------------------------------------

def _parse_fields(raw: list) -> list[FieldConfig]:
    configs = []
    for label, body in _labeled_list(raw):
        ftype = body.get("type", "scalar")
        lookup = body.get("lookup")
        if isinstance(lookup, dict):
            # lookup values may be expression strings — keep them as-is
            pass
        configs.append(FieldConfig(
            name=label,
            value=body.get("value"),
            type=ftype,
            resource=body.get("resource"),
            lookup=lookup,
            ensure=_bool(body.get("ensure", False)),
        ))
    return configs


def _parse_prerequisites(raw: list) -> list[PrerequisiteConfig]:
    configs = []
    for label, body in _labeled_list(raw):
        args = body.get("args", {})
        if not isinstance(args, dict):
            args = {}
        configs.append(PrerequisiteConfig(
            name=label,
            method=body.get("method", ""),
            args=args,
            optional=_bool(body.get("optional", False)),
        ))
    return configs


def _parse_ip_addresses(raw: list) -> list[IpAddressConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(IpAddressConfig(
            source_items=body.get("source_items", ""),
            primary_if=body.get("primary_if"),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
        ))
    return configs


def _parse_tagged_vlans(raw: list) -> list[TaggedVlanConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        lookup_by = body.get("lookup_by", ["vid"])
        if isinstance(lookup_by, str):
            lookup_by = [lookup_by]
        configs.append(TaggedVlanConfig(
            source_items=body.get("source_items", ""),
            netbox_resource=body.get("netbox_resource", "ipam.vlans"),
            lookup_by=list(lookup_by),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
        ))
    return configs


def _parse_interfaces(raw: list) -> list[InterfaceConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(InterfaceConfig(
            source_items=body.get("source_items", ""),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
            ip_addresses=_parse_ip_addresses(body.get("ip_address", [])),
            tagged_vlans=_parse_tagged_vlans(body.get("tagged_vlan", [])),
        ))
    return configs


def _parse_inventory_items(raw: list) -> list[InventoryItemConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(InventoryItemConfig(
            source_items=body.get("source_items", ""),
            role=body.get("role"),
            dedupe_by=body.get("dedupe_by"),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
        ))
    return configs


def _parse_disks(raw: list) -> list[DiskConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(DiskConfig(
            source_items=body.get("source_items", ""),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
        ))
    return configs


def _parse_modules(raw: list) -> list[ModuleConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(ModuleConfig(
            source_items=body.get("source_items", ""),
            profile=body.get("profile"),
            dedupe_by=body.get("dedupe_by"),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
            attributes=_parse_fields(body.get("attribute", [])),
        ))
    return configs


def _parse_objects(raw: list) -> list[ObjectConfig]:
    objects = []
    for label, body in _labeled_list(raw):
        lookup_by = body.get("lookup_by", ["name"])
        if isinstance(lookup_by, str):
            lookup_by = [lookup_by]
        max_workers_raw = body.get("max_workers")
        max_workers = _int(max_workers_raw) if max_workers_raw is not None else None

        objects.append(ObjectConfig(
            name=label,
            source_collection=body.get("source_collection", ""),
            netbox_resource=body.get("netbox_resource", ""),
            lookup_by=list(lookup_by),
            max_workers=max_workers,
            prerequisites=_parse_prerequisites(body.get("prerequisite", [])),
            fields=_parse_fields(body.get("field", [])),
            interfaces=_parse_interfaces(body.get("interface", [])),
            inventory_items=_parse_inventory_items(body.get("inventory_item", [])),
            disks=_parse_disks(body.get("disk", [])),
            modules=_parse_modules(body.get("module", [])),
        ))
    return objects


# ---------------------------------------------------------------------------
# Top-level loader
# ---------------------------------------------------------------------------

def load_config(mapping_path: str) -> CollectorConfig:
    """Parse an HCL mapping file and return a validated ``CollectorConfig``."""
    with open(mapping_path, "r") as fh:
        raw = hcl2.load(fh)

    # --- source ---
    source_list = raw.get("source", [])
    if not source_list:
        raise ValueError("HCL file is missing a 'source' block")
    source_label, source_body = _labeled_list(source_list)[0]

    # Parse optional collection {} labeled sub-blocks (used by RestSource)
    _SOURCE_SCALAR_KEYS = {"api_type", "url", "username", "password", "verify_ssl", "auth", "auth_header"}
    raw_collections = source_body.get("collection", [])
    collections: dict[str, CollectionConfig] = {}
    for col_label, col_body in _labeled_list(raw_collections):
        collections[col_label] = CollectionConfig(
            name=col_label,
            endpoint=col_body.get("endpoint", ""),
            list_key=col_body.get("list_key", ""),
            detail_endpoint=col_body.get("detail_endpoint", ""),
            detail_id_field=col_body.get("detail_id_field", "uuid"),
        )

    source_cfg = SourceConfig(
        api_type=_eval_config_str(source_body.get("api_type", source_label)),
        url=_eval_config_str(source_body.get("url", "")),
        username=_eval_config_str(source_body.get("username", "")),
        password=_eval_config_str(source_body.get("password", "")),
        verify_ssl=_bool(
            _eval_config_str(source_body.get("verify_ssl", "true")), default=True
        ),
        extra={
            k: _eval_config_str(v)
            for k, v in source_body.items()
            if k not in _SOURCE_SCALAR_KEYS and k != "collection"
        },
        collections=collections,
    )

    # --- netbox ---
    netbox_list = raw.get("netbox", [])
    if not netbox_list:
        raise ValueError("HCL file is missing a 'netbox' block")
    netbox_body = _unlabeled_list(netbox_list)[0]
    netbox_cfg = NetBoxConfig(
        url=_eval_config_str(netbox_body.get("url", "")),
        token=_eval_config_str(netbox_body.get("token", "")),
        cache=_eval_config_str(netbox_body.get("cache", "none")),
        cache_url=_eval_config_str(netbox_body.get("cache_url", "")),
        rate_limit=_float(_eval_config_str(netbox_body.get("rate_limit", 0))),
    )

    # --- collector ---
    _KNOWN_COLLECTOR_KEYS = {"max_workers", "dry_run", "sync_tag", "regex_dir"}
    col_body = {}
    collector_list = raw.get("collector", [])
    if collector_list:
        col_body = _unlabeled_list(collector_list)[0]

    extra_flags = {}
    for k, v in col_body.items():
        if k not in _KNOWN_COLLECTOR_KEYS:
            resolved = _eval_config_str(v)
            extra_flags[k] = _bool(resolved) if isinstance(resolved, str) else resolved

    collector_cfg = CollectorOptions(
        max_workers=_int(col_body.get("max_workers", 4), default=4),
        dry_run=_bool(_eval_config_str(col_body.get("dry_run", False))),
        sync_tag=_eval_config_str(col_body.get("sync_tag", "")),
        regex_dir=_eval_config_str(col_body.get("regex_dir", "./regex")),
        extra_flags=extra_flags,
    )

    # --- objects ---
    objects = _parse_objects(raw.get("object", []))

    return CollectorConfig(
        source=source_cfg,
        netbox=netbox_cfg,
        collector=collector_cfg,
        objects=objects,
    )
