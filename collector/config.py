"""Parse HCL mapping files into validated Python dataclasses.

python-hcl2 output structure for labeled blocks:
  ``source "vmware" {...}``  → ``{"source": [{"vmware": {body}}]}``
  ``object "host" {...}``    → ``{"object": [{"host": {body}}]}``

Unlabeled blocks:
  ``netbox {...}``           → ``{"netbox": [{body}]}``
  ``interface {...}``        → ``{"interface": [{body}]}``
  ``iterator {...}``         → ``{"iterator": [{body}]}``
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
    resolved at parse time.  The ``env()`` function checks the DB config_settings
    table first, then falls back to the OS environment.  All other values are
    returned unchanged.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if "env(" in stripped:
        try:
            from .db import get_config as _get_config  # noqa: PLC0415

            def _env_fn(k: str, d: str = "") -> str:
                return _get_config(k, d)
        except ImportError:
            def _env_fn(k: str, d: str = "") -> str:  # type: ignore[misc]
                return os.environ.get(k, d)
        try:
            return eval(stripped, {"__builtins__": {}}, {"env": _env_fn})
        except Exception:
            return value
    return value


def _eval_config_str_with_overrides(value: Any, overrides: dict) -> Any:
    """Evaluate env() references with iterator variable overrides.

    Identical to :func:`_eval_config_str` except that *overrides* is checked
    first before the database / OS environment.  Used by
    :func:`build_source_config` to inject per-iteration values.
    """
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    if "env(" in stripped:
        try:
            from .db import get_config as _get_config  # noqa: PLC0415

            def _env_fn(k: str, d: str = "") -> str:
                if k in overrides:
                    return str(overrides[k])
                return _get_config(k, d)
        except ImportError:
            def _env_fn(k: str, d: str = "") -> str:  # type: ignore[misc]
                if k in overrides:
                    return str(overrides[k])
                return os.environ.get(k, d)
        try:
            return eval(stripped, {"__builtins__": {}}, {"env": _env_fn})
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
class IteratorConfig:
    """One ``iterator {}`` block inside a ``collector {}`` block.

    Each key maps to a list of string values.  The engine iterates through
    the lists in lock-step (zip), running a full collection pass for each
    index.  The variable names are used as env-var overrides when evaluating
    ``env()`` calls in the ``source {}`` block.

    Example HCL::

        collector {
          iterator {
            VCENTER_URL  = ["vc1.example.com", "vc2.example.com"]
            VCENTER_USER = ["admin", "admin"]
            VCENTER_PASS = ["pass1", "pass2"]
          }
        }
    """

    variables: dict  # var_name → list of values

    def __len__(self) -> int:
        """Return the number of iterations (shortest list length)."""
        if not self.variables:
            return 0
        return min(
            len(v) if isinstance(v, list) else 1
            for v in self.variables.values()
        )

    def get_row(self, index: int) -> dict:
        """Return the variable values for iteration *index*."""
        row: dict = {}
        for k, v in self.variables.items():
            if isinstance(v, list):
                if index < len(v):
                    row[k] = v[index]
            else:
                row[k] = v
        return row


@dataclass
class NetBoxConfig:
    url: str
    token: str
    cache: str = "none"
    cache_url: str = ""
    cache_ttl: int = 300
    prewarm_sentinel_ttl: Optional[int] = None
    rate_limit: float = 0.0
    rate_limit_burst: int = 1
    retry_attempts: int = 3
    retry_initial_delay: float = 0.3
    retry_backoff_factor: float = 2.0
    retry_max_delay: float = 15.0
    retry_jitter: float = 0.0
    retry_on_4xx: str = "408,409,425,429"
    cache_key_prefix: str = "nbx:"
    branch: Optional[str] = None


@dataclass
class CollectorOptions:
    max_workers: int = 4
    dry_run: bool = False
    sync_tag: str = ""
    regex_dir: str = "./regex"
    extra_flags: dict = field(default_factory=dict)
    iterators: list[IteratorConfig] = field(default_factory=list)


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
    oob_if: Optional[str] = None
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
class PowerInputConfig:
    """Describes a ``power_input {}`` sub-block inside a ``module {}`` block.

    When present, the engine will create a ``dcim.power_ports`` record on the
    installed module after each successful module upsert.

    Both attributes are expression strings evaluated in the same resolver
    scope as the parent module's field expressions (i.e. ``source()`` accesses
    the current source item).

    Attributes:
      name — expression for the power port name
              (e.g. ``"'Power Input' + when(source('slot'), ' ' + str(source('slot')), '')"``).
      type — expression for the IEC 60320 connector type slug
              (e.g. ``"when(int(source('outputWatts') or 0) > 1800, 'iec-60320-c20', 'iec-60320-c14')"``).
              Defaults to ``"iec-60320-c14"`` when not specified or when the
              expression evaluates to a falsy value.
    """

    name: Optional[str] = None
    type: Optional[str] = None


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

    Optional sub-blocks:
      power_input  — if present, a ``dcim.power_ports`` record is created on
                     the installed module after each upsert.
                     
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
    power_input: Optional[PowerInputConfig] = None
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
    # Raw HCL source body kept for per-iteration re-evaluation (iterator feature)
    raw_source_body: dict = field(default_factory=dict)
    source_label: str = ""


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
            oob_if=body.get("oob_if"),
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


def _parse_power_input(raw: list) -> Optional[PowerInputConfig]:
    """Parse an optional ``power_input {}`` sub-block from a module block."""
    if not raw:
        return None
    bodies = _unlabeled_list(raw)
    if not bodies:
        return None
    pi_body = bodies[0]
    return PowerInputConfig(
        name=pi_body.get("name"),
        type=pi_body.get("type"),
    )


def _parse_modules(raw: list) -> list[ModuleConfig]:
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(ModuleConfig(
            source_items=body.get("source_items", ""),
            profile=body.get("profile"),
            dedupe_by=body.get("dedupe_by"),
            enabled_if=body.get("enabled_if"),
            fields=_parse_fields(body.get("field", [])),
            power_input=_parse_power_input(body.get("power_input", [])),
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


def _parse_iterators(raw: list) -> list[IteratorConfig]:
    """Parse ``iterator {}`` unlabeled blocks from a collector block body."""
    configs = []
    for body in _unlabeled_list(raw):
        configs.append(IteratorConfig(variables=body))
    return configs


def build_source_config(
    source_body: dict,
    source_label: str,
    overrides: Optional[dict] = None,
) -> SourceConfig:
    """Build a :class:`SourceConfig` from a raw HCL source block body.

    Parameters
    ----------
    source_body:
        The raw dict from python-hcl2 for the ``source "label" {}`` block.
    source_label:
        The label (e.g. ``"vmware"``) used as a fallback for ``api_type``.
    overrides:
        Optional mapping of variable names to values that take precedence
        when ``env()`` calls are evaluated.  Supplied per-iteration by the
        engine when :attr:`CollectorOptions.iterators` is non-empty.
    """
    _eval = (
        lambda v: _eval_config_str_with_overrides(v, overrides)
        if overrides
        else _eval_config_str(v)
    )

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

    return SourceConfig(
        api_type=_eval(source_body.get("api_type", source_label)),
        url=_eval(source_body.get("url", "")),
        username=_eval(source_body.get("username", "")),
        password=_eval(source_body.get("password", "")),
        verify_ssl=_bool(_eval(source_body.get("verify_ssl", "true")), default=True),
        extra={
            k: _eval(v)
            for k, v in source_body.items()
            if k not in _SOURCE_SCALAR_KEYS and k != "collection"
        },
        collections=collections,
    )


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

    source_cfg = build_source_config(source_body, source_label)

    # --- netbox ---
    netbox_list = raw.get("netbox", [])
    if not netbox_list:
        raise ValueError("HCL file is missing a 'netbox' block")
    netbox_body = _unlabeled_list(netbox_list)[0]
    _raw_sentinel_ttl = _eval_config_str(netbox_body.get("prewarm_sentinel_ttl", ""))
    _raw_branch = _eval_config_str(netbox_body.get("branch", ""))
    netbox_cfg = NetBoxConfig(
        url=_eval_config_str(netbox_body.get("url", "")),
        token=_eval_config_str(netbox_body.get("token", "")),
        cache=_eval_config_str(netbox_body.get("cache", "none")),
        cache_url=_eval_config_str(netbox_body.get("cache_url", "")),
        cache_ttl=_int(_eval_config_str(netbox_body.get("cache_ttl", 300))),
        prewarm_sentinel_ttl=_int(_raw_sentinel_ttl) if _raw_sentinel_ttl else None,
        rate_limit=_float(_eval_config_str(netbox_body.get("rate_limit", 0))),
        rate_limit_burst=_int(_eval_config_str(netbox_body.get("rate_limit_burst", 1)), default=1),
        retry_attempts=_int(_eval_config_str(netbox_body.get("retry_attempts", 3)), default=3),
        retry_initial_delay=_float(_eval_config_str(netbox_body.get("retry_initial_delay", 0.3)), default=0.3),
        retry_backoff_factor=_float(_eval_config_str(netbox_body.get("retry_backoff_factor", 2.0)), default=2.0),
        retry_max_delay=_float(_eval_config_str(netbox_body.get("retry_max_delay", 15.0)), default=15.0),
        retry_jitter=_float(_eval_config_str(netbox_body.get("retry_jitter", 0.0)), default=0.0),
        retry_on_4xx=_eval_config_str(netbox_body.get("retry_on_4xx", "408,409,425,429")),
        cache_key_prefix=_eval_config_str(netbox_body.get("cache_key_prefix", "nbx:")),
        branch=_raw_branch if _raw_branch else None,
    )

    # --- collector ---
    _KNOWN_COLLECTOR_KEYS = {"max_workers", "dry_run", "sync_tag", "regex_dir", "iterator"}
    col_body = {}
    collector_list = raw.get("collector", [])
    if collector_list:
        col_body = _unlabeled_list(collector_list)[0]

    extra_flags = {}
    for k, v in col_body.items():
        if k not in _KNOWN_COLLECTOR_KEYS:
            resolved = _eval_config_str(v)
            extra_flags[k] = _bool(resolved) if isinstance(resolved, str) else resolved

    iterators = _parse_iterators(col_body.get("iterator", []))

    collector_cfg = CollectorOptions(
        max_workers=_int(col_body.get("max_workers", 4), default=4),
        dry_run=_bool(_eval_config_str(col_body.get("dry_run", False))),
        sync_tag=_eval_config_str(col_body.get("sync_tag", "")),
        regex_dir=_eval_config_str(col_body.get("regex_dir", "./regex")),
        extra_flags=extra_flags,
        iterators=iterators,
    )

    # --- objects ---
    objects = _parse_objects(raw.get("object", []))

    return CollectorConfig(
        source=source_cfg,
        netbox=netbox_cfg,
        collector=collector_cfg,
        objects=objects,
        raw_source_body=source_body,
        source_label=source_label,
    )
