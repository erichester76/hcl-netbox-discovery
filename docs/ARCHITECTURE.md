# Modular NetBox Collector — Architecture

## Overview

The modular collector is a declarative, source-agnostic framework for syncing external infrastructure data into NetBox. Instead of writing hundreds of lines of Python per data source, each collector is described as an HCL mapping file that states:

- Where to connect (source system + NetBox)
- Which objects to fetch
- How to transform and map their fields to NetBox resources

The engine reads the HCL file, connects to both systems, and orchestrates the full sync — including prerequisite creation, field evaluation, parallel threading, tag management, and error isolation — without any source-specific code.

---

## Package Layout

```
netbox-collector/
├── pynetbox2.py                   # NetBox client library (unchanged)
├── collector/
│   ├── __init__.py
│   ├── engine.py                  # Top-level orchestrator
│   ├── config.py                  # HCL parser + config model
│   ├── context.py                 # Per-run state: NB client, source client, prereq cache
│   ├── field_resolvers.py         # Expression evaluator (source(), coalesce(), etc.)
│   ├── prerequisites.py           # Drives ensure_* chains from HCL prerequisite blocks
│   ├── parallel.py                # Shared ThreadPoolExecutor wrapper
│   └── sources/
│       ├── base.py                # Abstract DataSource interface
│       ├── vmware.py              # pyVmomi adapter
│       └── xclarity.py           # XClarity REST adapter
├── mappings/
│   ├── vmware.hcl                 # VMware collector definition
│   └── xclarity.hcl               # XClarity collector definition
├── regex/                         # Pattern files consumed by regex_file() expressions
├── main.py                        # CLI entry point
└── requirements.txt
```

---

## Component Roles

### `pynetbox2.py`

Production-ready NetBox client with:

- Three pluggable cache backends: `memory`, `redis`, `sqlite`
- Per-endpoint CRUD wrappers with diff-aware update (skips no-op writes)
- `upsert(resource, data, lookup_fields=[...])` — create-or-update with compound key support
- Rate limiting, retry with exponential backoff, optional Diode write backend
- Thread-safe locking and cache invalidation

The framework calls this exclusively for all NetBox writes. No direct `pynetbox` usage anywhere else.

---

### `collector/config.py`

Parses HCL files using `python-hcl2` and produces a validated `CollectorConfig` dataclass tree:

```
CollectorConfig
  .source        SourceConfig      (type, url, credentials, …)
  .netbox        NetBoxConfig      (url, token, cache, rate_limit, …)
  .collector     CollectorOptions  (max_workers, dry_run, sync_tag, …)
  .objects[]     ObjectConfig
      .name
      .source_collection
      .netbox_resource
      .lookup_by[]
      .prerequisites[]   PrerequisiteConfig
      .fields[]          FieldConfig
      .interfaces[]      InterfaceConfig
          .fields[]
          .ip_addresses[]
      .inventory_items[] InventoryItemConfig
          .fields[]
      .disks[]           DiskConfig
          .fields[]
```

---

### `collector/field_resolvers.py`

Evaluates field expressions at runtime against a source object and an execution context. Expressions are written as Python-evaluable strings using a small DSL of helper functions. The resolver exposes those helpers as a safe eval scope:

| Function | Description |
|---|---|
| `source("a.b.c")` | Dotted-path walk on source object (handles both `dict.get` and `getattr`) |
| `env("VAR", "default")` | `os.environ` lookup with optional default |
| `regex_file(value, "filename")` | Apply `regex/filename` pattern file to a string value |
| `map_value(value, {...}, default)` | Dict-based enum/lookup map |
| `when(cond, true_val, false_val)` | Conditional expression |
| `coalesce(a, b, c, …)` | First non-`None`/non-empty result |
| `replace(value, old, new)` | String `str.replace` |
| `upper(value)` / `lower(value)` | Case conversion |
| `truncate(value, n)` | Enforce max-length string |
| `join(sep, [a, b, …])` | Join non-empty strings |
| `to_gb(bytes_value)` | Convert bytes → GB (integer) |
| `prereq("name")` | Reference a resolved prerequisite value by name |
| `prereq("name.attr")` | Reference a named attribute on a multi-value prerequisite (e.g., `prereq("placement.site_id")`) |

Path traversal in `source()` supports:

- `"a.b.c"` — nested attribute/key access
- `"list[Key]"` — filter a list of dicts where `Key` is a key known to be present
- `"list[*]"` — flatten/iterate all items

---

### `collector/prerequisites.py`

Evaluates `prerequisite` blocks in declaration order before the main field payload is built. Each prerequisite maps to a `pynetbox2` `ensure_*` or `upsert` call:

| HCL `method` | pynetbox2 call |
|---|---|
| `ensure_manufacturer` | `nb.upsert("dcim.manufacturers", …)` |
| `ensure_device_type` | `nb.upsert("dcim.device-types", …)` |
| `ensure_device_role` | `nb.upsert("dcim.device-roles", …)` |
| `ensure_site` | `nb.upsert("dcim.sites", …)` |
| `ensure_location` | `nb.upsert("dcim.locations", …)` |
| `ensure_rack` | `nb.upsert("dcim.racks", …)` |
| `ensure_platform` | `nb.upsert("dcim.platforms", …)` |
| `ensure_inventory_item_role` | `nb.upsert("dcim.inventory-item-roles", …)` |
| `resolve_placement` | Site → location → rack → position chain; returns named dict |
| `lookup_tenant` | Pattern-based tenant lookup (project-ID or regex) |

Resolved IDs are stored in the execution context and made available to field expressions via `prereq("name")`.

---

### `collector/engine.py`

Top-level orchestrator per HCL file:

1. Load and validate `CollectorConfig` via `config.py`
2. Build pynetbox2 client from `netbox {}` block
3. Instantiate and connect the `DataSource` from `source {}` block
4. For each `object {}` block, in order:
   a. Call `source.get_objects(source_collection)` → list of raw items
   b. Fan out to `ThreadPoolExecutor(max_workers=object.max_workers or collector.max_workers)`
   c. Per item: resolve prerequisites → evaluate fields → `nb.upsert(resource, payload, lookup_fields=[…])`
   d. For each nested collection (`interface`, `inventory_item`, `disk`): inner loop with same pattern
5. Emit summary log: objects processed, created, updated, skipped, errored

Dry-run mode (when `collector.dry_run = true`) logs the payloads that *would* be sent but makes no writes.

---

### `collector/sources/base.py`

Minimal interface every source adapter must implement:

```python
class DataSource:
    def connect(self, config: SourceConfig) -> None: ...
    def get_objects(self, collection: str) -> list: ...
    def get_nested(self, parent_obj: Any, path: str) -> list: ...
    def close(self) -> None: ...
```

`get_nested` is used by the engine to expand `source_items` paths on nested collections.

---

### `collector/sources/vmware.py`

Wraps `pyVmomi`'s `SmartConnect`/`Disconnect` lifecycle. Implements `get_objects` for:

| `collection` | pyVmomi query |
|---|---|
| `"clusters"` | `vim.ClusterComputeResource` container view |
| `"hosts"` | `vim.HostSystem` container view |
| `"vms"` | `vim.VirtualMachine` container view |

Returns raw pyVmomi managed objects. The field resolver's `source()` function handles `getattr` traversal on them transparently.

---

### `collector/sources/xclarity.py`

Thin REST wrapper around the XClarity Controller API. Implements `get_objects` for:

| `collection` | XClarity endpoint |
|---|---|
| `"nodes"` | `GET /nodes` |
| `"chassis"` | `GET /chassis` |
| `"switches"` | `GET /switches` |
| `"storage"` | `GET /storage` |

Returns plain Python dicts. The field resolver's `source()` function handles `dict.get` traversal on them.

---

### `main.py`

CLI entry point:

```
usage: main.py [--mapping PATH] [--dry-run] [--log-level LEVEL]

  --mapping   PATH to an HCL mapping file (default: auto-discover mappings/*.hcl)
  --dry-run   Log payloads without writing to NetBox
  --log-level DEBUG / INFO / WARNING (default: INFO)
```

---

## Data Flow

```
HCL file
   │
   ▼
config.py ──► CollectorConfig
   │
   ├──► sources/vmware.py (or xclarity.py)
   │        └── get_objects("hosts") → [raw_obj, …]
   │
   └──► engine.py
           │
           ├── For each raw_obj:
           │       │
           │       ├── prerequisites.py → resolve prereqs → context.prereqs[name] = id
           │       │
           │       ├── field_resolvers.py → evaluate each field expr → payload dict
           │       │
           │       └── pynetbox2.upsert(resource, payload, lookup_fields)
           │
           └── For each nested collection (interfaces, inventory_items, …):
                   └── same inner loop, parent_id injected automatically
```

---

## Design Goals

| Goal | Approach |
|---|---|
| Minimal per-collector code | Field mappings are pure HCL data; no Python needed per source |
| Reuse existing NetBox client | `pynetbox2.py` is called unchanged |
| Thread safety | `parallel.py` wraps executor; each item gets an isolated context |
| No silent data loss | Prerequisites that fail cause the item to be skipped with a warning |
| Dry-run safety | Engine checks flag before every write call |
| Extensible sources | Add a new `sources/foo.py` implementing `DataSource`; no engine changes |
