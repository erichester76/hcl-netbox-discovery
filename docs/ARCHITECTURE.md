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
hcl-netbox-discovery/
├── lib/
│   └── pynetbox2.py               # NetBox client library
├── archive/
│   ├── README.md                  # Explains the archived scripts
│   ├── vmware-collector.py        # Original monolithic VMware collector
│   ├── xclarity-collector.py      # Original monolithic XClarity collector
│   └── cache-warmer.py            # Original standalone cache pre-warmer
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
│       ├── rest.py                # Generic REST adapter — no Python needed per source
│       ├── vmware.py              # pyVmomi adapter (SDK requires Python; fixed built-in)
│       ├── azure.py               # Microsoft Azure SDK adapter
│       ├── ldap.py                # LDAP directory adapter (ldap3)
│       ├── catc.py                # Cisco Catalyst Center adapter (dnacentersdk)
│       ├── nexus.py               # Cisco Nexus Dashboard Fabric Controller adapter
│       ├── f5.py                  # F5 BIG-IP iControl REST adapter
│       ├── prometheus.py          # Prometheus node-exporter adapter
│       └── snmp.py                # SNMP adapter (pysnmp ≥ 7.1, vendor-agnostic)
├── mappings/                      # HCL mapping file templates (copy to *.hcl to use)
│   ├── vmware.hcl.example
│   ├── xclarity.hcl.example
│   ├── xclarity-modules.hcl.example
│   ├── azure.hcl.example
│   ├── catc.hcl.example
│   ├── nexus.hcl.example
│   ├── f5.hcl.example
│   ├── prometheus.hcl.example
│   ├── juniper-snmp.hcl.example
│   ├── ldap.hcl.example
│   └── jnsu.hcl.example
├── regex/                         # Pattern files consumed by regex_file() expressions
├── main.py                        # CLI entry point
└── requirements.txt
```

---

## Adding a New REST-Based Collector (No Python Required)

Any HTTP/REST source can be supported by creating a single `.hcl` file.
No Python code is needed.

1. Copy the relevant `.hcl.example` template from `mappings/` and rename it to `.hcl`.
2. Set `api_type = "rest"` and choose an `auth` scheme (`basic`, `bearer`, or `header`).
3. Add `collection {}` sub-blocks to the `source` block describing each API endpoint.
4. Write `object {}` blocks as normal — `source_collection` refers to the collection label.

```hcl
source "my_api" {
  api_type = "rest"
  url      = env("MY_API_URL")
  username = env("MY_API_USER")
  password = env("MY_API_PASS")
  auth     = "basic"

  collection "servers" {
    endpoint = "/api/v1/servers"
    list_key = "items"           # optional: key inside the JSON response
  }
}
```

For `vmware`, `azure`, `ldap`, `catc`, `nexus`, `f5`, `prometheus`, and `snmp` sources,
dedicated adapters in `sources/` are required because they use proprietary SDKs or
protocols rather than plain HTTP REST.  These are fixed, internal components — no
changes are needed to add new deployments of these source types.

---

## Component Roles

### `lib/pynetbox2.py`

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
| `regex_replace(value, pattern, replacement)` | Regex substitution |
| `regex_extract(value, pattern, group=1)` | Return a capture group from a regex match |
| `upper(value)` / `lower(value)` | Case conversion |
| `truncate(value, n)` | Enforce max-length string |
| `join(sep, [a, b, …])` | Join non-empty strings |
| `to_gb(bytes_value)` | Convert bytes → GB (integer) |
| `to_mb(kb_value)` | Convert kilobytes → MB (integer) |
| `mask_to_prefix(mask)` | Convert dotted-decimal subnet mask to prefix length |
| `str(value)` / `int(value)` | Cast to string / integer |
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
| `ensure_cluster_type` | `nb.upsert("virtualization.cluster-types", …)` |
| `ensure_cluster_group` | `nb.upsert("virtualization.cluster-groups", …)` |
| `ensure_inventory_item_role` | `nb.upsert("dcim.inventory-item-roles", …)` |
| `ensure_tenant` | `nb.upsert("tenancy.tenants", …)` |
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
   d. For each nested collection (`interface`, `inventory_item`, `disk`, `module`): inner loop with same pattern
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

### `collector/sources/azure.py`

Uses the Azure SDK (`azure-identity`, `azure-mgmt-compute`, `azure-mgmt-network`, `azure-mgmt-subscription`) to enumerate resources across one or more Azure subscriptions.

Supports `api_type = "azure"` with `AZURE_AUTH_METHOD` selecting between `"default"` (DefaultAzureCredential) and `"service_principal"`.

Implements `get_objects` for collections: `"subscriptions"`, `"virtual_machines"`, `"prefixes"`.

---

### `collector/sources/ldap.py`

Generic LDAP adapter using `ldap3`. Supports any collection name; maps it to an LDAP search using `extra.search_base`, `extra.search_filter`, and `extra.attributes`. Returns raw LDAP entry dicts.

---

### `collector/sources/catc.py`

Cisco Catalyst Center (DNA Center) adapter using `dnacentersdk`. Authenticates via username/password and wraps the Device Inventory API.

Implements `get_objects` for collection `"devices"`.

---

### `collector/sources/nexus.py`

Cisco Nexus Dashboard Fabric Controller (NDFC) adapter. Uses token-based authentication (tries `/login` then the NDFC API token endpoint). Optionally fetches per-switch interface lists when `fetch_interfaces = "true"` and embeds them in each switch record.

Implements `get_objects` for collection `"switches"`.

---

### `collector/sources/f5.py`

F5 BIG-IP iControl REST adapter. Authenticates via username/password and fetches device identity from `sys/hardware` (with fallback to `identified-devices`), software version from `sys/version`, and management IP from `sys/management-ip`. Optionally fetches physical interfaces and self-IPs.

Implements `get_objects` for collection `"devices"`.

---

### `collector/sources/prometheus.py`

Prometheus HTTP API adapter. Queries `node_uname_info` to enumerate Linux hosts, then enriches each with `node_dmi_info`, `node_memory_MemTotal_bytes`, and `node_cpu_seconds_total`. Optionally fetches `node_network_info` for interface data.

Implements `get_objects` for collection `"nodes"`.

---

### `collector/sources/snmp.py`

Vendor-agnostic SNMP adapter using `pysnmp ≥ 7.1` async API (via `asyncio.run()`). Polls a comma-separated list of hosts from `url`/`SNMP_HOSTS`. Supports SNMPv2c (community string = `username`) and SNMPv3 (parameters from `extra`). Exposes `sys_object_id` and `if_type` (raw integers) so vendor-specific logic can live entirely in HCL.

Additional OIDs can be fetched per device using `extra_oids = { field_name = "oid" }` in the source block.

Implements `get_objects` for collection `"devices"` (with nested `"interfaces"` and `"ip_addresses"`).

---

### `collector/sources/rest.py`

Generic HTTP/REST adapter.  **No Python code is required per source** — all
configuration comes from `collection {}` sub-blocks in the HCL `source` block.

Supported auth schemes (configured via `auth` in the source block):

| `auth` value | Mechanism |
|---|---|
| `basic` (default) | HTTP Basic auth (`username` / `password`) |
| `bearer` | `Authorization: Bearer <password>` header |
| `header` | Arbitrary header; name set via `auth_header`, value via `password` |

Each `collection {}` block may specify:

| Attribute | Description |
|---|---|
| `endpoint` | REST path for the list request, e.g. `/nodes` |
| `list_key` | Optional key to extract the list from a dict response |
| `detail_endpoint` | Optional per-item detail path template, e.g. `/nodes/{uuid}` |
| `detail_id_field` | Field from the list item used to fill the `{…}` placeholder (default: `uuid`) |

When `detail_endpoint` is set the adapter fetches each item's detail and deep-merges it, so field expressions like `source("memoryModules")` work without any extra HCL.

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
   ├──► sources/<adapter>.py (vmware, azure, ldap, catc, nexus, f5, prometheus, snmp, rest)
   │        └── get_objects("collection") → [raw_obj, …]
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
           └── For each nested collection (interfaces, inventory_items, disks, modules):
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
