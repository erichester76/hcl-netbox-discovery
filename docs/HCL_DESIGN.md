# HCL Mapping Language Design

## Overview

Each collector is defined by a single `.hcl` file that lives in `mappings/`. The file is a complete, self-contained description of:

- How to connect to the source system
- How to connect to NetBox
- Which objects to collect and sync
- How to transform each source field into a NetBox field

The engine parses the file and drives the full sync. No Python code is needed per collector.

---

## Top-Level Blocks

Every mapping file contains exactly these four top-level block types:

```hcl
source   "TYPE" { … }   # source system connection
netbox            { … }   # NetBox connection + cache
collector         { … }   # runtime options
object   "NAME"  { … }   # one NetBox resource type (repeatable)
```

---

## `source` Block

Declares how to connect to the data source.

### VMware (pyVmomi SDK)

```hcl
source "vcenter" {
  api_type   = "vmware"
  url        = env("VCENTER_URL")
  username   = env("VCENTER_USER")
  password   = env("VCENTER_PASS")
}
```

### REST-based sources (no Python required)

Any HTTP/REST API uses `api_type = "rest"` with `collection {}` sub-blocks that
map collection names to endpoints.  No Python code is needed.

```hcl
source "xclarity" {
  api_type   = "rest"
  url        = env("XCLARITY_HOST")
  username   = env("XCLARITY_USER")
  password   = env("XCLARITY_PASS")
  verify_ssl = env("XCLARITY_VERIFY_SSL", "true")
  auth       = "basic"

  collection "nodes" {
    endpoint        = "/nodes"
    list_key        = "nodeList"
    detail_endpoint = "/nodes/{uuid}"
    detail_id_field = "uuid"
  }

  collection "switches" {
    endpoint = "/switches"
    list_key = "switchList"
  }
}
```

### `source` scalar attributes

| Attribute | Required | Description |
|---|---|---|
| `api_type` | yes | Selects the source adapter: `vmware` or `rest` |
| `url` | yes | Base URL / hostname of the source system |
| `username` | no | Credential (required for `basic` auth) |
| `password` | no | Credential / token value |
| `verify_ssl` | no | TLS certificate verification (default: `"true"`) |
| `auth` | no | Auth scheme for `rest` adapter: `basic` \| `bearer` \| `header` (default: `"basic"`) |
| `auth_header` | no | Header name when `auth = "header"` (default: `"X-Api-Key"`) |

### `collection {}` sub-block (REST adapter only)

One block per logical collection.  The block label becomes the collection name
referenced in `object.source_collection`.

| Attribute | Required | Description |
|---|---|---|
| `endpoint` | yes | REST path for the list request, e.g. `/nodes` |
| `list_key` | no | Key to extract the item list from a dict response |
| `detail_endpoint` | no | Per-item detail path template, e.g. `/nodes/{uuid}`.  Each item is enriched by merging the detail response on top. |
| `detail_id_field` | no | Field in the list item used to fill the `{…}` placeholder (default: `"uuid"`) |

---

## `netbox` Block

Configures the pynetbox2 client.

```hcl
netbox {
  url        = env("NETBOX_URL")
  token      = env("NETBOX_TOKEN")
  cache      = env("NETBOX_CACHE_BACKEND", "memory")   # none | memory | redis | sqlite
  cache_url  = env("NETBOX_CACHE_URL", "")
  rate_limit = 100                                       # calls/second (0 = unlimited)
}
```

| Attribute | Required | Description |
|---|---|---|
| `url` | yes | NetBox base URL |
| `token` | yes | API token |
| `cache` | no | Cache backend: `none`, `memory`, `redis`, `sqlite` (default: `"none"`) |
| `cache_url` | no | Redis URL or SQLite path when applicable |
| `rate_limit` | no | Max API calls per second (default: `0` = unlimited) |

---

## `collector` Block

Runtime options that apply globally to the run.

```hcl
collector {
  max_workers     = 8
  dry_run         = env("DRY_RUN", "false")
  sync_tag        = "vmware-sync"
  regex_dir       = "./regex"
  sync_interfaces = env("COLLECTOR_SYNC_INTERFACES", "true")
  sync_inventory  = env("COLLECTOR_SYNC_INVENTORY", "true")
  use_modules     = env("COLLECTOR_USE_MODULES", "false")
}
```

| Attribute | Required | Description |
|---|---|---|
| `max_workers` | no | Default thread pool size for all `object` blocks (default: `4`) |
| `dry_run` | no | Log payloads without writing to NetBox (default: `"false"`) |
| `sync_tag` | no | Tag applied to every object created/updated by this run |
| `regex_dir` | no | Directory containing regex pattern files (default: `"./regex"`) |
| `sync_interfaces` | no | Enable interface syncing (referenced by `enabled_if`) |
| `sync_inventory` | no | Enable inventory item syncing (referenced by `enabled_if`) |
| `use_modules` | no | Use NetBox modules instead of inventory items |

Custom boolean flags added here are available in `enabled_if` expressions as `collector.flag_name`.

---

## `object` Block

Defines one NetBox resource type to sync. The block name is a local label used in log messages.

```hcl
object "host" {
  source_collection = "hosts"              # passed to source.get_objects()
  netbox_resource   = "dcim.devices"       # NetBox REST resource path
  lookup_by         = ["name", "site"]     # compound upsert key
  max_workers       = 8                    # override collector.max_workers for this object

  prerequisite "…" { … }   # repeatable — evaluated before fields
  field        "…" { … }   # repeatable — build the NetBox payload
  interface        { … }   # repeatable — nested interface loop
  inventory_item   { … }   # repeatable — nested inventory item loop
  disk             { … }   # repeatable — nested disk loop
}
```

| Attribute | Required | Description |
|---|---|---|
| `source_collection` | yes | Collection name passed to `source.get_objects()` |
| `netbox_resource` | yes | NetBox resource path (e.g., `"dcim.devices"`) |
| `lookup_by` | no | Field names used as the upsert key (default: `["name"]`) |
| `max_workers` | no | Thread pool size for this object (overrides `collector.max_workers`) |

---

## `prerequisite` Block

Prerequisite blocks are evaluated in order before the field payload is assembled. Each one calls a named method on the pynetbox2 client and caches the resolved value(s) for use in field expressions via `prereq("name")`.

```hcl
prerequisite "manufacturer" {
  method   = "ensure_manufacturer"
  args     = { name = source("hardware.systemInfo.vendor") }
  optional = false   # if false (default) and resolution fails, item is skipped
}
```

| Attribute | Required | Description |
|---|---|---|
| `method` | yes | One of the registered prerequisite methods (see table below) |
| `args` | yes | Map of arguments; values are field expressions |
| `optional` | no | If `true`, failure is a warning and sync continues (default: `false`) |

### Registered Methods

| Method | NetBox endpoint | Returns |
|---|---|---|
| `ensure_manufacturer` | `dcim.manufacturers` | integer ID |
| `ensure_device_type` | `dcim.device-types` | integer ID |
| `ensure_device_role` | `dcim.device-roles` | integer ID |
| `ensure_site` | `dcim.sites` | integer ID |
| `ensure_location` | `dcim.locations` | integer ID |
| `ensure_rack` | `dcim.racks` | integer ID |
| `ensure_platform` | `dcim.platforms` | integer ID |
| `ensure_cluster_type` | `virtualization.cluster-types` | integer ID |
| `ensure_cluster_group` | `virtualization.cluster-groups` | integer ID |
| `ensure_inventory_item_role` | `dcim.inventory-item-roles` | integer ID |
| `resolve_placement` | site → location → rack chain | dict with `.site_id`, `.location_id`, `.rack_id`, `.rack_position` |
| `lookup_tenant` | `tenancy.tenants` (read-only lookup) | integer ID or `None` |

---

## `field` Block

Describes one field in the NetBox payload. Fields come in three flavours determined by the `type` attribute (default: scalar value).

### Scalar field (default)

```hcl
field "name" {
  value = replace(source("name"), ".clemson.edu", "")
}
```

The `value` expression is evaluated and the result is placed directly in the payload under the field name.

### Foreign-key field (`type = "fk"`)

```hcl
field "site" {
  type     = "fk"
  resource = "dcim.sites"
  lookup   = { name = regex_file(source("parent.name"), "cluster_to_site") }
  ensure   = false   # don't create if missing; just skip this field
}
```

The engine performs a `nb.get(resource, **lookup)` and places the resulting ID in the payload. If `ensure = true` it calls `nb.upsert` to create the object if it doesn't exist.

| Attribute | Required | Description |
|---|---|---|
| `type` | yes (for FK) | `"fk"` |
| `resource` | yes | NetBox resource path |
| `lookup` | yes | Map of filter fields → expressions |
| `ensure` | no | Create the FK target if missing (default: `false`) |

### Tags field (`type = "tags"`)

```hcl
field "tags" {
  type  = "tags"
  value = ["vmware-sync", source("customFields.environment")]
}
```

Tags are merged with any existing tags on the object (deduplication by normalized name) rather than replacing them.

---

## Nested Collection Blocks

### `interface` block

Defines interfaces to sync for each parent object. May contain nested `ip_address` blocks.

```hcl
interface {
  source_items = "config.network.pnic"   # dotted path into the parent source object
  enabled_if   = collector.sync_interfaces

  field "name"        { value = source("device") }
  field "mac_address" { value = upper(source("mac")) }
  field "type"        {
    value = map_value(source("linkSpeed.speedMb"), {
      1000   = "1000base-t"
      10000  = "10gbase-x-sfpp"
      25000  = "25gbase-x-sfp28"
      40000  = "40gbase-x-qsfpp"
      100000 = "100gbase-x-qsfp28"
    }, default = "other")
  }

  ip_address {
    source_items = "spec.ip"   # path relative to the interface object
    primary_if   = "first"     # mark first address as primary4 on the parent device/VM

    field "address" { value = join("/", [source("ipAddress"), source("subnetMask")]) }
    field "status"  { value = "active" }
  }
}
```

| Attribute | Required | Description |
|---|---|---|
| `source_items` | yes | Dotted path to the list of interfaces on the parent object |
| `enabled_if` | no | Boolean expression; block is skipped when `false` |

### `ip_address` block

Nested inside `interface`. Same structure as `interface` but targets `ipam.ip-addresses`.

| Attribute | Required | Description |
|---|---|---|
| `source_items` | yes | Dotted path to the list of IPs on the interface object |
| `primary_if` | no | `"first"` sets the first IP as `primary_ip4` on the parent object |
| `enabled_if` | no | Boolean expression |

### `inventory_item` block

```hcl
inventory_item {
  source_items = coalesce("processors", "processorSlots")
  role         = "CPU"
  enabled_if   = collector.sync_inventory

  field "name"        { value = coalesce(source("socket"), source("productName")) }
  field "part_id"     { value = coalesce(source("displayName"), source("partNumber"), "") }
  field "serial"      { value = coalesce(source("serialNumber"), "") }
  field "description" { value = join(", ", [source("model"), source("speed")]) }
}
```

| Attribute | Required | Description |
|---|---|---|
| `source_items` | yes | Dotted path (or `coalesce` of paths) to the list on the parent object |
| `role` | no | Inventory item role name (created if missing) |
| `dedupe_by` | no | Expression used as a deduplication key when the same item appears in multiple source lists |
| `enabled_if` | no | Boolean expression |

### `disk` block

Same structure as `inventory_item` but targets `virtualization.virtual-disks` for VMs.

---

## Field Expression Reference

All `value`, `lookup`, `args`, `source_items`, and `enabled_if` attributes accept field expressions. Expressions are evaluated as Python using a controlled scope of helper functions.

### Path navigation — `source("path")`

Walks the source object by dotted path. Works on both plain Python dicts (`dict.get`) and attribute objects (`getattr`), mixing styles at each step.

```
source("name")                              → obj["name"] or obj.name
source("hardware.systemInfo.vendor")        → obj.hardware.systemInfo.vendor
source("summary.hardware.otherIdentifyingInfo[SerialNumberTag].identifierValue")
   → filter list for items where "SerialNumberTag" is a key, return identifierValue
source("raidSettings[*].diskDrives")        → flatten all diskDrives from all raidSettings
```

### `env(name, default="")`

Returns `os.environ.get(name, default)`.

### `regex_file(value, filename)`

Applies the pattern-replacement pairs in `regex/<filename>` to `value`. Lines are `pattern,replacement` CSV. Returns the transformed string (or original if no pattern matches).

### `map_value(value, mapping, default=None)`

Dictionary lookup: `mapping.get(value, default)`. Values can be any type including strings and integers.

### `when(condition, true_val, false_val)`

Ternary: returns `true_val` if `condition` is truthy, else `false_val`.

### `coalesce(*args)`

Returns the first argument that is not `None` and not an empty string. When passed a single string, it is treated as a `source()` path — i.e., `coalesce("fieldA", "fieldB")` tries `source("fieldA")` then `source("fieldB")`.

### `replace(value, old, new)`

`str.replace(old, new)` on `value`.

### `upper(value)` / `lower(value)`

`str.upper()` / `str.lower()`.

### `truncate(value, n)`

Returns `value[:n]`.

### `join(sep, items)`

`sep.join(item for item in items if item)` — skips falsy items.

### `to_gb(bytes_value)`

`int(bytes_value / 1_073_741_824)`.

### `prereq("name")` / `prereq("name.attr")`

Reference a resolved prerequisite by name. Use dot notation to access attributes of multi-value prerequisites (e.g., `prereq("placement.site_id")`).

### `collector.flag_name`

Reference a boolean flag from the `collector {}` block (e.g., `collector.sync_interfaces`).

---

## Evaluation Order

For each source item, the engine evaluates in this order:

1. `prerequisite` blocks — in declaration order
2. `field` blocks — in declaration order, building the payload dict
3. `nb.upsert(resource, payload, lookup_by)` — single write call
4. For each `interface` block: inner loop over `source_items`
   - `field` blocks for the interface
   - `nb.upsert("dcim.interfaces" or "virtualization.interfaces", …)`
   - For each `ip_address` block: inner loop
     - `nb.upsert("ipam.ip-addresses", …)`
     - Set primary IP if `primary_if = "first"` and this is the first
5. For each `inventory_item` block: inner loop over `source_items`
6. For each `disk` block: inner loop

---

## Environment Variable Conventions

All sensitive values should come from environment variables via `env()`:

```
NETBOX_URL            NetBox base URL
NETBOX_TOKEN          NetBox API token
NETBOX_CACHE_BACKEND  Cache backend (none | memory | redis | sqlite)
NETBOX_CACHE_URL      Redis URL or SQLite file path
DRY_RUN               Set to "true" to enable dry-run mode
```

Source-specific variables are defined per mapping file and documented in the comments at the top of each `.hcl` file.
