# HCL Mapping Language Design

## Overview

Each collector is defined by a single `.hcl` file that lives in `mappings/`. The file is a complete, self-contained description of:

- How to connect to the source system
- How to connect to NetBox
- Which objects to collect and sync
- How to transform each source field into a NetBox field

The engine parses the file and drives the full sync. No Python code is needed per collector.

Mapping files ship as `*.hcl.example` templates. Copy and rename to `*.hcl` to activate:

```bash
cp mappings/vmware.hcl.example mappings/vmware.hcl
```

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

### Azure (`api_type = "azure"`)

```hcl
source "azure" {
  api_type    = "azure"
  auth_method = env("AZURE_AUTH_METHOD", "default")   # "default" | "service_principal"
  tenant_id   = env("AZURE_TENANT_ID", "")
  client_id   = env("AZURE_CLIENT_ID", "")
  password    = env("AZURE_CLIENT_SECRET", "")        # client secret for service_principal
  subscription_ids = env("AZURE_SUBSCRIPTION_IDS", "") # comma-separated, empty = all
}
```

### LDAP (`api_type = "ldap"`)

```hcl
source "ldap" {
  api_type   = "ldap"
  url        = env("LDAP_SERVER")    # e.g. ldaps://ldap.example.com:636
  username   = env("LDAP_USER")      # bind DN
  password   = env("LDAP_PASS")
  verify_ssl = true

  search_base   = env("LDAP_SEARCH_BASE")
  search_filter = env("LDAP_FILTER", "(objectClass=*)")
  attributes    = "*"                # comma-separated, or "*" for all
}
```

### Cisco Catalyst Center (`api_type = "catc"`)

```hcl
source "catc" {
  api_type   = "catc"
  url        = env("CATC_HOST")
  username   = env("CATC_USER")
  password   = env("CATC_PASS")
  verify_ssl = env("CATC_VERIFY_SSL", "true")
  fetch_interfaces          = env("CATC_FETCH_INTERFACES", "true")
  site_assignment_strategy  = env("CATC_SITE_ASSIGNMENT_STRATEGY", "auto")
  wait_on_rate_limit        = env("CATC_WAIT_ON_RATE_LIMIT", "true")
  rate_limit_retry_attempts = env("CATC_RATE_LIMIT_RETRY_ATTEMPTS", "3")
  rate_limit_retry_initial_delay = env("CATC_RATE_LIMIT_RETRY_INITIAL_DELAY", "1.0")
  rate_limit_retry_max_delay = env("CATC_RATE_LIMIT_RETRY_MAX_DELAY", "30.0")
  rate_limit_retry_jitter   = env("CATC_RATE_LIMIT_RETRY_JITTER", "0.5")
}
```

The Catalyst Center adapter fetches device inventory in bulk, then joins it to
site assignments using the site-assignment API at the shallowest non-Global
site roots it can find. This avoids one membership lookup per site on large
hierarchies. When the installed SDK or Catalyst Center release does not expose
that site-assignment API, the adapter falls back to the older per-site
membership walk.

`site_assignment_strategy` controls which path is tried first:

- `auto` (default): bulk site-assignment join first, per-site membership fallback
- `bulk`: bulk site-assignment join first, per-site membership fallback
- `membership`: per-site membership walk first, bulk site-assignment fallback

Use `membership` when you already know the bulk site-assignment API is slow or
times out in a given Catalyst Center environment, but still want the adapter to
fall back to bulk if the membership walk produces no usable device/site pairs.

When interface collection is enabled, the adapter also synthesizes `mgmt0` and
`radio0` for Unified AP devices so management interface/IP parity matches the
legacy CATC collector behavior.

Use `mappings/catalyst-center.hcl.example` as the single maintained Catalyst
Center mapping template so the deployed mapping path uses the same
name/site/device_type/manufacturer wiring that CI validates.

### Cisco Nexus Dashboard Fabric Controller (`api_type = "nexus"`)

```hcl
source "nexus" {
  api_type         = "nexus"
  url              = env("NDFC_HOST")
  username         = env("NDFC_USER")
  password         = env("NDFC_PASS")
  verify_ssl       = env("NDFC_VERIFY_SSL", "true")
  fetch_interfaces = env("NDFC_FETCH_INTERFACES", "false")
}
```

### F5 BIG-IP (`api_type = "f5"`)

```hcl
source "f5" {
  api_type         = "f5"
  url              = env("F5_HOST")
  username         = env("F5_USER")
  password         = env("F5_PASS")
  verify_ssl       = env("F5_VERIFY_SSL", "true")
  fetch_interfaces = env("F5_FETCH_INTERFACES", "false")
}
```

### Prometheus node-exporter (`api_type = "prometheus"`)

```hcl
source "prometheus" {
  api_type         = "prometheus"
  url              = env("PROMETHEUS_URL")
  username         = env("PROMETHEUS_USER", "")
  password         = env("PROMETHEUS_PASS", "")
  verify_ssl       = env("PROMETHEUS_VERIFY_SSL", "true")
  fetch_interfaces = env("PROMETHEUS_FETCH_INTERFACES", "true")
}
```

### SNMP (`api_type = "snmp"`)

```hcl
source "snmp_devices" {
  api_type   = "snmp"
  url        = env("SNMP_HOSTS")          # comma-separated list of hosts
  username   = env("SNMP_COMMUNITY", "public")  # v2c community string

  # Optional SNMP parameters
  version    = env("SNMP_VERSION", "2c")
  port       = env("SNMP_PORT", "161")
  timeout    = env("SNMP_TIMEOUT", "5")
  retries    = env("SNMP_RETRIES", "1")

  # SNMPv3 (only when version = "3")
  # v3_user       = env("SNMP_V3_USER")
  # v3_auth_pass  = env("SNMP_V3_AUTH_PASS")
  # v3_auth_proto = env("SNMP_V3_AUTH_PROTO", "sha")
  # v3_priv_pass  = env("SNMP_V3_PRIV_PASS")
  # v3_priv_proto = env("SNMP_V3_PRIV_PROTO", "aes")

  # Vendor-specific OIDs to fetch per device (added to device dict by field name)
  extra_oids = {
    jnx_model  = "1.3.6.1.4.1.2636.3.1.2.0"
    jnx_serial = "1.3.6.1.4.1.2636.3.1.3.0"
  }
}
```

The SNMP adapter is vendor-agnostic. It exposes `sys_object_id` (raw sysObjectID OID) and `if_type` (raw integer) on every device/interface. Vendor-specific detection and field extraction should be expressed in HCL field expressions using `when()`, `regex_extract()`, and `map_value()`.

### NetBox source (`api_type = "netbox"`)

```hcl
source "source_nb" {
  api_type   = "netbox"
  url        = env("SOURCE_NETBOX_URL")
  password   = env("SOURCE_NETBOX_TOKEN")       # source NetBox API token
  verify_ssl = env("SOURCE_NETBOX_VERIFY_SSL", "true")
  filters    = env("SOURCE_NETBOX_FILTERS", "") # optional JSON object string
  page_size  = "1000"
}
```

This adapter reads from a source NetBox instance and returns plain dicts that can be remapped into the destination NetBox.

### `source` scalar attributes

| Attribute | Required | Description |
|---|---|---|
| `api_type` | yes | Selects the source adapter: `vmware`, `rest`, `azure`, `ldap`, `catc`, `nexus`, `f5`, `prometheus`, `snmp`, `tenable`, or `netbox` |
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
  cache      = env("NETBOX_CACHE_BACKEND", "none")     # none | redis | sqlite
  cache_url  = env("NETBOX_CACHE_URL", "")
  cache_ttl  = env("NETBOX_CACHE_TTL", "300")
  rate_limit = env("NETBOX_RATE_LIMIT", "0")           # calls/second (0 = unlimited)
}
```

| Attribute | Required | Description |
|---|---|---|
| `url` | yes | NetBox base URL |
| `token` | yes | API token |
| `cache` | no | Cache backend: `none`, `redis`, `sqlite` (default: `"none"`) |
| `cache_url` | no | Redis URL or SQLite path when applicable |
| `cache_ttl` | no | Cache entry TTL in seconds (default: `300`) |
| `prewarm_sentinel_ttl` | no | Optional TTL used by cache pre-warm sentinels |
| `rate_limit` | no | Max API calls per second (default: `0` = unlimited) |
| `rate_limit_burst` | no | Token-bucket burst size (default: `1`) |
| `retry_attempts` | no | Retry attempts for transient NetBox failures (default: `3`) |
| `retry_initial_delay` | no | Initial retry delay in seconds (default: `0.3`) |
| `retry_backoff_factor` | no | Exponential retry backoff multiplier (default: `2.0`) |
| `retry_max_delay` | no | Maximum retry delay in seconds (default: `15.0`) |
| `retry_jitter` | no | Max jitter added to retry delays (default: `0.0`) |
| `retry_on_4xx` | no | Comma-separated retryable 4xx codes (default: `"408,409,425,429"`) |
| `retry_5xx_cooldown` | no | Shared cooldown in seconds after retry-triggering 5xx errors (default: `60.0`) |
| `cache_key_prefix` | no | Prefix used to namespace cache keys (default: `"nbx:"`) |
| `branch` | no | Optional NetBox branch name |

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
  sync_modules    = env("COLLECTOR_SYNC_MODULES", "true")
  use_modules     = env("COLLECTOR_USE_MODULES", "false")
  skip_link_local_ips = "env('COLLECTOR_SKIP_LINK_LOCAL_IPS', 'false')"

  iterator {
    max_workers = 2
    VCENTER_URL = ["vc1.example.com", "vc2.example.com"]
  }
}
```

| Attribute | Required | Description |
|---|---|---|
| `max_workers` | no | Default thread pool size for all `object` blocks (default: `4`) |
| `dry_run` | no | Log payloads without writing to NetBox (default: `"false"`) |
| `sync_tag` | no | Tag applied to every object created/updated by this run |
| `regex_dir` | no | Directory containing regex pattern files (default: `"./regex"`) |
| `iterator` | no | Repeatable unlabeled block that runs multiple source passes with per-row `env()` overrides |

The collector block reserves only `max_workers`, `dry_run`, `sync_tag`, `regex_dir`, and `iterator`. Any other key is treated as a custom flag and is available in expressions as `collector.flag_name`.

Common examples include `sync_interfaces`, `sync_inventory`, `sync_modules`, and `use_modules`.
This also includes source-specific control flags such as `skip_link_local_ips`.

### `iterator` block

Each `iterator {}` block defines one group of source-connection overrides. The engine re-evaluates `env()` calls in the `source {}` block for each row, then runs a full collector pass per row.

| Attribute | Required | Description |
|---|---|---|
| `max_workers` | no | How many iterator rows from this block may run in parallel (default: `1`) |
| any other key | yes | A scalar or list of values used as `env()` overrides when rebuilding the source config |

When multiple override keys are lists, iteration is zip-style: the shortest list length determines the number of passes.

For sources where every endpoint shares the same credentials, keep the shared
username/password in the `source {}` block and iterate only the URL/host
variable. For example, VMware can iterate `VCENTER_URL` while keeping
`VCENTER_USER` and `VCENTER_PASS` in the `source {}` block, and XClarity
can iterate `XCLARITY_HOST` while keeping `XCLARITY_USER` and
`XCLARITY_PASS` in the `source {}` block.

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
  module           { … }   # repeatable — nested module loop
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
| `ensure_tenant` | `tenancy.tenants` | integer ID |
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

Scalar, FK, and tags fields all support an optional `update_mode` attribute:

```hcl
field "rack" {
  value       = "prereq('placement.rack_id')"
  update_mode = "if_missing"
}
```

When `update_mode = "if_missing"`, the engine only writes that field when the
existing NetBox value is blank or unset. If the existing object already has a
value for the field, the field is omitted from the outgoing payload for both
dry-runs and live writes.

| Attribute | Required | Description |
|---|---|---|
| `update_mode` | no | Field write policy: `"replace"` (default) or `"if_missing"` |

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

Nested inside `interface`. Same structure as `interface` but targets `ipam.ip_addresses`.

| Attribute | Required | Description |
|---|---|---|
| `source_items` | yes | Dotted path to the list of IPs on the interface object |
| `primary_if` | no | `"first"` sets the first IP as `primary_ip4` on the parent object |
| `oob_if` | no | `"first"` sets the first IP as the out-of-band primary IP on the parent object |
| `enabled_if` | no | Boolean expression |

### `tagged_vlan` block

Nested inside `interface`. Each source item is resolved to a VLAN and attached as a tagged VLAN on the parent interface.

| Attribute | Required | Description |
|---|---|---|
| `source_items` | yes | Dotted path or expression resolving to VLAN-like items on the interface object |
| `netbox_resource` | no | NetBox resource to upsert/lookup (default: `ipam.vlans`) |
| `lookup_by` | no | Fields used to dedupe/upsert VLANs (default: `["vid"]`) |
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

### `module` block

```hcl
module {
  source_items = "processors"
  profile      = "CPU"              # module type profile (informational label)
  dedupe_by    = "source('socket')" # optional deduplication key
  enabled_if   = "collector.sync_modules"

  # Required fields
  field "bay_name"     { value = "coalesce('socket', 'productName', 'description')" }
  field "model"        { value = "coalesce('displayName', 'productVersion', 'model')" }

  # Optional fields
  field "position"     { value = "str(source('slot'))" }
  field "serial"       { value = "str(source('serialNumber'))" }
  field "manufacturer" { value = "source('manufacturer')" }
}
```

The engine performs a four-step chain for every source item:

1. **ModuleBayTemplate** — ensures the slot is declared on the `DeviceType`
   template (so new devices of that type will have the bay automatically).
2. **ModuleBay** — ensures the physical slot instance exists on the `Device`.
3. **ModuleType** — ensures a reusable make/model record exists
   (`model` + optional `manufacturer`).
4. **Module** — upserts the installed instance linking device, bay, and type.

| Attribute | Required | Description |
|---|---|---|
| `source_items` | yes | Dotted path to the list of components on the parent object |
| `profile` | no | Human-readable category label stored in `ModuleConfig` (e.g. `"CPU"`, `"Memory"`) |
| `dedupe_by` | no | Expression used as a deduplication key |
| `enabled_if` | no | Boolean expression; entire block is skipped when `false` |

**Special field names** interpreted by the module processor:

| Field | Required | Description |
|---|---|---|
| `bay_name` | yes | Slot label on the device (e.g. `"CPU Socket 1"`) |
| `model` | yes | ModuleType model string |
| `position` | no | Numeric or string position passed to the bay template |
| `serial` | no | Serial number of the installed module |
| `manufacturer` | no | Manufacturer name (looked up / created automatically) |

### `power_input` block

Nested inside `module`. When present, the engine creates a `dcim.power_ports` record on the installed module after each successful module upsert.

| Attribute | Required | Description |
|---|---|---|
| `name` | no | Expression for the power port name |
| `type` | no | Expression for the NetBox power-port type slug (defaults to `iec-60320-c14`) |

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

### `regex_replace(value, pattern, replacement)`

`re.sub(pattern, replacement, value)` — regex substitution on `value`.

### `regex_extract(value, pattern, group=1)`

Returns the specified capture `group` from the first match of `pattern` against `value`, or `None` if there is no match.

```
regex_extract(source("description"), r"version (\S+)")   → "12.1R3.5" (for example)
```

### `upper(value)` / `lower(value)`

`str.upper()` / `str.lower()`.

### `truncate(value, n)`

Returns `value[:n]`.

### `join(sep, items)`

`sep.join(item for item in items if item)` — skips falsy items.

### `to_gb(bytes_value)`

`int(bytes_value / 1_073_741_824)`.

### `to_mb(kb_value)`

`int(kb_value / 1_024)` — converts kilobytes to megabytes.

### `mask_to_prefix(mask)`

Converts a dotted-decimal subnet mask to a CIDR prefix length integer.

```
mask_to_prefix("255.255.255.0")   → 24
mask_to_prefix("255.255.0.0")     → 16
```

### `str(value)` / `int(value)`

Cast a value to string or integer.

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
7. For each `module` block: inner loop over `source_items`
   - Resolve manufacturer → ensure ModuleBayTemplate → ensure ModuleBay
     → ensure ModuleType → upsert Module

---

## Environment Variable Conventions

All sensitive values should come from environment variables via `env()`:

```
NETBOX_URL            NetBox base URL
NETBOX_TOKEN          NetBox API token
NETBOX_CACHE_BACKEND  Cache backend (none | memory | redis | sqlite)
NETBOX_CACHE_URL      Redis URL or SQLite file path
DRY_RUN               Set to "true" to enable dry-run mode
LOG_LEVEL             Logging verbosity: DEBUG | INFO | WARNING | ERROR
```

Source-specific variables are defined per mapping file and documented in the comments
at the top of each `.hcl.example` file, and in `.env.example` at the repository root.
Copy `.env.example` to `.env`, fill in your values, and load it before running:

```bash
cp .env.example .env
# edit .env with your credentials
set -a && source .env && set +a
```
