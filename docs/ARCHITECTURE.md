# Modular NetBox Collector — Architecture

## Overview

The modular collector is a declarative, source-agnostic framework for syncing external infrastructure data into NetBox. Instead of writing hundreds of lines of Python per data source, each collector is described as an HCL mapping file that states:

- Where to connect (source system + NetBox)
- Which objects to fetch
- How to transform and map their fields to NetBox resources

The engine reads the HCL file, connects to both systems, and orchestrates the full sync — including prerequisite creation, field evaluation, parallel threading, tag management, and error isolation — without any source-specific code.

The web UI (`src/web/web_server.py`) provides a real-time dashboard for monitoring jobs, managing schedules, editing configuration settings, and controlling the NetBox cache. The unattended scheduler itself runs in `main.py --run-scheduler`. All job history, logs, schedules, and DB-backed config overrides are stored in a local SQLite database.

Adapter-to-mapping payload contracts for placement and identity are documented
in `docs/SOURCE_PAYLOAD_CONTRACTS.md`.

---

## Package Layout

```
hcl-netbox-discovery/
├── src/collector/
│   ├── __init__.py
│   ├── engine.py                  # Top-level orchestrator
│   ├── config.py                  # HCL parser + config model
│   ├── context.py                 # Per-run state: NB client, source client, prereq cache
│   ├── db.py                      # SQLite jobs/logs/schedules/settings store (shared by CLI and web UI)
│   ├── field_resolvers.py         # Expression evaluator (source(), coalesce(), etc.)
│   ├── job_log_handler.py         # logging.Handler that writes to the jobs DB
│   ├── prerequisites.py           # Drives ensure_* chains from HCL prerequisite blocks
│   └── sources/
│       ├── base.py                # Abstract DataSource interface
│       ├── rest.py                # Generic REST adapter — no Python needed per source
│       ├── ansible.py             # Ansible facts artifact adapter
│       ├── vmware.py              # pyVmomi adapter (SDK requires Python; fixed built-in)
│       ├── azure.py               # Microsoft Azure SDK adapter
│       ├── ldap.py                # LDAP directory adapter (ldap3)
│       ├── catc.py                # Cisco Catalyst Center adapter (dnacentersdk)
│       ├── nexus.py               # Cisco Nexus Dashboard Fabric Controller adapter
│       ├── f5.py                  # F5 BIG-IP iControl REST adapter
│       ├── prometheus.py          # Prometheus node-exporter adapter
│       ├── snmp.py                # SNMP adapter (pysnmp ≥ 7.1, vendor-agnostic)
│       ├── tenable.py             # Tenable One / Nessus adapter
│       └── netbox.py              # NetBox-to-NetBox source adapter
├── src/web/
│   ├── __init__.py
│   ├── app.py                     # Flask application factory + all route handlers
│   └── templates/
│       ├── base.html              # Shared layout (navbar, Bootstrap 5)
│       ├── index.html             # Dashboard: run a job, view active/recent jobs
│       ├── job_detail.html        # Live-streaming log viewer for a single job
│       ├── schedules.html         # Scheduler management: add/list/edit/delete schedules
│       ├── schedule_edit.html     # Edit form for an existing schedule
│       ├── cache.html             # Cache backend stats and flush controls
│       ├── settings.html          # Editable configuration settings UI
│       └── 404.html               # Error page
├── data/                          # Runtime data directory (created automatically)
│   └── collector_jobs.sqlite3     # SQLite database (jobs, logs, schedules)
├── mappings/                      # HCL mapping file templates (copy to *.hcl to use)
│   ├── vmware.hcl.example
│   ├── xclarity.hcl.example
│   ├── azure.hcl.example
│   ├── catalyst-center.hcl.example
│   ├── nexus.hcl.example
│   ├── f5.hcl.example
│   ├── prometheus.hcl.example
│   ├── ansible.hcl.example
│   ├── salt.hcl.example
│   ├── juniper-snmp.hcl.example
│   ├── linux-snmp.hcl.example
│   ├── ldap.hcl.example
│   ├── active-directory-computers.hcl.example
│   ├── active-directory-users.hcl.example
│   └── tenable.hcl.example
├── regex/                         # Pattern files consumed by regex_file() expressions
├── docs/
│   ├── ARCHITECTURE.md            # This document
│   └── HCL_REFERENCE.md          # HCL mapping file syntax reference
├── main.py                        # CLI entry point (manual run + scheduler loop)
├── src/web/web_server.py          # Web UI entry point
├── Dockerfile
├── docker-compose.yml
```

---

## Running Modes

### Manual one-shot run (CLI)

Run a single HCL mapping file explicitly:

```
python main.py --mapping mappings/vmware.hcl [--dry-run] [--log-level LEVEL]
```

The `--mapping` flag may be repeated to run multiple files in sequence. Omitting it (without `--run-scheduler`) is an error — there is no automatic discovery of `*.hcl` files.

### Scheduler mode (long-running process)

```
python main.py --run-scheduler [--log-level LEVEL]
```

Polls the database every 60 seconds for cron schedules that are due and fires them in background threads. This is the default mode used by the Docker container (`CMD ["--run-scheduler"]`).

### Web UI

```
python -m web.web_server [--port PORT] [--host HOST] [--debug]
```

Serves the Flask dashboard on port 5000 (default). The web server and the scheduler process share the same SQLite database, so jobs started from either surface appear together in the dashboard.

The web UI does not execute collector code directly. It queues jobs in the database; `main.py --run-scheduler` picks up queued jobs and scheduled runs.

---

## SQLite Database

`src/collector/db.py` owns the single SQLite file (`data/collector_jobs.sqlite3` by default, overridden by `COLLECTOR_DB_PATH`). It is opened in **WAL mode** with **foreign key enforcement** enabled on every connection. A module-level `threading.Lock` serialises writes from the multi-threaded engine and from concurrent web requests.

The path is created automatically (`os.makedirs`) on first use.

### Schema

#### `jobs`

Tracks every sync execution, whether started from the CLI or the web UI.

```sql
CREATE TABLE jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    hcl_file    TEXT    NOT NULL,
    run_token   TEXT,
    status      TEXT    NOT NULL DEFAULT 'queued',  -- queued | running | success | partial | failed | stopped
    dry_run     INTEGER NOT NULL DEFAULT 0,
    debug_mode  INTEGER NOT NULL DEFAULT 0,
    stop_requested INTEGER NOT NULL DEFAULT 0,      -- cooperative stop flag for queued/running jobs
    created_at  TEXT    NOT NULL,   -- ISO-8601 UTC timestamp
    started_at  TEXT,               -- set by start_job()
    finished_at TEXT,               -- set by finish_job()
    summary     TEXT,               -- JSON blob: {object_name: {processed, created, ...}}
    artifact_json TEXT,             -- Structured per-job artifact metadata
    runtime_snapshot_json TEXT,     -- Masked effective runtime config / execution plan for the run
    code_version_json TEXT          -- Code + component version metadata (package, git, component versions/fingerprints)
);
```

Status lifecycle:

```
queued  →  running  →  success
                    →  partial
                    →  failed
queued  →  stopped
running →  stopped
```

#### `job_logs`

Stores captured log records for each job (written by `JobLogHandler`).

```sql
CREATE TABLE job_logs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    timestamp TEXT    NOT NULL,
    level     TEXT    NOT NULL,   -- DEBUG | INFO | WARNING | ERROR
    logger    TEXT,
    message   TEXT    NOT NULL
);

CREATE INDEX idx_job_logs_job_id ON job_logs(job_id);
```

#### `schedules`

Stores cron-based schedules managed through the web UI.

```sql
CREATE TABLE schedules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,        -- human-readable label
    hcl_file    TEXT    NOT NULL,        -- absolute path to the HCL mapping file
    cron_expr   TEXT    NOT NULL,        -- 5-field cron: "minute hour day month weekday"
    dry_run     INTEGER NOT NULL DEFAULT 0,   -- 0 = live, 1 = dry-run
    enabled     INTEGER NOT NULL DEFAULT 1,   -- 0 = paused, 1 = active
    created_at  TEXT    NOT NULL,
    last_run_at TEXT,                    -- UTC timestamp of the last successful fire
    next_run_at TEXT                     -- UTC timestamp of the next scheduled fire
);
```

`next_run_at` is computed by `croniter` when a schedule is created or edited, and advanced immediately when it fires so that a second poll in the same minute cannot double-fire the same schedule.

#### `config_settings`

Stores editable runtime configuration exposed through the web UI. `env()` lookups in HCL and config parsing consult this table first, then fall back to OS environment variables.

```sql
CREATE TABLE config_settings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    key           TEXT    NOT NULL UNIQUE,
    value         TEXT,
    default_value TEXT,
    description   TEXT,
    group_name    TEXT    NOT NULL DEFAULT 'General',
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);
```

### Public API (`src/collector/db.py`)

| Function | Description |
|---|---|
| `init_db()` | Create tables (idempotent — safe to call on every startup) |
| `create_job(hcl_file, dry_run=False, debug_mode=False)` → `int` | Insert a new queued job; return its id |
| `start_job(job_id)` | Mark job as running; set `started_at`; clear any stale `stop_requested` flag |
| `finish_job(job_id, success, summary, has_errors=False, artifact=None, forced_status=None)` | Mark job as success/partial/failed/stopped; store JSON summary and structured artifact |
| `update_job_runtime_metadata(job_id, runtime_snapshot=None, code_version=None)` | Persist masked runtime snapshot metadata plus code/component version metadata for a queued/running job |
| `get_job(job_id)` → `dict\|None` | Fetch a single job record |
| `get_jobs(limit)` → `list` | Most-recent jobs, newest first |
| `get_running_jobs()` → `list` | All queued/running jobs (no limit) |
| `get_queued_jobs()` → `list` | Queued jobs waiting for the scheduler worker |
| `request_job_stop(job_id)` → `str\|None` | Mark a queued job stopped immediately or flag a running job for cooperative stop |
| `job_stop_requested(job_id)` → `bool` | Return whether a running job has been asked to stop |
| `add_log(job_id, level, logger, message)` | Append one log line |
| `get_job_logs(job_id)` → `list` | All log lines for a job, chronological |
| `create_schedule(name, hcl_file, cron_expr, ...)` → `int` | Insert a new schedule |
| `get_schedules()` → `list` | All schedules ordered by name |
| `get_schedule(id)` → `dict\|None` | Single schedule or None |
| `update_schedule(id, ...)` | Update name, file, cron, flags, next_run_at |
| `delete_schedule(id)` | Remove a schedule |
| `get_due_schedules()` → `list` | Enabled schedules whose `next_run_at ≤ now()` |
| `update_schedule_run(id, last_run_at, next_run_at)` | Advance timestamps after a fire |
| `get_config(key, default)` | Effective config lookup: DB override → env var → default |
| `set_setting(key, value)` / `reset_setting(key)` | Update or clear a DB-backed config override |

---

## Web UI

`src/web/app.py` is a Flask application factory (`create_app()`). It imports `collector.db` directly for all data access. On-demand runs triggered from the UI are queued in the DB, and the scheduler worker in `main.py` executes them asynchronously.

### Routes

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Dashboard: active jobs panel (auto-polls `/api/running-jobs`) + recent history table + run-job form + header version label (prefers Git tag, falls back to version/commit) |
| `GET` | `/jobs/<id>` | Job detail page with live-streaming log viewer (polls `/jobs/<id>/logs`) and a modal for runtime snapshot / code version inspection |
| `GET` | `/jobs/<id>/logs` | JSON: new log lines since `?after_id=N` plus current job status |
| `GET` | `/api/running-jobs` | JSON: all queued/running jobs (used by dashboard polling); supports session auth or API token auth |
| `GET` | `/api/jobs` | JSON: recent jobs with optional `after_id`, `status`, `hcl_file`, and `limit` filters; supports session auth or API token auth |
| `GET` | `/api/jobs/<id>/logs` | JSON: new log lines since `?after_id=N` plus current job status; supports session auth or API token auth |
| `GET` | `/api/jobs/<id>/artifact` | JSON: persisted structured artifact payload for a single job; supports session auth or API token auth |
| `POST` | `/jobs/<id>/stop` | Request cooperative stop for a running job or immediately stop a queued job |
| `POST` | `/jobs/run` | Trigger an on-demand job from the dashboard form |
| `GET` | `/schedules` | Scheduler management page: list all schedules + add-schedule form |
| `POST` | `/schedules/create` | Create a new schedule (computes initial `next_run_at` via `croniter`) |
| `GET/POST` | `/schedules/<id>/edit` | Edit an existing schedule |
| `POST` | `/schedules/<id>/delete` | Delete a schedule |
| `POST` | `/schedules/<id>/toggle` | Enable or disable a schedule |
| `POST` | `/schedules/<id>/run-now` | Fire a schedule's mapping file immediately as a one-off job |
| `GET` | `/cache` | Cache backend stats (entry counts by resource) |
| `POST` | `/cache/flush` | Flush all cache entries or entries for a specific resource |
| `POST` | `/cache/prewarm` | Pre-warm all or one cache resource |
| `GET` | `/settings` | View grouped configuration settings |
| `POST` | `/settings/update` | Save or reset a configuration setting |

### Scheduler UI features

- Cron expression editor with preset buttons (hourly, daily 2am, weekly, monthly)
- Inline 5-field validation feedback (client-side)
- Cron expression reference table (field ranges + special characters)
- Enable/disable toggle without deleting the schedule
- "Run Now" button to trigger an immediate one-off execution of any schedule

### Settings UI features

- Seeded from `.env.example` into the `config_settings` table
- Grouped display of web, NetBox, source, and collector options
- Sensitive values rendered as password-style inputs
- Reset action that clears a DB override and falls back to env/default

### Job stop semantics

- Queued jobs can be stopped immediately from the UI; they transition directly to `stopped` without being claimed by the scheduler.
- Running jobs are stopped cooperatively. The web UI sets `stop_requested=1`, and the engine checks that flag between object and item boundaries.
- When a running job stops cooperatively, `finish_job()` stores terminal status `stopped` and persists the partial summary accumulated so far.

---

## Scheduler

`main.py --run-scheduler` runs an infinite loop:

1. Sleep 60 seconds
2. Call `db.get_due_schedules()` — returns enabled schedules where `next_run_at ≤ now()`
3. For each due schedule not already running (tracked in `_active_schedule_ids`):
   - Immediately advance `next_run_at` via `croniter` and write it to the DB (prevents double-fire)
   - Spawn a `daemon=True` background thread calling `_run_scheduled_job(sched)`
4. Call `db.get_queued_jobs()` to pick up one-off jobs created by the web UI
5. Spawn background threads for queued jobs not already in `_active_queued_job_ids`
6. Reconcile orphaned `running` jobs from a previous worker process before the loop starts
7. Worker threads attach a `JobLogHandler`, run the engine, then mark the job success / partial / failed

The `_active_schedule_ids` set (guarded by a `threading.Lock`) prevents a slow-running scheduled job from being re-fired on the next poll while it is still in progress. A separate `_active_queued_job_ids` guard prevents duplicate pickup of queued web jobs inside one worker process; persisted stale-job reconciliation covers jobs abandoned by a prior worker process.

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

For `vmware`, `azure`, `ldap`, `catc`, `nexus`, `f5`, `prometheus`, `snmp`, and `tenable` sources,
dedicated adapters in `sources/` are required because they use proprietary SDKs or
protocols rather than plain HTTP REST.  These are fixed, internal components — no
changes are needed to add new deployments of these source types.

---

## Component Roles

### `pynetbox-wrapper`

Production-ready NetBox client with:

- Three pluggable cache backends: `memory`, `redis`, `sqlite`
- Per-endpoint CRUD wrappers with diff-aware update (skips no-op writes)
- `upsert(resource, data, lookup_fields=[...])` — create-or-update with compound key support
- Rate limiting, retry with exponential backoff, optional Diode write backend
- Thread-safe locking and cache invalidation

The framework calls this exclusively for all NetBox writes. No direct `pynetbox` usage anywhere else.

---

### `src/collector/config.py`

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

### `src/collector/field_resolvers.py`

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

### `src/collector/prerequisites.py`

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

### `src/collector/engine.py`

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

### `src/collector/db.py`

See [SQLite Database](#sqlite-database) above for the full schema and public API reference.

---

### `src/collector/job_log_handler.py`

A `logging.Handler` subclass that writes formatted log records to the `job_logs` table via `db.add_log()`. It is attached to the root logger for the duration of each job run (CLI or web-triggered) and removed immediately afterwards so logs from other concurrent jobs are not cross-contaminated.

---

### `src/collector/sources/base.py`

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

### `src/collector/sources/vmware.py`

Wraps `pyVmomi`'s `SmartConnect`/`Disconnect` lifecycle. Implements `get_objects` for:

| `collection` | pyVmomi query |
|---|---|
| `"clusters"` | `vim.ClusterComputeResource` container view |
| `"hosts"` | `vim.HostSystem` container view |
| `"vms"` | `vim.VirtualMachine` container view |

Returns raw pyVmomi managed objects. The field resolver's `source()` function handles `getattr` traversal on them transparently.

---

### `src/collector/sources/azure.py`

Uses the Azure SDK (`azure-identity`, `azure-mgmt-compute`, `azure-mgmt-network`, `azure-mgmt-subscription`) to enumerate resources across one or more Azure subscriptions.

Supports `api_type = "azure"` with `AZURE_AUTH_METHOD` selecting between `"default"` (DefaultAzureCredential) and `"service_principal"`. Subscription scope can be limited with `subscription_ids` (comma-separated) in the source `extra` block.

Implements `get_objects` for collections: `"subscriptions"`, `"virtual_machines"`, `"prefixes"`, `"appliances"`, `"standalone_nics"`. VM records include `image` and `custom_fields` (instance_type, image). Shared Gallery images are resolved to their definition metadata.

---

### `src/collector/sources/ldap.py`

Generic LDAP adapter using `ldap3`. Supports any collection name; maps it to an LDAP search using `extra.search_base`, `extra.search_filter`, and `extra.attributes`. Returns raw LDAP entry dicts.

Because Active Directory exposes its data over the standard LDAP protocol, `api_type = "ldap"` also covers Active Directory. The included `active-directory-computers.hcl.example` and `active-directory-users.hcl.example` templates demonstrate syncing AD computer accounts as NetBox devices and AD user accounts as NetBox contacts respectively.

---

### `src/collector/sources/catc.py`

Cisco Catalyst Center (DNA Center) adapter using `dnacentersdk`. Authenticates with user credentials and wraps the Device Inventory API.

Implements `get_objects` for collection `"devices"`. Device inventory is fetched in bulk from Catalyst Center, then joined to site assignments using the site-assignment API rooted at the shallowest non-Global sites so large hierarchies do not require one membership lookup per site. If that newer site-assignment API is unavailable, the adapter falls back to the older per-site membership walk. When `fetch_interfaces = "true"` is set in the source block, per-device interface lists are fetched and embedded so the `interface {}` HCL block can sync them. The adapter also enables dnacentersdk rate-limit waiting and applies a small fallback retry loop when Catalyst Center still raises 429 responses back to the caller.

---

### `src/collector/sources/nexus.py`

Cisco Nexus Dashboard Fabric Controller (NDFC) adapter. Uses token-based authentication (tries `/login` then the NDFC API token endpoint). Optionally fetches per-switch interface lists when `fetch_interfaces = "true"` and embeds them in each switch record.

Implements `get_objects` for collection `"switches"`.

---

### `src/collector/sources/f5.py`

F5 BIG-IP iControl REST adapter. Authenticates with user credentials and fetches device identity from `sys/hardware` (with fallback to `identified-devices`), software version from `sys/version`, and management IP from `sys/management-ip`. Optionally fetches physical interfaces and self-IPs.

Implements `get_objects` for collection `"devices"`.

---

### `src/collector/sources/prometheus.py`

Prometheus HTTP API adapter. Queries `node_uname_info` to enumerate Linux hosts, then enriches each with `node_dmi_info`, `node_memory_MemTotal_bytes`, and `node_cpu_seconds_total`. Optionally fetches `node_network_info` for interface data.

Implements `get_objects` for collection `"nodes"`.

---

### `src/collector/sources/snmp.py`

Vendor-agnostic SNMP adapter using `pysnmp ≥ 7.1` async API (via `asyncio.run()`). Polls a comma-separated list of hosts from `url`/`SNMP_HOSTS`. Supports SNMPv2c (community string = `username`) and SNMPv3 (parameters from `extra`). Exposes `sys_object_id` and `if_type` (raw integers) so vendor-specific logic can live entirely in HCL.

Additional OIDs can be fetched per device using `extra_oids = { field_name = "oid" }` in the source block.

Implements `get_objects` for collection `"devices"` (with nested `"interfaces"` and `"ip_addresses"`).

Included example mappings:
- `juniper-snmp.hcl.example` — Juniper routers (Juniper enterprise OIDs, model/version via regex, interface type mapping)
- `linux-snmp.hcl.example` — Linux servers running `net-snmp` (kernel version extraction, standard interface types)

---

### `src/collector/sources/tenable.py`

Tenable One (cloud) and Nessus (on-premise) adapter. Supports `api_type = "tenable"`.

Authentication:
- **Tenable.io / Tenable One**: `X-ApiKeys` header using `username` (access key) and `password` (secret key).
- **Nessus**: POST `/session` token via `username`/`password` (set `extra.platform = "nessus"`).

Implements `get_objects` for collections:

| Collection | Description |
|---|---|
| `"assets"` | Network assets / hosts from Tenable |
| `"vulnerabilities"` | Vulnerability records |
| `"findings"` | Per-asset vulnerability lists (requires `extra.include_asset_details = "true"`) |

The `extra.date_range` key (default `30`) limits results to the last N days. Use `extra.verify_ssl = "false"` for self-signed Nessus certs.

---

### `src/collector/sources/rest.py`

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

CLI entry point. Supports three operating modes:

```
python main.py --mapping PATH [--mapping PATH ...] [--dry-run] [--log-level LEVEL]
    Run one or more HCL mapping files explicitly.  No automatic file discovery.

python main.py --run-scheduler [--log-level LEVEL]
    Run the scheduler loop.  Polls the DB every 60 s for due cron schedules.
    This is the default Docker container mode (CMD ["--run-scheduler"]).

python main.py
    Error — at least --mapping or --run-scheduler is required.
```

Every job run (whether from CLI or the web UI) is recorded in the `jobs` table so that
the web dashboard always shows a unified history regardless of how a job was started.

---

### `src/web/web_server.py`

Thin entry-point wrapper for the Flask web UI. Parses `--port`, `--host`, `--debug`, and
`--log-level` options (or their environment variable equivalents: `WEB_PORT`, `WEB_HOST`,
`FLASK_DEBUG`, `LOG_LEVEL`) and calls `web.app.create_app()`.

---

## Data Flow

### CLI / scheduled run

```
main.py (--mapping or --run-scheduler)
   │
   ├── init_db()                        ← ensure tables exist
   │
   ├── [scheduler] get_due_schedules()  ← find cron entries past their next_run_at
   │        └── update_schedule_run()   ← advance next_run_at immediately (no double-fire)
   │
   ├── create_job() / start_job()       ← record job in SQLite
   │
   ├── JobLogHandler attached to root logger
   │
   ├── HCL file
   │      │
   │      ▼
   │   config.py ──► CollectorConfig
   │      │
   │      ├──► sources/<adapter>.py → get_objects("collection") → [raw_obj, …]
   │      │
   │      └──► engine.py
   │               │
   │               ├── For each raw_obj:
   │               │       ├── prerequisites.py → resolve prereqs
   │               │       ├── field_resolvers.py → evaluate fields → payload
   │               │       └── pynetbox2.upsert(resource, payload, lookup_fields)
   │               │
   │               └── For each nested collection (interfaces, inventory_items, disks, modules):
   │                       └── same inner loop, parent_id injected automatically
   │
   └── finish_job(success, summary, artifact)     ← write final status + JSON summary/artifact to SQLite
```

### Web UI request flow

```
Browser → Flask (src/web/app.py)
              │
              ├── GET  /              → render_template("index.html",
              │                           running=get_running_jobs(),
              │                           recent=get_jobs())
              │
              ├── POST /jobs/run      → threading.Thread(_run_job_background)
              │                           └── same engine flow as CLI run above
              │
              ├── GET  /jobs/<id>     → render_template("job_detail.html",
              │                           job=get_job(id), logs=get_job_logs(id))
              │
              ├── GET  /jobs/<id>/logs → jsonify(new logs since after_id, job status)
              │                           (polled every 2 s by the log viewer)
              │
              ├── GET  /api/jobs/<id>/logs → jsonify(new logs since after_id, job status)
              │                               (used by token-authenticated automation)
              │
              ├── GET  /schedules     → render_template("schedules.html",
              │                           schedules=get_schedules())
              │
              ├── POST /schedules/create  → create_schedule(), compute next_run via croniter
              ├── POST /schedules/<id>/edit    → update_schedule()
              ├── POST /schedules/<id>/delete  → delete_schedule()
              ├── POST /schedules/<id>/toggle  → update_schedule(enabled=not current)
              └── POST /schedules/<id>/run-now → threading.Thread(_run_job_background)
```

---

## Design Goals

| Goal | Approach |
|---|---|
| Minimal per-collector code | Field mappings are pure HCL data; no Python needed per source |
| Reuse existing NetBox client | `pynetbox2.py` is called unchanged |
| Thread safety | `parallel.py` wraps executor; each item gets an isolated context; DB writes serialised by a module-level lock |
| No silent data loss | Prerequisites that fail cause the item to be skipped with a warning |
| Dry-run safety | Engine checks flag before every write call |
| Extensible sources | Add a new `sources/foo.py` implementing `DataSource`; no engine changes |
| Unified job visibility | CLI runs, scheduler runs, and web-triggered runs all write to the same SQLite DB |
| Unattended operation | `--run-scheduler` + `restart: unless-stopped` in Docker Compose keeps jobs firing on schedule |
