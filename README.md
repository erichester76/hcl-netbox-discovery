# hcl-netbox-discovery

A **modular, declarative framework** for syncing infrastructure data from multiple external sources into [NetBox](https://netbox.dev/). Instead of writing hundreds of lines of Python per data source, each collector is a single HCL mapping file — no Python required for REST-based systems.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Web UI](#web-ui)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Layout](#project-layout)
- [Further Documentation](#further-documentation)

---

## Overview

**hcl-netbox-discovery** was built at Clemson University to replace a collection of dozens of 1,500–2,300 line monolithic Python collectors with a single, reusable engine driven by declarative HCL configuration files. The engine handles:

- Connecting to various source systems via APIs and python modules
- Fetching raw infrastructure objects
- Transforming and mapping fields to NetBox resources
- Automatically creating prerequisite objects (manufacturers, device types, sites, racks, etc.)
- Upserting data into NetBox with compound-key deduplication
- Syncing nested resources: interfaces, IP addresses, inventory items, and virtual disks

Adding support for a new REST-based data source requires only a new `.hcl` file — no Python code, if its a complex API or SDK a small adapter has to be written

---

## Features

| Feature | Description |
|---|---|
| **Zero Python per new REST source** | Configure entirely in HCL; the generic `rest` adapter handles the HTTP layer |
| **Declarative HCL mappings** | Describe *what* to sync, not *how* — the engine takes care of execution |
| **Multi-source support** | VMware vCenter, Microsoft Azure, Lenovo XClarity, Cisco Catalyst Center, Cisco NDFC, F5 BIG-IP, Prometheus, Ansible facts artifacts, Salt grains (live master or artifact), LDAP, SNMP, Tenable, NetBox-to-NetBox, and any HTTP/REST API |
| **Automatic prerequisites** | Creates manufacturers, device types, sites, racks, platforms, cluster types, and more on the fly |
| **Thread-safe parallel execution** | Configurable worker pools; each item gets an isolated execution context |
| **Dry-run mode** | Preview all payloads that *would* be sent without writing anything to NetBox |
| **Smart caching** | Optional Redis or SQLite caching with flush/pre-warm controls in the web UI — reduces redundant NetBox API calls |
| **Flexible field expressions** | 20+ helper functions for transformation (`source()`, `coalesce()`, `regex_file()`, `map_value()`, etc.) |
| **Field-level update modes** | Mark individual fields as `if_missing` so confirmed NetBox values are only filled when blank |
| **Nested collections** | Sync interfaces, IP addresses, inventory items, virtual disks, and modules within a single mapping file |
| **Module support** | Full NetBox module bay / module type / module hierarchy for detailed hardware tracking |
| **Custom-object fallback** | Nexus topology mappings can target NetBox Custom Objects and automatically fall back to custom fields when the plugin endpoint is unavailable |
| **Error isolation** | Failures on individual items are logged and skipped without aborting the run |
| **Tag management** | Automatically tag every synced object; tags are merged with existing values |
| **Web UI** | Browser-based dashboard to queue syncs, monitor running jobs, browse logs, manage schedules and settings, and control the NetBox cache |

---

## Architecture

```
HCL mapping file
       │
       ▼
  config.py ──► CollectorConfig
       │
       ├──► sources/<adapter>.py
       │         └── get_objects("collection") → [raw_obj, …]
       │
       └──► engine.py
               │
               ├── For each raw_obj (ThreadPoolExecutor):
               │       │
               │       ├── prerequisites.py → resolve prereqs → context.prereqs
               │       │
               │       ├── field_resolvers.py → evaluate expressions → payload dict
               │       │
               │       └── pynetbox2.upsert(resource, payload, lookup_fields)
               │
               └── For each nested collection (interfaces, inventory_items, disks):
                       └── same inner loop, parent ID injected automatically
```

Key components:

| Component | Role |
|---|---|
| `main.py` | CLI entry point; runs explicit mappings or the long-running scheduler loop |
| `src/web/web_server.py` | Web UI entry point; starts the Flask monitor server |
| `src/collector/engine.py` | Top-level orchestrator per HCL file |
| `src/collector/config.py` | HCL parser; produces a validated `CollectorConfig` dataclass tree |
| `src/collector/db.py` | SQLite store for jobs, logs, schedules, and editable configuration settings |
| `src/collector/job_log_handler.py` | Logging handler that persists job log records to the SQLite DB |
| `src/collector/field_resolvers.py` | Expression evaluator with 20+ helper functions |
| `src/collector/prerequisites.py` | Resolves prerequisite objects (ensures they exist in NetBox) |
| `src/collector/sources/rest.py` | Generic HTTP/REST adapter — zero Python per source |
| `src/collector/sources/vmware.py` | pyVmomi adapter for VMware vCenter |
| `src/collector/sources/azure.py` | Azure SDK adapter |
| `src/collector/sources/ldap.py` | LDAP3 adapter |
| `src/collector/sources/catc.py` | Cisco Catalyst Center adapter |
| `src/collector/sources/ansible.py` | Ansible facts artifact adapter |
| `src/collector/sources/salt.py` | Salt grains adapter (live master or artifact) |
| `pynetbox-wrapper` dependency | Production-ready NetBox client wrapper: caching, rate-limiting, upsert, retry |
| `src/web/app.py` | Flask application factory; dashboard, job detail, schedules, cache, and settings routes |
| `src/web/templates/` | Jinja2 HTML templates (Bootstrap 5, Clemson colour palette) |

For a deeper dive, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Requirements

- Python 3.12+
- Access to a [NetBox](https://netbox.dev/) instance with an API token
- Credentials for the source systems you intend to sync

Python dependencies are managed in `pyproject.toml` through Poetry.

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/erichester76/hcl-netbox-discovery.git
cd hcl-netbox-discovery

# 2. Install Poetry (if needed)
pip install --user poetry

# 3. Install project dependencies (including dev tools)
poetry install --with dev
```

Run project commands with Poetry:

```bash
poetry run pytest
poetry run python main.py --mapping mappings/vmware.hcl --dry-run
poetry run python -m web.web_server
```

## Web UI

The web monitor provides a browser-based interface to:

- **View running jobs** in real time (logs stream automatically via polling)
- **See the running app version** in the header, using the exact Git tag when the app is running a tagged release and falling back to the project version plus commit
- **Stop queued or running jobs** from the dashboard or job detail page and persist a terminal `stopped` status with the partial summary collected so far
- **Browse previous job logs** with colour-coded log levels
- **Inspect sync summaries** (processed / created / updated / skipped / errored per object type, including `partial` runs)
- **Inspect runtime snapshots** for each job, including the masked effective runtime config and code version metadata used for that run
- **Fetch persisted job artifacts as JSON** for automation or remote triage
- **Queue new sync runs** by selecting an HCL mapping file with optional dry-run and debug-log capture flags
- **Manage cron schedules** for unattended runs
- **Edit configuration settings** stored in the shared SQLite database
- **Manage the NetBox cache** — inspect entry counts per resource, flush entries, and pre-warm the cache

The web UI does not execute collectors in-process. It creates queued jobs in the shared SQLite database, and the scheduler loop in `main.py --run-scheduler` picks them up for execution. On startup, the scheduler also reconciles orphaned `running` jobs left behind by a worker crash or container restart before claiming new work.

For automation, poll recent jobs and then fetch the persisted artifact for any
newly completed run:

```text
GET /api/jobs?after_id=<last_seen_id>&hcl_file=mappings/azure.hcl
GET /api/jobs/<id>/logs?after_id=<last_seen_log_id>
GET /api/jobs/<id>/artifact
```

For token-authenticated API access, configure `WEB_API_TOKEN` in the Settings UI and send either:

```text
Authorization: Bearer <token>
X-API-Key: <token>
```

### Starting the web server

```bash
# Development
poetry run python -m web.web_server      # listens on http://0.0.0.0:5000

# Custom port / host
poetry run python -m web.web_server --port 8080 --host 127.0.0.1

# Production (gunicorn)
poetry run gunicorn -w 2 -b 0.0.0.0:5000 "web.app:create_app()"
```

### Docker Compose

The `web` service is included in `docker-compose.yml` and starts automatically:

```bash
docker compose up web           # web UI only, shares the collector image
docker compose up               # scheduler + web UI (+ redis cache service)
```

The web UI listens on `http://localhost:${WEB_PORT:-5000}` and persists job history in a named Docker volume (`collector_db`).

### Web UI environment variables

| Variable | Default | Description |
|---|---|---|
| `WEB_PORT` | `5000` | TCP port the web server listens on |
| `WEB_HOST` | `0.0.0.0` | Bind address |
| `WEB_SECRET_KEY` | `change-me-in-production` | Flask session secret — **change this** |
| `WEB_AUTH_ENABLED` | `true` | Require login for the web UI and enforce CSRF on state-changing routes |
| `WEB_USERNAME` | `admin` | Username for the built-in web UI login |
| `WEB_PASSWORD_HASH` | empty | Preferred password hash for the built-in web UI login |
| `WEB_PASSWORD` | empty | Optional plaintext fallback when `WEB_PASSWORD_HASH` is not set |
| `WEB_SESSION_COOKIE_SECURE` | `false` | Mark session cookies secure when serving the UI over HTTPS |
| `COLLECTOR_DB_PATH` | `<project root>/data/collector_jobs.sqlite3` | Path to the SQLite job database |
| `COLLECTOR_DB_ENCRYPTION_KEY` | empty | Bootstrap key used to encrypt/decrypt sensitive DB-backed settings at rest |
| `FLASK_DEBUG` | `false` | Enable Flask debug mode (never use in production) |

Browser login settings remain environment-only. API token authentication is DB-backed through the `WEB_API_TOKEN` runtime setting and applies only to `/api/*` routes.
Sensitive DB-backed settings such as passwords, tokens, and client secrets are encrypted at rest when `COLLECTOR_DB_ENCRYPTION_KEY` is set. Encrypted rows cannot be decrypted/read, and setting a new secret value requires the same bootstrap key. Clearing or resetting an override may still be possible without the key.

---

## Quick Start

```bash
# Load environment variables from a .env file (recommended to avoid exposing
# credentials in shell history — see Configuration section below)
set -a && source .env && set +a

# Copy an example mapping to activate it (mapping files ship as *.hcl.example)
cp mappings/vmware.hcl.example mappings/vmware.hcl

# Dry-run to preview changes without writing
poetry run python main.py --mapping mappings/vmware.hcl --dry-run

# Run for real
poetry run python main.py --mapping mappings/vmware.hcl

# Run the long-running scheduler (executes DB-backed schedules and queued web jobs)
poetry run python main.py --run-scheduler
```

Create a `.env` file (and add it to `.gitignore`) to store startup settings securely:

```bash
# .env — never commit this file
WEB_SECRET_KEY=change-me-in-production
COLLECTOR_DB_PATH=./data/collector_jobs.sqlite3
LOG_LEVEL=INFO
```

### Job APIs

Job runs persist structured artifact JSON, masked runtime snapshots, and code
version metadata in the SQLite jobs table. The stored version metadata includes
component-level version numbers and content fingerprints for the collector,
engine, source adapters, mapping examples, and the specific mapping file used
for the run. The list APIs return lightweight job records without embedded
artifact or runtime metadata, while the artifact and job detail paths expose
the stored runtime metadata:

```text
GET /api/jobs
GET /api/running-jobs
GET /api/jobs/<id>/logs?after_id=<last_log_id>
GET /api/jobs/<id>/artifact
```

Use the job APIs and stored job logs as the supported inspection path. The old
file-based artifact capture/pull helper scripts have been removed.

---

## Configuration

### Source Of Truth

Startup settings stay in the process environment:

| Variable | Description |
|---|---|
| `WEB_PORT` | TCP port the web server listens on |
| `WEB_HOST` | Bind address |
| `WEB_SECRET_KEY` | Flask session secret |
| `WEB_AUTH_ENABLED` | Toggle built-in login protection and CSRF enforcement |
| `WEB_USERNAME` | Built-in web UI username |
| `WEB_PASSWORD_HASH` | Preferred password hash for the built-in web UI login |
| `WEB_PASSWORD` | Optional plaintext fallback for local/dev use |
| `WEB_SESSION_COOKIE_SECURE` | Mark session cookies secure when serving over HTTPS |
| `COLLECTOR_DB_PATH` | Path to the SQLite job/config database |
| `FLASK_DEBUG` | Flask debug toggle |
| `LOG_LEVEL` | Process log level |

All non-startup collector and source settings are DB-backed runtime configuration. That includes:
- NetBox URL/token and cache/retry tuning
- source credentials and connection settings
- global collector sync flags such as `DRY_RUN`
- API token authentication through `WEB_API_TOKEN`

The `env()` helper in HCL is retained for compatibility, but it now resolves DB-backed runtime settings rather than reading directly from `os.environ`.

Use the web Settings UI, or write directly to the `config_settings` table, to manage runtime values.

---

## Usage

```
usage: main.py [-h] [--mapping PATH] [--dry-run] [--log-level {DEBUG,INFO,WARNING,ERROR}] [--run-scheduler]

HCL-driven modular NetBox collector

options:
  -h, --help            Show this help message and exit
  --mapping PATH        HCL mapping file to run. May be specified multiple times.
  --dry-run             Log payloads without writing to NetBox.
                        Overrides the dry_run setting in the mapping file.
  --log-level LEVEL     Logging verbosity: DEBUG, INFO, WARNING, ERROR (default: INFO)
  --run-scheduler       Run the scheduler loop. Checks the database for due cron
                        schedules and queued web jobs every 60 seconds.
```

**Examples:**

```bash
# Run a single mapping
poetry run python main.py --mapping mappings/vmware.hcl

# Run multiple mappings in sequence
poetry run python main.py --mapping mappings/vmware.hcl --mapping mappings/xclarity.hcl

# Preview changes without writing
poetry run python main.py --mapping mappings/vmware.hcl --dry-run

# Verbose output for debugging
poetry run python main.py --mapping mappings/vmware.hcl --log-level DEBUG

# Run the scheduler loop used by the web UI / scheduled jobs
poetry run python main.py --run-scheduler
```

> **Tip:** Mapping files ship as `*.hcl.example` templates.  Copy and rename before running:
>
> ```bash
> cp mappings/vmware.hcl.example mappings/vmware.hcl
> ```

---

## HCL Mapping Files


For the full specification, see [`docs/HCL_REFERENCE.md`](docs/HCL_REFERENCE.md).

---

## Included Mappings

Mapping files ship as `*.hcl.example` templates.  Copy and rename to `*.hcl` to activate them:

```bash
cp mappings/vmware.hcl.example mappings/vmware.hcl
```

| File | Source System | Objects Synced |
|---|---|---|
| `mappings/vmware.hcl.example` | VMware vCenter | Clusters, hypervisor hosts, VMs, interfaces, IPs, virtual disks |
| `mappings/xclarity.hcl.example` | Lenovo XClarity | Servers, chassis, switches, storage, interfaces, inventory items, optional modules |
| `mappings/azure.hcl.example` | Microsoft Azure | VMs, IP prefixes, subscriptions, interfaces, managed disks, appliances, standalone NICs |
| `mappings/catalyst-center.hcl.example` | Cisco Catalyst Center | Network devices, interfaces, management IPs |
| `mappings/nexus.hcl.example` | Cisco Nexus Dashboard (NDFC) | Fabric switches, interfaces |
| `mappings/f5.hcl.example` | F5 BIG-IP | Appliances, interfaces, self-IPs |
| `mappings/prometheus.hcl.example` | Prometheus node-exporter | Linux hosts, interfaces |
| `mappings/ansible.hcl.example` | Ansible facts export / fact cache | Hosts, interfaces, IP addresses |
| `mappings/salt.hcl.example` | Salt master grains / artifact fallback | Hosts, interfaces, IP addresses |
| `mappings/juniper-snmp.hcl.example` | SNMP (Juniper routers) | Devices, interfaces, IP addresses |
| `mappings/linux-snmp.hcl.example` | SNMP (Linux / net-snmp) | Devices, interfaces, IP addresses |
| `mappings/ldap.hcl.example` | LDAP directory | Generic LDAP objects |
| `mappings/active-directory-computers.hcl.example` | Active Directory (LDAP) | Computer accounts → NetBox devices |
| `mappings/active-directory-users.hcl.example` | Active Directory (LDAP) | User accounts → NetBox contacts |
| `mappings/tenable.hcl.example` | Tenable One / Nessus | Assets, vulnerabilities, findings → NetBox IP addresses / virtual machines |

> **xclarity.hcl.example** — The unified XClarity example supports both inventory items and optional module sync. `COLLECTOR_SYNC_INVENTORY=true` keeps the default inventory-item behavior, while `COLLECTOR_SYNC_MODULES=true` enables the richer `dcim.module_bays` -> `dcim.modules` -> `dcim.module_types` object graph for CPUs, memory, drives, add-in cards, power supplies, and fans.

> **Ansible / Salt management tags** — The example mappings add
> `ansible-managed` and `salt-managed` as additive tags on positive discovery.
> Reuse the same `field "tags"` pattern in VM mappings when those hosts should
> sync to `virtualization.virtual_machines`.

---

## Regex Pattern Files

The `regex/` directory contains plain-text CSV files used by the `regex_file()` expression helper to map values from the source system (e.g., cluster names) to NetBox values (e.g., site names).

**Format:** `pattern,replacement` — one rule per line; first comma splits pattern from replacement.

```
# regex/cluster_to_site.example
^prod-cluster-atl.*,Atlanta
^prod-cluster-nyc.*,New York
^dev-cluster.*,Development
```

**Usage in a mapping file:**

```hcl
field "site" {
  value = regex_file(source("cluster.name"), "cluster_to_site")
}
```

Example files are provided in `regex/` with the `.example` extension. Copy and rename them (removing `.example`) to activate them:

```bash
cp regex/cluster_to_site.example regex/cluster_to_site
cp regex/vm_to_tenant.example    regex/vm_to_tenant
cp regex/host_to_tenant.example  regex/host_to_tenant
cp regex/vm_to_role.example      regex/vm_to_role
cp regex/xclarity_location_to_site.example  regex/xclarity_location_to_site
cp regex/xclarity_room_to_location.example  regex/xclarity_room_to_location
```

---

## Project Layout

```
hcl-netbox-discovery/
├── main.py                        # CLI entry point
├── src/
│   ├── collector/                 # Core framework package
│   │   ├── engine.py              # Top-level orchestrator
│   │   ├── config.py              # HCL parser + dataclass models
│   │   ├── context.py             # Per-run execution context
│   │   ├── db.py                  # SQLite job-tracking store
│   │   ├── job_log_handler.py     # Logging handler → job DB
│   │   ├── field_resolvers.py     # Expression evaluator
│   │   ├── prerequisites.py       # Prerequisite resolution
│   │   └── sources/
│   │       ├── base.py            # Abstract DataSource interface
│   │       ├── rest.py            # Generic REST adapter
│   │       ├── ansible.py         # Ansible facts artifact adapter
│   │       ├── salt.py            # Salt grains adapter (live master or artifact)
│   │       ├── vmware.py          # VMware vCenter (pyVmomi)
│   │       ├── azure.py           # Microsoft Azure
│   │       ├── ldap.py            # LDAP directory
│   │       ├── catc.py            # Cisco Catalyst Center
│   │       ├── nexus.py           # Cisco Nexus Dashboard (NDFC)
│   │       ├── f5.py              # F5 BIG-IP
│   │       ├── prometheus.py      # Prometheus node-exporter
│   │       ├── snmp.py            # SNMP (vendor-agnostic)
│   │       ├── tenable.py         # Tenable One / Nessus
│   │       └── netbox.py          # NetBox-to-NetBox source adapter
│   └── web/                       # Web UI package
│       ├── web_server.py          # Web UI entry point
│       ├── app.py                 # Flask application factory + routes
│       └── templates/
│           ├── base.html          # Shared navbar / layout (Bootstrap 5)
│           ├── index.html         # Dashboard: running jobs, recent history, run form
│           ├── job_detail.html    # Job log viewer + sync summary table
│           ├── schedules.html     # Schedule list and create form
│           ├── schedule_edit.html # Schedule edit form
│           ├── cache.html         # Cache status and flush UI
│           ├── settings.html      # DB-backed settings UI
│           └── 404.html           # Not-found page
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
│
├── regex/                         # Pattern files for field transformations
│   ├── cluster_to_site.example
│   ├── host_to_tenant.example
│   ├── vm_to_role.example
│   ├── vm_to_tenant.example
│   ├── xclarity_location_to_site.example
│   ├── xclarity_room_to_location.example
│   └── linux-interface-types      # Interface type map for linux-snmp.hcl.example
│
├── docs/
│   ├── ARCHITECTURE.md            # Framework design and data flow
│   ├── HCL_REFERENCE.md           # Full HCL language specification
│   ├── REPOSITORY_GUIDE.md        # Repo-specific developer guide
│   └── DEVELOPER_ONBOARDING.md    # Redirect to repository guide

├── CONTRIBUTING.md                # Local setup, workflow, and PR guidance


```

---

## Further Documentation

- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — Local setup, development workflow, testing, and PR expectations
- **[`docs/REPOSITORY_GUIDE.md`](docs/REPOSITORY_GUIDE.md)** — Repository-specific codebase tour, setup, and workflow guide
- **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — Full framework architecture, component roles, data flow, and design goals
- **[`docs/HCL_REFERENCE.md`](docs/HCL_REFERENCE.md)** — Complete HCL language specification with all blocks, attributes, and expression helpers
- **[`docs/ENGINEERING_AUDIT.md`](docs/ENGINEERING_AUDIT.md)** — Current refactor candidates and code-quality audit notes
- **[`SUPPORT.md`](SUPPORT.md)** — Where to ask for help, file bugs, and request new source support
- **[`SECURITY.md`](SECURITY.md)** — How to report security issues privately

## Branching And Releases

This repository uses a three-stage promotion model:

- `dev`: daily integration branch for feature and bugfix work
- `release/<version>`: versioned stabilization branch for the next production cut
- `main`: production branch only

Routine feature and bugfix branches should start from `origin/dev` and target
`dev`. Promote tested batches from `dev` to the active `release/<version>`
branch, then promote `release/<version>` to `main` for production. Create
release tags only from a clean checkout of current `origin/main`.

Use typed branch prefixes instead of a single catch-all branch namespace:

- `feature/<topic>` for backward-compatible feature work
- `bugfix/<topic>` for routine defect fixes
- `hotfix/<topic>` for urgent production fixes branched from `main`
- `docs/<topic>` for documentation-only changes
- `chore/<topic>` for maintenance work with no user-facing behavior change
- `release/<version>` for the active stabilization branch

Image publishing follows the same promotion model:

- pushes to `dev` publish a `:dev` image for code-bearing changes
- merges from `docs/*` and `chore/*` branches do not publish a `:dev` image
- pushes to `main` publish production images
- `v*` tags publish release-version images

Current release branch:

- `release/1.0.0`

## Versioning Policy

This repository uses semantic versioning once `1.0.0` is established:

- `MAJOR`
  - breaking changes only
  - examples: incompatible HCL behavior changes, removed settings, removed API
    fields/routes, schema changes requiring operator intervention
- `MINOR`
  - backward-compatible features
  - examples: new adapters, new HCL options, new UI or API endpoints that do
    not break existing usage
- `PATCH`
  - backward-compatible fixes only
  - examples: bug fixes, documentation corrections, internal refactors with no
    contract change

Breaking changes must only be released in a major version.

Use the helper script to bump the Poetry version before release promotions or
tag creation:

```bash
python scripts/bump_version.py patch
python scripts/bump_version.py minor
python scripts/bump_version.py major
```

Release tags must match `pyproject.toml` exactly. If the Poetry version is
`1.1.1`, the release tag must be `v1.1.1`. The Docker publish workflow rejects
`v*` tags that drift from the Poetry version.
