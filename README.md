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
| **Multi-source support** | VMware vCenter, Microsoft Azure, Lenovo XClarity, Cisco Catalyst Center, Cisco NDFC, F5 BIG-IP, Prometheus, LDAP, SNMP, Tenable, NetBox-to-NetBox, and any HTTP/REST API |
| **Automatic prerequisites** | Creates manufacturers, device types, sites, racks, platforms, cluster types, and more on the fly |
| **Thread-safe parallel execution** | Configurable worker pools; each item gets an isolated execution context |
| **Dry-run mode** | Preview all payloads that *would* be sent without writing anything to NetBox |
| **Smart caching** | Optional Redis or SQLite caching with flush/pre-warm controls in the web UI — reduces redundant NetBox API calls |
| **Flexible field expressions** | 20+ helper functions for transformation (`source()`, `coalesce()`, `regex_file()`, `map_value()`, etc.) |
| **Nested collections** | Sync interfaces, IP addresses, inventory items, virtual disks, and modules within a single mapping file |
| **Module support** | Full NetBox module bay / module type / module hierarchy for detailed hardware tracking |
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
| `web_server.py` | Web UI entry point; starts the Flask monitor server |
| `collector/engine.py` | Top-level orchestrator per HCL file |
| `collector/config.py` | HCL parser; produces a validated `CollectorConfig` dataclass tree |
| `collector/db.py` | SQLite store for jobs, logs, schedules, and editable configuration settings |
| `collector/job_log_handler.py` | Logging handler that persists INFO+ records to the job DB |
| `collector/field_resolvers.py` | Expression evaluator with 20+ helper functions |
| `collector/prerequisites.py` | Resolves prerequisite objects (ensures they exist in NetBox) |
| `collector/sources/rest.py` | Generic HTTP/REST adapter — zero Python per source |
| `collector/sources/vmware.py` | pyVmomi adapter for VMware vCenter |
| `collector/sources/azure.py` | Azure SDK adapter |
| `collector/sources/ldap.py` | LDAP3 adapter |
| `collector/sources/catc.py` | Cisco Catalyst Center adapter |
| `lib/pynetbox2.py` | Production-ready NetBox client: caching, rate-limiting, upsert, retry |
| `web/app.py` | Flask application factory; dashboard, job detail, schedules, cache, and settings routes |
| `web/templates/` | Jinja2 HTML templates (Bootstrap 5, Clemson colour palette) |

For a deeper dive, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Requirements

- Python 3.8+
- Access to a [NetBox](https://netbox.dev/) instance with an API token
- Credentials for the source systems you intend to sync

Python dependencies are managed in `pyproject.toml` (Poetry workflow).  
`requirements*.txt` remains as a legacy/fallback path.

```
requests==2.32.3
deepdiff==8.0.1
pyyaml==6.0.2
# Python-hcl2 for HCL parsing
python-hcl2>=4.3.0
# redis for caching
redis
# LDAP
ldap3
# Netbox
pynetbox==7.3.3
netboxlabs-diode-sdk
# VMware
pyvmomi==8.0.3.0.1
# Cisco Catalyst Center
dnacentersdk>=2.6.0
# Microsoft Azure
azure-identity>=1.15.0
azure-mgmt-compute>=30.0.0
azure-mgmt-network>=25.0.0
azure-mgmt-subscription>=3.1.1
# SNMP
pysnmp>=7.1
```

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
poetry run python web_server.py
```

Legacy fallback install path:

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt
```

---

## Web UI

The web monitor provides a browser-based interface to:

- **View running jobs** in real time (logs stream automatically via polling)
- **Browse previous job logs** with colour-coded log levels
- **Inspect sync summaries** (processed / created / updated / skipped / errored per object type, including `partial` runs)
- **Queue new sync runs** by selecting an HCL mapping file with optional dry-run and debug-log capture flags
- **Manage cron schedules** for unattended runs
- **Edit configuration settings** stored in the shared SQLite database
- **Manage the NetBox cache** — inspect entry counts per resource, flush entries, and pre-warm the cache

The web UI does not execute collectors in-process. It creates queued jobs in the shared SQLite database, and the scheduler loop in `main.py --run-scheduler` picks them up for execution.

### Starting the web server

```bash
# Development
poetry run python web_server.py      # listens on http://0.0.0.0:5000

# Custom port / host
poetry run python web_server.py --port 8080 --host 127.0.0.1

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
| `COLLECTOR_DB_PATH` | `<project root>/data/collector_jobs.sqlite3` | Path to the SQLite job database |
| `FLASK_DEBUG` | `false` | Enable Flask debug mode (never use in production) |

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

Create a `.env` file (and add it to `.gitignore`) to store credentials securely:

```bash
# .env — never commit this file
NETBOX_URL=https://netbox.example.com
NETBOX_TOKEN=your-netbox-api-token
VCENTER_URL=vcenter.example.com
VCENTER_USER=administrator@vsphere.local
VCENTER_PASS=secret
```

---

## Configuration

### Environment Variables

All sensitive values are read from environment variables inside the HCL files using the `env()` expression. The following variables are used across the included mappings:

#### NetBox (required by all mappings)

| Variable | Description |
|---|---|
| `NETBOX_URL` | NetBox base URL (e.g., `https://netbox.example.com`) |
| `NETBOX_TOKEN` | NetBox API token |
| `NETBOX_CACHE_BACKEND` | Cache backend: `none` \| `redis` \| `sqlite` (default: `none`) |
| `NETBOX_CACHE_URL` | Redis URL or SQLite file path when using those backends |
| `NETBOX_CACHE_TTL` | Cache entry TTL in seconds |
| `NETBOX_CACHE_KEY_PREFIX` | Cache key prefix for Redis/SQLite backends |
| `NETBOX_PREWARM_SENTINEL_TTL` | Optional sentinel TTL used by cache pre-warm operations |
| `NETBOX_RATE_LIMIT` | Max NetBox requests per second (`0` = unlimited) |
| `NETBOX_RATE_LIMIT_BURST` | Token-bucket burst size for rate limiting |
| `NETBOX_RETRY_ATTEMPTS` | Retry count for transient NetBox API failures |
| `NETBOX_RETRY_INITIAL_DELAY` | Initial retry delay in seconds |
| `NETBOX_RETRY_BACKOFF_FACTOR` | Exponential retry backoff multiplier |
| `NETBOX_RETRY_MAX_DELAY` | Max retry delay in seconds |
| `NETBOX_RETRY_JITTER` | Max jitter added to retry delays |
| `NETBOX_RETRY_ON_4XX` | Comma-separated retryable 4xx status codes |
| `NETBOX_BRANCH` | Optional NetBox branch name for branch-aware deployments |
| `DRY_RUN` | Set to `"true"` to log payloads without writing to NetBox |

#### VMware (`mappings/vmware.hcl.example`)

| Variable | Description |
|---|---|
| `VCENTER_URL` | vCenter hostname or IP (no `https://`) |
| `VCENTER_USER` | vCenter username |
| `VCENTER_PASS` | vCenter password |

#### Lenovo XClarity (`mappings/xclarity.hcl.example`, `mappings/xclarity-modules.hcl.example`)

| Variable | Description |
|---|---|
| `XCLARITY_HOST` | XClarity Controller hostname or IP |
| `XCLARITY_USER` | XClarity username |
| `XCLARITY_PASS` | XClarity password |
| `XCLARITY_VERIFY_SSL` | TLS verification: `"true"` or `"false"` (default: `"true"`) |

#### Microsoft Azure (`mappings/azure.hcl.example`)

| Variable | Description |
|---|---|
| `AZURE_AUTH_METHOD` | `"default"` (az login / managed identity) or `"service_principal"` |
| `AZURE_SUBSCRIPTION_IDS` | Comma-separated subscription IDs (leave empty for all visible subscriptions) |
| `AZURE_TENANT_ID` | Azure tenant ID (required for service-principal auth) |
| `AZURE_CLIENT_ID` | Service principal client ID |
| `AZURE_CLIENT_SECRET` | Service principal client secret |

#### Cisco Catalyst Center (`mappings/catc.hcl.example`)

| Variable | Description |
|---|---|
| `CATC_HOST` | Catalyst Center base URL |
| `CATC_USER` | Catalyst Center username |
| `CATC_PASS` | Catalyst Center password |
| `CATC_VERIFY_SSL` | TLS verification: `"true"` or `"false"` (default: `"true"`) |

#### LDAP (`mappings/ldap.hcl.example`)

| Variable | Description |
|---|---|
| `LDAP_SERVER` | LDAP server URI (e.g., `ldaps://ldap.example.com:636`) |
| `LDAP_USER` | Bind DN |
| `LDAP_PASS` | Bind password |
| `LDAP_SEARCH_BASE` | LDAP search base DN |
| `LDAP_FILTER` | LDAP search filter (default: `"(objectClass=*)"`) |

#### Cisco Nexus Dashboard Fabric Controller (`mappings/nexus.hcl.example`)

| Variable | Description |
|---|---|
| `NDFC_HOST` | Nexus Dashboard hostname or IP |
| `NDFC_USER` | Nexus Dashboard username |
| `NDFC_PASS` | Nexus Dashboard password |
| `NDFC_VERIFY_SSL` | TLS verification (default: `"true"`) |
| `NDFC_FETCH_INTERFACES` | Fetch per-switch interfaces: `"true"` or `"false"` (default: `"false"`) |

#### F5 BIG-IP (`mappings/f5.hcl.example`)

| Variable | Description |
|---|---|
| `F5_HOST` | BIG-IP hostname or IP |
| `F5_USER` | BIG-IP username |
| `F5_PASS` | BIG-IP password |
| `F5_VERIFY_SSL` | TLS verification (default: `"true"`) |
| `F5_FETCH_INTERFACES` | Fetch physical interfaces and self-IPs: `"true"` or `"false"` (default: `"false"`) |
| `F5_SITE` | NetBox site name to assign the appliance to (default: `"Default"`) |

#### Prometheus node-exporter (`mappings/prometheus.hcl.example`)

| Variable | Description |
|---|---|
| `PROMETHEUS_URL` | Prometheus server base URL (e.g., `http://prometheus:9090`) |
| `PROMETHEUS_USER` | HTTP basic auth username (optional) |
| `PROMETHEUS_PASS` | HTTP basic auth password (optional) |
| `PROMETHEUS_VERIFY_SSL` | TLS verification (default: `"true"`) |
| `PROMETHEUS_FETCH_INTERFACES` | Fetch per-node network interface info (default: `"true"`) |

#### SNMP / Juniper (`mappings/juniper-snmp.hcl.example`)

| Variable | Description |
|---|---|
| `SNMP_HOSTS` | Comma-separated list of hostnames or IP addresses to poll |
| `SNMP_COMMUNITY` | SNMP v2c community string (default: `"public"`) |
| `SNMP_VERSION` | SNMP version: `"1"`, `"2c"`, or `"3"` (default: `"2c"`) |
| `SNMP_PORT` | UDP port (default: `"161"`) |
| `SNMP_TIMEOUT` | Request timeout in seconds (default: `"5"`) |
| `SNMP_RETRIES` | Retry count (default: `"1"`) |
| `SNMP_V3_USER` | SNMPv3 username (only when `SNMP_VERSION=3`) |
| `SNMP_V3_AUTH_PASS` | SNMPv3 authentication password |
| `SNMP_V3_AUTH_PROTO` | Auth protocol: `md5`, `sha`, `sha256`, etc. (default: `"sha"`) |
| `SNMP_V3_PRIV_PASS` | SNMPv3 privacy (encryption) password |
| `SNMP_V3_PRIV_PROTO` | Privacy protocol: `des`, `aes`, `aes256`, etc. (default: `"aes"`) |

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
| `mappings/xclarity.hcl.example` | Lenovo XClarity | Servers, chassis, switches, storage, interfaces, **inventory items** |
| `mappings/xclarity-modules.hcl.example` | Lenovo XClarity | Servers, chassis, switches, storage, interfaces, **modules** (ModuleBay/Module/ModuleType) |
| `mappings/azure.hcl.example` | Microsoft Azure | VMs, IP prefixes, subscriptions, interfaces, managed disks, appliances, standalone NICs |
| `mappings/catc.hcl.example` | Cisco Catalyst Center | Network devices, interfaces, management IPs |
| `mappings/nexus.hcl.example` | Cisco Nexus Dashboard (NDFC) | Fabric switches, interfaces |
| `mappings/f5.hcl.example` | F5 BIG-IP | Appliances, interfaces, self-IPs |
| `mappings/prometheus.hcl.example` | Prometheus node-exporter | Linux hosts, interfaces |
| `mappings/juniper-snmp.hcl.example` | SNMP (Juniper routers) | Devices, interfaces, IP addresses |
| `mappings/linux-snmp.hcl.example` | SNMP (Linux / net-snmp) | Devices, interfaces, IP addresses |
| `mappings/ldap.hcl.example` | LDAP directory | Generic LDAP objects |
| `mappings/active-directory-computers.hcl.example` | Active Directory (LDAP) | Computer accounts → NetBox devices |
| `mappings/active-directory-users.hcl.example` | Active Directory (LDAP) | User accounts → NetBox contacts |
| `mappings/tenable.hcl.example` | Tenable One / Nessus | Assets, vulnerabilities, findings → NetBox IP addresses / virtual machines |

> **xclarity.hcl.example vs xclarity-modules.hcl.example** — Both files sync the same four device types from Lenovo XClarity.  The difference is how hardware components (CPUs, memory, drives, add-in cards, power supplies, fans) are recorded in NetBox: `xclarity.hcl.example` uses `dcim.inventory_items` while `xclarity-modules.hcl.example` uses the richer `dcim.module_bays` → `dcim.modules` → `dcim.module_types` object graph.  Use `xclarity-modules.hcl.example` when you need to track individual component installations, cable interfaces to specific PCIe cards, or leverage NetBox 4.0 module type profiles.

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
├── web_server.py                  # Web UI entry point
├── requirements.txt               # Legacy/fallback dependency export
│
├── collector/                     # Core framework package
│   ├── engine.py                  # Top-level orchestrator
│   ├── config.py                  # HCL parser + dataclass models
│   ├── context.py                 # Per-run execution context
│   ├── db.py                      # SQLite job-tracking store
│   ├── job_log_handler.py         # Logging handler → job DB
│   ├── field_resolvers.py         # Expression evaluator
│   ├── prerequisites.py           # Prerequisite resolution
│   └── sources/
│       ├── base.py                # Abstract DataSource interface
│       ├── rest.py                # Generic REST adapter
│       ├── vmware.py              # VMware vCenter (pyVmomi)
│       ├── azure.py               # Microsoft Azure
│       ├── ldap.py                # LDAP directory
│       ├── catc.py                # Cisco Catalyst Center
│       ├── nexus.py               # Cisco Nexus Dashboard (NDFC)
│       ├── f5.py                  # F5 BIG-IP
│       ├── prometheus.py          # Prometheus node-exporter
│       ├── snmp.py                # SNMP (vendor-agnostic)
│       ├── tenable.py             # Tenable One / Nessus
│       └── netbox.py              # NetBox-to-NetBox source adapter
│
├── web/                           # Web UI package
│   ├── app.py                     # Flask application factory + routes
│   └── templates/
│       ├── base.html              # Shared navbar / layout (Bootstrap 5)
│       ├── index.html             # Dashboard: running jobs, recent history, run form
│       ├── job_detail.html        # Job log viewer + sync summary table
│       ├── schedules.html         # Schedule list and create form
│       ├── schedule_edit.html     # Schedule edit form
│       ├── cache.html             # Cache status and flush UI
│       ├── settings.html          # DB-backed settings UI
│       └── 404.html               # Not-found page
│
├── lib/
│   └── pynetbox2.py               # NetBox client (caching, upsert, retry)
│
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
│   └── DEVELOPER_ONBOARDING.md    # New-developer orientation guide

├── CONTRIBUTING.md                # Local setup, workflow, and PR guidance


```

---

## Further Documentation

- **[`CONTRIBUTING.md`](CONTRIBUTING.md)** — Local setup, development workflow, testing, and PR expectations
- **[`docs/DEVELOPER_ONBOARDING.md`](docs/DEVELOPER_ONBOARDING.md)** — Codebase tour and first-day developer guide
- **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — Full framework architecture, component roles, data flow, and design goals
- **[`docs/HCL_REFERENCE.md`](docs/HCL_REFERENCE.md)** — Complete HCL language specification with all blocks, attributes, and expression helpers
- **[`SUPPORT.md`](SUPPORT.md)** — Where to ask for help, file bugs, and request new source support
- **[`SECURITY.md`](SECURITY.md)** — How to report security issues privately
