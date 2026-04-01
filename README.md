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
| **Multi-source support** | VMware vCenter, Microsoft Azure, Lenovo XClarity, Cisco Catalyst Center, Cisco NDFC, F5 BIG-IP, Prometheus, LDAP, SNMP, and any HTTP/REST API |
| **Automatic prerequisites** | Creates manufacturers, device types, sites, racks, platforms, cluster types, and more on the fly |
| **Thread-safe parallel execution** | Configurable worker pools; each item gets an isolated execution context |
| **Dry-run mode** | Preview all payloads that *would* be sent without writing anything to NetBox |
| **Smart caching** | Pluggable backends: in-memory, Redis, or SQLite — reduces redundant NetBox API calls |
| **Flexible field expressions** | 20+ helper functions for transformation (`source()`, `coalesce()`, `regex_file()`, `map_value()`, etc.) |
| **Nested collections** | Sync interfaces, IP addresses, inventory items, virtual disks, and modules within a single mapping file |
| **Module support** | Full NetBox module bay / module type / module hierarchy for detailed hardware tracking |
| **Error isolation** | Failures on individual items are logged and skipped without aborting the run |
| **Tag management** | Automatically tag every synced object; tags are merged with existing values |
| **Web UI** | Browser-based dashboard to monitor running jobs, browse logs of past runs, trigger new syncs, and manage the NetBox cache |

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
| `main.py` | CLI entry point; discovers and runs mapping files |
| `web_server.py` | Web UI entry point; starts the Flask monitor server |
| `collector/engine.py` | Top-level orchestrator per HCL file |
| `collector/config.py` | HCL parser; produces a validated `CollectorConfig` dataclass tree |
| `collector/db.py` | SQLite store for job status and log records |
| `collector/job_log_handler.py` | Logging handler that persists INFO+ records to the job DB |
| `collector/field_resolvers.py` | Expression evaluator with 20+ helper functions |
| `collector/prerequisites.py` | Resolves prerequisite objects (ensures they exist in NetBox) |
| `collector/sources/rest.py` | Generic HTTP/REST adapter — zero Python per source |
| `collector/sources/vmware.py` | pyVmomi adapter for VMware vCenter |
| `collector/sources/azure.py` | Azure SDK adapter |
| `collector/sources/ldap.py` | LDAP3 adapter |
| `collector/sources/catc.py` | Cisco Catalyst Center adapter |
| `lib/pynetbox2.py` | Production-ready NetBox client: caching, rate-limiting, upsert, retry |
| `web/app.py` | Flask application factory; dashboard, job detail, cache management routes |
| `web/templates/` | Jinja2 HTML templates (Bootstrap 5, Clemson colour palette) |

For a deeper dive, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Requirements

- Python 3.8+
- Access to a [NetBox](https://netbox.dev/) instance with an API token
- Credentials for the source systems you intend to sync

Python packages (see `requirements.txt`):

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

# 2. (Recommended) Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Web UI

The web monitor provides a browser-based interface to:

- **View running jobs** in real time (logs stream automatically via polling)
- **Browse previous job logs** with colour-coded log levels
- **Inspect sync summaries** (processed / created / updated / skipped / errored per object type)
- **Trigger new sync runs** by selecting an HCL mapping file from a drop-down (or entering a path manually) with an optional dry-run flag
- **Manage the NetBox cache** — inspect entry counts per resource and flush individual resource caches or the entire cache

### Starting the web server

```bash
# Development
python web_server.py                 # listens on http://0.0.0.0:5000

# Custom port / host
python web_server.py --port 8080 --host 127.0.0.1

# Production (gunicorn)
pip install gunicorn
gunicorn -w 2 -b 0.0.0.0:5000 "web.app:create_app()"
```

### Docker Compose

The `web` service is included in `docker-compose.yml` and starts automatically:

```bash
docker compose up web           # web UI only, shares the collector image
docker compose up               # collector one-shot + web UI
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
| `COLLECTOR_DB_PATH` | `<project root>/collector_jobs.sqlite3` | Path to the SQLite job database |
| `FLASK_DEBUG` | `false` | Enable Flask debug mode (never use in production) |

Web UI authentication settings are environment-only on purpose; they are not editable through the Settings page.

---

## Quick Start

```bash
# Load environment variables from a .env file (recommended to avoid exposing
# credentials in shell history — see Configuration section below)
set -a && source .env && set +a

# Copy an example mapping to activate it (mapping files ship as *.hcl.example)
cp mappings/vmware.hcl.example mappings/vmware.hcl

# Dry-run to preview changes without writing
python main.py --mapping mappings/vmware.hcl --dry-run

# Run for real
python main.py --mapping mappings/vmware.hcl

# Run all mappings discovered in the mappings/ directory
python main.py
```

Create a `.env` file (and add it to `.gitignore`) to store startup settings securely:

```bash
# .env — never commit this file
WEB_SECRET_KEY=change-me-in-production
COLLECTOR_DB_PATH=./data/collector_jobs.sqlite3
LOG_LEVEL=INFO
```

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

The `env()` helper in HCL is retained for compatibility, but it now resolves DB-backed runtime settings rather than reading directly from `os.environ`.

Use the web Settings UI, or write directly to the `config_settings` table, to manage runtime values.

---

## Usage

```
usage: main.py [-h] [--mapping PATH] [--dry-run] [--log-level {DEBUG,INFO,WARNING,ERROR}]

HCL-driven modular NetBox collector

options:
  -h, --help            Show this help message and exit
  --mapping PATH        HCL mapping file to run. May be specified multiple times.
                        If omitted, all *.hcl files in mappings/ are auto-discovered.
  --dry-run             Log payloads without writing to NetBox.
                        Overrides the dry_run setting in the mapping file.
  --log-level LEVEL     Logging verbosity: DEBUG, INFO, WARNING, ERROR (default: INFO)
```

**Examples:**

```bash
# Run a single mapping
python main.py --mapping mappings/vmware.hcl

# Run multiple mappings in sequence
python main.py --mapping mappings/vmware.hcl --mapping mappings/xclarity.hcl

# Preview changes without writing
python main.py --mapping mappings/vmware.hcl --dry-run

# Auto-discover and run all *.hcl files in mappings/
python main.py

# Verbose output for debugging
python main.py --mapping mappings/vmware.hcl --log-level DEBUG
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
├── requirements.txt               # Python dependencies
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
│       └── snmp.py                # SNMP (vendor-agnostic)
│
├── web/                           # Web UI package
│   ├── app.py                     # Flask application factory + routes
│   └── templates/
│       ├── base.html              # Shared navbar / layout (Bootstrap 5)
│       ├── index.html             # Dashboard: running jobs, recent history, run form
│       ├── job_detail.html        # Job log viewer + sync summary table
│       ├── cache.html             # Cache status and flush UI
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
    ├── ARCHITECTURE.md            # Framework design and data flow
    └── HCL_DESIGN.md              # Full HCL language specification


```

---

## Further Documentation

- **[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)** — Full framework architecture, component roles, data flow, and design goals
- **[`docs/HCL_REFERENCE.md`](docs/HCL_REFERENCE.md)** — Complete HCL language specification with all blocks, attributes, and expression helpers
