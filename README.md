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
| `collector/engine.py` | Top-level orchestrator per HCL file |
| `collector/config.py` | HCL parser; produces a validated `CollectorConfig` dataclass tree |
| `collector/field_resolvers.py` | Expression evaluator with 20+ helper functions |
| `collector/prerequisites.py` | Resolves prerequisite objects (ensures they exist in NetBox) |
| `collector/sources/rest.py` | Generic HTTP/REST adapter — zero Python per source |
| `collector/sources/vmware.py` | pyVmomi adapter for VMware vCenter |
| `collector/sources/azure.py` | Azure SDK adapter |
| `collector/sources/ldap.py` | LDAP3 adapter |
| `collector/sources/catc.py` | Cisco Catalyst Center adapter |
| `lib/pynetbox2.py` | Production-ready NetBox client: caching, rate-limiting, upsert, retry |

For a deeper dive, see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Requirements

- Python 3.8+
- Access to a [NetBox](https://netbox.dev/) instance with an API token
- Credentials for the source systems you intend to sync

Python packages (see `requirements.txt`):

```
pynetbox>=7.3.3
pyvmomi>=8.0.3.0.1
requests>=2.32.3
deepdiff>=8.0.1
pyyaml>=6.0.2
redis
ldap3
netboxlabs-diode-sdk
python-hcl2>=4.3.0
dnacentersdk>=2.6.0
azure-identity>=1.15.0
azure-mgmt-compute>=30.0.0
azure-mgmt-network>=25.0.0
azure-mgmt-subscription>=3.1.1
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
| `NETBOX_CACHE_BACKEND` | Cache backend: `none` \| `memory` \| `redis` \| `sqlite` (default: `none`) |
| `NETBOX_CACHE_URL` | Redis URL or SQLite file path when using those backends |
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
├── requirements.txt               # Python dependencies
│
├── collector/                     # Core framework package
│   ├── engine.py                  # Top-level orchestrator
│   ├── config.py                  # HCL parser + dataclass models
│   ├── context.py                 # Per-run execution context
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
