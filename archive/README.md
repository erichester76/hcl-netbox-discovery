# Archive — Legacy Monolithic Collectors

These scripts are the original, pre-framework collectors. They are preserved here for reference and to aid in understanding the domain logic that was extracted into the new modular framework.

## Contents

| File | Source system | Purpose |
|---|---|---|
| `vmware-collector.py` | VMware vCenter | Syncs clusters, hypervisor hosts, VMs, interfaces, IP addresses, VLANs, and tags from vCenter into NetBox |
| `xclarity-collector.py` | Lenovo XClarity Controller | Syncs physical servers, chassis, switches, and storage — including interfaces, inventory items (CPU, RAM, disks, PSUs), and rack placement — from XClarity into NetBox |
| `cache-warmer.py` | NetBox | Pre-warms the pynetbox2 cache by bulk-fetching commonly queried resources so subsequent collector runs hit the cache instead of the API |

## Why archived

Each script was ~1,500–2,300 lines of Python that mixed three concerns together:

1. **Source API wiring** — connecting to vCenter or XClarity and fetching raw data
2. **Transformation logic** — mapping source fields to NetBox fields, applying regex patterns, resolving FK prerequisites
3. **NetBox write orchestration** — threading, error isolation, upsert calls via `pynetbox2`

The new framework separates these concerns cleanly:

- Source API wiring → `collector/sources/vmware.py` and `collector/sources/xclarity.py`
- Transformation logic → `.hcl` mapping files in `mappings/`
- NetBox write orchestration → `collector/engine.py`, `collector/prerequisites.py`, `lib/pynetbox2.py`

Each new collector is an HCL file of ~200–400 lines instead of ~2,000 lines of Python. See [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) and [docs/HCL_DESIGN.md](../docs/HCL_DESIGN.md) for the framework design.

## Using these scripts directly

These scripts can still be run standalone against a live NetBox instance. They depend on `lib/pynetbox2.py` (formerly `pynetbox2.py` at the repo root) and the packages in `requirements.txt`. Update any `import pynetbox2` references to `sys.path` insert `lib/` if running them directly outside the package layout.
