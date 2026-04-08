# Developer Onboarding

This guide is for a new developer joining `hcl-netbox-discovery` and trying to become effective quickly.

## 1. Mental Model

At a high level, one collector run works like this:

1. `collector/config.py` parses an HCL mapping file into dataclasses.
2. `collector/engine.py` builds the source adapter and NetBox client.
3. The source adapter returns raw objects for a named collection.
4. The engine resolves prerequisites, evaluates fields, and upserts the parent object.
5. Nested blocks like `interface`, `ip_address`, `inventory_item`, `disk`, `module`, and `tagged_vlan` are processed.
6. Job state and logs are written to SQLite through `collector/db.py` and `collector/job_log_handler.py`.

The biggest operational nuance:

- `web_server.py` / `web/app.py` do not execute jobs directly
- the web UI queues jobs in SQLite
- `main.py --run-scheduler` executes queued and scheduled jobs

If you remember only one thing, remember that split.

## 2. Files To Read First

Read these in order:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/HCL_REFERENCE.md`
4. `collector/config.py`
5. `collector/engine.py`
6. `collector/field_resolvers.py`
7. `collector/db.py`
8. `web/app.py`

Then read a couple of focused tests:

- `tests/test_config.py`
- `tests/test_main.py`
- `tests/test_web.py`
- one adapter test for the area you plan to touch

## 3. First-Day Setup

```bash
pip install --user poetry
poetry install --with dev
```

Optional:

```bash
cp .env.example .env
```

Useful sanity checks:

```bash
poetry run pytest tests/test_config.py tests/test_db.py tests/test_main.py tests/test_web.py
poetry run ruff check .
poetry run ruff format . --check
```

## 3a. Branching Model

Use the long-lived branches intentionally:

- `dev`
  - default base for routine features and bug fixes
- `release/<version>`
  - versioned stabilization branch for the next production release
- `main`
  - production-only branch

Normal workflow:

1. `git fetch origin`
2. branch from `origin/dev`
3. open PR into `dev`
4. promote vetted batches from `dev` into the active `release/<version>` branch
5. promote `release/<version>` into `main`
6. create release tags from a clean checkout of `origin/main`

Only branch directly from `origin/main` for true production hotfixes, and back-port
those fixes to the active `release/<version>` branch and `dev` immediately after merge.

## 3b. Versioning

Releases follow semantic versioning from `1.0.0` onward:

- `MAJOR`: breaking changes only
- `MINOR`: backward-compatible features
- `PATCH`: backward-compatible fixes only

In this repository, treat these as breaking by default:

- incompatible HCL syntax or semantics
- removed settings or changed setting meaning
- removed API fields/routes or changed API response contracts
- schema changes that require operator action or migration awareness

## 4. Where Things Live

### Core runtime

- `main.py`: CLI entry point and scheduler loop
- `web_server.py`: Flask entry point
- `collector/config.py`: HCL parsing and runtime config modeling
- `collector/engine.py`: orchestration and threaded processing
- `collector/context.py`: per-item and per-run execution context
- `collector/field_resolvers.py`: HCL expression helpers
- `collector/prerequisites.py`: ensure/lookup helpers for NetBox prerequisites
- `collector/db.py`: jobs, logs, schedules, settings
- `collector/job_log_handler.py`: persistence of job logs

### Source systems

Adapters live in `collector/sources/`.

Use `rest.py` when the source can be modeled cleanly in HCL.
Use a dedicated adapter when the source needs SDK behavior, complicated auth, or source-specific enrichment.

### NetBox integration

- `pynetbox-wrapper`: the external custom NetBox client wrapper dependency

This file is important. Many behaviors that look like “engine logic” are actually implemented here:

- upsert semantics
- cache behavior
- retry behavior
- rate limiting
- cooldown / herd prevention

### Web UI

- `web/app.py`: routes and queueing behavior
- `web/templates/`: dashboard, job detail, schedules, cache, settings

## 5. Common Change Types

### Add a new HCL option

Touch:

- `docs/HCL_REFERENCE.md`
- `collector/config.py`
- `collector/engine.py` or the relevant consumer
- tests in `tests/test_config.py` and any affected behavior tests

### Add or update a source adapter

Touch:

- `collector/sources/<adapter>.py`
- `collector/engine.py` registry if adding a new adapter
- example mapping in `mappings/` if relevant
- tests for that adapter
- docs if the adapter is user-facing

### Change job/scheduler/web behavior

Touch:

- `main.py`
- `collector/db.py`
- `collector/job_log_handler.py`
- `web/app.py`
- `tests/test_main.py`, `tests/test_db.py`, `tests/test_web.py`
- `README.md` and `docs/ARCHITECTURE.md`

### Change field expression behavior

Touch:

- `collector/field_resolvers.py`
- `tests/test_field_resolvers.py`
- `docs/HCL_REFERENCE.md`

## 6. Test Strategy

This repo has a strong targeted test suite. Use it.

Recommended targeted runs:

- config parser: `poetry run pytest tests/test_config.py`
- engine logic: `poetry run pytest tests/test_engine_tags.py tests/test_engine_tagged_vlans.py tests/test_engine_primary_ip.py tests/test_engine_oob_ip.py`
- scheduler/web/DB: `poetry run pytest tests/test_db.py tests/test_main.py tests/test_web.py`
- adapters: run only the adapter you changed

Run the full suite before merging anything non-trivial.

## 7. Development Norms

- Prefer small, local changes over broad refactors.
- Keep docs current when behavior changes.
- If you add a new env-driven behavior, make sure `.env.example` and docs stay aligned.
- Preserve backward compatibility in HCL where practical.
- Use focused logging. Avoid noisy INFO logs unless they help operators.

## 8. Easy Mistakes To Avoid

- Do not assume the web UI runs jobs directly.
- Do not document mapping auto-discovery from `main.py`; manual runs require `--mapping`.
- Do not treat `collector {}` flags as hardcoded schema beyond the reserved keys. Extra keys become `collector.<flag>` values in expressions.
- Do not forget that DB-backed settings can override environment variables.
- Do not change `collector/db.py` casually without considering both web and scheduler paths.

## 9. Good Starter Tasks

If you are new to the repo, these are good first changes:

- improve or add a focused adapter test
- add a missing HCL reference example
- tighten README instructions around a confusing workflow
- add a small web UI polish that does not change execution behavior
- fix a doc/code drift issue with matching tests

## 10. Before You Open A PR

Run:

```bash
poetry run ruff check .
poetry run ruff format . --check
poetry run pytest
```

Then confirm:

- docs are updated if needed
- no secrets or local `.env` changes are included
- the change matches the existing architecture instead of introducing a parallel pattern
