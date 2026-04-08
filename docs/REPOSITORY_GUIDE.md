# Repository Guide

This guide covers repository-specific details for `hcl-netbox-discovery`.
Use it with `README.md`, `docs/ARCHITECTURE.md`, and
`docs/HCL_REFERENCE.md`.

## Documentation Map

Use the docs intentionally:

- `README.md`
  - user-facing setup, commands, deployment, operational behavior
- `docs/ARCHITECTURE.md`
  - runtime design, component interactions, DB/runtime flow
- `docs/HCL_REFERENCE.md`
  - HCL syntax, expressions, supported options, mapping semantics
- `CONTRIBUTING.md`
  - contributor workflow, PR expectations, branching/release process
- `docs/REPOSITORY_GUIDE.md`
  - repository-specific development context, commands, landmarks, guardrails,
    and operational workflows

## 1. Mental Model

At a high level, one collector run works like this:

1. `src/collector/config.py` parses an HCL mapping file into dataclasses.
2. `src/collector/engine.py` builds the source adapter and NetBox client.
3. The source adapter returns raw objects for a named collection.
4. The engine resolves prerequisites, evaluates fields, and upserts the parent
   object.
5. Nested blocks like `interface`, `ip_address`, `inventory_item`, `disk`,
   `module`, and `tagged_vlan` are processed.
6. Job state and logs are written to SQLite through `src/collector/db.py` and
   `src/collector/job_log_handler.py`.

The biggest operational nuance:

- `src/web/web_server.py` / `src/web/app.py` do not execute jobs directly
- the web UI queues jobs in SQLite
- `main.py --run-scheduler` executes queued and scheduled jobs

If you remember only one thing, remember that split.

## 2. Files To Read First

Read these in order:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/HCL_REFERENCE.md`
4. `src/collector/config.py`
5. `src/collector/engine.py`
6. `src/collector/field_resolvers.py`
7. `src/collector/db.py`
8. `src/web/app.py`

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

Only branch directly from `origin/main` for true production hotfixes, and
back-port those fixes to the active `release/<version>` branch and `dev`
immediately after merge.

### Branch Naming

Use prefixes that describe the change type:

- `feature/<topic>` for normal feature work
- `bugfix/<topic>` for routine defect fixes
- `docs/<topic>` for documentation-only changes
- `chore/<topic>` for maintenance work
- `hotfix/<topic>` for urgent production fixes from `origin/main`

Do not use a single generic branch namespace for unrelated work. The branch
name should make it obvious how the change should flow through `dev`,
`release/<version>`, and `main`.

Container publishing follows the same flow:

- `dev` pushes publish a `:dev` image for code-bearing merges
- `docs/*` and `chore/*` merges into `dev` do not publish a `:dev` image
- `main` publishes production images
- `v*` tags publish release images

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
- `src/web/web_server.py`: Flask entry point
- `src/collector/config.py`: HCL parsing and runtime config modeling
- `src/collector/engine.py`: orchestration and threaded processing
- `src/collector/context.py`: per-item and per-run execution context
- `src/collector/field_resolvers.py`: HCL expression helpers
- `src/collector/prerequisites.py`: ensure/lookup helpers for NetBox prerequisites
- `src/collector/db.py`: jobs, logs, schedules, settings
- `src/collector/job_log_handler.py`: persistence of job logs

### Source systems

Adapters live in `src/collector/sources/`.

Use `rest.py` when the source can be modeled cleanly in HCL.
Use a dedicated adapter when the source needs SDK behavior, complicated auth, or
source-specific enrichment.

### NetBox integration

- `pynetbox-wrapper`: the external custom NetBox client wrapper dependency

This file is important. Many behaviors that look like “engine logic” are
actually implemented here:

- upsert semantics
- cache behavior
- retry behavior
- rate limiting
- cooldown / herd prevention

### Web UI

- `src/web/app.py`: routes and queueing behavior
- `src/web/templates/`: dashboard, job detail, schedules, cache, settings

## 5. Common Change Types

### Add a new HCL option

Touch:

- `docs/HCL_REFERENCE.md`
- `src/collector/config.py`
- `src/collector/engine.py` or the relevant consumer
- tests in `tests/test_config.py` and any affected behavior tests

### Add or update a source adapter

Touch:

- `src/collector/sources/<adapter>.py`
- `src/collector/engine.py` registry if adding a new adapter
- example mapping in `mappings/` if relevant
- tests for that adapter
- docs if the adapter is user-facing

### Change job/scheduler/web behavior

Touch:

- `main.py`
- `src/collector/db.py`
- `src/collector/job_log_handler.py`
- `src/web/app.py`
- `tests/test_main.py`, `tests/test_db.py`, `tests/test_web.py`
- `README.md` and `docs/ARCHITECTURE.md`

### Change field expression behavior

Touch:

- `src/collector/field_resolvers.py`
- `tests/test_field_resolvers.py`
- `docs/HCL_REFERENCE.md`

## 6. Test Strategy

This repo has a strong targeted test suite. Use it.

Recommended targeted runs:

- config parser: `poetry run pytest tests/test_config.py`
- engine logic: `poetry run pytest tests/test_engine_tags.py tests/test_engine_tagged_vlans.py tests/test_engine_primary_ip.py tests/test_engine_oob_ip.py`
- scheduler, web, DB: `poetry run pytest tests/test_db.py tests/test_main.py tests/test_web.py`
- adapters: run only the adapter you changed

Run the full suite before merging anything non-trivial.

## 7. Development Norms

- Prefer small, local changes over broad refactors.
- Keep docs current when behavior changes.
- If you add a new env-driven behavior, make sure `.env.example` and docs stay
  aligned.
- Preserve backward compatibility in HCL where practical.
- Use focused logging. Avoid noisy INFO logs unless they help operators.

## 8. Root-Cause Guardrails

These rules are specific to this repository because several bug families have
come from local symptom fixes that left the underlying model inconsistent.

### Job lifecycle and scheduler changes

- Treat job claiming and schedule firing as **database-state problems first**,
  not in-process coordination problems.
- Do not rely on Python sets, thread-local state, or request-local checks as
  the sole correctness mechanism for queue claiming or schedule de-duplication.
- If you touch CLI runs, queued web jobs, or scheduled jobs, compare all three
  paths and keep their persisted metadata and terminal status semantics aligned.
- Prefer one shared execution/finish path over separate near-duplicate flows.
- If a run can complete with item-level errors, ensure the persisted status
  distinguishes that from full success.

### Failure semantics and data quality

- Do not silently convert fetch failures into empty collections unless the user
  explicitly asked for best-effort behavior and the docs/tests are updated to
  say so.
- Do not swallow expression/config evaluation errors and then continue with
  placeholder writes unless the placeholder behavior is explicitly intended.
- Be especially careful with `"Unknown"`-style fallback objects. Missing
  required source data should usually fail, skip, or mark partial rather than
  create shared junk records.
- If a behavior is intentionally best-effort, log it at a level operators will
  actually see and document the consequence.

### Nested write integrity

- Child objects must not be promoted or linked as if their parent write
  succeeded when the parent write actually failed.
- For parent/child flows such as interface → IP → primary IP assignment, guard
  the whole downstream chain on the parent object existing in NetBox.
- Keep object integrity more important than “partial progress” when the partial
  progress would produce misleading or unattached records.

### Retry, transport, and adapter behavior

- Do not add another retry loop on top of an existing retry loop without first
  proving why the current layer cannot own the behavior.
- Prefer one shared transport/retry policy per subsystem instead of duplicating
  session setup, timeout defaults, SSL handling, and backoff behavior across
  adapters.
- If an option is documented and parsed, wire it through completely or remove
  it. Avoid dead configuration surface area.
- When adapter behavior diverges from other adapters, document why the
  difference is intentional.

### Reporting and observability

- Counters, summaries, and job statuses must describe what actually happened.
- Do not record `created`, `updated`, `skipped`, or `success` unless the code
  can truly distinguish that outcome.
- Logging capture should include the important lifecycle and failure events for
  a job, not just the happy-path collector internals.

### Web and security changes

- Any new state-changing Flask route must be reviewed for authentication,
  authorization, and CSRF implications.
- Do not assume the web UI is private unless the task explicitly states that as
  a deployment constraint.

### Testing expectations

- Prefer tests that drive the real production path over tests that reimplement
  the same logic with mocks.
- For scheduler and queue changes, add tests that exercise cross-path
  invariants, not just one entry point.
- For bug fixes, add at least one regression test that would have failed before
  the fix and passes after it.
- For retries, pagination, and adapter fetch logic, test the full call chain so
  stacked retries or silent fallbacks are visible.

## 9. Remote Job API Workflow

Use the deployed web API as the default artifact/log retrieval path before
falling back to manual file sync.

### Current deployment

- Current Clemson deployment base URL:
  `http://4gk-mon-p-dkr01.server.clemson.edu:5000`
- API authentication is token-based for `/api/*`.
- The token is stored in the DB-backed `WEB_API_TOKEN` setting.
- **Never** commit the live token value into the repository or documentation.
  Get the current token from the operator or the deployed settings store at
  runtime.

### Required endpoints

- `GET /api/jobs`
  - Use for completed/recent job discovery.
  - Supports:
    - `limit`
    - `after_id`
    - `status`
    - `hcl_file`
- `GET /api/running-jobs`
  - Use only for active-job discovery.
- `GET /api/jobs/<id>/artifact`
  - Use for structured artifact retrieval after a job reaches terminal state.
- `GET /api/jobs/<id>/logs?after_id=<n>`
  - Use for token-authenticated log polling and live triage.

### Default polling loop

1. Poll `/api/jobs?after_id=<last_seen_id>&limit=<n>` for newly completed jobs.
2. For each new terminal job:
   - inspect `status`
   - fetch `/api/jobs/<id>/artifact`
3. If a job is still active and live evidence is needed:
   - poll `/api/jobs/<id>/logs?after_id=<last_log_id>`
4. Prefer completed-job polling over `running-jobs` for fast sources such as
   `azure` and `jnsu`, because they can finish between polls.

### Runtime notes

- Use `Authorization: Bearer <token>` or `X-API-Key: <token>` for `/api/*`.
- `/api/jobs` may include inline `artifact` content, but the canonical detailed
  artifact retrieval route is still `/api/jobs/<id>/artifact`.
- When triaging live runs, create issues from API log/artifact evidence instead
  of waiting for manual artifact sync if the API already exposes the needed
  data.

## 10. Easy Mistakes To Avoid

- Do not assume the web UI runs jobs directly.
- Do not document mapping auto-discovery from `main.py`; manual runs require
  `--mapping`.
- Do not treat `collector {}` flags as hardcoded schema beyond the reserved
  keys. Extra keys become `collector.<flag>` values in expressions.
- Do not forget that DB-backed settings can override environment variables.
- Do not change `src/collector/db.py` casually without considering both web and
  scheduler paths.

## 11. Good Starter Tasks

If you are new to the repo, these are good first changes:

- improve or add a focused adapter test
- add a missing HCL reference example
- tighten README instructions around a confusing workflow
- add a small web UI polish that does not change execution behavior
- fix a doc/code drift issue with matching tests

## 12. Before You Open A PR

Run:

```bash
poetry run ruff check .
poetry run ruff format . --check
poetry run pytest
```

Then confirm:

- docs are updated if needed
- no secrets or local `.env` changes are included
- the change matches the existing architecture instead of introducing a parallel
  pattern
