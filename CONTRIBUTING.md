# Contributing

This document is for developers new to `hcl-netbox-discovery` who want to get productive quickly without reverse-engineering the repo.

## What This Project Is

`hcl-netbox-discovery` is a Python 3.12 project that syncs infrastructure data into NetBox using HCL mapping files. The collector engine is in `collector/`, the Flask UI is in `web/`, and the custom NetBox client wrapper is provided by the external `pynetbox-wrapper` dependency.

The web UI and the scheduler share one SQLite database:

- the web UI queues jobs and manages schedules/settings
- `main.py --run-scheduler` executes queued and scheduled jobs

Read these first:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/HCL_REFERENCE.md`
4. `docs/REPOSITORY_GUIDE.md`

## Local Setup

### Recommended

```bash
pip install --user poetry
poetry install --with dev
```

Run commands through Poetry:

```bash
poetry run pytest
poetry run ruff check .
poetry run python main.py --mapping mappings/vmware.hcl --dry-run
```

### Legacy fallback

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

If you need environment-based config for local runs:

```bash
cp .env.example .env
```

Then edit `.env` with local credentials and settings. Do not commit it.

## Common Commands

### Tests

```bash
poetry run pytest
poetry run pytest tests/test_config.py
poetry run pytest tests/test_main.py tests/test_web.py
```

### Lint and format

```bash
poetry run ruff check .
poetry run ruff check . --fix
poetry run ruff format .
poetry run pre-commit run --all-files
```

### Run the collector

```bash
poetry run python main.py --mapping mappings/vmware.hcl --dry-run
poetry run python main.py --mapping mappings/vmware.hcl
poetry run python main.py --run-scheduler
```

### Run the web UI

```bash
poetry run python web_server.py
```

## Repo Layout

- `collector/`: parser, engine, DB, field resolvers, prerequisites, source adapters
- `web/`: Flask app and templates
- `pynetbox-wrapper`: external NetBox client wrapper dependency with retries, caching, and upsert helpers
- `mappings/`: example HCL mapping files
- `regex/`: plain-text mapping files used by `regex_file()`
- `tests/`: test suite
- `docs/`: design and HCL docs

## Development Workflow

1. Read the relevant docs and tests first.
2. Start from the correct long-lived branch:
   - feature and bugfix branches branch from `origin/dev`
   - stabilization branches target the active `release/<version>` branch
   - production promotions target `main`
3. Make the smallest change that solves the problem.
4. Run focused tests locally.
5. Update docs if behavior or usage changed.
6. Run `poetry run ruff check .` and `poetry run ruff format .` before opening a PR.

## Branching And Release Flow

This repository uses three long-lived branches:

- `dev`
  - integration branch for day-to-day feature and bugfix work
  - all normal feature/bug branches should start from `origin/dev`
  - all routine PRs should target `dev`
- `release/<version>`
  - versioned stabilization branch for the next production cut
  - promote work from `dev` into the active release branch in controlled batches
  - use this branch for final verification, documentation cleanup, and release notes
- `main`
  - production branch only
  - only release promotions and production hotfixes should land here
  - container/image builds and release tags should be treated as `main` artifacts

Image publishing policy:

- merges to `dev` publish a `:dev` image when the incoming branch is a code
  branch
- merges from `docs/*` and `chore/*` branches do not publish a `:dev` image
- pushes to `main` publish production images
- `v*` tags publish versioned release images

Recommended flow:

1. branch from `origin/dev`
2. open PR into `dev`
3. merge reviewed work into `dev`
4. periodically promote `dev` into the active `release/<version>` branch
5. validate that release branch
6. promote `release/<version>` into `main`
7. create version tags from a clean checkout of `origin/main`

Hotfix rule:

- if production needs an urgent fix, branch from `origin/main`
- merge the fix into `main`
- immediately back-merge or cherry-pick it into the active `release/<version>` branch and `dev` so the branches do not drift

## Branch Naming

Use branch names that communicate intent:

- `feature/<topic>`
  - default for backward-compatible features targeting `dev`
- `bugfix/<topic>`
  - default for routine fixes targeting `dev`
- `docs/<topic>`
  - documentation-only work, normally targeting `dev`
- `chore/<topic>`
  - maintenance or cleanup work with no user-facing contract change
- `hotfix/<topic>`
  - urgent production fixes branched from `origin/main`
- `release/<version>`
  - the active stabilization branch, not a short-lived feature branch

Avoid a generic branch prefix for unrelated work. The prefix should tell
reviewers what kind of change is being promoted through `dev`,
`release/<version>`, and `main`.

## Versioning Rules

Version tags follow semantic versioning:

- `MAJOR`
  - breaking changes only
  - examples: incompatible HCL semantics, removed settings, removed or changed
    API contracts, schema changes requiring operator action
- `MINOR`
  - backward-compatible features
- `PATCH`
  - backward-compatible fixes only

Before creating a release tag, explicitly classify the change set:

- if it contains any breaking change, bump `MAJOR`
- if it adds features without breaking compatibility, bump `MINOR`
- if it only fixes bugs or docs without breaking compatibility, bump `PATCH`

## Choosing Tests

You do not always need the full suite while iterating.

- HCL/config changes: `tests/test_config.py`
- engine behavior: `tests/test_engine*.py`
- DB / scheduler / job lifecycle: `tests/test_db.py tests/test_main.py tests/test_web.py`
- source adapter changes: matching adapter test module
- cache / retry behavior: `tests/test_pynetbox2_cache.py tests/test_thundering_herd.py`

Run the full suite before merging any non-trivial change.

## Documentation Rules

When a change affects behavior, update the matching docs in the same branch:

- `README.md`: user-visible commands and operational behavior
- `docs/ARCHITECTURE.md`: runtime model, components, DB design
- `docs/HCL_REFERENCE.md`: HCL syntax and supported options
- `docs/REPOSITORY_GUIDE.md`: contributor workflow and codebase orientation

## Pull Requests

Good PRs in this repo are:

- narrowly scoped
- tested
- explicit about runtime/config impact
- accompanied by doc updates when behavior changed

Include:

- what changed
- why it changed
- how you tested it
- any env var, schema, or Docker impact

Target branches:

- normal feature/bug PRs: `dev`
- stabilization/promote PRs: active `release/<version>` branch
- production promotion PRs: `main`

Do not open routine feature work directly against `main`.

## When In Doubt

- prefer surgical changes
- preserve the web-queues / scheduler-executes split
- avoid changing HCL semantics without updating tests and docs
- ask for clarification instead of broadening scope
