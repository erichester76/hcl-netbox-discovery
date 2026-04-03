# Contributing

This document is for developers new to `hcl-netbox-discovery` who want to get productive quickly without reverse-engineering the repo.

## What This Project Is

`hcl-netbox-discovery` is a Python 3.12 project that syncs infrastructure data into NetBox using HCL mapping files. The collector engine is in `collector/`, the Flask UI is in `web/`, and the custom NetBox client wrapper is in `lib/pynetbox2.py`.

The web UI and the scheduler share one SQLite database:

- the web UI queues jobs and manages schedules/settings
- `main.py --run-scheduler` executes queued and scheduled jobs

Read these first:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/HCL_REFERENCE.md`
4. `docs/DEVELOPER_ONBOARDING.md`

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
- `lib/pynetbox2.py`: NetBox client wrapper with retries, caching, and upsert helpers
- `mappings/`: example HCL mapping files
- `regex/`: plain-text mapping files used by `regex_file()`
- `tests/`: test suite
- `docs/`: design and HCL docs

## Development Workflow

1. Read the relevant docs and tests first.
2. Make the smallest change that solves the problem.
3. Run focused tests locally.
4. Update docs if behavior or usage changed.
5. Run `poetry run ruff check .` and `poetry run ruff format .` before opening a PR.

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
- `docs/DEVELOPER_ONBOARDING.md`: contributor workflow and codebase orientation

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

## When In Doubt

- prefer surgical changes
- preserve the web-queues / scheduler-executes split
- avoid changing HCL semantics without updating tests and docs
- ask for clarification instead of broadening scope
