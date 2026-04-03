# AGENTS.md

Repository-specific guidance for AI coding agents working in `hcl-netbox-discovery`.

## First Read

Before changing code, read:

1. `README.md`
2. `docs/ARCHITECTURE.md`
3. `docs/HCL_REFERENCE.md`

Those three files describe the current execution model, HCL surface area, and the split between the web UI and the scheduler worker.

## Project Reality

- This repository does **not** use a `/src` layout. Main code lives in `collector/`, `web/`, `lib/`, `main.py`, and `web_server.py`.
- The primary Python metadata file is `pyproject.toml`.
- Poetry is the primary dependency and environment workflow.
- `requirements.txt` and `requirements-dev.txt` are legacy/fallback install paths and should not become the primary source of truth.
- The project targets Python 3.12 in packaging and CI.
- Formatting and linting use `ruff`, not `black`.
- Tests use `pytest`.
- The long-running worker is `poetry run python main.py --run-scheduler`.
- The Flask UI entry point is `poetry run python web_server.py`.

## Development Commands

Use the lightest command that fits the task.

### Local environment

```bash
pip install --user poetry
poetry install --with dev
```

Run commands via Poetry:

```bash
poetry run pytest
poetry run ruff check .
poetry run python main.py --mapping mappings/vmware.hcl --dry-run
```

Legacy fallback:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

### Tests

```bash
poetry run pytest
poetry run pytest tests/test_config.py
poetry run pytest tests/test_web.py -q
```

### Lint and format

```bash
poetry run ruff check .
poetry run ruff check . --fix
poetry run ruff format .
poetry run pre-commit run --all-files
```

### Run the app

```bash
poetry run python main.py --mapping mappings/vmware.hcl --dry-run
poetry run python main.py --run-scheduler
poetry run python web_server.py
```

## How To Work In This Repo

- Stay tightly scoped to the user’s request.
- Prefer surgical edits over broad refactors.
- Keep changes consistent with the current architecture and naming.
- Update docs when behavior, commands, or architecture change.
- Add or update tests when you change behavior.
- Do not invent new frameworks, layers, or abstractions unless the task truly requires them.

## Root-Cause Guardrails

These rules are here because this codebase has accumulated several path-specific
fixes that solved one symptom while leaving the underlying model inconsistent.

### Job lifecycle and scheduler changes

- Treat job claiming and schedule firing as **database-state problems first**, not
  in-process coordination problems.
- Do not rely on Python sets, thread-local state, or request-local checks as the
  sole correctness mechanism for queue claiming or schedule de-duplication.
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
- Be especially careful with `"Unknown"`-style fallback objects. Missing required
  source data should usually fail, skip, or mark partial rather than create
  shared junk records.
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
- Logging capture should include the important lifecycle and failure events for a
  job, not just the happy-path collector internals.

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

## Review-To-Delivery Workflow

- When working from review findings, group the work by root cause instead of
  applying one-off local patches in unrelated branches.
- Prefer one GitHub issue per root cause area or tightly related bug family.
- In issue and PR descriptions, call out:
  - the invariant being restored
  - the execution paths affected
  - the regression tests added
- Keep branches focused. Avoid mixing scheduler lifecycle work, engine integrity
  fixes, and adapter transport refactors in one PR unless the user explicitly
  wants a larger coordinated change.

## Codebase Landmarks

- `collector/config.py`: HCL parsing and config dataclasses
- `collector/engine.py`: main orchestration and threaded object processing
- `collector/field_resolvers.py`: expression evaluation helpers used by HCL
- `collector/prerequisites.py`: ensure/lookup helpers for NetBox prerequisites
- `collector/db.py`: shared SQLite jobs/logs/schedules/settings store
- `collector/sources/`: source adapters
- `lib/pynetbox2.py`: NetBox client wrapper with cache, retry, and upsert behavior
- `web/app.py`: Flask routes and web-side job queueing
- `tests/`: primary regression safety net

## Documentation Expectations

If you change:

- CLI or web behavior: update `README.md`
- system design or runtime flow: update `docs/ARCHITECTURE.md`
- HCL syntax or supported options: update `docs/HCL_REFERENCE.md`
- contributor/developer workflow: update `CONTRIBUTING.md` or `docs/DEVELOPER_ONBOARDING.md`

## Safety Notes

- Never commit secrets.
- Treat `.env.example` as the reference list of supported environment variables.
- Be careful with SQLite schema changes in `collector/db.py`; the web UI and scheduler both depend on that file.
- Preserve the web/scheduler split: the web UI queues jobs, and the scheduler worker executes them.

## Testing Heuristics

- For parser/config changes: run `poetry run pytest tests/test_config.py`
- For engine changes: run the relevant `poetry run pytest tests/test_engine*.py` modules
- For DB or scheduler changes: run `poetry run pytest tests/test_db.py tests/test_main.py tests/test_web.py`
- For source adapter changes: run the matching adapter test module with `poetry run pytest`

## Scope Discipline

- Do exactly what was asked.
- Do not silently rewrite unrelated docs or code.
- If you notice stale guidance that directly affects the requested task, fix it in the same change and mention it clearly.

Last updated: 2026-04-01
