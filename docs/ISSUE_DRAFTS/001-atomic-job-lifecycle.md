# Atomic Job Lifecycle Across CLI, Queued, and Scheduled Runs

## Summary

Unify job claiming and lifecycle state transitions so queued jobs and due
schedules are claimed atomically in the database, and CLI, queued, and scheduled
runs all persist metadata and terminal status consistently.

## Why

The review found that queue claiming and schedule de-duplication currently rely
on process-local Python sets instead of database-backed claims. That makes the
execution model unsafe once more than one scheduler process exists. It also
found drift between CLI, queued, and scheduled execution paths, especially in
how they persist `dry_run`, compute `partial` vs `success`, and record failures.

## Review Findings Covered

- `[P1] Queued jobs are not claimed atomically`
- `[P1] Due schedules can double-fire across processes`
- Scheduled runs can also be lost if `next_run_at` is advanced before the worker
  successfully starts
- CLI, queued, and scheduled runs currently persist inconsistent metadata and
  terminal states

## Goals

- Introduce an atomic claim model for queued jobs
- Introduce an atomic claim/fire model for due schedules
- Remove correctness dependence on `_active_queued_job_ids` and
  `_active_schedule_ids`
- Align terminal status semantics for CLI, queued, and scheduled runs
- Ensure item-level errors persist as `partial` in every execution path
- Ensure missing mapping files and other startup failures are visible in job
  history consistently

## Non-Goals

- Replacing SQLite
- Adding distributed locking infrastructure
- Reworking unrelated web UI presentation

## Acceptance Criteria

- A queued job cannot be executed twice just because two scheduler processes are
  polling the same DB
- A due schedule cannot double-fire just because two scheduler processes poll at
  the same time
- Schedule claiming does not require pre-advancing `next_run_at` in a way that
  can silently lose a run
- CLI, queued, and scheduled runs all persist comparable metadata and use the
  same `success` / `partial` / `failed` semantics
- Missing mapping files result in durable failure visibility, not just logs

## Suggested Implementation Notes

- Add DB-level claim/update APIs instead of read-then-start flows
- Prefer one shared execution wrapper used by all run entry points
- Separate “claim accepted” from “run finished” cleanly in the schema/API

## Tests

- `tests/test_db.py`
- `tests/test_main.py`
- `tests/test_web.py`
- Add regression tests covering concurrent claim attempts and aligned terminal
  status semantics across CLI, queued, and scheduled runs

## Suggested Branch

- `bugfix/atomic-job-lifecycle`
