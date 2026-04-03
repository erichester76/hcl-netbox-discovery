# Make Job Reporting and Observability Match Actual Outcomes

## Summary

Fix counters, summaries, and logging capture so operator-facing reporting
describes what actually happened during a run.

## Why

The review found that successful upserts are always counted as `created`, even
when NetBox updated an existing record or performed a no-op. More broadly, job
status and logging visibility drifted across execution paths, and some important
lifecycle events are easy to miss.

## Review Findings Covered

- `[P2] Successful upserts are always counted as created`
- Related job-summary/status drift discovered during the review
- Logging and job visibility gaps worth tightening while the reporting model is
  being corrected

## Goals

- Distinguish create vs update vs no-op outcomes in runtime reporting
- Ensure persisted job summaries reflect real write outcomes
- Improve visibility into lifecycle failures and partial-success states

## Non-Goals

- Rebuilding the whole logging stack
- Changing business logic unrelated to reporting correctness

## Acceptance Criteria

- Update-heavy runs no longer report everything as `created`
- Job summaries and terminal status are consistent with actual engine outcomes
- Tests prove the reported counters match real create/update/no-op behavior

## Suggested Implementation Notes

- The fix may require the NetBox wrapper to return richer write outcome metadata
  instead of just an object reference
- Keep reporting changes coordinated with the lifecycle issue so job summaries
  and terminal state semantics remain aligned

## Tests

- `tests/test_engine*.py`
- Any `lib/pynetbox2.py` tests needed for richer upsert outcome reporting

## Suggested Branch

- `codex/truthful-reporting`

