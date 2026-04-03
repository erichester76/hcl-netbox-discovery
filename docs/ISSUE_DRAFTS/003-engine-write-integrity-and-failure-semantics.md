# Enforce Engine Write Integrity and Fail-Loud Semantics

## Summary

Tighten engine behavior so parent-child write chains remain consistent and
invalid expressions or prerequisite inputs do not silently turn into placeholder
NetBox data.

## Why

The review found two closely related problems. First, nested writes can continue
after a parent write fails, which allows IPs to be created and promoted even
when the interface write failed. Second, expression and prerequisite failures
are often normalized into `None` or `"Unknown"` and then converted into valid
NetBox writes, which hides real source or mapping errors.

## Review Findings Covered

- `[P1] Interface failure does not block nested IP promotion`
- Broken expressions can silently become placeholder prerequisite objects
- Targeted race handling exists in one prerequisite path without a generalized
  strategy
- Some “first” assignment semantics are scoped per interface block rather than
  per parent object

## Goals

- Prevent downstream child promotion when required parent writes fail
- Revisit resolver and prerequisite failure semantics so invalid inputs fail,
  skip, or mark partial rather than create junk records
- Normalize prerequisite collision handling into a reusable pattern instead of
  one-off fixes
- Clarify primary/OOB assignment invariants

## Non-Goals

- Rewriting the full HCL language
- Broad source-adapter changes outside engine/prerequisite behavior

## Acceptance Criteria

- Failed interface writes cannot still produce promoted primary or OOB IPs
- Invalid expressions and missing required prerequisite inputs do not silently
  create shared `"Unknown"` records unless that fallback is explicitly intended
- Prerequisite collision handling is consistent across the `ensure_*` family
- Tests cover the production execution path rather than a reimplemented mock-only
  copy of the logic

## Tests

- `tests/test_engine*.py`
- `tests/test_prerequisites.py`
- Add regression coverage for interface failure, primary/OOB assignment, and
  expression/prerequisite error handling

## Suggested Branch

- `codex/engine-write-integrity`

