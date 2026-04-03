# Fix Adapter Contract Drift and Consolidate Transport/Retry Behavior

## Summary

Correct broken adapter contracts and reduce duplicated transport and retry logic
so adapters behave consistently and documented options actually work.

## Why

The review found several adapter-level problems that all point to the same root
cause: transport, retry, and failure semantics are duplicated across layers and
adapters. That has already produced a broken REST detail templating contract,
stacked NetBox retry loops, dead configuration surface such as `NetBoxSource`
`page_size`, and adapters that silently convert fetch failures into empty data.

## Review Findings Covered

- `[P2] REST detail templating ignores placeholder names`
- NetBox wrapper retries are layered multiple times on the same call path
- Tenable fetch failures can become empty successful runs
- `NetBoxSource.page_size` is documented and parsed but not enforced
- Adapter transport behavior has drifted across source types

## Goals

- Fix REST detail template substitution to honor placeholder names from list
  items
- Collapse duplicate retry ownership so one layer is responsible for backoff
  behavior
- Remove or fully wire documented adapter options
- Align adapter failure semantics so outages are visible instead of silently
  treated as empty data

## Non-Goals

- A full adapter rewrite in one PR
- Replacing every source adapter abstraction at once

## Acceptance Criteria

- REST detail enrichment supports named placeholders correctly
- NetBox retry behavior is owned by one layer per call path, not stacked across
  nested loops
- `page_size` either works end-to-end or is removed from docs/config surface
- Source fetch failures that should fail a run are no longer silently converted
  into healthy empty collections

## Suggested Implementation Notes

- This issue may be split into focused PRs while staying under one root-cause
  umbrella:
  - REST contract fix
  - retry ownership cleanup
  - failure-semantics alignment
- Keep shared transport behavior centralized where practical

## Tests

- `tests/test_rest.py`
- `tests/test_netbox.py`
- `tests/test_tenable.py`
- `tests/test_pynetbox2_cache.py`
- Add regression tests that prove the full call chain behavior, not just helper
  functions

## Suggested Branch

- `codex/adapter-contracts-transport`
