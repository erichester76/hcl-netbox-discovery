# Add Authentication and CSRF Protection to the Web Control Plane

## Summary

Protect all state-changing Flask routes with an explicit authentication boundary
and CSRF protection.

## Why

The review found that the web control plane exposes job execution, schedule
mutation, cache actions, and settings updates as unauthenticated POST routes.
That is acceptable only in a tightly isolated environment, but the codebase does
not currently enforce or document such an assumption.

## Review Findings Covered

- `[P1] State-changing web routes have no auth boundary`

## Goals

- Add a clear authentication mechanism for the Flask UI
- Add CSRF protection for state-changing form submissions and POST endpoints
- Ensure the security model is documented in the README and architecture docs

## Non-Goals

- Building a full RBAC system unless required later
- Reworking unrelated dashboard UX

## Acceptance Criteria

- Writable routes cannot be triggered anonymously
- State-changing POST requests are CSRF-protected
- The deployment assumptions are documented explicitly
- Tests cover both allowed and rejected behavior

## Suggested Implementation Notes

- Keep the auth model simple and explicit
- Favor a solution that fits the current Flask app shape without introducing a
  large framework migration

## Tests

- `tests/test_web.py`
- Add route-level security regression tests for job run, schedules, cache, and
  settings endpoints

## Suggested Branch

- `codex/web-control-plane-security`

