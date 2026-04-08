# Engineering Audit

This document captures concrete refactor candidates identified during the
repository hygiene pass. These are not speculative rewrites. Each item points
at code that is currently working but has accumulated enough stacked fixes,
duplication, or coupling to justify a future focused change.

## 1. Unify job lifecycle transitions

**Files**
- `main.py`
- `src/collector/db.py`
- `src/web/app.py`

**Why**
- The project now supports queued, running, success, partial, failed, and
  stopped states, plus `stop_requested`, `artifact_json`, and stale-job
  reconciliation.
- Recent fixes have touched all three execution surfaces: CLI runs, queued web
  jobs, and the scheduler startup path.

**Refactor target**
- Extract a single shared lifecycle helper for:
  - claiming a queued job
  - attaching log capture
  - finishing with status/summary/artifact
  - reconciling orphaned in-flight jobs

## 2. Consolidate job API response shaping

**Files**
- `src/web/app.py`
- `src/collector/db.py`

**Why**
- Job JSON is now exposed through multiple routes:
  - `/api/jobs`
  - `/api/running-jobs`
  - `/api/jobs/<id>/artifact`
  - `/api/jobs/<id>/logs`
- The route layer still performs some response shaping that could be centralized.

**Refactor target**
- Move job serialization rules into one helper so route handlers stay thin and
  field additions do not drift between endpoints.

## 3. Collapse duplicated config evaluation helpers

**Files**
- `src/collector/config.py`

**Why**
- `_eval_config_str()` and `_eval_config_str_with_overrides()` are closely
  related and differ mainly in lookup precedence.
- Auto source fan-out and legacy iterator support increased the number of paths
  that rebuild `source` config state.

**Refactor target**
- Introduce one internal evaluator with pluggable lookup order, then keep the
  public helper names as thin wrappers if needed.

## 4. Reduce module schema derivation coupling

**Files**
- `src/collector/engine.py`
- `src/collector/prerequisites.py`
- `mappings/xclarity-modules.hcl.example`

**Why**
- XClarity module-profile churn and null-schema bugs required fixes in both the
  engine and prerequisite layers.
- The schema contract is currently influenced by both configured fields and
  observed runtime payload values.

**Refactor target**
- Make module-type profile schema derivation explicit and deterministic from the
  configured module field set, with runtime values only filling data, not
  altering schema identity.

## 5. Split auth helpers from Flask route definitions

**Files**
- `src/web/app.py`

**Why**
- The app factory now contains session auth, API token auth, CSRF checks, route
  definitions, and helper utilities in one module.
- This still works, but it broadens the review surface for security-sensitive
  changes.

**Refactor target**
- Move auth/token/CSRF helpers into a small internal module or dedicated helper
  section so route changes and auth changes can be reviewed separately.

## 6. Clarify source payload contracts for placement and identity

**Files**
- `src/collector/sources/catc.py`
- `src/collector/sources/vmware.py`
- `src/collector/sources/netbox.py`
- related mappings under `mappings/`

**Why**
- Several recent bug fixes were really adapter-to-mapping contract problems:
  hierarchy depth, site context, device identity, and nested parent context.

**Refactor target**
- Document and enforce a clearer contract for source payloads:
  - placement-related fields
  - stable identity fields
  - nested object parent context
