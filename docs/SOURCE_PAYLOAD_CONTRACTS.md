# Source Payload Contracts

This document defines the stable adapter-to-mapping field names that shipped
example mappings rely on for placement and identity. Adapters should emit these
fields consistently, and mapping changes should update these contracts and the
associated tests together.

## Placement fields

Use these names when a source can expose placement context:

- `site_name`
  - normalized site label intended for direct NetBox site resolution or regex
    mapping
- `location_name`
  - child location/building/floor label beneath `site_name`
- `site_candidate`
  - raw source string used to derive `site`; useful for diagnostics
- `location_candidate`
  - raw source string used to derive `location`; useful for diagnostics
- `datacenter_candidate`
  - raw datacenter value when the source exposes a distinct datacenter field

Current contract expectations:

- Catalyst Center adapters emit `site_name` and `location_name`.
- XClarity node mappings may forward `site_candidate`,
  `location_candidate`, and `datacenter_candidate` into
  `resolve_placement` so fallback logs can name unresolved source inputs.

## Identity fields

Use stable identifiers before display names whenever the source exposes them:

- `serial`
  - preferred unique hardware identity for physical assets
- `name`
  - human-readable object name; may be non-unique
- parent-context identifiers
  - examples: `cluster`, `device`, `virtual_machine`

Guidance:

- Physical assets should prefer `serial` lookups before `name`.
- Nested objects should carry enough parent context to satisfy NetBox
  uniqueness constraints rather than reconstructing parent identity later in
  the engine.

## Enforcement

The test suite should assert that shipped example mappings consume these
contract fields explicitly. If an adapter changes field names, mapping tests
should fail until the documentation and examples are updated in the same
change.
