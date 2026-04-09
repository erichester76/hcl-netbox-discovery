"""Process-local keyed locks for serializing lookup-based NetBox writes."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any

_locks: dict[tuple[Any, ...], threading.RLock] = {}
_lock_refcounts: dict[tuple[Any, ...], int] = {}
_locks_guard = threading.Lock()


def _freeze(value: Any) -> Any:
    """Convert nested identity values into hashable tuples."""
    if isinstance(value, dict):
        return tuple((key, _freeze(val)) for key, val in sorted(value.items()))
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _extract_stable_id(value: Any) -> Any:
    """Normalize object/dict/int references to a stable comparable identity."""
    if isinstance(value, dict):
        return value.get("id")
    if hasattr(value, "id"):
        return value.id
    return value


def build_hotspot_upsert_lock_key(
    resource: str,
    payload: dict[str, Any],
    lookup_fields: list[str],
) -> tuple[Any, ...] | None:
    """Return a keyed lock identity for any lookup-based upsert."""
    if not lookup_fields:
        return None
    identity: list[tuple[str, Any]] = []
    for field in lookup_fields:
        if field not in payload:
            return None
        identity.append((field, _freeze(payload[field])))
    return ("upsert", resource, tuple(identity))


def build_vlan_lock_key(vlan_payload: dict[str, Any]) -> tuple[Any, ...] | None:
    """Return a keyed lock identity for multi-site VLAN resolution."""
    vid = vlan_payload.get("vid")
    if vid is None:
        return None
    return (
        "vlan",
        _freeze(vid),
        _freeze(_extract_stable_id(vlan_payload.get("site"))),
        _freeze(_extract_stable_id(vlan_payload.get("group"))),
    )


@contextmanager
def keyed_lock(lock_key: tuple[Any, ...] | None):
    """Serialize callers that share *lock_key*; no-op when key is ``None``."""
    if lock_key is None:
        yield
        return

    with _locks_guard:
        lock = _locks.get(lock_key)
        if lock is None:
            lock = threading.RLock()
            _locks[lock_key] = lock
        _lock_refcounts[lock_key] = _lock_refcounts.get(lock_key, 0) + 1

    try:
        with lock:
            yield
    finally:
        with _locks_guard:
            remaining = _lock_refcounts[lock_key] - 1
            if remaining <= 0:
                _lock_refcounts.pop(lock_key, None)
                _locks.pop(lock_key, None)
            else:
                _lock_refcounts[lock_key] = remaining
