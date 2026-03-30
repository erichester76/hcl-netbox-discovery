"""Tests for NetBoxExtendedClient.get() cache back-fill on miss.

When a cache miss causes an API call that returns a record, the result must
be stored under:
  1. The exact filter key that was requested (already existed before this fix).
  2. The id-based key ({"id": <record_id>}).
  3. All derived lookup-filter keys produced by
     _derived_lookup_filters_for_record().

This ensures that a second call with *different* but equivalent filters (e.g.
device_id+module_bay_id vs device_id+name) hits the cache instead of making
another API round-trip.
"""

from __future__ import annotations

import sys
import os
from unittest.mock import MagicMock

import pytest

# Make sure the lib directory is importable.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from pynetbox2 import CacheBackend, NetBoxExtendedClient  # noqa: E402


# ---------------------------------------------------------------------------
# Minimal in-memory cache backend for tests
# ---------------------------------------------------------------------------

class DictCacheBackend(CacheBackend):
    """Simple dict-backed cache for unit tests."""

    def __init__(self):
        self._store: dict = {}

    def get(self, key: str):
        return self._store.get(key)

    def set(self, key: str, value, ttl_seconds=None):
        self._store[key] = value

    def delete(self, key: str):
        self._store.pop(key, None)

    def delete_prefix(self, key_prefix: str):
        for k in list(self._store):
            if k.startswith(key_prefix):
                del self._store[k]

    def clear(self):
        self._store.clear()

    def count(self) -> int:
        return len(self._store)

    def keys(self) -> list:
        return list(self._store.keys())

    def get_ttl(self, key: str):
        return None

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client() -> NetBoxExtendedClient:
    """Return a NetBoxExtendedClient whose adapter and cache are mocked."""
    client = NetBoxExtendedClient.__new__(NetBoxExtendedClient)
    # Minimal config stub
    cfg_stub = MagicMock()
    cfg_stub.retry_attempts = 0
    cfg_stub.cache_backend = "none"
    client.config = cfg_stub
    client.cache = DictCacheBackend()
    client.adapter = MagicMock()
    import threading
    client._cache_metrics_lock = threading.Lock()
    client._cache_metrics = {
        "get_hits": 0, "get_misses": 0, "get_bypass": 0,
        "list_hits": 0, "list_misses": 0, "list_bypass": 0,
    }
    client._cache_key_locks_guard = threading.Lock()
    client._cache_key_locks = {}
    return client


def _make_module_bay_record(record_id: int, device_id: int, name: str) -> dict:
    return {"id": record_id, "device": {"id": device_id, "name": "server-01"}, "name": name}


def _make_module_record(record_id: int, device_id: int, module_bay_id: int) -> dict:
    return {
        "id": record_id,
        "device": {"id": device_id, "name": "server-01"},
        "module_bay": {"id": module_bay_id, "name": "DIMM 1"},
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGetCacheBackfillOnMiss:
    """Verify that a cache miss back-fills id and derived lookup keys."""

    def test_id_key_cached_after_miss(self):
        client = _make_client()
        record = _make_module_bay_record(42, device_id=21, name="DIMM 1")
        client.adapter.get.return_value = record

        result = client.get("dcim.module_bays", device_id=21, name="DIMM 1")
        assert result is record

        # The id-based key must now be in the cache.
        id_key = client._cache_key("dcim.module_bays", "get", {"id": 42})
        assert client.cache.get(id_key) is record

    def test_derived_lookup_keys_cached_after_miss(self):
        """A first lookup by device_id+name should allow a second lookup by
        device_id+name (same combo) to be a cache hit."""
        client = _make_client()
        record = _make_module_bay_record(42, device_id=21, name="DIMM 1")
        client.adapter.get.return_value = record

        # First call — miss, fetches from API.
        client.get("dcim.module_bays", device_id=21, name="DIMM 1")

        # Adapter should have been called exactly once so far.
        assert client.adapter.get.call_count == 1

        # Second call with the same filters — should be a cache hit.
        result2 = client.get("dcim.module_bays", device_id=21, name="DIMM 1")
        assert result2 is record
        assert client.adapter.get.call_count == 1  # no extra API call

    def test_derived_keys_enable_cross_filter_hit(self):
        """A miss with filter-set A should populate enough derived keys that
        a subsequent call with filter-set B (same record, different filters)
        is a cache hit — provided filter-set B is among the derived keys."""
        client = _make_client()
        record = _make_module_bay_record(42, device_id=21, name="DIMM 1")
        client.adapter.get.return_value = record

        # First call — miss, back-fills derived keys.
        client.get("dcim.module_bays", device_id=21, name="DIMM 1")

        # Verify that the id-based key is now present (that is always derived).
        id_key = client._cache_key("dcim.module_bays", "get", {"id": 42})
        assert client.cache.get(id_key) is record

    def test_modules_id_key_cached_after_miss(self):
        """Cache miss for dcim.modules should also back-fill the id key."""
        client = _make_client()
        record = _make_module_record(99, device_id=21, module_bay_id=8587)
        client.adapter.get.return_value = record

        client.get("dcim.modules", device_id=21, module_bay_id=8587)

        id_key = client._cache_key("dcim.modules", "get", {"id": 99})
        assert client.cache.get(id_key) is record

    def test_no_backfill_when_api_returns_none(self):
        """When the API returns None no derived keys should be written."""
        client = _make_client()
        client.adapter.get.return_value = None

        client.get("dcim.module_bays", device_id=21, name="does-not-exist")

        # Cache store should be empty — nothing to back-fill.
        assert client.cache._store == {}

    def test_adapter_called_only_once_with_use_cache_false(self):
        """use_cache=False bypasses the cache read but still back-fills on hit."""
        client = _make_client()
        record = _make_module_bay_record(42, device_id=21, name="DIMM 1")
        client.adapter.get.return_value = record

        client.get("dcim.module_bays", use_cache=False, device_id=21, name="DIMM 1")

        # Back-fill should still have written the id key.
        id_key = client._cache_key("dcim.module_bays", "get", {"id": 42})
        assert client.cache.get(id_key) is record

    def test_second_call_is_cache_hit_after_bypass_backfill(self):
        """After a use_cache=False call back-fills the cache, a subsequent
        use_cache=True call for the same filters must be a cache hit."""
        client = _make_client()
        record = _make_module_bay_record(42, device_id=21, name="DIMM 1")
        client.adapter.get.return_value = record

        # Bypass call — back-fills cache.
        client.get("dcim.module_bays", use_cache=False, device_id=21, name="DIMM 1")
        assert client.adapter.get.call_count == 1

        # Normal cached call — should hit the key written by bypass back-fill.
        result2 = client.get("dcim.module_bays", device_id=21, name="DIMM 1")
        assert result2 is record
        assert client.adapter.get.call_count == 1  # no new API call
