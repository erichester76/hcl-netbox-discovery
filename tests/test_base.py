"""Tests for the DataSource abstract base class."""

from __future__ import annotations

import pytest

from collector.sources.base import DataSource


# ---------------------------------------------------------------------------
# Concrete stub for testing
# ---------------------------------------------------------------------------


class _StubSource(DataSource):
    """Minimal concrete DataSource used only in these tests."""

    connected = False
    closed = False
    objects: dict = {}

    def connect(self, config):
        self.connected = True
        self._obj = {"a": {"b": "value"}}

    def get_objects(self, collection: str) -> list:
        return self.objects.get(collection, [])

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDataSourceInterface:
    def test_connect_sets_state(self):
        src = _StubSource()
        src.connect(None)
        assert src.connected is True

    def test_close_sets_state(self):
        src = _StubSource()
        src.connect(None)
        src.close()
        assert src.closed is True

    def test_get_objects_returns_empty_list_for_unknown_collection(self):
        src = _StubSource()
        src.connect(None)
        result = src.get_objects("nonexistent")
        assert result == []

    def test_get_objects_returns_configured_items(self):
        src = _StubSource()
        src.objects = {"devices": [{"name": "switch-01"}]}
        src.connect(None)
        assert src.get_objects("devices") == [{"name": "switch-01"}]

    def test_get_nested_returns_list(self):
        src = _StubSource()
        src.connect(None)
        # get_nested delegates to walk_path via base class
        result = src.get_nested({"items": [1, 2, 3]}, "items")
        assert result == [1, 2, 3]

    def test_get_nested_returns_empty_for_missing_path(self):
        src = _StubSource()
        src.connect(None)
        result = src.get_nested({"name": "x"}, "nonexistent")
        assert result == []

    def test_get_nested_wraps_scalar_in_list(self):
        src = _StubSource()
        src.connect(None)
        result = src.get_nested({"name": "switch-01"}, "name")
        assert result == ["switch-01"]

    def test_cannot_instantiate_abstract_class(self):
        with pytest.raises(TypeError):
            DataSource()  # type: ignore[abstract]
