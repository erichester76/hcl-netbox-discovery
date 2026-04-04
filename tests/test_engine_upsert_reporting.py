"""Regression tests for truthful upsert reporting in Engine._upsert.

These tests exercise the production _upsert() method and RunStats counters
to ensure create/update/no-op outcomes are reported distinctly.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

from collector.engine import Engine, RunStats


def _ctx(*, nb, dry_run: bool = False):
    return SimpleNamespace(nb=nb, dry_run=dry_run)


class TestEngineUpsertReporting:
    def test_created_outcome_increments_created(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        nb.upsert_with_outcome.return_value = SimpleNamespace(
            object={"id": 11},
            outcome="created",
        )

        result = engine._upsert(
            _ctx(nb=nb),
            "dcim.devices",
            {"name": "r1"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result == {"id": 11}
        assert stats.processed == 1
        assert stats.created == 1
        assert stats.updated == 0
        assert stats.skipped == 0
        assert stats.errored == 0

    def test_updated_outcome_increments_updated(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        nb.upsert_with_outcome.return_value = SimpleNamespace(
            object={"id": 22},
            outcome="updated",
        )

        result = engine._upsert(
            _ctx(nb=nb),
            "dcim.devices",
            {"name": "r1"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result == {"id": 22}
        assert stats.processed == 1
        assert stats.created == 0
        assert stats.updated == 1
        assert stats.skipped == 0
        assert stats.errored == 0

    def test_noop_outcome_increments_skipped(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        existing = {"id": 33, "name": "r1"}
        nb.upsert_with_outcome.return_value = SimpleNamespace(
            object=existing,
            outcome="noop",
        )

        result = engine._upsert(
            _ctx(nb=nb),
            "dcim.devices",
            {"name": "r1"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result == existing
        assert stats.processed == 1
        assert stats.created == 0
        assert stats.updated == 0
        assert stats.skipped == 1
        assert stats.errored == 0

    def test_fallback_upsert_without_outcome_counts_created(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        del nb.upsert_with_outcome
        nb.upsert.return_value = {"id": 44}

        result = engine._upsert(
            _ctx(nb=nb),
            "dcim.devices",
            {"name": "r2"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result == {"id": 44}
        assert stats.processed == 1
        assert stats.created == 1
        assert stats.updated == 0
        assert stats.skipped == 0
        assert stats.errored == 0

    def test_missing_lookup_field_records_error_without_writing(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()

        result = engine._upsert(
            _ctx(nb=nb),
            "dcim.devices",
            {"role": 7},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result is None
        assert stats.processed == 1
        assert stats.created == 0
        assert stats.updated == 0
        assert stats.skipped == 0
        assert stats.errored == 1
        nb.upsert.assert_not_called()
        nb.upsert_with_outcome.assert_not_called()

    def test_blank_lookup_field_is_treated_as_missing(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()

        result = engine._upsert(
            _ctx(nb=nb),
            "dcim.devices",
            {"name": "   "},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result is None
        assert stats.processed == 1
        assert stats.errored == 1
        nb.upsert.assert_not_called()
        nb.upsert_with_outcome.assert_not_called()

    def test_dry_run_created_outcome_counts_created(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        nb.get.return_value = None

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "dcim.devices",
            {"name": "r1"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result is None
        assert stats.processed == 1
        assert stats.created == 1
        assert stats.updated == 0
        assert stats.skipped == 0
        assert stats.errored == 0
        nb.get.assert_called_once_with("dcim.devices", name="r1")
        nb.upsert.assert_not_called()
        nb.upsert_with_outcome.assert_not_called()

    def test_dry_run_updated_outcome_counts_updated(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        nb.get.return_value = {"id": 22, "name": "r1", "role": 1}

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "dcim.devices",
            {"name": "r1", "role": 2},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result is None
        assert stats.processed == 1
        assert stats.created == 0
        assert stats.updated == 1
        assert stats.skipped == 0
        assert stats.errored == 0
        nb.get.assert_called_once_with("dcim.devices", name="r1")
        nb.upsert.assert_not_called()
        nb.upsert_with_outcome.assert_not_called()

    def test_dry_run_updated_outcome_logs_diff_at_debug(self, caplog):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        nb.get.return_value = {"id": 22, "name": "r1", "role": 1}

        with caplog.at_level(logging.DEBUG):
            engine._upsert(
                _ctx(nb=nb, dry_run=True),
                "dcim.devices",
                {"name": "r1", "role": 2},
                lookup_fields=["name"],
                stats=stats,
            )

        assert "resource=dcim.devices" in caplog.text
        assert "diff=" in caplog.text
        assert "role" in caplog.text

    def test_dry_run_noop_outcome_counts_skipped(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        nb.get.return_value = {"id": 33, "name": "r1"}

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "dcim.devices",
            {"name": "r1"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result is None
        assert stats.processed == 1
        assert stats.created == 0
        assert stats.updated == 0
        assert stats.skipped == 1
        assert stats.errored == 0
        nb.get.assert_called_once_with("dcim.devices", name="r1")
        nb.upsert.assert_not_called()
        nb.upsert_with_outcome.assert_not_called()
