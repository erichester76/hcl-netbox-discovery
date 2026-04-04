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

        assert result["name"] == "r1"
        assert result["id"] < 0
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

        assert result == {"id": 22, "name": "r1", "role": 1}
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

        assert result == {"id": 33, "name": "r1"}
        assert stats.processed == 1
        assert stats.created == 0
        assert stats.updated == 0
        assert stats.skipped == 1
        assert stats.errored == 0
        nb.get.assert_called_once_with("dcim.devices", name="r1")
        nb.upsert.assert_not_called()
        nb.upsert_with_outcome.assert_not_called()

    def test_dry_run_tags_with_same_name_do_not_report_update(self):
        engine = Engine()
        stats = RunStats("clusters")
        nb = MagicMock()
        existing_tag = SimpleNamespace(id=1, name="vmware-sync")
        nb.get.return_value = {"id": 88, "name": "cluster-a", "tags": [existing_tag]}

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "virtualization.clusters",
            {"name": "cluster-a", "tags": [{"name": "vmware-sync"}]},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result["id"] == 88
        assert stats.updated == 0
        assert stats.skipped == 1

    def test_dry_run_status_choice_object_matches_string(self):
        engine = Engine()
        stats = RunStats("devices")
        nb = MagicMock()
        existing_status = SimpleNamespace(value="active", label="Active")
        nb.get.return_value = {"id": 99, "name": "leaf-01", "status": existing_status}

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "dcim.devices",
            {"name": "leaf-01", "status": "active"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result["id"] == 99
        assert stats.updated == 0
        assert stats.skipped == 1

    def test_dry_run_interface_type_choice_object_matches_slug(self):
        engine = Engine()
        stats = RunStats("interfaces")
        nb = MagicMock()
        existing_type = SimpleNamespace(value="40gbase-x-qsfpp", label="QSFP+ (40GE)")
        nb.get.return_value = {"id": 77, "name": "vmnic0", "type": existing_type}

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "dcim.interfaces",
            {"name": "vmnic0", "type": "40gbase-x-qsfpp"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result["id"] == 77
        assert stats.updated == 0
        assert stats.skipped == 1

    def test_missing_nested_lookup_records_nested_skip_without_item_error(self):
        engine = Engine()
        stats = RunStats("vms")
        nb = MagicMock()

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "virtualization.interfaces",
            {"name": "nic0"},
            lookup_fields=["name", "virtual_machine"],
            nested_stats=stats,
        )

        assert result is None
        assert stats.processed == 0
        assert stats.errored == 0
        assert stats.nested_skipped == {
            "virtualization.interfaces:virtual_machine": 1
        }

    def test_dry_run_preview_parent_lookup_skips_remote_get(self):
        engine = Engine()
        stats = RunStats("vms")
        nb = MagicMock()
        nb.get.side_effect = AssertionError("preview-parent child lookup should not call NetBox")

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "virtualization.interfaces",
            {"name": "eth0", "virtual_machine": -7},
            lookup_fields=["name", "virtual_machine"],
            stats=stats,
        )

        assert result["id"] < 0
        assert stats.created == 1
        nb.get.assert_not_called()

    def test_dry_run_ignores_preview_relation_fields_in_diff(self):
        engine = Engine()
        stats = RunStats("ip-addresses")
        nb = MagicMock()
        nb.get.return_value = {
            "id": 55,
            "address": "10.0.0.1/24",
            "assigned_object_id": 58,
            "assigned_object_type": "dcim.interface",
        }

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "ipam.ip_addresses",
            {
                "address": "10.0.0.1/24",
                "assigned_object_id": -1,
                "assigned_object_type": "dcim.interface",
            },
            lookup_fields=["address"],
            stats=stats,
        )

        assert result["id"] == 55
        assert stats.updated == 0
        assert stats.skipped == 1

    def test_dry_run_create_preview_does_not_call_extract_id_helper_with_none(self):
        engine = Engine()

        class NBClient:
            def get(self, *args, **kwargs):
                return None

            @staticmethod
            def _extract_id(value):
                if value is None:
                    raise AssertionError("_extract_id helper must not be called with None")
                return getattr(value, "id", None)

        stats = RunStats("devices")

        result = engine._upsert(
            _ctx(nb=NBClient(), dry_run=True),
            "dcim.devices",
            {"name": "r1"},
            lookup_fields=["name"],
            stats=stats,
        )

        assert result["id"] < 0
        assert result["name"] == "r1"

    def test_log_summary_sorts_nested_skip_keys(self, caplog):
        stats = RunStats("vms")
        stats.nested_skipped["zeta"] = 1
        stats.nested_skipped["alpha"] = 2

        with caplog.at_level(logging.INFO):
            stats.log_summary()

        assert "nested_skipped={'alpha': 2, 'zeta': 1}" in caplog.text
