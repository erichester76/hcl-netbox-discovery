from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from collector.engine import Engine, RunStats


def _ctx(*, nb, dry_run: bool = False, source_obj=None):
    return SimpleNamespace(nb=nb, dry_run=dry_run, source_obj=source_obj)


class TestVMwareVirtualDiskDescriptionNormalization:
    def test_live_upsert_normalizes_snapshot_suffix_before_write(self):
        engine = Engine()
        stats = RunStats("virtual_disks")
        nb = MagicMock()
        nb.upsert_with_outcome.return_value = SimpleNamespace(
            object={"id": 101},
            outcome="created",
        )
        payload = {
            "virtual_machine": 7,
            "name": "Hard disk 1",
            "description": "[CU-Core-VSAN] 2a317765-74e4-4749-6815-3868dd772bb0/4gk-swd-p-pol72-000001.vmdk (Thin Provisioned persistent)",
        }

        result = engine._upsert(
            _ctx(nb=nb),
            "virtualization.virtual_disks",
            payload,
            lookup_fields=["virtual_machine", "name"],
            stats=stats,
        )

        assert result == {"id": 101}
        nb.upsert_with_outcome.assert_called_once_with(
            "virtualization.virtual_disks",
            {
                "virtual_machine": 7,
                "name": "Hard disk 1",
                "description": "[CU-Core-VSAN] 2a317765-74e4-4749-6815-3868dd772bb0/4gk-swd-p-pol72.vmdk (Thin Provisioned persistent)",
            },
            lookup_fields=["virtual_machine", "name"],
        )
        assert stats.created == 1
        assert stats.updated == 0
        assert stats.skipped == 0

    def test_dry_run_treats_snapshot_suffix_as_noop_against_stable_description(self):
        engine = Engine()
        stats = RunStats("virtual_disks")
        nb = MagicMock()
        existing = {
            "id": 202,
            "virtual_machine": 7,
            "name": "Hard disk 1",
            "description": "[CU-Core-VSAN] 2a317765-74e4-4749-6815-3868dd772bb0/4gk-swd-p-pol72.vmdk (Thin Provisioned persistent)",
        }
        nb.get.return_value = existing
        payload = {
            "virtual_machine": 7,
            "name": "Hard disk 1",
            "description": "[CU-Core-VSAN] 2a317765-74e4-4749-6815-3868dd772bb0/4gk-swd-p-pol72-000001.vmdk (Thin Provisioned persistent)",
        }

        result = engine._upsert(
            _ctx(nb=nb, dry_run=True),
            "virtualization.virtual_disks",
            payload,
            lookup_fields=["virtual_machine", "name"],
            stats=stats,
        )

        assert result == existing
        assert stats.created == 0
        assert stats.updated == 0
        assert stats.skipped == 1
