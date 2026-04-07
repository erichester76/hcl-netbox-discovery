"""Regression tests for lookup-field validation before Engine upserts."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from collector.config import CollectorOptions, FieldConfig, ObjectConfig, PrerequisiteConfig
from collector.context import RunContext
from collector.engine import Engine, RunStats
from collector.prerequisites import PrerequisiteRunner


def _make_ctx(source_obj: dict, *, dry_run: bool = False) -> RunContext:
    opts = CollectorOptions(
        max_workers=1,
        dry_run=dry_run,
        sync_tag="",
        regex_dir="/tmp/regex",
    )
    return RunContext(
        nb=MagicMock(),
        source_adapter=None,
        collector_opts=opts,
        regex_dir="/tmp/regex",
        prereqs={},
        source_obj=source_obj,
        parent_nb_obj=None,
        dry_run=dry_run,
    )


def test_process_item_records_error_when_lookup_identity_is_missing():
    engine = Engine()
    ctx = _make_ctx({"hostname": None, "role": "leaf"})
    stats = RunStats("devices")
    obj_cfg = ObjectConfig(
        name="device",
        source_collection="devices",
        netbox_resource="dcim.devices",
        lookup_by=["name"],
        fields=[
            FieldConfig(name="name", value="source('hostname')"),
            FieldConfig(name="role", value="source('role')"),
        ],
    )

    with patch.object(engine, "_process_interfaces") as mock_interfaces, patch.object(
        engine, "_process_inventory_items"
    ) as mock_inventory, patch.object(engine, "_process_disks") as mock_disks, patch.object(
        engine, "_process_modules"
    ) as mock_modules:
        engine._process_item(
            ctx.source_obj,
            obj_cfg,
            ctx,
            PrerequisiteRunner(ctx.nb),
            stats,
        )

    assert stats.processed == 1
    assert stats.errored == 1
    ctx.nb.upsert.assert_not_called()
    ctx.nb.upsert_with_outcome.assert_not_called()
    mock_interfaces.assert_not_called()
    mock_inventory.assert_not_called()
    mock_disks.assert_not_called()
    mock_modules.assert_not_called()


def test_dry_run_prereq_backed_lookup_does_not_fail_required_fk_validation():
    engine = Engine()
    ctx = _make_ctx(
        {"hostname": "leaf-01", "vendor": "Cisco", "model": "N9K-C93180YC-EX"},
        dry_run=True,
    )
    stats = RunStats("devices")
    obj_cfg = ObjectConfig(
        name="device",
        source_collection="devices",
        netbox_resource="dcim.devices",
        lookup_by=["name"],
        prerequisites=[
            PrerequisiteConfig(
                name="manufacturer",
                method="ensure_manufacturer",
                args={"name": "source('vendor')"},
            ),
        ],
        fields=[
            FieldConfig(name="name", value="source('hostname')"),
            FieldConfig(
                name="device_type",
                type="fk",
                resource="dcim.device_types",
                lookup={
                    "model": "source('model')",
                    "manufacturer": "prereq('manufacturer')",
                },
            ),
        ],
    )

    with patch.object(engine, "_process_interfaces") as mock_interfaces, patch.object(
        engine, "_process_inventory_items"
    ) as mock_inventory, patch.object(engine, "_process_disks") as mock_disks, patch.object(
        engine, "_process_modules"
    ) as mock_modules:
        engine._process_item(
            ctx.source_obj,
            obj_cfg,
            ctx,
            PrerequisiteRunner(ctx.nb),
            stats,
        )

    assert stats.processed == 1
    assert stats.errored == 0
    assert isinstance(ctx.prereqs["manufacturer"], int)
    assert ctx.prereqs["manufacturer"] < 0
    ctx.nb.upsert.assert_not_called()
    ctx.nb.upsert_with_outcome.assert_not_called()
    mock_interfaces.assert_called_once()
    mock_inventory.assert_called_once()
    mock_disks.assert_called_once()
    mock_modules.assert_called_once()
