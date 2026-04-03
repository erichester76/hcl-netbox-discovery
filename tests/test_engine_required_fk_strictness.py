"""Regression tests for strict handling of required foreign-key fields."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from collector.config import CollectorOptions, FieldConfig, ObjectConfig
from collector.context import RunContext
from collector.engine import Engine, RunStats
from collector.field_resolvers import Resolver
from collector.prerequisites import PrerequisiteRunner


def _make_ctx(source_obj: dict) -> RunContext:
    opts = CollectorOptions(
        max_workers=1,
        dry_run=False,
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
        dry_run=False,
    )


def test_eval_field_raises_when_required_fk_lookup_is_partial():
    engine = Engine()
    ctx = _make_ctx({"site_name": "DC1", "region_name": None})
    resolver = Resolver(ctx)
    field_cfg = FieldConfig(
        name="site",
        type="fk",
        resource="dcim.sites",
        lookup={
            "name": "source('site_name')",
            "region": "source('region_name')",
        },
    )

    with pytest.raises(ValueError, match="missing lookup values"):
        engine._eval_field(field_cfg, resolver, ctx, strict=True)

    ctx.nb.get.assert_not_called()
    ctx.nb.upsert.assert_not_called()


def test_process_item_records_error_when_required_fk_lookup_is_partial():
    engine = Engine()
    ctx = _make_ctx({"hostname": "leaf-01", "site_name": "DC1", "region_name": None})
    stats = RunStats("devices")
    obj_cfg = ObjectConfig(
        name="device",
        source_collection="devices",
        netbox_resource="dcim.devices",
        lookup_by=["site"],
        fields=[
            FieldConfig(
                name="site",
                type="fk",
                resource="dcim.sites",
                lookup={
                    "name": "source('site_name')",
                    "region": "source('region_name')",
                },
            ),
            FieldConfig(name="name", value="source('hostname')"),
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
    ctx.nb.get.assert_not_called()
    ctx.nb.upsert.assert_not_called()
    ctx.nb.upsert_with_outcome.assert_not_called()
    mock_interfaces.assert_not_called()
    mock_inventory.assert_not_called()
    mock_disks.assert_not_called()
    mock_modules.assert_not_called()
