"""Regression tests for required prerequisite fail-loud behavior."""

from __future__ import annotations

from unittest.mock import MagicMock

from collector.config import CollectorOptions, FieldConfig, ObjectConfig, PrerequisiteConfig
from collector.context import RunContext
from collector.engine import Engine, RunStats
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


def test_required_prerequisite_failure_skips_item_without_writing():
    engine = Engine()
    ctx = _make_ctx({"hostname": "leaf-01"})
    stats = RunStats("devices")
    obj_cfg = ObjectConfig(
        name="device",
        source_collection="devices",
        netbox_resource="dcim.devices",
        prerequisites=[
            PrerequisiteConfig(
                name="site",
                method="ensure_site",
                args={"name": "source('missing_site_name')"},
            )
        ],
        fields=[FieldConfig(name="name", value="source('hostname')")],
    )

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
