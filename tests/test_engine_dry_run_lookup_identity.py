"""Regression coverage for dry-run lookup identity preservation."""

from __future__ import annotations

from unittest.mock import MagicMock

from collector.config import CollectorOptions, FieldConfig, ObjectConfig, PrerequisiteConfig
from collector.context import RunContext
from collector.engine import Engine, RunStats
from collector.field_resolvers import Resolver
from collector.prerequisites import PrerequisiteRunner


def _make_ctx(source_obj: dict, *, dry_run: bool = True) -> RunContext:
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


def test_dry_run_required_fk_field_returns_preview_reference():
    engine = Engine()
    ctx = _make_ctx({"site_name": "DC1"})
    resolver = Resolver(ctx)
    field_cfg = FieldConfig(
        name="site",
        type="fk",
        resource="dcim.sites",
        lookup={"name": "source('site_name')"},
    )

    value = engine._eval_field(field_cfg, resolver, ctx, strict=True)

    assert isinstance(value, dict)
    assert value["_dry_run_resource"] == "dcim.sites"
    assert value["name"] == "DC1"
    assert value["id"] < 0
    ctx.nb.get.assert_not_called()
    ctx.nb.upsert.assert_not_called()


def test_dry_run_prerequisite_backed_lookup_field_is_not_skipped():
    engine = Engine()
    ctx = _make_ctx({"hostname": "leaf-01", "site_name": "DC1"})
    stats = RunStats("devices")
    obj_cfg = ObjectConfig(
        name="device",
        source_collection="devices",
        netbox_resource="dcim.devices",
        lookup_by=["site", "name"],
        prerequisites=[
            PrerequisiteConfig(
                name="site",
                method="ensure_site",
                args={"name": "source('site_name')"},
            )
        ],
        fields=[
            FieldConfig(name="site", value="prereq('site')"),
            FieldConfig(name="name", value="source('hostname')"),
        ],
    )

    engine._process_item(
        ctx.source_obj,
        obj_cfg,
        ctx,
        PrerequisiteRunner(ctx.nb),
        stats,
    )

    assert stats.processed == 1
    assert stats.created == 1
    assert stats.errored == 0
    ctx.nb.get.assert_not_called()
    ctx.nb.upsert.assert_not_called()
    ctx.nb.upsert_with_outcome.assert_not_called()
