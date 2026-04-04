"""Regression coverage for optional prerequisite noise suppression."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from collector.config import CollectorOptions, FieldConfig, ObjectConfig, PrerequisiteConfig
from collector.context import RunContext
from collector.engine import Engine, RunStats


def _make_obj_cfg() -> ObjectConfig:
    return ObjectConfig(
        name="cluster",
        source_collection="clusters",
        netbox_resource="virtualization.clusters",
        fields=[FieldConfig(name="name", value="'cluster'")],
        prerequisites=[PrerequisiteConfig(
            name="cluster_group",
            method="ensure_cluster_group",
            args={},
            optional=True,
        )],
    )


def _make_ctx() -> RunContext:
    opts = CollectorOptions(max_workers=1, dry_run=True, sync_tag="", regex_dir="/tmp/regex")
    return RunContext(
        nb=MagicMock(),
        source_adapter=None,
        collector_opts=opts,
        regex_dir="/tmp/regex",
        prereqs={},
        source_obj={"name": "cluster"},
        parent_nb_obj=None,
        dry_run=True,
    )


def test_optional_prereq_empty_text_silenced(caplog):
    engine = Engine()
    obj_cfg = _make_obj_cfg()
    ctx = _make_ctx()
    item = {"name": "cluster"}
    prereq_runner = MagicMock()
    prereq_runner.run.side_effect = ValueError("ensure_cluster_group requires a non-empty 'name'")
    stats = RunStats(obj_cfg.name)

    with patch.object(engine, "_upsert", return_value={"id": 1}):
        with caplog.at_level(logging.DEBUG):
            engine._process_item(item, obj_cfg, ctx.for_item(item), prereq_runner, stats)

    assert "Optional prereq" not in caplog.text
