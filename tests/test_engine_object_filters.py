from collector.config import CollectorOptions, FieldConfig, ObjectConfig
from collector.context import RunContext
from collector.engine import Engine


class _FakeSource:
    def __init__(self, items):
        self._items = items

    def get_objects(self, collection):
        assert collection == "clusters"
        return list(self._items)


class _FakeNB:
    def upsert(self, resource, payload, lookup_fields):
        return {"id": 1, **payload}


def test_object_enabled_if_filters_items_before_processing(tmp_path):
    items = [
        {"name": "Staging - POD"},
        {"name": "Poole-Core"},
    ]
    ctx = RunContext(
        nb=_FakeNB(),
        source_adapter=_FakeSource(items),
        collector_opts=CollectorOptions(max_workers=1),
        regex_dir=str(tmp_path),
        prereqs={},
        source_obj=None,
        parent_nb_obj=None,
        dry_run=False,
    )
    obj_cfg = ObjectConfig(
        name="cluster",
        source_collection="clusters",
        netbox_resource="virtualization.clusters",
        lookup_by=["name"],
        enabled_if="not source('name').startswith('Staging')",
        fields=[FieldConfig(name="name", value="source('name')")],
    )

    stats = Engine()._process_object(obj_cfg, ctx)

    assert stats.processed == 1
    assert stats.created + stats.updated + stats.skipped + stats.errored == 1
