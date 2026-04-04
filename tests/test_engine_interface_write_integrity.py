"""Regression tests for interface child-write integrity in Engine._process_interfaces."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from collector.config import (
    CollectorOptions,
    FieldConfig,
    InterfaceConfig,
    IpAddressConfig,
    ObjectConfig,
    TaggedVlanConfig,
)
from collector.context import RunContext
from collector.engine import Engine


def _make_ctx(source_obj: dict, dry_run: bool = False) -> RunContext:
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


def _make_obj_cfg() -> ObjectConfig:
    return ObjectConfig(
        name="device",
        source_collection="devices",
        netbox_resource="dcim.devices",
        interfaces=[
            InterfaceConfig(
                source_items="_interfaces",
                fields=[
                    FieldConfig(name="name", value="source('name')"),
                ],
                ip_addresses=[
                    IpAddressConfig(
                        source_items="_ips",
                        primary_if="first",
                        oob_if="first",
                        fields=[
                            FieldConfig(name="address", value="source('address')"),
                        ],
                    )
                ],
                tagged_vlans=[
                    TaggedVlanConfig(
                        source_items="_vlans",
                        fields=[
                            FieldConfig(name="vid", value="source('id')"),
                            FieldConfig(name="name", value="source('name')"),
                        ],
                    )
                ],
            )
        ],
    )


class TestInterfaceWriteIntegrity:
    def test_failed_interface_upsert_skips_child_ip_and_vlan_work(self):
        engine = Engine()
        obj_cfg = _make_obj_cfg()
        ctx = _make_ctx(
            {
                "_interfaces": [
                    {
                        "name": "mgmt0",
                        "_ips": [{"address": "10.0.0.1/24"}],
                        "_vlans": [{"id": 10, "name": "VLAN10"}],
                    }
                ]
            }
        )
        parent_nb_obj = SimpleNamespace(id=99)

        with patch.object(engine, "_upsert", side_effect=[None]) as mock_upsert, patch.object(
            engine, "_process_tagged_vlans"
        ) as mock_process_tagged_vlans:
            engine._process_interfaces(obj_cfg, parent_nb_obj, ctx)

        assert mock_upsert.call_count == 1
        resource, payload = mock_upsert.call_args.args[1:3]
        assert resource == "dcim.interfaces"
        assert payload["name"] == "mgmt0"
        ctx.nb.update.assert_not_called()
        mock_process_tagged_vlans.assert_not_called()

    def test_dry_run_still_traverses_nested_ip_blocks(self):
        engine = Engine()
        obj_cfg = _make_obj_cfg()
        ctx = _make_ctx(
            {
                "_interfaces": [
                    {
                        "name": "mgmt0",
                        "_ips": [{"address": "10.0.0.1/24"}],
                        "_vlans": [],
                    }
                ]
            },
            dry_run=True,
        )
        parent_nb_obj = SimpleNamespace(id=99)

        with patch.object(engine, "_upsert", side_effect=[None, None]) as mock_upsert:
            engine._process_interfaces(obj_cfg, parent_nb_obj, ctx)

        assert mock_upsert.call_count == 2
        assert mock_upsert.call_args_list[0].args[1] == "dcim.interfaces"
        assert mock_upsert.call_args_list[1].args[1] == "ipam.ip_addresses"
