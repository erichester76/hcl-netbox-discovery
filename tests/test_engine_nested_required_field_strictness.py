"""Regression tests for strict nested identity-field evaluation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from collector.config import (
    CollectorOptions,
    DiskConfig,
    FieldConfig,
    InterfaceConfig,
    InventoryItemConfig,
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


class TestNestedRequiredFieldStrictness:
    def test_invalid_interface_name_expression_skips_interface_write(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="device",
            source_collection="devices",
            netbox_resource="dcim.devices",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="undefined_func()")],
                )
            ],
        )
        ctx = _make_ctx({"_interfaces": [{"raw_name": "mgmt0"}]})

        with patch.object(engine, "_upsert") as mock_upsert:
            engine._process_interfaces(obj_cfg, SimpleNamespace(id=99), ctx)

        mock_upsert.assert_not_called()

    def test_invalid_ip_address_expression_skips_only_ip_write(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="device",
            source_collection="devices",
            netbox_resource="dcim.devices",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                    ip_addresses=[
                        IpAddressConfig(
                            source_items="_ips",
                            fields=[FieldConfig(name="address", value="undefined_func()")],
                        )
                    ],
                )
            ],
        )
        ctx = _make_ctx(
            {"_interfaces": [{"name": "mgmt0", "_ips": [{"cidr": "10.0.0.1/24"}]}]}
        )

        with patch.object(engine, "_upsert", side_effect=[SimpleNamespace(id=7)]) as mock_upsert:
            engine._process_interfaces(obj_cfg, SimpleNamespace(id=99), ctx)

        assert mock_upsert.call_count == 1
        assert mock_upsert.call_args_list[0].args[1] == "dcim.interfaces"

    def test_invalid_tagged_vlan_lookup_expression_skips_vlan_write(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="device",
            source_collection="devices",
            netbox_resource="dcim.devices",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                    tagged_vlans=[
                        TaggedVlanConfig(
                            source_items="_vlans",
                            lookup_by=["custom_id"],
                            fields=[
                                FieldConfig(name="custom_id", value="undefined_func()"),
                                FieldConfig(name="name", value="source('name')"),
                            ],
                        )
                    ],
                )
            ],
        )
        ctx = _make_ctx(
            {"_interfaces": [{"name": "mgmt0", "_vlans": [{"name": "blue"}]}]}
        )

        with patch.object(engine, "_upsert", side_effect=[SimpleNamespace(id=7)]) as mock_upsert:
            engine._process_interfaces(obj_cfg, SimpleNamespace(id=99), ctx)

        assert mock_upsert.call_count == 1
        ctx.nb.upsert.assert_not_called()
        ctx.nb.update.assert_not_called()

    def test_invalid_inventory_name_expression_skips_inventory_write(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="device",
            source_collection="devices",
            netbox_resource="dcim.devices",
            inventory_items=[
                InventoryItemConfig(
                    source_items="_inventory",
                    fields=[FieldConfig(name="name", value="undefined_func()")],
                )
            ],
        )
        ctx = _make_ctx({"_inventory": [{"name": "DIMM A1"}]})

        with patch.object(engine, "_upsert") as mock_upsert:
            engine._process_inventory_items(obj_cfg, SimpleNamespace(id=99), ctx)

        mock_upsert.assert_not_called()

    def test_invalid_disk_name_expression_skips_disk_write(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="vm",
            source_collection="vms",
            netbox_resource="virtualization.virtual_machines",
            disks=[
                DiskConfig(
                    source_items="_disks",
                    fields=[FieldConfig(name="name", value="undefined_func()")],
                )
            ],
        )
        ctx = _make_ctx({"_disks": [{"label": "Hard disk 1"}]})

        with patch.object(engine, "_upsert") as mock_upsert:
            engine._process_disks(obj_cfg, SimpleNamespace(id=99), ctx)

        mock_upsert.assert_not_called()
