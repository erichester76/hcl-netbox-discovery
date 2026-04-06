"""Regression tests for interface child-write integrity in Engine._process_interfaces."""

from __future__ import annotations

from types import SimpleNamespace
import logging
from unittest.mock import MagicMock, call, patch

from collector.config import (
    CollectorOptions,
    DiskConfig,
    FieldConfig,
    InterfaceConfig,
    IpAddressConfig,
    ObjectConfig,
    TaggedVlanConfig,
)
from collector.context import RunContext
from collector.engine import Engine, RunStats
from collector.prerequisites import PrerequisiteRunner


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

    def test_primary_ip_is_temporarily_cleared_before_same_parent_reassignment(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="vm",
            source_collection="vms",
            netbox_resource="virtualization.virtual_machines",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                    ip_addresses=[
                        IpAddressConfig(
                            source_items="_ips",
                            primary_if="never",
                            fields=[FieldConfig(name="address", value="source('address')")],
                        )
                    ],
                )
            ],
        )
        ctx = _make_ctx(
            {"_interfaces": [{"name": "eth0", "_ips": [{"address": "10.0.0.1/24"}]}]}
        )
        parent_nb_obj = SimpleNamespace(id=99, primary_ip4=SimpleNamespace(id=500))
        ctx.nb.get.side_effect = [
            SimpleNamespace(id=500, assigned_object_id=7),
            SimpleNamespace(id=99, primary_ip4=SimpleNamespace(id=500)),
        ]

        with patch.object(
            engine,
            "_upsert",
            side_effect=[SimpleNamespace(id=8), SimpleNamespace(id=500)],
        ):
            engine._process_interfaces(obj_cfg, parent_nb_obj, ctx)

        assert ctx.nb.update.call_args_list == [
            call("virtualization.virtual_machines", 99, {"primary_ip4": None}),
            call("virtualization.virtual_machines", 99, {"primary_ip4": 500}),
        ]

    def test_primary_ip_is_restored_when_reassignment_upsert_fails(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="vm",
            source_collection="vms",
            netbox_resource="virtualization.virtual_machines",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                    ip_addresses=[
                        IpAddressConfig(
                            source_items="_ips",
                            primary_if="never",
                            fields=[FieldConfig(name="address", value="source('address')")],
                        )
                    ],
                )
            ],
        )
        ctx = _make_ctx(
            {"_interfaces": [{"name": "eth0", "_ips": [{"address": "10.0.0.1/24"}]}]}
        )
        parent_nb_obj = SimpleNamespace(id=99, primary_ip4=SimpleNamespace(id=500))
        ctx.nb.get.side_effect = [
            SimpleNamespace(id=500, assigned_object_id=7),
            SimpleNamespace(id=99, primary_ip4=SimpleNamespace(id=500)),
        ]

        with patch.object(
            engine,
            "_upsert",
            side_effect=[SimpleNamespace(id=8), None],
        ):
            engine._process_interfaces(obj_cfg, parent_nb_obj, ctx)

        assert ctx.nb.update.call_args_list == [
            call("virtualization.virtual_machines", 99, {"primary_ip4": None}),
            call("virtualization.virtual_machines", 99, {"primary_ip4": 500}),
        ]

    def test_primary_ip_guard_uses_fresh_parent_state_when_parent_obj_is_stale(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="vm",
            source_collection="vms",
            netbox_resource="virtualization.virtual_machines",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                    ip_addresses=[
                        IpAddressConfig(
                            source_items="_ips",
                            primary_if="never",
                            fields=[FieldConfig(name="address", value="source('address')")],
                        )
                    ],
                )
            ],
        )
        ctx = _make_ctx(
            {"_interfaces": [{"name": "eth0", "_ips": [{"address": "10.0.0.1/24"}]}]}
        )
        parent_nb_obj = SimpleNamespace(id=99, primary_ip4=None)
        ctx.nb.get.side_effect = [
            SimpleNamespace(id=500, assigned_object_id=7),
            SimpleNamespace(id=99, primary_ip4=SimpleNamespace(id=500)),
        ]

        with patch.object(
            engine,
            "_upsert",
            side_effect=[SimpleNamespace(id=8), SimpleNamespace(id=500)],
        ):
            engine._process_interfaces(obj_cfg, parent_nb_obj, ctx)

        assert ctx.nb.update.call_args_list == [
            call("virtualization.virtual_machines", 99, {"primary_ip4": None}),
            call("virtualization.virtual_machines", 99, {"primary_ip4": 500}),
        ]

    def test_guest_only_interface_skip_does_not_log_follow_on_warning(self, caplog):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="vm",
            source_collection="vms",
            netbox_resource="virtualization.virtual_machines",
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                    ip_addresses=[
                        IpAddressConfig(
                            source_items="_ips",
                            fields=[FieldConfig(name="address", value="source('address')")],
                        )
                    ],
                )
            ],
        )
        guest_iface = SimpleNamespace(
            _guest_only_vm_interface=True,
            _ips=[{"address": "10.0.0.1/24"}],
        )
        ctx = _make_ctx({"_interfaces": [guest_iface]})
        parent_nb_obj = SimpleNamespace(id=99)

        with patch.object(engine, "_build_payload", return_value={"virtual_machine": 99}), patch.object(
            engine,
            "_upsert",
            return_value=None,
        ):
            with caplog.at_level(logging.WARNING):
                engine._process_interfaces(obj_cfg, parent_nb_obj, ctx)

        assert "did not return an id" not in caplog.text

    def test_dry_run_existing_parent_preserves_device_identity_for_child_lookup(self):
        engine = Engine()
        source_adapter = MagicMock()
        source_adapter.get_objects.return_value = [
            {
                "name": "leaf-01",
                "_interfaces": [{"name": "mgmt0", "_ips": [], "_vlans": []}],
            }
        ]
        ctx = _make_ctx({}, dry_run=True)
        ctx.source_adapter = source_adapter
        stats = RunStats("device")
        obj_cfg = ObjectConfig(
            name="device",
            source_collection="devices",
            netbox_resource="dcim.devices",
            lookup_by=["name"],
            fields=[FieldConfig(name="name", value="source('name')")],
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                )
            ],
        )
        ctx.nb.get.side_effect = [
            {"id": 101, "name": "leaf-01"},
            None,
        ]

        engine._process_item(
            source_adapter.get_objects.return_value[0],
            obj_cfg,
            ctx.for_item(source_adapter.get_objects.return_value[0]),
            PrerequisiteRunner(ctx.nb),
            stats,
        )

        assert ctx.nb.get.call_args_list[1].args[0] == "dcim.interfaces"
        assert ctx.nb.get.call_args_list[1].kwargs == {"name": "mgmt0", "device": 101}

    def test_dry_run_created_vm_skips_child_gets_against_preview_parent(self):
        engine = Engine()
        ctx = _make_ctx(
            {
                "name": "vm-01",
                "_interfaces": [{"name": "eth0", "_ips": [], "_vlans": []}],
                "_disks": [{"name": "Hard disk 1"}],
            },
            dry_run=True,
        )
        stats = RunStats("vm")
        obj_cfg = ObjectConfig(
            name="vm",
            source_collection="vms",
            netbox_resource="virtualization.virtual_machines",
            lookup_by=["name"],
            fields=[FieldConfig(name="name", value="source('name')")],
            interfaces=[
                InterfaceConfig(
                    source_items="_interfaces",
                    fields=[FieldConfig(name="name", value="source('name')")],
                )
            ],
            disks=[
                DiskConfig(
                    source_items="_disks",
                    fields=[FieldConfig(name="name", value="source('name')")],
                )
            ],
        )
        ctx.nb.get.return_value = None

        engine._process_item(
            ctx.source_obj,
            obj_cfg,
            ctx.for_item(ctx.source_obj),
            PrerequisiteRunner(ctx.nb),
            stats,
        )

        assert len(ctx.nb.get.call_args_list) == 1
        assert ctx.nb.get.call_args_list[0].args[0] == "virtualization.virtual_machines"
        assert ctx.nb.get.call_args_list[0].kwargs == {"name": "vm-01"}

    def test_dry_run_top_level_none_stops_nested_processing(self):
        engine = Engine()
        obj_cfg = ObjectConfig(
            name="device",
            source_collection="devices",
            netbox_resource="dcim.devices",
            lookup_by=["name"],
            fields=[FieldConfig(name="name", value="source('name')")],
            interfaces=_make_obj_cfg().interfaces,
        )
        item = {
            "name": "leaf-01",
            "_interfaces": [{"name": "mgmt0", "_ips": [], "_vlans": []}],
        }
        ctx = _make_ctx(item, dry_run=True)
        stats = RunStats("device")

        with patch.object(engine, "_upsert", return_value=None) as mock_upsert, patch.object(
            engine, "_process_interfaces"
        ) as mock_process_interfaces, patch.object(
            engine, "_process_inventory_items"
        ) as mock_process_inventory_items, patch.object(
            engine, "_process_disks"
        ) as mock_process_disks, patch.object(
            engine, "_process_modules"
        ) as mock_process_modules:
            engine._process_item(
                item,
                obj_cfg,
                ctx.for_item(item),
                PrerequisiteRunner(ctx.nb),
                stats,
            )

        assert mock_upsert.call_count == 1
        mock_process_interfaces.assert_not_called()
        mock_process_inventory_items.assert_not_called()
        mock_process_disks.assert_not_called()
        mock_process_modules.assert_not_called()
