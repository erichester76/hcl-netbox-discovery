"""Regression tests for the Nexus example mapping."""

from __future__ import annotations

from collector.config import load_config


def _device_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "device"), None)


def _shared_ip_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "shared_ip"), None)


def _shared_fhrp_group_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "shared_fhrp_group"), None)


def _shared_fhrp_vip_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "shared_fhrp_vip"), None)


def _shared_fhrp_assignment_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "shared_fhrp_assignment"), None)


def test_nexus_example_mapping_includes_interface_ip_sync(monkeypatch):
    monkeypatch.delenv("NDFC_FETCH_INTERFACES", raising=False)
    cfg = load_config("mappings/nexus.hcl.example")

    assert cfg.source.api_type == "nexus"
    assert cfg.source.extra.get("fetch_interfaces") == "false"

    device = _device_object(cfg)
    assert device is not None

    field_values = {field.name: field.value for field in device.fields}
    assert field_values["name"] == "when(source('name'), source('name'), 'Unknown')"
    assert "primary_ip4" not in field_values

    prereq_args = {prereq.name: prereq.args for prereq in device.prerequisites}
    assert prereq_args["device_type"]["model"] == "when(source('model'), source('model'), 'Unknown')"
    assert prereq_args["role"]["name"] == "when(source('role'), source('role'), 'Network Device')"
    assert (
        prereq_args["site"]["name"]
        == "when(source('site_name'), regex_file(source('site_name'), 'nexus_site_to_site'), 'Unknown')"
    )
    assert prereq_args["platform"]["name"] == "when(source('platform_name'), source('platform_name'), 'NX-OS')"

    assert device.interfaces, "device should define interfaces"
    interface = device.interfaces[0]
    interface_fields = {field.name: field.value for field in interface.fields}
    assert interface_fields["type"] == "when(source('type'), source('type'), 'other')"
    assert interface_fields["description"] == "when(source('description'), source('description'), '')"
    assert interface_fields["mgmt_only"] == "source('mgmt_only')"
    assert interface_fields["speed"] == "source('speed')"
    assert interface_fields["mtu"] == "source('mtu')"
    assert interface_fields["mode"] == "source('mode')"

    lag_field = next((field for field in interface.fields if field.name == "lag"), None)
    assert lag_field is not None
    assert lag_field.type == "fk"
    assert lag_field.resource == "dcim.interfaces"
    assert lag_field.lookup == {
        "device": "when(source('lag_name') != '', parent_id, None)",
        "name": "when(source('lag_name') != '', source('lag_name'), None)",
    }

    untagged_vlan_field = next((field for field in interface.fields if field.name == "untagged_vlan"), None)
    assert untagged_vlan_field is not None
    assert untagged_vlan_field.type == "fk"
    assert untagged_vlan_field.resource == "ipam.vlans"
    assert untagged_vlan_field.lookup == {
        "vid": "source('untagged_vlan_vid')",
        "site": "when(getattr(parent, 'site', None), getattr(getattr(parent, 'site', None), 'id', None), None)",
    }

    assert len(interface.ip_addresses) == 2, "interface block must declare routed and mgmt ip blocks"
    assert len(interface.tagged_vlans) == 1, "interface block must declare tagged vlan sync"

    routed_ip_block = interface.ip_addresses[0]
    assert routed_ip_block.primary_if == "first"
    assert routed_ip_block.oob_if is None
    assert routed_ip_block.enabled_if == "not source('mgmt_only')"
    assert (
        routed_ip_block.source_items
        == "when(source('ip_address') != '', [{'address': source('ip_address')}], [])"
    )

    routed_address_field = next((field for field in routed_ip_block.fields if field.name == "address"), None)
    routed_status_field = next((field for field in routed_ip_block.fields if field.name == "status"), None)
    assert routed_address_field is not None and routed_address_field.value == "source('address')"
    assert routed_status_field is not None and routed_status_field.value == "'active'"

    mgmt_ip_block = interface.ip_addresses[1]
    assert mgmt_ip_block.primary_if is None
    assert mgmt_ip_block.oob_if == "first"
    assert mgmt_ip_block.enabled_if == "source('mgmt_only')"
    assert (
        mgmt_ip_block.source_items
        == "when(source('ip_address') != '', [{'address': source('ip_address')}], [])"
    )

    mgmt_address_field = next((field for field in mgmt_ip_block.fields if field.name == "address"), None)
    mgmt_status_field = next((field for field in mgmt_ip_block.fields if field.name == "status"), None)
    assert mgmt_address_field is not None and mgmt_address_field.value == "source('address')"
    assert mgmt_status_field is not None and mgmt_status_field.value == "'active'"

    shared_ip = _shared_ip_object(cfg)
    assert shared_ip is not None
    assert shared_ip.source_collection == "shared_ips"
    assert shared_ip.netbox_resource == "ipam.ip_addresses"
    assert shared_ip.lookup_by == ["address"]

    shared_fields = {field.name: field.value for field in shared_ip.fields}
    assert shared_fields["address"] == "source('address')"
    assert shared_fields["role"] == "source('role')"
    assert shared_fields["status"] == "'active'"
    assert shared_fields["description"] == "join(', ', source('references') or [])"
    assert shared_fields["tags"] == "['ndfc-sync']"

    shared_fhrp_group = _shared_fhrp_group_object(cfg)
    assert shared_fhrp_group is not None
    assert shared_fhrp_group.source_collection == "shared_fhrp_groups"
    assert shared_fhrp_group.netbox_resource == "ipam.fhrp_groups"
    assert shared_fhrp_group.lookup_by == ["name"]

    fhrp_group_fields = {field.name: field.value for field in shared_fhrp_group.fields}
    assert fhrp_group_fields["name"] == "source('group_name')"
    assert fhrp_group_fields["protocol"] == "source('protocol')"
    assert fhrp_group_fields["group_id"] == "source('group_id')"
    assert fhrp_group_fields["description"] == "join(', ', source('references') or [])"
    assert fhrp_group_fields["tags"] == "['ndfc-sync']"

    shared_fhrp_vip = _shared_fhrp_vip_object(cfg)
    assert shared_fhrp_vip is not None
    assert shared_fhrp_vip.source_collection == "shared_fhrp_groups"
    assert shared_fhrp_vip.netbox_resource == "ipam.ip_addresses"
    assert shared_fhrp_vip.lookup_by == ["address"]

    fhrp_vip_fields = {field.name: field.value for field in shared_fhrp_vip.fields}
    assert fhrp_vip_fields["address"] == "source('address')"
    assert fhrp_vip_fields["role"] == "source('role')"
    assert fhrp_vip_fields["status"] == "'active'"
    assert fhrp_vip_fields["assigned_object_type"] == "'ipam.fhrpgroup'"
    assert fhrp_vip_fields["description"] == "join(', ', source('references') or [])"
    assert fhrp_vip_fields["tags"] == "['ndfc-sync']"

    assigned_object_id_field = next(
        (field for field in shared_fhrp_vip.fields if field.name == "assigned_object_id"),
        None,
    )
    assert assigned_object_id_field is not None
    assert assigned_object_id_field.type == "fk"
    assert assigned_object_id_field.resource == "ipam.fhrp_groups"
    assert assigned_object_id_field.lookup == {"name": "source('group_name')"}

    shared_fhrp_assignment = _shared_fhrp_assignment_object(cfg)
    assert shared_fhrp_assignment is not None
    assert cfg.objects.index(shared_fhrp_assignment) > cfg.objects.index(device)
    assert shared_fhrp_assignment.source_collection == "shared_fhrp_assignments"
    assert shared_fhrp_assignment.netbox_resource == "ipam.fhrp_group_assignments"
    assert shared_fhrp_assignment.lookup_by == ["group", "interface_type", "interface_id"]

    assignment_fields = {field.name: field.value for field in shared_fhrp_assignment.fields}
    assert assignment_fields["interface_type"] == "'dcim.interface'"
    assert assignment_fields["interface_id"] == (
        "nb_id('dcim.interfaces', {'device_id': nb_id('dcim.devices', {'name': source('device_name'), 'site': nb_id('dcim.sites', {'name': when(source('site_name'), regex_file(source('site_name'), 'nexus_site_to_site'), None)})}), 'name': source('interface_name')})"
    )
    assert assignment_fields["priority"] == "source('priority')"

    group_field = next((field for field in shared_fhrp_assignment.fields if field.name == "group"), None)
    assert group_field is not None
    assert group_field.type == "fk"
    assert group_field.resource == "ipam.fhrp_groups"
    assert group_field.lookup == {"name": "source('group_name')"}

    tagged_vlan_block = interface.tagged_vlans[0]
    assert tagged_vlan_block.source_items == "[{'vid': vid} for vid in (source('tagged_vlan_vids') or [])]"
    tagged_vlan_fields = {field.name: field.value for field in tagged_vlan_block.fields}
    assert tagged_vlan_fields["vid"] == "source('vid')"
    assert (
        tagged_vlan_fields["site"]
        == "when(getattr(parent, 'site', None), getattr(getattr(parent, 'site', None), 'id', None), None)"
    )
