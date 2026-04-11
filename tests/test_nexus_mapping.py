"""Regression tests for the Nexus example mapping."""

from __future__ import annotations

from collector.config import load_config


def _device_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "device"), None)


def _shared_ip_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "shared_ip"), None)


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

    lag_field = next((field for field in interface.fields if field.name == "lag"), None)
    assert lag_field is not None
    assert lag_field.type == "fk"
    assert lag_field.resource == "dcim.interfaces"
    assert lag_field.lookup == {
        "device": "when(source('lag_name') != '', parent_id, None)",
        "name": "when(source('lag_name') != '', source('lag_name'), None)",
    }

    assert len(interface.ip_addresses) == 2, "interface block must declare routed and mgmt ip blocks"

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
    assert shared_fields["tags"] == "['ndfc-sync', 'ndfc-shared-ip']"
