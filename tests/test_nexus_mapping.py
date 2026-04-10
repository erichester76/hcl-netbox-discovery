"""Regression tests for the Nexus example mapping."""

from __future__ import annotations

from collector.config import load_config


def _device_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "device"), None)


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

    lag_field = next((field for field in interface.fields if field.name == "lag"), None)
    assert lag_field is not None
    assert lag_field.type == "fk"
    assert lag_field.resource == "dcim.interfaces"
    assert lag_field.lookup == {
        "device": "when(source('lag_name') != '', parent_id, None)",
        "name": "when(source('lag_name') != '', source('lag_name'), None)",
    }

    assert interface.ip_addresses, "interface block must declare ip_address"
    ip_block = interface.ip_addresses[0]
    assert ip_block.primary_if == "first"
    assert (
        ip_block.source_items
        == "when(source('ip_address') != '', [{'address': source('ip_address')}], [])"
    )

    address_field = next((field for field in ip_block.fields if field.name == "address"), None)
    status_field = next((field for field in ip_block.fields if field.name == "status"), None)
    assert address_field is not None and address_field.value == "source('address')"
    assert status_field is not None and status_field.value == "'active'"
