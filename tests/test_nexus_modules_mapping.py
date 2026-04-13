"""Regression tests for Nexus module blocks in the unified example mapping."""

from __future__ import annotations

from collector.config import load_config


def _device_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "device"), None)


def _object(cfg, name: str):
    return next((obj for obj in cfg.objects if obj.name == name), None)


def test_nexus_modules_example_mapping_defines_module_blocks(monkeypatch):
    monkeypatch.delenv("NDFC_FETCH_MODULES", raising=False)
    monkeypatch.delenv("NETBOX_USE_CUSTOM_OBJECTS", raising=False)
    cfg = load_config("mappings/nexus.hcl.example")

    assert cfg.source.api_type == "nexus"
    assert cfg.source.extra.get("fetch_modules") == "false"
    assert "sync_modules" in cfg.collector.extra_flags
    assert cfg.collector.extra_flags["use_custom_objects"] is False

    shared_ip = _object(cfg, "shared_ip")
    assert shared_ip is not None
    assert shared_ip.source_collection == "shared_ips"
    assert shared_ip.netbox_resource == "ipam.ip_addresses"

    fallback_custom_fields = _object(cfg, "ndfc_topology_custom_field")
    assert fallback_custom_fields is not None
    assert fallback_custom_fields.source_collection == "topology_custom_fields"
    assert fallback_custom_fields.netbox_resource == "extras.custom_fields"
    assert fallback_custom_fields.enabled_if == "not collector.use_custom_objects"

    fabric = _object(cfg, "ndfc_fabric")
    assert fabric is not None
    assert fabric.source_collection == "fabrics"
    assert fabric.netbox_resource == "plugins.custom_objects.ndfc_fabrics"
    assert fabric.enabled_if == "collector.use_custom_objects"

    domain = _object(cfg, "ndfc_vpc_domain")
    assert domain is not None
    assert domain.source_collection == "vpc_domains"
    assert domain.netbox_resource == "plugins.custom_objects.ndfc_vpc_domains"
    assert domain.enabled_if == "collector.use_custom_objects"

    peer_link = _object(cfg, "ndfc_vpc_peer_link")
    assert peer_link is not None
    assert peer_link.source_collection == "vpc_peer_links"
    assert peer_link.netbox_resource == "plugins.custom_objects.ndfc_vpc_peer_links"
    assert peer_link.enabled_if == "collector.use_custom_objects"

    device = _device_object(cfg)
    assert device is not None
    assert len(device.modules) == 3
    assert device.inventory_items == []
    device_fields = {field.name: field.value for field in device.fields}
    assert (
        device_fields["custom_fields"]
        == "when(not collector.use_custom_objects, {k: v for k, v in {'ndfc_fabric': source('fabric_name'), 'ndfc_vpc_domain': source('vpc_domain_id'), 'ndfc_vpc_role': source('vpc_role'), 'ndfc_vpc_peer': source('vpc_peer_name'), 'ndfc_tenant': source('tenant_name')}.items() if v not in (None, '', [], {})}, {})"
    )

    assert device.interfaces, "device should define interfaces"
    interface = device.interfaces[0]
    interface_fields = {field.name: field.value for field in interface.fields}
    assert interface_fields["speed"] == "source('speed')"
    assert interface_fields["mtu"] == "source('mtu')"
    assert interface_fields["mode"] == "source('mode')"
    assert (
        interface_fields["custom_fields"]
        == "when(not collector.use_custom_objects, {k: v for k, v in {'ndfc_fabric': source('fabric_name'), 'ndfc_vrf': source('vrf_name'), 'ndfc_vpc': source('vpc_name'), 'ndfc_vpc_parent_lag': source('vpc_parent_lag_name')}.items() if v not in (None, '', [], {})}, {})"
    )

    untagged_vlan_field = next((field for field in interface.fields if field.name == "untagged_vlan"), None)
    assert untagged_vlan_field is not None
    assert untagged_vlan_field.type == "fk"
    assert untagged_vlan_field.resource == "ipam.vlans"

    assert len(interface.tagged_vlans) == 1

    modules_by_profile = {module.profile: module for module in device.modules}
    assert set(modules_by_profile) == {"Power supply", "Fan", "Transceiver"}

    psu = modules_by_profile["Power supply"]
    assert psu.enabled_if == "collector.sync_modules"
    assert psu.dedupe_by == "source('serial') or source('bay_name')"
    assert psu.power_input is not None
    assert psu.power_input.name == "'Power Input ' + str(source('position') or source('bay_name'))"
    assert psu.power_input.type == "'iec-60320-c14'"
    psu_fields = {field.name: field.value for field in psu.fields}
    assert psu_fields["description"] == "source('description')"
    assert (
        psu_fields["status"]
        == "map_value(lower(source('status')), {'ok': 'active', 'active': 'active', 'up': 'active', 'offenvpower': 'offline', 'down': 'offline'}, 'active')"
    )

    fan = modules_by_profile["Fan"]
    fan_fields = {field.name: field.value for field in fan.fields}
    assert fan_fields["description"] == "source('description')"
    assert (
        fan_fields["status"]
        == "map_value(lower(source('status')), {'ok': 'active', 'active': 'active', 'up': 'active', 'offenvpower': 'offline', 'down': 'offline'}, 'active')"
    )

    transceiver = modules_by_profile["Transceiver"]
    assert transceiver.dedupe_by == "source('serial') or source('bay_name') or source('model')"
    transceiver_fields = {field.name: field.value for field in transceiver.fields}
    assert transceiver_fields["description"] == "source('description')"
    assert (
        transceiver_fields["status"]
        == "map_value(lower(source('status')), {'ok': 'active', 'active': 'active', 'up': 'active', 'offenvpower': 'offline', 'down': 'offline'}, 'active')"
    )
