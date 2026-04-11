"""Regression tests for optional Nexus custom-object mapping blocks."""

from __future__ import annotations

from collector.config import load_config


def _object(cfg, name: str):
    return next((obj for obj in cfg.objects if obj.name == name), None)


def test_nexus_example_mapping_includes_optional_custom_object_blocks(monkeypatch):
    monkeypatch.delenv("NETBOX_USE_CUSTOM_OBJECTS", raising=False)
    cfg = load_config("mappings/nexus.hcl.example")

    assert cfg.collector.extra_flags["use_custom_objects"] is False

    fallback_custom_fields = _object(cfg, "ndfc_topology_custom_field")
    assert fallback_custom_fields is not None
    assert fallback_custom_fields.source_collection == "topology_custom_fields"
    assert fallback_custom_fields.netbox_resource == "extras.custom_fields"
    assert fallback_custom_fields.lookup_by == ["name"]
    assert fallback_custom_fields.enabled_if == "not collector.use_custom_objects"
    fallback_fields = {field.name: field.value for field in fallback_custom_fields.fields}
    assert fallback_fields["name"] == "source('name')"
    assert fallback_fields["label"] == "source('label')"
    assert fallback_fields["group_name"] == "source('group_name')"
    assert fallback_fields["description"] == "source('description')"
    assert fallback_fields["type"] == "source('type')"
    assert fallback_fields["object_types"] == "source('object_types')"
    assert fallback_fields["ui_visible"] == "source('ui_visible')"
    assert fallback_fields["ui_editable"] == "source('ui_editable')"

    fabric = _object(cfg, "ndfc_fabric")
    assert fabric is not None
    assert fabric.source_collection == "fabrics"
    assert fabric.netbox_resource == "plugins.custom_objects.ndfc_fabrics"
    assert fabric.lookup_by == ["identifier"]
    assert fabric.enabled_if == "collector.use_custom_objects"
    fabric_fields = {field.name: field.value for field in fabric.fields}
    assert fabric_fields["identifier"] == "source('identifier')"
    assert fabric_fields["fabric_name"] == "source('fabric_name')"
    assert fabric_fields["site_names"] == "source('site_names')"
    assert fabric_fields["tenant_names"] == "source('tenant_names')"
    assert fabric_fields["devices"] == "[{'name': name} for name in (source('device_names') or [])]"
    assert fabric_fields["tags"] == "['ndfc-sync']"

    domain = _object(cfg, "ndfc_vpc_domain")
    assert domain is not None
    assert domain.source_collection == "vpc_domains"
    assert domain.netbox_resource == "plugins.custom_objects.ndfc_vpc_domains"
    assert domain.lookup_by == ["identifier"]
    assert domain.enabled_if == "collector.use_custom_objects"
    domain_fields = {field.name: field.value for field in domain.fields}
    assert domain_fields["identifier"] == "source('identifier')"
    assert domain_fields["fabric_identifier"] == "source('fabric_identifier')"
    assert domain_fields["fabric_name"] == "source('fabric_name')"
    assert domain_fields["vpc_domain_id"] == "source('vpc_domain_id')"
    assert domain_fields["vpc_name"] == "source('vpc_name')"
    assert (
        domain_fields["primary_device"]
        == "when(source('primary_device_name') != '', {'name': source('primary_device_name')}, None)"
    )
    assert (
        domain_fields["secondary_device"]
        == "when(source('secondary_device_name') != '', {'name': source('secondary_device_name')}, None)"
    )
    assert domain_fields["peer_devices"] == "[{'name': name} for name in (source('peer_device_names') or [])]"
    assert (
        domain_fields["member_lags"]
        == "[{'device': {'name': ref['device_name']}, 'name': ref['name']} for ref in (source('member_lag_refs') or [])]"
    )
    assert (
        domain_fields["vpc_interfaces"]
        == "[{'device': {'name': ref['device_name']}, 'name': ref['name']} for ref in (source('vpc_interface_refs') or [])]"
    )
    assert domain_fields["tenant_names"] == "source('tenant_names')"
    assert domain_fields["vrf_names"] == "source('vrf_names')"
    assert domain_fields["tags"] == "['ndfc-sync']"

    peer_link = _object(cfg, "ndfc_vpc_peer_link")
    assert peer_link is not None
    assert peer_link.source_collection == "vpc_peer_links"
    assert peer_link.netbox_resource == "plugins.custom_objects.ndfc_vpc_peer_links"
    assert peer_link.lookup_by == ["identifier"]
    assert peer_link.enabled_if == "collector.use_custom_objects"
    peer_link_fields = {field.name: field.value for field in peer_link.fields}
    assert peer_link_fields["identifier"] == "source('identifier')"
    assert peer_link_fields["fabric_identifier"] == "source('fabric_identifier')"
    assert peer_link_fields["fabric_name"] == "source('fabric_name')"
    assert peer_link_fields["vpc_domain_identifier"] == "source('vpc_domain_identifier')"
    assert peer_link_fields["devices"] == "[{'name': name} for name in (source('device_names') or [])]"
    assert (
        peer_link_fields["interfaces"]
        == "[{'device': {'name': ref['device_name']}, 'name': ref['name']} for ref in (source('interface_refs') or [])]"
    )
    assert peer_link_fields["status"] == "source('status')"
    assert peer_link_fields["tenant_names"] == "source('tenant_names')"
    assert peer_link_fields["vrf_names"] == "source('vrf_names')"
    assert peer_link_fields["tags"] == "['ndfc-sync']"
