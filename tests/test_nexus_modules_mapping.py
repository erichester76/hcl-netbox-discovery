"""Regression tests for the Nexus modules example mapping."""

from __future__ import annotations

from collector.config import load_config


def _device_object(cfg):
    return next((obj for obj in cfg.objects if obj.name == "device"), None)


def test_nexus_modules_example_mapping_defines_module_blocks(monkeypatch):
    monkeypatch.delenv("NDFC_FETCH_MODULES", raising=False)
    cfg = load_config("mappings/nexus-modules.hcl.example")

    assert cfg.source.api_type == "nexus"
    assert cfg.source.extra.get("fetch_modules") == "false"
    assert "sync_modules" in cfg.collector.extra_flags

    device = _device_object(cfg)
    assert device is not None
    assert len(device.modules) == 3
    assert device.inventory_items == []

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

    transceiver = modules_by_profile["Transceiver"]
    assert transceiver.dedupe_by == "source('serial') or source('bay_name') or source('model')"
    transceiver_fields = {field.name: field.value for field in transceiver.fields}
    assert transceiver_fields["description"] == "source('description')"
    assert (
        transceiver_fields["status"]
        == "map_value(lower(source('status')), {'ok': 'active', 'active': 'active', 'up': 'active', 'offenvpower': 'offline', 'down': 'offline'}, 'active')"
    )
