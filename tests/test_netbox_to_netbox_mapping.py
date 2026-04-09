"""Targeted tests for the shipped NetBox-to-NetBox example mapping."""

from __future__ import annotations

from collector.config import load_config


def test_site_lookup_uses_name_only() -> None:
    cfg = load_config("mappings/netbox-to-netbox.hcl.example")
    site_obj = next((obj for obj in cfg.objects if obj.name == "site"), None)

    assert site_obj is not None, (
        "Expected object 'site' in mappings/netbox-to-netbox.hcl.example"
    )
    assert site_obj.lookup_by == ["name"]
