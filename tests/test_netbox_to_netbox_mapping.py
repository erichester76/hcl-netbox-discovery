"""Targeted tests for the shipped NetBox-to-NetBox example mapping."""

from __future__ import annotations

from collector.config import load_config


def test_site_lookup_uses_name_only() -> None:
    cfg = load_config("mappings/netbox-to-netbox.hcl.example")
    site_obj = next(obj for obj in cfg.objects if obj.name == "site")

    assert site_obj.lookup_by == ["name"]


def test_contact_lookup_uses_name_only() -> None:
    cfg = load_config("mappings/netbox-to-netbox.hcl.example")
    contact_obj = next(obj for obj in cfg.objects if obj.name == "contact")

    assert contact_obj.lookup_by == ["name"]
