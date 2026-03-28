"""Tests for tagged VLAN processing in Engine._process_tagged_vlans.

Covers:
- VLANs are upserted in ipam.vlans with the correct payload
- Interface is updated with mode="tagged" and the resolved VLAN IDs
- Multiple VLANs from a single tagged_vlan block are all associated
- Empty source_items list skips the interface update entirely
- enabled_if=False skips the block
- dry_run logs but does not write to NetBox
- Errors during VLAN upsert are caught; interface update is still attempted
  for any successfully resolved VLANs
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from collector.config import (
    CollectorOptions,
    FieldConfig,
    InterfaceConfig,
    TaggedVlanConfig,
)
from collector.context import RunContext
from collector.engine import Engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine() -> Engine:
    return Engine()


def _make_ctx(dry_run: bool = False, prereqs: dict | None = None) -> RunContext:
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
        prereqs=prereqs or {},
        source_obj=None,
        parent_nb_obj=None,
        dry_run=dry_run,
    )


def _make_nb_iface(iface_id: int = 42) -> MagicMock:
    iface = MagicMock()
    iface.id = iface_id
    return iface


def _make_nb_vlan(vlan_id: int) -> MagicMock:
    vlan = MagicMock()
    vlan.id = vlan_id
    return vlan


def _make_tagged_vlan_cfg(source_items: str = "_vlans") -> TaggedVlanConfig:
    return TaggedVlanConfig(
        source_items=source_items,
        fields=[
            FieldConfig(name="vid", value="source('id')"),
            FieldConfig(name="name", value="source('name')"),
        ],
    )


def _make_iface_cfg_with_tagged_vlans(
    tagged_vlan_cfg: TaggedVlanConfig | None = None,
) -> InterfaceConfig:
    if tagged_vlan_cfg is None:
        tagged_vlan_cfg = _make_tagged_vlan_cfg()
    return InterfaceConfig(
        source_items="_nics",
        tagged_vlans=[tagged_vlan_cfg],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProcessTaggedVlans:
    """Direct unit tests for Engine._process_tagged_vlans."""

    def test_single_vlan_upserted_and_interface_updated(self):
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(42)
        nb_vlan = _make_nb_vlan(101)
        ctx.nb.upsert.return_value = nb_vlan

        iface_item = SimpleNamespace(_vlans=[{"id": 10, "name": "VLAN10"}])
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        # VLAN upsert should have been called
        ctx.nb.upsert.assert_called_once()
        upsert_args = ctx.nb.upsert.call_args
        assert upsert_args[0][0] == "ipam.vlans"
        assert upsert_args[0][1].get("vid") == 10
        assert upsert_args[0][1].get("name") == "VLAN10"

        # Interface should be updated with mode=tagged and the VLAN ID
        ctx.nb.update.assert_called_once_with(
            "dcim.interfaces", 42, {"mode": "tagged", "tagged_vlans": [101]}
        )

    def test_multiple_vlans_all_associated(self):
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(5)
        nb_vlan_a = _make_nb_vlan(201)
        nb_vlan_b = _make_nb_vlan(202)
        ctx.nb.upsert.side_effect = [nb_vlan_a, nb_vlan_b]

        iface_item = SimpleNamespace(
            _vlans=[{"id": 20, "name": "VLAN20"}, {"id": 30, "name": "VLAN30"}]
        )
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        assert ctx.nb.upsert.call_count == 2
        ctx.nb.update.assert_called_once_with(
            "dcim.interfaces", 5, {"mode": "tagged", "tagged_vlans": [201, 202]}
        )

    def test_empty_vlans_list_skips_interface_update(self):
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(7)

        iface_item = SimpleNamespace(_vlans=[])
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        ctx.nb.upsert.assert_not_called()
        ctx.nb.update.assert_not_called()

    def test_dry_run_skips_writes(self):
        engine = _make_engine()
        ctx = _make_ctx(dry_run=True)
        nb_iface = _make_nb_iface(8)

        iface_item = SimpleNamespace(_vlans=[{"id": 50, "name": "VLAN50"}])
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        ctx.nb.upsert.assert_not_called()
        ctx.nb.update.assert_not_called()

    def test_enabled_if_false_skips_block(self):
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(9)

        iface_item = SimpleNamespace(_vlans=[{"id": 60, "name": "VLAN60"}])
        vlan_cfg = TaggedVlanConfig(
            source_items="_vlans",
            enabled_if="False",
            fields=[
                FieldConfig(name="vid", value="source('id')"),
            ],
        )
        iface_cfg = InterfaceConfig(source_items="_nics", tagged_vlans=[vlan_cfg])

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        ctx.nb.upsert.assert_not_called()
        ctx.nb.update.assert_not_called()

    def test_vlan_upsert_error_is_swallowed(self):
        """A VLAN upsert failure should not raise; interface update is skipped."""
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(10)
        ctx.nb.upsert.side_effect = Exception("NetBox connection error")

        iface_item = SimpleNamespace(_vlans=[{"id": 70, "name": "VLAN70"}])
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        # Should not raise
        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        # Interface update should not be called since no VLAN IDs were collected
        ctx.nb.update.assert_not_called()

    def test_virtualization_interface_resource_used_correctly(self):
        """tagged_vlans update should use the iface_resource passed in."""
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(15)
        nb_vlan = _make_nb_vlan(301)
        ctx.nb.upsert.return_value = nb_vlan

        iface_item = SimpleNamespace(_vlans=[{"id": 80, "name": "VLAN80"}])
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg,
            nb_iface,
            iface_item,
            nested_ctx,
            resolver,
            "virtualization.interfaces",  # VM interface resource
        )

        ctx.nb.update.assert_called_once_with(
            "virtualization.interfaces", 15, {"mode": "tagged", "tagged_vlans": [301]}
        )

    def test_site_prereq_included_in_vlan_lookup(self):
        """When the context has a 'site' prereq, it should appear in the VLAN payload."""
        engine = _make_engine()
        ctx = _make_ctx(prereqs={"site": 7})
        nb_iface = _make_nb_iface(20)
        nb_vlan = _make_nb_vlan(401)
        ctx.nb.upsert.return_value = nb_vlan

        iface_item = SimpleNamespace(_vlans=[{"id": 90, "name": "VLAN90"}])

        vlan_cfg = TaggedVlanConfig(
            source_items="_vlans",
            fields=[
                FieldConfig(name="vid", value="source('id')"),
                FieldConfig(name="name", value="source('name')"),
                FieldConfig(name="site", value="prereq('site')"),
            ],
        )
        iface_cfg = InterfaceConfig(source_items="_nics", tagged_vlans=[vlan_cfg])

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        upsert_payload = ctx.nb.upsert.call_args[0][1]
        assert upsert_payload.get("site") == 7

    def test_custom_netbox_resource_and_lookup_by(self):
        """netbox_resource and lookup_by from TaggedVlanConfig are used instead of defaults."""
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(30)
        nb_obj = _make_nb_vlan(501)
        ctx.nb.upsert.return_value = nb_obj

        # Imagine a non-standard resource keyed on "id" instead of "vid"
        iface_item = SimpleNamespace(_items=[{"custom_id": 99, "label": "Item99"}])

        vlan_cfg = TaggedVlanConfig(
            source_items="_items",
            netbox_resource="extras.custom_items",
            lookup_by=["custom_id"],
            fields=[
                FieldConfig(name="custom_id", value="source('custom_id')"),
                FieldConfig(name="label", value="source('label')"),
            ],
        )
        iface_cfg = InterfaceConfig(source_items="_nics", tagged_vlans=[vlan_cfg])

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        # Upsert should use the custom resource
        upsert_call = ctx.nb.upsert.call_args
        assert upsert_call[0][0] == "extras.custom_items"
        assert upsert_call[1]["lookup_fields"] == ["custom_id"]
        # Interface update should include the resolved ID
        ctx.nb.update.assert_called_once_with(
            "dcim.interfaces", 30, {"mode": "tagged", "tagged_vlans": [501]}
        )

    def test_sync_tag_injected_into_vlan_payload(self):
        """Sync tag should be added to the VLAN upsert payload for consistency."""
        engine = _make_engine()
        ctx = _make_ctx()
        ctx.collector_opts = CollectorOptions(
            max_workers=1, dry_run=False, sync_tag="my-sync", regex_dir="/tmp/regex"
        )
        nb_iface = _make_nb_iface(50)
        nb_vlan = _make_nb_vlan(601)
        ctx.nb.upsert.return_value = nb_vlan

        iface_item = SimpleNamespace(_vlans=[{"id": 10, "name": "VLAN10"}])
        iface_cfg = _make_iface_cfg_with_tagged_vlans()

        from collector.field_resolvers import Resolver

        nested_ctx = ctx.for_nested(iface_item, nb_iface)
        resolver = Resolver(nested_ctx)

        engine._process_tagged_vlans(
            iface_cfg, nb_iface, iface_item, nested_ctx, resolver, "dcim.interfaces"
        )

        upsert_payload = ctx.nb.upsert.call_args[0][1]
        assert {"name": "my-sync"} in upsert_payload.get("tags", [])
