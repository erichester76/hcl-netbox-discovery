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
        ctx.nb.list.return_value = []  # no pre-existing VLANs
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
        ctx.nb.list.return_value = []  # no pre-existing VLANs
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
        ctx.nb.list.return_value = []  # no pre-existing VLANs
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
        ctx.nb.list.return_value = []  # no pre-existing VLANs
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


# ---------------------------------------------------------------------------
# Tests for _find_or_create_vlan_multisite
# ---------------------------------------------------------------------------


def _make_nb_vlan_obj(record_id: int, vid: int, site_id: int | None = None) -> MagicMock:
    """Create a mock NetBox VLAN object like pynetbox returns."""
    vlan = MagicMock()
    vlan.id = record_id
    vlan.vid = vid
    if site_id is None:
        vlan.site = None
    else:
        site = MagicMock()
        site.id = site_id
        vlan.site = site
    return vlan


class TestFindOrCreateVlanMultisite:
    """Unit tests for Engine._find_or_create_vlan_multisite."""

    def test_siteless_vlan_is_preferred(self):
        """When a siteless VLAN exists it should be updated in-place."""
        engine = _make_engine()
        ctx = _make_ctx(prereqs={"site": 7})
        nb_vlan_obj = _make_nb_vlan_obj(record_id=10, vid=100, site_id=None)
        ctx.nb.list.return_value = [nb_vlan_obj]
        ctx.nb.upsert.return_value = _make_nb_vlan(10)

        result = engine._find_or_create_vlan_multisite(
            {"vid": 100, "name": "VLAN100", "site": 7}, ctx
        )

        # list() called with vid to find existing VLANs
        ctx.nb.list.assert_called_once_with("ipam.vlans", vid=100)
        # upsert called with the siteless VLAN's id; site removed
        upsert_call = ctx.nb.upsert.call_args
        assert upsert_call[0][0] == "ipam.vlans"
        payload = upsert_call[0][1]
        assert payload["id"] == 10
        assert "site" not in payload
        assert upsert_call[1]["lookup_fields"] == ["id"]
        assert result is not None

    def test_site_vlan_matched_exactly(self):
        """When the requested site has a matching VLAN, update that record."""
        engine = _make_engine()
        ctx = _make_ctx()
        nb_vlan_obj = _make_nb_vlan_obj(record_id=20, vid=200, site_id=5)
        ctx.nb.list.return_value = [nb_vlan_obj]
        ctx.nb.upsert.return_value = _make_nb_vlan(20)

        result = engine._find_or_create_vlan_multisite(
            {"vid": 200, "name": "VLAN200", "site": 5}, ctx
        )

        upsert_call = ctx.nb.upsert.call_args
        payload = upsert_call[0][1]
        assert payload["id"] == 20
        assert payload["site"] == 5
        assert upsert_call[1]["lookup_fields"] == ["id"]
        assert result is not None

    def test_multisite_vlan_no_siteless_uses_matched_site(self):
        """Two site-scoped VLANs with same vid; one matches our site → keep it."""
        engine = _make_engine()
        ctx = _make_ctx()
        vlan_site1 = _make_nb_vlan_obj(record_id=30, vid=300, site_id=1)
        vlan_site2 = _make_nb_vlan_obj(record_id=31, vid=300, site_id=2)
        ctx.nb.list.return_value = [vlan_site1, vlan_site2]
        ctx.nb.upsert.return_value = _make_nb_vlan(30)

        result = engine._find_or_create_vlan_multisite(
            {"vid": 300, "name": "VLAN300", "site": 1}, ctx
        )

        upsert_call = ctx.nb.upsert.call_args
        payload = upsert_call[0][1]
        # Should update the site-1 VLAN, not site-2
        assert payload["id"] == 30
        assert payload["site"] == 1
        assert result is not None

    def test_multisite_vlan_siteless_preferred_over_site_match(self):
        """A siteless VLAN takes priority over a site-matched one."""
        engine = _make_engine()
        ctx = _make_ctx()
        vlan_siteless = _make_nb_vlan_obj(record_id=40, vid=400, site_id=None)
        vlan_site1 = _make_nb_vlan_obj(record_id=41, vid=400, site_id=3)
        ctx.nb.list.return_value = [vlan_siteless, vlan_site1]
        ctx.nb.upsert.return_value = _make_nb_vlan(40)

        result = engine._find_or_create_vlan_multisite(
            {"vid": 400, "name": "VLAN400", "site": 3}, ctx
        )

        upsert_call = ctx.nb.upsert.call_args
        payload = upsert_call[0][1]
        assert payload["id"] == 40
        assert "site" not in payload
        assert result is not None

    def test_no_site_requested_only_site_scoped_exist_returns_none(self):
        """Refuse to auto-promote to siteless when no site is requested."""
        engine = _make_engine()
        ctx = _make_ctx()
        vlan_site1 = _make_nb_vlan_obj(record_id=50, vid=500, site_id=9)
        ctx.nb.list.return_value = [vlan_site1]
        ctx.nb.upsert.return_value = None

        result = engine._find_or_create_vlan_multisite(
            {"vid": 500, "name": "VLAN500"}, ctx  # no "site" key
        )

        ctx.nb.upsert.assert_not_called()
        assert result is None

    def test_vlan_exists_only_at_other_sites_creates_new_scoped(self):
        """VLANs exist at other sites but not ours → create a new site-scoped VLAN."""
        engine = _make_engine()
        ctx = _make_ctx()
        vlan_other = _make_nb_vlan_obj(record_id=60, vid=600, site_id=99)
        ctx.nb.list.return_value = [vlan_other]
        new_vlan = _make_nb_vlan(61)
        ctx.nb.upsert.return_value = new_vlan

        result = engine._find_or_create_vlan_multisite(
            {"vid": 600, "name": "VLAN600", "site": 7}, ctx
        )

        upsert_call = ctx.nb.upsert.call_args
        payload = upsert_call[0][1]
        assert "id" not in payload  # creating, not updating
        assert payload["site"] == 7
        assert upsert_call[1]["lookup_fields"] == []
        assert result == new_vlan

    def test_no_existing_vlans_creates_new(self):
        """When no VLANs exist with this vid, create from scratch."""
        engine = _make_engine()
        ctx = _make_ctx()
        ctx.nb.list.return_value = []
        new_vlan = _make_nb_vlan(70)
        ctx.nb.upsert.return_value = new_vlan

        result = engine._find_or_create_vlan_multisite(
            {"vid": 700, "name": "VLAN700", "site": 3}, ctx
        )

        upsert_call = ctx.nb.upsert.call_args
        payload = upsert_call[0][1]
        assert "id" not in payload
        assert payload["site"] == 3
        assert upsert_call[1]["lookup_fields"] == []
        assert result == new_vlan

    def test_process_tagged_vlans_uses_multisite_for_ipam_vlans(self):
        """_process_tagged_vlans should call _find_or_create_vlan_multisite for ipam.vlans."""
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(42)

        # No existing VLANs → will create
        ctx.nb.list.return_value = []
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

        # list() should be called (multi-site path) rather than plain upsert
        ctx.nb.list.assert_called_once_with("ipam.vlans", vid=10)
        ctx.nb.update.assert_called_once_with(
            "dcim.interfaces", 42, {"mode": "tagged", "tagged_vlans": [101]}
        )

    def test_process_tagged_vlans_non_ipam_vlans_uses_standard_upsert(self):
        """Non-ipam.vlans resources still use the standard upsert path."""
        engine = _make_engine()
        ctx = _make_ctx()
        nb_iface = _make_nb_iface(30)
        nb_obj = _make_nb_vlan(501)
        ctx.nb.upsert.return_value = nb_obj

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

        # list() should NOT be called; standard upsert is used
        ctx.nb.list.assert_not_called()
        upsert_call = ctx.nb.upsert.call_args
        assert upsert_call[0][0] == "extras.custom_items"
