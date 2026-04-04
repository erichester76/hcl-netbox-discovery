"""Tests for tag formatting fixes in the Engine (engine.py).

Covers:
- _eval_field normalises plain-string tags to {"name": tag} dicts (Fix 1a)
- _inject_sync_tag uses {"name": sync_tag} dicts (Fix 1b)
- _inject_sync_tag de-duplicates correctly when tags already contain the tag (Fix 1c)
- _ensure_sync_tag returns True on success and False on failure (Fix 2)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from collector.engine import Engine
from collector.config import FieldConfig


# ---------------------------------------------------------------------------
# _inject_sync_tag
# ---------------------------------------------------------------------------


class TestInjectSyncTag:
    def test_adds_tag_as_dict(self):
        payload: dict = {}
        Engine._inject_sync_tag(payload, "vmware-sync")
        assert payload["tags"] == [{"name": "vmware-sync"}]

    def test_does_not_duplicate_existing_dict_tag(self):
        payload = {"tags": [{"name": "vmware-sync"}]}
        Engine._inject_sync_tag(payload, "vmware-sync")
        assert payload["tags"] == [{"name": "vmware-sync"}]

    def test_does_not_duplicate_when_existing_string_tag_matches(self):
        # Defensive: pre-existing tags list already has the plain-string form
        payload = {"tags": ["vmware-sync"]}
        Engine._inject_sync_tag(payload, "vmware-sync")
        # Should NOT add a second entry
        assert len(payload["tags"]) == 1

    def test_appends_to_existing_different_tags(self):
        payload = {"tags": [{"name": "manual"}]}
        Engine._inject_sync_tag(payload, "vmware-sync")
        assert {"name": "vmware-sync"} in payload["tags"]
        assert {"name": "manual"} in payload["tags"]

    def test_noop_when_sync_tag_is_empty(self):
        payload: dict = {}
        Engine._inject_sync_tag(payload, "")
        assert "tags" not in payload

    def test_non_list_tags_replaced_with_single_dict(self):
        payload = {"tags": "not-a-list"}
        Engine._inject_sync_tag(payload, "vmware-sync")
        assert payload["tags"] == [{"name": "vmware-sync"}]

    def test_deduplicates_case_insensitive_tag_names(self):
        payload = {"tags": [{"name": "VMWARE-SYNC"}]}
        Engine._inject_sync_tag(payload, "vmware-sync")
        assert len(payload["tags"]) == 1


class TestAdditiveTagBehavior:
    def test_dry_run_noop_when_desired_tag_already_present_with_other_tags(self):
        engine = Engine()
        ctx = MagicMock()
        ctx.nb.get.return_value = {
            "id": 101,
            "serial": "ABC123",
            "name": "server-1",
            "tags": [{"name": "netbox-sync"}, {"name": "xclarity-sync"}],
        }
        payload = {
            "serial": "ABC123",
            "name": "server-1",
            "tags": [{"name": "xclarity-sync"}],
        }

        outcome, _, _ = engine._dry_run_outcome(ctx, "dcim.devices", payload, ["serial"])

        assert outcome == "would_noop"

    def test_dry_run_update_when_desired_tag_missing(self):
        engine = Engine()
        ctx = MagicMock()
        ctx.nb.get.return_value = {
            "id": 102,
            "serial": "ABC124",
            "name": "server-2",
            "tags": [{"name": "netbox-sync"}],
        }
        payload = {
            "serial": "ABC124",
            "name": "server-2",
            "tags": [{"name": "xclarity-sync"}],
        }

        outcome, _, _ = engine._dry_run_outcome(ctx, "dcim.devices", payload, ["serial"])

        assert outcome == "would_update"

    def test_non_dry_run_merge_keeps_existing_tags_and_adds_sync_tag(self):
        engine = Engine()
        ctx = MagicMock()
        ctx.nb.get.return_value = {
            "id": 103,
            "serial": "ABC125",
            "tags": [{"name": "netbox-sync"}, {"name": "manual"}],
        }
        payload = {"serial": "ABC125", "tags": [{"name": "xclarity-sync"}]}

        engine._merge_payload_tags_for_upsert(ctx, "dcim.devices", payload, ["serial"])

        assert payload["tags"] == [
            {"name": "netbox-sync"},
            {"name": "manual"},
            {"name": "xclarity-sync"},
        ]


# ---------------------------------------------------------------------------
# _ensure_sync_tag
# ---------------------------------------------------------------------------


class TestEnsureSyncTag:
    """Unit-test _ensure_sync_tag return values and error handling."""

    def test_returns_true_on_successful_upsert(self):
        nb = MagicMock()
        nb.upsert.return_value = {"id": 1, "name": "vmware-sync"}
        engine = Engine()
        result = engine._ensure_sync_tag(nb, "vmware-sync")
        assert result is True
        nb.upsert.assert_called_once_with(
            "extras.tags",
            {"name": "vmware-sync", "slug": "vmware-sync", "color": "9e9e9e"},
            lookup_fields=["slug"],
        )

    def test_returns_false_when_upsert_raises(self):
        nb = MagicMock()
        nb.upsert.side_effect = Exception("NetBox 403 Forbidden")
        engine = Engine()
        result = engine._ensure_sync_tag(nb, "vmware-sync")
        assert result is False

    def test_slug_is_derived_from_tag_name(self):
        """Verify the slug sent to NetBox is the slugified form of the name."""
        nb = MagicMock()
        nb.upsert.return_value = {"id": 2, "name": "My Tag"}
        engine = Engine()
        engine._ensure_sync_tag(nb, "My Tag")
        call_kwargs = nb.upsert.call_args
        payload = call_kwargs[0][1]
        assert payload["slug"] == "my-tag"
        assert payload["name"] == "My Tag"


# ---------------------------------------------------------------------------
# _eval_field – tags type
# ---------------------------------------------------------------------------


class TestEvalFieldTags:
    """Unit-test the tags branch of Engine._eval_field."""

    def _make_field_cfg(self, value: str) -> FieldConfig:
        return FieldConfig(name="tags", value=value, type="tags")

    def _make_resolver(self, raw_value):
        resolver = MagicMock()
        resolver.evaluate.return_value = raw_value
        return resolver

    def _eval(self, raw_value):
        engine = Engine()
        field_cfg = self._make_field_cfg("['vmware-sync']")
        resolver = self._make_resolver(raw_value)
        ctx = MagicMock()
        return engine._eval_field(field_cfg, resolver, ctx)

    def test_string_tags_normalised_to_dicts(self):
        result = self._eval(["vmware-sync"])
        assert result == [{"name": "vmware-sync"}]

    def test_dict_tags_passed_through_unchanged(self):
        result = self._eval([{"name": "vmware-sync"}])
        assert result == [{"name": "vmware-sync"}]

    def test_mixed_tags_all_normalised(self):
        result = self._eval(["tag-a", {"name": "tag-b"}])
        assert {"name": "tag-a"} in result
        assert {"name": "tag-b"} in result

    def test_empty_list_returns_empty(self):
        result = self._eval([])
        assert result == []

    def test_none_raw_returns_empty(self):
        result = self._eval(None)
        assert result == []

    def test_single_string_tag_wrapped_in_list(self):
        result = self._eval("vmware-sync")
        assert result == [{"name": "vmware-sync"}]
