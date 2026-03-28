"""Tests for prerequisite methods in collector/prerequisites.py.

Covers:
- _ensure_platform race condition: uniqueness error falls back to GET (Fix 3)
- _resolve_placement: rack_position must be a positive integer and only set
  when rack_id is resolved
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from collector.prerequisites import PrerequisiteRunner


class TestEnsurePlatformRaceCondition:
    """_ensure_platform should recover from a uniqueness collision."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        platform_obj = MagicMock(id=42)
        nb.upsert.return_value = platform_obj

        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

        assert result == 42

    def test_falls_back_to_get_on_uniqueness_error(self):
        """If upsert raises a 'unique' error, _ensure_platform falls back to GET."""
        nb = MagicMock()
        nb.upsert.side_effect = Exception(
            "The request failed with code 400 Bad Request: "
            "{'__all__': ['Platform name must be unique.']}"
        )
        platform_obj = MagicMock(id=99)
        nb.get.return_value = platform_obj

        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

        assert result == 99
        nb.get.assert_called_once_with("dcim.platforms", slug="vmware-esxi-70")

    def test_returns_none_when_fallback_get_also_fails(self):
        nb = MagicMock()
        nb.upsert.side_effect = Exception(
            "The request failed with code 400 Bad Request: "
            "{'__all__': ['Platform name must be unique.']}"
        )
        nb.get.side_effect = Exception("not found")

        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

        assert result is None

    def test_re_raises_non_uniqueness_errors(self):
        nb = MagicMock()
        nb.upsert.side_effect = Exception("Network timeout")

        runner = self._make_runner(nb)
        with pytest.raises(Exception, match="Network timeout"):
            runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

    def test_re_raises_400_without_unique_keyword(self):
        """A 400 error not related to uniqueness must still propagate."""
        nb = MagicMock()
        nb.upsert.side_effect = Exception(
            "The request failed with code 400 Bad Request: {'name': ['This field is required.']}"
        )

        runner = self._make_runner(nb)
        with pytest.raises(Exception, match="400"):
            runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

    def test_dry_run_returns_none_without_network_calls(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=True)

        assert result is None
        nb.upsert.assert_not_called()
        nb.get.assert_not_called()

    def test_passes_manufacturer_id_to_upsert(self):
        nb = MagicMock()
        platform_obj = MagicMock(id=10)
        nb.upsert.return_value = platform_obj

        runner = self._make_runner(nb)
        runner._ensure_platform({"name": "ESXi", "manufacturer": 5}, dry_run=False)

        nb.upsert.assert_called_once()
        payload = nb.upsert.call_args[0][1]
        assert payload.get("manufacturer") == 5


class TestEnsurePlatformManufacturerName:
    """_ensure_platform derives manufacturer from manufacturer_name when no ID given."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_ensures_manufacturer_from_name_and_uses_id(self):
        nb = MagicMock()
        manufacturer_obj = MagicMock(id=7)
        platform_obj = MagicMock(id=15)
        # First upsert call → manufacturer, second → platform
        nb.upsert.side_effect = [manufacturer_obj, platform_obj]

        runner = self._make_runner(nb)
        result = runner._ensure_platform(
            {"name": "VMware ESXi 7.0", "manufacturer_name": "VMware"},
            dry_run=False,
        )

        assert result == 15
        # Platform payload must include the resolved manufacturer ID
        platform_payload = nb.upsert.call_args_list[1][0][1]
        assert platform_payload.get("manufacturer") == 7

    def test_manufacturer_id_takes_precedence_over_manufacturer_name(self):
        """When both manufacturer (ID) and manufacturer_name are given, ID wins."""
        nb = MagicMock()
        platform_obj = MagicMock(id=20)
        nb.upsert.return_value = platform_obj

        runner = self._make_runner(nb)
        runner._ensure_platform(
            {"name": "ESXi", "manufacturer": 5, "manufacturer_name": "VMware"},
            dry_run=False,
        )

        # Should only be one upsert call (the platform one, not a manufacturer ensure)
        nb.upsert.assert_called_once()
        payload = nb.upsert.call_args[0][1]
        assert payload.get("manufacturer") == 5

    def test_dry_run_skips_manufacturer_ensure_and_platform_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_platform(
            {"name": "VMware ESXi 7.0", "manufacturer_name": "VMware"},
            dry_run=True,
        )
        assert result is None
        nb.upsert.assert_not_called()

    def test_empty_manufacturer_name_skips_manufacturer_ensure(self):
        nb = MagicMock()
        platform_obj = MagicMock(id=30)
        nb.upsert.return_value = platform_obj

        runner = self._make_runner(nb)
        result = runner._ensure_platform(
            {"name": "Unknown", "manufacturer_name": ""},
            dry_run=False,
        )

        assert result == 30
        # Only one upsert: the platform itself (no manufacturer ensure)
        nb.upsert.assert_called_once()
        payload = nb.upsert.call_args[0][1]
        assert "manufacturer" not in payload


class TestResolvePlacement:
    """_resolve_placement must guard against invalid position values."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def _mock_upsert_side_effect(self, resource, payload, lookup_fields=None):
        """Return a mock NetBox object whose id depends on the resource type."""
        obj = MagicMock()
        if resource == "dcim.sites":
            obj.id = 1
        elif resource == "dcim.locations":
            obj.id = 2
        elif resource == "dcim.racks":
            obj.id = 3
        else:
            obj.id = 99
        return obj

    def test_position_zero_is_treated_as_none(self):
        """lowestRackUnit=0 must not be forwarded to NetBox (position >= 0.5 required)."""
        nb = MagicMock()
        nb.upsert.side_effect = self._mock_upsert_side_effect

        runner = self._make_runner(nb)
        result = runner._resolve_placement(
            {"site": "DC1", "rack": "Rack-01", "position": "0"},
            dry_run=False,
        )

        assert result["rack_id"] == 3
        assert result["rack_position"] is None

    def test_position_negative_is_treated_as_none(self):
        """A negative lowestRackUnit must never be forwarded to NetBox."""
        nb = MagicMock()
        nb.upsert.side_effect = self._mock_upsert_side_effect

        runner = self._make_runner(nb)
        result = runner._resolve_placement(
            {"site": "DC1", "rack": "Rack-01", "position": "-1"},
            dry_run=False,
        )

        assert result["rack_id"] == 3
        assert result["rack_position"] is None

    def test_valid_position_is_set_when_rack_resolved(self):
        """A positive rack unit position must be stored when rack_id is present."""
        nb = MagicMock()
        nb.upsert.side_effect = self._mock_upsert_side_effect

        runner = self._make_runner(nb)
        result = runner._resolve_placement(
            {"site": "DC1", "rack": "Rack-01", "position": "5"},
            dry_run=False,
        )

        assert result["rack_id"] == 3
        assert result["rack_position"] == 5

    def test_position_not_set_when_rack_unresolved(self):
        """rack_position must remain None when rack_id could not be resolved."""
        nb = MagicMock()
        nb.upsert.side_effect = self._mock_upsert_side_effect

        runner = self._make_runner(nb)
        # No rack name supplied → rack_id stays None → rack_position must be None
        result = runner._resolve_placement(
            {"site": "DC1", "position": "5"},
            dry_run=False,
        )

        assert result["rack_id"] is None
        assert result["rack_position"] is None

    def test_position_none_input_leaves_rack_position_none(self):
        """Passing position=None explicitly keeps rack_position as None."""
        nb = MagicMock()
        nb.upsert.side_effect = self._mock_upsert_side_effect

        runner = self._make_runner(nb)
        result = runner._resolve_placement(
            {"site": "DC1", "rack": "Rack-01", "position": None},
            dry_run=False,
        )

        assert result["rack_position"] is None

    def test_all_none_when_no_site(self):
        """Without a site, every placement field must be None."""
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._resolve_placement({}, dry_run=False)

        assert result == {
            "site_id": None,
            "location_id": None,
            "rack_id": None,
            "rack_position": None,
        }
        nb.upsert.assert_not_called()
