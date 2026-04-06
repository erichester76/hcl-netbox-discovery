"""Tests for prerequisite methods in collector/prerequisites.py.

Covers:
- _ensure_platform race condition: uniqueness error falls back to GET (Fix 3)
- _resolve_placement: rack_position must be a positive integer and only set
  when rack_id is resolved
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from collector.prerequisites import PrerequisiteArgumentError, PrerequisiteRunner


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


def test_require_text_arg_raises_specific_validation_type():
    nb = MagicMock()
    runner = PrerequisiteRunner(nb)

    with pytest.raises(PrerequisiteArgumentError, match="ensure_site"):
        runner._ensure_site({"name": "   "}, dry_run=True)

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

    def test_dry_run_preserves_placeholder_ids_for_site_location_and_rack(self):
        """Dry-run placement should keep prerequisite identities for downstream lookups."""
        nb = MagicMock()
        runner = self._make_runner(nb)

        result = runner._resolve_placement(
            {
                "site": "DC1",
                "location": "Room 31",
                "rack": "AZ-40",
                "position": "40",
            },
            dry_run=True,
        )

        assert isinstance(result["site_id"], int)
        assert result["site_id"] < 0
        assert isinstance(result["location_id"], int)
        assert result["location_id"] < 0
        assert isinstance(result["rack_id"], int)
        assert result["rack_id"] < 0
        assert result["rack_position"] == 40
        nb.upsert.assert_not_called()

    def test_dry_run_ensure_site_reuses_placeholder_for_same_lookup(self):
        nb = MagicMock()
        runner = self._make_runner(nb)

        first = runner._ensure_site(
            {"name": "Clemson University Information Technology Center (ITC)"},
            dry_run=True,
        )
        second = runner._ensure_site(
            {"name": "Clemson University Information Technology Center (ITC)"},
            dry_run=True,
        )
        different = runner._ensure_site({"name": "Unknown"}, dry_run=True)

        assert isinstance(first, int)
        assert first < 0
        assert second == first
        assert different < 0
        assert different != first
        nb.upsert.assert_not_called()


class TestEnsureTenantGroup:
    """_ensure_tenant_group should upsert tenancy.tenant_groups."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=10)
        runner = self._make_runner(nb)
        result = runner._ensure_tenant_group({"name": "Engineering"}, dry_run=False)
        assert result == 10
        nb.upsert.assert_called_once()
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "tenancy.tenant_groups"
        assert payload["name"] == "Engineering"
        assert payload["slug"] == "engineering"

    def test_returns_none_for_missing_name(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_tenant_group({}, dry_run=False)
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_returns_none_without_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_tenant_group({"name": "Engineering"}, dry_run=True)
        assert result is None
        nb.upsert.assert_not_called()

    def test_description_included_in_payload(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=5)
        runner = self._make_runner(nb)
        runner._ensure_tenant_group(
            {"name": "Engineering", "description": "Eng teams"}, dry_run=False
        )
        payload = nb.upsert.call_args[0][1]
        assert payload["description"] == "Eng teams"


class TestEnsureContactGroup:
    """_ensure_contact_group should upsert tenancy.contact_groups."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=20)
        runner = self._make_runner(nb)
        result = runner._ensure_contact_group({"name": "NOC"}, dry_run=False)
        assert result == 20
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "tenancy.contact_groups"
        assert payload["name"] == "NOC"
        assert payload["slug"] == "noc"

    def test_returns_none_for_missing_name(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_contact_group({}, dry_run=False)
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_returns_none_without_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_contact_group({"name": "NOC"}, dry_run=True)
        assert result is None
        nb.upsert.assert_not_called()


class TestEnsureRegion:
    """_ensure_region should upsert dcim.regions."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=30)
        runner = self._make_runner(nb)
        result = runner._ensure_region({"name": "North America"}, dry_run=False)
        assert result == 30
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "dcim.regions"
        assert payload["name"] == "North America"
        assert payload["slug"] == "north-america"

    def test_returns_none_for_missing_name(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_region({}, dry_run=False)
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_returns_none_without_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_region({"name": "EMEA"}, dry_run=True)
        assert result is None
        nb.upsert.assert_not_called()

    def test_description_included_in_payload(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=31)
        runner = self._make_runner(nb)
        runner._ensure_region({"name": "APAC", "description": "Asia Pacific"}, dry_run=False)
        payload = nb.upsert.call_args[0][1]
        assert payload["description"] == "Asia Pacific"


class TestEnsureVlanGroup:
    """_ensure_vlan_group should upsert ipam.vlan_groups with min/max_vid defaults."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_with_default_vid_range(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=40)
        runner = self._make_runner(nb)
        result = runner._ensure_vlan_group({"name": "Office VLANs"}, dry_run=False)
        assert result == 40
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "ipam.vlan_groups"
        assert payload["name"] == "Office VLANs"
        assert payload["slug"] == "office-vlans"
        assert payload["min_vid"] == 1
        assert payload["max_vid"] == 4094

    def test_custom_vid_range_is_forwarded(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=41)
        runner = self._make_runner(nb)
        runner._ensure_vlan_group({"name": "Core", "min_vid": 100, "max_vid": 200}, dry_run=False)
        payload = nb.upsert.call_args[0][1]
        assert payload["min_vid"] == 100
        assert payload["max_vid"] == 200

    def test_returns_none_for_missing_name(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_vlan_group({}, dry_run=False)
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_returns_none_without_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_vlan_group({"name": "Core"}, dry_run=True)
        assert result is None
        nb.upsert.assert_not_called()


class TestEnsureVrf:
    """_ensure_vrf should upsert ipam.vrfs by name."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=50)
        runner = self._make_runner(nb)
        result = runner._ensure_vrf({"name": "MGMT"}, dry_run=False)
        assert result == 50
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "ipam.vrfs"
        assert payload["name"] == "MGMT"
        assert "rd" not in payload

    def test_rd_included_when_provided(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=51)
        runner = self._make_runner(nb)
        runner._ensure_vrf({"name": "MGMT", "rd": "65000:1"}, dry_run=False)
        payload = nb.upsert.call_args[0][1]
        assert payload["rd"] == "65000:1"

    def test_returns_none_for_missing_name(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_vrf({}, dry_run=False)
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_returns_none_without_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_vrf({"name": "MGMT"}, dry_run=True)
        assert result is None
        nb.upsert.assert_not_called()

    def test_description_included_in_payload(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=52)
        runner = self._make_runner(nb)
        runner._ensure_vrf({"name": "PROD", "description": "Production VRF"}, dry_run=False)
        payload = nb.upsert.call_args[0][1]
        assert payload["description"] == "Production VRF"


class TestRequiredIdentityValidation:
    """Prerequisites that create named objects must reject missing identities."""

    @pytest.mark.parametrize(
        ("method_name", "args", "error_text"),
        [
            ("_ensure_manufacturer", {}, "ensure_manufacturer"),
            ("_ensure_device_type", {"manufacturer": 5}, "ensure_device_type"),
            ("_ensure_device_role", {"name": "   "}, "ensure_device_role"),
            ("_ensure_site", {}, "ensure_site"),
            ("_ensure_cluster_type", {}, "ensure_cluster_type"),
            ("_ensure_cluster_group", {"name": ""}, "ensure_cluster_group"),
            ("_ensure_cluster", {}, "ensure_cluster"),
            ("_ensure_inventory_item_role", {"name": ""}, "ensure_inventory_item_role"),
            ("_ensure_tenant", {}, "ensure_tenant"),
            ("_ensure_module_bay_template", {"device_type": 5}, "ensure_module_bay_template"),
            ("_ensure_module_bay", {"device": 3, "name": "   "}, "ensure_module_bay"),
            ("_ensure_module_type_profile", {}, "ensure_module_type_profile"),
            ("_ensure_module_type", {"manufacturer": 7}, "ensure_module_type"),
        ],
    )
    def test_missing_required_identity_raises_without_writing(
        self,
        method_name,
        args,
        error_text,
    ):
        nb = MagicMock()
        runner = PrerequisiteRunner(nb)

        with pytest.raises(ValueError, match=error_text):
            getattr(runner, method_name)(args, dry_run=False)

        nb.upsert.assert_not_called()

    def test_dry_run_still_validates_required_identity(self):
        nb = MagicMock()
        runner = PrerequisiteRunner(nb)

        with pytest.raises(ValueError, match="ensure_site"):
            runner._ensure_site({"name": "   "}, dry_run=True)

        nb.upsert.assert_not_called()
