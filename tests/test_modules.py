"""Tests for the new module block support.

Covers:
- ModuleConfig dataclass and _parse_modules in collector/config.py
- _ensure_module_bay_template, _ensure_module_bay, _ensure_module_type
  in collector/prerequisites.py
- Engine._process_modules in collector/engine.py
- xclarity-modules.hcl.example parses without error
"""

from __future__ import annotations

import logging
import textwrap
import threading
from concurrent import futures
from pathlib import Path
from unittest.mock import MagicMock, patch

from collector.config import (
    ModuleConfig,
    load_config,
)
from collector.prerequisites import PrerequisiteRunner, load_current_field

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_hcl(tmp_path: Path, body: str) -> str:
    p = tmp_path / "test.hcl"
    p.write_text(textwrap.dedent(body))
    return str(p)


# ---------------------------------------------------------------------------
# ModuleConfig / config parser
# ---------------------------------------------------------------------------


class TestModuleConfigParsing:
    """Verify that module {} blocks inside object {} are parsed correctly."""

    def test_basic_module_block_parsed(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"
                profile      = "CPU"
                enabled_if   = "collector.sync_modules"
                dedupe_by    = "source('socket')"

                field "bay_name"     { value = "source('socket')" }
                field "model"        { value = "source('displayName')" }
                field "serial"       { value = "str(source('serialNumber'))" }
                field "manufacturer" { value = "source('manufacturer')" }
              }
            }
        """)
        cfg = load_config(path)
        obj = cfg.objects[0]
        assert len(obj.modules) == 1
        mod = obj.modules[0]
        assert isinstance(mod, ModuleConfig)
        assert mod.source_items == "processors"
        assert mod.profile == "CPU"
        assert mod.enabled_if == "collector.sync_modules"
        assert mod.dedupe_by == "source('socket')"

    def test_module_block_fields_parsed(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"

                field "bay_name"     { value = "source('socket')" }
                field "position"     { value = "str(source('slot'))" }
                field "model"        { value = "source('displayName')" }
                field "serial"       { value = "str(source('serialNumber'))" }
                field "manufacturer" { value = "source('manufacturer')" }
              }
            }
        """)
        cfg = load_config(path)
        mod = cfg.objects[0].modules[0]
        field_names = [f.name for f in mod.fields]
        assert "bay_name" in field_names
        assert "position" in field_names
        assert "model" in field_names
        assert "serial" in field_names
        assert "manufacturer" in field_names

    def test_multiple_module_blocks_parsed(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"
                profile      = "CPU"
                field "bay_name" { value = "source('socket')" }
                field "model"    { value = "source('displayName')" }
              }

              module {
                source_items = "memoryModules"
                profile      = "Memory"
                field "bay_name" { value = "source('displayName')" }
                field "model"    { value = "source('partNumber')" }
              }
            }
        """)
        cfg = load_config(path)
        modules = cfg.objects[0].modules
        assert len(modules) == 2
        assert modules[0].profile == "CPU"
        assert modules[1].profile == "Memory"

    def test_object_without_modules_has_empty_list(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              field "name" { value = "source('name')" }
            }
        """)
        cfg = load_config(path)
        assert cfg.objects[0].modules == []

    def test_module_optional_attrs_default_to_none(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"
                field "bay_name" { value = "source('socket')" }
                field "model"    { value = "source('displayName')" }
              }
            }
        """)
        cfg = load_config(path)
        mod = cfg.objects[0].modules[0]
        assert mod.profile is None
        assert mod.dedupe_by is None
        assert mod.enabled_if is None
        assert mod.attributes == []

    def test_attribute_sub_blocks_parsed(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"
                profile      = "CPU"

                field "bay_name"     { value = "source('socket')" }
                field "model"        { value = "source('displayName')" }

                attribute "cores"        { value = "int(source('cores')) if source('cores') != None else None" }
                attribute "speed"        { value = "float(source('speed')) if source('speed') != None else None" }
                attribute "architecture" { value = "source('architecture')" }
              }
            }
        """)
        cfg = load_config(path)
        mod = cfg.objects[0].modules[0]
        attr_names = [a.name for a in mod.attributes]
        assert "cores" in attr_names
        assert "speed" in attr_names
        assert "architecture" in attr_names
        assert len(mod.attributes) == 3

    def test_attribute_blocks_independent_of_field_blocks(self, tmp_path):
        """attribute {} and field {} sub-blocks must be parsed into separate lists."""
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"

                field "bay_name" { value = "source('socket')" }
                field "model"    { value = "source('displayName')" }

                attribute "cores" { value = "source('cores')" }
              }
            }
        """)
        cfg = load_config(path)
        mod = cfg.objects[0].modules[0]
        field_names = [f.name for f in mod.fields]
        attr_names = [a.name for a in mod.attributes]
        assert "bay_name" in field_names
        assert "model" in field_names
        assert "cores" not in field_names
        assert "cores" in attr_names
        assert "bay_name" not in attr_names


# ---------------------------------------------------------------------------
# xclarity-modules.hcl.example parses without error
# ---------------------------------------------------------------------------


class TestXclarityModulesHcl:
    """The new mapping file should parse cleanly and contain module blocks."""

    HCL_PATH = "mappings/xclarity-modules.hcl.example"

    def test_parses_without_error(self):
        cfg = load_config(self.HCL_PATH)
        assert cfg is not None

    def test_node_object_has_module_blocks(self):
        cfg = load_config(self.HCL_PATH)
        node = next((o for o in cfg.objects if o.name == "node"), None)
        assert node is not None
        assert len(node.modules) > 0

    def test_node_object_has_no_inventory_items(self):
        cfg = load_config(self.HCL_PATH)
        node = next((o for o in cfg.objects if o.name == "node"), None)
        assert node is not None
        assert node.inventory_items == []

    def test_module_profiles_present(self):
        cfg = load_config(self.HCL_PATH)
        node = next((o for o in cfg.objects if o.name == "node"), None)
        profiles = {m.profile for m in node.modules}
        assert "CPU" in profiles
        assert "Memory" in profiles
        assert "Hard disk" in profiles
        assert "Power supply" in profiles

    def test_hard_disk_dedupe_aligns_with_bay_identity(self):
        cfg = load_config(self.HCL_PATH)
        node = next((o for o in cfg.objects if o.name == "node"), None)
        assert node is not None, "node object should exist in xclarity mapping"
        hd_mod = next((m for m in node.modules if m.profile == "Hard disk"), None)
        assert hd_mod is not None, "Hard disk module block should exist for node"
        assert hd_mod.dedupe_by == "coalesce('name', 'description')"

    def test_sync_modules_flag_in_collector(self):
        cfg = load_config(self.HCL_PATH)
        # sync_modules should be present as an extra_flag
        assert "sync_modules" in cfg.collector.extra_flags

    def test_all_four_device_objects_present(self):
        cfg = load_config(self.HCL_PATH)
        names = {o.name for o in cfg.objects}
        assert {"node", "chassis", "switch", "storage"} <= names

    def _get_module_attr_expr(self, profile: str, attr_name: str):
        """Return an attribute expression from a module block in the example mapping."""
        cfg = load_config(self.HCL_PATH)
        node = next(o for o in cfg.objects if o.name == "node")
        mod = next(m for m in node.modules if m.profile == profile)
        attr = next(a for a in mod.attributes if a.name == attr_name)
        return attr.value

    def _eval_module_attr(self, profile: str, attr_name: str, source_obj):
        """Evaluate a module attribute expression against a source object."""
        from collector.config import CollectorOptions
        from collector.context import RunContext
        from collector.field_resolvers import Resolver

        opts = CollectorOptions(
            max_workers=1,
            dry_run=False,
            sync_tag="test",
            regex_dir="/tmp/regex",
        )
        ctx = RunContext(
            nb=None,
            source_adapter=None,
            collector_opts=opts,
            regex_dir="/tmp/regex",
            prereqs={},
            source_obj=source_obj,
            parent_nb_obj=None,
            dry_run=False,
        )
        expr = self._get_module_attr_expr(profile, attr_name)
        return Resolver(ctx).evaluate(expr)

    def _eval_disk_type(self, source_obj):
        return self._eval_module_attr("Hard disk", "type", source_obj)

    def test_hard_disk_type_hdd_from_media_type(self):
        """When mediaType is 'HDD', the type attribute should be 'HDD'."""
        result = self._eval_disk_type({"mediaType": "HDD"})
        assert result == "HDD"

    def test_hard_disk_type_normalises_rotational_to_hdd(self):
        """When the source reports type='rotational' (and no mediaType), the
        attribute must be normalised to 'HDD' so it does not flip-flop."""
        result = self._eval_disk_type({"type": "rotational"})
        assert result == "HDD"

    def test_hard_disk_type_normalises_flash_to_ssd(self):
        """When the source reports type='flash' (and no mediaType), the
        attribute must be normalised to 'SSD'."""
        result = self._eval_disk_type({"type": "flash"})
        assert result == "SSD"

    def test_hard_disk_type_ssd_from_media_type(self):
        """When mediaType is 'SSD', the type attribute should be 'SSD'."""
        result = self._eval_disk_type({"mediaType": "SSD"})
        assert result == "SSD"

    def test_hard_disk_type_media_type_takes_priority_over_type(self):
        """mediaType should take priority over type when both are present."""
        result = self._eval_disk_type({"mediaType": "HDD", "type": "rotational"})
        assert result == "HDD"

    def test_hard_disk_type_normalises_case_insensitive(self):
        """Normalisation must be case-insensitive ('Rotational' → 'HDD')."""
        result = self._eval_disk_type({"type": "Rotational"})
        assert result == "HDD"

    def test_cpu_speed_zero_is_suppressed(self):
        result = self._eval_module_attr("CPU", "speed", {"speed": 0, "maxSpeedMHZ": 0})
        assert result is None

    def test_memory_size_zero_is_suppressed(self):
        result = self._eval_module_attr("Memory", "size", {"capacity": 0})
        assert result is None

    def test_memory_data_rate_zero_is_suppressed(self):
        result = self._eval_module_attr("Memory", "data_rate", {"speed": 0})
        assert result is None

    def test_hard_disk_size_zero_is_suppressed(self):
        result = self._eval_module_attr("Hard disk", "size", {"capacity": 0})
        assert result is None

    def test_hard_disk_speed_zero_is_suppressed(self):
        result = self._eval_module_attr("Hard disk", "speed", {"rpm": 0})
        assert result is None


# ---------------------------------------------------------------------------
# prerequisites — ensure_module_bay_template
# ---------------------------------------------------------------------------


class TestEnsureModuleBayTemplate:
    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=10)
        runner = self._make_runner(nb)
        result = runner._ensure_module_bay_template(
            {"device_type": 5, "name": "CPU Socket 1", "position": "1"},
            dry_run=False,
        )
        assert result == 10
        nb.upsert.assert_called_once()
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "dcim.module_bay_templates"
        assert payload["device_type"] == 5
        assert payload["name"] == "CPU Socket 1"
        assert payload["position"] == "1"

    def test_returns_none_when_device_type_missing(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_bay_template(
            {"name": "CPU Socket 1"}, dry_run=False
        )
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_skips_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_bay_template(
            {"device_type": 5, "name": "CPU Socket 1"}, dry_run=True
        )
        assert result is None
        nb.upsert.assert_not_called()

    def test_omits_position_when_empty(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=7)
        runner = self._make_runner(nb)
        runner._ensure_module_bay_template(
            {"device_type": 5, "name": "Bay A", "position": ""},
            dry_run=False,
        )
        payload = nb.upsert.call_args[0][1]
        assert "position" not in payload


# ---------------------------------------------------------------------------
# prerequisites — ensure_module_bay
# ---------------------------------------------------------------------------


class TestEnsureModuleBay:
    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=20)
        runner = self._make_runner(nb)
        result = runner._ensure_module_bay(
            {"device": 3, "name": "CPU Socket 1", "position": "1"},
            dry_run=False,
        )
        assert result == 20
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "dcim.module_bays"
        assert payload["device"] == 3
        assert payload["name"] == "CPU Socket 1"

    def test_returns_none_when_device_missing(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_bay({"name": "CPU Socket 1"}, dry_run=False)
        assert result is None
        nb.upsert.assert_not_called()

    def test_dry_run_skips_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_bay(
            {"device": 3, "name": "CPU Socket 1"}, dry_run=True
        )
        assert result is None
        nb.upsert.assert_not_called()

    def test_omits_position_when_empty(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=8)
        runner = self._make_runner(nb)
        runner._ensure_module_bay(
            {"device": 3, "name": "Bay A", "position": ""},
            dry_run=False,
        )
        payload = nb.upsert.call_args[0][1]
        assert "position" not in payload


# ---------------------------------------------------------------------------
# prerequisites — ensure_module_type_profile
# ---------------------------------------------------------------------------


class TestEnsureModuleTypeProfile:
    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=99)
        runner = self._make_runner(nb)
        result = runner._ensure_module_type_profile({"name": "CPU"}, dry_run=False)
        assert result == 99
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "dcim.module_type_profiles"
        assert payload["name"] == "CPU"
        assert payload["slug"] == "cpu"

    def test_slug_derived_from_name(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=100)
        runner = self._make_runner(nb)
        runner._ensure_module_type_profile({"name": "Power supply"}, dry_run=False)
        payload = nb.upsert.call_args[0][1]
        assert payload["slug"] == "power-supply"

    def test_dry_run_skips_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_type_profile({"name": "Fan"}, dry_run=True)
        assert result is None
        nb.upsert.assert_not_called()

    def test_lookup_by_name(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=101)
        runner = self._make_runner(nb)
        runner._ensure_module_type_profile({"name": "Memory"}, dry_run=False)
        kwargs = nb.upsert.call_args[1]
        assert kwargs.get("lookup_fields") == ["name"]


# ---------------------------------------------------------------------------
# prerequisites — ensure_module_type
# ---------------------------------------------------------------------------


class TestEnsureModuleType:
    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=30)
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {"model": "Intel Xeon Gold 6240", "manufacturer": 5},
            dry_run=False,
        )
        assert result == 30
        resource, payload = nb.upsert.call_args[0][:2]
        assert resource == "dcim.module_types"
        assert payload["model"] == "Intel Xeon Gold 6240"
        assert payload["manufacturer"] == 5

    def test_slug_derived_from_model(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=31)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Intel Xeon Gold 6240"},
            dry_run=False,
        )
        payload = nb.upsert.call_args[0][1]
        assert payload["slug"] == "intel-xeon-gold-6240"

    def test_omits_manufacturer_when_not_given(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=32)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4"},
            dry_run=False,
        )
        payload = nb.upsert.call_args[0][1]
        assert "manufacturer" not in payload

    def test_lookup_uses_manufacturer_and_model(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=33)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4", "manufacturer": 7},
            dry_run=False,
        )
        kwargs = nb.upsert.call_args[1]
        lf = kwargs.get("lookup_fields", [])
        assert "manufacturer" in lf
        assert "model" in lf
        assert "slug" not in lf

    def test_lookup_fields_use_model_without_manufacturer(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=34)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4"},
            dry_run=False,
        )
        kwargs = nb.upsert.call_args[1]
        lf = kwargs.get("lookup_fields", [])
        assert "model" in lf
        assert "slug" not in lf

    def test_dry_run_skips_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4"}, dry_run=True
        )
        assert result is None
        nb.upsert.assert_not_called()

    def test_profile_included_in_payload_when_given(self):
        # First upsert call resolves the profile (returns id=99),
        # second upsert call creates/updates the module_type (returns id=34).
        nb = MagicMock()
        nb.upsert.side_effect = [MagicMock(id=99), MagicMock(id=34)]
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {"model": "Intel Xeon Gold 6240", "profile": "CPU"},
            dry_run=False,
        )
        assert result == 34
        # The first upsert should be for the profile
        profile_call = nb.upsert.call_args_list[0]
        assert profile_call[0][0] == "dcim.module_type_profiles"
        assert profile_call[0][1]["name"] == "CPU"
        # The second upsert should be for the module_type with the numeric profile ID
        module_type_call = nb.upsert.call_args_list[1]
        payload = module_type_call[0][1]
        assert payload["profile"] == 99

    def test_profile_omitted_when_not_given(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=35)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4"},
            dry_run=False,
        )
        payload = nb.upsert.call_args[0][1]
        assert "profile" not in payload

    def test_attributes_applied_via_patch_after_upsert(self):
        """Attributes must be written via nb.update (PATCH) after the module
        type upsert so that the profile is committed before attributes are
        validated against it."""
        nb = MagicMock()
        # upsert calls: profile, then module_type
        nb.upsert.side_effect = [MagicMock(id=99), MagicMock(id=50)]
        nb.update.return_value = MagicMock(id=50)
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "profile": "CPU",
                "attributes": {"cores": 16, "speed": 2.5},
            },
            dry_run=False,
        )
        assert result == 50
        # There should be an nb.update call for dcim.module_types with attributes
        update_calls = nb.update.call_args_list
        module_type_attr_calls = [
            c for c in update_calls if c[0][0] == "dcim.module_types"
        ]
        assert len(module_type_attr_calls) == 1
        update_call = module_type_attr_calls[0]
        assert update_call[0][1] == 50
        assert update_call[0][2] == {"attributes": {"cores": 16, "speed": 2.5}}
        # The module_type upsert payload should NOT contain attributes
        module_type_upsert = nb.upsert.call_args_list[1]
        assert "attributes" not in module_type_upsert[0][1]

    def test_attributes_patch_skipped_when_existing_attributes_match(self):
        nb = MagicMock()
        nb.upsert.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50, "attributes": {"cores": 16, "speed": 2.5}},
        ]
        nb.get.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50, "attributes": {"cores": 16, "speed": 2.5}},
        ]
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "profile": "CPU",
                "attributes": {"cores": 16, "speed": 2.5},
            },
            dry_run=False,
        )
        assert result == 50
        nb.update.assert_not_called()
        assert nb.get.call_args_list == [
            (("dcim.module_type_profiles",), {"id": 99}),
            (("dcim.module_types",), {"id": 50}),
        ]

    def test_attributes_patch_skipped_when_refreshed_record_matches(self):
        nb = MagicMock()
        nb.upsert.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50},
        ]
        nb.get.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50, "attributes": {"cores": 16, "speed": 2.5}},
        ]
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "profile": "CPU",
                "attributes": {"cores": 16, "speed": 2.5},
            },
            dry_run=False,
        )
        assert result == 50
        module_type_updates = [
            call for call in nb.update.call_args_list if call[0][0] == "dcim.module_types"
        ]
        assert module_type_updates == []
        assert nb.get.call_args_list == [
            (("dcim.module_type_profiles",), {"id": 99}),
            (("dcim.module_types",), {"id": 50}),
        ]

    def test_attributes_patch_skipped_when_refreshed_record_serializes_to_match(self):
        class SerializableValue:
            def __init__(self, value):
                self._value = value

            def serialize(self):
                return self._value

        nb = MagicMock()
        nb.upsert.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50},
        ]
        nb.get.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50, "attributes": SerializableValue({"speed": 2.5, "cores": 16})},
        ]
        runner = self._make_runner(nb)

        result = runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "profile": "CPU",
                "attributes": {"cores": 16, "speed": 2.5},
            },
            dry_run=False,
        )

        assert result == 50
        module_type_updates = [
            call for call in nb.update.call_args_list if call[0][0] == "dcim.module_types"
        ]
        assert module_type_updates == []

    def test_live_field_refresh_is_cached_per_run(self):
        nb = MagicMock()
        nb.upsert.side_effect = [
            {"id": 99},
            {"id": 50},
            {"id": 99},
            {"id": 50},
        ]
        nb.get.side_effect = [
            {"id": 99, "schema": {"type": "object", "properties": {"cores": {}, "speed": {}}}},
            {"id": 50, "attributes": {"cores": 16, "speed": 2.5}},
        ]
        runner = self._make_runner(nb)

        args = {
            "model": "Intel Xeon Gold 6240",
            "profile": "CPU",
            "attributes": {"cores": 16, "speed": 2.5},
        }
        assert runner._ensure_module_type(args, dry_run=False) == 50
        assert runner._ensure_module_type(args, dry_run=False) == 50

        assert nb.get.call_args_list == [
            (("dcim.module_type_profiles",), {"id": 99}),
            (("dcim.module_types",), {"id": 50}),
        ]
        nb.update.assert_not_called()

    def test_failed_refresh_fallback_is_not_cached(self):
        nb = MagicMock()
        nb.get.side_effect = [
            Exception("temporary refresh failure"),
            {"id": 50, "attributes": {"cores": 16, "speed": 2.5}},
        ]
        runner = self._make_runner(nb)

        first = runner._load_live_field(
            "dcim.module_types",
            50,
            {"id": 50},
            "attributes",
        )
        second = runner._load_live_field(
            "dcim.module_types",
            50,
            {"id": 50},
            "attributes",
        )

        assert first is None
        assert second == {"cores": 16, "speed": 2.5}
        assert nb.get.call_count == 2

    def test_load_current_field_bypasses_cache_when_supported(self):
        calls: list[tuple[str, bool, int]] = []

        class FakeNetBox:
            def get(self, resource, use_cache=True, **kwargs):
                calls.append((resource, use_cache, kwargs["id"]))
                return {"id": kwargs["id"], "attributes": {"cores": 16, "speed": 2.5}}

        value = load_current_field(
            FakeNetBox(),
            "dcim.module_types",
            50,
            {"id": 50},
            "attributes",
        )

        assert value == {"cores": 16, "speed": 2.5}
        assert calls == [("dcim.module_types", False, 50)]

    def test_attributes_not_called_when_empty(self):
        """When attributes dict is empty/None, nb.update should not be called."""
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=36)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4", "attributes": {}},
            dry_run=False,
        )
        nb.update.assert_not_called()

    def test_attributes_not_called_when_none(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=37)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4", "attributes": None},
            dry_run=False,
        )
        nb.update.assert_not_called()

    def test_none_attribute_values_filtered_out(self):
        """Attribute keys with None values should be excluded from the PATCH."""
        nb = MagicMock()
        nb.upsert.side_effect = [MagicMock(id=99), MagicMock(id=51)]
        nb.update.return_value = MagicMock(id=51)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "profile": "CPU",
                "attributes": {"cores": 16, "speed": None, "architecture": "x86"},
            },
            dry_run=False,
        )
        update_attrs = nb.update.call_args[0][2]["attributes"]
        assert "speed" not in update_attrs
        assert update_attrs["cores"] == 16
        assert update_attrs["architecture"] == "x86"

    def test_profile_schema_set_when_attributes_provided(self):
        """When attributes are specified, a schema is attached to the profile
        to prevent NetBox from wiping attribute values on every save."""
        nb = MagicMock()
        nb.upsert.side_effect = [MagicMock(id=99), MagicMock(id=52)]
        nb.update.return_value = MagicMock(id=52)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "profile": "CPU",
                "attributes": {"cores": 16},
            },
            dry_run=False,
        )
        # nb.update should be called at least twice:
        #   once for the profile schema, once for module_type attributes
        assert nb.update.call_count >= 1
        update_calls = nb.update.call_args_list
        profile_schema_calls = [
            c for c in update_calls
            if c[0][0] == "dcim.module_type_profiles"
        ]
        assert len(profile_schema_calls) == 1
        schema_payload = profile_schema_calls[0][0][2]
        assert "schema" in schema_payload
        schema = schema_payload["schema"]
        assert schema["type"] == "object"
        assert "cores" in schema["properties"]

    def test_dry_run_includes_attributes_in_log(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {
                "model": "Intel Xeon Gold 6240",
                "attributes": {"cores": 16},
            },
            dry_run=True,
        )
        assert result is None
        nb.upsert.assert_not_called()
        nb.update.assert_not_called()


# ---------------------------------------------------------------------------
# prerequisites — ensure_module_type_profile (schema support)
# ---------------------------------------------------------------------------


class TestEnsureModuleTypeProfileSchema:
    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_schema_applied_via_update_when_provided(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=10)
        nb.update.return_value = MagicMock(id=10)
        runner = self._make_runner(nb)
        schema = {"type": "object", "properties": {"cores": {}}}
        nb.get.return_value = {"id": 10}
        runner._ensure_module_type_profile(
            {"name": "CPU", "schema": schema},
            dry_run=False,
        )
        nb.update.assert_called_once()
        update_call = nb.update.call_args
        assert update_call[0][0] == "dcim.module_type_profiles"
        assert update_call[0][1] == 10
        assert update_call[0][2]["schema"] == schema

    def test_schema_update_skipped_when_existing_schema_matches(self):
        nb = MagicMock()
        schema = {"type": "object", "properties": {"cores": {}}}
        nb.upsert.return_value = {"id": 10, "schema": schema}
        nb.get.return_value = {"id": 10, "schema": schema}
        runner = self._make_runner(nb)
        result = runner._ensure_module_type_profile(
            {"name": "CPU", "schema": schema},
            dry_run=False,
        )
        assert result == 10
        nb.update.assert_not_called()
        nb.get.assert_called_once_with("dcim.module_type_profiles", id=10)

    def test_schema_update_skipped_when_refreshed_record_matches(self):
        nb = MagicMock()
        schema = {"type": "object", "properties": {"cores": {}}}
        nb.upsert.return_value = {"id": 10}
        nb.get.return_value = {"id": 10, "schema": schema}
        runner = self._make_runner(nb)
        result = runner._ensure_module_type_profile(
            {"name": "CPU", "schema": schema},
            dry_run=False,
        )
        assert result == 10
        nb.update.assert_not_called()
        nb.get.assert_called_once_with("dcim.module_type_profiles", id=10)

    def test_schema_update_skipped_when_refreshed_record_serializes_to_match(self):
        class SerializableValue:
            def __init__(self, value):
                self._value = value

            def serialize(self):
                return self._value

        nb = MagicMock()
        schema = {"type": "object", "properties": {"cores": {}}}
        nb.upsert.return_value = {"id": 10}
        nb.get.return_value = {"id": 10, "schema": SerializableValue(schema)}
        runner = self._make_runner(nb)

        result = runner._ensure_module_type_profile(
            {"name": "CPU", "schema": schema},
            dry_run=False,
        )

        assert result == 10
        nb.update.assert_not_called()

    def test_schema_refresh_and_patch_are_singleflight_per_profile(self):
        nb = MagicMock()
        schema = {"type": "object", "properties": {"cores": {}}}
        nb.upsert.return_value = {"id": 10}

        counters = {"get": 0, "update": 0}
        counters_lock = threading.Lock()
        start_event = threading.Event()
        entered_get = threading.Event()
        release_get = threading.Event()

        def fake_get(resource, **kwargs):
            assert resource == "dcim.module_type_profiles"
            assert kwargs == {"id": 10}
            with counters_lock:
                counters["get"] += 1
            entered_get.set()
            assert release_get.wait(timeout=1)
            return {"id": 10}

        def fake_update(resource, object_id, payload):
            assert resource == "dcim.module_type_profiles"
            assert object_id == 10
            assert payload == {"schema": schema}
            with counters_lock:
                counters["update"] += 1
            return {"id": 10}

        nb.get.side_effect = fake_get
        nb.update.side_effect = fake_update

        runner = self._make_runner(nb)

        def run_once():
            assert start_event.wait(timeout=1)
            return runner._ensure_module_type_profile(
                {"name": "CPU", "schema": schema},
                dry_run=False,
            )

        with futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_results = [executor.submit(run_once) for _ in range(4)]
            start_event.set()
            assert entered_get.wait(timeout=1)
            release_get.set()
            results = [future.result(timeout=2) for future in future_results]

        assert results == [10, 10, 10, 10]
        assert counters == {"get": 1, "update": 1}

    def test_schema_auto_generated_from_attribute_names(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=11)
        nb.update.return_value = MagicMock(id=11)
        runner = self._make_runner(nb)
        runner._ensure_module_type_profile(
            {"name": "CPU", "attribute_names": ["cores", "speed", "architecture"]},
            dry_run=False,
        )
        nb.update.assert_called_once()
        schema = nb.update.call_args[0][2]["schema"]
        assert schema["type"] == "object"
        assert "cores" in schema["properties"]
        assert "speed" in schema["properties"]
        assert "architecture" in schema["properties"]

    def test_no_update_when_no_schema_and_no_attribute_names(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=12)
        runner = self._make_runner(nb)
        runner._ensure_module_type_profile({"name": "CPU"}, dry_run=False)
        nb.update.assert_not_called()

    def test_dry_run_skips_both_upsert_and_update(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_type_profile(
            {"name": "CPU", "schema": {"type": "object"}},
            dry_run=True,
        )
        assert result is None
        nb.upsert.assert_not_called()
        nb.update.assert_not_called()


class TestProcessModules:
    """Unit-tests for Engine._process_modules."""

    def _make_engine(self):
        from collector.engine import Engine
        return Engine()

    def _make_ctx(self, source_obj, dry_run=False, extra_flags=None):
        from collector.config import CollectorOptions
        from collector.context import RunContext

        opts = CollectorOptions(
            max_workers=1,
            dry_run=dry_run,
            sync_tag="test-sync",
            regex_dir="/tmp/regex",
            extra_flags=extra_flags or {"sync_modules": True},
        )
        nb = MagicMock()
        ctx = RunContext(
            nb=nb,
            source_adapter=None,
            collector_opts=opts,
            regex_dir="/tmp/regex",
            prereqs={},
            source_obj=source_obj,
            parent_nb_obj=None,
            dry_run=dry_run,
        )
        return ctx

    def _make_obj_cfg_with_module(self, source_items="processors", enabled_if=None):
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig
        mod_cfg = ModuleConfig(
            source_items=source_items,
            profile="CPU",
            enabled_if=enabled_if,
            fields=[
                FieldConfig(name="bay_name", value="source('socket')"),
                FieldConfig(name="model", value="source('displayName')"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
                FieldConfig(name="manufacturer", value="source('manufacturer')"),
            ],
        )
        return ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )

    def test_upserts_module_for_each_source_item(self):
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                    "manufacturer": "Intel",
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        # Manufacturer upsert
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type
            MagicMock(id=40),  # upsert module
        ]

        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_obj_cfg_with_module()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        # At least one call to upsert dcim.modules
        module_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.modules"
        ]
        assert len(module_calls) == 1
        module_payload = module_calls[0][0][1]
        assert module_payload["device"] == 99
        assert module_payload["status"] == "active"
        assert module_payload["serial"] == "ABC123"

    def test_skips_item_when_bay_name_missing(self):
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": None,
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.return_value = MagicMock(id=1)

        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_obj_cfg_with_module()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        # No dcim.modules upsert when bay_name is missing
        module_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.modules"
        ]
        assert len(module_calls) == 0

    def test_invalid_model_expression_skips_module_before_writes(self, caplog):
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig

        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb

        mod_cfg = ModuleConfig(
            source_items="processors",
            profile="CPU",
            fields=[
                FieldConfig(name="bay_name", value="source('socket')"),
                FieldConfig(name="model", value="undefined_func()"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
            ],
        )
        obj_cfg = ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}

        with caplog.at_level(logging.WARNING):
            engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        assert "Skipping module item due to required field error" in caplog.text
        nb.upsert.assert_not_called()

    def test_module_bay_failure_logs_warning_and_skips_install(self, caplog):
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                    "manufacturer": "Intel",
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=10),  # ensure_module_bay_template
        ]

        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_obj_cfg_with_module()

        with patch.object(
            PrerequisiteRunner,
            "_ensure_module_bay",
            side_effect=Exception("module bay failed"),
        ), caplog.at_level(logging.WARNING):
            engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        assert "ensure_module_bay 'CPU Socket 1' failed: module bay failed" in caplog.text
        assert "Could not obtain module_bay for 'CPU Socket 1'" in caplog.text
        module_calls = [c for c in nb.upsert.call_args_list if c[0][0] == "dcim.modules"]
        assert len(module_calls) == 0

    def test_module_type_failure_logs_warning_and_skips_install(self, caplog):
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                    "manufacturer": "Intel",
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
        ]

        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_obj_cfg_with_module()

        with patch.object(
            PrerequisiteRunner,
            "_ensure_module_type",
            side_effect=Exception("module type failed"),
        ), caplog.at_level(logging.WARNING):
            engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        assert "ensure_module_type 'Intel Xeon Gold 6240' failed: module type failed" in caplog.text
        assert "Could not obtain module_type for 'Intel Xeon Gold 6240'" in caplog.text
        module_calls = [c for c in nb.upsert.call_args_list if c[0][0] == "dcim.modules"]
        assert len(module_calls) == 0

    def test_dry_run_logs_without_writing(self):
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                }
            ]
        }
        ctx = self._make_ctx(source_obj, dry_run=True)
        nb = ctx.nb

        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_obj_cfg_with_module()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        nb.upsert.assert_not_called()

    def test_no_modules_config_exits_early(self):
        from collector.config import ObjectConfig
        engine = self._make_engine()
        source_obj = {}
        ctx = self._make_ctx(source_obj)
        obj_cfg = ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[],
        )
        parent_nb_obj = {"id": 99}
        engine._process_modules(obj_cfg, parent_nb_obj, ctx)
        ctx.nb.upsert.assert_not_called()

    def test_enabled_if_false_skips_processing(self):
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                }
            ]
        }
        # sync_modules=False → enabled_if evaluates to False
        ctx = self._make_ctx(source_obj, extra_flags={"sync_modules": False})
        nb = ctx.nb
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_obj_cfg_with_module(enabled_if="collector.sync_modules")

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        module_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.modules"
        ]
        assert len(module_calls) == 0

    def test_deduplication_by_serial(self):
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {"socket": "CPU 1", "displayName": "Xeon 6240", "serialNumber": "SAME"},
                {"socket": "CPU 2", "displayName": "Xeon 6240", "serialNumber": "SAME"},
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.return_value = MagicMock(id=1)

        mod_cfg = ModuleConfig(
            source_items="processors",
            dedupe_by="source('serialNumber')",
            fields=[
                FieldConfig(name="bay_name", value="source('socket')"),
                FieldConfig(name="model", value="source('displayName')"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
            ],
        )
        obj_cfg = ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        module_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.modules"
        ]
        # Only one module should be installed (second is deduplicated)
        assert len(module_calls) == 1


# ---------------------------------------------------------------------------
# PowerInputConfig / config parser
# ---------------------------------------------------------------------------


class TestPowerInputConfigParsing:
    """Verify that power_input {} sub-blocks inside module {} are parsed."""

    def test_power_input_parsed(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "powerSupplies"
                profile      = "Power supply"

                field "bay_name" { value = "source('name')" }
                field "model"    { value = "source('partNumber')" }

                power_input {
                  name = "'Power Input' + when(source('slot'), ' ' + str(source('slot')), '')"
                  type = "when(int(source('outputWatts') or 0) > 1800, 'iec-60320-c20', 'iec-60320-c14')"
                }
              }
            }
        """)
        cfg = load_config(path)
        mod = cfg.objects[0].modules[0]
        from collector.config import PowerInputConfig
        assert mod.power_input is not None
        assert isinstance(mod.power_input, PowerInputConfig)
        assert "Power Input" in mod.power_input.name
        assert "iec-60320-c14" in mod.power_input.type

    def test_module_without_power_input_has_none(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            object "node" {
              source_collection = "nodes"
              netbox_resource   = "dcim.devices"

              module {
                source_items = "processors"
                field "bay_name" { value = "source('socket')" }
                field "model"    { value = "source('displayName')" }
              }
            }
        """)
        cfg = load_config(path)
        assert cfg.objects[0].modules[0].power_input is None

    def test_xclarity_power_supply_module_has_power_input(self):
        cfg = load_config("mappings/xclarity-modules.hcl.example")
        node = next(o for o in cfg.objects if o.name == "node")
        psu_mod = next(m for m in node.modules if m.profile == "Power supply")
        assert psu_mod.power_input is not None
        assert psu_mod.power_input.name is not None
        assert psu_mod.power_input.type is not None

    def test_xclarity_non_psu_modules_have_no_power_input(self):
        cfg = load_config("mappings/xclarity-modules.hcl.example")
        node = next(o for o in cfg.objects if o.name == "node")
        non_psu = [m for m in node.modules if m.profile != "Power supply"]
        assert len(non_psu) > 0
        for mod in non_psu:
            assert mod.power_input is None, f"Module {mod.profile!r} should not have power_input"


# ---------------------------------------------------------------------------
# Engine._process_modules — power input port creation
# ---------------------------------------------------------------------------


class TestProcessModulesPowerInput:
    """Tests for power port creation via the power_input {} sub-block."""

    _DEFAULT_PI_NAME = "'Power Input ' + str(source('slot') or source('name') or source('description'))"
    _DEFAULT_PI_TYPE = (
        "when(int(coalesce(source('outputWatts'), source('powerAllocation.totalOutputPower')) or 0)"
        " > 1800, 'iec-60320-c20', 'iec-60320-c14')"
    )

    def _make_engine(self):
        from collector.engine import Engine
        return Engine()

    def _make_ctx(self, source_obj, dry_run=False):
        from collector.config import CollectorOptions
        from collector.context import RunContext

        opts = CollectorOptions(
            max_workers=1,
            dry_run=dry_run,
            sync_tag="test-sync",
            regex_dir="/tmp/regex",
            extra_flags={"sync_modules": True},
        )
        nb = MagicMock()
        return RunContext(
            nb=nb,
            source_adapter=None,
            collector_opts=opts,
            regex_dir="/tmp/regex",
            prereqs={},
            source_obj=source_obj,
            parent_nb_obj=None,
            dry_run=dry_run,
        )

    def _make_psu_obj_cfg(self, pi_name=None, pi_type=None):
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig, PowerInputConfig
        mod_cfg = ModuleConfig(
            source_items="powerSupplies",
            profile="Power supply",
            enabled_if="collector.sync_modules",
            fields=[
                FieldConfig(name="bay_name", value="source('name')"),
                FieldConfig(name="model", value="source('partNumber')"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
                FieldConfig(name="manufacturer", value="source('manufacturer')"),
                FieldConfig(name="position", value="str(source('slot'))"),
            ],
            power_input=PowerInputConfig(
                name=pi_name or self._DEFAULT_PI_NAME,
                type=pi_type or self._DEFAULT_PI_TYPE,
            ),
        )
        return ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )

    def test_power_port_created_after_module_install(self):
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "Power Supply 1",
                    "partNumber": "SP57A01228",
                    "serialNumber": "PSU001",
                    "manufacturer": "Lenovo",
                    "slot": "1",
                    "outputWatts": 900,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type_profile ("Power supply")
            MagicMock(id=40),  # ensure_module_type
            MagicMock(id=50),  # upsert module
            MagicMock(id=60),  # upsert power_port
        ]
        nb.update.return_value = MagicMock(id=40)
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 1
        pp_payload = power_port_calls[0][0][1]
        assert pp_payload["device"] == 99
        assert pp_payload["module"] == 50
        assert pp_payload["name"] == "Power Input 1"
        assert pp_payload["type"] == "iec-60320-c14"

    def test_power_port_lookup_uses_device_and_name_only(self):
        """Power port upsert must use lookup_fields=['device', 'name'] so that
        existing ports are found even when the module ID changes (e.g. after a
        module reinstall).  Using 'module' in the lookup would fail to match the
        old record and then hit NetBox's (device, name) unique constraint."""
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "Power Supply 1",
                    "partNumber": "SP57A01228",
                    "serialNumber": "PSU001",
                    "manufacturer": "Lenovo",
                    "slot": "1",
                    "outputWatts": 900,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type_profile ("Power supply")
            MagicMock(id=40),  # ensure_module_type
            MagicMock(id=50),  # upsert module
            MagicMock(id=60),  # upsert power_port
        ]
        nb.update.return_value = MagicMock(id=40)
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 1
        # lookup_fields must NOT include 'module' — NetBox's unique constraint
        # is (device, name); including module causes misses when the module
        # record is re-created with a new ID.
        lookup_fields = power_port_calls[0][1].get("lookup_fields") or power_port_calls[0][0][2]
        assert "module" not in lookup_fields
        assert "device" in lookup_fields
        assert "name" in lookup_fields

    def test_attribute_fields_evaluated_and_passed_to_ensure_module_type(self):
        """Attribute sub-blocks are evaluated per source item and forwarded to
        _ensure_module_type as the ``attributes`` dict, which then applies them
        via a PATCH after profile assignment."""
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                    "manufacturer": "Intel",
                    "cores": 16,
                    "speed": 2.5,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=99),  # ensure_module_type_profile
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type
            MagicMock(id=40),  # upsert module
        ]
        nb.update.return_value = MagicMock(id=30)

        mod_cfg = ModuleConfig(
            source_items="processors",
            profile="CPU",
            fields=[
                FieldConfig(name="bay_name", value="source('socket')"),
                FieldConfig(name="model", value="source('displayName')"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
                FieldConfig(name="manufacturer", value="source('manufacturer')"),
            ],
            attributes=[
                FieldConfig(name="cores", value="source('cores')"),
                FieldConfig(name="speed", value="source('speed')"),
            ],
        )
        obj_cfg = ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        # nb.update should have been called for attributes on the module type
        update_calls = nb.update.call_args_list
        module_type_attr_calls = [
            c for c in update_calls
            if c[0][0] == "dcim.module_types"
        ]
        assert len(module_type_attr_calls) == 1
        attrs_payload = module_type_attr_calls[0][0][2]["attributes"]
        assert attrs_payload["cores"] == 16
        assert attrs_payload["speed"] == 2.5

    def test_high_wattage_psu_gets_c20_port(self):
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "Power Supply 2",
                    "partNumber": "SP57A02000",
                    "serialNumber": "PSU002",
                    "manufacturer": "Lenovo",
                    "slot": "2",
                    "outputWatts": 2000,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),
            MagicMock(id=10),
            MagicMock(id=20),
            MagicMock(id=30),  # ensure_module_type_profile
            MagicMock(id=40),  # ensure_module_type
            MagicMock(id=50),  # upsert module
            MagicMock(id=60),  # upsert power_port
        ]
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 1
        assert power_port_calls[0][0][1]["type"] == "iec-60320-c20"

    def test_power_port_name_without_slot(self):
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "PSU",
                    "partNumber": "SP57A01228",
                    "serialNumber": "PSU003",
                    "manufacturer": "Lenovo",
                    "slot": None,
                    "outputWatts": 900,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),
            MagicMock(id=10),
            MagicMock(id=20),
            MagicMock(id=30),  # ensure_module_type_profile
            MagicMock(id=40),  # ensure_module_type
            MagicMock(id=50),  # upsert module
            MagicMock(id=60),  # upsert power_port
        ]
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 1
        # With no slot, name falls back to the PSU name field
        assert power_port_calls[0][0][1]["name"] == "Power Input PSU"

    def test_two_psus_without_slot_get_unique_names(self):
        """Two PSUs with no slot number must each create their own power port."""
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "Power Supply 1",
                    "partNumber": "SP57A01228",
                    "serialNumber": "PSU001",
                    "manufacturer": "Lenovo",
                    "slot": None,
                    "outputWatts": 900,
                },
                {
                    "name": "Power Supply 2",
                    "partNumber": "SP57A01228",
                    "serialNumber": "PSU002",
                    "manufacturer": "Lenovo",
                    "slot": None,
                    "outputWatts": 900,
                },
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),    # ensure_manufacturer (PSU 1)
            MagicMock(id=10),   # ensure_module_bay_template (PSU 1)
            MagicMock(id=20),   # ensure_module_bay (PSU 1)
            MagicMock(id=30),   # ensure_module_type_profile (PSU 1)
            MagicMock(id=40),   # ensure_module_type (PSU 1)
            MagicMock(id=50),   # upsert module (PSU 1)
            MagicMock(id=60),   # upsert power_port (PSU 1)
            MagicMock(id=2),    # ensure_manufacturer (PSU 2)
            MagicMock(id=11),   # ensure_module_bay_template (PSU 2)
            MagicMock(id=21),   # ensure_module_bay (PSU 2)
            MagicMock(id=31),   # ensure_module_type_profile (PSU 2)
            MagicMock(id=41),   # ensure_module_type (PSU 2)
            MagicMock(id=51),   # upsert module (PSU 2)
            MagicMock(id=61),   # upsert power_port (PSU 2)
        ]
        nb.update.return_value = MagicMock(id=40)
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 2, (
            f"Expected one power port per PSU; got {len(power_port_calls)}"
        )
        names = {c[0][1]["name"] for c in power_port_calls}
        assert names == {"Power Input Power Supply 1", "Power Input Power Supply 2"}

    def test_no_power_port_when_module_upsert_returns_none(self):
        """If module install fails (returns None), no power port is created."""
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "PSU",
                    "partNumber": "SP57A01228",
                    "serialNumber": "PSU001",
                    "slot": "1",
                    "outputWatts": 900,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=10),  # ensure_module_bay_template (no manufacturer, skip)
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type_profile
            MagicMock(id=40),  # ensure_module_type
            None,              # upsert module fails
        ]
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 0

    def test_no_power_input_config_means_no_power_port(self):
        """Modules without power_input config do not create power ports."""
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "ABC123",
                    "manufacturer": "Intel",
                    "cores": 16,
                    "speed": 2.5,
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        # Calls: ensure_manufacturer, ensure_module_type_profile (upsert),
        #        ensure_module_bay_template, ensure_module_bay,
        #        ensure_module_type (upsert), dcim.modules (upsert)
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=99),  # ensure_module_type_profile
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type
            MagicMock(id=40),  # upsert module
        ]
        nb.update.return_value = MagicMock(id=30)

        mod_cfg = ModuleConfig(
            source_items="processors",
            profile="CPU",
            fields=[
                FieldConfig(name="bay_name", value="source('socket')"),
                FieldConfig(name="model", value="source('displayName')"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
                FieldConfig(name="manufacturer", value="source('manufacturer')"),
            ],
        )
        obj_cfg = ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 0

    def test_none_attribute_values_not_forwarded(self):
        """Attribute fields that evaluate to None are not included in the
        attributes dict passed to _ensure_module_type."""
        from collector.config import FieldConfig, ModuleConfig, ObjectConfig
        engine = self._make_engine()
        source_obj = {
            "processors": [
                {
                    "socket": "CPU Socket 1",
                    "displayName": "Intel Xeon Gold 6240",
                    "serialNumber": "CPU001",
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),   # ensure_manufacturer
            MagicMock(id=99),  # ensure_module_type_profile (when called)
            MagicMock(id=10),  # ensure_module_bay_template
            MagicMock(id=20),  # ensure_module_bay
            MagicMock(id=30),  # ensure_module_type
            MagicMock(id=40),  # upsert module
        ]
        nb.update.return_value = MagicMock(id=30)

        mod_cfg = ModuleConfig(
            source_items="processors",
            profile="CPU",
            fields=[
                FieldConfig(name="bay_name", value="source('socket')"),
                FieldConfig(name="model", value="source('displayName')"),
                FieldConfig(name="serial", value="str(source('serialNumber'))"),
            ],
            power_input=None,
            attributes=[
                FieldConfig(name="cores", value="source('cores')"),
            ],
        )
        obj_cfg = ObjectConfig(
            name="node",
            source_collection="nodes",
            netbox_resource="dcim.devices",
            modules=[mod_cfg],
        )
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 0

    def test_wattage_from_power_allocation_nested_path(self):
        """outputWatts from nested powerAllocation.totalOutputPower triggers c20."""
        engine = self._make_engine()
        source_obj = {
            "powerSupplies": [
                {
                    "name": "PSU",
                    "partNumber": "SP57A02000",
                    "serialNumber": "PSU004",
                    "manufacturer": "Lenovo",
                    "slot": "1",
                    "powerAllocation": {"totalOutputPower": 2400},
                }
            ]
        }
        ctx = self._make_ctx(source_obj)
        nb = ctx.nb
        nb.upsert.side_effect = [
            MagicMock(id=1),
            MagicMock(id=10),
            MagicMock(id=20),
            MagicMock(id=30),  # ensure_module_type_profile
            MagicMock(id=40),  # ensure_module_type
            MagicMock(id=50),  # upsert module
            MagicMock(id=60),  # upsert power_port
        ]
        parent_nb_obj = {"id": 99, "device_type": {"id": 5}}
        obj_cfg = self._make_psu_obj_cfg()

        engine._process_modules(obj_cfg, parent_nb_obj, ctx)

        power_port_calls = [
            c for c in nb.upsert.call_args_list
            if c[0][0] == "dcim.power_ports"
        ]
        assert len(power_port_calls) == 1
        assert power_port_calls[0][0][1]["type"] == "iec-60320-c20"
        # With all attribute values being None, nb.update for module_types
        # should not be called (no clean attributes to write).
        module_type_attr_calls = [
            c for c in nb.update.call_args_list
            if c[0][0] == "dcim.module_types"
        ]
        assert len(module_type_attr_calls) == 0

    def test_xclarity_modules_hcl_has_attribute_blocks(self):
        """The xclarity-modules.hcl.example mapping file should have attribute blocks
        on its CPU, Memory, Hard disk, Expansion card, and Power supply
        module blocks."""
        from collector.config import load_config
        cfg = load_config("mappings/xclarity-modules.hcl.example")
        node = next(o for o in cfg.objects if o.name == "node")
        profile_to_attrs: dict = {
            m.profile: [a.name for a in m.attributes]
            for m in node.modules
            if m.profile is not None
        }
        # CPU must have cores, speed, architecture
        assert "cores" in profile_to_attrs.get("CPU", [])
        assert "speed" in profile_to_attrs.get("CPU", [])
        assert "architecture" in profile_to_attrs.get("CPU", [])
        # Memory must have size, class, data_rate, ecc
        assert "size" in profile_to_attrs.get("Memory", [])
        assert "class" in profile_to_attrs.get("Memory", [])
        assert "data_rate" in profile_to_attrs.get("Memory", [])
        assert "ecc" in profile_to_attrs.get("Memory", [])
        # Hard disk must have size, speed, type
        assert "size" in profile_to_attrs.get("Hard disk", [])
        assert "type" in profile_to_attrs.get("Hard disk", [])
        # Expansion card must have connector_type
        assert "connector_type" in profile_to_attrs.get("Expansion card", [])
        # Power supply must have input_current, input_voltage
        assert "input_current" in profile_to_attrs.get("Power supply", [])
        assert "input_voltage" in profile_to_attrs.get("Power supply", [])
        # Fan intentionally has no attribute blocks because XClarity reports
        # live tachometer values rather than stable hardware characteristics.
        assert profile_to_attrs.get("Fan", []) == []
