"""Tests for the new module block support.

Covers:
- ModuleConfig dataclass and _parse_modules in collector/config.py
- _ensure_module_bay_template, _ensure_module_bay, _ensure_module_type
  in collector/prerequisites.py
- Engine._process_modules in collector/engine.py
- xclarity-modules.hcl parses without error
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from collector.config import (
    ModuleConfig,
    load_config,
)
from collector.prerequisites import PrerequisiteRunner


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


# ---------------------------------------------------------------------------
# xclarity-modules.hcl parses without error
# ---------------------------------------------------------------------------


class TestXclarityModulesHcl:
    """The new mapping file should parse cleanly and contain module blocks."""

    HCL_PATH = "mappings/xclarity-modules.hcl"

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

    def test_sync_modules_flag_in_collector(self):
        cfg = load_config(self.HCL_PATH)
        # sync_modules should be present as an extra_flag
        assert "sync_modules" in cfg.collector.extra_flags

    def test_all_four_device_objects_present(self):
        cfg = load_config(self.HCL_PATH)
        names = {o.name for o in cfg.objects}
        assert {"node", "chassis", "switch", "storage"} <= names


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

    def test_lookup_fields_include_manufacturer_when_given(self):
        nb = MagicMock()
        nb.upsert.return_value = MagicMock(id=33)
        runner = self._make_runner(nb)
        runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4", "manufacturer": 7},
            dry_run=False,
        )
        kwargs = nb.upsert.call_args[1]
        assert "manufacturer" in kwargs.get("lookup_fields", [])

    def test_dry_run_skips_upsert(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_module_type(
            {"model": "Samsung 32GB DDR4"}, dry_run=True
        )
        assert result is None
        nb.upsert.assert_not_called()


# ---------------------------------------------------------------------------
# Engine._process_modules
# ---------------------------------------------------------------------------


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
