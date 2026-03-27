"""Tests for the HCL config parser (collector/config.py)."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from collector.config import (
    CollectionConfig,
    CollectorConfig,
    CollectorOptions,
    FieldConfig,
    NetBoxConfig,
    ObjectConfig,
    PrerequisiteConfig,
    SourceConfig,
    _bool,
    _eval_config_str,
    _int,
    _labeled_list,
    _unlabeled_list,
    load_config,
)


# ---------------------------------------------------------------------------
# Unit helpers
# ---------------------------------------------------------------------------


class TestBool:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("True", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("0", False),
            ("no", False),
            ("off", False),
            ("", False),
            (None, False),
            (42, False),  # non-bool, non-string → default
        ],
    )
    def test_bool(self, value, expected):
        assert _bool(value) == expected


class TestInt:
    def test_int_from_string(self):
        assert _int("8") == 8

    def test_int_from_int(self):
        assert _int(4) == 4

    def test_int_bad_value_returns_default(self):
        assert _int("abc", default=99) == 99

    def test_int_none_returns_default(self):
        assert _int(None, default=0) == 0


class TestLabeledList:
    def test_extracts_label_and_body(self):
        raw = [{"vmware": {"url": "https://vcenter.example.com"}}]
        result = _labeled_list(raw)
        assert result == [("vmware", {"url": "https://vcenter.example.com"})]

    def test_empty_list(self):
        assert _labeled_list([]) == []

    def test_non_dict_items_skipped(self):
        raw = ["string_item", {"key": {}}]
        result = _labeled_list(raw)
        assert len(result) == 1


class TestUnlabeledList:
    def test_returns_body_dicts(self):
        raw = [{"url": "https://netbox.example.com", "token": "abc"}]
        result = _unlabeled_list(raw)
        assert result == [{"url": "https://netbox.example.com", "token": "abc"}]

    def test_empty_list(self):
        assert _unlabeled_list([]) == []


class TestEvalConfigStr:
    def test_plain_string_returned_as_is(self):
        assert _eval_config_str("hello") == "hello"

    def test_env_call_resolved(self, monkeypatch):
        monkeypatch.setenv("MY_URL", "https://example.com")
        assert _eval_config_str("env('MY_URL')") == "https://example.com"

    def test_env_call_with_default(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR_ZZZ", raising=False)
        assert _eval_config_str("env('MISSING_VAR_ZZZ', 'fallback')") == "fallback"

    def test_non_string_returned_as_is(self):
        assert _eval_config_str(42) == 42
        assert _eval_config_str(True) is True
        assert _eval_config_str(None) is None

    def test_invalid_expr_returns_original(self):
        result = _eval_config_str("env(")
        assert result == "env("


# ---------------------------------------------------------------------------
# load_config() with in-memory HCL files
# ---------------------------------------------------------------------------


def _write_hcl(tmp_path: Path, content: str) -> str:
    mapping = tmp_path / "test_mapping.hcl"
    mapping.write_text(textwrap.dedent(content))
    return str(mapping)


class TestLoadConfigMinimal:
    def test_parses_minimal_valid_config(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type  = "vmware"
              url       = "vcenter.example.com"
              username  = "admin"
              password  = "secret"
              verify_ssl = false
            }

            netbox {
              url   = "https://netbox.example.com"
              token = "abc123"
            }

            collector {
              max_workers = 8
              dry_run     = false
              sync_tag    = "vmware-sync"
            }
        """)
        cfg = load_config(path)

        assert isinstance(cfg, CollectorConfig)
        assert cfg.source.api_type == "vmware"
        assert cfg.source.url == "vcenter.example.com"
        assert cfg.source.username == "admin"
        assert cfg.source.verify_ssl is False
        assert cfg.netbox.url == "https://netbox.example.com"
        assert cfg.netbox.token == "abc123"
        assert cfg.collector.max_workers == 8
        assert cfg.collector.dry_run is False
        assert cfg.collector.sync_tag == "vmware-sync"
        assert cfg.objects == []

    def test_raises_when_source_block_missing(self, tmp_path):
        path = _write_hcl(tmp_path, """
            netbox {
              url   = "https://netbox.example.com"
              token = "abc"
            }
        """)
        with pytest.raises(ValueError, match="source"):
            load_config(path)

    def test_raises_when_netbox_block_missing(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
        """)
        with pytest.raises(ValueError, match="netbox"):
            load_config(path)


class TestLoadConfigWithObjects:
    def test_parses_object_block(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }

            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }

            object "cluster" {
              source_collection = "clusters"
              netbox_resource   = "virtualization.clusters"
              lookup_by         = ["name"]

              field "name" {
                value = "source('name')"
              }
            }
        """)
        cfg = load_config(path)
        assert len(cfg.objects) == 1
        obj = cfg.objects[0]
        assert obj.name == "cluster"
        assert obj.source_collection == "clusters"
        assert obj.netbox_resource == "virtualization.clusters"
        assert obj.lookup_by == ["name"]
        assert len(obj.fields) == 1
        assert obj.fields[0].name == "name"
        assert obj.fields[0].value == "source('name')"

    def test_parses_prerequisite_block(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "catc" {
              api_type = "catc"
              url      = "https://catc.example.com"
            }

            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }

            object "device" {
              source_collection = "devices"
              netbox_resource   = "dcim.devices"

              prerequisite "device_type" {
                method = "ensure_device_type"
                args = {
                  model        = "source('model')"
                  manufacturer = "prereq('manufacturer')"
                }
              }

              field "name" {
                value = "source('name')"
              }
            }
        """)
        cfg = load_config(path)
        obj = cfg.objects[0]
        assert len(obj.prerequisites) == 1
        prereq = obj.prerequisites[0]
        assert prereq.name == "device_type"
        assert prereq.method == "ensure_device_type"
        assert "model" in prereq.args

    def test_parses_rest_collection_blocks(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "xclarity" {
              api_type = "rest"
              url      = "https://xclarity.example.com"
              username = "admin"
              password = "secret"
              auth     = "basic"

              collection "nodes" {
                endpoint        = "/nodes"
                list_key        = "nodeList"
                detail_endpoint = "/nodes/{uuid}"
                detail_id_field = "uuid"
              }

              collection "chassis" {
                endpoint = "/chassis"
                list_key = "chassisList"
              }
            }

            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
        """)
        cfg = load_config(path)
        assert "nodes" in cfg.source.collections
        assert "chassis" in cfg.source.collections

        nodes = cfg.source.collections["nodes"]
        assert nodes.endpoint == "/nodes"
        assert nodes.list_key == "nodeList"
        assert nodes.detail_endpoint == "/nodes/{uuid}"
        assert nodes.detail_id_field == "uuid"

        chassis = cfg.source.collections["chassis"]
        assert chassis.endpoint == "/chassis"
        assert chassis.list_key == "chassisList"
        assert chassis.detail_endpoint == ""  # not set → empty string default


class TestLoadConfigEnvResolution:
    def test_env_resolved_in_url(self, tmp_path, monkeypatch):
        monkeypatch.setenv("VCENTER_URL", "vcenter.prod.example.com")
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "env('VCENTER_URL')"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
        """)
        cfg = load_config(path)
        assert cfg.source.url == "vcenter.prod.example.com"

    def test_env_default_used_when_not_set(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOT_SET_COLLECTOR_VAR", raising=False)
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "env('NOT_SET_COLLECTOR_VAR', 'default.example.com')"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
        """)
        cfg = load_config(path)
        assert cfg.source.url == "default.example.com"


class TestLoadConfigCollectorDefaults:
    def test_defaults_applied_when_no_collector_block(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
        """)
        cfg = load_config(path)
        assert cfg.collector.max_workers == 4
        assert cfg.collector.dry_run is False
        assert cfg.collector.regex_dir == "./regex"
        assert cfg.collector.sync_tag == ""

    def test_extra_flags_parsed(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            collector {
              sync_tag   = "test-sync"
              full_sync  = "true"
              batch_size = 50
            }
        """)
        cfg = load_config(path)
        assert cfg.collector.sync_tag == "test-sync"
        assert cfg.collector.extra_flags.get("full_sync") is True
        assert cfg.collector.extra_flags.get("batch_size") == 50


class TestLoadConfigNetBoxOptions:
    def test_netbox_cache_and_rate_limit(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url        = "https://nb.example.com"
              token      = "tok"
              cache      = "redis"
              cache_url  = "redis://localhost:6379/0"
              rate_limit = 0.5
            }
        """)
        cfg = load_config(path)
        assert cfg.netbox.cache == "redis"
        assert cfg.netbox.cache_url == "redis://localhost:6379/0"
        assert cfg.netbox.rate_limit == pytest.approx(0.5)


class TestDataclasses:
    def test_source_config_defaults(self):
        cfg = SourceConfig(api_type="vmware", url="vc.example.com")
        assert cfg.username == ""
        assert cfg.password == ""
        assert cfg.verify_ssl is True
        assert cfg.extra == {}
        assert cfg.collections == {}

    def test_collector_options_defaults(self):
        opts = CollectorOptions()
        assert opts.max_workers == 4
        assert opts.dry_run is False
        assert opts.sync_tag == ""
        assert opts.regex_dir == "./regex"
        assert opts.extra_flags == {}

    def test_collection_config_defaults(self):
        col = CollectionConfig(name="nodes", endpoint="/nodes")
        assert col.list_key == ""
        assert col.detail_endpoint == ""
        assert col.detail_id_field == "uuid"
