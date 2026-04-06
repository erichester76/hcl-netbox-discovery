"""Tests for the HCL config parser (collector/config.py)."""

from __future__ import annotations

import os
import threading
import textwrap
from pathlib import Path

import pytest

import collector.db as db_module
from collector.config import (
    CollectionConfig,
    CollectorConfig,
    CollectorOptions,
    FieldConfig,
    IteratorConfig,
    NetBoxConfig,
    ObjectConfig,
    PrerequisiteConfig,
    SourceConfig,
    _bool,
    _eval_config_str,
    _eval_config_str_with_overrides,
    _field_update_mode,
    _int,
    _labeled_list,
    _unlabeled_list,
    build_source_config,
    load_config,
)
from collector.db import init_db, set_setting


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


class TestFieldUpdateMode:
    def test_replace_default(self):
        assert _field_update_mode(None) == "replace"

    def test_if_missing_mode(self):
        assert _field_update_mode("if_missing") == "if_missing"

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="update_mode"):
            _field_update_mode("preserve_forever")


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

    def test_env_call_resolved_from_runtime_config(self, tmp_path, monkeypatch):
        _init_runtime_config_db(tmp_path, monkeypatch, NETBOX_URL="https://example.com")
        assert _eval_config_str("env('NETBOX_URL')") == "https://example.com"

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


def _init_runtime_config_db(tmp_path: Path, monkeypatch, **values: str) -> None:
    db_path = str(tmp_path / "test_config.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setattr(db_module, "_lock", threading.Lock())
    init_db()
    for key, value in values.items():
        set_setting(key, value)


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

    def test_parses_field_update_mode(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }

            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }

            object "device" {
              source_collection = "devices"
              netbox_resource   = "dcim.devices"

              field "rack" {
                value       = "source('rack')"
                update_mode = "if_missing"
              }
            }
        """)
        cfg = load_config(path)
        field_cfg = cfg.objects[0].fields[0]
        assert field_cfg.name == "rack"
        assert field_cfg.update_mode == "if_missing"

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
        _init_runtime_config_db(tmp_path, monkeypatch, VCENTER_URL="vcenter.prod.example.com")
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

    def test_cache_settings_fallback_to_runtime_config(self, tmp_path, monkeypatch):
        """Cache settings omitted from HCL should fall back to DB-backed runtime settings."""
        _init_runtime_config_db(
            tmp_path,
            monkeypatch,
            NETBOX_CACHE_BACKEND="redis",
            NETBOX_CACHE_URL="redis://redis:6379/0",
            NETBOX_CACHE_TTL="14400",
            NETBOX_CACHE_KEY_PREFIX="myapp:",
            NETBOX_PREWARM_SENTINEL_TTL="7200",
        )

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
        assert cfg.netbox.cache == "redis"
        assert cfg.netbox.cache_url == "redis://redis:6379/0"
        assert cfg.netbox.cache_ttl == 14400
        assert cfg.netbox.cache_key_prefix == "myapp:"
        assert cfg.netbox.prewarm_sentinel_ttl == 7200

    def test_cache_settings_hcl_takes_priority_over_runtime_config(self, tmp_path, monkeypatch):
        """Explicit HCL values must override DB-backed runtime settings."""
        _init_runtime_config_db(
            tmp_path,
            monkeypatch,
            NETBOX_CACHE_BACKEND="sqlite",
            NETBOX_CACHE_TTL="9999",
        )

        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url       = "https://nb.example.com"
              token     = "tok"
              cache     = "redis"
              cache_ttl = 600
            }
        """)
        cfg = load_config(path)
        # HCL wins over DB-backed runtime settings
        assert cfg.netbox.cache == "redis"
        assert cfg.netbox.cache_ttl == 600

    def test_rate_limit_fallback_to_runtime_config(self, tmp_path, monkeypatch):
        """rate_limit and rate_limit_burst omitted from HCL should fall back to DB-backed runtime settings."""
        _init_runtime_config_db(
            tmp_path,
            monkeypatch,
            NETBOX_RATE_LIMIT="5",
            NETBOX_RATE_LIMIT_BURST="3",
        )

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
        assert cfg.netbox.rate_limit == pytest.approx(5.0)
        assert cfg.netbox.rate_limit_burst == 3

    def test_rate_limit_hcl_takes_priority_over_runtime_config(self, tmp_path, monkeypatch):
        """Explicit rate_limit in HCL must override the DB-backed runtime setting."""
        _init_runtime_config_db(tmp_path, monkeypatch, NETBOX_RATE_LIMIT="99")

        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url        = "https://nb.example.com"
              token      = "tok"
              rate_limit = 2
            }
        """)
        cfg = load_config(path)
        assert cfg.netbox.rate_limit == pytest.approx(2.0)

    def test_retry_settings_fallback_to_runtime_config(self, tmp_path, monkeypatch):
        """Retry settings omitted from HCL should fall back to DB-backed runtime settings."""
        _init_runtime_config_db(
            tmp_path,
            monkeypatch,
            NETBOX_RETRY_ATTEMPTS="7",
            NETBOX_RETRY_INITIAL_DELAY="0.5",
            NETBOX_RETRY_BACKOFF_FACTOR="3.0",
            NETBOX_RETRY_MAX_DELAY="30.0",
            NETBOX_RETRY_JITTER="0.1",
            NETBOX_RETRY_ON_4XX="429,503",
        )

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
        assert cfg.netbox.retry_attempts == 7
        assert cfg.netbox.retry_initial_delay == pytest.approx(0.5)
        assert cfg.netbox.retry_backoff_factor == pytest.approx(3.0)
        assert cfg.netbox.retry_max_delay == pytest.approx(30.0)
        assert cfg.netbox.retry_jitter == pytest.approx(0.1)
        assert cfg.netbox.retry_on_4xx == "429,503"

    def test_retry_settings_hcl_takes_priority_over_runtime_config(self, tmp_path, monkeypatch):
        """Explicit retry settings in HCL must override DB-backed runtime settings."""
        _init_runtime_config_db(
            tmp_path,
            monkeypatch,
            NETBOX_RETRY_ATTEMPTS="99",
            NETBOX_RETRY_ON_4XX="503",
        )

        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url            = "https://nb.example.com"
              token          = "tok"
              retry_attempts = 2
              retry_on_4xx   = "429"
            }
        """)
        cfg = load_config(path)
        assert cfg.netbox.retry_attempts == 2
        assert cfg.netbox.retry_on_4xx == "429"


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
        assert opts.iterators == []

    def test_collection_config_defaults(self):
        col = CollectionConfig(name="nodes", endpoint="/nodes")
        assert col.list_key == ""
        assert col.detail_endpoint == ""
        assert col.detail_id_field == "uuid"


# ---------------------------------------------------------------------------
# IteratorConfig unit tests
# ---------------------------------------------------------------------------


class TestIteratorConfig:
    def test_len_returns_shortest_list(self):
        it = IteratorConfig(variables={"A": ["a1", "a2", "a3"], "B": ["b1", "b2"]})
        assert len(it) == 2

    def test_len_empty_variables(self):
        it = IteratorConfig(variables={})
        assert len(it) == 0

    def test_len_scalar_value_counts_as_one(self):
        it = IteratorConfig(variables={"A": ["a1", "a2"], "B": "scalar"})
        assert len(it) == 1

    def test_get_row_returns_correct_values(self):
        it = IteratorConfig(
            variables={
                "URL": ["vc1.example.com", "vc2.example.com"],
                "USER": ["admin", "readonly"],
            }
        )
        assert it.get_row(0) == {"URL": "vc1.example.com", "USER": "admin"}
        assert it.get_row(1) == {"URL": "vc2.example.com", "USER": "readonly"}

    def test_get_row_out_of_range_skips_key(self):
        it = IteratorConfig(variables={"A": ["a1"]})
        row = it.get_row(99)
        assert "A" not in row

    def test_max_workers_defaults_to_one(self):
        it = IteratorConfig(variables={"A": ["x"]})
        assert it.max_workers == 1

    def test_max_workers_set(self):
        it = IteratorConfig(variables={"A": ["x", "y"]}, max_workers=3)
        assert it.max_workers == 3


# ---------------------------------------------------------------------------
# _eval_config_str_with_overrides unit tests
# ---------------------------------------------------------------------------


class TestEvalConfigStrWithOverrides:
    def test_override_takes_precedence_over_runtime_config(self, tmp_path, monkeypatch):
        _init_runtime_config_db(tmp_path, monkeypatch, NETBOX_URL="from_db")
        result = _eval_config_str_with_overrides("env('NETBOX_URL')", {"NETBOX_URL": "from_override"})
        assert result == "from_override"

    def test_falls_back_to_runtime_config_when_no_override(self, tmp_path, monkeypatch):
        _init_runtime_config_db(tmp_path, monkeypatch, NETBOX_URL="from_db")
        result = _eval_config_str_with_overrides("env('NETBOX_URL')", {})
        assert result == "from_db"

    def test_plain_string_returned_as_is(self):
        result = _eval_config_str_with_overrides("hello", {"hello": "world"})
        assert result == "hello"

    def test_non_string_returned_as_is(self):
        assert _eval_config_str_with_overrides(42, {"A": "B"}) == 42


# ---------------------------------------------------------------------------
# build_source_config unit tests
# ---------------------------------------------------------------------------


class TestBuildSourceConfig:
    def test_builds_source_config_without_overrides(self, tmp_path, monkeypatch):
        _init_runtime_config_db(tmp_path, monkeypatch, VCENTER_URL="vc.example.com")
        body = {"api_type": "vmware", "url": "env('VCENTER_URL')", "username": "admin"}
        cfg = build_source_config(body, "vmware")
        assert cfg.api_type == "vmware"
        assert cfg.url == "vc.example.com"

    def test_builds_source_config_with_overrides(self, tmp_path, monkeypatch):
        _init_runtime_config_db(tmp_path, monkeypatch, VCENTER_URL="should_not_be_used")
        body = {"api_type": "vmware", "url": "env('VCENTER_URL')", "username": "admin"}
        cfg = build_source_config(body, "vmware", overrides={"VCENTER_URL": "vc2.example.com"})
        assert cfg.url == "vc2.example.com"

    def test_falls_back_to_source_label_for_api_type(self):
        body = {"url": "vc.example.com"}
        cfg = build_source_config(body, "vmware")
        assert cfg.api_type == "vmware"


# ---------------------------------------------------------------------------
# load_config() iterator parsing
# ---------------------------------------------------------------------------


class TestLoadConfigIterator:
    def test_parses_iterator_block(self, tmp_path):
        path = _write_hcl(tmp_path, """
            source "vmware" {
              api_type = "vmware"
              url      = "env('VCENTER_URL')"
              username = "env('VCENTER_USER')"
              password = "env('VCENTER_PASS')"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
            collector {
              iterator {
                VCENTER_URL  = ["vc1.example.com", "vc2.example.com"]
                VCENTER_USER = ["admin", "readonly"]
                VCENTER_PASS = ["pass1", "pass2"]
              }
            }
        """)
        cfg = load_config(path)
        assert len(cfg.collector.iterators) == 1
        it = cfg.collector.iterators[0]
        assert len(it) == 2
        assert it.max_workers == 1
        row0 = it.get_row(0)
        assert row0["VCENTER_URL"] == "vc1.example.com"
        assert row0["VCENTER_USER"] == "admin"
        row1 = it.get_row(1)
        assert row1["VCENTER_URL"] == "vc2.example.com"
        assert row1["VCENTER_PASS"] == "pass2"

    def test_parses_iterator_max_workers(self, tmp_path):
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
              iterator {
                max_workers  = 3
                VCENTER_URL  = ["vc1.example.com", "vc2.example.com", "vc3.example.com"]
                VCENTER_PASS = ["p1", "p2", "p3"]
              }
            }
        """)
        cfg = load_config(path)
        it = cfg.collector.iterators[0]
        assert it.max_workers == 3
        assert len(it) == 3
        assert "max_workers" not in it.variables

    def test_no_iterator_block_yields_empty_list(self, tmp_path):
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
        assert cfg.collector.iterators == []

    def test_raw_source_body_stored(self, tmp_path):
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
        assert cfg.source_label == "vmware"
        assert isinstance(cfg.raw_source_body, dict)
        assert "url" in cfg.raw_source_body

    def test_iterator_max_workers_not_in_extra_flags(self, tmp_path):
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
              iterator {
                max_workers = 2
                VCENTER_URL = ["vc1.example.com", "vc2.example.com"]
              }
            }
        """)
        cfg = load_config(path)
        assert "iterator" not in cfg.collector.extra_flags


class TestXClarityMappings:
    PATHS = [
        "mappings/xclarity.hcl.example",
        "mappings/xclarity-modules.hcl.example",
    ]
    OBJECT_NAMES = {"node", "chassis", "switch", "storage"}
    CANONICAL_MANUFACTURER = "when(source('manufacturer'), regex_replace(source('manufacturer'), '(?i)^lenovo.*', 'Lenovo'), 'Lenovo')"

    @pytest.mark.parametrize("mapping_path", PATHS)
    def test_manufacturer_prereqs_canonicalize_lenovo(self, mapping_path):
        cfg = load_config(mapping_path)
        for name in self.OBJECT_NAMES:
            obj = next((o for o in cfg.objects if o.name == name), None)
            assert obj is not None, f"missing object {name} in {mapping_path}"
            match = [p for p in obj.prerequisites if p.name == "manufacturer"]
            assert match, f"object {name} lacks manufacturer prerequisite"
            assert match[0].args.get("name") == self.CANONICAL_MANUFACTURER

    @pytest.mark.parametrize("mapping_path", PATHS)
    def test_site_fields_use_if_missing(self, mapping_path):
        cfg = load_config(mapping_path)
        for name in self.OBJECT_NAMES:
            obj = next((o for o in cfg.objects if o.name == name), None)
            assert obj is not None, f"missing object {name} in {mapping_path}"
            site_field = next((f for f in obj.fields if f.name == "site"), None)
            assert site_field is not None, f"object {name} missing site field"
            assert site_field.update_mode == "if_missing"
