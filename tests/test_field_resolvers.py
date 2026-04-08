"""Tests for the field expression evaluator (collector/field_resolvers.py)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from collector.config import CollectorOptions
from collector.context import RunContext
from collector.field_resolvers import Resolver, walk_path

# ---------------------------------------------------------------------------
# walk_path()
# ---------------------------------------------------------------------------


class TestWalkPath:
    def test_simple_dict_key(self):
        assert walk_path({"name": "vm-01"}, "name") == "vm-01"

    def test_nested_dict(self):
        assert walk_path({"a": {"b": {"c": 42}}}, "a.b.c") == 42

    def test_attribute_access(self):
        obj = SimpleNamespace(name="switch-01")
        assert walk_path(obj, "name") == "switch-01"

    def test_mixed_dict_and_attr(self):
        obj = SimpleNamespace(info={"model": "C9300"})
        assert walk_path(obj, "info.model") == "C9300"

    def test_missing_key_returns_none(self):
        assert walk_path({"name": "x"}, "missing") is None

    def test_missing_nested_key_returns_none(self):
        assert walk_path({"a": {}}, "a.b.c") is None

    def test_none_object_returns_none(self):
        assert walk_path(None, "name") is None

    def test_empty_path_returns_object(self):
        assert walk_path({"name": "x"}, "") is None

    def test_bracket_filter_by_key_presence(self):
        items = [{"type": "primary", "value": "10.0.0.1"}, {"value": "10.0.0.2"}]
        result = walk_path({"nics": items}, "nics[type]")
        assert result == {"type": "primary", "value": "10.0.0.1"}

    def test_bracket_star_returns_all_items(self):
        items = [1, 2, 3]
        result = walk_path({"nums": items}, "nums[*]")
        assert result == [1, 2, 3]

    def test_bracket_filter_followed_by_field(self):
        items = [{"type": "primary", "addr": "10.0.0.1"}, {"other": True, "addr": "10.0.0.2"}]
        result = walk_path({"nics": items}, "nics[type].addr")
        assert result == "10.0.0.1"

    def test_no_match_returns_none(self):
        items = [{"value": "a"}]
        result = walk_path({"nics": items}, "nics[no_such_key]")
        assert result is None

    def test_list_access_returns_single_item_unwrapped(self):
        items = [{"type": "primary", "ip": "1.2.3.4"}]
        result = walk_path({"nics": items}, "nics[type].ip")
        assert result == "1.2.3.4"


# ---------------------------------------------------------------------------
# Resolver – source()
# ---------------------------------------------------------------------------


def _make_resolver(source_obj, prereqs=None, regex_dir="/tmp"):
    opts = CollectorOptions(
        max_workers=4,
        dry_run=False,
        sync_tag="test",
        regex_dir=regex_dir,
    )
    ctx = RunContext(
        nb=None,
        source_adapter=None,
        collector_opts=opts,
        regex_dir=regex_dir,
        prereqs=prereqs or {},
        source_obj=source_obj,
        parent_nb_obj=None,
        dry_run=False,
    )
    return Resolver(ctx)


class TestResolverSource:
    def test_simple_field(self):
        r = _make_resolver({"name": "vm-01"})
        assert r.evaluate("source('name')") == "vm-01"

    def test_nested_field(self):
        r = _make_resolver({"hardware": {"vcpus": 4}})
        assert r.evaluate("source('hardware.vcpus')") == 4

    def test_missing_path_returns_none(self):
        r = _make_resolver({"name": "vm-01"})
        assert r.evaluate("source('nonexistent')") is None

    def test_non_string_value_returned_as_is(self):
        r = _make_resolver({})
        assert r.evaluate(42) == 42
        assert r.evaluate(True) is True
        assert r.evaluate(None) is None


# ---------------------------------------------------------------------------
# Resolver – env()
# ---------------------------------------------------------------------------


class TestResolverEnv:
    def test_reads_runtime_config_value(self):
        from unittest.mock import patch

        with patch("collector.field_resolvers._get_config", return_value="hello") as mock_gc:
            r = _make_resolver({})
            result = r.evaluate("env('TEST_VAR')")
        mock_gc.assert_called_once_with("TEST_VAR", "")
        assert result == "hello"

    def test_reads_default_when_runtime_config_missing(self):
        r = _make_resolver({})
        assert r.evaluate("env('DEFINITELY_NOT_SET_ZZZ', 'fallback')") == "fallback"

    def test_returns_default_when_missing(self):
        r = _make_resolver({})
        assert r.evaluate("env('DEFINITELY_NOT_SET_ZZZ', 'fallback')") == "fallback"

    def test_returns_empty_string_default(self, monkeypatch):
        monkeypatch.delenv("EMPTY_ENV_VAR", raising=False)
        r = _make_resolver({})
        assert r.evaluate("env('EMPTY_ENV_VAR')") == ""

    def test_db_value_is_authoritative(self):
        """DB-backed runtime config should be the only non-startup source."""
        from unittest.mock import patch

        with patch("collector.field_resolvers._get_config", return_value="from_db") as mock_gc:
            r = _make_resolver({})
            result = r.evaluate("env('TEST_DB_OVERRIDE')")
        mock_gc.assert_called_once_with("TEST_DB_OVERRIDE", "")
        assert result == "from_db"

    def test_default_used_when_runtime_config_unavailable(self, monkeypatch):
        """When runtime config cannot resolve a key, env() should return the explicit default."""
        monkeypatch.delenv("COLLECTOR_DB_PATH", raising=False)
        r = _make_resolver({})
        result = r.evaluate("env('TEST_FALLBACK_VAR', 'fallback')")
        assert result == "fallback"


# ---------------------------------------------------------------------------
# Resolver – string helpers
# ---------------------------------------------------------------------------


class TestResolverStringHelpers:
    def test_upper(self):
        r = _make_resolver({"status": "active"})
        assert r.evaluate("upper(source('status'))") == "ACTIVE"

    def test_lower(self):
        r = _make_resolver({"name": "SWITCH-01"})
        assert r.evaluate("lower(source('name'))") == "switch-01"

    def test_replace(self):
        r = _make_resolver({"name": "vm-01.example.com"})
        assert r.evaluate("replace(source('name'), '.example.com', '')") == "vm-01"

    def test_truncate(self):
        r = _make_resolver({"name": "averylongname"})
        assert r.evaluate("truncate(source('name'), 5)") == "avery"

    def test_split_returns_list(self):
        r = _make_resolver({"fullName": "VMware ESXi 7.0"})
        assert r.evaluate("split(source('fullName'))") == ["VMware", "ESXi", "7.0"]

    def test_split_first_word(self):
        r = _make_resolver({"fullName": "VMware ESXi 7.0"})
        assert r.evaluate("split(source('fullName'))[0]") == "VMware"

    def test_split_with_sep(self):
        r = _make_resolver({"version": "7.0.1"})
        assert r.evaluate("split(source('version'), '.')[1]") == "0"

    def test_split_none_returns_empty_list(self):
        r = _make_resolver({})
        assert r.evaluate("split(None)") == []

    def test_join(self):
        r = _make_resolver({"tags": ["web", "prod", "linux"]})
        assert r.evaluate("join(',', source('tags'))") == "web,prod,linux"

    def test_regex_replace(self):
        r = _make_resolver({"version": "IOS-XE 17.6.4"})
        result = r.evaluate("regex_replace(source('version'), r'\\s+', '-')")
        assert result == "IOS-XE-17.6.4"


# ---------------------------------------------------------------------------
# Resolver – numeric helpers
# ---------------------------------------------------------------------------


class TestResolverNumericHelpers:
    def test_to_gb(self):
        r = _make_resolver({})
        assert r.evaluate("to_gb(1073741824)") == 1

    def test_to_gb_none(self):
        r = _make_resolver({})
        assert r.evaluate("to_gb(None)") is None

    def test_to_mb(self):
        r = _make_resolver({})
        assert r.evaluate("to_mb(1024)") == 1

    def test_to_mb_none(self):
        r = _make_resolver({})
        assert r.evaluate("to_mb(None)") is None

    def test_int_conversion(self):
        r = _make_resolver({})
        assert r.evaluate("int('42')") == 42

    def test_int_bad_value_returns_default(self):
        r = _make_resolver({})
        assert r.evaluate("int('abc')") == 0

    def test_str_conversion(self):
        r = _make_resolver({})
        assert r.evaluate("str(99)") == "99"

    def test_str_none_returns_empty(self):
        r = _make_resolver({})
        assert r.evaluate("str(None)") == ""


# ---------------------------------------------------------------------------
# Resolver – getattr()
# ---------------------------------------------------------------------------


class TestResolverGetattr:
    def test_getattr_existing_attribute(self):
        from types import SimpleNamespace
        obj = SimpleNamespace(capacityInKB=1024)
        r = _make_resolver({"device": obj})
        assert r.evaluate("getattr(source('device'), 'capacityInKB', None)") == 1024

    def test_getattr_missing_attribute_returns_default(self):
        from types import SimpleNamespace
        obj = SimpleNamespace()  # no capacityInKB
        r = _make_resolver({"device": obj})
        assert r.evaluate("getattr(source('device'), 'capacityInKB', None)") is None

    def test_getattr_used_in_list_comprehension_filters_non_disks(self):
        """Simulates filtering VirtualDisk devices from a mixed hardware device list."""
        from types import SimpleNamespace
        disk = SimpleNamespace(capacityInKB=20971520, deviceInfo=SimpleNamespace(label="Hard disk 1"))
        nic = SimpleNamespace(deviceInfo=SimpleNamespace(label="Network adapter 1"))
        cdrom = SimpleNamespace(deviceInfo=SimpleNamespace(label="CD/DVD drive 1"))
        r = _make_resolver({"devices": [disk, nic, cdrom]})
        result = r.evaluate(
            "[d for d in (source('devices') or []) if getattr(d, 'capacityInKB', None)]"
        )
        assert len(result) == 1
        assert result[0] is disk


# ---------------------------------------------------------------------------
# Resolver – when() and coalesce()
# ---------------------------------------------------------------------------


class TestResolverConditionals:
    def test_when_true(self):
        r = _make_resolver({"active": True})
        assert r.evaluate("when(source('active'), 'yes', 'no')") == "yes"

    def test_when_false(self):
        r = _make_resolver({"active": False})
        assert r.evaluate("when(source('active'), 'yes', 'no')") == "no"

    def test_coalesce_returns_first_non_empty_path(self):
        # Bare string paths (no parens/spaces) are auto-resolved as source() paths
        r = _make_resolver({"a": "", "b": "value"})
        assert r.evaluate("coalesce('a', 'b')") == "value"

    def test_coalesce_returns_none_when_all_paths_empty(self):
        r = _make_resolver({"a": "", "b": ""})
        assert r.evaluate("coalesce('a', 'b')") is None

    def test_coalesce_auto_resolves_plain_path(self):
        # coalesce("name") should auto-resolve to source("name")
        r = _make_resolver({"name": "vm-01"})
        assert r.evaluate("coalesce('name')") == "vm-01"

    def test_coalesce_with_integer_fallback(self):
        # Non-string values pass through without path resolution
        r = _make_resolver({"a": ""})
        assert r.evaluate("coalesce('a', 42)") == 42


# ---------------------------------------------------------------------------
# Resolver – map_value()
# ---------------------------------------------------------------------------


class TestResolverMapValue:
    def test_key_found(self):
        r = _make_resolver({"state": "running"})
        result = r.evaluate("map_value(source('state'), {'running': 'active', 'stopped': 'offline'})")
        assert result == "active"

    def test_key_not_found_returns_default(self):
        r = _make_resolver({"state": "unknown"})
        result = r.evaluate("map_value(source('state'), {'running': 'active'}, 'offline')")
        assert result == "offline"

    def test_key_not_found_no_default_returns_none(self):
        r = _make_resolver({"state": "unknown"})
        result = r.evaluate("map_value(source('state'), {'running': 'active'})")
        assert result is None


# ---------------------------------------------------------------------------
# Resolver – prereq()
# ---------------------------------------------------------------------------


class TestResolverPrereq:
    def test_prereq_scalar(self):
        r = _make_resolver({}, prereqs={"manufacturer": 42})
        assert r.evaluate("prereq('manufacturer')") == 42

    def test_prereq_dict_dotted(self):
        r = _make_resolver({}, prereqs={"placement": {"site_id": 7, "location_id": None}})
        assert r.evaluate("prereq('placement.site_id')") == 7

    def test_prereq_missing_returns_none(self):
        r = _make_resolver({}, prereqs={})
        assert r.evaluate("prereq('missing_prereq')") is None


# ---------------------------------------------------------------------------
# Resolver – regex_file()
# ---------------------------------------------------------------------------


class TestResolverRegexFile:
    def test_applies_first_matching_pattern(self, tmp_path):
        regex_file = tmp_path / "cluster_to_site"
        regex_file.write_text("^cluster-east$,East Campus\n^cluster-west$,West Campus\n")

        r = _make_resolver({"cluster": "cluster-east"}, regex_dir=str(tmp_path))
        result = r.evaluate("regex_file(source('cluster'), 'cluster_to_site')")
        assert result == "East Campus"

    def test_returns_original_when_no_match(self, tmp_path):
        regex_file = tmp_path / "cluster_to_site"
        regex_file.write_text("^cluster-east$,East Campus\n")

        r = _make_resolver({"cluster": "cluster-north"}, regex_dir=str(tmp_path))
        result = r.evaluate("regex_file(source('cluster'), 'cluster_to_site')")
        assert result == "cluster-north"

    def test_returns_original_when_file_not_found(self):
        r = _make_resolver({"cluster": "cluster-east"}, regex_dir="/nonexistent")
        result = r.evaluate("regex_file(source('cluster'), 'cluster_to_site')")
        assert result == "cluster-east"

    def test_skips_comment_lines(self, tmp_path):
        regex_file = tmp_path / "vm_to_role"
        regex_file.write_text("# this is a comment\n^web-\\d+$,Web Server\n")

        r = _make_resolver({"name": "web-01"}, regex_dir=str(tmp_path))
        result = r.evaluate("regex_file(source('name'), 'vm_to_role')")
        assert result == "Web Server"


# ---------------------------------------------------------------------------
# Resolver – error handling
# ---------------------------------------------------------------------------


class TestResolverErrorHandling:
    def test_eval_failure_returns_none(self):
        r = _make_resolver({})
        # This expression will raise a NameError since 'undefined_func' is not in scope
        result = r.evaluate("undefined_func()")
        assert result is None

    def test_no_builtins_exposed(self):
        r = _make_resolver({})
        # __import__ should not be accessible (builtins replaced with {})
        result = r.evaluate("__import__('os')")
        assert result is None

    def test_literals_available(self):
        r = _make_resolver({})
        assert r.evaluate("True") is True
        assert r.evaluate("False") is False
        assert r.evaluate("None") is None

    def test_quoted_string_literal_returns_string(self):
        r = _make_resolver({})
        # Properly-quoted Python string literals evaluate to their string value
        assert r.evaluate("'VMware vSphere'") == "VMware vSphere"
        assert r.evaluate("'Hypervisor Host'") == "Hypervisor Host"
        assert r.evaluate("'Azure VM'") == "Azure VM"

    def test_unquoted_multiword_string_returns_none(self):
        r = _make_resolver({})
        # Bare strings with spaces are not valid Python — eval fails and returns None
        assert r.evaluate("VMware vSphere") is None

    def test_evaluate_strict_raises_on_eval_failure(self):
        r = _make_resolver({})
        with pytest.raises(ValueError, match="lookup_name evaluation failed"):
            r.evaluate_strict("undefined_func()", label="lookup_name")

    def test_evaluate_strict_allows_valid_literals(self):
        r = _make_resolver({})
        assert r.evaluate_strict("'vmware-host'", label="lookup_name") == "vmware-host"
        assert r.evaluate("Hypervisor Host") is None


# ---------------------------------------------------------------------------
# Resolver – VMware virtual disk description (backing fields)
# ---------------------------------------------------------------------------

_DISK_DESC_EXPR = (
    "truncate("
    "(source('backing.fileName') or '') + ' (' +"
    " when(source('backing.thinProvisioned'), 'Thin Provisioned', 'Thick Provisioned')"
    " + ' ' + (source('backing.diskMode') or '') + ')',"
    " 200)"
)

_JNSU_DESC_EXPR = (
    "truncate(regex_replace(regex_replace(source('DirXMLjnsuDescription'), '^Connected to ', ''), "
    "'[\\\\r\\\\n ]{2,}|[\\\\r\\\\n]', ' '), 64) if source('DirXMLjnsuStaticAddrs') else "
    "truncate(regex_replace(upper(regex_replace(source('DirXMLjnsuUserDN'), '^cn=(.+?),.*$', '\\\\1') "
    "or source('DirXMLjnsuUserDN') or 'UNKNOWN') + '@' + env('LDAP_UPN_DOMAIN', 'CLEMSON.EDU') + ': ' + "
    "(source('DirXMLjnsuDescription') or ''), '[\\\\r\\\\n ]{2,}|[\\\\r\\\\n]', ' '), 64)"
)


class TestVMwareDiskDescription:
    """Tests for the disk description expression used in mappings/vmware.hcl."""

    def _disk_obj(self, file_name=None, thin=False, disk_mode=None):
        """Build a SimpleNamespace that mimics a pyVmomi VirtualDisk device."""
        from types import SimpleNamespace

        backing = SimpleNamespace(
            fileName=file_name,
            thinProvisioned=thin,
            diskMode=disk_mode,
        )
        return SimpleNamespace(backing=backing)

    def test_thin_provisioned_disk_full_info(self):
        disk = self._disk_obj(
            file_name="[datastore1] vm-01/vm-01.vmdk",
            thin=True,
            disk_mode="persistent",
        )
        r = _make_resolver(disk)
        result = r.evaluate(_DISK_DESC_EXPR)
        assert result == "[datastore1] vm-01/vm-01.vmdk (Thin Provisioned persistent)"

    def test_thick_provisioned_disk_full_info(self):
        disk = self._disk_obj(
            file_name="[datastore2] vm-02/vm-02.vmdk",
            thin=False,
            disk_mode="persistent",
        )
        r = _make_resolver(disk)
        result = r.evaluate(_DISK_DESC_EXPR)
        assert result == "[datastore2] vm-02/vm-02.vmdk (Thick Provisioned persistent)"

    def test_missing_thin_provisioned_defaults_to_thick(self):
        """When thinProvisioned is None (attribute absent), default to Thick Provisioned."""
        from types import SimpleNamespace

        # backing without thinProvisioned attribute
        backing = SimpleNamespace(fileName="[ds] vm/vm.vmdk", diskMode="persistent")
        disk = SimpleNamespace(backing=backing)
        r = _make_resolver(disk)
        result = r.evaluate(_DISK_DESC_EXPR)
        assert "Thick Provisioned" in result

    def test_missing_file_name_uses_empty_string(self):
        disk = self._disk_obj(file_name=None, thin=True, disk_mode="persistent")
        r = _make_resolver(disk)
        result = r.evaluate(_DISK_DESC_EXPR)
        assert result == " (Thin Provisioned persistent)"

    def test_missing_disk_mode_uses_empty_string(self):
        disk = self._disk_obj(
            file_name="[ds] vm/vm.vmdk", thin=False, disk_mode=None
        )
        r = _make_resolver(disk)
        result = r.evaluate(_DISK_DESC_EXPR)
        assert result == "[ds] vm/vm.vmdk (Thick Provisioned )"

    def test_description_truncated_to_200_chars(self):
        long_path = "[datastore1] " + "a" * 200 + "/vm.vmdk"
        disk = self._disk_obj(file_name=long_path, thin=True, disk_mode="persistent")
        r = _make_resolver(disk)
        result = r.evaluate(_DISK_DESC_EXPR)
        assert len(result) == 200


class TestJNSUDescriptionExpression:
    def test_static_entry_description_is_cleaned(self):
        r = _make_resolver(
            {
                "DirXMLjnsuStaticAddrs": ["172.19.1.10"],
                "DirXMLjnsuDescription": "Connected to AP-01\r\n  Closet",
            }
        )
        assert r.evaluate_strict(_JNSU_DESC_EXPR, label="description") == "AP-01 Closet"

    def test_dynamic_entry_description_is_nil_safe(self):
        r = _make_resolver(
            {
                "DirXMLjnsuStaticAddrs": [],
                "DirXMLjnsuDescription": None,
                "DirXMLjnsuUserDN": None,
            }
        )
        assert r.evaluate_strict(_JNSU_DESC_EXPR, label="description") == "UNKNOWN@CLEMSON.EDU: "

    def test_dynamic_entry_extracts_cn_when_present(self):
        r = _make_resolver(
            {
                "DirXMLjnsuStaticAddrs": [],
                "DirXMLjnsuDescription": "Printer in Lab",
                "DirXMLjnsuUserDN": "cn=lab-printer,ou=Devices,dc=example,dc=org",
            }
        )
        assert r.evaluate_strict(_JNSU_DESC_EXPR, label="description") == (
            "LAB-PRINTER@CLEMSON.EDU: Printer in Lab"
        )
# Resolver – mask_to_prefix()
# ---------------------------------------------------------------------------


class TestResolverMaskToPrefix:
    def test_class_c_mask(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix('255.255.255.0')") == 24

    def test_class_b_mask(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix('255.255.0.0')") == 16

    def test_class_a_mask(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix('255.0.0.0')") == 8

    def test_slash_30_mask(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix('255.255.255.252')") == 30

    def test_host_mask_slash_32(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix('255.255.255.255')") == 32

    def test_none_returns_none(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix(None)") is None

    def test_invalid_mask_returns_none(self):
        r = _make_resolver({})
        assert r.evaluate("mask_to_prefix('not-a-mask')") is None

    def test_used_with_source_in_expression(self):
        """Simulates the vmware.hcl vNIC ip_address field expression."""
        from types import SimpleNamespace
        ip_obj = SimpleNamespace(ipAddress="10.1.2.3", subnetMask="255.255.255.0")
        r = _make_resolver(ip_obj)
        result = r.evaluate(
            "join('/', [source('ipAddress'), str(mask_to_prefix(source('subnetMask')))])"
        )
        assert result == "10.1.2.3/24"
