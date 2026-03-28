"""Tests for the VMware source adapter (collector/sources/vmware.py).

All vCenter / pyVmomi calls are mocked so that no real vCenter is needed.
pyVim and pyVmomi are optional runtime dependencies; this module patches them
into sys.modules so that the adapter code can be exercised without installing
the VMware SDK.
"""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Create lightweight fake pyVim / pyVmomi modules so the adapter can be
# imported and tested without the real VMware SDK.
# ---------------------------------------------------------------------------
_fake_pyvim = ModuleType("pyVim")
_fake_pyvim_connect = ModuleType("pyVim.connect")
_fake_pyvim_connect.SmartConnect = MagicMock(return_value=MagicMock())
_fake_pyvim_connect.Disconnect = MagicMock()
_fake_pyvim.connect = _fake_pyvim_connect

_fake_pyvmomi = ModuleType("pyVmomi")
_fake_vim = MagicMock()
_fake_pyvmomi.vim = _fake_vim

sys.modules.setdefault("pyVim", _fake_pyvim)
sys.modules.setdefault("pyVim.connect", _fake_pyvim_connect)
sys.modules.setdefault("pyVmomi", _fake_pyvmomi)

from collector.sources.vmware import VMwareSource  # noqa: E402 (after sys.modules setup)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_managed_obj(name: str, mo_id: str = "vm-1") -> MagicMock:
    """Return a minimal pyVmomi-style managed-object mock."""
    obj = MagicMock()
    obj.name = name
    obj._moId = mo_id
    return obj


def _make_container_view(items: list) -> MagicMock:
    view_mock = MagicMock()
    view_mock.view = items
    return view_mock


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestVMwareConnect:
    def test_connect_calls_SmartConnect(self, vmware_config):
        fake_sc = MagicMock(return_value=MagicMock())
        _fake_pyvim_connect.SmartConnect = fake_sc
        src = VMwareSource()
        src.connect(vmware_config)
        # vmware_config has verify_ssl=False, so disableSslCertValidation=True
        # should be passed instead of sslContext (pyVmomi 8.x compatibility).
        fake_sc.assert_called_once_with(
            host=vmware_config.url,
            user=vmware_config.username,
            pwd=vmware_config.password,
            disableSslCertValidation=True,
        )

    def test_connect_calls_SmartConnect_verify_ssl_true(self):
        """When verify_ssl=True, disableSslCertValidation must NOT be passed."""
        from collector.config import SourceConfig
        config = SourceConfig(
            api_type="vmware",
            url="vcenter.example.com",
            username="admin",
            password="secret",
            verify_ssl=True,
        )
        fake_sc = MagicMock(return_value=MagicMock())
        _fake_pyvim_connect.SmartConnect = fake_sc
        src = VMwareSource()
        src.connect(config)
        fake_sc.assert_called_once_with(
            host=config.url,
            user=config.username,
            pwd=config.password,
        )

    def test_connect_raises_if_pyvmomi_missing(self, vmware_config):
        src = VMwareSource()
        with patch.dict("sys.modules", {"pyVim": None, "pyVim.connect": None}):
            with pytest.raises((RuntimeError, ImportError)):
                src.connect(vmware_config)

    def test_fetch_tags_session_created_when_enabled(self, vmware_config):
        vmware_config.extra["fetch_tags"] = "true"
        _fake_pyvim_connect.SmartConnect = MagicMock(return_value=MagicMock())
        with patch.object(VMwareSource, "_connect_rest", return_value=MagicMock()) as cr:
            src = VMwareSource()
            src.connect(vmware_config)
            cr.assert_called_once()

    def test_no_rest_session_by_default(self, vmware_config):
        _fake_pyvim_connect.SmartConnect = MagicMock(return_value=MagicMock())
        src = VMwareSource()
        src.connect(vmware_config)
        assert src._rest_session is None


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestVMwareGetObjects:
    def _connected_source(self) -> VMwareSource:
        src = VMwareSource()
        src._api_client = MagicMock()
        return src

    def test_raises_without_connect(self):
        src = VMwareSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("clusters")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("unknown_collection")

    def test_get_clusters(self):
        src = self._connected_source()
        fake_clusters = [_make_managed_obj("cluster-01"), _make_managed_obj("cluster-02")]
        view = _make_container_view(fake_clusters)
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view

        result = src._get_clusters()

        assert result == fake_clusters
        view.Destroy.assert_called_once()

    def test_get_hosts(self):
        src = self._connected_source()
        fake_hosts = [_make_managed_obj("esxi-01")]
        view = _make_container_view(fake_hosts)
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view

        result = src._get_hosts()

        assert result == fake_hosts

    def test_get_vms(self):
        src = self._connected_source()
        fake_vms = [_make_managed_obj("vm-01"), _make_managed_obj("vm-02")]
        view = _make_container_view(fake_vms)
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view

        result = src._get_vms()

        assert len(result) == 2

    def test_get_vms_attaches_rest_tags(self):
        src = self._connected_source()
        vm = _make_managed_obj("vm-01", "vm-10")
        view = _make_container_view([vm])
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view
        src._rest_session = MagicMock()

        with patch.object(src, "_fetch_rest_tags", return_value={"vm-10": {"tag-A": True}}) as frt:
            result = src._get_vms()

        frt.assert_called_once()
        assert result[0]._rest_tags == {"tag-A": True}

    def test_collection_names_case_insensitive(self):
        src = self._connected_source()
        view = _make_container_view([])
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view

        result = src.get_objects("CLUSTERS")

        assert result == []


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestVMwareClose:
    def test_close_calls_disconnect(self):
        fake_disconnect = MagicMock()
        _fake_pyvim_connect.Disconnect = fake_disconnect
        src = VMwareSource()
        src._api_client = MagicMock()
        src.close()
        fake_disconnect.assert_called_once()
        assert src._api_client is None

    def test_close_handles_disconnect_error(self):
        _fake_pyvim_connect.Disconnect = MagicMock(side_effect=Exception("oops"))
        src = VMwareSource()
        src._api_client = MagicMock()
        src.close()  # should not raise
        assert src._api_client is None

    def test_close_clears_rest_session(self):
        _fake_pyvim_connect.Disconnect = MagicMock()
        src = VMwareSource()
        src._api_client = MagicMock()
        rest = MagicMock()
        src._rest_session = rest
        src.close()
        rest.delete.assert_called_once()
        assert src._rest_session is None

    def test_close_noop_when_not_connected(self):
        src = VMwareSource()
        src.close()  # should not raise


# ---------------------------------------------------------------------------
# REST helpers
# ---------------------------------------------------------------------------


class TestVMwareRestHelpers:
    def test_fetch_rest_tags_returns_empty_when_no_session(self):
        src = VMwareSource()
        assert src._fetch_rest_tags() == {}

    def test_fetch_rest_tags_parses_response(self):
        src = VMwareSource()
        session = MagicMock()
        session._base_url = "https://vcenter.example.com"
        resp = MagicMock()
        resp.json.return_value = {
            "value": [
                {"object_id": {"id": "vm-42"}, "tag_ids": ["tag-1", "tag-2"]}
            ]
        }
        session.get.return_value = resp
        src._rest_session = session

        result = src._fetch_rest_tags()
        assert result == {"vm-42": {"tag-1": True, "tag-2": True}}

    def test_fetch_rest_tags_returns_empty_on_error(self):
        src = VMwareSource()
        session = MagicMock()
        session._base_url = "https://vcenter.example.com"
        session.get.side_effect = Exception("network error")
        src._rest_session = session

        result = src._fetch_rest_tags()
        assert result == {}


# ---------------------------------------------------------------------------
# VMware VLAN enrichment helpers
# ---------------------------------------------------------------------------


def _make_dvs_portgroup(key: str, name: str, vlan_id: int) -> MagicMock:
    """Return a mock DVS DistributedVirtualPortgroup and register its type."""
    pg = MagicMock()
    pg.key = key
    pg.name = name
    pg.config.defaultPortConfig.vlan.vlanId = vlan_id
    # Make isinstance(pg, vim.dvs.DistributedVirtualPortgroup) return True
    _fake_vim.dvs.DistributedVirtualPortgroup = type(pg)
    return pg


class TestBuildPortgroupVlanMap:
    """Tests for VMwareSource._build_portgroup_vlan_map."""

    def test_returns_empty_when_no_networks(self):
        host = MagicMock()
        host.network = []
        host.config.network.portgroup = []
        src = VMwareSource()
        result = src._build_portgroup_vlan_map(host)
        assert result == {}

    def test_dvs_portgroup_indexed_by_key_and_name(self):
        pg = _make_dvs_portgroup("dvpg-10", "VLAN10-PG", 10)

        host = MagicMock()
        host.network = [pg]
        host.config.network.portgroup = []

        src = VMwareSource()
        result = src._build_portgroup_vlan_map(host)

        assert result.get("dvpg-10") == {"id": 10, "name": "VLAN10-PG"}
        assert result.get("VLAN10-PG") == {"id": 10, "name": "VLAN10-PG"}

    def test_vlan_id_zero_excluded(self):
        """VLAN ID 0 means 'no VLAN tag'; should not be included."""
        pg = _make_dvs_portgroup("dvpg-0", "NoVlan-PG", 0)

        host = MagicMock()
        host.network = [pg]
        host.config.network.portgroup = []

        src = VMwareSource()
        result = src._build_portgroup_vlan_map(host)
        assert result == {}

    def test_standard_portgroup_included(self):
        std_pg = MagicMock()
        std_pg.spec.name = "Management Network"
        std_pg.spec.vlanId = 100

        host = MagicMock()
        host.network = []
        host.config.network.portgroup = [std_pg]

        src = VMwareSource()
        result = src._build_portgroup_vlan_map(host)
        assert result.get("Management Network") == {"id": 100, "name": "Management Network"}


class TestEnrichHostVnics:
    """Tests for VMwareSource._enrich_host_vnics."""

    def test_vnic_gets_vlans_from_dvs_portgroup_key(self):
        vnic = MagicMock()
        vnic.spec.distributedVirtualPort.portgroupKey = "dvpg-20"
        vnic.spec.portgroup = None

        host = MagicMock()
        host.config.network.vnic = [vnic]

        src = VMwareSource()
        pg_to_vlan = {"dvpg-20": {"id": 20, "name": "VLAN20"}}

        with patch.object(src, "_build_portgroup_vlan_map", return_value=pg_to_vlan):
            src._enrich_host_vnics(host)

        assert vnic._vlans == [{"id": 20, "name": "VLAN20"}]

    def test_vnic_falls_back_to_portgroup_name(self):
        vnic = MagicMock()
        # No DVS port
        del vnic.spec.distributedVirtualPort
        vnic.spec.portgroup = "Management Network"

        host = MagicMock()
        host.config.network.vnic = [vnic]

        src = VMwareSource()
        pg_to_vlan = {"Management Network": {"id": 100, "name": "Management Network"}}

        with patch.object(src, "_build_portgroup_vlan_map", return_value=pg_to_vlan):
            src._enrich_host_vnics(host)

        assert vnic._vlans == [{"id": 100, "name": "Management Network"}]

    def test_vnic_with_no_vlan_gets_empty_list(self):
        vnic = MagicMock()
        vnic.spec.distributedVirtualPort.portgroupKey = "dvpg-unknown"
        vnic.spec.portgroup = "UnknownPG"

        host = MagicMock()
        host.config.network.vnic = [vnic]

        src = VMwareSource()
        with patch.object(src, "_build_portgroup_vlan_map", return_value={}):
            src._enrich_host_vnics(host)

        assert vnic._vlans == []

    def test_get_hosts_calls_enrich(self):
        src = VMwareSource()
        src._api_client = MagicMock()
        fake_host = _make_managed_obj("esxi-01")
        view = _make_container_view([fake_host])
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view

        with patch.object(src, "_enrich_host_vnics") as mock_enrich:
            src._get_hosts()
            mock_enrich.assert_called_once_with(fake_host)


class TestEnrichVmInterfaces:
    """Tests for VMwareSource._enrich_vm_interfaces."""

    def test_vm_net_gets_vlans_from_dvs_portgroup(self):
        net = MagicMock()
        net.network = "VLAN30-PG"

        pg = _make_dvs_portgroup("dvpg-30", "VLAN30-PG", 30)

        vm = MagicMock()
        vm.network = [pg]
        vm.guest.net = [net]

        src = VMwareSource()
        src._enrich_vm_interfaces(vm)

        assert net._vlans == [{"id": 30, "name": "VLAN30-PG"}]

    def test_vm_net_with_no_matching_portgroup_gets_empty_list(self):
        net = MagicMock()
        net.network = "SomeOtherNetwork"

        vm = MagicMock()
        vm.network = []
        vm.guest.net = [net]

        src = VMwareSource()
        src._enrich_vm_interfaces(vm)

        assert net._vlans == []

    def test_get_vms_calls_enrich(self):
        src = VMwareSource()
        src._api_client = MagicMock()
        fake_vm = _make_managed_obj("vm-01", "vm-10")
        view = _make_container_view([fake_vm])
        content = src._api_client.RetrieveContent.return_value
        content.viewManager.CreateContainerView.return_value = view

        with patch.object(src, "_enrich_vm_interfaces") as mock_enrich:
            src._get_vms()
            mock_enrich.assert_called_once_with(fake_vm)
