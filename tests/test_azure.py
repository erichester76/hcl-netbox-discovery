"""Tests for the Azure source adapter (collector/sources/azure.py).

All Azure SDK calls are mocked so that no real Azure subscription is needed.
The azure-identity / azure-mgmt-* packages are optional runtime deps; this
module injects lightweight fake modules into sys.modules so the adapter can
be exercised without installing the Azure SDK.
"""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Inject fake azure modules so the adapter imports succeed in CI / dev envs
# that don't have the Azure SDK installed.
# ---------------------------------------------------------------------------
def _ensure_fake_azure():
    """Create lightweight stub azure.* modules if the real ones are absent."""
    if "azure" in sys.modules and not isinstance(sys.modules["azure"], ModuleType):
        return  # already mocked
    if "azure.identity" not in sys.modules:
        _azure = ModuleType("azure")
        _azure_identity = ModuleType("azure.identity")
        _azure_identity.DefaultAzureCredential = MagicMock()
        _azure_identity.ClientSecretCredential = MagicMock()
        _azure_mgmt = ModuleType("azure.mgmt")
        _azure_mgmt_sub = ModuleType("azure.mgmt.subscription")
        _azure_mgmt_sub.SubscriptionClient = MagicMock()
        _azure_mgmt_compute = ModuleType("azure.mgmt.compute")
        _azure_mgmt_compute.ComputeManagementClient = MagicMock()
        _azure_mgmt_network = ModuleType("azure.mgmt.network")
        _azure_mgmt_network.NetworkManagementClient = MagicMock()
        for name, mod in [
            ("azure", _azure),
            ("azure.identity", _azure_identity),
            ("azure.mgmt", _azure_mgmt),
            ("azure.mgmt.subscription", _azure_mgmt_sub),
            ("azure.mgmt.compute", _azure_mgmt_compute),
            ("azure.mgmt.network", _azure_mgmt_network),
        ]:
            sys.modules.setdefault(name, mod)


_ensure_fake_azure()

from collector.sources.azure import AzureSource, _truncate  # noqa: E402


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


class TestTruncate:
    def test_truncate_basic(self):
        assert _truncate("hello", 3) == "hel"

    def test_truncate_strips_domain(self):
        assert _truncate("vm-01.example.com", 64) == "vm-01"

    def test_truncate_empty_string(self):
        assert _truncate("", 64) == ""

    def test_truncate_none(self):
        assert _truncate(None, 64) == ""

    def test_truncate_exact_length(self):
        assert _truncate("abcde", 5) == "abcde"


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestAzureConnect:
    def test_connect_default_credential(self, azure_config):
        azure_config.extra["auth_method"] = "default"
        fake_cred = MagicMock()
        fake_sub_client = MagicMock()
        fake_sub_client.subscriptions.list.return_value = []

        with patch("collector.sources.azure.AzureSource._build_credential", return_value=fake_cred):
            with patch("collector.sources.azure.AzureSource._list_subscriptions", return_value=[]):
                src = AzureSource()
                src.connect(azure_config)

        assert src._credential is fake_cred
        assert src._subscriptions == []

    def test_connect_service_principal(self, azure_config):
        fake_cred = MagicMock()
        fake_subs = [SimpleNamespace(subscription_id="sub-1", display_name="Dev")]

        with patch("collector.sources.azure.AzureSource._build_credential", return_value=fake_cred):
            with patch("collector.sources.azure.AzureSource._list_subscriptions", return_value=fake_subs):
                src = AzureSource()
                src.connect(azure_config)

        assert len(src._subscriptions) == 1

    def test_connect_raises_if_tenant_id_missing(self, azure_config):
        azure_config.extra.pop("tenant_id")
        src = AzureSource()
        with pytest.raises(ValueError, match="tenant_id"):
            src._build_credential(azure_config)

    def test_connect_raises_if_azure_identity_missing(self, azure_config):
        src = AzureSource()
        with patch.dict("sys.modules", {"azure.identity": None}):
            with pytest.raises((RuntimeError, ImportError)):
                src.connect(azure_config)


# ---------------------------------------------------------------------------
# get_objects()
# ---------------------------------------------------------------------------


class TestAzureGetObjects:
    def _connected_source(self, subscriptions=None) -> AzureSource:
        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = subscriptions or []
        src._config = MagicMock()
        return src

    def test_raises_without_connect(self):
        src = AzureSource()
        with pytest.raises(RuntimeError, match="connect\\(\\) has not been called"):
            src.get_objects("vms")

    def test_raises_for_unknown_collection(self):
        src = self._connected_source()
        with pytest.raises(ValueError, match="unknown collection"):
            src.get_objects("not_a_thing")

    def test_get_subscriptions(self):
        sub = SimpleNamespace(subscription_id="sub-1", display_name="Prod")
        src = self._connected_source(subscriptions=[sub])
        result = src.get_objects("subscriptions")
        assert result == [{"id": "sub-1", "display_name": "Prod"}]

    def test_get_subscriptions_empty(self):
        src = self._connected_source(subscriptions=[])
        assert src.get_objects("subscriptions") == []

    def test_collection_names_case_insensitive(self):
        sub = SimpleNamespace(subscription_id="sub-1", display_name="Dev")
        src = self._connected_source(subscriptions=[sub])
        result = src.get_objects("SUBSCRIPTIONS")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _get_subscriptions()
# ---------------------------------------------------------------------------


class TestAzureGetSubscriptions:
    def test_returns_id_and_name(self):
        src = AzureSource()
        src._subscriptions = [
            SimpleNamespace(subscription_id="sub-abc", display_name="My Sub"),
        ]
        result = src._get_subscriptions()
        assert result == [{"id": "sub-abc", "display_name": "My Sub"}]


# ---------------------------------------------------------------------------
# _get_prefixes()
# ---------------------------------------------------------------------------


class TestAzureGetPrefixes:
    def test_returns_vnet_and_subnet_prefixes(self):
        sub = SimpleNamespace(subscription_id="sub-1", display_name="Dev")

        vnet = MagicMock()
        vnet.name = "vnet-east"
        vnet.location = "eastus"
        vnet.id = "/subscriptions/sub-1/resourceGroups/rg-east/providers/Microsoft.Network/virtualNetworks/vnet-east"
        vnet.address_space.address_prefixes = ["10.0.0.0/16"]

        subnet = MagicMock()
        subnet.name = "subnet-a"
        subnet.address_prefix = "10.0.1.0/24"

        network_client = MagicMock()
        network_client.virtual_networks.list_all.return_value = [vnet]
        network_client.subnets.list.return_value = [subnet]

        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = [sub]

        # Patch the NetworkManagementClient inside the azure.mgmt.network fake module
        fake_nmc = sys.modules["azure.mgmt.network"]
        original = getattr(fake_nmc, "NetworkManagementClient", MagicMock())
        fake_nmc.NetworkManagementClient = MagicMock(return_value=network_client)
        try:
            result = src._get_prefixes()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert len(result) == 2
        vnet_prefix = next(r for r in result if r["is_vnet"])
        subnet_prefix = next(r for r in result if not r["is_vnet"])

        assert vnet_prefix["prefix"] == "10.0.0.0/16"
        assert vnet_prefix["vnet_name"] == "vnet-east"
        assert subnet_prefix["prefix"] == "10.0.1.0/24"
        assert subnet_prefix["subnet_name"] == "subnet-a"

    def test_skips_prefixes_without_slash(self):
        sub = SimpleNamespace(subscription_id="sub-1", display_name="Dev")

        vnet = MagicMock()
        vnet.name = "vnet-east"
        vnet.location = "eastus"
        vnet.id = "/subscriptions/sub-1/resourceGroups/rg/providers/x/vnets/vnet-east"
        vnet.address_space.address_prefixes = ["not-a-cidr"]

        network_client = MagicMock()
        network_client.virtual_networks.list_all.return_value = [vnet]
        network_client.subnets.list.return_value = []

        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = [sub]

        fake_nmc = sys.modules["azure.mgmt.network"]
        original = getattr(fake_nmc, "NetworkManagementClient", MagicMock())
        fake_nmc.NetworkManagementClient = MagicMock(return_value=network_client)
        try:
            result = src._get_prefixes()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert result == []

    def test_continues_on_vnet_list_failure(self):
        sub = SimpleNamespace(subscription_id="sub-1", display_name="Dev")
        network_client = MagicMock()
        network_client.virtual_networks.list_all.side_effect = Exception("API error")

        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = [sub]

        fake_nmc = sys.modules["azure.mgmt.network"]
        original = getattr(fake_nmc, "NetworkManagementClient", MagicMock())
        fake_nmc.NetworkManagementClient = MagicMock(return_value=network_client)
        try:
            result = src._get_prefixes()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert result == []


# ---------------------------------------------------------------------------
# _get_vm_status()
# ---------------------------------------------------------------------------


class TestAzureVMStatus:
    def _make_status(self, code: str) -> MagicMock:
        s = MagicMock()
        s.code = code
        return s

    def _make_instance_view(self, codes: list[str]) -> MagicMock:
        iv = MagicMock()
        iv.statuses = [self._make_status(c) for c in codes]
        return iv

    def test_running_returns_active(self):
        src = AzureSource()
        vm = MagicMock()
        vm.name = "vm-01"
        compute = MagicMock()
        compute.virtual_machines.get.return_value.instance_view = self._make_instance_view(
            ["ProvisioningState/succeeded", "PowerState/running"]
        )
        result = src._get_vm_status(vm, "rg-east", compute)
        assert result == "active"

    def test_deallocated_returns_offline(self):
        src = AzureSource()
        vm = MagicMock()
        vm.name = "vm-01"
        compute = MagicMock()
        compute.virtual_machines.get.return_value.instance_view = self._make_instance_view(
            ["PowerState/deallocated"]
        )
        assert src._get_vm_status(vm, "rg", compute) == "offline"

    def test_failed_returns_failed(self):
        src = AzureSource()
        vm = MagicMock()
        vm.name = "vm-01"
        compute = MagicMock()
        compute.virtual_machines.get.return_value.instance_view = self._make_instance_view(
            ["PowerState/failed"]
        )
        assert src._get_vm_status(vm, "rg", compute) == "failed"

    def test_defaults_to_active_on_api_error(self):
        src = AzureSource()
        vm = MagicMock()
        vm.name = "vm-01"
        compute = MagicMock()
        compute.virtual_machines.get.side_effect = Exception("API error")
        assert src._get_vm_status(vm, "rg", compute) == "active"


# ---------------------------------------------------------------------------
# _get_vm_disks()
# ---------------------------------------------------------------------------


class TestAzureVMDisks:
    def test_os_disk_included(self):
        src = AzureSource()
        sp = MagicMock()
        sp.os_disk.disk_size_gb = 128
        sp.data_disks = []
        result = src._get_vm_disks(sp)
        assert result == [{"name": "os-disk", "size_mb": 128 * 1024}]

    def test_data_disks_included(self):
        src = AzureSource()
        sp = MagicMock()
        sp.os_disk.disk_size_gb = 64
        data_disk = MagicMock()
        data_disk.disk_size_gb = 512
        sp.data_disks = [data_disk]
        result = src._get_vm_disks(sp)
        assert len(result) == 2
        assert result[1] == {"name": "data-disk-1", "size_mb": 512 * 1024}

    def test_returns_empty_when_no_storage_profile(self):
        src = AzureSource()
        assert src._get_vm_disks(None) == []

    def test_data_disk_without_size_skipped(self):
        src = AzureSource()
        sp = MagicMock()
        sp.os_disk.disk_size_gb = None
        data_disk = MagicMock()
        data_disk.disk_size_gb = None
        sp.data_disks = [data_disk]
        result = src._get_vm_disks(sp)
        assert result == []


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestAzureClose:
    def test_close_clears_credential_and_subscriptions(self, azure_config):
        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = [MagicMock()]
        src.close()
        assert src._credential is None
        assert src._subscriptions == []
