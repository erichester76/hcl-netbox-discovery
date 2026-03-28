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
        _azure_mgmt_resource = ModuleType("azure.mgmt.resource")
        _azure_mgmt_resource.ResourceManagementClient = MagicMock()
        for name, mod in [
            ("azure", _azure),
            ("azure.identity", _azure_identity),
            ("azure.mgmt", _azure_mgmt),
            ("azure.mgmt.subscription", _azure_mgmt_sub),
            ("azure.mgmt.compute", _azure_mgmt_compute),
            ("azure.mgmt.network", _azure_mgmt_network),
            ("azure.mgmt.resource", _azure_mgmt_resource),
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


# ---------------------------------------------------------------------------
# Subscription ID filtering
# ---------------------------------------------------------------------------


class TestAzureSubscriptionFilter:
    def test_filter_keeps_matching_subscriptions(self):
        subs = [
            SimpleNamespace(subscription_id="sub-1", display_name="Dev"),
            SimpleNamespace(subscription_id="sub-2", display_name="Prod"),
        ]
        config = MagicMock()
        config.extra = {"auth_method": "default", "subscription_ids": "sub-1"}

        with patch("collector.sources.azure.AzureSource._build_credential", return_value=MagicMock()):
            with patch("collector.sources.azure.AzureSource._list_subscriptions", return_value=subs):
                src = AzureSource()
                src.connect(config)

        assert len(src._subscriptions) == 1
        assert src._subscriptions[0].subscription_id == "sub-1"

    def test_filter_with_multiple_ids(self):
        subs = [
            SimpleNamespace(subscription_id="sub-1", display_name="Dev"),
            SimpleNamespace(subscription_id="sub-2", display_name="Prod"),
            SimpleNamespace(subscription_id="sub-3", display_name="Test"),
        ]
        config = MagicMock()
        config.extra = {"auth_method": "default", "subscription_ids": "sub-1, sub-3"}

        with patch("collector.sources.azure.AzureSource._build_credential", return_value=MagicMock()):
            with patch("collector.sources.azure.AzureSource._list_subscriptions", return_value=subs):
                src = AzureSource()
                src.connect(config)

        ids = {s.subscription_id for s in src._subscriptions}
        assert ids == {"sub-1", "sub-3"}

    def test_empty_filter_keeps_all(self):
        subs = [
            SimpleNamespace(subscription_id="sub-1", display_name="Dev"),
            SimpleNamespace(subscription_id="sub-2", display_name="Prod"),
        ]
        config = MagicMock()
        config.extra = {"auth_method": "default", "subscription_ids": ""}

        with patch("collector.sources.azure.AzureSource._build_credential", return_value=MagicMock()):
            with patch("collector.sources.azure.AzureSource._list_subscriptions", return_value=subs):
                src = AzureSource()
                src.connect(config)

        assert len(src._subscriptions) == 2


# ---------------------------------------------------------------------------
# _resolve_image_reference()
# ---------------------------------------------------------------------------


class TestAzureResolveImageReference:
    def test_marketplace_image_returns_original_and_empty_label(self):
        src = AzureSource()
        src._credential = MagicMock()
        img = MagicMock()
        img.id = None
        img.publisher = "Canonical"
        img.offer = "UbuntuServer"
        img.sku = "22.04-LTS"

        compute = MagicMock()
        resolved_img, label = src._resolve_image_reference(img, "sub-1", compute, "vm-01")

        assert resolved_img is img
        assert label == ""

    def test_gallery_image_resolves_identifier(self):
        src = AzureSource()
        src._credential = MagicMock()

        gallery_img_id = (
            "/subscriptions/sub-1/resourceGroups/rg-shared/providers/"
            "Microsoft.Compute/galleries/MyGallery/images/MyImage/versions/1.0.0"
        )
        img = MagicMock()
        img.id = gallery_img_id

        fake_identifier = MagicMock()
        fake_identifier.publisher = "MyPublisher"
        fake_identifier.offer = "MyOffer"
        fake_identifier.sku = "MySku"

        gallery_def = MagicMock()
        gallery_def.identifier = fake_identifier
        compute = MagicMock()
        compute.gallery_images.get.return_value = gallery_def

        resolved_img, label = src._resolve_image_reference(img, "sub-1", compute, "vm-01")

        assert resolved_img is fake_identifier
        assert "Gallery: MyGallery / MyImage" in label

    def test_gallery_image_fallback_on_api_error(self):
        src = AzureSource()
        src._credential = MagicMock()

        gallery_img_id = (
            "/subscriptions/sub-1/resourceGroups/rg-shared/providers/"
            "Microsoft.Compute/galleries/MyGallery/images/MyImage/versions/1.0.0"
        )
        img = MagicMock()
        img.id = gallery_img_id

        compute = MagicMock()
        compute.gallery_images.get.side_effect = Exception("API error")

        resolved_img, label = src._resolve_image_reference(img, "sub-1", compute, "vm-01")

        assert resolved_img is img
        assert "Gallery" in label


# ---------------------------------------------------------------------------
# _build_vm_dict() — custom_fields and image_reference
# ---------------------------------------------------------------------------


class TestAzureBuildVmDictCustomFields:
    def _make_vm(self, name="vm-01", location="eastus"):
        vm = MagicMock()
        vm.name = name
        vm.location = location
        vm.id = f"/subscriptions/sub-1/resourceGroups/rg/providers/x/{name}"
        vm.hardware_profile.vm_size = "Standard_D2s_v3"
        vm.storage_profile.image_reference.id = None
        vm.storage_profile.image_reference.publisher = "Canonical"
        vm.storage_profile.image_reference.offer = "UbuntuServer"
        vm.storage_profile.image_reference.sku = "22.04-LTS"
        vm.network_profile.network_interfaces = []
        vm.storage_profile.os_disk.disk_size_gb = None
        vm.storage_profile.data_disks = []
        return vm

    def test_vm_dict_contains_instance_type_in_custom_fields(self):
        src = AzureSource()
        src._credential = MagicMock()
        vm = self._make_vm()
        compute = MagicMock()
        compute.virtual_machines.get.side_effect = Exception("skip")
        compute.virtual_machine_sizes.list.return_value = []
        network = MagicMock()

        result = src._build_vm_dict(vm, "sub-1", "Dev", compute, network, {})

        assert result["custom_fields"]["instance_type"] == "Standard_D2s_v3"

    def test_vm_dict_contains_image_reference_in_custom_fields(self):
        src = AzureSource()
        src._credential = MagicMock()
        vm = self._make_vm()
        compute = MagicMock()
        compute.virtual_machines.get.side_effect = Exception("skip")
        compute.virtual_machine_sizes.list.return_value = []
        network = MagicMock()

        result = src._build_vm_dict(vm, "sub-1", "Dev", compute, network, {})

        assert "image_reference" in result
        assert "image_reference" in result["custom_fields"]
        assert "Marketplace" in result["image_reference"]

    def test_vm_dict_instance_type_field(self):
        src = AzureSource()
        src._credential = MagicMock()
        vm = self._make_vm()
        compute = MagicMock()
        compute.virtual_machines.get.side_effect = Exception("skip")
        compute.virtual_machine_sizes.list.return_value = []
        network = MagicMock()

        result = src._build_vm_dict(vm, "sub-1", "Dev", compute, network, {})

        assert result["instance_type"] == "Standard_D2s_v3"


# ---------------------------------------------------------------------------
# _get_appliances()
# ---------------------------------------------------------------------------


class TestAzureGetAppliances:
    def _connected_src(self, sub_id="sub-1", sub_name="Dev"):
        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = [SimpleNamespace(subscription_id=sub_id, display_name=sub_name)]
        return src

    def _patch_network_client(self, network_client):
        fake_nmc = sys.modules["azure.mgmt.network"]
        original = getattr(fake_nmc, "NetworkManagementClient", MagicMock())
        fake_nmc.NetworkManagementClient = MagicMock(return_value=network_client)
        return fake_nmc, original

    def test_returns_empty_on_all_api_errors(self):
        network = MagicMock()
        network.network_security_groups.list_all.side_effect = Exception("err")
        network.application_gateways.list_all.side_effect = Exception("err")
        network.load_balancers.list_all.side_effect = Exception("err")
        network.azure_firewalls.list_all.side_effect = Exception("err")

        src = self._connected_src()
        fake_nmc, original = self._patch_network_client(network)
        # Also stub azure.mgmt.resource so VPN path doesn't blow up
        fake_rmc = sys.modules.get("azure.mgmt.resource")
        orig_rmc = getattr(fake_rmc, "ResourceManagementClient", MagicMock()) if fake_rmc else None
        if fake_rmc:
            fake_rmc.ResourceManagementClient = MagicMock(
                return_value=MagicMock(resource_groups=MagicMock(list=MagicMock(return_value=[])))
            )
        try:
            result = src._get_appliances()
        finally:
            fake_nmc.NetworkManagementClient = original
            if fake_rmc and orig_rmc is not None:
                fake_rmc.ResourceManagementClient = orig_rmc

        assert result == []

    def test_nsg_appliance_shape(self):
        nsg = MagicMock()
        nsg.name = "my-nsg"
        nsg.location = "eastus"

        network = MagicMock()
        network.network_security_groups.list_all.return_value = [nsg]
        network.application_gateways.list_all.return_value = []
        network.load_balancers.list_all.return_value = []
        network.azure_firewalls.list_all.return_value = []

        src = self._connected_src()
        fake_nmc, original = self._patch_network_client(network)
        fake_rmc = sys.modules.get("azure.mgmt.resource")
        orig_rmc = getattr(fake_rmc, "ResourceManagementClient", MagicMock()) if fake_rmc else None
        if fake_rmc:
            fake_rmc.ResourceManagementClient = MagicMock(
                return_value=MagicMock(resource_groups=MagicMock(list=MagicMock(return_value=[])))
            )
        try:
            result = src._get_appliances()
        finally:
            fake_nmc.NetworkManagementClient = original
            if fake_rmc and orig_rmc is not None:
                fake_rmc.ResourceManagementClient = orig_rmc

        assert len(result) == 1
        item = result[0]
        assert item["name"] == "my-nsg"
        assert item["role_name"] == "Azure NSG"
        assert item["appliance_type"] == "nsg"
        assert item["location"] == "eastus"
        assert item["cluster_name"] == "Azure eastus"
        assert item["subscription_name"] == "Dev"
        assert "custom_fields" in item
        assert isinstance(item["nics"], list)

    def test_load_balancer_extracts_public_ip(self):
        lb = MagicMock()
        lb.name = "my-lb"
        lb.location = "westus"
        lb.sku.name = "Standard"

        frontend = MagicMock()
        frontend.name = "frontend-ip"
        frontend.private_ip_address = None
        pub_ip_ref = MagicMock()
        pub_ip_ref.id = "/subscriptions/sub-1/resourceGroups/rg/providers/x/publicIPAddresses/my-pip"
        frontend.public_ip_address = pub_ip_ref
        lb.frontend_ip_configurations = [frontend]

        pip_obj = MagicMock()
        pip_obj.ip_address = "20.1.2.3"
        network = MagicMock()
        network.network_security_groups.list_all.return_value = []
        network.application_gateways.list_all.return_value = []
        network.load_balancers.list_all.return_value = [lb]
        network.azure_firewalls.list_all.return_value = []
        network.public_ip_addresses.get.return_value = pip_obj

        src = self._connected_src()
        fake_nmc, original = self._patch_network_client(network)
        fake_rmc = sys.modules.get("azure.mgmt.resource")
        orig_rmc = getattr(fake_rmc, "ResourceManagementClient", MagicMock()) if fake_rmc else None
        if fake_rmc:
            fake_rmc.ResourceManagementClient = MagicMock(
                return_value=MagicMock(resource_groups=MagicMock(list=MagicMock(return_value=[])))
            )
        try:
            result = src._get_appliances()
        finally:
            fake_nmc.NetworkManagementClient = original
            if fake_rmc and orig_rmc is not None:
                fake_rmc.ResourceManagementClient = orig_rmc

        assert len(result) == 1
        item = result[0]
        assert item["role_name"] == "Azure Load Balancer"
        assert len(item["nics"]) == 1
        assert item["nics"][0]["ips"] == [{"address": "20.1.2.3/32"}]


# ---------------------------------------------------------------------------
# _get_standalone_nics()
# ---------------------------------------------------------------------------


class TestAzureGetStandaloneNics:
    def _connected_src(self):
        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = [
            SimpleNamespace(subscription_id="sub-1", display_name="Dev")
        ]
        return src

    def _patch_network(self, network_client):
        fake_nmc = sys.modules["azure.mgmt.network"]
        original = getattr(fake_nmc, "NetworkManagementClient", MagicMock())
        fake_nmc.NetworkManagementClient = MagicMock(return_value=network_client)
        return fake_nmc, original

    def test_skips_vm_attached_nics(self):
        vm_nic = MagicMock()
        vm_nic.virtual_machine = MagicMock()  # attached

        network = MagicMock()
        network.network_interfaces.list_all.return_value = [vm_nic]

        src = self._connected_src()
        fake_nmc, original = self._patch_network(network)
        try:
            result = src._get_standalone_nics()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert result == []

    def test_orphaned_nic_shape(self):
        nic = MagicMock()
        nic.name = "orphan-nic"
        nic.location = "eastus"
        nic.virtual_machine = None
        nic.private_endpoint = None
        nic.private_link_service = None
        nic.mac_address = "AA:BB:CC:DD:EE:FF"

        ip_cfg = MagicMock()
        ip_cfg.private_ip_address = "10.0.0.5"
        ip_cfg.public_ip_address = None
        nic.ip_configurations = [ip_cfg]

        network = MagicMock()
        network.network_interfaces.list_all.return_value = [nic]

        src = self._connected_src()
        fake_nmc, original = self._patch_network(network)
        try:
            result = src._get_standalone_nics()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert len(result) == 1
        item = result[0]
        assert item["name"] == "orphan-nic"
        assert item["role_name"] == "Azure Orphaned NIC"
        assert item["nic_type"] == "orphaned"
        assert item["cluster_name"] == "Azure eastus"
        assert item["nics"][0]["mac_address"] == "AA:BB:CC:DD:EE:FF"
        assert item["nics"][0]["ips"] == [{"address": "10.0.0.5/32"}]
        assert "custom_fields" in item

    def test_private_endpoint_nic_role(self):
        nic = MagicMock()
        nic.name = "pe-nic"
        nic.location = "westus"
        nic.virtual_machine = None
        nic.private_endpoint = MagicMock()  # marks as PE
        nic.private_link_service = None
        nic.mac_address = ""
        nic.ip_configurations = []

        network = MagicMock()
        network.network_interfaces.list_all.return_value = [nic]

        src = self._connected_src()
        fake_nmc, original = self._patch_network(network)
        try:
            result = src._get_standalone_nics()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert result[0]["role_name"] == "Azure Private Endpoint"
        assert result[0]["nic_type"] == "private_endpoint"

    def test_private_link_service_nic_role(self):
        nic = MagicMock()
        nic.name = "pls-nic"
        nic.location = "centralus"
        nic.virtual_machine = None
        nic.private_endpoint = None
        nic.private_link_service = MagicMock()  # marks as PLS
        nic.mac_address = ""
        nic.ip_configurations = []

        network = MagicMock()
        network.network_interfaces.list_all.return_value = [nic]

        src = self._connected_src()
        fake_nmc, original = self._patch_network(network)
        try:
            result = src._get_standalone_nics()
        finally:
            fake_nmc.NetworkManagementClient = original

        assert result[0]["role_name"] == "Azure Private Link Service"
        assert result[0]["nic_type"] == "private_link_service"

    def test_collections_registered_in_get_objects(self):
        src = AzureSource()
        src._credential = MagicMock()
        src._subscriptions = []

        # Both new collections should be recognised (return empty list with no subs)
        assert src.get_objects("appliances") == []
        assert src.get_objects("standalone_nics") == []
