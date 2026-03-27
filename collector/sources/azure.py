"""Microsoft Azure data source adapter.

Fetches Azure resources across all visible subscriptions using the
``azure-mgmt-compute``, ``azure-mgmt-network``, and ``azure-identity``
packages.  Returns plain Python dicts suitable for HCL field expressions.

Authentication
--------------
The adapter uses ``DefaultAzureCredential`` by default, which works with
environment variables, managed identities, and ``az login`` sessions.  To
use an explicit service principal, set the following source extras:

  auth_method = "service_principal"
  tenant_id   = env("AZURE_TENANT_ID")   # client_id via source.username
                                           # client_secret via source.password

Supported collections
---------------------
``"vms"``
    Azure Virtual Machines across all subscriptions.  Each dict contains:

    - name, status, vcpus, memory_mb, instance_type
    - subscription_name, subscription_id  (used for tenant prerequisite)
    - location                             (used for cluster prerequisite)
    - platform_publisher, platform_offer, platform_sku, platform_name
    - nics  – list of ``{name, mac_address, ips: [{address}]}``
    - disks – list of ``{name, size_mb}``

``"prefixes"``
    Azure VNet address spaces and subnet prefixes.  Each dict contains:

    - prefix   (CIDR notation)
    - description
    - is_vnet  (True for VNet-level ranges, False for subnets)
    - vnet_name, subnet_name (subnet_name is empty string for VNet ranges)
    - subscription_name, subscription_id

``"subscriptions"``
    Raw subscription records (id, display_name).  Useful for building
    tenant prerequisites in a dedicated object block.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)


class AzureSource(DataSource):
    """Azure SDK-backed source adapter."""

    def __init__(self) -> None:
        self._credential: Optional[Any] = None
        self._subscriptions: list[Any] = []
        self._config: Optional[Any] = None

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Authenticate to Azure using settings from *config*."""
        self._config = config
        self._credential = self._build_credential(config)
        self._subscriptions = self._list_subscriptions()
        logger.info(
            "AzureSource connected: %d subscription(s) visible",
            len(self._subscriptions),
        )

    def get_objects(self, collection: str) -> list:
        """Return items for *collection*."""
        if self._credential is None:
            raise RuntimeError("AzureSource: connect() has not been called")

        collectors = {
            "vms":           self._get_vms,
            "prefixes":      self._get_prefixes,
            "subscriptions": self._get_subscriptions,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"AzureSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Release credential references."""
        self._credential = None
        self._subscriptions = []

    # ------------------------------------------------------------------
    # Credential helpers
    # ------------------------------------------------------------------

    def _build_credential(self, config: Any) -> Any:
        """Build an Azure credential object from the source config."""
        try:
            from azure.identity import (  # type: ignore[import]
                ClientSecretCredential,
                DefaultAzureCredential,
            )
        except ImportError as exc:
            raise RuntimeError(
                "azure-identity is required for the Azure source adapter. "
                "Install it with: pip install azure-identity"
            ) from exc

        auth_method = (config.extra.get("auth_method") or "default").lower()
        if auth_method == "service_principal":
            tenant_id = config.extra.get("tenant_id", "")
            if not tenant_id:
                raise ValueError(
                    "AzureSource: 'tenant_id' must be set in source.extra "
                    "when auth_method = 'service_principal'"
                )
            logger.info("Azure auth: service principal (tenant=%s)", tenant_id)
            return ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=config.username,
                client_secret=config.password,
            )
        logger.info("Azure auth: DefaultAzureCredential")
        return DefaultAzureCredential()

    def _list_subscriptions(self) -> list:
        """Return all visible Azure subscriptions."""
        try:
            from azure.mgmt.subscription import SubscriptionClient  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "azure-mgmt-subscription is required. "
                "Install it with: pip install azure-mgmt-subscription"
            ) from exc
        client = SubscriptionClient(self._credential)
        subs = list(client.subscriptions.list())
        logger.debug("AzureSource: %d subscription(s) found", len(subs))
        return subs

    # ------------------------------------------------------------------
    # Collection: subscriptions
    # ------------------------------------------------------------------

    def _get_subscriptions(self) -> list[dict]:
        return [
            {
                "id":           sub.subscription_id,
                "display_name": sub.display_name,
            }
            for sub in self._subscriptions
        ]

    # ------------------------------------------------------------------
    # Collection: prefixes
    # ------------------------------------------------------------------

    def _get_prefixes(self) -> list[dict]:
        """Return VNet address spaces and subnet prefixes from all subscriptions."""
        try:
            from azure.mgmt.network import NetworkManagementClient  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "azure-mgmt-network is required. "
                "Install it with: pip install azure-mgmt-network"
            ) from exc

        prefixes: list[dict] = []
        for sub in self._subscriptions:
            sub_id   = sub.subscription_id
            sub_name = sub.display_name or sub_id[:8]
            network_client = NetworkManagementClient(self._credential, sub_id)

            try:
                vnets = list(network_client.virtual_networks.list_all())
            except Exception as exc:
                logger.warning("Failed to list VNets in subscription %s: %s", sub_id[:8], exc)
                continue

            for vnet in vnets:
                vnet_name = vnet.name
                # VNet-level address spaces
                for cidr in (vnet.address_space.address_prefixes or []):
                    if cidr and "/" in cidr:
                        prefixes.append({
                            "prefix":            cidr,
                            "description":       f"Azure VNet: {vnet_name} ({sub_name})",
                            "is_vnet":           True,
                            "vnet_name":         vnet_name,
                            "subnet_name":       "",
                            "subscription_name": sub_name,
                            "subscription_id":   sub_id,
                            "location":          vnet.location or "",
                            "status":            "active",
                        })
                # Subnet prefixes
                rg = vnet.id.split("/")[4] if vnet.id else ""
                try:
                    subnets = list(network_client.subnets.list(rg, vnet_name))
                except Exception as exc:
                    logger.warning("Failed to list subnets for VNet %s: %s", vnet_name, exc)
                    subnets = []

                for subnet in subnets:
                    cidr = subnet.address_prefix
                    if cidr and "/" in cidr:
                        prefixes.append({
                            "prefix":            cidr,
                            "description":       f"Azure Subnet: {subnet.name} (VNet: {vnet_name}, {sub_name})",
                            "is_vnet":           False,
                            "vnet_name":         vnet_name,
                            "subnet_name":       subnet.name,
                            "subscription_name": sub_name,
                            "subscription_id":   sub_id,
                            "location":          vnet.location or "",
                            "status":            "active",
                        })

        logger.debug("AzureSource: returning %d prefix records", len(prefixes))
        return prefixes

    # ------------------------------------------------------------------
    # Collection: vms
    # ------------------------------------------------------------------

    def _get_vms(self) -> list[dict]:
        """Return all VMs with NIC, IP and disk details across all subscriptions."""
        try:
            from azure.mgmt.compute import ComputeManagementClient  # type: ignore[import]
            from azure.mgmt.network import NetworkManagementClient  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "azure-mgmt-compute and azure-mgmt-network are required. "
                "Install them with: pip install azure-mgmt-compute azure-mgmt-network"
            ) from exc

        result: list[dict] = []
        for sub in self._subscriptions:
            sub_id   = sub.subscription_id
            sub_name = sub.display_name or sub_id[:8]
            compute  = ComputeManagementClient(self._credential, sub_id)
            network  = NetworkManagementClient(self._credential, sub_id)

            # Build NIC lookup indexed by NIC resource ID (lower-case)
            try:
                all_nics = {nic.id.lower(): nic for nic in compute._list_nics_via_network(network)}
            except Exception:
                # Fallback: list NICs directly
                try:
                    all_nics = {nic.id.lower(): nic for nic in network.network_interfaces.list_all()}
                except Exception as exc:
                    logger.warning("Failed to list NICs in subscription %s: %s", sub_id[:8], exc)
                    all_nics = {}

            try:
                vms = list(compute.virtual_machines.list_all())
            except Exception as exc:
                logger.warning("Failed to list VMs in subscription %s: %s", sub_id[:8], exc)
                continue

            for vm in vms:
                vm_dict = self._build_vm_dict(vm, sub_id, sub_name, compute, network, all_nics)
                result.append(vm_dict)

        logger.debug("AzureSource: returning %d VM records", len(result))
        return result

    def _build_vm_dict(
        self,
        vm: Any,
        sub_id: str,
        sub_name: str,
        compute: Any,
        network: Any,
        all_nics: dict,
    ) -> dict:
        """Build a single VM dict from an Azure VirtualMachine SDK object."""
        vm_name  = _truncate(vm.name, 64)
        location = vm.location or ""
        rg       = vm.id.split("/")[4] if vm.id else ""

        # Power state
        status = self._get_vm_status(vm, rg, compute)

        # Image / platform info
        platform_publisher = ""
        platform_offer     = ""
        platform_sku       = ""
        platform_name      = "Unknown"
        instance_type      = ""

        hw = vm.hardware_profile
        if hw:
            instance_type = hw.vm_size or ""

        sp = vm.storage_profile
        if sp and sp.image_reference:
            img = sp.image_reference
            platform_publisher = (img.publisher or "").replace("-", " ").title()
            platform_publisher = platform_publisher.replace("Microsoftwindowsserver", "Microsoft")
            platform_offer     = img.offer or ""
            platform_sku       = img.sku or ""
            platform_name      = f"{platform_offer} {platform_sku}".strip() or "Unknown"

        # NICs and IPs
        nics_data  = self._get_vm_nics(vm, network, all_nics)

        # Disks
        disks_data = self._get_vm_disks(sp)

        # vCPUs and memory (fetched from size catalog on first call, then cached)
        vcpus     = None
        memory_mb = None
        if instance_type:
            size_info = self._get_vm_size(location, instance_type, compute)
            if size_info:
                vcpus     = size_info.get("vcpus")
                memory_mb = size_info.get("memory_mb")

        return {
            "name":                vm_name,
            "status":              status,
            "vcpus":               vcpus,
            "memory":              memory_mb,
            "instance_type":       instance_type,
            "subscription_name":   sub_name,
            "subscription_id":     sub_id,
            "location":            location,
            "cluster_name":        f"Azure {location}".strip(),
            "platform_publisher":  platform_publisher,
            "platform_offer":      platform_offer,
            "platform_sku":        platform_sku,
            "platform_name":       platform_name,
            "nics":                nics_data,
            "disks":               disks_data,
        }

    # ------------------------------------------------------------------
    # VM detail helpers
    # ------------------------------------------------------------------

    def _get_vm_status(self, vm: Any, rg: str, compute: Any) -> str:
        """Return NetBox status string from Azure VM power state."""
        try:
            iv = compute.virtual_machines.get(
                resource_group_name=rg,
                vm_name=vm.name,
                expand="instanceView",
            ).instance_view
            for status in (iv.statuses or []):
                code = status.code or ""
                if code.startswith("PowerState/"):
                    power = code.split("/")[-1].lower()
                    if power == "running":
                        return "active"
                    if power in ("stopped", "deallocated", "stopping", "deallocating", "starting", "creating"):
                        return "offline"
                    if power in ("failed", "unhealthy"):
                        return "failed"
        except Exception as exc:
            logger.debug("Failed to get instance view for %s: %s", vm.name, exc)
        return "active"

    def _get_vm_nics(self, vm: Any, network: Any, all_nics: dict) -> list[dict]:
        """Return NIC dicts for the VM."""
        nics: list[dict] = []
        np = vm.network_profile
        if not np:
            return nics
        for nic_ref in (np.network_interfaces or []):
            nic_id = (nic_ref.id or "").lower()
            nic_obj = all_nics.get(nic_id)
            if nic_obj is None:
                # Try direct lookup
                try:
                    parts = nic_ref.id.split("/")
                    nic_rg = parts[4]
                    nic_name = parts[-1]
                    nic_obj = network.network_interfaces.get(nic_rg, nic_name)
                except Exception:
                    pass
            if nic_obj is None:
                continue

            ips: list[dict] = []
            for cfg in (nic_obj.ip_configurations or []):
                if cfg.private_ip_address:
                    ips.append({"address": f"{cfg.private_ip_address}/32"})
                if cfg.public_ip_address:
                    try:
                        pip_id   = cfg.public_ip_address.id
                        pip_rg   = pip_id.split("/")[4]
                        pip_name = pip_id.split("/")[-1]
                        pip      = network.public_ip_addresses.get(pip_rg, pip_name)
                        if pip.ip_address:
                            ips.append({"address": f"{pip.ip_address}/32"})
                    except Exception:
                        pass

            nics.append({
                "name":         _truncate(nic_obj.name, 64),
                "mac_address":  nic_obj.mac_address or "",
                "ips":          ips,
            })
        return nics

    def _get_vm_disks(self, sp: Any) -> list[dict]:
        """Return disk dicts for the VM."""
        disks: list[dict] = []
        if not sp:
            return disks
        if sp.os_disk and sp.os_disk.disk_size_gb:
            disks.append({
                "name":    "os-disk",
                "size_mb": sp.os_disk.disk_size_gb * 1024,
            })
        for idx, data_disk in enumerate(sp.data_disks or [], start=1):
            size_gb = getattr(data_disk, "disk_size_gb", None)
            if size_gb:
                disks.append({
                    "name":    f"data-disk-{idx}",
                    "size_mb": size_gb * 1024,
                })
        return disks

    # VM size cache: {(location, size_name): {vcpus, memory_mb}}
    _size_cache: dict = {}

    def _get_vm_size(self, location: str, size_name: str, compute: Any) -> Optional[dict]:
        """Look up vCPU / memory for a VM size, with a per-run in-memory cache."""
        cache_key = (location, size_name)
        if cache_key in self._size_cache:
            return self._size_cache[cache_key]
        try:
            for size in compute.virtual_machine_sizes.list(location):
                if size.name.lower() == size_name.lower():
                    entry = {
                        "vcpus":     size.number_of_cores,
                        "memory_mb": size.memory_in_mb,
                    }
                    self._size_cache[cache_key] = entry
                    return entry
        except Exception as exc:
            logger.debug("VM size lookup failed for %s/%s: %s", location, size_name, exc)
        self._size_cache[cache_key] = None
        return None


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _truncate(value: Any, n: int) -> str:
    """Truncate *value* to *n* characters, stripping domain suffixes."""
    if not value:
        return ""
    s = str(value)
    if "." in s:
        s = s.split(".")[0]
    return s[:n]


# Monkey-patch helper used internally (avoids circular import issues)
def _list_nics_via_network(compute_client: Any, network_client: Any) -> list:
    """List all NICs using the network client (helper for AzureSource._get_vms)."""
    return list(network_client.network_interfaces.list_all())


# Attach the helper as a method so it can be used inside _get_vms without
# importing the network client type again.
import types as _types
ComputeManagementClientProxy = None
try:
    from azure.mgmt.compute import ComputeManagementClient as _CMC  # type: ignore[import]
    _CMC._list_nics_via_network = lambda self, network: list(network.network_interfaces.list_all())
except ImportError:
    pass
