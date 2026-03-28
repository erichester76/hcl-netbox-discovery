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

Subscription filtering
----------------------
To limit collection to specific subscriptions, set ``subscription_ids``
in source extras to a comma-separated list of subscription IDs:

  subscription_ids = env("AZURE_SUBSCRIPTION_IDS", "")

Supported collections
---------------------
``"vms"``
    Azure Virtual Machines across all subscriptions.  Each dict contains:

    - name, status, vcpus, memory_mb, instance_type
    - subscription_name, subscription_id  (used for tenant prerequisite)
    - location                             (used for cluster prerequisite)
    - platform_publisher, platform_offer, platform_sku, platform_name
    - image_reference                      (formatted image string)
    - custom_fields                        (dict: instance_type, image_reference)
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

``"appliances"``
    Azure network appliances (NSGs, Application Gateways, Load Balancers,
    Azure Firewalls, VPN Gateways) represented as virtual-machine-like dicts
    so they can be imported into NetBox as VMs.  Each dict contains:

    - name, status, location, cluster_name
    - role_name                  (e.g. "Azure App Gateway")
    - appliance_type             (nsg | app_gateway | load_balancer | firewall | vpn_gateway)
    - instance_type              (SKU / tier string when available)
    - subscription_name, subscription_id
    - nics  – list of ``{name, mac_address, ips: [{address}]}``

``"standalone_nics"``
    Azure network interfaces that are **not** attached to a VM.  Includes
    orphaned NICs, private-endpoint NICs, and private-link-service NICs.
    Each dict has the same shape as appliance dicts so they can be handled
    by the same HCL ``object`` block pattern.  ``role_name`` will be one of:

    - "Azure Orphaned NIC"
    - "Azure Private Endpoint"
    - "Azure Private Link Service"
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
        # Apply optional subscription ID filter
        filter_ids_raw = (config.extra or {}).get("subscription_ids", "")
        if filter_ids_raw:
            filter_ids = {s.strip() for s in filter_ids_raw.split(",") if s.strip()}
            if filter_ids:
                self._subscriptions = [
                    s for s in self._subscriptions
                    if s.subscription_id in filter_ids
                ]
                logger.info(
                    "AzureSource: subscription filter applied — %d subscription(s) after filtering",
                    len(self._subscriptions),
                )
        logger.info(
            "AzureSource connected: %d subscription(s) visible",
            len(self._subscriptions),
        )

    def get_objects(self, collection: str) -> list:
        """Return items for *collection*."""
        if self._credential is None:
            raise RuntimeError("AzureSource: connect() has not been called")

        collectors = {
            "vms":            self._get_vms,
            "prefixes":       self._get_prefixes,
            "subscriptions":  self._get_subscriptions,
            "appliances":     self._get_appliances,
            "standalone_nics": self._get_standalone_nics,
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
        image_reference    = ""

        hw = vm.hardware_profile
        if hw:
            instance_type = hw.vm_size or ""

        sp = vm.storage_profile
        if sp and sp.image_reference:
            img = sp.image_reference
            # Resolve Shared Gallery images to their definition metadata
            img, image_reference = self._resolve_image_reference(
                img, sub_id, compute, vm_name
            )
            if img is not None:
                platform_publisher = (getattr(img, "publisher", "") or "").replace("-", " ").title()
                platform_publisher = platform_publisher.replace("Microsoftwindowsserver", "Microsoft")
                platform_offer     = getattr(img, "offer", "") or ""
                platform_sku       = getattr(img, "sku", "") or ""
                platform_name      = f"{platform_offer} {platform_sku}".strip() or "Unknown"
                if not image_reference:
                    image_reference = f"Marketplace: {platform_publisher} / {platform_name}".strip(" /")

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
            "image_reference":     image_reference,
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
            "custom_fields":       {
                "instance_type":   instance_type,
                "image_reference": image_reference,
            },
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

    def _resolve_image_reference(
        self, img: Any, sub_id: str, compute: Any, vm_name: str
    ) -> tuple:
        """Resolve an image reference, handling Shared Gallery images.

        Returns ``(image_ref_obj, image_reference_str)``.  For marketplace
        images ``image_ref_obj`` is the original ``img`` object.  For gallery
        images it is the ``GalleryImageIdentifier`` of the gallery definition.
        ``image_reference_str`` is a human-readable string or empty string if
        a marketplace label will be built by the caller.
        """
        img_id = getattr(img, "id", None) or ""
        if img_id and "galleries" in img_id.lower():
            # Shared Image Gallery reference — resolve to gallery definition
            parts = img_id.split("/")
            try:
                gallery_sub_id = parts[parts.index("subscriptions") + 1]
                rg_name        = parts[parts.index("resourceGroups") + 1]
                gallery_name   = parts[parts.index("galleries") + 1]
                image_def_name = parts[parts.index("images") + 1]
            except (ValueError, IndexError):
                logger.debug("Could not parse gallery image ID for %s: %s", vm_name, img_id)
                return img, ""

            try:
                gallery_compute = compute
                if gallery_sub_id != sub_id:
                    # Import lazily to avoid hard dependency at module level
                    from azure.mgmt.compute import ComputeManagementClient  # type: ignore[import]
                    gallery_compute = ComputeManagementClient(self._credential, gallery_sub_id)
                gallery_image_def = gallery_compute.gallery_images.get(
                    resource_group_name=rg_name,
                    gallery_name=gallery_name,
                    gallery_image_name=image_def_name,
                )
                identifier = gallery_image_def.identifier
                label = f"Gallery: {gallery_name} / {image_def_name}"
                logger.debug("Resolved gallery image for %s: %s", vm_name, label)
                return identifier, label
            except Exception as exc:
                logger.warning(
                    "Failed to resolve gallery image %s/%s for %s: %s",
                    gallery_name, image_def_name, vm_name, exc,
                )
                return img, f"Gallery: {img_id.split('/')[-1]}"

        return img, ""

    # ------------------------------------------------------------------
    # Collection: appliances
    # ------------------------------------------------------------------

    def _get_appliances(self) -> list[dict]:
        """Return Azure network appliances as pseudo-VM dicts.

        Covers NSGs, Application Gateways, Load Balancers, Azure Firewalls,
        and VPN Gateways.  Each returned dict has the same top-level shape as
        a VM dict so the HCL object block can treat them uniformly.
        """
        try:
            from azure.mgmt.network import NetworkManagementClient  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "azure-mgmt-network is required. "
                "Install it with: pip install azure-mgmt-network"
            ) from exc

        try:
            from azure.mgmt.resource import ResourceManagementClient  # type: ignore[import]
        except ImportError:
            ResourceManagementClient = None  # type: ignore[assignment,misc]

        result: list[dict] = []
        for sub in self._subscriptions:
            sub_id   = sub.subscription_id
            sub_name = sub.display_name or sub_id[:8]
            network  = NetworkManagementClient(self._credential, sub_id)

            # NSGs
            try:
                for nsg in network.network_security_groups.list_all():
                    result.append(self._build_appliance_dict(
                        name=nsg.name,
                        appliance_type="nsg",
                        role_name="Azure NSG",
                        location=nsg.location or "",
                        sub_id=sub_id,
                        sub_name=sub_name,
                        instance_type="",
                        nics=[],
                    ))
            except Exception as exc:
                logger.warning("Failed to list NSGs in %s: %s", sub_id[:8], exc)

            # Application Gateways
            try:
                for appgw in network.application_gateways.list_all():
                    sku_name = getattr(getattr(appgw, "sku", None), "name", None) or ""
                    sku_tier = getattr(getattr(appgw, "sku", None), "tier", None) or ""
                    instance_type = f"App Gateway {sku_name} {sku_tier}".strip()
                    nics = self._extract_appliance_nics_from_frontend_ips(
                        appgw.frontend_ip_configurations or [],
                        network,
                    )
                    result.append(self._build_appliance_dict(
                        name=appgw.name,
                        appliance_type="app_gateway",
                        role_name="Azure App Gateway",
                        location=appgw.location or "",
                        sub_id=sub_id,
                        sub_name=sub_name,
                        instance_type=instance_type,
                        nics=nics,
                    ))
            except Exception as exc:
                logger.warning("Failed to list App Gateways in %s: %s", sub_id[:8], exc)

            # Load Balancers
            try:
                for lb in network.load_balancers.list_all():
                    sku_name = getattr(getattr(lb, "sku", None), "name", None) or ""
                    instance_type = f"Load Balancer {sku_name}".strip()
                    nics = self._extract_appliance_nics_from_frontend_ips(
                        lb.frontend_ip_configurations or [],
                        network,
                    )
                    result.append(self._build_appliance_dict(
                        name=lb.name,
                        appliance_type="load_balancer",
                        role_name="Azure Load Balancer",
                        location=lb.location or "",
                        sub_id=sub_id,
                        sub_name=sub_name,
                        instance_type=instance_type,
                        nics=nics,
                    ))
            except Exception as exc:
                logger.warning("Failed to list Load Balancers in %s: %s", sub_id[:8], exc)

            # Azure Firewalls
            try:
                for fw in network.azure_firewalls.list_all():
                    tier = getattr(getattr(fw, "sku", None), "name", None) or "Standard"
                    instance_type = f"Firewall {tier}".strip()
                    nics: list[dict] = []
                    for cfg in (fw.ip_configurations or []):
                        priv_ip = getattr(cfg, "private_ip_address", None)
                        if priv_ip:
                            nics.append({
                                "name":        "azure-firewall-subnet",
                                "mac_address": "",
                                "ips":         [{"address": f"{priv_ip}/32"}],
                            })
                    result.append(self._build_appliance_dict(
                        name=fw.name,
                        appliance_type="firewall",
                        role_name="Azure Firewall",
                        location=fw.location or "",
                        sub_id=sub_id,
                        sub_name=sub_name,
                        instance_type=instance_type,
                        nics=nics,
                    ))
            except Exception as exc:
                logger.warning("Failed to list Firewalls in %s: %s", sub_id[:8], exc)

            # VPN Gateways (listed per resource group)
            try:
                rg_iter = []
                if ResourceManagementClient is not None:
                    try:
                        resource_client = ResourceManagementClient(self._credential, sub_id)
                        rg_iter = list(resource_client.resource_groups.list())
                    except Exception as exc:
                        logger.debug("Failed to list resource groups in %s: %s", sub_id[:8], exc)
                for rg in rg_iter:
                    try:
                        for vpn in network.virtual_network_gateways.list(rg.name):
                            sku_name = getattr(getattr(vpn, "sku", None), "name", None) or ""
                            nics = []
                            for cfg in (vpn.ip_configurations or []):
                                pub_ip_ref = getattr(cfg, "public_ip_address", None)
                                if pub_ip_ref and pub_ip_ref.id:
                                    try:
                                        pip_parts = pub_ip_ref.id.split("/")
                                        pip = network.public_ip_addresses.get(pip_parts[4], pip_parts[-1])
                                        if pip.ip_address:
                                            nics.append({
                                                "name":        "gateway-subnet",
                                                "mac_address": "",
                                                "ips":         [{"address": f"{pip.ip_address}/32"}],
                                            })
                                    except Exception:
                                        pass
                            result.append(self._build_appliance_dict(
                                name=vpn.name,
                                appliance_type="vpn_gateway",
                                role_name="Azure VPN Gateway",
                                location=vpn.location or "",
                                sub_id=sub_id,
                                sub_name=sub_name,
                                instance_type=sku_name,
                                nics=nics,
                            ))
                    except Exception as exc:
                        logger.debug("Failed to list VPN Gateways in RG %s: %s", rg.name, exc)
            except Exception as exc:
                logger.warning("Failed to enumerate VPN Gateways in %s: %s", sub_id[:8], exc)

        logger.debug("AzureSource: returning %d appliance records", len(result))
        return result

    def _build_appliance_dict(
        self,
        name: str,
        appliance_type: str,
        role_name: str,
        location: str,
        sub_id: str,
        sub_name: str,
        instance_type: str,
        nics: list[dict],
    ) -> dict:
        """Return a normalized appliance dict suitable for HCL field mapping."""
        return {
            "name":              _truncate(name, 64),
            "status":            "active",
            "role_name":         role_name,
            "appliance_type":    appliance_type,
            "instance_type":     instance_type,
            "location":          location,
            "cluster_name":      f"Azure {location}".strip(),
            "subscription_name": sub_name,
            "subscription_id":   sub_id,
            "nics":              nics,
            "custom_fields":     {"instance_type": instance_type},
        }

    def _extract_appliance_nics_from_frontend_ips(
        self, frontend_configs: list, network: Any
    ) -> list[dict]:
        """Build NIC dicts from an appliance's frontend IP configuration list."""
        nics: list[dict] = []
        for frontend in frontend_configs:
            nic_name = getattr(frontend, "name", None) or "frontend"
            ips: list[dict] = []
            pub_ip_ref = getattr(frontend, "public_ip_address", None)
            if pub_ip_ref and getattr(pub_ip_ref, "id", None):
                try:
                    parts = pub_ip_ref.id.split("/")
                    pip = network.public_ip_addresses.get(parts[4], parts[-1])
                    if pip.ip_address:
                        ips.append({"address": f"{pip.ip_address}/32"})
                except Exception:
                    pass
            priv_ip = getattr(frontend, "private_ip_address", None)
            if priv_ip:
                ips.append({"address": f"{priv_ip}/32"})
            nics.append({"name": nic_name, "mac_address": "", "ips": ips})
        return nics

    # ------------------------------------------------------------------
    # Collection: standalone_nics
    # ------------------------------------------------------------------

    def _get_standalone_nics(self) -> list[dict]:
        """Return unattached NICs (orphans, private endpoints, PLS) as pseudo-VM dicts."""
        try:
            from azure.mgmt.network import NetworkManagementClient  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "azure-mgmt-network is required. "
                "Install it with: pip install azure-mgmt-network"
            ) from exc

        result: list[dict] = []
        for sub in self._subscriptions:
            sub_id   = sub.subscription_id
            sub_name = sub.display_name or sub_id[:8]
            network  = NetworkManagementClient(self._credential, sub_id)

            try:
                all_nics = list(network.network_interfaces.list_all())
            except Exception as exc:
                logger.warning("Failed to list NICs in %s: %s", sub_id[:8], exc)
                continue

            for nic in all_nics:
                # Skip NICs that are attached to VMs
                if getattr(nic, "virtual_machine", None):
                    continue

                location = getattr(nic, "location", "") or ""
                nic_name = _truncate(nic.name, 64)

                # Classify the NIC
                if getattr(nic, "private_endpoint", None):
                    role_name = "Azure Private Endpoint"
                    nic_type  = "private_endpoint"
                elif getattr(nic, "private_link_service", None):
                    role_name = "Azure Private Link Service"
                    nic_type  = "private_link_service"
                else:
                    role_name = "Azure Orphaned NIC"
                    nic_type  = "orphaned"

                # Build IP list
                ips: list[dict] = []
                for cfg in (nic.ip_configurations or []):
                    priv = getattr(cfg, "private_ip_address", None)
                    if priv:
                        ips.append({"address": f"{priv}/32"})
                    pub_ref = getattr(cfg, "public_ip_address", None)
                    if pub_ref and getattr(pub_ref, "id", None):
                        try:
                            parts = pub_ref.id.split("/")
                            pip = network.public_ip_addresses.get(parts[4], parts[-1])
                            if pip.ip_address:
                                ips.append({"address": f"{pip.ip_address}/32"})
                        except Exception:
                            pass

                nics = [{
                    "name":        "primary",
                    "mac_address": nic.mac_address or "",
                    "ips":         ips,
                }]

                result.append({
                    "name":              nic_name,
                    "status":            "active",
                    "role_name":         role_name,
                    "nic_type":          nic_type,
                    "instance_type":     f"NIC ({nic_type})",
                    "location":          location,
                    "cluster_name":      f"Azure {location}".strip(),
                    "subscription_name": sub_name,
                    "subscription_id":   sub_id,
                    "nics":              nics,
                    "custom_fields":     {"instance_type": f"NIC ({nic_type})"},
                })

        logger.debug("AzureSource: returning %d standalone NIC records", len(result))
        return result

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
