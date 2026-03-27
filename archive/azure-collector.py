import os
import re
import sys
from dotenv import load_dotenv
import logging
import argparse
import urllib3
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from cu_tools import (
    get_netbox_client,
    create_or_update,
    get_with_cache,
    run_precache,
    get_find_function,
    get_create_function
)

load_dotenv()

# Suppress urllib3 InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress Azure SDK noise
logging.getLogger('azure').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)
logging.getLogger('azure.identity').setLevel(logging.WARNING)
logging.getLogger('azure.mgmt').setLevel(logging.WARNING)

def truncate_name(name, max_length=64):
    if '.' in name:
        name = name.split('.')[0]
    if len(name) > max_length:
        logger.warning(f"Name truncated: {name[:max_length]}")
        name = name[:max_length]
    return name


def cached_find(object_type, nb, **query_params):
    find_func = get_find_function(object_type, nb)
    return get_with_cache(object_type, find_func, query_params)


def live_find(object_type, nb, **query_params):
    find_func = get_find_function(object_type, nb)
    return find_func(**query_params)


def ensure_ip_on_interface(nb, address, interface_id, tenant_id, tag_ids):
    existing_ip = live_find('ip_address', nb, address=address)
    if existing_ip:
        if getattr(existing_ip, 'assigned_object_id', None) == interface_id:
            return existing_ip
        logger.warning(
            f"Skipping IP {address} reassignment from interface "
            f"{getattr(existing_ip, 'assigned_object_id', 'unknown')} to {interface_id}"
        )
        return None

    ip_data = {
        'address': address,
        'assigned_object_type': 'virtualization.vminterface',
        'assigned_object_id': interface_id,
        'tenant': tenant_id,
        'status': 'active',
        'tags': tag_ids,
    }
    ip_id = create_or_update('ip_address', ip_data)
    if not ip_id:
        return None

    ip_obj = live_find('ip_address', nb, id=ip_id)
    if ip_obj and getattr(ip_obj, 'assigned_object_id', None) == interface_id:
        return ip_obj
    return None


def get_azure_credentials(use_interactive=False):
    if use_interactive:
        logger.info("Using interactive browser authentication for Azure")
        return InteractiveBrowserCredential()
    else:
        logger.info("Using default Azure credential chain")
        return DefaultAzureCredential()


def get_azure_subscriptions(credential):
    logger.info("Fetching Azure subscriptions...")
    client = SubscriptionClient(credential)
    subscriptions = list(client.subscriptions.list())
    logger.info(f"Found {len(subscriptions)} subscriptions")
    return subscriptions


# Azure Routines
def get_vnets_and_subnets(subscription_id, credential):
    logger.info(f"Fetching VNets and subnets in subscription {subscription_id[:8]}...")
    network_client = NetworkManagementClient(credential, subscription_id)
    vnets = list(network_client.virtual_networks.list_all())

    vnet_data = []
    for vnet in vnets:
        rg_name = vnet.id.split('/')[4]
        vnet_name = vnet.name

        vnet_info = {
            'name': vnet.name,
            'id': vnet.id,
            'resource_group': rg_name,
            'location': vnet.location,
            'address_space': [p for p in vnet.address_space.address_prefixes if p and '/' in p],
            'subnets': []
        }

        # Fetch detailed subnets to get NSG associations
        try:
            subnets = network_client.subnets.list(rg_name, vnet_name)
            for subnet in subnets:
                if subnet.address_prefix and '/' in subnet.address_prefix:
                    subnet_info = {
                        'name': subnet.name,
                        'id': subnet.id,
                        'address_prefix': subnet.address_prefix,
                        'network_security_group': subnet.network_security_group
                    }
                    vnet_info['subnets'].append(subnet_info)
        except Exception as e:
            logger.warning(f"Failed to fetch detailed subnets for VNet {vnet_name}: {e}")

        vnet_data.append(vnet_info)
    logger.info(f"Found {len(vnets)} VNets with detailed subnets")
    return vnet_data


def get_vms_and_network_appliances(subscription_id, credential):
    logger.info(f"Fetching VMs, NSGs, Gateways, Load Balancers, etc. in subscription {subscription_id[:8]}...")

    network_client = NetworkManagementClient(credential, subscription_id)
    compute_client = ComputeManagementClient(credential, subscription_id)
    resource_client = ResourceManagementClient(credential, subscription_id)

    vms = list(compute_client.virtual_machines.list_all())
    nics = list(network_client.network_interfaces.list_all())
    nsgs = list(network_client.network_security_groups.list_all())
    app_gateways = list(network_client.application_gateways.list_all())
    load_balancers = list(network_client.load_balancers.list_all())
    firewalls = list(network_client.azure_firewalls.list_all())

    # VPN Gateways - list per resource group
    vpn_gateways = []
    resource_groups = list(resource_client.resource_groups.list())
    for rg in resource_groups:
        try:
            gateways_in_rg = network_client.virtual_network_gateways.list(rg.name)
            vpn_gateways.extend(gateways_in_rg)
        except Exception as e:
            logger.warning(f"Failed to list VPN gateways in resource group {rg.name}: {e}")

    vms_dict = {vm.id.lower(): {'vm': vm, 'interfaces': []} for vm in vms}
    standalone_nics = []
    standalone_nics = []              # true orphans
    private_endpoint_nics = []        # private endpoints (outbound to PaaS)
    private_link_service_nics = []    # private link services (inbound to your services)
    for nic in nics:
        if nic.virtual_machine:
            # attached to VM -> already handled
            vm_id = nic.virtual_machine.id.lower()
            if vm_id in vms_dict:
                vms_dict[vm_id]['interfaces'].append({
                    'nic': nic,
                    'mac_address': nic.mac_address
                })
        else:
            # unattached -> classify further
            if hasattr(nic, 'private_endpoint') and nic.private_endpoint is not None:
                private_endpoint_nics.append({
                    'nic': nic,
                    'mac_address': nic.mac_address,
                    'private_endpoint_id': nic.private_endpoint.id if nic.private_endpoint else None
                })
            elif hasattr(nic, 'private_link_service') and nic.private_link_service is not None:
                private_link_service_nics.append({
                    'nic': nic,
                    'mac_address': nic.mac_address,
                    'private_link_service_id': nic.private_link_service.id if nic.private_link_service else None
                })
            else:
                standalone_nics.append({
                    'nic': nic,
                    'mac_address': nic.mac_address
                })

    appliances = {
        'nsgs': nsgs,
        'app_gateways': app_gateways,
        'load_balancers': load_balancers,
        'firewalls': firewalls,
        'vpn_gateways': vpn_gateways,
    }

    logger.info(f"Found {len(vms)} VMs, {len(nsgs)} NSGs, {len(app_gateways)} App Gateways, "
                f"{len(load_balancers)} Load Balancers, {len(firewalls)} Firewalls, {len(vpn_gateways)} VPN Gateways")

    return {
        'vms': list(vms_dict.values()),
        'standalone_nics': standalone_nics,
        'private_endpoint_nics': private_endpoint_nics,
        'private_link_service_nics': private_link_service_nics,
        'appliances': appliances,
        'compute_client': compute_client,
        'network_client': network_client,
        'nics': nics,
    }



# Netbox Routines
def get_or_create_tag(nb, name, slug, description):
    """Get or create a tag using cu_tools."""
    tag_data = {
        'name': name,
        'slug': slug,
        'description': description
    }
    tag_id = create_or_update('tag', tag_data)
    if tag_id:
        return cached_find('tag', nb, id=tag_id)
    return None


def get_or_create_prefix(nb, value, defaults, tenant_id):
    """Get or create a prefix using cu_tools."""
    if not value or '/' not in value:
        logger.warning(f"Skipping invalid prefix: {value}")
        return None, False

    prefix_data = {
        'prefix': value,
        'tenant': tenant_id,
        **defaults
    }

    # Check if prefix exists before creating
    find_func = get_find_function('prefix', nb)
    existing = get_with_cache('prefix', find_func, {'prefix': value})
    was_created = existing is None

    prefix_id = create_or_update('prefix', prefix_data)

    if prefix_id:
        prefix = get_with_cache('prefix', find_func, {'id': prefix_id})
        if was_created:
            logger.info(f"Created new prefix with tenant: {value}")
        return prefix, was_created

    return None, False


def get_or_create_platform(nb, platform, manufacturer, offer="", sku=""):
    """Get or create a platform using cu_tools."""

    # First ensure manufacturer exists
    manufacturer_data = {
        'name': manufacturer,
        'slug': manufacturer.lower().replace(" ", "-"),
        'description': f"Azure publisher: {manufacturer}"
    }
    manufacturer_id = create_or_update('manufacturer', manufacturer_data)

    if not manufacturer_id:
        logger.warning(f"Failed to create manufacturer {manufacturer}, using fallback")
        # Try to get "Unknown" manufacturer as fallback
        find_func = get_find_function('manufacturer', nb)
        fallback = find_func(name="Unknown")
        manufacturer_id = fallback.id if fallback else None

    # Now create or update platform
    slug = platform.lower().replace(" ", "-").replace(".", "-")
    platform_data = {
        'name': platform,
        'slug': slug,
        'manufacturer': manufacturer_id,
        'description': f"Azure image: publisher={manufacturer}, offer={offer}, sku={sku}"
    }

    platform_id = create_or_update('platform', platform_data)

    if platform_id:
        return cached_find('platform', nb, id=platform_id)

    # Fallback to Unknown platform
    logger.warning(f"Failed to create platform {platform}, trying fallback")
    find_func = get_find_function('platform', nb)
    return find_func(name="Unknown")


def get_or_create_cluster(nb, name, tags):
    """Get or create a cluster using cu_tools."""
    # First ensure cluster type exists
    find_func = get_find_function('cluster_type', nb)
    cluster_types = []
    try:
        # Try to find Azure cluster type
        all_types = find_func()
        if hasattr(all_types, '__iter__'):
            cluster_types = [ct for ct in all_types if "azure" in ct.name.lower()]
    except:
        pass

    cluster_type_id = None
    if cluster_types:
        cluster_type_id = cluster_types[0].id
    else:
        # Create Azure cluster type
        cluster_type_data = {
            'name': 'Azure',
            'slug': 'azure'
        }
        cluster_type_id = create_or_update('cluster_type', cluster_type_data)

    # Now create or get cluster
    cluster_data = {
        'name': name.upper(),
        'type': cluster_type_id,
        'status': 'active',
        'tags': tags
    }

    cluster_id = create_or_update('cluster', cluster_data)

    if cluster_id:
        return cached_find('cluster', nb, id=cluster_id)
    return None


def get_or_create_vm_role(nb, name, tags):
    """Get or create a device role using cu_tools."""
    slug = name.lower().replace(" ", "-")

    device_role_data = {
        'name': name,
        'slug': slug,
        'vm_role': True,
        'tags': tags
    }

    role_id = create_or_update('device_role', device_role_data, preserve_existing_tags=True)

    if role_id:
        return cached_find('device_role', nb, id=role_id)
    return None

def get_or_create_tenant(nb, subscription_name, sub_id, tags):
    """Get or create a tenant using cu_tools."""
    slug = f"azure-sub-{sub_id[:8]}".lower()

    # Extract project_id from subscription name (first xxx-xxx pattern before space)
    project_id = subscription_name.split()[0] if subscription_name and ' ' in subscription_name else subscription_name

    tenant_data = {
        'name': subscription_name,
        'slug': slug,
        'description': f"Azure Subscription {sub_id}",
        'tags': tags,
        'custom_fields': {'project_id': project_id}
    }

    tenant_id = create_or_update('tenant', tenant_data)

    if tenant_id:
        return cached_find('tenant', nb, id=tenant_id)
    return None


def get_or_create_size_tag(nb, size_value, azure_tag):
    """Get or create a size tag using cu_tools."""
    if not size_value:
        return None

    tag_slug = f"azure-size-{size_value.lower().replace('_', '-').replace(' ', '-')}"
    tag_data = {
        'name': f"Azure Size: {size_value}",
        'slug': tag_slug,
        'description': f"Azure instance/SKU: {size_value}"
    }

    tag_id = create_or_update('tag', tag_data)

    if tag_id:
        return cached_find('tag', nb, id=tag_id)
    return None

def sync_unattached_nics(
    nb,
    standalone_nics,
    private_endpoint_nics,
    private_link_service_nics,
    tenant,
    azure_tag,
    network_client,
    vnets
    ):

    def create_pseudo_nic_vm_and_interface(

        nic_obj,
        nic_name,
        location,
        role_name,
        description_suffix,
        instance_type_cf,
        intf_name="primary",
        extra_tags=None,
    ):
        """Create a pseudo VM and interface for an unattached NIC using cu_tools."""
        cluster_name = f"Azure {location}" if location else "Azure Global Resources"
        cluster = get_or_create_cluster(nb, cluster_name, [azure_tag.id])

        role = get_or_create_vm_role(nb, role_name, [azure_tag.id])

        platform = get_or_create_platform(nb, "Azure Network Interface", "Microsoft")

        # Create or update VM
        vm_data = {
            'name': nic_name,
            'cluster': cluster.id,
            'role': role.id,
            'tenant': tenant.id,
            'status': 'active',
            'platform': platform.id if platform else None,
            'tags': [azure_tag.id] + (extra_tags or []),
            'custom_fields': {'instance_type': instance_type_cf}
        }

        vm_id = create_or_update('virtual_machine', vm_data)
        if not vm_id:
            logger.error(f"Failed creating {role_name} pseudo-VM {nic_name}")
            return None, None

        nb_vm = cached_find('virtual_machine', nb, id=vm_id)

        # Create or update interface
        intf_data = {
            'virtual_machine': nb_vm.id,
            'name': intf_name,
            'type': 'virtual',
            'mac_address': nic_obj.mac_address,
            'description': f"{description_suffix} - {nic_name}",
            'tags': [azure_tag.id]
        }

        intf_id = create_or_update('vminterface', intf_data)
        if not intf_id:
            logger.error(f"Failed creating interface on {nic_name}")
            return nb_vm, None

        nb_intf = cached_find('vminterface', nb, id=intf_id)

        return nb_vm, nb_intf

    def assign_ips_to_interface(nb_intf, nic_obj, tenant_id, azure_tag_id):
        """Assign IPs to an interface using cu_tools."""
        assigned_ips = []
        primary_candidate = None

        for cfg in nic_obj.ip_configurations or []:
            if not cfg.private_ip_address:
                continue

            priv_str = f"{cfg.private_ip_address}/32"
            ip_priv = ensure_ip_on_interface(nb, priv_str, nb_intf.id, tenant_id, [azure_tag_id])
            if ip_priv:
                assigned_ips.append(ip_priv)
                if not primary_candidate and not priv_str.startswith(('8.', '20.', '52.', '13.', '35.')):
                    primary_candidate = ip_priv

            if cfg.public_ip_address:
                try:
                    pip_id_parts = cfg.public_ip_address.id.split('/')
                    rg = pip_id_parts[4]
                    pip_name = pip_id_parts[-1]
                    pip = network_client.public_ip_addresses.get(rg, pip_name)
                    if pip.ip_address:
                        pub_str = f"{pip.ip_address}/32"
                        ip_pub = ensure_ip_on_interface(nb, pub_str, nb_intf.id, tenant_id, [azure_tag_id])
                        if ip_pub:
                            assigned_ips.append(ip_pub)
                except Exception as e:
                    logger.warning(f"Failed to attach public IP for {pip_name}: {e}")

        valid_assigned_ips = [
            ip for ip in assigned_ips
            if ip and getattr(ip, 'assigned_object_id', None) == nb_intf.id
        ]

        if valid_assigned_ips and nb_intf.virtual_machine and not nb_intf.virtual_machine.primary_ip4:
            # Update VM with primary IP
            vm_update_data = {
                'name': nb_intf.virtual_machine.name,
                'cluster': nb_intf.virtual_machine.cluster.id,
                'primary_ip4': primary_candidate.id if primary_candidate else valid_assigned_ips[0].id
            }
            create_or_update('virtual_machine', vm_update_data)
            logger.info(f"Set primary_ip4 on {nb_intf.virtual_machine.name}")

    for item in standalone_nics:
        nic = item['nic']
        name = truncate_name(nic.name)
        loc = getattr(nic, 'location', None)

        vm, intf = create_pseudo_nic_vm_and_interface(
            nic, name, loc,
            role_name="Azure Orphaned NIC",
            description_suffix="Detached/orphaned NIC",
            instance_type_cf="Orphaned NIC",
            intf_name="primary",
        )
        if intf:
            assign_ips_to_interface(intf, nic, tenant.id, azure_tag.id)

    for item in private_endpoint_nics:
        nic = item['nic']
        name = truncate_name(nic.name)
        loc = getattr(nic, 'location', None)
        pe_name = "unknown-pe"

        if item.get('private_endpoint_id'):
            try:
                rg = nic.id.split('/')[4]
                pe_res_name = item['private_endpoint_id'].split('/')[-1]
                pe = network_client.private_endpoints.get(rg, pe_res_name)
                pe_name = pe.name
            except:
                pass

        vm, intf = create_pseudo_nic_vm_and_interface(
            nic, name, loc,
            role_name="Azure Private Endpoint",
            description_suffix=f"Private Endpoint linked to {pe_name}",
            instance_type_cf=f"Private Endpoint: {pe_name}",
            intf_name="endpoint",
        )
        if intf:
            assign_ips_to_interface(intf, nic, tenant.id, azure_tag.id)

    for item in private_link_service_nics:
        nic = item['nic']
        name = truncate_name(nic.name)
        loc = getattr(nic, 'location', None)
        pls_name = "unknown-pls"

        if item.get('private_link_service_id'):
            try:
                rg = nic.id.split('/')[4]
                pls_res_name = item['private_link_service_id'].split('/')[-1]
                pls = network_client.private_link_services.get(rg, pls_res_name)
                pls_name = pls.name
            except:
                pass

        vm, intf = create_pseudo_nic_vm_and_interface(
            nic, name, loc,
            role_name="Azure Private Link Service",
            description_suffix=f"PLS frontend for {pls_name}",
            instance_type_cf=f"PLS Frontend: {pls_name}",
            intf_name="frontend",
            extra_tags=[]
        )
        if intf:
            assign_ips_to_interface(intf, nic, tenant.id, azure_tag.id)

    logger.debug("Completed unattached NICs sync (orphans + private endpoints + private link services)")

def sync_network_appliances(nb, appliances, tenant, azure_tag, network_client, vnets, nics):
    def create_appliance_vm(name, role_name, location, serial, instance_type=None, extra_tags=None):
        """Create or update an appliance VM using cu_tools."""
        cluster_name = f"Azure {location}"
        cluster = get_or_create_cluster(nb, cluster_name, [azure_tag.id])
        role = get_or_create_vm_role(nb, role_name, [azure_tag.id])
        platform = get_or_create_platform(nb, 'Azure Appliance', 'Microsoft')

        # Prepare VM data
        vm_data = {
            'name': name,
            'cluster': cluster.id,
            'role': role.id,
            'tenant': tenant.id,
            'status': 'active',
            'platform': platform.id if platform else None,
            'tags': [azure_tag.id] + (extra_tags or [])
        }

        # Add custom fields if provided
        if instance_type:
            vm_data['custom_fields'] = {'instance_type': instance_type}

        vm_id = create_or_update('virtual_machine', vm_data)
        if not vm_id:
            logger.error(f"Failed to create {role_name} VM {name}")
            return None

        return cached_find('virtual_machine', nb, id=vm_id)

    # NSGs - one interface per directly attached subnet + cable to prefix
    for nsg in appliances.get('nsgs', []):
        nsg_name = truncate_name(nsg.name)
        nb_appliance = create_appliance_vm(nsg_name, "Azure NSG", nsg.location, nsg.id)
        if not nb_appliance:
            continue

        attached_subnets = []
        for vnet in vnets:
            for subnet in vnet['subnets']:
                nsg_ref = subnet.get('network_security_group')
                if nsg_ref and hasattr(nsg_ref, 'id') and nsg_ref.id == nsg.id:
                    attached_subnets.append(subnet)

        logger.debug(f"NSG {nsg_name} - Attached subnets found (direct association): {len(attached_subnets)}")
        logger.debug(f"Attached subnets names: {[s['name'] for s in attached_subnets]}")

        for subnet in attached_subnets:
            intf_name = f"subnet-{subnet['name']}-{subnet['address_prefix'].replace('/', '-')}"

            # Create or update interface using cu_tools
            intf_data = {
                'virtual_machine': nb_appliance.id,
                'name': intf_name,
                'type': 'virtual',
                'description': f"Direct attachment to subnet {subnet['address_prefix']}",
                'tags': [azure_tag.id]
            }

            intf_id = create_or_update('vminterface', intf_data)
            if not intf_id:
                logger.warning(f"Failed to create NSG interface '{intf_name}' for {nsg_name}")
                continue

            intf = cached_find('vminterface', nb, id=intf_id)

            # Get prefix and create cable
            subnet_prefix = cached_find('prefix', nb, prefix=subnet['address_prefix'])
            if subnet_prefix:
                # Cable creation - using direct API as cu_tools doesn't have cable support yet
                try:
                    create_cable_func = get_create_function('cable', nb)
                    create_cable_func({
                        'a_terminations': [{
                            'object_type': 'virtualization.vminterface',
                            'object_id': intf.id
                        }],
                        'b_terminations': [{
                            'object_type': 'ipam.prefix',
                            'object_id': subnet_prefix.id
                        }],
                        'status': 'connected',
                        'tags': [azure_tag.id]
                    })
                    logger.info(f"Cabled NSG interface {intf_name} -> Prefix {subnet['address_prefix']}")
                except Exception as e:
                    logger.warning(f"Failed to cable NSG interface to prefix {subnet['address_prefix']}: {e}")

    # Application Gateways
    for appgw in appliances.get('app_gateways', []):
        appgw_name = truncate_name(appgw.name)
        sku_name = appgw.sku.name if appgw.sku else None
        sku_tier = appgw.sku.tier if appgw.sku and hasattr(appgw.sku, 'tier') else None
        sku_value = f"{sku_name}-{sku_tier}" if sku_name and sku_tier else sku_name or None
        instance_type = f"App Gateway {sku_name or 'Unknown'} {sku_tier or ''}".strip()
        nb_appliance = create_appliance_vm(appgw_name, "Azure App Gateway", appgw.location, appgw.id, instance_type)
        if nb_appliance:
            for frontend in appgw.frontend_ip_configurations or []:
                intf_name = frontend.name

                # Create or update interface
                intf_data = {
                    'virtual_machine': nb_appliance.id,
                    'name': intf_name,
                    'type': 'virtual',
                    'tags': [azure_tag.id]
                }

                intf_id = create_or_update('vminterface', intf_data)
                if not intf_id:
                    continue

                intf = cached_find('vminterface', nb, id=intf_id)

                if frontend.public_ip_address:
                    try:
                        public_ip_res = network_client.public_ip_addresses.get(
                            frontend.public_ip_address.id.split('/')[4],
                            frontend.public_ip_address.id.split('/')[-1]
                        )
                        if public_ip_res.ip_address:
                            ip_str = f"{public_ip_res.ip_address}/32"
                            ip_obj = ensure_ip_on_interface(nb, ip_str, intf.id, tenant.id, [azure_tag.id])
                            ip_id = ip_obj.id if ip_obj else None
                            if ip_id and not nb_appliance.primary_ip4:
                                # Update VM with primary IP
                                vm_update_data = {
                                    'name': appgw_name,
                                    'cluster': nb_appliance.cluster.id,
                                    'primary_ip4': ip_id
                                }
                                create_or_update('virtual_machine', vm_update_data)
                    except Exception as e:
                        logger.warning(f"Failed to attach public IP for app gateway {appgw_name}: {e}")

    # Load Balancers
    for lb in appliances.get('load_balancers', []):
        lb_name = truncate_name(lb.name)
        sku_value = lb.sku.name if lb.sku else None
        instance_type = f"Load Balancer {lb.sku.name if lb.sku else 'Unknown'}"
        nb_appliance = create_appliance_vm(lb_name, "Azure Load Balancer", lb.location, lb.id, instance_type)
        if nb_appliance:
            for frontend in lb.frontend_ip_configurations or []:
                intf_name = frontend.name

                # Create or update interface
                intf_data = {
                    'virtual_machine': nb_appliance.id,
                    'name': intf_name,
                    'type': 'virtual',
                    'tags': [azure_tag.id]
                }

                intf_id = create_or_update('vminterface', intf_data)
                if not intf_id:
                    continue

                intf = cached_find('vminterface', nb, id=intf_id)

                if frontend.public_ip_address:
                    try:
                        public_ip_res = network_client.public_ip_addresses.get(
                            frontend.public_ip_address.id.split('/')[4],
                            frontend.public_ip_address.id.split('/')[-1]
                        )
                        if public_ip_res.ip_address:
                            ip_str = f"{public_ip_res.ip_address}/32"
                            ip_obj = ensure_ip_on_interface(nb, ip_str, intf.id, tenant.id, [azure_tag.id])
                            ip_id = ip_obj.id if ip_obj else None
                            if ip_id and not nb_appliance.primary_ip4:
                                # Update VM with primary IP
                                vm_update_data = {
                                    'name': lb_name,
                                    'cluster': nb_appliance.cluster.id,
                                    'primary_ip4': ip_id
                                }
                                create_or_update('virtual_machine', vm_update_data)
                    except Exception as e:
                        logger.warning(f"Failed to attach public IP for load balancer {lb_name}: {e}")

   # Azure Firewalls
    for fw in appliances.get('firewalls', []):
        fw_name = truncate_name(fw.name)
        firewall_tier = fw.sku.name if fw.sku and fw.sku.name else "Standard"
        instance_type = f"Firewall {firewall_tier}"
        nb_appliance = create_appliance_vm(fw_name, "Azure Firewall", fw.location, fw.id, instance_type)
        if nb_appliance:
            intf_name = "azure-firewall-subnet"

            # Create or update interface
            intf_data = {
                'virtual_machine': nb_appliance.id,
                'name': intf_name,
                'type': 'virtual',
                'tags': [azure_tag.id]
            }

            intf_id = create_or_update('vminterface', intf_data)
            if intf_id:
                intf = cached_find('vminterface', nb, id=intf_id)

                if fw.ip_configurations:
                    for config in fw.ip_configurations:
                        if config.private_ip_address:
                            ip_str = f"{config.private_ip_address}/32"
                            ip_obj = ensure_ip_on_interface(nb, ip_str, intf.id, tenant.id, [azure_tag.id])
                            ip_id = ip_obj.id if ip_obj else None
                            if ip_id and not nb_appliance.primary_ip4:
                                # Update VM with primary IP
                                vm_update_data = {
                                    'name': fw_name,
                                    'cluster': nb_appliance.cluster.id,
                                    'primary_ip4': ip_id
                                }
                                create_or_update('virtual_machine', vm_update_data)

    # VPN Gateways
    for vpn in appliances.get('vpn_gateways', []):
        vpn_name = truncate_name(vpn.name)
        sku_value = vpn.sku.name if vpn.sku else None
        nb_appliance = create_appliance_vm(vpn_name, "Azure VPN Gateway", vpn.location, vpn.id, sku_value)
        if nb_appliance:
            intf_name = "gateway-subnet"

            # Create or update interface
            intf_data = {
                'virtual_machine': nb_appliance.id,
                'name': intf_name,
                'type': 'virtual',
                'tags': [azure_tag.id]
            }

            intf_id = create_or_update('vminterface', intf_data)
            if intf_id:
                intf = cached_find('vminterface', nb, id=intf_id)

                if vpn.ip_configurations:
                    for config in vpn.ip_configurations:
                        if config.public_ip_address:
                            try:
                                public_ip_res = network_client.public_ip_addresses.get(
                                    config.public_ip_address.id.split('/')[4],
                                    config.public_ip_address.id.split('/')[-1]
                                )
                                if public_ip_res.ip_address:
                                    ip_str = f"{public_ip_res.ip_address}/32"
                                    ip_obj = ensure_ip_on_interface(nb, ip_str, intf.id, tenant.id, [azure_tag.id])
                                    ip_id = ip_obj.id if ip_obj else None
                                    if ip_id and not nb_appliance.primary_ip4:
                                        # Update VM with primary IP
                                        vm_update_data = {
                                            'name': vpn_name,
                                            'cluster': nb_appliance.cluster.id,
                                            'primary_ip4': ip_id
                                        }
                                        create_or_update('virtual_machine', vm_update_data)
                            except Exception as e:
                                logger.warning(f"Failed to attach public IP for VPN gateway {vpn_name}: {e}")

    logger.debug("Network appliances synced")


def sync_to_netbox(all_network_data, netbox_url, netbox_token):
    logger.info(f"Starting sync to NetBox at {netbox_url}")

    # Get NetBox client using cu_tools
    client_info = get_netbox_client()
    nb = client_info.get("client")

    if nb is None:
        logger.error("Failed to initialize NetBox client")
        return

    azure_tag = get_or_create_tag(nb, "Azure Sync", "azure-sync", "Synced from Azure")

    for subscription_data in all_network_data:
        sub_id = subscription_data['subscription_id']
        sub_name = subscription_data.get('subscription_name', sub_id[:8])
        logger.info(f"Processing subscription: {sub_name}")

        tenant = get_or_create_tenant(nb, sub_name, sub_id, [azure_tag.id])

        compute_client = subscription_data['compute_client']
        network_client = subscription_data['network_client']
        credential = subscription_data['credential']

        # Prefixes
        vnet_prefixes = {}
        for vnet in subscription_data['vnets']:
            for addr in vnet['address_space']:
                prefix, _ = get_or_create_prefix(nb, addr, {
                    'description': f"Azure VNet: {vnet['name']} (Sub: {sub_name})",
                    'status': 'active',
                    'tags': [azure_tag.id]
                }, tenant.id)
                if prefix:
                    vnet_prefixes[vnet['id']] = prefix

            for subnet in vnet['subnets']:
                subnet_defaults = {
                    'description': f"Azure Subnet: {subnet['name']} (VNet: {vnet['name']})",
                    'status': 'active',
                    'tags': [azure_tag.id]
                }
                prefix, created = get_or_create_prefix(nb, subnet['address_prefix'], subnet_defaults, tenant.id)

                if created and vnet['id'] in vnet_prefixes:
                    parent = vnet_prefixes[vnet['id']]
                    try:
                        prefix.parent = parent.id
                        prefix.save()
                        logger.info(f"Assigned parent to new subnet prefix: {subnet['address_prefix']}")
                    except Exception as e:
                        logger.warning(f"Failed to set parent on new prefix {subnet['address_prefix']}: {e}")

        # Sync VMs
        for vm_info in subscription_data['vms']:
            vm = vm_info['vm']
            location = vm.location
            cluster = get_or_create_cluster(nb, f"Azure {location}", [azure_tag.id])

            vm_name = truncate_name(vm.name)
            vm_role = get_or_create_vm_role(nb, "Azure VM", [azure_tag.id])

            # Fetch VM instance view to get current power state
            vm_power_status = 'active'  # Default fallback
            try:
                instance_view = compute_client.virtual_machines.get(
                    resource_group_name=vm.id.split('/')[4],
                    vm_name=vm.name,
                    expand='instanceView'
                ).instance_view

                # Get the power state from statuses
                for status in instance_view.statuses or []:
                    if status.code.startswith('PowerState/'):
                        power_code = status.code.split('/')[-1].lower()
                        if power_code == 'running':
                            vm_power_status = 'active'
                        elif power_code in ('stopped', 'deallocated'):
                            vm_power_status = 'offline'
                        elif power_code in ('stopping', 'deallocating'):
                            vm_power_status = 'offline'
                        elif power_code == 'starting':
                            vm_power_status = 'offline'
                        elif power_code == 'creating':
                            vm_power_status = 'offline'
                        elif power_code in ('failed', 'unhealthy'):
                            vm_power_status = 'failed'
                        break  # First power state wins

                logger.debug(f"VM {vm_name} Azure power state: {vm_power_status}")
            except Exception as e:
                logger.warning(f"Failed to fetch instance view for {vm_name}: {e}")

            # Create or update VM using cu_tools
            vm_data = {
                'name': vm_name,
                'cluster': cluster.id,
                'role': vm_role.id,
                'tenant': tenant.id,
                'status': vm_power_status,
                'tags': [azure_tag.id]
            }

            vm_id = create_or_update('virtual_machine', vm_data)
            if not vm_id:
                logger.error(f"Failed to create/update VM {vm_name}")
                continue

            # Get the VM object for further updates
            nb_vm = cached_find('virtual_machine', nb, id=vm_id)

            image_ref = vm.storage_profile.image_reference
            precise_platform = "Unknown"
            platform_clean = "Unknown"
            publisher_clean= "Unknown"
            image_ref_formatted = "Unknown"

            # Collect all VM updates in one dict
            vm_updates = {}

            if image_ref:

                # Shared Gallery image - parse RG, gallery, image def, version from ID
                if image_ref.id and 'galleries' in image_ref.id.lower():
                    parts = image_ref.id.split('/')
                    rg_name = None
                    gallery_name = "UnknownGallery"
                    image_def_name = "UnknownImage"

                    try:
                        sub_idx = parts.index('subscriptions')
                        gallery_sub_id = parts[sub_idx + 1]
                        rg_idx = parts.index('resourceGroups')
                        rg_name = parts[rg_idx + 1]
                        gallery_idx = parts.index('galleries')
                        gallery_name = parts[gallery_idx + 1]
                        image_idx = parts.index('images')
                        image_def_name = parts[image_idx + 1]


                    except ValueError:
                        pass

                    if rg_name and gallery_name and image_def_name:
                        try:
                            # Fetch Gallery Image Definition (use the parsed gallery RG)
                            logger.debug(f"Gallery lookup for {vm_name}: gallery={gallery_name}, definition={image_def_name}, rg={rg_name}")
                            gallery_compute_client = ComputeManagementClient(credential, gallery_sub_id)
                            gallery_image_def = gallery_compute_client.gallery_images.get(
                                resource_group_name=rg_name,
                                gallery_name=gallery_name,
                                gallery_image_name=image_def_name
                            )
                            image_ref = gallery_image_def.identifier
                            image_ref_formatted = f"Gallery: {gallery_name} / {image_def_name}"
                        except Exception as e:
                            logger.warning(f"Failed to fetch gallery image definition {image_def_name} in RG {rg_name} for {vm_name}: {e}")


                publisher = image_ref.publisher
                offer = image_ref.offer or ""
                sku = image_ref.sku or ""

                precise_platform = f"{offer} {sku}".strip()
                result = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', precise_platform)
                platform_clean = " ".join(word.title() for word in result.split("-"))
                platform_clean = ".".join(word.title() for word in platform_clean.split("_"))

                logging.debug(f"Transformed Platform {precise_platform} into {platform_clean}")

                publisher_clean = " ".join(word.title() for word in publisher.split("-"))
                publisher_clean = re.sub(r'Microsoftwindowsserver', 'Microsoft', publisher_clean)

                if not 'Gallery' in image_ref_formatted: image_ref_formatted = f"MarketPlace: {publisher_clean} / {platform_clean}"

                # Prepare custom field updates
                vm_updates['custom_fields'] = {
                    'image': image_ref_formatted
                }

                # Set cf_instance_type custom field (internal name: 'instance_type')
                vm_size_name = vm.hardware_profile.vm_size if hasattr(vm.hardware_profile, 'vm_size') else None
                if vm_size_name:
                    vm_updates['custom_fields']['instance_type'] = vm_size_name

            # Get platform
            platform = get_or_create_platform(nb, platform_clean, publisher_clean, offer, sku)
            if platform:
                vm_updates['platform'] = platform.id

            # Sync vCPUs and Memory from VM size
            vm_size_name = vm.hardware_profile.vm_size if hasattr(vm.hardware_profile, 'vm_size') else None
            if vm_size_name:
                try:
                    vm_sizes = compute_client.virtual_machine_sizes.list(location)
                    for size in vm_sizes:
                        if size.name.lower() == vm_size_name.lower():
                            vm_updates['vcpus'] = size.number_of_cores
                            vm_updates['memory'] = size.memory_in_mb
                            logger.debug(f"Will update resources for {vm_name}: vCPUs={size.number_of_cores}, Memory={size.memory_in_mb} MB")
                            break
                    else:
                        logger.warning(f"VM size {vm_size_name} not found in region {location} for {vm_name}")
                except Exception as e:
                    logger.warning(f"Failed to fetch VM size details for {vm_name}: {e}")

            # Apply all updates in one call if there are any
            if vm_updates:
                vm_updates.update({
                    'name': vm_name,
                    'cluster': cluster.id,
                    'role': vm_role.id,
                    'tenant': tenant.id,
                    'status': vm_power_status,
                    'tags': [azure_tag.id]
                })
                vm_id = create_or_update('virtual_machine', vm_updates)
                if vm_id:
                    nb_vm = cached_find('virtual_machine', nb, id=vm_id)

            # Create interfaces for the VM
            created_ips = []
            for intf_info in vm_info['interfaces']:
                nic = intf_info['nic']
                intf_name = truncate_name(nic.name)

                # Create or update interface using cu_tools
                intf_data = {
                    'virtual_machine': nb_vm.id,
                    'name': intf_name,
                    'type': 'virtual',
                    'mac_address': nic.mac_address,
                    'tags': [azure_tag.id]
                }

                intf_id = create_or_update('vminterface', intf_data)
                if not intf_id:
                    logger.warning(f"Failed to create/update interface {intf_name} for {vm_name}")
                    continue

                nb_intf = cached_find('vminterface', nb, id=intf_id)

                # Assign IPs using cu_tools
                for ip_config in nic.ip_configurations:
                    if ip_config.private_ip_address:
                        ip_str = f"{ip_config.private_ip_address}/32"

                        ip = ensure_ip_on_interface(nb, ip_str, nb_intf.id, tenant.id, [azure_tag.id])
                        if ip:
                            created_ips.append(ip)

            # Set primary IPv4
            valid_created_ips = [
                ip for ip in created_ips
                if ip and getattr(ip, 'assigned_object_id', None) == nb_intf.id
            ]

            if valid_created_ips:
                primary_ip = next((ip for ip in valid_created_ips if ip.address.split('/')[0].startswith('10.') or
                                  ip.address.split('/')[0].startswith('192.168.') or
                                  ip.address.split('/')[0].startswith('172.')), None) or valid_created_ips[0]

                # Update VM with primary IP using cu_tools
                vm_primary_ip_data = {
                    'name': vm_name,
                    'cluster': cluster.id,
                    'primary_ip4': primary_ip.id
                }
                create_or_update('virtual_machine', vm_primary_ip_data)
                logger.info(f"Set primary IPv4 for {vm_name}: {primary_ip.address}")

            # Virtual Disks
            if hasattr(vm.storage_profile, 'os_disk') and vm.storage_profile.os_disk.disk_size_gb:
                disk_size_mb = vm.storage_profile.os_disk.disk_size_gb * 1024
                disk_data = {
                    'virtual_machine': nb_vm.id,
                    'name': "os-disk",
                    'size': disk_size_mb,
                    'description': "OS Disk from Azure"
                }
                create_or_update('virtualdisk', disk_data)

            if hasattr(vm.storage_profile, 'data_disks'):
                for idx, data_disk in enumerate(vm.storage_profile.data_disks, 1):
                    if hasattr(data_disk, 'disk_size_gb') and data_disk.disk_size_gb:
                        disk_size_mb = data_disk.disk_size_gb * 1024
                        disk_data = {
                            'virtual_machine': nb_vm.id,
                            'name': f"data-disk-{idx}",
                            'size': disk_size_mb,
                            'description': f"Data Disk {idx} from Azure"
                        }
                        create_or_update('virtualdisk', disk_data)

        # Sync network appliances
        sync_network_appliances(
            nb,
            subscription_data['appliances'],
            tenant,
            azure_tag,
            subscription_data['network_client'],
            subscription_data['vnets'],
            subscription_data['nics']
        )

        sync_unattached_nics(
            nb,
            subscription_data['standalone_nics'],
            subscription_data['private_endpoint_nics'],
            subscription_data['private_link_service_nics'],
            tenant,
            azure_tag,
            subscription_data['network_client'],
            subscription_data['vnets']
        )

    logger.info("NetBox sync completed")

def parse_arguments():
    parser = argparse.ArgumentParser(description='Azure to NetBox sync')
    parser.add_argument('--netbox-url', default=os.environ.get('NETBOX_URL'))
    parser.add_argument('--netbox-token', default=os.environ.get('NETBOX_TOKEN'))
    parser.add_argument('--interactive', action='store_true')
    parser.add_argument('--subscription-id')
    return parser.parse_args()


def main():
    args = parse_arguments()

    if not args.netbox_url or not args.netbox_token:
        logger.error("NetBox URL and token are required")
        sys.exit(1)

    try:
        credential = get_azure_credentials(args.interactive)

        if args.subscription_id:
            subscriptions = [type('Sub', (), {
                'subscription_id': args.subscription_id,
                'display_name': f"Sub-{args.subscription_id}"
            })]
        else:
            subscriptions = get_azure_subscriptions(credential)

        all_data = []
        for sub in subscriptions:
            sub_id = sub.subscription_id
            network_client = NetworkManagementClient(credential, sub_id)
            compute_client = ComputeManagementClient(credential, sub_id)

            vnets = get_vnets_and_subnets(sub_id, credential)
            vm_and_appliances = get_vms_and_network_appliances(sub_id, credential)

            all_data.append({
                'subscription_id': sub_id,
                'subscription_name': getattr(sub, 'display_name', sub_id[:8]),
                'vnets': vnets,
                'vms': vm_and_appliances['vms'],
                'standalone_nics': vm_and_appliances['standalone_nics'],
                'private_endpoint_nics': vm_and_appliances['private_endpoint_nics'],
                'private_link_service_nics': vm_and_appliances['private_link_service_nics'],
                'appliances': vm_and_appliances['appliances'],
                'compute_client': compute_client,
                'network_client': network_client,
                'credential': credential,
                'nics': vm_and_appliances['nics']
            })

        sync_to_netbox(all_data, args.netbox_url, args.netbox_token)

    except Exception as e:
        logger.error("Fatal error during sync", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()