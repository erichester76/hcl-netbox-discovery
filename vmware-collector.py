
#!/usr/bin/env python3
"""
VMware vCenter → NetBox Synchronization Script using pynetbox2
"""

import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import threading
from pyVmomi import vim
from pyVim.connect import SmartConnect, Disconnect
import urllib3
import ssl
import re
import logging
import ipaddress
import json
import requests
from urllib.parse import urljoin

import pynetbox2 as pynetbox

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl_context = ssl._create_unverified_context()

PROTECTED_FIELDS = ['description', 'role']
TENANT_PROJECT_CACHE = {}
TENANT_NAME_CACHE = {}
TENANT_PROJECT_CACHE_INITIALIZED = False
TENANT_PROJECT_CACHE_LOCK = threading.Lock()
VLAN_CACHE = {}
VLAN_CACHE_LOCK = threading.Lock()
HOST_AUTOSTART_CACHE = {}
HOST_AUTOSTART_CACHE_LOCK = threading.Lock()

# Set logging level from DEBUG environment variable
DEBUG_ENV = os.environ.get("DEBUG", "false").lower()
LOG_LEVEL = logging.DEBUG if DEBUG_ENV in ("true", "1", "yes", "on") else logging.INFO
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

_NETBOX_CLIENT_INFO = None
_NETBOX_CLIENT_LOCK = threading.Lock()

OBJECT_TYPE_TO_RESOURCE = {
    "cluster": "virtualization.clusters",
    "clustergroup": "virtualization.cluster_groups",
    "clustertype": "virtualization.cluster_types",
    "site": "dcim.sites",
    "role": "dcim.device_roles",
    "manufacturer": "dcim.manufacturers",
    "location": "dcim.locations",
    "platform": "dcim.platforms",
    "tag": "extras.tags",
    "device": "dcim.devices",
    "host": "dcim.devices",
    "devicetype": "dcim.device_types",
    "virtualmachine": "virtualization.virtual_machines",
    "vm": "virtualization.virtual_machines",
    "interface": "dcim.interfaces",
    "vminterface": "virtualization.interfaces",
    "virtualdisk": "virtualization.virtual_disks",
    "ip_address": "ipam.ip_addresses",
    "prefix": "ipam.prefixes",
    "vlan": "ipam.vlans",
    "mac_address": "dcim.mac_addresses",
}


def _normalize_lookup_value(value):
    if hasattr(value, 'id'):
        return value.id
    return value


def _extract_id(obj):
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("id")
    return getattr(obj, "id", None)


def _env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("true", "1", "yes", "on")


def _determine_lookup_fields(object_type, data):
    if data.get("id") is not None:
        return ["id"]

    lookup_map = {
        "cluster": [["name", "group"], ["name"]],
        "clustergroup": [["name"]],
        "clustertype": [["name"]],
        "site": [["name"]],
        "role": [["name"]],
        "manufacturer": [["name"]],
        "location": [["name", "site"], ["name"]],
        "platform": [["name"]],
        "tag": [["name"]],
        "device": [["name", "site"], ["name"]],
        "host": [["name", "site"], ["name"]],
        "devicetype": [["model", "manufacturer"], ["model"]],
        "virtualmachine": [["name", "cluster"], ["name"]],
        "vm": [["name", "cluster"], ["name"]],
        "interface": [["name", "device"], ["name"]],
        "vminterface": [["name", "virtual_machine"], ["name"]],
        "virtualdisk": [["name", "virtual_machine"], ["name"]],
        "ip_address": [["address"]],
        "prefix": [["prefix"]],
        "vlan": [["vid", "site"], ["vid"], ["vid", "name"]],
        "mac_address": [["mac_address"]],
    }

    candidates = lookup_map.get(object_type, [])
    for candidate in candidates:
        if all(field in data and data.get(field) not in (None, "") for field in candidate):
            return candidate

    fallback = [k for k, v in data.items() if v not in (None, "")][:2]
    return fallback


def _merge_tags(existing_obj, incoming_tags):
    merged = {}
    name_to_id_key = {}

    def _add(tag_id, tag_name):
        normalized_name = str(tag_name).strip().lower() if tag_name else None

        # If this name is already represented by an id-backed tag, reuse that key.
        if normalized_name and normalized_name in name_to_id_key:
            key = name_to_id_key[normalized_name]
            existing_payload = merged.get(key)
            if existing_payload and "id" in existing_payload:
                return

        if tag_id is not None:
            key = f"id:{tag_id}"
            merged[key] = {"id": tag_id}
            if normalized_name:
                name_to_id_key[normalized_name] = key
            return

        if normalized_name:
            key = f"name:{normalized_name}"
            merged[key] = {"name": tag_name}

    for tag in getattr(existing_obj, 'tags', []) or []:
        _add(getattr(tag, 'id', None), getattr(tag, 'name', None))

    for tag in incoming_tags or []:
        if isinstance(tag, dict):
            _add(tag.get("id"), tag.get("name"))
        else:
            _add(getattr(tag, 'id', None), getattr(tag, 'name', None))

    return [item for item in merged.values() if item]


def _ensure_sync_tag(tags):
    merged = list(tags or [])
    for tag in merged:
        if isinstance(tag, dict):
            tag_name = str(tag.get("name", "")).strip().lower()
        else:
            tag_name = str(getattr(tag, "name", "")).strip().lower()
        if tag_name == "vmware-sync":
            return merged
    merged.append({"name": "VMWare-Sync"})
    return merged


def get_netbox_client():
    global _NETBOX_CLIENT_INFO

    with _NETBOX_CLIENT_LOCK:
        if _NETBOX_CLIENT_INFO is not None:
            return _NETBOX_CLIENT_INFO

        logger.info(
            "Initializing NetBox client backend=%s",
            os.environ.get("NETBOX_BACKEND", "pynetbox")
        )

        client = pynetbox.api(
            url=os.environ.get("NETBOX_URL", "http://localhost:8000"),
            token=os.environ.get("NETBOX_TOKEN", ""),
            branch=os.environ.get("NETBOX_BRANCH", None),
            #pynetbox2 specific config
            backend=os.environ.get("NETBOX_BACKEND", "pynetbox"),
            cache_backend = os.environ.get("NETBOX_CACHE_BACKEND", "sqlite"),
            cache_ttl_seconds = int(os.environ.get("NETBOX_CACHE_TTL_SECONDS", "3600")),
            cache_key_prefix=os.environ.get("NETBOX_CACHE_KEY_PREFIX", "netbox:"),
            redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            sqlite_path = os.environ.get("NETBOX_SQLITE_CACHE_PATH", ".netbox_cache.sqlite3"),
            rate_limit_per_second=float(os.environ.get("NETBOX_RATE_LIMIT_PER_SECOND", "0")),
            rate_limit_burst=int(os.environ.get("NETBOX_RATE_LIMIT_BURST", "1")),
            retry_attempts=int(os.environ.get("NETBOX_RETRY_ATTEMPTS", "3")),
            retry_initial_delay_seconds=float(os.environ.get("NETBOX_RETRY_INITIAL_DELAY", "0.3")),
            retry_backoff_factor=float(os.environ.get("NETBOX_RETRY_BACKOFF_FACTOR", "2.0")),
            retry_max_delay_seconds=float(os.environ.get("NETBOX_RETRY_MAX_DELAY", "15.0")),
            retry_jitter_seconds=float(os.environ.get("NETBOX_RETRY_JITTER", "0.0")),
            diode_target=os.environ.get("DIODE_URL", "grpcs://localhost:8080"),
            diode_client_id=os.environ.get("DIODE_CLIENT_ID", ""),
            diode_client_secret=os.environ.get("DIODE_CLIENT_SECRET", ""),
            diode_cert_file=os.environ.get("DIODE_CERT_FILE", None),
            diode_skip_tls_verify=os.environ.get("DIODE_SKIP_TLS_VERIFY", "false").lower() in ("true", "1", "yes", "on"),
            diode_read_fallback=os.environ.get("DIODE_READ_FALLBACK", "false").lower() in ("true", "1", "yes", "on"),
            diode_batch_size=int(os.environ.get("DIODE_BATCH_SIZE", "1")),
        )

        _NETBOX_CLIENT_INFO = {
            "type": os.environ.get("NETBOX_BACKEND", "pynetbox").lower(),
            "client": client,
        }
        return _NETBOX_CLIENT_INFO


def _parse_cache_prewarm_spec(prewarm_spec):
    if not prewarm_spec:
        return {}

    if prewarm_spec.startswith("{"):
        loaded = json.loads(prewarm_spec)
        if not isinstance(loaded, dict):
            raise ValueError("NETBOX_CACHE_PREWARM JSON must be an object")

        normalized = {}
        for resource, filters in loaded.items():
            if not isinstance(resource, str) or not resource.strip():
                continue
            normalized[resource.strip()] = dict(filters or {})
        return normalized

    resources = [item.strip() for item in prewarm_spec.split(",") if item.strip()]
    return {resource: {} for resource in resources}


def _prewarm_resource_worker(nb_client, resource, filters):
    try:
        logger.info("Starting NetBox cache prewarm thread resource=%s filters=%s", resource, filters)
        summary = nb_client.prewarm({resource: filters})
        logger.info(
            "Completed NetBox cache prewarm thread resource=%s count=%s",
            resource,
            summary.get(resource, 0),
        )
    except Exception as exc:
        logger.warning("NetBox cache prewarm thread failed resource=%s error=%s", resource, exc)


def start_cache_prewarm_threads(nb_client):
    prewarm_spec = os.environ.get("NETBOX_CACHE_PREWARM", "").strip()
    if not prewarm_spec:
        return []

    try:
        resource_filters = _parse_cache_prewarm_spec(prewarm_spec)
    except Exception as exc:
        logger.warning("Invalid NETBOX_CACHE_PREWARM value '%s': %s", prewarm_spec, exc)
        return []

    if not resource_filters:
        logger.warning("NETBOX_CACHE_PREWARM parsed to empty resource list; skipping")
        return []

    threads = []
    for resource, filters in resource_filters.items():
        thread_name = f"Prewarm-{resource}"
        t = threading.Thread(
            target=_prewarm_resource_worker,
            name=thread_name,
            args=(nb_client, resource, filters),
            daemon=True,
        )
        t.start()
        threads.append(t)

    logger.info(
        "Launched NetBox cache prewarm threads count=%s resources=%s",
        len(threads),
        list(resource_filters.keys()),
    )
    return threads


def netbox_read(read_function, *args, default=None, **kwargs):
    try:
        return read_function(*args, **kwargs)
    except Exception as exc:
        logger.warning(f"NetBox read failed: {exc}")
        if default is not None:
            return default
        raise

def get_with_cache(object_type, find_function, query_params):
    normalized_params = {k: _normalize_lookup_value(v) for k, v in query_params.items()}
    return find_function(**normalized_params)


def invalidate_cache_entry(object_type, data=None):
    resource = OBJECT_TYPE_TO_RESOURCE.get(object_type)
    if not resource:
        return
    try:
        get_netbox_client()["client"].clear_cache(resource)
    except Exception as exc:
        logger.debug(f"Cache invalidation skipped for {object_type}: {exc}")

def _generate_slug(name):
    """Generate a slug from a name: lowercase, hyphens for spaces/specials, alphanum/hyphen/underscore only."""
    import re
    slug = name.strip().lower()
    slug = re.sub(r'[^a-z0-9\-_]+', '-', slug)  # Replace non-alphanum/hyphen/underscore with hyphen
    slug = re.sub(r'-+', '-', slug)  # Collapse multiple hyphens
    slug = slug.strip('-')
    return slug

def create_or_update(object_type, data, protected_fields=None, preserve_existing_tags=False):
    resource = OBJECT_TYPE_TO_RESOURCE.get(object_type)
    if not resource:
        raise ValueError(f"Unsupported object type: {object_type}")

    client_info = get_netbox_client()
    nb_client = client_info["client"]
    backend_type = client_info.get("type", "pynetbox")
    # Remove non-NetBox fields that may have leaked in
    payload = {k: v for k, v in dict(data).items() if k not in ("resource", "status", "payload")}

    # Auto-generate slug if missing and name is present
    if "name" in payload and ("slug" not in payload or not payload["slug"]):
        payload["slug"] = _generate_slug(payload["name"])
    lookup_fields = _determine_lookup_fields(object_type, payload)
    use_cache_for_lookup = _env_bool("NETBOX_USE_CACHE_FOR_UPSERT_LOOKUP", default=False)
    use_cache_for_tag_preserve_lookup = _env_bool("NETBOX_USE_CACHE_FOR_TAG_PRESERVE_LOOKUP", default=False)

    if preserve_existing_tags and "tags" in payload:
        filters = {field: _normalize_lookup_value(payload.get(field)) for field in lookup_fields if payload.get(field) is not None}
        if filters:
            existing = nb_client.get(resource, use_cache=use_cache_for_tag_preserve_lookup, **filters)
            if existing is not None:
                payload["tags"] = _merge_tags(existing, payload.get("tags"))

    result = nb_client.upsert(
        resource,
        payload,
        lookup_fields=lookup_fields,
        preserve_fields=protected_fields,
        use_cache_for_lookup=use_cache_for_lookup,
    )
    result_id = _extract_id(result)
    return result_id if result_id is not None else result


def log_performance_summary():
    logger.info(
        "Sync summary: tenants_cached=%s vlan_cached=%s host_autostart_cached=%s",
        len(TENANT_PROJECT_CACHE),
        len(VLAN_CACHE),
        len(HOST_AUTOSTART_CACHE),
    )


class RelativeSession(requests.Session):
    def __init__(self, base_url):
        super().__init__()
        self.__base_url = base_url

    def request(self, method, url, **kwargs):
        url = urljoin(self.__base_url, url)
        return super().request(method, url, **kwargs)


def subnet_mask_to_prefix_length(subnet_mask):
    try:
        return sum(bin(int(octet)).count('1') for octet in subnet_mask.split('.'))
    except Exception as e:
        logger.error(f"Failed to convert subnet mask {subnet_mask} to prefix length: {e}")
        return None


def apply_regex_patterns(value, filename):
    original_value = value
    try:
        with open(f"regex/{filename}", 'r') as file:
            patterns = [tuple(line.strip().split(',', 1)) for line in file if ',' in line]
    except FileNotFoundError:
        logger.error(f"Regex patterns file not found: regex/{filename}")
        return value
    except Exception as e:
        logger.error(f"Error reading regex patterns from {filename}: {e}")
        return value

    for pattern, replacement in patterns:
        value = re.sub(pattern.strip(), replacement.strip(), value)

    return value if value != original_value else 'Unknown'


def extract_vm_project_id(vm_name):
    vm_short_name = vm_name.replace('.clemson.edu', '')
    match = re.match(r'^([A-Za-z0-9]{3}-[A-Za-z0-9]{3})', vm_short_name)
    return match.group(1).lower() if match else None


def ensure_tenant_cache_initialized():
    global TENANT_PROJECT_CACHE_INITIALIZED

    with TENANT_PROJECT_CACHE_LOCK:
        if TENANT_PROJECT_CACHE_INITIALIZED:
            return

        try:
            nb_client = get_netbox_client()["client"]
            for tenant in netbox_read(nb_client.tenancy.tenants.all, default=[]):
                custom_fields = getattr(tenant, 'custom_fields', {}) or {}
                tenant_project_id = custom_fields.get('project_id')
                if tenant_project_id:
                    project_key = str(tenant_project_id).strip().lower()
                    tenant_id = getattr(tenant, 'id', None)
                    if project_key and tenant_id:
                        TENANT_PROJECT_CACHE[project_key] = {
                            "id": tenant_id,
                            "name": getattr(tenant, 'name', project_key),
                        }

                tenant_name = str(getattr(tenant, 'name', '')).strip()
                tenant_id = getattr(tenant, 'id', None)
                if tenant_name and tenant_id:
                    TENANT_NAME_CACHE[tenant_name.lower()] = {
                        "id": tenant_id,
                        "name": tenant_name,
                    }

            logger.info(
                f"Loaded {len(TENANT_PROJECT_CACHE)} tenant project_id mappings and "
                f"{len(TENANT_NAME_CACHE)} tenant name mappings"
            )
        except Exception as e:
            logger.warning(f"Failed to build tenant caches: {e}")
        finally:
            TENANT_PROJECT_CACHE_INITIALIZED = True


def get_tenant_for_project_id(project_id):
    if not project_id:
        return None

    ensure_tenant_cache_initialized()

    return TENANT_PROJECT_CACHE.get(project_id.lower())


def get_tenant_by_name(tenant_name):
    if not tenant_name:
        return None

    ensure_tenant_cache_initialized()
    return TENANT_NAME_CACHE.get(str(tenant_name).strip().lower())


def calculate_network_address(ip, prefix_length):
    try:
        network = ipaddress.ip_network(f"{ip}/{prefix_length}", strict=False)
        return str(network.network_address)
    except ValueError as e:
        logger.error(f"Error calculating network address for IP {ip}/{prefix_length}: {e}")
        return None


def fetch_vmware_tags(rest_session):
    try:
        cat_dict = {}
        cat_response = rest_session.get("/rest/com/vmware/cis/tagging/category")
        cat_response.raise_for_status()
        for category_id in cat_response.json().get("value", []):
            detail_resp = rest_session.get(f"/rest/com/vmware/cis/tagging/category/id:{category_id}")
            detail_resp.raise_for_status()
            cat_dict[category_id] = detail_resp.json().get("value", {}).get("name")

        tag_dict = {}
        tag_response = rest_session.get("/rest/com/vmware/cis/tagging/tag")
        tag_response.raise_for_status()
        for tag_id in tag_response.json().get("value", []):
            detail_resp = rest_session.get(f"/rest/com/vmware/cis/tagging/tag/id:{tag_id}")
            detail_resp.raise_for_status()
            tag_detail = detail_resp.json().get("value", {})
            tag_dict[tag_id] = {
                "name": tag_detail.get("name"),
                "category_id": tag_detail.get("category_id"),
            }

        vm_response = rest_session.get("/rest/vcenter/vm")
        vm_response.raise_for_status()
        vm_objs = [{"id": vm["vm"], "type": "VirtualMachine"} for vm in vm_response.json().get("value", [])]

        vm_tags = {}
        tag_assoc_payload = {"object_ids": vm_objs}
        assoc_resp = rest_session.post(
            "/rest/com/vmware/cis/tagging/tag-association?~action=list-attached-tags-on-objects",
            json=tag_assoc_payload
        )
        assoc_resp.raise_for_status()
        for assoc in assoc_resp.json().get("value", []):
            vm_id = assoc["object_id"]["id"]
            cat_tag_dict = {}
            for tag_id in assoc.get("tag_ids", []):
                tag_info = tag_dict.get(tag_id, {})
                cat_name = cat_dict.get(tag_info.get("category_id"))
                if cat_name not in cat_tag_dict:
                    cat_tag_dict[cat_name] = []
                cat_tag_dict[cat_name].append(tag_info.get("name"))
            vm_tags[vm_id] = cat_tag_dict

        logger.debug("Processed Tags")
        return vm_tags
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching VM tags: {e}")
        return {}


def fetch_vmware_data(api_client, rest_session):
    content = api_client.RetrieveContent()
    clusters = content.viewManager.CreateContainerView(content.rootFolder, [vim.ClusterComputeResource], True).view
    cluster_data = []
    vm_tags = fetch_vmware_tags(rest_session)

    for cluster in clusters:
        cluster_entry = {
            "cluster": cluster,
            "cluster_name": cluster.name,
            "hosts": []
        }
        for host in cluster.host:
            host_entry = {
                "host": host,
                "nics": [],
                "vms": [],
            }
            dvs_info = {}
            logger.debug(f"Fetching Network Info for {host}")
            for network in host.network:
                if isinstance(network, vim.dvs.DistributedVirtualPortgroup):
                    vlan_config = getattr(network.config.defaultPortConfig.vlan, 'vlanId', None)
                    dvs_info[network.key] = {"id": vlan_config, "name": network.name}
                    dvs_info[network.name] = {"id": vlan_config, "name": network.name}

            for pnic in getattr(host.config.network, 'pnic', []):
                nic_entry = {
                    "name": pnic.device,
                    "mac_address": getattr(pnic, 'mac', None),
                    "link_speed": getattr(pnic.linkSpeed, 'speedMb', None),
                    "duplex": getattr(pnic.linkSpeed, 'duplex', None),
                }
                host_entry["nics"].append(nic_entry)

            for vnic in getattr(host.config.network, 'vnic', []):
                portgroup_name = getattr(vnic.spec.distributedVirtualPort, 'portgroupKey', None)
                nic_entry = {
                    "name": vnic.device,
                    "mac_address": getattr(vnic.spec, 'mac', None),
                    "address": getattr(vnic.spec.ip, 'ipAddress', None),
                    "prefix_length": subnet_mask_to_prefix_length(getattr(vnic.spec.ip, 'subnetMask', None)),
                    "vlan": dvs_info.get(portgroup_name) if portgroup_name in dvs_info else None,
                }
                host_entry["nics"].append(nic_entry)

            logger.debug(f"Fetching VMs for {host}")
            for vm in host.vm:
                vm_entry = {
                    "vm": vm,
                    "vm_name": vm.name.replace('.clemson.edu', '')[:64],
                    "interfaces": [],
                    "disks": [],
                    "tags": vm_tags.get(vm._moId, {})
                }
                logger.debug(f"Fetching Network Info for {vm}")
                for net in getattr(vm.guest, 'net', []):
                    if hasattr(net, 'macAddress'):
                        for device in getattr(vm.config.hardware, 'device', []):
                            if isinstance(device, vim.vm.device.VirtualEthernetCard) and net.macAddress == device.macAddress:
                                portgroup_name = getattr(net, "network", None)
                                nic_entry = {
                                    "name": device.deviceInfo.label,
                                    "mac_address": device.macAddress if hasattr(device, 'macAddress') else None,
                                    "enabled": device.connectable.connected if hasattr(device, 'connectable') else False,
                                    "vlan": dvs_info.get(portgroup_name) if portgroup_name in dvs_info else None,
                                    "description": portgroup_name[:200] if portgroup_name else None,
                                    "ipv4_addresses": [
                                        {"address": ip.ipAddress, "prefix_length": getattr(ip, 'prefixLength', 24)}
                                        for ip in getattr(net.ipConfig, 'ipAddress', []) if ":" not in ip.ipAddress
                                    ],
                                    "ipv6_addresses": [
                                        {"address": ip.ipAddress, "prefix_length": getattr(ip, 'prefixLength', 48)}
                                        for ip in getattr(net.ipConfig, 'ipAddress', []) if ":" in ip.ipAddress
                                    ],
                                }
                                vm_entry["interfaces"].append(nic_entry)

                for device in getattr(vm.config.hardware, 'device', []):
                    if isinstance(device, vim.vm.device.VirtualDisk):
                        disk_entry = {
                            "name": device.deviceInfo.label,
                            "capacity": device.capacityInKB / 1024,
                            "datastore": getattr(device.backing.datastore, 'name', None) if hasattr(device.backing, 'datastore') else None,
                            "vmdk": getattr(device.backing, 'fileName', None),
                            "type": getattr(device.backing, 'diskMode', None),
                            "thin": getattr(device.backing, 'thinProvisioned', False),
                        }
                        vm_entry["disks"].append(disk_entry)

                host_entry["vms"].append(vm_entry)

            cluster_entry["hosts"].append(host_entry)

        cluster_data.append(cluster_entry)

    return cluster_data


def process_related_field(object_type, field_data):
    if not field_data:
        return None

    return create_or_update(
        object_type,
        field_data,
        preserve_existing_tags=True
    )


def process_cluster(cluster):
    site_name = apply_regex_patterns(cluster.name, 'cluster_to_site')
    cluster_group_name = cluster.parent.parent.name if cluster.parent and cluster.parent.parent else "Unknown"
    if str(cluster_group_name).strip().lower() in ('host', 'vm', ''):
        cluster_group_name = "Unknown"
    
    group = process_related_field("clustergroup", {"name": cluster_group_name})
    site = process_related_field("site", {"name": site_name})
    cluster_type = process_related_field("clustertype", {"name": "VMWare"})

    return {
        "name": cluster.name,
        "group": group,
        "site": site,
        "type": cluster_type,
    }


def process_host(host):
    site_name = apply_regex_patterns(host.parent.name, 'cluster_to_site')
    manufacturer = process_related_field("manufacturer", {"name": host.hardware.systemInfo.vendor})
    tenant_name = apply_regex_patterns(host.name, 'host_to_tenant')
    if tenant_name == host.name:
        tenant_name = "Unknown"

    cluster_data = process_cluster(host.parent)
    serial_number = "Unknown"
    for identifier in host.summary.hardware.otherIdentifyingInfo:
        if getattr(identifier.identifierType, 'key', None) == 'SerialNumberTag':
            serial_number = getattr(identifier, 'identifierValue', "Unknown")

    return {
        "name": host.name.replace('.clemson.edu', ''),
        "cluster": process_related_field("cluster", cluster_data),
        "site": process_related_field("site", {"name": site_name}),
        "device_type": process_related_field(
            "devicetype",
            {"model": host.hardware.systemInfo.model, "manufacturer": manufacturer}
        ),
        "role": process_related_field("role", {"name": "Hypervisor Host"}),
        "status": "active" if host.runtime.connectionState == "connected" else "offline",
        "platform": process_related_field(
            "platform",
            {"name": host.config.product.fullName if host.config.product else "Unknown"}
        ),
        "serial": serial_number,
    }


def get_host_vm_autostart_actions(host):
    if not host:
        return {}

    host_id = getattr(host, '_moId', None) or str(host)
    with HOST_AUTOSTART_CACHE_LOCK:
        if host_id in HOST_AUTOSTART_CACHE:
            return HOST_AUTOSTART_CACHE[host_id]

    vm_actions = {}
    try:
        auto_start_manager = getattr(getattr(host, 'configManager', None), 'autoStartManager', None)
        auto_start_config = getattr(auto_start_manager, 'config', None)
        power_info = getattr(auto_start_config, 'powerInfo', None) or []

        for entry in power_info:
            vm_ref = getattr(entry, 'key', None)
            vm_moid = getattr(vm_ref, '_moId', None)
            start_action = str(getattr(entry, 'startAction', '')).strip().lower()
            if vm_moid:
                vm_actions[vm_moid] = start_action
    except Exception as e:
        logger.debug(f"Could not load VM autostart actions for host {host_id}: {e}")

    with HOST_AUTOSTART_CACHE_LOCK:
        HOST_AUTOSTART_CACHE[host_id] = vm_actions

    return vm_actions


def get_vm_start_on_boot(vm):
    try:
        vm_name = getattr(vm, 'name', 'unknown')
        vm_moid = getattr(vm, '_moId', None)
        vm_actions = get_host_vm_autostart_actions(getattr(vm.runtime, 'host', None))
        host_action = vm_actions.get(vm_moid)
        if host_action:
            start_on_boot = host_action == 'poweron'
            logger.debug(
                f"Resolved start_on_boot for VM {vm_name} from host autostart action '{host_action}': {start_on_boot}"
            )
            return start_on_boot

        # vApp-managed VMs may carry start action here.
        vapp_config = getattr(getattr(vm, 'config', None), 'vAppConfig', None)
        vapp_start_action = str(getattr(vapp_config, 'startAction', '')).strip().lower()
        if vapp_start_action:
            start_on_boot = vapp_start_action == 'poweron'
            logger.debug(
                f"Resolved start_on_boot for VM {vm_name} from vApp start action '{vapp_start_action}': {start_on_boot}"
            )
            return start_on_boot

        logger.debug(f"No VMware autostart metadata found for VM {vm_name}; leaving start_on_boot unchanged")
    except Exception as e:
        logger.debug(f"Could not determine start_on_boot for VM {getattr(vm, 'name', 'unknown')}: {e}")

    return None


def process_vm(vm, host):
    vm_short_name = vm.name.replace('.clemson.edu', '')
    project_id = extract_vm_project_id(vm.name)
    tenant_match = get_tenant_for_project_id(project_id) if project_id else None
    tenant_name_hint = None
    if not tenant_match:
        tenant_name_hint = apply_regex_patterns(vm_short_name, 'vm_to_tenant')
        if tenant_name_hint and tenant_name_hint != 'Unknown':
            tenant_match = get_tenant_by_name(tenant_name_hint)

    tenant = tenant_match["name"] if tenant_match else (
        tenant_name_hint if tenant_name_hint and tenant_name_hint != 'Unknown' else "Unknown"
    )
    if not project_id:
        logger.debug(f"VM {vm.name} has no leading project_id pattern (expected xxx-xxx)")
    elif not tenant_match:
        logger.debug(f"No tenant match for VM {vm.name} project_id '{project_id}'")
    role = apply_regex_patterns(vm.name, 'vm_to_role')

    platform_name = vm.guest.guestFullName if vm.guest.guestFullName else None
    platform = None
    if platform_name:
        manufacturer_name = platform_name.split()[0] if platform_name.split() else "Unknown"
        manufacturer = process_related_field(
            "manufacturer",
            {"name": manufacturer_name, "slug": re.sub(r'\W+', '-', manufacturer_name.lower())}
        )
        if manufacturer:
            platform_data = {
                "name": platform_name,
                "manufacturer": manufacturer,
                "slug": re.sub(r'\W+', '-', platform_name.lower())
            }
            platform = process_related_field("platform", platform_data)
        else:
            logger.warning(f"Skipping platform creation for VM {vm.name}: Failed to create manufacturer {manufacturer_name}")

    cluster_data = process_cluster(vm.runtime.host.parent)
    cluster = process_related_field("cluster", cluster_data)

    vm_data = {
        "name": vm.name.replace('.clemson.edu', '')[:64],
        "cluster": cluster,
        
        "device": process_related_field("device", host),
        "role": process_related_field("role", {"name": role}),
        "description": f"{role} VM for {tenant}"[:200],
        "status": "active" if vm.runtime.powerState == "poweredOn" else "offline",
        "serial": vm.config.uuid,
        "vcpus": vm.config.hardware.numCPU if vm.config.hardware else 0,
        "memory": vm.config.hardware.memoryMB if vm.config.hardware else 0,
    }
    vm_start_on_boot = get_vm_start_on_boot(vm)
    if vm_start_on_boot is not None:
        vm_data["start_on_boot"] = vm_start_on_boot

    if tenant_match:
        vm_data["tenant"] = tenant_match["id"]
    if platform:
        vm_data["platform"] = platform

    return vm_data


def process_host_interface(nic, host):
    interface_data = {
        "name": nic.get("name"),
        "device": process_related_field("device", host),
        "description": f"{host['name']} {nic.get('name')}"[:200],
        "mac_address": nic.get("mac_address").upper() if nic.get("mac_address") else None,
    }

    # Always set 'type', default to 'other' if not set below
    interface_type = None
    if nic.get("link_speed"):
        speed_map = {
            1000: "1000base-t",
            10000: "10gbase-x-sfpp",
            25000: "25gbase-x-sfp28",
            40000: "40gbase-x-qsfpp",
            100000: "100gbase-x-qsfp28",
        }
        interface_type = speed_map.get(nic.get("link_speed"), "other")
        interface_data["speed"] = nic.get("link_speed") * 1000
        interface_data["duplex"] = "full" if nic.get("duplex") else "half" if nic.get("duplex") is not None else None
    # If not set by link_speed, try to infer from name (e.g., vmk* = virtual)
    if not interface_type and nic.get("name"):
        interface_type = "other"
        
    interface_data["type"] = interface_type

    if nic.get("vlan") and isinstance(nic.get("vlan"), dict) and nic.get("vlan").get('id'):
        interface_data["mode"] = "tagged"

    return interface_data


def process_vm_interface(interface, vm):
    return {
        "name": interface.get("name"),
        "virtual_machine": process_related_field("virtualmachine", vm),
        "description": f"{vm.get('name')} {interface.get('name')}"[:200],
        "mac_address": interface.get("mac_address").upper(),
        "enabled": interface.get("enabled")
    }


def process_disk(disk, vm):
    return {
        "name": disk.get("name"),
        "virtual_machine": process_related_field("virtualmachine", vm),
        "size": disk.get("capacity"),
        "description": f'{disk.get("vmdk")} ({"Thin Provisioned" if disk.get("thin") else "Thick Provisioned"} {disk.get("type")})'[:200]
    }


def clear_primary_ip_for_address(nb_client, ip_address_str):
    try:
        ip_obj = get_with_cache('ip_address', nb_client.ipam.ip_addresses.get, {"address": ip_address_str})
        if not ip_obj:
            logger.warning(f"Could not find IP address object for {ip_address_str}")
            return False

        ip_id = ip_obj.id
        cleared = False

        devices_ipv4 = netbox_read(nb_client.dcim.devices.filter, primary_ip4_id=ip_id, default=[])
        for device in devices_ipv4:
            logger.debug(f"Clearing primary IPv4 on device {device.id} ({device.name}) for IP {ip_address_str}")
            try:
                create_or_update('device', {"id": device.id, "primary_ip4": None}, protected_fields=PROTECTED_FIELDS)
                cleared = True
            except Exception as e:
                logger.warning(f"Could not clear primary IPv4 on device {device.id} ({device.name}): {e}")

        devices_ipv6 = netbox_read(nb_client.dcim.devices.filter, primary_ip6_id=ip_id, default=[])
        for device in devices_ipv6:
            logger.debug(f"Clearing primary IPv6 on device {device.id} ({device.name}) for IP {ip_address_str}")
            try:
                create_or_update('device', {"id": device.id, "primary_ip6": None}, protected_fields=PROTECTED_FIELDS)
                cleared = True
            except Exception as e:
                logger.warning(f"Could not clear primary IPv6 on device {device.id} ({device.name}): {e}")

        vms_ipv4 = netbox_read(nb_client.virtualization.virtual_machines.filter, primary_ip4_id=ip_id, default=[])
        for vm in vms_ipv4:
            logger.debug(f"Clearing primary IPv4 on VM {vm.id} ({vm.name}) for IP {ip_address_str}")
            try:
                create_or_update('virtualmachine', {"id": vm.id, "primary_ip4": None}, protected_fields=PROTECTED_FIELDS)
                cleared = True
            except Exception as item_exc:
                if 'not assigned to this cluster' in str(item_exc):
                    # VM has a stale device/cluster inconsistency; clear device too so the save succeeds
                    logger.warning(f"VM {vm.id} ({vm.name}) has device/cluster inconsistency; clearing device field to fix: {item_exc}")
                    try:
                        create_or_update('virtualmachine', {"id": vm.id, "primary_ip4": None, "device": None}, protected_fields=PROTECTED_FIELDS)
                        cleared = True
                    except Exception as fix_exc:
                        logger.warning(f"Could not clear primary IPv4 on VM {vm.id} ({vm.name}) even after removing device: {fix_exc}")
                else:
                    logger.warning(f"Could not clear primary IPv4 on VM {vm.id} ({vm.name}): {item_exc}")

        vms_ipv6 = netbox_read(nb_client.virtualization.virtual_machines.filter, primary_ip6_id=ip_id, default=[])
        for vm in vms_ipv6:
            logger.debug(f"Clearing primary IPv6 on VM {vm.id} ({vm.name}) for IP {ip_address_str}")
            try:
                create_or_update('virtualmachine', {"id": vm.id, "primary_ip6": None}, protected_fields=PROTECTED_FIELDS)
                cleared = True
            except Exception as item_exc:
                if 'not assigned to this cluster' in str(item_exc):
                    logger.warning(f"VM {vm.id} ({vm.name}) has device/cluster inconsistency; clearing device field to fix: {item_exc}")
                    try:
                        create_or_update('virtualmachine', {"id": vm.id, "primary_ip6": None, "device": None}, protected_fields=PROTECTED_FIELDS)
                        cleared = True
                    except Exception as fix_exc:
                        logger.warning(f"Could not clear primary IPv6 on VM {vm.id} ({vm.name}) even after removing device: {fix_exc}")
                else:
                    logger.warning(f"Could not clear primary IPv6 on VM {vm.id} ({vm.name}): {item_exc}")

        return cleared
    except Exception as e:
        logger.warning(f"Error while clearing primary IP references for {ip_address_str}: {e}")
        return False


def process_ip_address(nb_client, ip, parent_object, vlan_id, is_primary=False):
    if not parent_object.get("id"):
        logger.error(f"Cannot process IP address {ip['address']} for {parent_object.get('description', 'unknown object')}: Invalid assigned_object_id")
        return None

    ip_addr = ip['address']
    is_link_local = ip_addr.startswith('fe80:') or ip_addr.startswith('169.254.')
    track_link_local = os.getenv("TRACK_LINK_LOCAL_IPS", "false").lower() in ('true', '1', 'yes', 'on')

    if is_link_local and not track_link_local:
        logger.debug(f"Skipping link-local IP {ip_addr}: TRACK_LINK_LOCAL_IPS is disabled")
        return None

    ip_data = {
        "address": f"{ip['address']}/{ip['prefix_length']}",
        "status": "active",
        "description": (parent_object['description'][:200] if 'description' in parent_object else " "),
        "assigned_object_type": 'dcim.interface' if 'device' in parent_object else 'virtualization.vminterface',
        "assigned_object_id": parent_object.get("id"),
        "tags": ip.get("tags", [])
    }

    try:
        ip_object = create_or_update(
            'ip_address',
            ip_data,
            preserve_existing_tags=True
        )
    except Exception as e:
        error_str = str(e)
        if 'Cannot reassign IP address while it is designated as the primary IP' in error_str:
            logger.debug(f"IP {ip_data['address']} is primary, clearing references and retrying")
            if clear_primary_ip_for_address(nb_client, ip_data['address']):
                ip_object = create_or_update(
                    'ip_address',
                    ip_data,
                    preserve_existing_tags=True
                )
            else:
                logger.error(f"Failed to clear primary IP references for {ip_data['address']}")
                return None
        else:
            raise

    obj = {}
    if is_primary and ip_object:
        try:
            ip_object_id = ip_object.id if hasattr(ip_object, 'id') else ip_object
            fresh_ip = get_with_cache('ip_address', nb_client.ipam.ip_addresses.get, {"id": ip_object_id})

            expected_type = 'dcim.interface' if 'device' in parent_object else 'virtualization.vminterface'
            expected_id = parent_object.get("id")

            if not fresh_ip:
                logger.warning(f"Unable to fetch IP object {ip_object_id} before setting primary IP")
            elif getattr(fresh_ip, 'assigned_object_type', None) != expected_type or getattr(fresh_ip, 'assigned_object_id', None) != expected_id:
                logger.warning(
                    f"Skipping primary IP set for {ip['address']}: IP assigned to "
                    f"{getattr(fresh_ip, 'assigned_object_type', None)}:{getattr(fresh_ip, 'assigned_object_id', None)} "
                    f"instead of {expected_type}:{expected_id}"
                )
            else:
                if ":" not in ip["address"]:
                    obj['primary_ip4'] = ip_object_id
                else:
                    obj['primary_ip6'] = ip_object_id

                logger.debug(f"Setting {ip['address']} as primary IP for {parent_object}")
                if "device" in parent_object:
                    obj['id'] = parent_object['device']
                    create_or_update('device', obj, protected_fields=PROTECTED_FIELDS)
                elif "virtual_machine" in parent_object:
                    obj['id'] = parent_object['virtual_machine']
                    create_or_update('virtualmachine', obj, protected_fields=PROTECTED_FIELDS)
        except Exception as e:
            if 'is not assigned to this VM' in str(e) or 'is not assigned to this device' in str(e):
                logger.warning(f"Skipping primary IP assignment for {ip['address']} due to assignment race: {e}")
            else:
                raise

    prefix = calculate_network_address(ip['address'], ip['prefix_length'])
    is_link_local_prefix = prefix.startswith('fe80::') or prefix.startswith('169.254.')

    if (":" in prefix and ip['prefix_length'] < 128) or (":" not in prefix and ip['prefix_length'] < 32):
        logger.debug(f"Processing IP Prefix {prefix}/{ip['prefix_length']}")
        prefix_data = {
            "prefix": f"{prefix}/{ip['prefix_length']}",
            "status": "active",
            "tags": ip.get("tags", [])
        }
        if isinstance(vlan_id, int) and not is_link_local_prefix:
            prefix_data['vlan'] = vlan_id

        try:
            create_or_update('prefix', prefix_data, preserve_existing_tags=True)
        except Exception as e:
            if 'Duplicate' in str(e):
                logger.warning(f"Prefix {prefix}/{ip['prefix_length']} conflicts with existing overlapping prefix: {e}. Skipping.")
            else:
                raise

    return ip_object


def get_vlan_site_id(vlan_obj):
    site = getattr(vlan_obj, 'site', None)
    if site is None:
        return None
    if hasattr(site, 'id'):
        return site.id
    return site


def get_vlan_site_name(vlan_obj):
    site = getattr(vlan_obj, 'site', None)
    if site is None:
        return None
    return getattr(site, 'name', None) or str(site)


def get_site_name(nb_client, site_id):
    if site_id is None:
        return None

    try:
        site_obj = get_with_cache('site', nb_client.dcim.sites.get, {"id": site_id})
        if site_obj:
            return getattr(site_obj, 'name', None)
    except Exception as e:
        logger.debug(f"Could not resolve site name for site {site_id}: {e}")

    return str(site_id)


def get_virtual_machine_site_id(nb_client, vm_id, fallback_site_id=None):
    if not vm_id:
        return fallback_site_id

    try:
        vm_obj = get_with_cache('virtualmachine', nb_client.virtualization.virtual_machines.get, {"id": vm_id})
        if not vm_obj:
            return fallback_site_id

        site = getattr(vm_obj, 'site', None)
        if site is not None:
            return site.id if hasattr(site, 'id') else site

        cluster = getattr(vm_obj, 'cluster', None)
        cluster_site = getattr(cluster, 'site', None) if cluster else None
        if cluster_site is not None:
            return cluster_site.id if hasattr(cluster_site, 'id') else cluster_site
    except Exception as e:
        logger.debug(f"Could not resolve VM site for VM {vm_id}: {e}")

    return fallback_site_id


def cache_vlan_result(vlan_vid, vlan_db_id, site_id=None, fallback_to_siteless=False):
    VLAN_CACHE[(vlan_vid, site_id)] = vlan_db_id
    if fallback_to_siteless:
        VLAN_CACHE[(vlan_vid, None)] = vlan_db_id


def normalize_vlan_description(vlan_id, description, fallback_name=None):
    normalized_description = str(description).strip() if description else ""
    if not normalized_description and fallback_name:
        normalized_description = str(fallback_name).strip()

    prefixed_vlan_description = f"VLAN{vlan_id} : "
    if normalized_description.startswith(prefixed_vlan_description):
        normalized_description = normalized_description[len(prefixed_vlan_description):].strip()

    return normalized_description or f"VLAN{vlan_id}"


def build_siteless_vlan_description(vlan_id, description_entries):
    base_description = f"VLAN{vlan_id}"
    merged_description = base_description
    seen_descriptions = {base_description.lower()}

    for site_name, description in description_entries:
        description_parts = [p.strip() for p in str(description or "").split('|') if p.strip()]
        if not description_parts:
            description_parts = [""]

        for part in description_parts:
            normalized_description = normalize_vlan_description(vlan_id, part)
            if not normalized_description:
                continue
            if normalized_description.lower() == base_description.lower():
                continue

            has_site_prefix = ': ' in normalized_description
            fragment = normalized_description if has_site_prefix else (
                f"{site_name}: {normalized_description}" if site_name else normalized_description
            )

            normalized_key = fragment.lower()
            if normalized_key in seen_descriptions:
                continue

            if normalized_key in merged_description.lower():
                seen_descriptions.add(normalized_key)
                continue

            candidate_description = f"{merged_description} | {fragment}"
            if len(candidate_description) > 200:
                remaining_chars = 200 - len(merged_description) - 3
                if remaining_chars <= 0:
                    break
                fragment = fragment[:remaining_chars].rstrip()
                if not fragment:
                    break
                candidate_description = f"{merged_description} | {fragment}"

            merged_description = candidate_description
            seen_descriptions.add(normalized_key)

            if len(merged_description) >= 200:
                break

        if len(merged_description) >= 200:
            break

    return merged_description[:200]


def process_vlan(vlan, site_id=None):
    if not vlan or not isinstance(vlan, dict) or not vlan.get('id') or not vlan.get('name'):
        logger.error(f"Invalid VLAN data: {vlan}. Skipping VLAN processing.")
        return None

    try:
        vlan_id = int(vlan['id'])
        if vlan_id < 1 or vlan_id > 4094:
            logger.error(f"Invalid VLAN ID {vlan_id}. Must be between 1 and 4094.")
            return None

        with VLAN_CACHE_LOCK:
            cache_key = (vlan_id, site_id)
            siteless_cache_key = (vlan_id, None)
            if cache_key in VLAN_CACHE:
                logger.debug(f"VLAN {vlan_id} for site {site_id} found in cache")
                return VLAN_CACHE[cache_key]
            if site_id is not None and siteless_cache_key in VLAN_CACHE:
                logger.debug(f"Using siteless cached VLAN {vlan_id} as fallback for site {site_id}")
                return VLAN_CACHE[siteless_cache_key]

        vlan_name = vlan.get('name', 'Unknown')
        prefixed_description = f"VLAN{vlan_id} : {vlan_name}"[:200]

        vlan_data = {
            "vid": vlan_id,
            "name": f"VLAN{vlan_id}",
            "description": prefixed_description,
            "tags": vlan.get('tags', []),
        }

        with VLAN_CACHE_LOCK:
            if cache_key in VLAN_CACHE:
                return VLAN_CACHE[cache_key]
            if site_id is not None and siteless_cache_key in VLAN_CACHE:
                return VLAN_CACHE[siteless_cache_key]

            nb_client = get_netbox_client()["client"]
            existing_vlans = list(netbox_read(nb_client.ipam.vlans.filter, vid=vlan_id, default=[]))
            requested_site_name = get_site_name(nb_client, site_id)
            siteless_description = build_siteless_vlan_description(
                vlan_id,
                [
                    (requested_site_name, vlan.get('description') or vlan_name),
                    *[
                        (
                            get_vlan_site_name(existing_vlan),
                            getattr(existing_vlan, 'description', None) or getattr(existing_vlan, 'name', None),
                        )
                        for existing_vlan in existing_vlans
                    ],
                ]
            )

            siteless_vlan = None
            site_vlan = None
            other_site_vlans = []

            for existing_vlan in existing_vlans:
                existing_site_id = get_vlan_site_id(existing_vlan)
                if existing_site_id is None and siteless_vlan is None:
                    siteless_vlan = existing_vlan
                elif site_id is not None and existing_site_id == site_id and site_vlan is None:
                    site_vlan = existing_vlan
                else:
                    other_site_vlans.append(existing_vlan)

            if siteless_vlan:
                vlan_object = create_or_update(
                    'vlan',
                    {**vlan_data, "id": siteless_vlan.id, "description": siteless_description},
                    preserve_existing_tags=True
                )
                vlan_result = vlan_object.id if hasattr(vlan_object, 'id') else vlan_object
                cache_vlan_result(vlan_id, vlan_result, site_id=site_id, fallback_to_siteless=True)
                return vlan_result

            if site_vlan:
                if other_site_vlans:
                    logger.info(
                        f"VLAN {vlan_id} exists in multiple site-scoped records; preserving requested site {site_id} without promoting to siteless"
                    )
                vlan_payload = {**vlan_data, "id": site_vlan.id, "site": site_id}
                vlan_object = create_or_update('vlan', vlan_payload, preserve_existing_tags=True)
                vlan_result = vlan_object.id if hasattr(vlan_object, 'id') else vlan_object
                cache_vlan_result(vlan_id, vlan_result, site_id=site_id)
                return vlan_result

            if site_id is None and existing_vlans:
                logger.warning(
                    f"VLAN {vlan_id} was requested without a site, but only site-scoped VLANs exist; refusing to auto-promote to siteless"
                )
                return None

            if other_site_vlans:
                logger.info(
                    f"VLAN {vlan_id} exists at other sites but not site {site_id}; creating a site-scoped VLAN for the requested site"
                )

            vlan_payload = dict(vlan_data)
            if site_id is not None:
                vlan_payload["site"] = site_id
            else:
                vlan_payload["description"] = siteless_description

            vlan_object = create_or_update('vlan', vlan_payload, preserve_existing_tags=True)
            if vlan_object is None:
                return None

            vlan_result = vlan_object.id if hasattr(vlan_object, 'id') else vlan_object
            cache_vlan_result(vlan_id, vlan_result, site_id=site_id, fallback_to_siteless=site_id is None)
            return vlan_result
    except ValueError as e:
        logger.error(f"Invalid VLAN ID format in {vlan}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing VLAN {vlan}: {e}")
        return None


def process_tags(vm_name, tags, cluster_id=None):
    query_params = {"name": vm_name}
    if cluster_id:
        query_params["cluster_id"] = cluster_id

    vm_getter = get_netbox_client()["client"].virtualization.virtual_machines.get
    vm = get_with_cache('virtualmachine', vm_getter, query_params)
    existing_tag_ids = [{"id": tag.id} for tag in getattr(vm, 'tags', [])] if vm else []

    new_tag_ids = []
    for category, tag_list in tags.items():
        for tag in tag_list:
            full_tag_name = f"{category}:{tag}"
            tag_id = create_or_update("tag", {"name": full_tag_name})
            if tag_id and {"id": tag_id} not in new_tag_ids:
                new_tag_ids.append({"id": tag_id})

    vm_sync_tag_id = create_or_update("tag", {"name": "VMWare-Sync"})
    if vm_sync_tag_id and {"id": vm_sync_tag_id} not in new_tag_ids:
        new_tag_ids.append({"id": vm_sync_tag_id})

    merged_tag_ids = existing_tag_ids[:]
    for new_tag in new_tag_ids:
        if new_tag not in merged_tag_ids:
            merged_tag_ids.append(new_tag)

    return merged_tag_ids


def process_host_and_nested(host_entry, netbox_url, netbox_token):
    logger.info(f"Processing host: {host_entry['host'].name}")
    threading.current_thread().name = f"Host-{host_entry['host'].name}"

    client_info = get_netbox_client()
    nb_client = client_info["client"]

    host_data = process_host(host_entry["host"])
    create_or_update(
        'device',
        host_data,
        protected_fields=PROTECTED_FIELDS,
        preserve_existing_tags=True
    )

    is_primary = True
    for interface_entry in host_entry["nics"]:
        logger.debug(f"Processing NIC: {interface_entry['name']} for host: {host_entry['host'].name}")
        interface_data = process_host_interface(interface_entry, host_data)
        vlan_id = None

        if 'vlan' in interface_entry and isinstance(interface_entry['vlan'], dict) and interface_entry['vlan'].get('id'):
            interface_entry['vlan']['tags'] = [{"name": "VMWare-Sync"}]
            vlan_id = process_vlan(interface_entry['vlan'], host_data.get('site'))
            if vlan_id:
                interface_data['tagged_vlans'] = [vlan_id]
                interface_data['mode'] = 'tagged'

        interface_id = create_or_update('interface', interface_data, preserve_existing_tags=True)
        if not interface_id:
            logger.error(f"Failed to create/update interface {interface_entry['name']}; skipping IP assignment")
            continue

        if 'address' in interface_entry:
            ip_data = {
                "address": interface_entry.get("address"),
                "prefix_length": interface_entry.get("prefix_length", 24 if ':' not in interface_entry.get("address", "") else 64),
                "tags": [{"name": "VMWare-Sync"}]
            }
            process_ip_address(
                nb_client,
                ip_data,
                {
                    "id": interface_id,
                    "description": interface_data["description"],
                    "device": interface_data.get("device"),
                },
                vlan_id,
                is_primary
            )
            is_primary = False

    for vm_entry in host_entry["vms"]:
        if any(x in vm_entry["vm"].name for x in ['Z-VRA', 'ISO', 'emplate']):
            continue

        logger.debug(f"Processing virtual machine: {vm_entry['vm'].name}")
        vm_data = process_vm(vm_entry["vm"], host_data)
        vm_data['tags'] = process_tags(
            vm_entry["vm"].name,
            vm_entry['tags'],
            cluster_id=vm_data.get('cluster')
        )
        vm_id = create_or_update(
            'virtualmachine',
            vm_data,
            protected_fields=PROTECTED_FIELDS,
            preserve_existing_tags=True
        )
        vm_site_id = get_virtual_machine_site_id(nb_client, vm_id, fallback_site_id=host_data.get('site'))

        is_primary_v4 = True
        is_primary_v6 = True
        for interface_entry in vm_entry["interfaces"]:
            vlan_id = None
            vm_interface_data = process_vm_interface(interface_entry, vm_data)
            if 'vlan' in interface_entry and isinstance(interface_entry['vlan'], dict) and interface_entry['vlan'].get('id'):
                interface_entry['vlan']['tags'] = [{"name": "VMWare-Sync"}]
                vlan_id = process_vlan(interface_entry['vlan'], vm_site_id)
                if vlan_id:
                    vm_interface_data['tagged_vlans'] = [vlan_id]
                    vm_interface_data['mode'] = 'tagged'

            vm_interface_data['tags'] = [{"name": "VMWare-Sync"}]
            vm_interface_id = create_or_update('vminterface', vm_interface_data, preserve_existing_tags=True)
            if not vm_interface_id:
                continue

            for ip_entry in interface_entry.get("ipv4_addresses", []) + interface_entry.get("ipv6_addresses", []):
                if not isinstance(ip_entry, dict):
                    continue
                ip_address = ip_entry.get("address")
                if not ip_address:
                    continue

                is_ipv6 = ':' in ip_address
                is_primary = is_primary_v6 if is_ipv6 else is_primary_v4
                ip_data = {
                    "address": ip_address,
                    "prefix_length": ip_entry.get("prefix_length", 64 if is_ipv6 else 24),
                    "tags": [{"name": "VMWare-Sync"}]
                }
                process_ip_address(
                    nb_client,
                    ip_data,
                    {
                        "id": vm_interface_id,
                        "description": vm_interface_data["description"],
                        "virtual_machine": vm_interface_data.get("virtual_machine"),
                    },
                    vlan_id,
                    is_primary
                )
                if is_ipv6:
                    is_primary_v6 = False
                else:
                    is_primary_v4 = False

        for disk_entry in vm_entry["disks"]:
            disk_data = process_disk(disk_entry, vm_data)
            create_or_update('virtualdisk', disk_data)

    return host_entry["host"].name


def process_single_vcenter(vcenter, username, password, netbox_url, netbox_token):
    
    logger.info(f"Processing vcenter: {vcenter}")

    session = RelativeSession(f'https://{vcenter}')
    session.headers.update({"Content-Type": "application/json"})
    login_url = f"/rest/com/vmware/cis/session"
    try:
        response = session.post(login_url, auth=(username, password))
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to authenticate with vCenter {vcenter}: {e}")
        return None

    logger.info(f"Authenticated to vcenter: {vcenter}")

    try:
        logger.info(f"Fetching {vcenter} cluster data (this takes a while)")

        cluster_data = fetch_vmware_data(SmartConnect(host=vcenter, user=username, pwd=password, sslContext=ssl_context), session)
        if not cluster_data:
            return None

        try:
            max_host_workers = int(os.getenv("MAX_HOST_WORKERS", "5"))
            if max_host_workers <= 0:
                logger.warning("Invalid MAX_HOST_WORKERS value <= 0; using default of 5")
                max_host_workers = 5
        except (TypeError, ValueError):
            logger.warning("Invalid MAX_HOST_WORKERS value; using default of 5")
            max_host_workers = 5

        logger.info(f"Starting {max_host_workers} host workers for: {vcenter}")

        with ThreadPoolExecutor(max_workers=max_host_workers) as executor:
            futures = []
            for cluster_entry in cluster_data:
                if 'Staging' in cluster_entry['cluster'].name:
                    continue
                logger.info(f"Processing Cluster {cluster_entry['cluster'].name}")
                cluster_data_entry = process_cluster(cluster_entry["cluster"])
                cluster_id = create_or_update('cluster', cluster_data_entry, preserve_existing_tags=True)
                if not cluster_id:
                    continue
                for host_entry in cluster_entry["hosts"]:
                    futures.append(executor.submit(process_host_and_nested, host_entry, netbox_url, netbox_token))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Host processing failed: {e}", exc_info=True)

        return cluster_data
    except Exception as e:
        logger.error(f"Error processing vCenter {vcenter}: {e}", exc_info=True)
        return None
    finally:
        try:
            Disconnect(SmartConnect(host=vcenter, user=username, pwd=password, sslContext=ssl_context))
        except Exception:
            pass


def extract_vcenter_vm_names_and_clusters(vcenter_vms):
    vcenter_vm_names = set()
    processed_clusters = set()

    flat_clusters = []
    for entry in vcenter_vms:
        if isinstance(entry, list):
            flat_clusters.extend(entry)
        elif isinstance(entry, dict):
            flat_clusters.append(entry)

    for cluster in flat_clusters:
        cluster_name = cluster.get('cluster_name')
        if cluster_name:
            processed_clusters.add(cluster_name)

        for host in cluster.get('hosts', []):
            for vm in host.get('vms', []):
                vm_name = vm.get('vm_name') or (vm['vm'].name.replace('.clemson.edu', '')[:64] if 'vm' in vm else None)
                if vm_name:
                    vcenter_vm_names.add(vm_name)

    return vcenter_vm_names, processed_clusters


def mark_decommissioned_vms(nb_client, vcenter_vm_names, processed_clusters):
    if not vcenter_vm_names or not processed_clusters:
        return

    cluster_ids = []
    for cluster_name in processed_clusters:
        cluster = get_with_cache('cluster', nb_client.virtualization.clusters.get, {"name": cluster_name})
        if cluster:
            cluster_ids.append(cluster.id)

    if not cluster_ids:
        return

    netbox_vms = netbox_read(
        nb_client.virtualization.virtual_machines.filter,
        tag="vmware-sync",
        cluster_id=cluster_ids,
        default=[]
    )

    for vm in netbox_vms:
        vm_name = vm.name
        current_status = vm.status.value if hasattr(vm.status, 'value') else str(vm.status)

        if vm_name not in vcenter_vm_names:
            if current_status != 'decommissioning':
                create_or_update(
                    'virtualmachine',
                    {"id": vm.id, "status": "decommissioning"},
                    preserve_existing_tags=True
                )
        elif current_status == 'decommissioning':
            create_or_update(
                'virtualmachine',
                {"id": vm.id, "status": "active"},
                preserve_existing_tags=True
            )


def delete_stale_decommissioned_vms(nb_client, decom_days):
    try:
        decom_days_int = int(decom_days)
    except (TypeError, ValueError):
        decom_days_int = 5

    cutoff_date = datetime.datetime.now().astimezone() - datetime.timedelta(days=decom_days_int)

    decom_vms = netbox_read(
        nb_client.virtualization.virtual_machines.filter,
        tag="vmware-sync",
        status="decommissioning",
        default=[]
    )

    for vm in decom_vms:
        last_updated = vm.last_updated
        if not last_updated:
            continue

        if isinstance(last_updated, str):
            try:
                last_updated = datetime.datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            except ValueError:
                continue

        if last_updated < cutoff_date:
            vm.delete()


def main():
    global VLAN_CACHE, HOST_AUTOSTART_CACHE
    
    username = os.environ.get("VCENTER_USERNAME")
    password = os.environ.get("VCENTER_PASSWORD")
    netbox_url = os.environ.get("NETBOX_URL")
    netbox_token = os.environ.get("NETBOX_TOKEN")
    decom_days = os.environ.get("DECOM_DAYS", "5")
    
    with open("vcenters.txt", "r") as f:
        vcenters = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

    if not all([username, password, netbox_url, netbox_token]):
        logger.error("Missing required environment variables")
        return

    client_info = get_netbox_client()
    nb_client = client_info["client"]
    
    start_cache_prewarm_threads(nb_client)
    
    # Clear per-run caches at the start of each sync run
    with VLAN_CACHE_LOCK:
        VLAN_CACHE.clear()
    with HOST_AUTOSTART_CACHE_LOCK:
        HOST_AUTOSTART_CACHE.clear()
    
    all_cluster_data = []
    max_vcenter_workers = min(len(vcenters), int(os.environ.get("MAX_VCENTER_WORKERS", "3")))

    with ThreadPoolExecutor(max_workers=max_vcenter_workers) as executor:
        futures = {
            executor.submit(process_single_vcenter, vcenter, username, password, netbox_url, netbox_token): vcenter
            for vcenter in vcenters
        }

        for future in as_completed(futures):
            vcenter = futures[future]
            try:
                cluster_data = future.result()
                if cluster_data:
                    all_cluster_data.append(cluster_data)
            except Exception as e:
                logger.error(f"vCenter {vcenter} failed: {e}", exc_info=True)

    if not all_cluster_data:
        log_performance_summary()
        return

    vcenter_vm_names, processed_clusters = extract_vcenter_vm_names_and_clusters(all_cluster_data)

    mark_decommissioned_vms(nb_client, vcenter_vm_names, processed_clusters)
    delete_stale_decommissioned_vms(nb_client, decom_days)

    log_performance_summary()


if __name__ == "__main__":
    main()