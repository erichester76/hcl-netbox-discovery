#!/usr/bin/env python3

import os
import logging
import ldap3
from ipaddress import ip_address, ip_network
import re
import time
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
import cu_tools
from pathlib import Path
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

LDAP_SERVER = os.getenv("LDAP_SERVER", "ldaps://yourldap.example.com")
LDAP_USER = os.getenv("LDAP_USER", "cn=admin,dc=example,dc=com")
LDAP_PASSWORD = os.getenv("LDAP_PASSWORD", "yourpassword")
LDAP_SEARCH_BASE = os.getenv("LDAP_SEARCH_BASE", "ou=Network Devices,o=jntsu")
LDAP_FILTER = os.getenv("LDAP_FILTER", "(&(DirXMLjnsuDHCPAddress=*)(|(DirXMLjnsuRegVersion=1)(DirXMLjnsuStaticAddrs=*)))")
NETBOX_URL = os.getenv("NETBOX_URL", "https://netbox.example.com")
NETBOX_TOKEN = os.getenv("NETBOX_TOKEN", "your_netbox_token")
NB_BRANCH = os.getenv("NB_BRANCH", "no-branch")

# Debug: Log what was actually read from environment
logger.info(f"Environment loaded: LDAP_SERVER={LDAP_SERVER}")
logger.info(f"Environment loaded: NETBOX_URL={NETBOX_URL}")
logger.info(f"Environment loaded: NETBOX_TOKEN={'*' * min(len(NETBOX_TOKEN), 8) if NETBOX_TOKEN != 'your_netbox_token' else 'DEFAULT_VALUE'}")

# Global cache for prefix lookups (stored as list of tuples)
prefix_cache = []


def _load_prefix_cache_from_redis():
    """Load prefix cache entries from centralized Redis pre-cache."""
    r = cu_tools.get_redis()
    if not r:
        logger.warning("Redis unavailable; centralized prefix pre-cache cannot be loaded")
        return []

    cache_type = cu_tools.cache_namespace("prefix")
    pattern = f"{cu_tools.REDIS_PREFIX}{cache_type}:*"
    loaded = []
    seen = set()

    try:
        for key in r.scan_iter(match=pattern, count=1000):
            payload = r.get(key)
            if not payload:
                continue

            try:
                cached_obj = pickle.loads(payload)
            except Exception:
                continue

            prefix_value = None
            if hasattr(cached_obj, "prefix"):
                prefix_value = getattr(cached_obj, "prefix", None)
            elif isinstance(cached_obj, dict):
                prefix_value = cached_obj.get("prefix")
            elif isinstance(cached_obj, str) and "/" in cached_obj:
                prefix_value = cached_obj

            if not prefix_value or prefix_value in seen:
                continue

            try:
                net = ip_network(prefix_value)
                loaded.append((net, prefix_value.split('/')[1]))
                seen.add(prefix_value)
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"Failed scanning Redis for prefix cache: {e}")
        return []

    loaded.sort(key=lambda x: x[0].prefixlen, reverse=True)
    return loaded


def build_prefix_cache(nb):
    """Build a sorted cache of all prefixes from most specific to least specific."""
    global prefix_cache
    logger.info("Building prefix cache...")

    redis_loaded = _load_prefix_cache_from_redis()
    if redis_loaded:
        prefix_cache = redis_loaded
        logger.info(f"Loaded {len(prefix_cache)} prefixes from centralized Redis pre-cache")
        return

    logger.warning("Centralized Redis prefix pre-cache is empty; falling back to direct NetBox prefix read")

    try:
        all_prefixes = list(nb.ipam.prefixes.all())
        # Store as (network object, prefix_length_str) tuples sorted by specificity
        prefix_cache = []
        for p in all_prefixes:
            try:
                net = ip_network(p.prefix)
                prefix_length_str = p.prefix.split('/')[1]
                prefix_cache.append((net, prefix_length_str))
            except Exception as e:
                logger.warning(f"Skipping invalid prefix {p.prefix}: {e}")
        # Sort by prefix length (most specific first)
        prefix_cache.sort(key=lambda x: x[0].prefixlen, reverse=True)
        logger.info(f"Cached {len(prefix_cache)} prefixes")
    except Exception as e:
        logger.error(f"Error building prefix cache: {e}")


def get_prefix_for_ip(ip_str):
    """Look up prefix from cache instead of API call"""
    try:
        ip_obj = ip_address(ip_str)
        for net, prefix_length_str in prefix_cache:
            if ip_obj in net:
                logger.debug(f"Prefix cache HIT for {ip_str}: {net.cidr}")
                return prefix_length_str
        logger.debug(f"Prefix cache MISS for {ip_str} (checked {len(prefix_cache)} prefixes)")
        return None
    except Exception as e:
        logger.error(f"Error looking up prefix for IP {ip_str}: {e}")
        return None


def is_transient_netbox_error(exc):
    error_text = str(exc)
    transient_markers = [" 503 ", "503 Service Unavailable", " 502 ", " 504 ", " 429 "]
    return any(marker in error_text for marker in transient_markers)


def is_mac_write_not_allowed_error(exc):
    error_text = str(exc)
    return (
        "405 Method Not Allowed" in error_text
        or "code 405" in error_text
        or "403 Forbidden" in error_text
        or "code 403" in error_text
    )


def process_single_ldap_entry(entry, mac_sync_enabled_ref, netbox_retries, netbox_retry_delay):
    """
    Process a single LDAP entry for IP/MAC sync.
    Backend-agnostic: delegates all NetBox operations to cu_tools.
    mac_sync_enabled_ref is a list [bool] to allow modification across threads.
    Returns dict with results or raises exception on error.
    """
    entry_start = time.time()
    try:
        ip = str(entry.DirXMLjnsuDHCPAddress)
        user_dn = str(entry.DirXMLjnsuUserDN)
        desc = str(entry.DirXMLjnsuDescription)
        device_name = str(entry.DirXMLjnsuDeviceName)
        lease_type = "Static" if hasattr(entry, 'DirXMLjnsuStaticAddrs') and len(entry.DirXMLJnsuStaticAddrs) > 0 else 'Registered'
        mac_address = str(entry.DirXMLjnsuHWAddress).upper() if hasattr(entry, 'DirXMLjnsuHWAddress') else None

        if '-ap' in desc or 'WAP' in desc:
            logger.debug(f"Skipping AP {desc}")
            return {"skipped": True, "ip": ip}

        if lease_type == "Static":
            full_description = f"{desc}"[:64]
        else:
            user_dn = re.sub(r'^cn=(.+),ou=.+$',r'\1@clemson.edu',user_dn).upper()
            full_description = f"{user_dn}: {desc}"[:64]

        full_description = re.sub('([\r\n]|\ {2,})','',full_description)
        full_description = re.sub('Connected to ','',full_description)

        # Time prefix lookup
        prefix_start = time.time()
        prefix_length = get_prefix_for_ip(ip)
        prefix_elapsed = time.time() - prefix_start

        if prefix_length:
            ip_with_prefix = f"{ip}/{prefix_length}"
        else:
            ip_with_prefix = ip

        logger.info(f"Processing IP {ip_with_prefix} {full_description} (prefix_lookup={prefix_elapsed:.3f}s)")

        # Add MAC address to NetBox MAC Address table (DCIM)
        mac_id = None
        if mac_address and mac_sync_enabled_ref[0]:
            mac_start = time.time()
            try:
                mac_payload = {
                    "mac_address": mac_address,
                    "description": f"{device_name} ({ip_with_prefix})",
                    "tags": [{"name": "JuNetSu"}]
                }
                # Add branch if specified (supported by Diode)
                if NB_BRANCH and NB_BRANCH != "no-branch":
                    mac_payload["branch"] = NB_BRANCH
                mac_id = cu_tools.create_or_update(
                    'mac_address',
                    mac_payload,
                    preserve_existing_tags=True,
                    retries=netbox_retries,
                    initial_delay_seconds=netbox_retry_delay,
                )
                mac_elapsed = time.time() - mac_start
                if mac_id:
                    logger.debug(f"Processed MAC address {mac_address} with ID {mac_id} ({mac_elapsed:.2f}s)")
            except Exception as e2:
                mac_elapsed = time.time() - mac_start
                if is_mac_write_not_allowed_error(e2):
                    mac_sync_enabled_ref[0] = False
                    logger.warning(
                        "MAC address writes are not allowed by this NetBox API/token; "
                        "disabling MAC sync for the rest of this run."
                    )
                logger.warning(f"Failed to update MAC address {mac_address}: {e2} ({mac_elapsed:.2f}s)")

        # Create or update IP address
        ip_start = time.time()
        cf_mac_value = [mac_id] if mac_id else []
        ip_payload = {
            'address': ip_with_prefix,
            'description': full_description,
            'status': 'dhcp',
            'tags': [{"name": "JuNetSu"}],
            'custom_fields': {'mac_address': cf_mac_value} if cf_mac_value else {}
        }
        # Add branch if specified (supported by Diode)
        if NB_BRANCH and NB_BRANCH != "no-branch":
            ip_payload["branch"] = NB_BRANCH

        ip_id = cu_tools.create_or_update(
            'ip_address',
            ip_payload,
            preserve_existing_tags=True,
            retries=netbox_retries,
            initial_delay_seconds=netbox_retry_delay,
        )
        ip_elapsed = time.time() - ip_start
        if ip_id:
            logger.debug(f"Processed IP address {ip_with_prefix} (ip_write={ip_elapsed:.2f}s)")

        total_elapsed = time.time() - entry_start
        logger.debug(f"Entry {ip_with_prefix} completed in {total_elapsed:.2f}s")

        return {"success": True, "ip": ip_with_prefix, "mac_id": mac_id, "ip_id": ip_id, "elapsed": total_elapsed}
    except Exception as e:
        total_elapsed = time.time() - entry_start
        logger.error(f"Failed to process entry: {e} ({total_elapsed:.2f}s)")
        raise




def main():
    netbox_retries = cu_tools.NETBOX_RETRIES
    netbox_retry_delay = cu_tools.NETBOX_RETRY_DELAY
    mac_sync_enabled = cu_tools.get_env("LDAP_ENABLE_MAC_SYNC", False)

    logger.info(f"Using NetBox backend: {cu_tools.NETBOX_BACKEND}")
    if not mac_sync_enabled:
        logger.info("MAC sync disabled for this run (LDAP_ENABLE_MAC_SYNC=false)")

    try:
        server = ldap3.Server(LDAP_SERVER, get_info=ldap3.ALL)
        conn = ldap3.Connection(server, user=LDAP_USER, password=LDAP_PASSWORD, auto_bind=True)
        logger.info("Connected to LDAP server")
    except Exception as e:
        logger.error(f"Failed to connect to LDAP server: {e}")
        raise SystemExit(1)

    try:
        # Initialize cu_tools NetBox client (backend agnostic)
        cu_tools._init_netbox_client()
        logger.info("Connected to NetBox")

    except Exception as e:
        logger.error(f"Failed to connect to NetBox: {e}")
        raise SystemExit(1)

    def run_ldap_query():
        logger.info("Retrieving DHCP Records from LDAP server")
        conn.search(
            search_base=LDAP_SEARCH_BASE,
            search_filter=LDAP_FILTER,
            attributes=[
                "DirXMLjnsuDHCPAddress",
                "DirXMLjnsuDeviceName",
                "DirXMLjnsuHWAddress",
                "DirXMLjnsuDescription",
                "DirXMLjnsuUserDN",
                "DirXMLJnsuDisabled",
                "DirXMLjnsuStaticAddrs"
            ]
        )
        return conn.entries


    def run_cache_warmup():
        backend = cu_tools.NETBOX_BACKEND
        if backend == "diode":
            logger.info("Diode backend detected: skipping prefix cache build")
            return

        logger.info("Starting local prefix cache build")

        client_info = cu_tools.get_netbox_client()
        nb = client_info.get("client") if client_info else None
        if nb is None:
            logger.warning("NetBox client unavailable during cache warmup; prefix cache not built")
            return

        build_prefix_cache(nb)

    try:
        logger.info("Starting LDAP query and NetBox cache warmup concurrently")
        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="startup") as executor:
            ldap_future = executor.submit(run_ldap_query)
            cache_future = executor.submit(run_cache_warmup)

            entries = ldap_future.result()
            cache_future.result()

        logger.info(f"Found {len(entries)} entries in LDAP")
        logger.info("All caches built successfully")
    except Exception as e:
        logger.error(f"Concurrent startup failed: {e}")
        raise SystemExit(1)

    # Process entries concurrently using a thread pool
    max_concurrent_writes = cu_tools.get_env("LDAP_MAX_CONCURRENT_WRITES", 5, min_value=1)
    mac_sync_enabled_ref = [mac_sync_enabled]  # Use list to allow modification in worker threads

    processed_count = 0
    failed_count = 0
    total_time = 0

    logger.info(f"Processing {len(entries)} LDAP entries with {max_concurrent_writes} concurrent writers")
    overall_start = time.time()

    with ThreadPoolExecutor(max_workers=max_concurrent_writes, thread_name_prefix="ip_writer") as executor:
        futures = {}
        for idx, entry in enumerate(entries):
            future = executor.submit(
                process_single_ldap_entry,
                entry,
                mac_sync_enabled_ref,
                netbox_retries,
                netbox_retry_delay
            )
            futures[future] = (idx, entry)

        # Wait for all to complete and collect results
        from concurrent.futures import as_completed

        for completed_future in as_completed(futures):
            idx, entry = futures[completed_future]
            try:
                result = completed_future.result()
                if not result.get("skipped"):
                    processed_count += 1
                    total_time += result.get("elapsed", 0)
                if idx % 500 == 0:
                    logger.info(f"Progress: {idx}/{len(entries)} entries processed")
            except Exception as e:
                failed_count += 1
                if failed_count <= 5:  # Log first 5 failures in detail
                    logger.error(f"Entry processing failed: {e}")

    overall_elapsed = time.time() - overall_start
    avg_time_per_entry = total_time / processed_count if processed_count > 0 else 0
    logger.info(f"LDAP sync completed: processed={processed_count}, failed={failed_count}, total_time={overall_elapsed:.2f}s, avg_per_entry={avg_time_per_entry:.2f}s")
    cu_tools.log_performance_summary()


if __name__ == "__main__":
    main()