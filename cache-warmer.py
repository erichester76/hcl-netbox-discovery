#!/usr/bin/env python3
"""
Central NetBox Cache Warmer – keeps Redis cache warm using sentinel keys
Runs continuously, refreshes on sentinel expiration
"""

from __future__ import annotations

import os
import time
import concurrent.futures

# Allow max prewarm workers to be set via env
MAX_PREWARM_WORKERS = int(os.getenv("MAX_PREWARM_WORKERS", "8"))
import logging
from datetime import datetime
from typing import Any

import pynetbox2
import redis

logger = logging.getLogger(__name__)


DEFAULT_PRECACHE_TYPES = "prefix,ip_address,mac_address"

OBJECT_TYPE_ALIASES = {
    "vm": "virtualmachine",
    "host": "device",
}

OBJECT_TYPE_TO_RESOURCE = {
    "prefix": "ipam.prefixes",
    "ip_address": "ipam.ip_addresses",
    "mac_address": "dcim.mac_addresses",
    "site": "dcim.sites",
    "location": "dcim.locations",
    "manufacturer": "dcim.manufacturers",
    "platform": "dcim.platforms",
    "tag": "extras.tags",
    "device": "dcim.devices",
    "devicetype": "dcim.device_types",
    "interface": "dcim.interfaces",
    "virtualmachine": "virtualization.virtual_machines",
    "vminterface": "virtualization.interfaces",
    "virtualdisk": "virtualization.virtual_disks",
    "vlan": "ipam.vlans",
    "cluster": "virtualization.clusters",
    "clustergroup": "virtualization.cluster_groups",
    "clustertype": "virtualization.cluster_types",
}

# Configurable via env
CHECK_INTERVAL_SECONDS = int(os.getenv("CACHE_WARMER_CHECK_INTERVAL", "300"))     # 5 min
SENTINEL_TTL_SECONDS   = int(os.getenv("PRECACHE_SENTINEL_TTL", "14400"))         # 4 hours

# Keep cache key namespace identical to vmware-sync/pynetbox2.
CACHE_KEY_PREFIX = os.getenv(
    "NETBOX_CACHE_KEY_PREFIX",
    os.getenv("REDIS_KEY_PREFIX", "netbox:"),
)
SENTINEL_KEY_PREFIX = CACHE_KEY_PREFIX + "precache:complete:"

NETBOX_BACKEND = os.getenv("NETBOX_BACKEND", "pynetbox").strip().lower()
NETBOX_URL = os.getenv("NETBOX_URL", "https://netbox.example.com").strip()
NETBOX_TOKEN = os.getenv("NETBOX_TOKEN", "").strip()
NETBOX_BRANCH = os.getenv("NETBOX_BRANCH", "").strip() or None

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0").strip()

REDIS_TTL_SECONDS = int(
    os.getenv(
        "NETBOX_CACHE_TTL_SECONDS",
        os.getenv("REDIS_TTL", os.getenv("REDIS_TTL_SECONDS", "86400")),
    )
)

NETBOX_RETRIES = int(os.getenv("NETBOX_RETRY_ATTEMPTS", os.getenv("NETBOX_RETRIES", "3")))
NETBOX_RETRY_DELAY = float(os.getenv("NETBOX_RETRY_INITIAL_DELAY", os.getenv("NETBOX_RETRY_DELAY", "0.3")))

RATE_LIMIT_PER_SECOND = float(os.getenv("NETBOX_RATE_LIMIT_PER_SECOND", os.getenv("NETBOX_RATE_LIMIT", "0")))
RATE_LIMIT_BURST = int(os.getenv("NETBOX_RATE_LIMIT_BURST", "1"))

DIODE_TARGET = os.getenv("DIODE_URL", "grpcs://localhost:8080").strip()
DIODE_CLIENT_ID = os.getenv("DIODE_CLIENT_ID", "").strip()
DIODE_CLIENT_SECRET = os.getenv("DIODE_CLIENT_SECRET", "").strip()
DIODE_CERT_FILE = os.getenv("DIODE_CERT_FILE", "").strip() or None
DIODE_SKIP_TLS_VERIFY = os.getenv("DIODE_SKIP_TLS_VERIFY", "false").strip().lower() in ("1", "true", "yes", "on")

_redis_client: redis.Redis | None = None
_nb_client: pynetbox2.NetBoxAPI | None = None

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(threadName)s] [%(levelname)s] [%(funcName)s] %(message)s',
    handlers=[logging.StreamHandler()]
)


def get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    try:
        _redis_client = redis.from_url(REDIS_URL, decode_responses=False)
        _redis_client.ping()
        logger.info("Redis connected: %s", REDIS_URL)
        return _redis_client
    except Exception as exc:
        logger.error("Failed to connect Redis at %s: %s", REDIS_URL, exc)
        _redis_client = None
        return None


def _normalize_object_type(name: str) -> str:
    lowered = name.strip().lower()
    return OBJECT_TYPE_ALIASES.get(lowered, lowered)


def get_enabled_precache_types() -> set[str]:
    configured = os.getenv("PRECACHE_OBJECT_TYPES", DEFAULT_PRECACHE_TYPES)
    return {
        _normalize_object_type(item)
        for item in configured.split(",")
        if item.strip()
    }


def get_netbox_client() -> dict[str, Any]:
    global _nb_client
    if _nb_client is not None:
        return {"type": NETBOX_BACKEND, "client": _nb_client}

    if NETBOX_BACKEND == "diode":
        return {"type": "diode", "client": None}

    if not NETBOX_URL or not NETBOX_TOKEN:
        raise RuntimeError("NETBOX_URL and NETBOX_TOKEN must be set")

    _nb_client = pynetbox2.api(
        url=NETBOX_URL,
        token=NETBOX_TOKEN,
        branch=NETBOX_BRANCH,
        backend="pynetbox",
        cache_backend="redis",
        cache_ttl_seconds=REDIS_TTL_SECONDS,
        cache_key_prefix=CACHE_KEY_PREFIX,
        redis_url=REDIS_URL,
        rate_limit_per_second=RATE_LIMIT_PER_SECOND,
        rate_limit_burst=RATE_LIMIT_BURST,
        retry_attempts=NETBOX_RETRIES,
        retry_initial_delay_seconds=NETBOX_RETRY_DELAY,
        retry_backoff_factor=float(os.getenv("NETBOX_RETRY_BACKOFF_FACTOR", "2.0")),
        retry_max_delay_seconds=float(os.getenv("NETBOX_RETRY_MAX_DELAY", "15.0")),
        retry_jitter_seconds=float(os.getenv("NETBOX_RETRY_JITTER", "0.0")),
        prewarm_sentinel_ttl_seconds=SENTINEL_TTL_SECONDS,
        diode_target=DIODE_TARGET,
        diode_client_id=DIODE_CLIENT_ID,
        diode_client_secret=DIODE_CLIENT_SECRET,
        diode_cert_file=DIODE_CERT_FILE,
        diode_skip_tls_verify=DIODE_SKIP_TLS_VERIFY,
    )
    logger.info(
        "Initialized pynetbox2 client: url=%s backend=%s cache_key_prefix=%s cache_ttl_seconds=%s",
        NETBOX_URL,
        NETBOX_BACKEND,
        CACHE_KEY_PREFIX,
        REDIS_TTL_SECONDS,
    )
    return {"type": NETBOX_BACKEND, "client": _nb_client}


def _resolve_precache_targets() -> tuple[dict[str, str], list[str]]:
    enabled_types = get_enabled_precache_types()
    mapping: dict[str, str] = {}
    unknown_types: list[str] = []

    for obj_type in sorted(enabled_types):
        resource = OBJECT_TYPE_TO_RESOURCE.get(obj_type)
        if resource is None:
            unknown_types.append(obj_type)
            continue
        mapping[obj_type] = resource

    if unknown_types:
        logger.warning("Unknown PRECACHE_OBJECT_TYPES entries skipped: %s", ",".join(sorted(unknown_types)))

    return mapping, sorted(mapping.keys())


def warm_the_cache(nb, object_type_resource_map, object_types_to_warm):
    if not nb:
        logger.error("Cannot get NetBox client → skipping precache")
        return

    if not object_types_to_warm:
        logger.warning("No valid pre-cache object types resolved → skipping precache")
        return

    logger.info(f"Starting NetBox precache for: {', '.join(object_types_to_warm)}")
    start = time.perf_counter()

    resource_filters = {
        object_type_resource_map[obj_type]: {}
        for obj_type in object_types_to_warm
        if obj_type in object_type_resource_map
    }

    if not resource_filters:
        logger.warning("No valid resources resolved for stale object types; skipping prewarm")
        return

    prewarm_summary = {}
    errors = {}
    def prewarm_one(resource, filters):
        try:
            result = nb.prewarm({resource: filters})
            return (resource, result.get(resource, 0))
        except Exception as e:
            logger.error(f"Precache failed for {resource}: {e}", exc_info=True)
            return (resource, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_PREWARM_WORKERS, len(resource_filters))) as executor:
        future_to_resource = {
            executor.submit(prewarm_one, resource, filters): resource
            for resource, filters in resource_filters.items()
        }
        for future in concurrent.futures.as_completed(future_to_resource):
            resource = future_to_resource[future]
            res, count = future.result()
            if count is not None:
                prewarm_summary[res] = count
            else:
                errors[res] = True

    duration = time.perf_counter() - start
    total_objects = sum(int(count) for count in prewarm_summary.values())
    logger.info(
        "Precache completed in %.1f seconds (%s objects across %s resources, %s errors)",
        duration,
        total_objects,
        len(prewarm_summary),
        len(errors),
    )


def main():
    logger.info("Cache Warmer started – monitoring sentinels")
    configured_types = sorted(get_enabled_precache_types())
    logger.info(f"Configured object types to warm: {', '.join(configured_types) if configured_types else '(none)'}")
    logger.info(f"Check interval: {CHECK_INTERVAL_SECONDS}s | Sentinel TTL: {SENTINEL_TTL_SECONDS}s")

    while True:
        try:
            client_info = get_netbox_client()
            if client_info["type"] == "diode":
                logger.debug("Diode backend skipping cache warmer cycle")
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue

            nb = client_info["client"]
            if not nb:
                logger.error("Cannot get NetBox client in warmer loop")
                time.sleep(CHECK_INTERVAL_SECONDS)
                continue

            object_type_resource_map, object_types_to_warm = _resolve_precache_targets()

            logger.info(f"Prewarm cycle triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            warm_the_cache(nb, object_type_resource_map, object_types_to_warm)
        except Exception as e:
            logger.error(f"Unexpected error in warmer loop: {e}", exc_info=True)

        logger.info(f"Sleeping for {CHECK_INTERVAL_SECONDS} seconds")
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()