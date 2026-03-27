#!/usr/bin/env python3
"""Lenovo XClarity Administrator → NetBox collector.

Pulls managed device data from the XClarity REST API and upserts it into
NetBox using the pynetbox2 client that lives alongside this script.

Usage
-----
  python collector.py [--env-file ENV_FILE] [--dry-run] [--verbose]

Configuration is read from environment variables.  A ``.env`` file is loaded
first (if present / specified), then any variables already set in the shell
take precedence.  See ``.env.example`` for the full list of supported keys.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import re
import sys
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# ---------------------------------------------------------------------------
# Ensure the directory that contains pynetbox2.py is on sys.path so that the
# module can be imported regardless of where the script is invoked from.
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import pynetbox2  # noqa: E402  (after sys.path manipulation)

logger = logging.getLogger(__name__)

# Capacity values above this threshold (in raw units from XClarity) are
# assumed to be in bytes and converted to GB.  Values at or below the
# threshold are assumed to already be in GB (some older XClarity versions
# report capacity in GB directly).
_CAPACITY_BYTES_THRESHOLD = 1_000_000

# NetBox limits interface names to 64 characters.  When we build a name as
# "{card_name} Port {port_num}" the card-name portion is capped here so the
# full string never exceeds the limit (worst-case suffix " Port XX" = 8 chars).
_IFACE_CARD_NAME_MAX = 55

# PSUs with output wattage above this threshold require a C19/C20 connector
# instead of the lighter-duty C13/C14 pairing.
_PSU_C20_WATTAGE_THRESHOLD = 1800

# Name, slug and colour used for the tag that is added to every item created
# or updated by this collector.
_SYNC_TAG_NAME = "XClarity-Sync"
_SYNC_TAG_SLUG = "xclarity-sync"
_SYNC_TAG_COLOR = "9e9e9e"

# JSON Schemas for each ModuleTypeProfile.  NetBox validates the ``attributes``
# field on a ModuleType against this schema on every save.  Critically, if the
# profile has *no* schema, NetBox wipes ``attributes`` to NULL on every save –
# so every profile we create must carry a schema that covers the attributes we
# write.
#
# Schemas use draft-07 / JSON Schema "object" with typed properties.
# ``additionalProperties`` is intentionally omitted so extra keys are allowed
# (future-proof against new XClarity fields).
_PROFILE_SCHEMAS: dict[str, dict] = {
    "CPU": {
        "type": "object",
        "properties": {
            "cores":        {"type": "integer", "title": "Cores"},
            "speed":        {"type": "number",  "title": "Speed (GHz)"},
            "architecture": {"type": "string",  "title": "Architecture"},
        },
    },
    "Memory": {
        "type": "object",
        "properties": {
            "size":      {"type": "integer", "title": "Size (GB)"},
            "class":     {"type": "string",  "title": "Memory Type"},
            "data_rate": {"type": "integer", "title": "Data Rate (MT/s)"},
            "ecc":       {"type": "boolean", "title": "ECC"},
        },
    },
    "Hard disk": {
        "type": "object",
        "properties": {
            "size":  {"type": "integer", "title": "Size (GB)"},
            "speed": {"type": "integer", "title": "RPM"},
            "type":  {"type": "string",  "title": "Media Type"},
        },
    },
    "Expansion card": {
        "type": "object",
        "properties": {
            "bandwidth":      {"type": "integer", "title": "Bandwidth (Gbps)"},
            "connector_type": {"type": "string",  "title": "Connector Type"},
        },
    },
    "Fan": {
        "type": "object",
        "properties": {
            "rpm": {"type": "integer", "title": "RPM"},
        },
    },
    "Power supply": {
        "type": "object",
        "properties": {
            "hot_swappable":  {"type": "boolean", "title": "Hot-swappable",   "default": False},
            "input_current":  {"type": "string",  "title": "Current type",    "default": "AC", "enum": ["AC", "DC"]},
            "input_voltage":  {"type": "integer", "title": "Voltage",         "default": 120},
            "wattage":        {"type": "integer", "description": "Available output power (watts)"},
        },
        "required": ["input_current", "input_voltage"],
    },
}

# NetBox resources that support the ``tags`` field.  The sync tag is appended
# to every object in these resources when it is created or updated.
_TAGGABLE_RESOURCES: frozenset[str] = frozenset({
    "dcim.devices",
    "dcim.device_roles",
    "dcim.device_types",
    "dcim.interfaces",
    "dcim.inventory_item_roles",
    "dcim.inventory_items",
    "dcim.locations",
    "dcim.manufacturers",
    "dcim.module_types",
    "dcim.modules",
    "dcim.platforms",
    "dcim.power_ports",
    "dcim.racks",
    "dcim.sites",
    "ipam.ip_addresses",
})

# ---------------------------------------------------------------------------
# XClarity client
# ---------------------------------------------------------------------------

class XClarityClient:
    """Minimal REST client for Lenovo XClarity Administrator.

    Only the endpoints needed by the collector are implemented:
    * ``/nodes``    – managed servers / compute nodes
    * ``/chassis``  – chassis (blade-centre chassis)
    * ``/switches`` – managed top-of-rack / embedded switches
    * ``/storage``  – managed storage subsystems

    The REST API is accessed directly at the host root (e.g. ``/nodes``),
    not under any ``/aicc`` prefix.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        port: int = 443,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        self.base_url = f"https://{host}:{port}"
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._session.verify = verify_ssl
        self._session.headers.update({"Accept": "application/json"})
        if not verify_ssl:
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        logger.debug("XClarity GET %s params=%s", url, params)
        response = self._session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_nodes(self) -> list[dict]:
        """Return all managed compute nodes (servers)."""
        data = self._get("/nodes")
        return data.get("nodeList", data) if isinstance(data, dict) else data

    def get_chassis(self) -> list[dict]:
        """Return all managed chassis."""
        data = self._get("/chassis")
        return data.get("chassisList", data) if isinstance(data, dict) else data

    def get_switches(self) -> list[dict]:
        """Return all managed switches."""
        data = self._get("/switches")
        return data.get("switchList", data) if isinstance(data, dict) else data

    def get_storage(self) -> list[dict]:
        """Return all managed storage devices."""
        data = self._get("/storage")
        return data.get("storageList", data) if isinstance(data, dict) else data

    def get_node_details(self, uuid: str) -> dict:
        """Return detailed information for a single node."""
        return self._get(f"/nodes/{uuid}")

    def get_chassis_details(self, uuid: str) -> dict:
        """Return detailed information for a single chassis."""
        return self._get(f"/chassis/{uuid}")


# ---------------------------------------------------------------------------
# Configuration loading
# ---------------------------------------------------------------------------

def _env(key: str, fallback: Optional[str] = None) -> str:
    """Return the value of environment variable *key*, or *fallback*."""
    return os.environ.get(key, fallback or "")


def load_env_file(path: Optional[str]) -> None:
    """Load variables from a ``.env`` file into the process environment.

    Shell variables already set take precedence (``override=False``).
    If *path* is given explicitly and the file does not exist the process exits.
    When *path* is ``None`` the default ``.env`` in the current directory is
    loaded if it exists (silently skipped when absent).
    """
    if path:
        if not os.path.exists(path):
            logger.error(".env file not found: %s", path)
            sys.exit(1)
        load_dotenv(dotenv_path=path, override=False)
    else:
        load_dotenv(override=False)  # loads .env in cwd if present, no-op otherwise


def _validate_env() -> None:
    """Abort if any required environment variable is unset or empty."""
    required = [
        "XCLARITY_HOST",
        "XCLARITY_USERNAME",
        "XCLARITY_PASSWORD",
        "NETBOX_URL",
        "NETBOX_TOKEN",
    ]
    missing = [key for key in required if not _env(key)]
    if missing:
        for key in missing:
            logger.error("Missing required environment variable: %s", key)
        sys.exit(1)


# ---------------------------------------------------------------------------
# NetBox helper: ensure prerequisite objects exist
# ---------------------------------------------------------------------------

class NetBoxSync:
    """Thin wrapper around pynetbox2 that handles prerequisite lookups/creation."""

    # Lenovo is always the manufacturer for XClarity-managed hardware
    MANUFACTURER_NAME = "Lenovo"
    MANUFACTURER_SLUG = "lenovo"

    def __init__(self, nb: pynetbox2.NetBoxAPI, dry_run: bool = False) -> None:
        self.nb = nb
        self.dry_run = dry_run
        self._manufacturer_cache: dict[str, int] = {}
        self._sync_tag_id: Optional[int] = None

    # ------------------------------------------------------------------
    # Manufacturer
    # ------------------------------------------------------------------

    def ensure_manufacturer(self, name: Optional[str] = None) -> Optional[int]:
        """Return the NetBox ID for a manufacturer, creating it if needed.

        If *name* is ``None`` the default Lenovo manufacturer is used.
        Results are cached so repeated calls do not hit the API twice.
        """
        mfr_name = name or self.MANUFACTURER_NAME
        if mfr_name in self._manufacturer_cache:
            return self._manufacturer_cache[mfr_name]
        mfr_slug = _slugify(mfr_name)
        obj = self._upsert(
            "dcim.manufacturers",
            {"name": mfr_name, "slug": mfr_slug},
            lookup_fields=["slug"],
        )
        mfr_id = self._id(obj)
        if mfr_id is not None:
            self._manufacturer_cache[mfr_name] = mfr_id
        return mfr_id

    # ------------------------------------------------------------------
    # Sync tag
    # ------------------------------------------------------------------

    def ensure_sync_tag(self) -> Optional[int]:
        """Return the NetBox ID for the XClarity-Sync tag, creating it if needed.

        The tag is created once per run and the result is cached so that
        repeated calls do not hit the API again.  A direct ``nb.upsert`` call
        is used here (bypassing the ``_upsert`` wrapper) to avoid any chance
        of recursive tag-injection for the tag object itself.
        """
        if self._sync_tag_id is not None:
            return self._sync_tag_id
        if self.dry_run:
            logger.info("[DRY-RUN] ensure sync tag %s", _SYNC_TAG_NAME)
            return None
        try:
            obj = self.nb.upsert(
                "extras.tags",
                {"name": _SYNC_TAG_NAME, "slug": _SYNC_TAG_SLUG, "color": _SYNC_TAG_COLOR},
                lookup_fields=["slug"],
            )
            self._sync_tag_id = self._id(obj)
        except Exception as exc:
            logger.error("Failed to ensure sync tag: %s", exc)
        return self._sync_tag_id

    # ------------------------------------------------------------------
    # Device type
    # ------------------------------------------------------------------

    def ensure_device_type(self, model: str, part_number: str = "", manufacturer_name: Optional[str] = None) -> Optional[int]:
        """Return the NetBox ID for a device type, creating it if needed.

        Device types are looked up by ``manufacturer`` + ``model`` (which is
        unique in NetBox) rather than by ``slug`` because a device type may
        have been created in NetBox manually – or by a previous version of this
        code that generated a different slug – causing a slug-based lookup to
        return zero results even though the device type already exists.
        """
        if not model:
            return None
        slug = _slugify(model)
        manufacturer_id = self.ensure_manufacturer(manufacturer_name)
        payload: dict[str, Any] = {
            "manufacturer": manufacturer_id,
            "model": model,
            "slug": slug,
        }
        if part_number:
            payload["part_number"] = part_number
        obj = self._upsert("dcim.device_types", payload, lookup_fields=["manufacturer", "model"])
        return self._id(obj)

    # ------------------------------------------------------------------
    # Device role
    # ------------------------------------------------------------------

    def ensure_device_role(self, name: str, slug: str, color: str = "9e9e9e") -> Optional[int]:
        """Return the NetBox ID for a device role, creating it if needed."""
        obj = self._upsert(
            "dcim.device_roles",
            {"name": name, "slug": slug, "color": color},
            lookup_fields=["slug"],
        )
        return self._id(obj)

    # ------------------------------------------------------------------
    # Site
    # ------------------------------------------------------------------

    def ensure_site(self, name: str, slug: str) -> Optional[int]:
        """Return the NetBox ID for a site, creating it if needed.

        Sites are looked up by ``name`` (which is unique in NetBox) rather than
        ``slug`` because a site may have been created in NetBox manually with a
        different slug than the one generated here, causing a slug-based lookup
        to return zero results even though the site already exists.
        """
        obj = self._upsert("dcim.sites", {"name": name, "slug": slug}, lookup_fields=["name"])
        return self._id(obj)

    # ------------------------------------------------------------------
    # Location (room / area within a site)
    # ------------------------------------------------------------------

    def ensure_location(self, name: str, site_id: int) -> Optional[int]:
        """Return the NetBox ID for a location (room/area within a site), creating it if needed."""
        slug = _slugify(name)
        obj = self._upsert(
            "dcim.locations",
            {"name": name, "slug": slug, "site": site_id},
            lookup_fields=["name", "site"],
        )
        return self._id(obj)

    # ------------------------------------------------------------------
    # Rack
    # ------------------------------------------------------------------

    def ensure_rack(self, name: str, site_id: int, location_id: Optional[int] = None) -> Optional[int]:
        """Return the NetBox ID for a rack, creating it if needed."""
        payload: dict[str, Any] = {"name": name, "site": site_id}
        if location_id is not None:
            payload["location"] = location_id
        obj = self._upsert("dcim.racks", payload, lookup_fields=["name", "site"])
        return self._id(obj)

    # ------------------------------------------------------------------
    # Platform
    # ------------------------------------------------------------------

    def ensure_platform(self, name: str, slug: str) -> Optional[int]:
        """Return the NetBox ID for a platform, creating it if needed."""
        manufacturer_id = self.ensure_manufacturer()
        obj = self._upsert(
            "dcim.platforms",
            {"name": name, "slug": slug, "manufacturer": manufacturer_id},
            lookup_fields=["slug"],
        )
        return self._id(obj)

    # ------------------------------------------------------------------
    # Device
    # ------------------------------------------------------------------

    def upsert_device(self, payload: dict[str, Any]) -> Optional[Any]:
        """Create or update a device record.  *payload* must include ``name`` and ``serial``."""
        lookup = ["serial"] if payload.get("serial") else ["name"]
        return self._upsert("dcim.devices", payload, lookup_fields=lookup)

    def ensure_inventory_item_role(self, name: str) -> Optional[int]:
        """Return the NetBox ID for an inventory item role, creating it if needed."""
        slug = _slugify(name)
        obj = self._upsert(
            "dcim.inventory_item_roles",
            {"name": name, "slug": slug, "color": "9e9e9e"},
            lookup_fields=["slug"],
        )
        return self._id(obj)

    def ensure_module_bay_template(self, device_type_id: int, name: str, position: str = "") -> Optional[int]:
        """Return the NetBox ID for a module bay template on a device type, creating it if needed.

        Module bay templates are the blueprint entries attached to a device type.
        When a device is created from that type, NetBox instantiates a matching
        ``ModuleBay`` for every template automatically.
        """
        payload: dict[str, Any] = {"device_type": device_type_id, "name": name}
        if position:
            payload["position"] = position
        obj = self._upsert(
            "dcim.module_bay_templates",
            payload,
            lookup_fields=["device_type", "name"],
        )
        return self._id(obj)

    # ------------------------------------------------------------------
    # Module bay (slot instance on a device)
    # ------------------------------------------------------------------

    def ensure_module_bay(self, device_id: int, name: str, position: str = "") -> Optional[int]:
        """Return the NetBox ID for a module bay on a device, creating it if needed."""
        payload: dict[str, Any] = {"device": device_id, "name": name}
        if position:
            payload["position"] = position
        obj = self._upsert(
            "dcim.module_bays",
            payload,
            lookup_fields=["device", "name"],
        )
        return self._id(obj)

    # ------------------------------------------------------------------
    # Module type profile
    # ------------------------------------------------------------------


    def ensure_module_type_profile(
        self,
        name: str,
        schema: Optional[dict] = None,
    ) -> Optional[int]:
        """Return the NetBox ID for a module type profile, creating it if needed.

        Module type profiles (NetBox 4.0+) allow grouping module types and
        attaching shared custom fields to each category (CPU, Memory, etc.).
        If the endpoint does not exist (pre-4.0) the call returns None silently.

        *schema*, when supplied, is persisted as the profile's JSON Schema.
        NetBox validates ``attributes`` on a ModuleType against this schema
        on every save; if the profile has *no* schema NetBox will wipe the
        ``attributes`` field to NULL.  Always pass a schema when you intend
        to set attributes on module types that use this profile.
        """
        payload: dict[str, Any] = {"name": name}
        if schema is not None:
            payload["schema"] = schema
        obj = self._upsert(
            "dcim.module_type_profiles",
            payload,
            lookup_fields=["name"],
        )
        return self._id(obj)

    # ------------------------------------------------------------------
    # Module type
    # ------------------------------------------------------------------

    def ensure_module_type(
        self,
        model: str,
        manufacturer_name: Optional[str] = None,
        profile_name: Optional[str] = None,
        attributes: Optional[dict] = None,
    ) -> Optional[int]:
        """Return the NetBox ID for a module type, creating it if needed.

        *profile_name*, when supplied, is used to look up or create a
        ``ModuleTypeProfile`` (NetBox 4.0+) which is then linked to the
        module type via the ``profile`` field.  The profile is always
        created with a JSON Schema (from ``_PROFILE_SCHEMAS``) so that
        NetBox does not wipe ``attributes`` on save.

        *attributes*, when supplied, is sent as the ``attributes`` field on
        the module type so that profile-specific fields (e.g. CPU cores/speed)
        are visible on the type record in NetBox.

        The profile is applied in a dedicated first request so that it is
        persisted before ``attributes`` is sent.  NetBox validates attribute
        values against the profile schema; sending both fields in a single
        request causes the attributes to be silently ignored on some NetBox
        versions (order-of-operations issue).

        Module types are looked up by ``manufacturer`` + ``model`` (which is
        unique in NetBox) rather than by ``slug`` because two different model
        names may slugify to the same value (e.g. when they differ only in
        special characters), causing a slug-based lookup to match the wrong
        existing module type instead of creating a new one.
        """
        if not model:
            return None
        slug = _slugify(model)
        manufacturer_id = self.ensure_manufacturer(manufacturer_name)
        payload: dict[str, Any] = {
            "manufacturer": manufacturer_id,
            "model": model,
            "slug": slug,
        }
        if profile_name:
            profile_schema = _PROFILE_SCHEMAS.get(profile_name)
            profile_id = self.ensure_module_type_profile(profile_name, schema=profile_schema)
            if profile_id is not None:
                payload["profile"] = profile_id
        # Step 1: create/update the module type with the profile set.
        # attributes is intentionally omitted here so that the profile is
        # committed to NetBox before attributes are applied in step 2.
        obj = self._upsert(
            "dcim.module_types",
            payload,
            lookup_fields=["manufacturer", "model"],
        )
        module_type_id = self._id(obj)

        # Step 2: apply attributes in a direct PATCH after the profile has
        # been persisted.  The NetBox API field is "attributes" (not
        # "attribute_data").  A direct _update (PATCH) is used here instead
        # of _upsert so that attributes are always written without being
        # filtered out by the diff/cache logic in _upsert.
        if module_type_id and attributes:
            clean_attrs = {k: v for k, v in attributes.items() if v is not None}
            if clean_attrs:
                self._update("dcim.module_types", module_type_id, {"attributes": clean_attrs})

        return module_type_id

    # ------------------------------------------------------------------
    # Module (installed component)
    # ------------------------------------------------------------------

    def upsert_module(self, payload: dict[str, Any]) -> Optional[Any]:
        """Create or update a module installed in a module bay."""
        return self._upsert("dcim.modules", payload, lookup_fields=["device", "module_bay"])

    # ------------------------------------------------------------------
    # Interface
    # ------------------------------------------------------------------

    def upsert_interface(self, payload: dict[str, Any]) -> Optional[Any]:
        """Create or update an interface on a device."""
        return self._upsert("dcim.interfaces", payload, lookup_fields=["device", "name"])

    # ------------------------------------------------------------------
    # Power port
    # ------------------------------------------------------------------

    def upsert_power_port(self, payload: dict[str, Any]) -> Optional[Any]:
        """Create or update a power port on a device (or module)."""
        return self._upsert("dcim.power_ports", payload, lookup_fields=["device", "module", "name"])

    # ------------------------------------------------------------------
    # IP address
    # ------------------------------------------------------------------

    def upsert_ip_address(self, payload: dict[str, Any]) -> Optional[Any]:
        """Create or update an IP address record."""
        return self._upsert("ipam.ip_addresses", payload, lookup_fields=["address"])

    # ------------------------------------------------------------------
    # Inventory item
    # ------------------------------------------------------------------

    def upsert_inventory_item(self, payload: dict[str, Any]) -> Optional[Any]:
        """Create or update an inventory item on a device."""
        return self._upsert(
            "dcim.inventory_items",
            payload,
            lookup_fields=["device", "name"],
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _upsert(
        self,
        resource: str,
        payload: dict[str, Any],
        lookup_fields: Optional[list[str]] = None,
    ) -> Optional[Any]:
        if self.dry_run:
            logger.info("[DRY-RUN] upsert %s %s", resource, payload)
            return None
        try:
            obj = self.nb.upsert(resource, payload, lookup_fields=lookup_fields)
            if resource in _TAGGABLE_RESOURCES:
                obj = self._append_sync_tag(resource, obj)
            return obj
        except Exception as exc:
            logger.error("Failed to upsert %s payload=%s: %s", resource, payload, exc)
            return None

    def _update(
        self,
        resource: str,
        object_id: int,
        data: dict[str, Any],
    ) -> Optional[Any]:
        if self.dry_run:
            logger.info("[DRY-RUN] update %s id=%s %s", resource, object_id, data)
            return None
        try:
            return self.nb.update(resource, object_id, data)
        except Exception as exc:
            logger.error("Failed to update %s id=%s data=%s: %s", resource, object_id, data, exc)
            return None

    def _append_sync_tag(self, resource: str, obj: Any) -> Any:
        """Ensure the XClarity-Sync tag is present on *obj*.

        Existing tags are preserved; the sync tag is only appended when it is
        not already there.  A direct ``nb.update`` call is used so that the
        tag injection bypasses the diff/cache logic in pynetbox2's upsert.
        """
        if obj is None:
            return obj
        obj_id = self._id(obj)
        if obj_id is None:
            return obj

        existing_tags = getattr(obj, "tags", None) or []
        for tag in existing_tags:
            if getattr(tag, "name", None) == _SYNC_TAG_NAME:
                return obj  # Already present – nothing to do.

        tag_id = self.ensure_sync_tag()
        if tag_id is None:
            return obj

        # Preserve all existing tags and append the sync tag.
        new_tags = [
            {"id": getattr(t, "id")}
            for t in existing_tags
            if getattr(t, "id", None) is not None
        ]
        new_tags.append({"id": tag_id})
        try:
            updated = self.nb.update(resource, obj_id, {"tags": new_tags})
            return updated if updated is not None else obj
        except Exception as exc:
            logger.warning("Failed to add sync tag to %s id=%s: %s", resource, obj_id, exc)
            return obj

    @staticmethod
    def _id(obj: Any) -> Optional[int]:
        if obj is None:
            return None
        if isinstance(obj, int):
            return obj
        return getattr(obj, "id", None)


# ---------------------------------------------------------------------------
# Main collector logic
# ---------------------------------------------------------------------------

class Collector:
    """Orchestrates data collection from XClarity and sync to NetBox."""

    # Map from XClarity ``type`` values to human-readable labels
    _TYPE_LABELS: dict[str, str] = {
        "Rack-Tower Server": "server",
        "Blade Server": "server",
        "Dense Server": "server",
        "System x": "server",
        "ThinkSystem": "server",
    }

    def __init__(
        self,
        xc: XClarityClient,
        nb_sync: NetBoxSync,
    ) -> None:
        self.xc = xc
        self.nb_sync = nb_sync
        self._categories = [c.strip() for c in _env("COLLECTOR_CATEGORIES", "nodes,chassis,switches,storage").split(",")]
        self._sync_interfaces = _env("COLLECTOR_SYNC_INTERFACES", "true").lower() not in ("false", "0", "no")
        self._sync_inventory = _env("COLLECTOR_SYNC_INVENTORY", "true").lower() not in ("false", "0", "no")
        self._use_modules = _env("COLLECTOR_USE_MODULES", "false").lower() not in ("false", "0", "no")
        self._max_workers = int(_env("COLLECTOR_MAX_WORKERS", "10"))

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("Collector starting. Categories: %s", self._categories)

        if "nodes" in self._categories:
            self._collect_nodes()
        if "chassis" in self._categories:
            self._collect_chassis()
        if "switches" in self._categories:
            self._collect_switches()
        if "storage" in self._categories:
            self._collect_storage()

        logger.info("Collector finished.")

    # ------------------------------------------------------------------
    # Parallel dispatch helper
    # ------------------------------------------------------------------

    @staticmethod
    def _device_name(device: dict) -> str:
        """Return a human-readable identifier for *device* suitable for log messages."""
        return (
            device.get("name")
            or device.get("hostname")
            or device.get("uuid", "unknown")
        )

    def _run_parallel(self, items: list, sync_func, category: str) -> None:
        """Submit *sync_func(item)* for every item in *items* via a thread pool.

        Exceptions raised by individual sync calls are caught and logged so
        that a failure for one device does not abort the rest of the batch.
        *max_workers* comes from the ``COLLECTOR_MAX_WORKERS`` environment
        variable (default: 10).
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {executor.submit(sync_func, item): item for item in items}
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    item = futures[future]
                    logger.error(
                        "Unhandled error syncing %s %s: %s",
                        category,
                        self._device_name(item),
                        exc,
                    )

    # ------------------------------------------------------------------
    # Nodes (servers)
    # ------------------------------------------------------------------

    def _collect_nodes(self) -> None:
        logger.info("Collecting nodes (servers) …")
        try:
            nodes = self.xc.get_nodes()
        except Exception as exc:
            logger.error("Failed to retrieve nodes from XClarity: %s", exc)
            return

        logger.info("Found %d node(s).", len(nodes))
        self._run_parallel(nodes, self._sync_node, "node")

    def _sync_node(self, node: dict) -> None:
        raw_name = node.get("name") or node.get("hostname") or node.get("uuid", "unknown")
        name = self._apply_name_regex(str(raw_name))
        logger.debug("Syncing node: %s", name)

        model = _build_model_name(node)
        part_number = node.get("partNumber") or node.get("productName") or ""
        serial = node.get("serialNumber") or node.get("serial") or ""
        mfr_name = node.get("manufacturer") or None

        device_type_id = self.nb_sync.ensure_device_type(model, part_number, mfr_name)
        role_slug = _env("NETBOX_SERVER_ROLE", "server")
        role_id = self.nb_sync.ensure_device_role(
            name=role_slug.title(),
            slug=role_slug,
            color="2196f3",
        )
        site_id, location_id, rack_id, rack_position = self._resolve_placement(node)

        if device_type_id is None or role_id is None or site_id is None:
            logger.warning("Skipping node %s: missing device_type/role/site", name)
            return

        payload: dict[str, Any] = {
            "name": name,
            "device_type": device_type_id,
            "role": role_id,
            "site": site_id,
            "status": "active",
        }
        if serial:
            payload["serial"] = serial
        if location_id is not None:
            payload["location"] = location_id
        if rack_id is not None:
            payload["rack"] = rack_id
            payload["face"] = "front"
        if rack_position is not None and rack_id is not None:
            payload["position"] = rack_position

        device = self.nb_sync.upsert_device(payload)

        if device is None:
            return

        device_id = self.nb_sync._id(device)
        if device_id is None:
            return

        if self._sync_inventory and self._use_modules:
            # Sync modules first so the slot→module map is available when
            # creating interfaces (interfaces reference their parent module).
            slot_module_map = self._sync_node_modules(node, device_id)
        else:
            slot_module_map = {}

        if self._sync_interfaces:
            self._sync_node_interfaces(node, device_id, module_map=slot_module_map)

        if self._sync_inventory and not self._use_modules:
            self._sync_node_inventory(node, device_id)

    def _sync_node_interfaces(
        self,
        node: dict,
        device_id: int,
        module_map: Optional[dict[str, int]] = None,
    ) -> None:
        """Sync NIC / management interfaces reported by XClarity for a node.

        XClarity exposes network ports through PCI device records that carry a
        ``portInfo.physicalPorts`` list.  Each physical port has a speed (Gbps)
        and zero or more logical ports that carry the MAC address.

        Older XClarity payloads (or non-standard keys) may instead use the
        ``adapterList`` / ``adapters`` / ``networkAdapters`` structure which is
        also handled below for backwards compatibility.

        *module_map* is a dict mapping module-bay name → NetBox module ID,
        built by ``_sync_node_modules`` when running in modules mode.  When
        supplied, each PCI-adapter interface is linked to the module installed
        in the corresponding bay so that NetBox shows the interface under the
        correct installed component.
        """
        if module_map is None:
            module_map = {}

        # ----------------------------------------------------------------
        # 1. PCI-based adapters (new-style: portInfo.physicalPorts)
        #
        # Process onboard devices (LOM) separately from add-in cards so that
        # the first onboard ethernet port can be identified as the management
        # (ILOM/LOM) interface instead of creating a synthetic "mgmt0".
        # ----------------------------------------------------------------

        # When node.get("pciDevices") already provides a combined list we
        # cannot distinguish onboard from add-in.  Prefer the split lists
        # when available.
        if node.get("pciDevices"):
            # Pre-combined: identify onboard devices by name heuristic
            onboard_pci_devices: list[dict] = [
                d for d in node.get("pciDevices")
                if any(
                    kw in (d.get("name") or d.get("productName") or "").upper()
                    for kw in ("LOM", "ONBOARD", "INTEGRATED", "EMBEDDED")
                )
            ]
            addin_pci_devices: list[dict] = [
                d for d in node.get("pciDevices")
                if d not in onboard_pci_devices
            ]
        else:
            onboard_pci_devices = list(node.get("onboardPciDevices") or [])
            addin_pci_devices = list(node.get("addinCards") or [])

        # The first ethernet port on the first onboard device becomes the
        # management (ILOM / LOM) interface; its ID is used for the BMC IP.
        mgmt_iface_id: Optional[int] = None

        def _sync_pci_device_ports(
            pci_dev: dict,
            is_first_onboard: bool = False,
        ) -> Optional[int]:
            """Sync all ethernet ports from *pci_dev*.

            Returns the ID of the first ethernet interface when
            *is_first_onboard* is True (used as the management interface),
            or None otherwise.
            """
            port_info = pci_dev.get("portInfo") or {}
            physical_ports = port_info.get("physicalPorts") or []
            if not physical_ports:
                return None

            dev_name = pci_dev.get("name") or pci_dev.get("productName") or "Adapter"

            # Derive the slot name the same way _sync_node_modules does so
            # we can look the installed module up in module_map.
            slot_name = (
                pci_dev.get("slotName")
                or (
                    f"PCIe Slot {pci_dev.get('slotNumber', pci_dev.get('slot', '?'))}"
                    if pci_dev.get("slotNumber") or pci_dev.get("slot")
                    else None
                )
            )
            module_id = module_map.get(slot_name) if slot_name else None

            first_eth_iface_id: Optional[int] = None
            for phys_port in physical_ports:
                port_type_str = (phys_port.get("portType") or "").upper()
                if "ETHERNET" not in port_type_str and "ETH" not in port_type_str:
                    continue

                port_num = (
                    phys_port.get("portNumber")
                    or phys_port.get("physicalPortIndex")
                    or "?"
                )
                iface_name = f"{dev_name[:_IFACE_CARD_NAME_MAX]} Port {port_num}"

                # Extract MAC from the first logical port that has one
                mac = ""
                for lp in (phys_port.get("logicalPorts") or []):
                    mac = _normalise_mac(lp.get("addresses") or "")
                    if mac:
                        break

                # Speed is reported in Gbps on physical ports
                speed_gbps = phys_port.get("speed")
                iface_type = (
                    _port_type_gbps(speed_gbps)
                    if speed_gbps is not None
                    else _port_type(phys_port)
                )


                iface_payload: dict[str, Any] = {
                    "device": device_id,
                    "name": iface_name,
                    "type": iface_type,
                }
                if mac:
                    iface_payload["mac_address"] = mac
                if module_id is not None:
                    iface_payload["module"] = module_id
                # Mark the first port of the first onboard adapter as
                # management-only (this is the LOM / ILOM port).
                if is_first_onboard and first_eth_iface_id is None:
                    iface_payload["mgmt_only"] = True

                iface = self.nb_sync.upsert_interface(iface_payload)
                iface_id = self.nb_sync._id(iface)

                if first_eth_iface_id is None and iface_id is not None:
                    first_eth_iface_id = iface_id

                # IPs attached to logical ports (rare, but supported)
                if iface_id:
                    for lp in (phys_port.get("logicalPorts") or []):
                        for ip_key in ("ipInterfaces", "ipAddresses", "ips"):
                            for ip_info in lp.get(ip_key) or []:
                                address = (
                                    ip_info.get("address")
                                    or ip_info.get("IPv4addresses")
                                    or ip_info.get("ipv4Address")
                                )
                                if isinstance(address, list):
                                    address = address[0] if address else None
                                if not address:
                                    continue
                                cidr = _to_cidr(address, ip_info.get("subnet") or ip_info.get("netmask"))
                                if cidr:
                                    self.nb_sync.upsert_ip_address({
                                        "address": cidr,
                                        "assigned_object_type": "dcim.interface",
                                        "assigned_object_id": iface_id,
                                        "status": "active",
                                    })

            return first_eth_iface_id if is_first_onboard else None

        # Process onboard (LOM) devices first; capture the management iface ID
        # from the first onboard device's first ethernet port.
        for idx, pci_dev in enumerate(onboard_pci_devices):
            result = _sync_pci_device_ports(pci_dev, is_first_onboard=(idx == 0))
            if idx == 0 and result is not None:
                mgmt_iface_id = result

        # Process add-in cards
        for pci_dev in addin_pci_devices:
            _sync_pci_device_ports(pci_dev)

        # ----------------------------------------------------------------
        # 2. Old-style adapters (adapterList / adapters / networkAdapters)
        # ----------------------------------------------------------------
        adapters = (
            node.get("adapterList")
            or node.get("adapters")
            or node.get("networkAdapters")
            or []
        )
        for adapter in adapters:
            ports = adapter.get("portList") or adapter.get("ports") or []
            for port in ports:
                port_index = port.get("portIndex", "?")
                iface_name = port.get("portName") or port.get("name") or f"port-{port_index}"
                mac = _normalise_mac(port.get("macAddress") or port.get("mac") or "")
                iface_payload = {
                    "device": device_id,
                    "name": iface_name,
                    "type": _port_type(port),
                }
                if mac:
                    iface_payload["mac_address"] = mac

                iface = self.nb_sync.upsert_interface(iface_payload)
                iface_id = self.nb_sync._id(iface)
                if iface_id is None:
                    continue

                for ip_key in ("ipInterfaces", "ipAddresses", "ips"):
                    for ip_info in port.get(ip_key) or []:
                        address = (
                            ip_info.get("IPv4addresses")
                            or ip_info.get("address")
                            or ip_info.get("ipv4Address")
                        )
                        if isinstance(address, list):
                            address = address[0] if address else None
                        if not address:
                            continue
                        cidr = _to_cidr(address, ip_info.get("subnet") or ip_info.get("netmask"))
                        if cidr:
                            self.nb_sync.upsert_ip_address({
                                "address": cidr,
                                "assigned_object_type": "dcim.interface",
                                "assigned_object_id": iface_id,
                                "status": "active",
                            })

        # ----------------------------------------------------------------
        # 3. Management (BMC / ILOM) interface + IP
        #
        # Prefer the first onboard LOM port identified above.  If none was
        # found (e.g. no onboard PCI device data) search existing interfaces
        # for one whose name contains "LOM Port 1" (e.g. "Intel X722 LOM Port 1").
        # Do NOT create a synthetic interface named "LOM Port 1".
        # ----------------------------------------------------------------
        if mgmt_iface_id is None:
            # No onboard interface captured; look for an existing interface
            # whose name contains "LOM PORT 1" (case-insensitive substring match,
            # e.g. "Intel X722 LOM Port 1").
            try:
                ifaces = self.nb_sync.nb.list("dcim.interfaces", device=device_id)
                for iface in ifaces:
                    if isinstance(iface, dict):
                        iface_name = iface.get("name") or ""
                        iface_id = iface.get("id")
                    else:
                        iface_name = getattr(iface, "name", "") or ""
                        iface_id = getattr(iface, "id", None)
                    if "LOM PORT 1" in iface_name.upper():
                        mgmt_iface_id = iface_id
                        logger.debug(
                            "Found management interface %r (id=%s) for device %s",
                            iface_name, mgmt_iface_id, device_id,
                        )
                        break
            except Exception as exc:
                logger.warning(
                    "Could not search interfaces for device %s: %s", device_id, exc
                )

        oob_ip_id: Optional[int] = None

        if mgmt_iface_id:
            synced_mgmt_ips: set[str] = set()

            # Prefer ipInterfaces (includes subnet info for proper CIDR)
            for iface_info in (node.get("ipInterfaces") or []):
                for assignment in (iface_info.get("IPv4assignments") or []):
                    addr = assignment.get("address") or ""
                    if not addr:
                        continue
                    cidr = _to_cidr(addr, assignment.get("subnet"))
                    if cidr:
                        ip_obj = self.nb_sync.upsert_ip_address({
                            "address": cidr,
                            "assigned_object_type": "dcim.interface",
                            "assigned_object_id": mgmt_iface_id,
                            "status": "active",
                        })
                        synced_mgmt_ips.add(addr)
                        if oob_ip_id is None:
                            oob_ip_id = self.nb_sync._id(ip_obj)

            # Fall back to top-level mgmt IP fields if not already synced
            mgmt_ip = (
                node.get("mgmtProcIPaddress")
                or node.get("ipAddress")
                or node.get("primaryMgmtIPaddress")
            )
            if mgmt_ip and mgmt_ip not in synced_mgmt_ips:
                cidr = _to_cidr(mgmt_ip)
                if cidr:
                    ip_obj = self.nb_sync.upsert_ip_address({
                        "address": cidr,
                        "assigned_object_type": "dcim.interface",
                        "assigned_object_id": mgmt_iface_id,
                        "status": "active",
                    })
                    if oob_ip_id is None:
                        oob_ip_id = self.nb_sync._id(ip_obj)

        # Set the management IP as the OOB IP on the device.
        # The first mgmt IP synced is used; additional IPs on the same
        # interface represent aliases and are not promoted to OOB.
        if oob_ip_id is not None:
            self.nb_sync._update("dcim.devices", device_id, {"oob_ip": oob_ip_id})

    def _sync_node_inventory(self, node: dict, device_id: int) -> None:
        """Sync CPUs, DIMMs, disk drives, add-in cards, PSUs, fans and
        backplanes as inventory items (used when COLLECTOR_USE_MODULES=false)."""
        default_mfr_id = self.nb_sync.ensure_manufacturer()

        def _item_mfr_id(item: dict) -> Optional[int]:
            mfr = item.get("manufacturer") or item.get("mfrName") or None
            return self.nb_sync.ensure_manufacturer(mfr) if mfr else default_mfr_id

        # Pre-fetch role IDs once
        role_cpu = self.nb_sync.ensure_inventory_item_role("CPU")
        role_memory = self.nb_sync.ensure_inventory_item_role("Memory")
        role_storage = self.nb_sync.ensure_inventory_item_role("Storage")
        role_adapter = self.nb_sync.ensure_inventory_item_role("Adapter")
        role_psu = self.nb_sync.ensure_inventory_item_role("Power Supply")
        role_fan = self.nb_sync.ensure_inventory_item_role("Fan")

        # ------------------------------------------------------------------
        # CPUs
        # ------------------------------------------------------------------
        for cpu in node.get("processors") or node.get("processorSlots") or []:
            # socket ("CPU 1") is the best slot name; fall back to slot/description
            name = (
                cpu.get("socket")
                or cpu.get("productName")
                or cpu.get("description")
                or f"CPU {cpu.get('slot', '?')}"
            )
            # maxSpeedMHZ is the turbo/max speed; base speed is in GHz
            speed_mhz = cpu.get("maxSpeedMHZ")
            speed_desc = f"{speed_mhz} MHz" if speed_mhz else (
                f"{cpu['speed']} GHz" if cpu.get("speed") else ""
            )
            desc_parts = [
                cpu.get("productVersion") or cpu.get("model") or "",
                speed_desc,
                f"{cpu['cores']} cores" if cpu.get("cores") else "",
            ]
            payload: dict[str, Any] = {
                "device": device_id,
                "name": name,
                "manufacturer": _item_mfr_id(cpu),
                # displayName carries the full model string (e.g. "Intel(R) Xeon(R) Gold 6240")
                "part_id": cpu.get("displayName") or cpu.get("partNumber") or "",
                "serial": cpu.get("serialNumber") or "",
                "description": ", ".join(p for p in desc_parts if p),
            }
            if role_cpu:
                payload["role"] = role_cpu
            self.nb_sync.upsert_inventory_item(payload)

        # ------------------------------------------------------------------
        # Memory / DIMMs
        # ------------------------------------------------------------------
        for dimm in node.get("memoryModules") or node.get("dimmSlots") or []:
            # displayName gives the slot label ("DIMM 1"); fall back to slot number
            name = (
                dimm.get("displayName")
                or dimm.get("productName")
                or dimm.get("description")
                or f"DIMM {dimm.get('slot', '?')}"
            )
            # capacity is in GB in XClarity
            capacity = dimm.get("capacity") or dimm.get("size") or ""
            desc_parts = [
                f"{capacity} GB" if capacity else "",
                f"{dimm['speed']} MHz" if dimm.get("speed") else "",
                dimm.get("memoryType") or dimm.get("type") or dimm.get("model") or "",
            ]
            payload = {
                "device": device_id,
                "name": name,
                "manufacturer": _item_mfr_id(dimm),
                "part_id": dimm.get("partNumber") or "",
                "serial": dimm.get("serialNumber") or "",
                "description": ", ".join(p for p in desc_parts if p),
            }
            if role_memory:
                payload["role"] = role_memory
            self.nb_sync.upsert_inventory_item(payload)

        # ------------------------------------------------------------------
        # Disk drives
        # Drives appear in raidSettings[*].diskDrives (capacity in bytes),
        # and may also be at the top level under several key names.
        # Deduplicate by serial or UUID to avoid double-counting.
        # ------------------------------------------------------------------
        seen_drives: set[str] = set()
        all_drives: list[dict] = []

        def _collect_drive(d: dict) -> None:
            key = d.get("serialNumber") or d.get("uuid") or d.get("name") or str(id(d))
            if key not in seen_drives:
                seen_drives.add(key)
                all_drives.append(d)

        for d in (
            node.get("diskDrives")
            or node.get("drives")
            or node.get("storageDisks")
            or node.get("diskDriveList")
            or []
        ):
            _collect_drive(d)

        # raidSettings hold the primary drive inventory (with capacity in bytes)
        for ctrl in (node.get("raidSettings") or []):
            for d in (ctrl.get("diskDrives") or []):
                _collect_drive(d)

        for drive in all_drives:
            name = (
                drive.get("name")
                or drive.get("productName")
                or drive.get("description")
                or f"Drive {drive.get('bay', drive.get('slot', '?'))}"
            )
            # Capacity may be raw bytes (>threshold) or already in GB
            capacity_raw = drive.get("capacity") or 0
            if capacity_raw > _CAPACITY_BYTES_THRESHOLD:
                capacity_gb = round(capacity_raw / 1_000_000_000)
            else:
                capacity_gb = int(capacity_raw) if capacity_raw else 0
            desc_parts = [
                f"{capacity_gb} GB" if capacity_gb else "",
                drive.get("mediaType") or drive.get("type") or drive.get("interfaceType") or "",
                f"{drive['rpm']} RPM" if drive.get("rpm") else "",
                drive.get("model") or "",
            ]
            payload = {
                "device": device_id,
                "name": name,
                "manufacturer": _item_mfr_id(drive),
                "part_id": drive.get("partNumber") or "",
                "serial": drive.get("serialNumber") or "",
                "description": ", ".join(p for p in desc_parts if p),
            }
            if role_storage:
                payload["role"] = role_storage
            self.nb_sync.upsert_inventory_item(payload)

        # ------------------------------------------------------------------
        # Add-in cards (PCIe)
        # ------------------------------------------------------------------
        for card in (
            node.get("addinCards")
            or node.get("pciExpressCards")
            or node.get("pciCards")
            or node.get("addinCardList")
            or []
        ):
            name = (
                card.get("productName")
                or card.get("description")
                or card.get("name")
                or f"Addin Card {card.get('slotNumber', card.get('slot', '?'))}"
            )
            desc_parts = [
                f"PCI bus {card['pciBusNumber']}" if card.get("pciBusNumber") else "",
                f"Slot {card.get('slotNumber', card.get('slot', ''))}" if card.get("slotNumber") or card.get("slot") else "",
                card.get("slotName") or "",
                card.get("class") or card.get("type") or "",
            ]
            payload = {
                "device": device_id,
                "name": name,
                "manufacturer": _item_mfr_id(card),
                "part_id": card.get("partNumber") or "",
                # fruSerialNumber is the FRU-level serial on XClarity add-in cards
                "serial": card.get("serialNumber") or card.get("fruSerialNumber") or "",
                "description": ", ".join(p for p in desc_parts if p),
            }
            if role_adapter:
                payload["role"] = role_adapter
            self.nb_sync.upsert_inventory_item(payload)

        # ------------------------------------------------------------------
        # Power supplies
        # ------------------------------------------------------------------
        for psu in node.get("powerSupplies") or node.get("powerSupplySlots") or []:
            name = psu.get("name") or psu.get("productName") or psu.get("description") or f"PSU {psu.get('slot', '?')}"
            # Output watts lives under powerAllocation in real XClarity responses
            output_watts = (
                psu.get("outputWatts")
                or (psu.get("powerAllocation") or {}).get("totalOutputPower")
            )
            # Input voltage type: prefer flag then string field
            if psu.get("inputVoltageIsAC") is True:
                input_voltage = "AC"
            elif psu.get("inputVoltageIsAC") is False:
                input_voltage = "DC"
            else:
                input_voltage = psu.get("inputVoltageType") or ""
            desc_parts = [
                psu.get("model") or "",
                input_voltage,
                f"{output_watts} W" if output_watts else "",
            ]
            payload = {
                "device": device_id,
                "name": name,
                "manufacturer": _item_mfr_id(psu),
                "part_id": psu.get("partNumber") or "",
                "serial": psu.get("serialNumber") or "",
                "description": ", ".join(p for p in desc_parts if p),
            }
            if role_psu:
                payload["role"] = role_psu
            self.nb_sync.upsert_inventory_item(payload)

        # ------------------------------------------------------------------
        # Fans
        # ------------------------------------------------------------------
        for fan in node.get("fans") or node.get("fanSlots") or []:
            name = fan.get("name") or fan.get("description") or f"Fan {fan.get('slot', '?')}"
            desc_parts = [f"{fan['speed']} RPM" if fan.get("speed") else ""]
            payload = {
                "device": device_id,
                "name": name,
                "manufacturer": _item_mfr_id(fan),
                "part_id": fan.get("partNumber") or "",
                "serial": fan.get("serialNumber") or "",
                "description": ", ".join(p for p in desc_parts if p),
            }
            if role_fan:
                payload["role"] = role_fan
            self.nb_sync.upsert_inventory_item(payload)

        # ------------------------------------------------------------------
        # Backplanes (faceplateIDs)
        # ------------------------------------------------------------------
        role_backplane = self.nb_sync.ensure_inventory_item_role("Backplane")
        for bp in (node.get("faceplateIDs") or []):
            name = bp.get("name") or f"Backplane {bp.get('deviceId', '?')}"
            payload = {
                "device": device_id,
                "name": name,
                "manufacturer": default_mfr_id,
                "part_id": bp.get("partNumber") or bp.get("fruNumber") or "",
                "serial": bp.get("serialNumber") or "",
                "description": "",
            }
            if role_backplane:
                payload["role"] = role_backplane
            self.nb_sync.upsert_inventory_item(payload)

    def _sync_node_modules(self, node: dict, device_id: int) -> dict[str, int]:
        """Sync hardware components as NetBox Modules (used when COLLECTOR_USE_MODULES=true).

        The NetBox module model closely mirrors physical reality:

        * A **ModuleBayTemplate** on the *DeviceType* declares that a slot
          exists (e.g. "CPU Socket 1").  NetBox auto-creates matching
          ``ModuleBay`` instances when a device is first created from that
          type.  We also create them here explicitly so that devices that
          pre-date the templates still get populated.

        * A **ModuleType** captures the make & model of an installed component
          (e.g. "Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz").  Module types
          are reusable across devices.  A **ModuleTypeProfile** (NetBox 4.0+)
          groups related module types and enables category-specific custom
          fields.

        * A **Module** is the installed instance – it lives in a specific
          ``ModuleBay`` on a specific ``Device`` and carries the serial number.

        Because ``ModuleBayTemplate`` additions happen *before* device
        creation they are safe to call even when the device already exists;
        the template will simply be skipped on the device type and the bay
        ensured directly on the device.

        Returns a dict mapping bay/slot name → installed NetBox module ID.
        This is consumed by ``_sync_node_interfaces`` to attach interfaces to
        the correct module.
        """
        # Retrieve the device_type_id from the device record so we can add
        # bay templates to it.
        device_type_id = self._get_device_type_id(device_id)

        # Maps bay_name → module_id; returned to the caller so interfaces can
        # be linked to the correct module.
        slot_module_map: dict[str, int] = {}

        def _ensure_slot(bay_name: str, position: str = "") -> Optional[int]:
            """Ensure a bay template + bay instance exist; return the bay ID."""
            if device_type_id is not None:
                self.nb_sync.ensure_module_bay_template(device_type_id, bay_name, position)
            return self.nb_sync.ensure_module_bay(device_id, bay_name, position)

        def _install_module(
            bay_id: Optional[int],
            bay_name: str,
            model: str,
            serial: str,
            mfr_name: Optional[str],
            profile_name: Optional[str] = None,
            attributes: Optional[dict] = None,
        ) -> Optional[int]:
            """Install a module in *bay_id*; return its NetBox ID (or None)."""
            if bay_id is None or not model:
                return None
            module_type_id = self.nb_sync.ensure_module_type(model, mfr_name, profile_name, attributes)
            if module_type_id is None:
                return None
            payload: dict[str, Any] = {
                "device": device_id,
                "module_bay": bay_id,
                "module_type": module_type_id,
                "status": "active",
            }
            if serial:
                payload["serial"] = serial
            # Note: attribute_data is intentionally NOT included here.
            # Profile-specific attributes belong on the module *type* record
            # (set in ensure_module_type step 2) and carry forward automatically
            # to the installed module via the type relationship.
            module = self.nb_sync.upsert_module(payload)
            module_id = self.nb_sync._id(module)
            if module_id is not None and bay_name:
                slot_module_map[bay_name] = module_id
            return module_id

        # ------------------------------------------------------------------
        # CPUs
        # ------------------------------------------------------------------
        for cpu in node.get("processors") or node.get("processorSlots") or []:
            bay_name = (
                cpu.get("socket")
                or cpu.get("productName")
                or f"CPU {cpu.get('slot', '?')}"
            )
            position = str(cpu.get("slot", ""))
            bay_id = _ensure_slot(bay_name, position)
            model = (
                cpu.get("displayName")
                or cpu.get("productVersion")
                or cpu.get("model")
                or ""
            )
            _install_module(bay_id, bay_name, model, cpu.get("serialNumber") or "", cpu.get("manufacturer"), "CPU", _cpu_attributes(cpu))

        # ------------------------------------------------------------------
        # Memory / DIMMs
        # ------------------------------------------------------------------
        for dimm in node.get("memoryModules") or node.get("dimmSlots") or []:
            bay_name = (
                dimm.get("displayName")
                or dimm.get("productName")
                or f"DIMM {dimm.get('slot', '?')}"
            )
            position = str(dimm.get("slot", ""))
            bay_id = _ensure_slot(bay_name, position)
            # Model name: part number + capacity + type gives a useful identifier
            capacity = dimm.get("capacity") or ""
            mem_type = dimm.get("type") or dimm.get("memoryType") or dimm.get("model") or ""
            part = dimm.get("partNumber") or ""
            model = part or (f"{capacity}GB {mem_type}".strip() if capacity or mem_type else "")
            _install_module(bay_id, bay_name, model, dimm.get("serialNumber") or "", dimm.get("manufacturer"), "Memory", _memory_attributes(dimm))

        # ------------------------------------------------------------------
        # Disk drives (from raidSettings and top-level)
        # ------------------------------------------------------------------
        seen_drives: set[str] = set()

        def _sync_drive_module(drive: dict) -> None:
            key = drive.get("serialNumber") or drive.get("uuid") or drive.get("name") or str(id(drive))
            if key in seen_drives:
                return
            seen_drives.add(key)
            bay_name = (
                drive.get("name")
                or drive.get("description")
                or f"Drive Bay {drive.get('bay', drive.get('slot', '?'))}"
            )
            position = str(drive.get("bay", drive.get("slot", "")))
            bay_id = _ensure_slot(bay_name, position)
            capacity_raw = drive.get("capacity") or 0
            capacity_gb = round(capacity_raw / 1_000_000_000) if capacity_raw > _CAPACITY_BYTES_THRESHOLD else int(capacity_raw) if capacity_raw else 0
            media = drive.get("mediaType") or drive.get("interfaceType") or ""
            part = drive.get("partNumber") or ""
            model_str = drive.get("model") or part or (f"{capacity_gb}GB {media}".strip() if capacity_gb or media else "")
            _install_module(bay_id, bay_name, model_str, drive.get("serialNumber") or "", drive.get("manufacturer"), "Hard disk", _storage_attributes(drive, capacity_gb))

        for d in (node.get("diskDrives") or node.get("drives") or node.get("storageDisks") or node.get("diskDriveList") or []):
            _sync_drive_module(d)
        for ctrl in (node.get("raidSettings") or []):
            for d in (ctrl.get("diskDrives") or []):
                _sync_drive_module(d)

        # ------------------------------------------------------------------
        # Add-in cards (PCIe)
        # ------------------------------------------------------------------
        for card in (
            node.get("addinCards")
            or node.get("pciExpressCards")
            or node.get("pciCards")
            or node.get("addinCardList")
            or []
        ):
            slot_name = card.get("slotName") or f"PCIe Slot {card.get('slotNumber', card.get('slot', '?'))}"
            position = str(card.get("slotNumber") or card.get("slot") or "")
            bay_id = _ensure_slot(slot_name, position)
            model = card.get("productName") or card.get("name") or card.get("description") or ""
            serial = card.get("serialNumber") or card.get("fruSerialNumber") or ""
            _install_module(bay_id, slot_name, model, serial, card.get("manufacturer"), "Expansion card", _expansion_card_attributes(card))

        # ------------------------------------------------------------------
        # Power supplies
        # ------------------------------------------------------------------
        for i, psu in enumerate(node.get("powerSupplies") or node.get("powerSupplySlots") or [], start=1):
            # slots is a list; take the first element as position
            slots = psu.get("slots") or []
            position = str(slots[0]) if slots else str(psu.get("slot", ""))
            bay_name = psu.get("name") or psu.get("description") or f"Power Supply {position}"
            bay_id = _ensure_slot(bay_name, position)
            model = psu.get("partNumber") or psu.get("model") or psu.get("productName") or ""
            module_id = _install_module(bay_id, bay_name, model, psu.get("serialNumber") or "", psu.get("manufacturer"), "Power supply", _psu_attributes(psu))
            # Add a power inlet port to the PSU module so the power draw
            # can be tracked and cables can be attached.
            # Use C20 for high-wattage PSUs (>1800 W), C14 for lower-wattage ones.
            if module_id is not None:
                self.nb_sync.upsert_power_port({
                    "device": device_id,
                    "module": module_id,
                    "name": f"Power Input {position if position != '' else i}",
                    "type": _psu_plug_type(psu),
                })

        # ------------------------------------------------------------------
        # Fans
        # ------------------------------------------------------------------
        for fan in node.get("fans") or node.get("fanSlots") or []:
            position = str(fan.get("slot") or fan.get("slots") or "")
            bay_name = fan.get("name") or fan.get("description") or f"Fan {position}"
            bay_id = _ensure_slot(bay_name, position)
            # Fans rarely have a meaningful model; skip module install if no part number
            model = fan.get("partNumber") or fan.get("model") or ""
            _install_module(bay_id, bay_name, model, fan.get("serialNumber") or "", fan.get("manufacturer"), "Fan", _fan_attributes(fan))

        # ------------------------------------------------------------------
        # Backplanes (faceplateIDs)
        # ------------------------------------------------------------------
        for bp in (node.get("faceplateIDs") or []):
            bay_name = bp.get("name") or f"Backplane {bp.get('deviceId', '?')}"
            bay_id = _ensure_slot(bay_name)
            model = bp.get("partNumber") or bp.get("fruNumber") or ""
            _install_module(bay_id, bay_name, model, bp.get("serialNumber") or "", None)

        return slot_module_map

    def _get_device_type_id(self, device_id: int) -> Optional[int]:
        """Retrieve the device_type ID for an existing device."""
        try:
            device = self.nb_sync.nb.get("dcim.devices", id=device_id)
            if device is None:
                return None
            if isinstance(device, dict):
                dt = device.get("device_type")
                if isinstance(dt, dict):
                    return dt.get("id")
                return dt
            dt = getattr(device, "device_type", None)
            if dt is None:
                return None
            if isinstance(dt, dict):
                return dt.get("id")
            return getattr(dt, "id", None)
        except Exception as exc:
            logger.warning("Could not retrieve device_type for device %s: %s", device_id, exc)
            return None

    # ------------------------------------------------------------------
    # Chassis
    # ------------------------------------------------------------------

    def _collect_chassis(self) -> None:
        logger.info("Collecting chassis …")
        try:
            chassis_list = self.xc.get_chassis()
        except Exception as exc:
            logger.error("Failed to retrieve chassis from XClarity: %s", exc)
            return

        logger.info("Found %d chassis.", len(chassis_list))
        self._run_parallel(chassis_list, self._sync_chassis, "chassis")

    def _sync_chassis(self, chassis: dict) -> None:
        raw_name = chassis.get("name") or chassis.get("hostname") or chassis.get("uuid", "unknown")
        name = self._apply_name_regex(str(raw_name))
        logger.debug("Syncing chassis: %s", name)

        model = _build_model_name(chassis)
        part_number = chassis.get("partNumber") or ""
        serial = chassis.get("serialNumber") or ""
        mfr_name = chassis.get("manufacturer") or None

        device_type_id = self.nb_sync.ensure_device_type(model, part_number, mfr_name)
        role_slug = _env("NETBOX_CHASSIS_ROLE", "chassis")
        role_id = self.nb_sync.ensure_device_role(
            name=role_slug.title(),
            slug=role_slug,
            color="9c27b0",
        )
        site_id, location_id, rack_id, rack_position = self._resolve_placement(chassis)

        if device_type_id is None or role_id is None or site_id is None:
            logger.warning("Skipping chassis %s: missing device_type/role/site", name)
            return

        payload: dict[str, Any] = {
            "name": name,
            "device_type": device_type_id,
            "role": role_id,
            "site": site_id,
            "status": "active",
        }
        if serial:
            payload["serial"] = serial
        if location_id is not None:
            payload["location"] = location_id
        if rack_id is not None:
            payload["rack"] = rack_id
            payload["face"] = "front"
        if rack_position is not None and rack_id is not None:
            payload["position"] = rack_position

        self.nb_sync.upsert_device(payload)

    # ------------------------------------------------------------------
    # Switches
    # ------------------------------------------------------------------

    def _collect_switches(self) -> None:
        logger.info("Collecting switches …")
        try:
            switches = self.xc.get_switches()
        except Exception as exc:
            logger.error("Failed to retrieve switches from XClarity: %s", exc)
            return

        logger.info("Found %d switch(es).", len(switches))
        self._run_parallel(switches, self._sync_switch, "switch")

    def _sync_switch(self, switch: dict) -> None:
        raw_name = switch.get("name") or switch.get("hostname") or switch.get("uuid", "unknown")
        name = self._apply_name_regex(str(raw_name))
        logger.debug("Syncing switch: %s", name)

        model = _build_model_name(switch)
        part_number = switch.get("partNumber") or ""
        serial = switch.get("serialNumber") or ""
        mfr_name = switch.get("manufacturer") or None

        device_type_id = self.nb_sync.ensure_device_type(model, part_number, mfr_name)
        role_slug = _env("NETBOX_SWITCH_ROLE", "switch")
        role_id = self.nb_sync.ensure_device_role(
            name=role_slug.title(),
            slug=role_slug,
            color="4caf50",
        )
        site_id, location_id, rack_id, rack_position = self._resolve_placement(switch)


        if device_type_id is None or role_id is None or site_id is None:
            logger.warning("Skipping switch %s: missing device_type/role/site", name)
            return

        payload: dict[str, Any] = {
            "name": name,
            "device_type": device_type_id,
            "role": role_id,
            "site": site_id,
            "status": "active",
        }
        if serial:
            payload["serial"] = serial
        if location_id is not None:
            payload["location"] = location_id
        if rack_id is not None:
            payload["rack"] = rack_id
            payload["face"] = "front"
        if rack_position is not None and rack_id is not None:
            payload["position"] = rack_position

        device = self.nb_sync.upsert_device(payload)

        if device is not None and self._sync_interfaces:
            device_id = self.nb_sync._id(device)
            if device_id:
                self._sync_switch_interfaces(switch, device_id)

    def _sync_switch_interfaces(self, switch: dict, device_id: int) -> None:
        """Sync switch ports reported by XClarity."""
        ports = switch.get("portList") or switch.get("ports") or []
        for port in ports:
            port_index = port.get("portIndex", "?")
            port_name = port.get("portName") or port.get("name") or f"port-{port_index}"
            mac = _normalise_mac(port.get("macAddress") or port.get("mac") or "")
            iface_payload: dict[str, Any] = {
                "device": device_id,
                "name": port_name,
                "type": _port_type(port),
            }
            if mac:
                iface_payload["mac_address"] = mac
            self.nb_sync.upsert_interface(iface_payload)

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def _collect_storage(self) -> None:
        logger.info("Collecting storage devices …")
        try:
            storage_list = self.xc.get_storage()
        except Exception as exc:
            logger.error("Failed to retrieve storage from XClarity: %s", exc)
            return

        logger.info("Found %d storage device(s).", len(storage_list))
        self._run_parallel(storage_list, self._sync_storage, "storage")

    def _sync_storage(self, storage: dict) -> None:
        raw_name = storage.get("name") or storage.get("hostname") or storage.get("uuid", "unknown")
        name = self._apply_name_regex(str(raw_name))
        logger.debug("Syncing storage: %s", name)

        model = _build_model_name(storage)
        part_number = storage.get("partNumber") or ""
        serial = storage.get("serialNumber") or ""
        mfr_name = storage.get("manufacturer") or None

        device_type_id = self.nb_sync.ensure_device_type(model, part_number, mfr_name)
        role_slug = _env("NETBOX_STORAGE_ROLE", "storage")
        role_id = self.nb_sync.ensure_device_role(
            name=role_slug.title(),
            slug=role_slug,
            color="ff9800",
        )
        site_id, location_id, rack_id, rack_position = self._resolve_placement(storage)

        if device_type_id is None or role_id is None or site_id is None:
            logger.warning("Skipping storage %s: missing device_type/role/site", name)
            return

        payload: dict[str, Any] = {
            "name": name,
            "device_type": device_type_id,
            "role": role_id,
            "site": site_id,
            "status": "active",
        }
        if serial:
            payload["serial"] = serial
        if location_id is not None:
            payload["location"] = location_id
        if rack_id is not None:
            payload["rack"] = rack_id
            payload["face"] = "front"
        if rack_position is not None and rack_id is not None:
            payload["position"] = rack_position

        self.nb_sync.upsert_device(payload)

    # ------------------------------------------------------------------
    # Name regex transformation
    # ------------------------------------------------------------------

    def _apply_name_regex(self, name: str) -> str:
        """Apply hostname regex transformation defined in environment variables."""
        return _apply_regex(
            name,
            _env("COLLECTOR_HOSTNAME_REGEX", ""),
            _env("COLLECTOR_HOSTNAME_REPLACE", ""),
        )

    # ------------------------------------------------------------------
    # Placement resolution (site, location, rack, rack position)
    # ------------------------------------------------------------------

    def _resolve_placement(
        self, device: dict
    ) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        """Return ``(site_id, location_id, rack_id, rack_position)`` from device metadata.

        XClarity location mapping:

        * ``location.location``      → NetBox **site**
        * ``location.room``          → NetBox **location** (area within site)
        * ``location.rack``          → NetBox **rack**
        * ``location.lowestRackUnit`` → rack **position** (front face)
        """
        loc = device.get("location") or {}
        if not isinstance(loc, dict):
            loc = {}

        # --- Site ---
        site_raw = (
            loc.get("location")
            or device.get("dataCenter")
            or loc.get("dataCenter")
            or ""
        )
        if not site_raw:
            site_raw = _env("NETBOX_DEFAULT_SITE", "default")
        site_name = _apply_regex(
            str(site_raw),
            _env("COLLECTOR_LOCATION_REGEX", ""),
            _env("COLLECTOR_LOCATION_REPLACE", ""),
        )
        site_id = self.nb_sync.ensure_site(site_name, _slugify(site_name))

        # --- Location (room within the site) ---
        room_raw = loc.get("room") or device.get("room") or ""
        location_id: Optional[int] = None
        if room_raw and site_id is not None:
            room_name = _apply_regex(
                str(room_raw),
                _env("COLLECTOR_ROOM_REGEX", ""),
                _env("COLLECTOR_ROOM_REPLACE", ""),
            )
            location_id = self.nb_sync.ensure_location(room_name, site_id)

        # --- Rack ---
        rack_raw = loc.get("rack") or device.get("rack") or ""
        rack_id: Optional[int] = None
        if rack_raw and site_id is not None:
            rack_id = self.nb_sync.ensure_rack(str(rack_raw), site_id, location_id)

        # --- Rack position ---
        rack_position: Optional[int] = None
        raw_pos = loc.get("lowestRackUnit")
        if raw_pos is not None:
            try:
                rack_position = int(raw_pos)
            except (ValueError, TypeError):
                pass

        return site_id, location_id, rack_id, rack_position


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _cpu_attributes(cpu: dict) -> dict:
    """Extract NetBox module profile attributes for a CPU component.

    Returns a dict with only the fields that can be determined from *cpu*.
    The caller is responsible for omitting empty dicts from the payload.
    """
    attrs: dict = {}
    try:
        cores = cpu.get("cores")
        if cores is not None:
            attrs["cores"] = int(cores)
    except (ValueError, TypeError):
        pass
    try:
        speed = cpu.get("speed")
        if speed is not None:
            attrs["speed"] = float(speed)
    except (ValueError, TypeError):
        pass
    arch = cpu.get("architecture") or cpu.get("cpuFamily") or cpu.get("family")
    if arch:
        attrs["architecture"] = str(arch)
    return attrs


def _memory_attributes(dimm: dict) -> dict:
    """Extract NetBox module profile attributes for a Memory (DIMM) component."""
    attrs: dict = {}
    try:
        capacity = dimm.get("capacity") or dimm.get("size")
        if capacity is not None:
            attrs["size"] = int(capacity)
    except (ValueError, TypeError):
        pass
    mem_type_raw = (dimm.get("memoryType") or dimm.get("type") or "").upper()
    for prefix in ("DDR5", "DDR4", "DDR3"):
        if prefix in mem_type_raw:
            attrs["class"] = prefix
            break
    try:
        speed = dimm.get("speed")
        if speed is not None:
            attrs["data_rate"] = int(speed)
    except (ValueError, TypeError):
        pass
    ecc = dimm.get("eccEnabled") if dimm.get("eccEnabled") is not None else dimm.get("ecc")
    if ecc is not None:
        attrs["ecc"] = bool(ecc)
    return attrs


def _storage_attributes(drive: dict, capacity_gb: int) -> dict:
    """Extract NetBox module profile attributes for a Storage (disk) component."""
    attrs: dict = {}
    if capacity_gb:
        attrs["size"] = int(capacity_gb)
    try:
        rpm = drive.get("rpm")
        if rpm is not None:
            attrs["speed"] = int(rpm)
    except (ValueError, TypeError):
        pass
    media_raw = (drive.get("mediaType") or drive.get("type") or drive.get("interfaceType") or "").upper()
    if "NVME" in media_raw or "NVM" in media_raw:
        attrs["type"] = "NVME"
    elif "SSD" in media_raw or "SOLID" in media_raw or "FLASH" in media_raw:
        attrs["type"] = "SSD"
    elif media_raw:
        attrs["type"] = "HD"
    return attrs


def _expansion_card_attributes(card: dict) -> dict:
    """Extract NetBox module profile attributes for an Expansion Card component."""
    attrs: dict = {}
    try:
        bandwidth = card.get("bandwidth")
        if bandwidth is not None:
            attrs["bandwidth"] = int(bandwidth)
    except (ValueError, TypeError):
        pass
    connector = (
        card.get("pciExpressConnectorType")
        or card.get("connectorType")
        or card.get("slotType")
        or ""
    )
    if connector:
        attrs["connector_type"] = str(connector)
    return attrs


def _fan_attributes(fan: dict) -> dict:
    """Extract NetBox module profile attributes for a Fan component."""
    attrs: dict = {}
    try:
        rpm = fan.get("speed") or fan.get("rpm")
        if rpm is not None:
            attrs["rpm"] = int(rpm)
    except (ValueError, TypeError):
        pass
    return attrs


def _psu_attributes(psu: dict) -> dict:
    """Extract NetBox module profile attributes for a Power Supply component."""
    attrs: dict = {}
    # input_current: required by profile schema; default to AC
    if psu.get("inputVoltageIsAC") is False:
        attrs["input_current"] = "DC"
    else:
        input_v_type = (psu.get("inputVoltageType") or "").upper()
        attrs["input_current"] = "DC" if input_v_type == "DC" else "AC"
    # input_voltage: required by profile schema; default to 120 V when absent
    try:
        input_v = (
            psu.get("inputVoltage")
            or psu.get("normalInputVoltage")
            or psu.get("nominalVoltage")
        )
        attrs["input_voltage"] = int(input_v) if input_v is not None else 120
    except (ValueError, TypeError):
        attrs["input_voltage"] = 120
    # wattage (available output power)
    try:
        output_watts = psu.get("outputWatts")
        if output_watts is None:
            output_watts = (psu.get("powerAllocation") or {}).get("totalOutputPower")
        if output_watts is not None:
            attrs["wattage"] = int(output_watts)
    except (ValueError, TypeError):
        pass
    # hot_swappable
    hot_swap = psu.get("hotSwappable")
    if hot_swap is None:
        hot_swap = psu.get("isHotSwappable")
    if hot_swap is not None:
        attrs["hot_swappable"] = bool(hot_swap)
    return attrs


def _psu_plug_type(psu: dict) -> str:
    """Return the IEC 60320 inlet plug type for a PSU based on output wattage.

    PSUs above 1800 W draw more current than C13/C14 connectors are rated for,
    so they require the heavier-duty C19/C20 pairing instead.
    """
    try:
        output_watts = psu.get("outputWatts")
        if output_watts is None:
            output_watts = (psu.get("powerAllocation") or {}).get("totalOutputPower")
        if output_watts is not None and int(output_watts) > _PSU_C20_WATTAGE_THRESHOLD:
            return "iec-60320-c20"
    except (ValueError, TypeError):
        pass
    return "iec-60320-c14"


def _slugify(value: str) -> str:
    """Convert *value* to a NetBox-compatible slug (lowercase, hyphens)."""
    value = value.lower().strip()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[\s_]+", "-", value)
    value = re.sub(r"-+", "-", value)
    value = value.strip("-")
    return value[:100]


def _apply_regex(value: str, pattern: str, replacement: str) -> str:
    """Apply ``re.sub(pattern, replacement, value)``.

    Returns *value* unchanged when *pattern* is empty or invalid.
    """
    if not pattern or not value:
        return value
    try:
        return re.sub(pattern, replacement, value)
    except re.error as exc:
        logger.warning("Invalid regex '%s': %s", pattern, exc)
        return value


def _build_model_name(device: dict) -> str:
    """Build a combined device-type model name from XClarity fields.

    Format: ``<productName> -[<machineType><model>]-``

    Example: ``ThinkSystem SR650 -[7X06CTO1WW]-``

    Falls back to ``machineType`` or ``model`` alone when the richer fields
    are absent, so behaviour is backwards-compatible.
    """
    manufacturer = device.get("manufacturer", "")
    product_name = device.get("productName", "")
    machine_type = device.get("machineType", "")
    model_code = device.get("model", "")
    parts: list[str] = []
    if product_name:
        parts.append(product_name)
    suffix = f"{machine_type}{model_code}".strip()
    if suffix:
        parts.append(f"-[{suffix}]-")
    return " ".join(parts) if parts else (machine_type or model_code or "")


def _port_type(port: dict) -> str:
    """Infer a NetBox interface type string from XClarity port speed/type data."""
    speed = str(port.get("speed") or port.get("portSpeed") or "").lower()
    # Extract numeric tokens so substring matches do not cause false positives
    # (e.g. "10000" must not be confused with "100000").
    nums = set(re.findall(r"\d+", speed))
    if "100g" in speed or "100000" in nums:
        return "100gbase-x-qsfp28"
    if "40g" in speed or "40000" in nums:
        return "40gbase-x-qsfpp"
    if "25g" in speed or "25000" in nums:
        return "25gbase-x-sfp28"
    if "10g" in speed or "10000" in nums:
        return "10gbase-t"
    return "1000base-t"


def _port_type_gbps(speed_gbps: Any) -> str:
    """Infer a NetBox interface type from a raw Gbps integer (as reported by
    XClarity ``portInfo.physicalPorts[*].speed``)."""
    try:
        gbps = int(speed_gbps)
    except (TypeError, ValueError):
        return "1000base-t"
    if gbps >= 100:
        return "100gbase-x-qsfp28"
    if gbps >= 40:
        return "40gbase-x-qsfpp"
    if gbps >= 25:
        return "25gbase-x-sfp28"
    if gbps >= 10:
        return "10gbase-t"
    return "1000base-t"


def _normalise_mac(mac: str) -> str:
    """Normalise a MAC address to ``AA:BB:CC:DD:EE:FF`` format."""
    if not mac:
        return ""
    digits = mac.replace(":", "").replace("-", "").replace(".", "").upper()
    if len(digits) != 12:
        return ""
    return ":".join(digits[i:i+2] for i in range(0, 12, 2))



def _to_cidr(ip: str, netmask: Optional[str] = None) -> Optional[str]:
    """Return *ip* in CIDR notation.  Falls back to /32 if *netmask* is absent."""
    if not ip:
        return None
    if "/" in ip:
        return ip
    if netmask:
        try:
            import ipaddress
            network = ipaddress.IPv4Network(f"{ip}/{netmask}", strict=False)
            return f"{ip}/{network.prefixlen}"
        except Exception:
            pass
    return f"{ip}/32"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Lenovo XClarity Administrator devices into NetBox.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--env-file",
        metavar="FILE",
        help="Path to a .env file to load (see .env.example). "
             "Defaults to .env in the current directory if present.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without modifying NetBox.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)

    # Load .env first so that the log-level variable is available
    load_env_file(args.env_file)

    log_level = (
        logging.DEBUG
        if args.verbose
        else getattr(logging, _env("COLLECTOR_LOG_LEVEL", "INFO").upper(), logging.INFO)
    )
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    _validate_env()

    if args.dry_run:
        logger.info("*** DRY-RUN mode — no changes will be written to NetBox ***")

    # Build XClarity client
    xc = XClarityClient(
        host=_env("XCLARITY_HOST"),
        username=_env("XCLARITY_USERNAME"),
        password=_env("XCLARITY_PASSWORD"),
        port=int(_env("XCLARITY_PORT", "443")),
        verify_ssl=_env("XCLARITY_VERIFY_SSL", "true").lower() not in ("false", "0", "no"),
        timeout=int(_env("XCLARITY_TIMEOUT", "30")),
    )

    # Build NetBox client
    nb = pynetbox2.api(
        url=_env("NETBOX_URL"),
        token=_env("NETBOX_TOKEN"),
        rate_limit_per_second=float(_env("NETBOX_RATE_LIMIT", "5")),
        retry_attempts=int(_env("NETBOX_RETRY_ATTEMPTS", "3")),
        cache_backend=_env("NETBOX_CACHE_BACKEND", "sqlite"),
        cache_ttl_seconds=int(_env("NETBOX_CACHE_TTL", "300")),
        sqlite_path=_env("NETBOX_CACHE_SQLITE_PATH", ".nbx_cache.sqlite3"),
        redis_url=_env("NETBOX_CACHE_REDIS_URL", "redis://localhost:6379/0"),
    )

    nb_sync = NetBoxSync(nb, dry_run=args.dry_run)
    collector = Collector(xc, nb_sync)
    collector.run()


if __name__ == "__main__":
    main()