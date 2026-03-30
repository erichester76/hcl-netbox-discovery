"""NetBox-to-NetBox data source adapter.

Connects to a *source* NetBox instance using ``pynetbox`` and returns records
as plain Python dicts so they can be mapped and written into a *destination*
NetBox instance by the engine.

Supported collections
---------------------
Collections are specified as ``<app>.<resource>`` dotted paths that correspond
directly to the pynetbox endpoint hierarchy.  Examples:

    ``dcim.devices``            – all devices
    ``dcim.interfaces``         – all interfaces
    ``dcim.device_types``       – device types
    ``dcim.manufacturers``      – manufacturers
    ``dcim.sites``              – sites
    ``dcim.racks``              – racks
    ``dcim.platforms``          – platforms
    ``dcim.device_roles``       – device roles
    ``ipam.ip_addresses``       – IP addresses
    ``ipam.prefixes``           – IP prefixes
    ``ipam.vlans``              – VLANs
    ``ipam.vlan_groups``        – VLAN groups
    ``ipam.vrfs``               – VRFs
    ``virtualization.virtual_machines``  – virtual machines
    ``virtualization.interfaces``        – VM interfaces
    ``virtualization.clusters``          – clusters

Any dotted path that pynetbox supports can be used.

Source HCL block example::

    source "source_nb" {
      api_type   = "netbox"
      url        = env("SOURCE_NETBOX_URL")
      # Use the API token as the password field
      password   = env("SOURCE_NETBOX_TOKEN")
      verify_ssl = env("SOURCE_NETBOX_VERIFY_SSL", "true")

      # Optional: limit results per page (default: 1000 — set to 0 for no limit)
      page_size  = "1000"
    }

Each returned dict is a flat representation of the NetBox record with nested
objects (foreign keys) serialised as sub-dicts so that HCL field expressions
can navigate them via ``source('site.name')``, ``source('device_type.model')``,
etc.

Created: 2026-03-30
Author: GitHub Copilot
Last Changed: GitHub Copilot Issue: #(netbox-source-type)
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)

# Default page size when fetching records from the source NetBox.
_DEFAULT_PAGE_SIZE = 1000


def _record_to_dict(record: Any) -> dict:
    """Recursively convert a pynetbox Record (or plain dict) to a plain dict.

    Foreign-key fields (nested Record objects) are converted to dicts so that
    HCL expressions can navigate them with dotted paths, e.g.
    ``source('site.name')`` or ``source('device_type.manufacturer.name')``.

    The ``tags`` field is returned as a list of slug strings so that it can be
    used directly in ``tags`` field mappings.
    """
    if record is None:
        return {}

    # pynetbox exposes a dict-like interface via .dict or iteration
    if hasattr(record, "items"):
        raw = dict(record)
    elif hasattr(record, "_full_cache"):
        raw = dict(record)
    else:
        # Fallback: treat as an already-plain dict/object
        try:
            raw = dict(record)
        except (TypeError, ValueError):
            return {}

    result: dict = {}
    for key, val in raw.items():
        if val is None:
            result[key] = None
        elif key == "tags":
            # Normalise tags to a list of slugs
            result[key] = _normalise_tags(val)
        elif key == "custom_fields" and isinstance(val, dict):
            result[key] = val
        elif hasattr(val, "items") or hasattr(val, "_full_cache"):
            # Nested Record / dict-like foreign key — recurse one level
            result[key] = _record_to_dict(val)
        elif isinstance(val, list):
            result[key] = [
                _record_to_dict(item) if (hasattr(item, "items") or hasattr(item, "_full_cache"))
                else item
                for item in val
            ]
        else:
            result[key] = val
    return result


def _normalise_tags(tags: Any) -> list:
    """Return a list of tag slug strings from various pynetbox tag representations."""
    if not tags:
        return []
    result = []
    for tag in tags:
        if isinstance(tag, dict):
            result.append(tag.get("slug") or tag.get("name") or str(tag))
        elif hasattr(tag, "slug"):
            result.append(str(tag.slug))
        elif hasattr(tag, "name"):
            result.append(str(tag.name))
        else:
            result.append(str(tag))
    return result


# ---------------------------------------------------------------------------
# NetBoxSource
# ---------------------------------------------------------------------------


class NetBoxSource(DataSource):
    """pynetbox-backed source adapter for reading records from a NetBox instance."""

    def __init__(self) -> None:
        self._nb: Optional[Any] = None
        self._config: Optional[Any] = None

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to the source NetBox instance using settings from *config*.

        The ``password`` field is treated as the NetBox API token.
        The ``url`` field must be the full base URL of the source NetBox
        instance (e.g. ``https://netbox.example.com``).
        """
        try:
            import pynetbox  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "pynetbox is required for the NetBox source adapter. "
                "Install it with: pip install pynetbox"
            ) from exc

        self._config = config
        url = (config.url or "").strip().rstrip("/")
        token = config.password or ""

        if not url:
            raise ValueError("NetBoxSource: 'url' must be set to the source NetBox base URL")
        if not token:
            raise ValueError(
                "NetBoxSource: 'password' must be set to the source NetBox API token"
            )

        verify_ssl = config.verify_ssl
        logger.info("NetBoxSource: connecting to %s (verify_ssl=%s)", url, verify_ssl)

        nb = pynetbox.api(url, token=token)

        # Disable SSL verification if requested
        if not verify_ssl:
            import urllib3  # type: ignore[import]
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            import requests  # type: ignore[import]
            session = requests.Session()
            session.verify = False
            nb.http_session = session

        self._nb = nb
        logger.info("NetBoxSource: connected to %s", url)

    def get_objects(self, collection: str) -> list:
        """Fetch and return all records for *collection* as plain dicts.

        *collection* must be a dotted path recognised by pynetbox, e.g.
        ``dcim.devices``, ``ipam.ip_addresses``, ``virtualization.virtual_machines``.

        An optional ``filters`` dict can be supplied via ``extra.filters`` in
        the source HCL block as a JSON string:
        ``filters = '{"site": "hq", "status": "active"}'``
        """
        if self._nb is None:
            raise RuntimeError("NetBoxSource: connect() has not been called")

        endpoint = self._resolve_endpoint(collection)
        filters = self._get_filters()
        page_size = self._get_page_size()

        logger.info(
            "NetBoxSource: fetching %s (filters=%s, page_size=%s)",
            collection, filters, page_size,
        )

        try:
            if filters:
                records = list(endpoint.filter(**filters))
            else:
                records = list(endpoint.all())
        except Exception as exc:
            logger.error("NetBoxSource: failed to fetch %s: %s", collection, exc)
            raise

        logger.debug("NetBoxSource: fetched %d records from %s", len(records), collection)
        return [_record_to_dict(r) for r in records]

    def close(self) -> None:
        """No persistent connection to close for the HTTP-based pynetbox client."""
        self._nb = None
        self._config = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_endpoint(self, collection: str) -> Any:
        """Resolve a dotted collection name to a pynetbox endpoint object.

        Raises ``ValueError`` if the path cannot be resolved.
        """
        # Normalise: replace hyphens with underscores, strip whitespace
        path = collection.strip().replace("-", "_")

        parts = path.split(".")
        if len(parts) != 2:
            raise ValueError(
                f"NetBoxSource: collection must be a two-part dotted path like "
                f"'dcim.devices', got {collection!r}"
            )

        app_name, resource_name = parts
        try:
            app = getattr(self._nb, app_name)
        except AttributeError:
            raise ValueError(
                f"NetBoxSource: unknown NetBox app {app_name!r} in collection {collection!r}"
            )
        try:
            endpoint = getattr(app, resource_name)
        except AttributeError:
            raise ValueError(
                f"NetBoxSource: unknown resource {resource_name!r} under app {app_name!r}"
            )
        return endpoint

    def _get_filters(self) -> dict:
        """Return any filters configured in extra.filters (JSON string or dict)."""
        if self._config is None:
            return {}
        extra = self._config.extra or {}
        raw = extra.get("filters", "")
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        import json
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning("NetBoxSource: could not parse extra.filters %r: %s", raw, exc)
            return {}

    def _get_page_size(self) -> int:
        """Return the configured page size (default: _DEFAULT_PAGE_SIZE)."""
        if self._config is None:
            return _DEFAULT_PAGE_SIZE
        extra = self._config.extra or {}
        raw = extra.get("page_size", str(_DEFAULT_PAGE_SIZE))
        try:
            return int(raw)
        except (ValueError, TypeError):
            return _DEFAULT_PAGE_SIZE
