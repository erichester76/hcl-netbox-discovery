"""Generic REST API data source adapter.

Any HTTP/REST-based source can be described entirely in HCL using
``collection {}`` sub-blocks inside the ``source`` block — no Python code
is required per collector.

Supported auth schemes (set via ``auth`` in the source block):

  ``basic``   – HTTP Basic auth (username / password)  [default]
  ``bearer``  – ``Authorization: Bearer <password>`` header
  ``header``  – arbitrary header; set ``auth_header`` to the header name
                and ``password`` to the value

Example HCL source block::

    source "xclarity" {
      api_type   = "rest"
      url        = env("XCLARITY_HOST")
      username   = env("XCLARITY_USER")
      password   = env("XCLARITY_PASS")
      verify_ssl = env("XCLARITY_VERIFY_SSL", "true")
      auth       = "basic"

      collection "nodes" {
        endpoint        = "/nodes"
        list_key        = "nodeList"
        detail_endpoint = "/nodes/{uuid}"
        detail_id_field = "uuid"
      }

      collection "chassis" {
        endpoint        = "/chassis"
        list_key        = "chassisList"
        detail_endpoint = "/chassis/{uuid}"
        detail_id_field = "uuid"
      }

      collection "switches" {
        endpoint = "/switches"
        list_key = "switchList"
      }

      collection "storage" {
        endpoint = "/storage"
        list_key = "storageList"
      }
    }

Collection block attributes
---------------------------
endpoint          (required) REST path to fetch the list, e.g. ``/nodes``.
list_key          (optional) If the response is a dict, extract the list from
                  this key.  When absent the response is used as-is.
detail_endpoint   (optional) Per-item detail path template.  Use ``{field}``
                  placeholders that reference fields from the list item
                  (e.g. ``/nodes/{uuid}``).  When set, each list item is
                  enriched by merging the detail response on top of it.
detail_id_field   (optional) The field in the list item used to fill the first
                  placeholder in *detail_endpoint*.  Defaults to ``"uuid"``.
                  Ignored when *detail_endpoint* is not set.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests

from .base import DataSource
from .utils import close_http_session, disable_ssl_warnings

logger = logging.getLogger(__name__)


class RestSource(DataSource):
    """Generic HTTP/REST source adapter driven entirely by HCL ``collection {}`` blocks."""

    def __init__(self) -> None:
        self._session: requests.Session | None = None
        self._base_url: str = ""
        self._collections: dict[str, Any] = {}  # name → CollectionConfig

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Set up an HTTP session using settings from *config* (SourceConfig)."""
        if not config.collections:
            raise ValueError(
                "RestSource requires at least one 'collection {}' block in the "
                "source HCL block.  No collections were found."
            )

        self._collections = config.collections
        self._base_url = self._normalise_url(config.url)

        verify_ssl = config.verify_ssl
        if not verify_ssl:
            disable_ssl_warnings()

        session = requests.Session()
        session.verify = verify_ssl
        session.headers.update({"Accept": "application/json"})

        auth_scheme = (config.extra.get("auth") or "basic").lower()

        if auth_scheme == "basic":
            session.auth = (config.username, config.password)
        elif auth_scheme == "bearer":
            session.headers["Authorization"] = f"Bearer {config.password}"
        elif auth_scheme == "header":
            header_name = config.extra.get("auth_header", "X-Api-Key")
            session.headers[header_name] = config.password
        else:
            raise ValueError(
                f"RestSource: unknown auth scheme {auth_scheme!r}. "
                "Supported: basic, bearer, header"
            )

        self._session = session
        logger.info("RestSource connected: %s (%s auth)", self._base_url, auth_scheme)

    def get_objects(self, collection: str) -> list:
        """Fetch and return all items for the named *collection*."""
        if self._session is None:
            raise RuntimeError("RestSource: connect() has not been called")

        col = self._collections.get(collection)
        if col is None:
            available = sorted(self._collections)
            raise ValueError(
                f"RestSource: unknown collection {collection!r}. "
                f"Defined in HCL: {available}"
            )

        items = self._fetch_list(col)

        if col.detail_endpoint:
            items = self._enrich_with_detail(items, col)

        return items

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session = close_http_session(self._session, "RestSource")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_url(url: str) -> str:
        """Ensure the base URL has a scheme, an explicit port, and no trailing slash.

        Including the port explicitly (e.g. ``https://host:443``) keeps the
        behaviour identical to the legacy XClarityClient in the archive, which
        always constructs ``https://{host}:{port}``.  Some embedded REST servers
        (Lenovo XClarity Administrator is a known example) validate the ``Host``
        request header and return HTTP 500 when the port is omitted.
        """
        url = url.strip().rstrip("/")
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        parsed = urlparse(url)
        if not parsed.port:
            default_port = 443 if parsed.scheme == "https" else 80
            netloc = f"{parsed.hostname}:{default_port}"
            url = urlunparse((parsed.scheme, netloc, parsed.path or "", "", "", ""))
        return url

    def _get(self, path: str) -> Any:
        """Perform a GET request and return the parsed JSON response."""
        # Ensure path starts with / for safe joining with the base URL
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        logger.debug("RestSource GET %s", url)
        resp = self._session.get(url, timeout=30)  # type: ignore[union-attr]
        resp.raise_for_status()
        return resp.json()

    def _fetch_list(self, col: Any) -> list:
        """Fetch the list endpoint and extract the item list."""
        data = self._get(col.endpoint)

        if col.list_key and isinstance(data, dict):
            items = data.get(col.list_key, data)
        elif isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            # No list_key specified — try common list-shaped responses first,
            # then fall back to the values of the dict.
            for fallback_key in ("items", "results", "data", "records"):
                if fallback_key in data and isinstance(data[fallback_key], list):
                    items = data[fallback_key]
                    break
            else:
                items = list(data.values()) if data else []
        else:
            items = []

        if not isinstance(items, list):
            items = [items] if items else []

        logger.debug("RestSource: fetched %d items from %s", len(items), col.endpoint)
        return items

    def _enrich_with_detail(self, items: list, col: Any) -> list:
        """Merge each item with its detail response."""
        id_field = col.detail_id_field or "uuid"
        enriched = []
        for item in items:
            item_id = item.get(id_field) if isinstance(item, dict) else None
            if item_id is None:
                enriched.append(item)
                continue
            path = self._render_detail_path(col.detail_endpoint, item, id_field)
            try:
                detail = self._get(path)
                merged = {**item, **detail} if isinstance(detail, dict) else item
            except Exception as exc:
                logger.warning(
                    "RestSource: failed to fetch detail %s for %s=%s: %s",
                    col.detail_endpoint, id_field, item_id, exc,
                )
                merged = item
            enriched.append(merged)

        logger.debug(
            "RestSource: enriched %d items via %s", len(enriched), col.detail_endpoint
        )
        return enriched

    @staticmethod
    def _render_detail_path(detail_endpoint: str, item: Any, id_field: str) -> str:
        """Render *detail_endpoint* placeholders from *item*.

        Named placeholders like ``{uuid}`` or ``{tenant.id}`` are resolved
        against fields on the list item. For backward compatibility, missing
        placeholders fall back to the configured *id_field* value when present.
        """

        if not isinstance(item, dict):
            return detail_endpoint

        item_id = item.get(id_field)

        def replace_placeholder(match: re.Match[str]) -> str:
            placeholder = match.group(1).strip()
            if not placeholder:
                return match.group(0)

            value = item
            for part in placeholder.split("."):
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = getattr(value, part, None)
                if value is None:
                    break

            if value is None:
                value = item_id
            if value is None:
                return match.group(0)
            return str(value)

        return re.sub(r"\{([^}]+)\}", replace_placeholder, detail_endpoint)
