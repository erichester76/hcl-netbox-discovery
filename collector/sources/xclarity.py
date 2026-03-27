"""Lenovo XClarity Administrator data source adapter.

Wraps a thin REST client for XClarity and returns plain Python dicts for the
four supported collections:

  ``"nodes"``    – GET /nodes    → managed compute nodes (servers)
  ``"chassis"``  – GET /chassis  → chassis (blade centres)
  ``"switches"`` – GET /switches → managed top-of-rack / embedded switches
  ``"storage"``  – GET /storage  → managed storage subsystems

Individual node / chassis details (needed for inventory sub-items) are lazily
fetched on demand by the engine via ``source()`` path traversal, because
XClarity exposes detailed data at per-UUID endpoints that differ from the list
payload.  The source adapter enriches each item returned from the list endpoint
with its full detail dict so that field expressions can navigate the full
attribute tree without extra HCL configuration.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning  # type: ignore[import]

from .base import DataSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Minimal XClarity REST client (extracted from the archive script)
# ---------------------------------------------------------------------------

class _XClarityClient:
    """Minimal REST wrapper for the Lenovo XClarity Administrator API."""

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
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._session.verify = verify_ssl
        self._session.headers.update({"Accept": "application/json"})
        if not verify_ssl:
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        url = f"{self.base_url}{path}"
        logger.debug("XClarity GET %s params=%s", url, params)
        response = self._session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_nodes(self) -> list[dict]:
        """Return all managed compute nodes."""
        data = self._get("/nodes")
        return data.get("nodeList", data) if isinstance(data, dict) else data

    def get_node_details(self, uuid: str) -> dict:
        """Return detailed information for a single node."""
        return self._get(f"/nodes/{uuid}")

    def get_chassis(self) -> list[dict]:
        """Return all managed chassis."""
        data = self._get("/chassis")
        return data.get("chassisList", data) if isinstance(data, dict) else data

    def get_chassis_details(self, uuid: str) -> dict:
        """Return detailed information for a single chassis."""
        return self._get(f"/chassis/{uuid}")

    def get_switches(self) -> list[dict]:
        """Return all managed switches."""
        data = self._get("/switches")
        return data.get("switchList", data) if isinstance(data, dict) else data

    def get_storage(self) -> list[dict]:
        """Return all managed storage devices."""
        data = self._get("/storage")
        return data.get("storageList", data) if isinstance(data, dict) else data


# ---------------------------------------------------------------------------
# XClaritySource adapter
# ---------------------------------------------------------------------------

class XClaritySource(DataSource):
    """XClarity REST-backed source adapter."""

    def __init__(self) -> None:
        self._client: Optional[_XClarityClient] = None
        self._config: Optional[Any] = None
        # Controls whether full node detail is fetched on each node (expensive
        # but required for sub-inventory access).  Set via
        # ``source.extra.fetch_node_details = "true"`` in the HCL file.
        self._fetch_node_details: bool = True

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to XClarity using settings from *config* (SourceConfig)."""
        self._config = config
        verify_ssl = config.verify_ssl

        # Allow overriding fetch_node_details from the HCL extra dict
        if config.extra.get("fetch_node_details", "").lower() in ("false", "0", "no"):
            self._fetch_node_details = False

        logger.info("Connecting to XClarity: %s", config.url)
        self._client = _XClarityClient(
            host=config.url,
            username=config.username,
            password=config.password,
            verify_ssl=verify_ssl,
        )
        # Verify connectivity with a lightweight call
        try:
            self._client._get("/aicc/discover")
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                pass  # endpoint may not exist; connection is still valid
            else:
                raise
        except Exception:
            pass  # non-fatal; actual failures will surface on data calls
        logger.info("XClarity connection established: %s", config.url)

    def get_objects(self, collection: str) -> list:
        """Return a flat list of enriched dicts for *collection*."""
        if self._client is None:
            raise RuntimeError("XClaritySource: connect() has not been called")

        collectors = {
            "nodes": self._get_nodes,
            "chassis": self._get_chassis,
            "switches": self._get_switches,
            "storage": self._get_storage,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"XClaritySource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Close the underlying requests session."""
        if self._client is not None:
            try:
                self._client._session.close()
            except Exception as exc:
                logger.debug("XClarity session close error: %s", exc)
            finally:
                self._client = None

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_nodes(self) -> list[dict]:
        nodes = self._client.get_nodes()  # type: ignore[union-attr]
        logger.debug("XClarity: fetched %d nodes (list)", len(nodes))

        if not self._fetch_node_details:
            return nodes

        # Enrich each node with full detail payload so that field expressions
        # like source("memoryModules") work without extra HCL.
        enriched = []
        for node in nodes:
            uuid = node.get("uuid")
            if uuid:
                try:
                    detail = self._client.get_node_details(uuid)  # type: ignore[union-attr]
                    merged = {**node, **detail}
                except Exception as exc:
                    logger.warning("Failed to fetch node detail uuid=%s: %s", uuid, exc)
                    merged = node
            else:
                merged = node
            enriched.append(merged)

        logger.debug("XClarity: enriched %d nodes", len(enriched))
        return enriched

    def _get_chassis(self) -> list[dict]:
        chassis_list = self._client.get_chassis()  # type: ignore[union-attr]
        logger.debug("XClarity: fetched %d chassis (list)", len(chassis_list))

        if not self._fetch_node_details:
            return chassis_list

        enriched = []
        for ch in chassis_list:
            uuid = ch.get("uuid")
            if uuid:
                try:
                    detail = self._client.get_chassis_details(uuid)  # type: ignore[union-attr]
                    merged = {**ch, **detail}
                except Exception as exc:
                    logger.warning("Failed to fetch chassis detail uuid=%s: %s", uuid, exc)
                    merged = ch
            else:
                merged = ch
            enriched.append(merged)

        logger.debug("XClarity: enriched %d chassis", len(enriched))
        return enriched

    def _get_switches(self) -> list[dict]:
        switches = self._client.get_switches()  # type: ignore[union-attr]
        logger.debug("XClarity: fetched %d switches", len(switches))
        return switches

    def _get_storage(self) -> list[dict]:
        storage = self._client.get_storage()  # type: ignore[union-attr]
        logger.debug("XClarity: fetched %d storage devices", len(storage))
        return storage
