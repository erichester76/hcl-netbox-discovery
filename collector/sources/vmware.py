"""VMware vCenter data source adapter.

Wraps pyVmomi's SmartConnect / Disconnect lifecycle and returns managed
objects for the three supported collections:

  ``"clusters"``  – vim.ClusterComputeResource
  ``"hosts"``     – vim.HostSystem
  ``"vms"``       – vim.VirtualMachine

Field expressions in HCL use ``source()`` with dotted paths to traverse the
pyVmomi attribute graph directly (e.g. ``source("hardware.systemInfo.vendor")``).
The field_resolvers ``source()`` function handles ``getattr`` traversal on
pyVmomi managed objects transparently.

REST session (vSphere REST API)
-------------------------------
A requests-based REST session is optionally established alongside the pyVmomi
connection so that HCL field expressions can call ``source("restTags")`` on
VM objects.  The session populates a ``_vm_tags`` attribute on each VM dict
wrapper when ``fetch_tags`` is enabled in the source config.
"""

from __future__ import annotations

import logging
import ssl
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)


class VMwareSource(DataSource):
    """pyVmomi-backed source adapter for VMware vCenter."""

    def __init__(self) -> None:
        self._api_client: Optional[Any] = None
        self._rest_session: Optional[Any] = None
        self._config: Optional[Any] = None
        self._ssl_ctx = ssl._create_unverified_context()

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to vCenter using settings from *config* (SourceConfig)."""
        try:
            from pyVim.connect import SmartConnect  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "pyVmomi is required for the VMware source adapter. "
                "Install it with: pip install pyvmomi"
            ) from exc

        self._config = config
        logger.info("Connecting to vCenter: %s", config.url)
        self._api_client = SmartConnect(
            host=config.url,
            user=config.username,
            pwd=config.password,
            sslContext=self._ssl_ctx,
        )
        logger.info("Connected to vCenter: %s", config.url)

        # Optionally establish a REST session for tag fetching
        if config.extra.get("fetch_tags", "false").lower() in ("true", "1", "yes"):
            self._rest_session = self._connect_rest(config)

    def get_objects(self, collection: str) -> list:
        """Return a flat list of raw pyVmomi managed objects for *collection*."""
        if self._api_client is None:
            raise RuntimeError("VMwareSource: connect() has not been called")

        collectors = {
            "clusters": self._get_clusters,
            "hosts": self._get_hosts,
            "vms": self._get_vms,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"VMwareSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Disconnect from vCenter."""
        if self._api_client is not None:
            try:
                from pyVim.connect import Disconnect  # type: ignore[import]
                Disconnect(self._api_client)
            except Exception as exc:
                logger.debug("vCenter disconnect error: %s", exc)
            finally:
                self._api_client = None

        if self._rest_session is not None:
            try:
                self._rest_session.delete(
                    "/rest/com/vmware/cis/session",
                    timeout=5,
                )
            except Exception as exc:
                logger.debug("REST session logout error: %s", exc)
            finally:
                self._rest_session = None

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_clusters(self) -> list:
        from pyVmomi import vim  # type: ignore[import]
        content = self._api_client.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.ClusterComputeResource], True
        )
        clusters = list(view.view)
        view.Destroy()
        logger.debug("VMware: fetched %d clusters", len(clusters))
        return clusters

    def _get_hosts(self) -> list:
        from pyVmomi import vim  # type: ignore[import]
        content = self._api_client.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.HostSystem], True
        )
        hosts = list(view.view)
        view.Destroy()
        logger.debug("VMware: fetched %d hosts", len(hosts))
        return hosts

    def _get_vms(self) -> list:
        from pyVmomi import vim  # type: ignore[import]
        content = self._api_client.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.VirtualMachine], True
        )
        vms = list(view.view)
        view.Destroy()
        logger.debug("VMware: fetched %d VMs", len(vms))

        # Optionally attach REST tags to each VM
        if self._rest_session is not None:
            tags_by_moid = self._fetch_rest_tags()
            for vm in vms:
                vm._rest_tags = tags_by_moid.get(getattr(vm, "_moId", None), {})

        return vms

    # ------------------------------------------------------------------
    # REST session helpers
    # ------------------------------------------------------------------

    def _connect_rest(self, config: Any) -> Any:
        """Authenticate to the vSphere REST API and return the session."""
        import requests
        from urllib.parse import urljoin
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        session = requests.Session()
        session.verify = config.verify_ssl
        base_url = f"https://{config.url}"
        try:
            resp = session.post(
                urljoin(base_url, "/rest/com/vmware/cis/session"),
                auth=(config.username, config.password),
                timeout=30,
            )
            resp.raise_for_status()
            session.headers["vmware-api-session-id"] = resp.json().get("value", "")
            session._base_url = base_url  # type: ignore[attr-defined]
            logger.info("VMware REST session established for tag fetch")
            return session
        except Exception as exc:
            logger.warning("Could not establish REST session (tags will be unavailable): %s", exc)
            return None

    def _fetch_rest_tags(self) -> dict[str, dict]:
        """Fetch vSphere tags for all VMs and return a dict keyed by moId."""
        if self._rest_session is None:
            return {}
        try:
            base = getattr(self._rest_session, "_base_url", "")
            resp = self._rest_session.get(
                f"{base}/rest/com/vmware/cis/tagging/tag-association?action=list-attached-tags-on-objects",
                timeout=30,
            )
            resp.raise_for_status()
            # Response is a list of {object_id: {id, type}, tag_ids: [...]}
            # We build a simple {moId: {tag_name: tag_category}} dict
            result: dict[str, dict] = {}
            for entry in resp.json().get("value", []):
                moid = entry.get("object_id", {}).get("id", "")
                if moid:
                    result[moid] = {tag: True for tag in entry.get("tag_ids", [])}
            return result
        except Exception as exc:
            logger.warning("Failed to fetch REST tags: %s", exc)
            return {}
