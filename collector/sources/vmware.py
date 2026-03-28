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
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)


class VMwareSource(DataSource):
    """pyVmomi-backed source adapter for VMware vCenter."""

    def __init__(self) -> None:
        self._api_client: Optional[Any] = None
        self._rest_session: Optional[Any] = None
        self._config: Optional[Any] = None

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

        if not config.username:
            logger.warning("VMware: username is empty – authentication will likely fail")
        if not config.password:
            logger.warning("VMware: password is empty – authentication will likely fail")

        logger.info("Connecting to vCenter: %s", config.url)

        import urllib3

        connect_kwargs: dict = {
            "host": config.url,
            "user": config.username,
            "pwd": config.password,
        }
        if not config.verify_ssl:
            # pyVmomi 8.x: disableSslCertValidation is the correct way to skip
            # certificate verification.  Passing only an unverified sslContext
            # without this flag causes pyVmomi to still attempt thumbprint
            # validation, which can result in vim.fault.InvalidLogin.
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            connect_kwargs["disableSslCertValidation"] = True

        self._api_client = SmartConnect(**connect_kwargs)
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

        # Enrich each host's vNIC objects with VLAN info so that HCL
        # expressions can reference source('_vlans') on a vnic item.
        for host in hosts:
            self._enrich_host_vnics(host)

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

        # Enrich each VM's guest NIC objects with VLAN info so that HCL
        # expressions can reference source('_vlans') on a guest.net item.
        for vm in vms:
            self._enrich_vm_interfaces(vm)

        return vms

    # ------------------------------------------------------------------
    # REST session helpers
    # ------------------------------------------------------------------

    def _build_portgroup_vlan_map(self, host: Any) -> dict:
        """Return a ``{portgroup_key_or_name: {"id": vid, "name": name}}`` dict.

        Gathers VLAN data from both DVS distributed port-groups (via
        ``host.network``) and standard vSwitch port-groups (via
        ``host.config.network.portgroup``).  Only VLANs with a valid 802.1Q
        VLAN ID (1–4094) are included.
        """
        try:
            from pyVmomi import vim  # type: ignore[import]
        except ImportError:
            return {}

        pg_to_vlan: dict = {}

        # --- DVS distributed port-groups ---
        try:
            for network in getattr(host, "network", []):
                if isinstance(network, vim.dvs.DistributedVirtualPortgroup):
                    try:
                        vlan_id = network.config.defaultPortConfig.vlan.vlanId
                    except Exception:
                        vlan_id = None
                    if isinstance(vlan_id, int) and 0 < vlan_id <= 4094:
                        info = {"id": vlan_id, "name": network.name}
                        pg_to_vlan[network.key] = info
                        pg_to_vlan[network.name] = info
        except Exception as exc:
            logger.debug("Could not read DVS portgroup info for host: %s", exc)

        # --- Standard vSwitch port-groups ---
        try:
            for pg in getattr(
                getattr(getattr(host, "config", None), "network", None), "portgroup", []
            ):
                vlan_id = getattr(getattr(pg, "spec", None), "vlanId", None)
                pg_name = getattr(getattr(pg, "spec", None), "name", None)
                if pg_name and isinstance(vlan_id, int) and 0 < vlan_id <= 4094:
                    pg_to_vlan[pg_name] = {"id": vlan_id, "name": pg_name}
        except Exception as exc:
            logger.debug("Could not read standard vSwitch portgroups for host: %s", exc)

        return pg_to_vlan

    def _enrich_host_vnics(self, host: Any) -> None:
        """Attach a ``_vlans`` list to each VMkernel NIC on *host*.

        Each element of ``_vlans`` is a ``{"id": <vid>, "name": <name>}`` dict.
        When the vNIC portgroup has no VLAN (or the VLAN ID is 0) the list is
        empty.  This lets HCL ``tagged_vlan`` blocks reference
        ``source_items = "_vlans"`` on the vNIC item.
        """
        pg_to_vlan = self._build_portgroup_vlan_map(host)

        try:
            for vnic in getattr(
                getattr(getattr(host, "config", None), "network", None), "vnic", []
            ):
                vlan_info = None

                # DVS portgroup key takes priority
                dvp = getattr(getattr(vnic, "spec", None), "distributedVirtualPort", None)
                if dvp is not None:
                    pg_key = getattr(dvp, "portgroupKey", None)
                    if pg_key:
                        vlan_info = pg_to_vlan.get(pg_key)

                # Fall back to standard portgroup name
                if vlan_info is None:
                    pg_name = getattr(getattr(vnic, "spec", None), "portgroup", None)
                    if pg_name:
                        vlan_info = pg_to_vlan.get(pg_name)

                vnic._vlans = [vlan_info] if vlan_info else []
        except Exception as exc:
            logger.debug("Could not enrich vNIC VLAN info for host: %s", exc)

    def _enrich_vm_interfaces(self, vm: Any) -> None:
        """Attach a ``_vlans`` list to each ``guest.net`` NIC entry on *vm*.

        Mirrors :meth:`_enrich_host_vnics`: each element of ``_vlans`` is a
        ``{"id": <vid>, "name": <name>}`` dict so HCL ``tagged_vlan`` blocks
        can reference ``source_items = "_vlans"`` on a VM guest NIC item.
        """
        try:
            from pyVmomi import vim  # type: ignore[import]
        except ImportError:
            return

        # Build portgroup-name → VLAN info from the VM's attached networks
        network_to_vlan: dict = {}
        try:
            for network in getattr(vm, "network", []):
                if isinstance(network, vim.dvs.DistributedVirtualPortgroup):
                    try:
                        vlan_id = network.config.defaultPortConfig.vlan.vlanId
                    except Exception:
                        vlan_id = None
                    if isinstance(vlan_id, int) and 0 < vlan_id <= 4094:
                        info = {"id": vlan_id, "name": network.name}
                        network_to_vlan[network.name] = info
                        network_to_vlan[network.key] = info
        except Exception as exc:
            logger.debug("Could not read VM network info: %s", exc)

        # Enrich each GuestNicInfo with the resolved VLAN info
        try:
            for net in getattr(getattr(vm, "guest", None), "net", []):
                network_name = getattr(net, "network", None)
                vlan_info = network_to_vlan.get(network_name) if network_name else None
                net._vlans = [vlan_info] if vlan_info else []
        except Exception as exc:
            logger.debug("Could not enrich VM interface VLAN info: %s", exc)

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
