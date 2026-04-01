"""Top-level orchestrator for HCL-driven NetBox syncing.

Usage
-----
from collector.engine import Engine
engine = Engine()
engine.run("mappings/vmware.hcl")
"""

from __future__ import annotations

import contextvars
import ipaddress
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

from .config import (
    CollectorConfig,
    CollectorOptions,
    DiskConfig,
    FieldConfig,
    InterfaceConfig,
    InventoryItemConfig,
    ModuleConfig,
    ObjectConfig,
    PowerInputConfig,
    SourceConfig,
    TaggedVlanConfig,
    build_source_config,
    load_config,
)
from .context import RunContext
from .field_resolvers import Resolver, walk_path
from .prerequisites import PrerequisiteRunner, extract_id, slugify

logger = logging.getLogger(__name__)

# Default IEC 60320 power-port connector type used when a power_input block
# does not specify a type expression or when the expression evaluates to a
# falsy value.
_DEFAULT_POWER_PORT_TYPE = "iec-60320-c14"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_nb_client(cfg_nb: Any) -> Any:
    """Construct a pynetbox2 NetBoxAPI client from *cfg_nb* (NetBoxConfig)."""
    lib_dir = os.path.join(os.path.dirname(__file__), "..", "lib")
    lib_dir = os.path.normpath(lib_dir)
    if lib_dir not in sys.path:
        sys.path.insert(0, lib_dir)
    import pynetbox2 as pynetbox  # type: ignore[import]

    kwargs: dict[str, Any] = dict(
        url=cfg_nb.url,
        token=cfg_nb.token,
        rate_limit_per_second=cfg_nb.rate_limit,
        rate_limit_burst=cfg_nb.rate_limit_burst,
        cache_backend=cfg_nb.cache if cfg_nb.cache in ("none", "redis", "sqlite") else "none",
        cache_ttl_seconds=cfg_nb.cache_ttl,
        cache_key_prefix=cfg_nb.cache_key_prefix,
        retry_attempts=cfg_nb.retry_attempts,
        retry_initial_delay_seconds=cfg_nb.retry_initial_delay,
        retry_backoff_factor=cfg_nb.retry_backoff_factor,
        retry_max_delay_seconds=cfg_nb.retry_max_delay,
        retry_jitter_seconds=cfg_nb.retry_jitter,
        retry_5xx_cooldown_seconds=cfg_nb.retry_5xx_cooldown,
    )
    if cfg_nb.retry_on_4xx:
        try:
            kwargs["retry_on_4xx"] = [
                int(c.strip()) for c in cfg_nb.retry_on_4xx.split(",") if c.strip()
            ]
        except ValueError:
            logger.warning(
                "NETBOX_RETRY_ON_4XX value %r is malformed; using pynetbox2 default",
                cfg_nb.retry_on_4xx,
            )
    if cfg_nb.branch:
        kwargs["branch"] = cfg_nb.branch
    if cfg_nb.prewarm_sentinel_ttl is not None:
        kwargs["prewarm_sentinel_ttl_seconds"] = cfg_nb.prewarm_sentinel_ttl
    if cfg_nb.cache == "redis":
        kwargs["redis_url"] = cfg_nb.cache_url or "redis://localhost:6379/0"
    if cfg_nb.cache == "sqlite":
        kwargs["sqlite_path"] = cfg_nb.cache_url or ".nbx_cache.sqlite3"

    return pynetbox.api(**kwargs)


def _get_source_adapter(api_type: str) -> Any:
    """Instantiate the correct DataSource sub-class for *api_type*."""
    from .sources.azure import AzureSource
    from .sources.catc import CatalystCenterSource
    from .sources.f5 import F5Source
    from .sources.ldap import LDAPSource
    from .sources.netbox import NetBoxSource
    from .sources.nexus import NexusDashboardSource
    from .sources.prometheus import PrometheusSource
    from .sources.rest import RestSource
    from .sources.snmp import SNMPSource
    from .sources.tenable import TenableSource
    from .sources.vmware import VMwareSource

    registry = {
        "vmware":     VMwareSource,
        "rest":       RestSource,
        "catc":       CatalystCenterSource,
        "ldap":       LDAPSource,
        "azure":      AzureSource,
        "snmp":       SNMPSource,
        "nexus":      NexusDashboardSource,
        "f5":         F5Source,
        "prometheus": PrometheusSource,
        "tenable":    TenableSource,
        "netbox":     NetBoxSource,
    }
    cls = registry.get(api_type.lower())
    if cls is None:
        raise ValueError(
            f"Unknown source api_type {api_type!r}. Supported: {sorted(registry)}"
        )
    return cls()


def _get_nested_items(parent_obj: Any, source_items_expr: str, resolver: Resolver) -> list:
    """Return a list of nested items for *source_items_expr*.

    If the expression looks like a plain dotted path (no parentheses or
    spaces) it is walked directly on *parent_obj*.  Otherwise it is evaluated
    as a full Python expression in the resolver scope.
    """
    if not source_items_expr:
        return []
    stripped = source_items_expr.strip()
    if not stripped:
        return []

    if not any(c in stripped for c in "() "):
        result = walk_path(parent_obj, stripped)
    else:
        result = resolver.evaluate(stripped)

    if result is None:
        return []
    if isinstance(result, list):
        return result
    return [result]


# ---------------------------------------------------------------------------
# Run stats (thread-safe)
# ---------------------------------------------------------------------------

class RunStats:
    """Thread-safe counters for one object block."""

    def __init__(self, object_name: str) -> None:
        self.object_name = object_name
        self.processed = 0
        self.created = 0
        self.updated = 0
        self.skipped = 0
        self.errored = 0
        self._lock = threading.Lock()

    def record(self, result: str) -> None:
        with self._lock:
            self.processed += 1
            if result == "created":
                self.created += 1
            elif result == "updated":
                self.updated += 1
            elif result == "skipped":
                self.skipped += 1

    def record_error(self) -> None:
        with self._lock:
            self.processed += 1
            self.errored += 1

    def log_summary(self) -> None:
        logger.info(
            "Object %-24s processed=%-4d  created=%-4d  updated=%-4d  "
            "skipped=%-4d  errored=%d",
            self.object_name,
            self.processed,
            self.created,
            self.updated,
            self.skipped,
            self.errored,
        )


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class Engine:
    """Drive a full collector run from an HCL mapping file."""

    def run(
        self,
        mapping_path: str,
        dry_run_override: Optional[bool] = None,
    ) -> list[RunStats]:
        """Parse *mapping_path* and sync all objects to NetBox.

        Parameters
        ----------
        mapping_path:
            Path to the ``.hcl`` collector mapping file.
        dry_run_override:
            If not ``None``, overrides the ``dry_run`` setting from the HCL
            ``collector`` block.

        Returns
        -------
        list[RunStats]
            One ``RunStats`` instance per ``object`` block per iteration, in
            declaration order.  When no ``iterator {}`` block is present the
            behaviour is identical to previous versions (one pass, one entry
            per ``object`` block).
        """
        cfg = load_config(mapping_path)
        dry_run = dry_run_override if dry_run_override is not None else cfg.collector.dry_run

        logger.info(
            "Collector run start  mapping=%s  source=%s  dry_run=%s",
            mapping_path,
            cfg.source.api_type,
            dry_run,
        )

        nb = _build_nb_client(cfg.netbox)

        if cfg.collector.sync_tag and not dry_run:
            # Ensure the sync tag exists once (shared across all iterations)
            tag_ok = self._ensure_sync_tag(nb, cfg.collector.sync_tag)
            if not tag_ok:
                logger.error(
                    "Sync tag %r could not be created in NetBox; "
                    "tag injection disabled for this run to prevent 400 errors",
                    cfg.collector.sync_tag,
                )
                cfg.collector.sync_tag = ""

        # Build groups of (source_configs, max_workers).  When iterator blocks
        # are present each row produces its own SourceConfig with env() calls
        # re-evaluated using that row's variable overrides.  The iterator's
        # max_workers controls how many passes within that group run in parallel.
        if cfg.collector.iterators:
            groups: list[tuple[list[SourceConfig], int]] = []
            for iterator in cfg.collector.iterators:
                n = len(iterator)
                if n == 0:
                    logger.warning("Iterator block is empty; skipping")
                    continue
                rows: list[SourceConfig] = [
                    build_source_config(cfg.raw_source_body, cfg.source_label, iterator.get_row(i))
                    for i in range(n)
                ]
                groups.append((rows, max(1, iterator.max_workers)))
        else:
            groups = [([cfg.source], 1)]

        all_stats: list[RunStats] = []

        try:
            for rows, pass_workers in groups:
                total = len(rows)
                if pass_workers > 1 and total > 1:
                    logger.info(
                        "Running %d iterator passes in parallel (max_workers=%d)",
                        total,
                        pass_workers,
                    )
                    stats_by_idx: dict[int, list[RunStats]] = {}
                    with ThreadPoolExecutor(
                        max_workers=pass_workers,
                        thread_name_prefix="iter",
                    ) as executor:
                        futures = {
                            executor.submit(
                                contextvars.copy_context().run,
                                self._run_pass,
                                source_cfg,
                                cfg,
                                nb,
                                dry_run,
                                idx,
                                total,
                            ): idx
                            for idx, source_cfg in enumerate(rows)
                        }
                        for future in as_completed(futures):
                            idx = futures[future]
                            exc = future.exception()
                            if exc:
                                logger.error(
                                    "Iterator pass %d/%d failed: %s",
                                    idx + 1,
                                    total,
                                    exc,
                                    exc_info=exc,
                                )
                            else:
                                stats_by_idx[idx] = future.result()
                    # Preserve declaration order in returned stats
                    for i in sorted(stats_by_idx):
                        all_stats.extend(stats_by_idx[i])
                else:
                    for idx, source_cfg in enumerate(rows):
                        pass_stats = self._run_pass(source_cfg, cfg, nb, dry_run, idx, total)
                        all_stats.extend(pass_stats)
        finally:
            nb.close()

        logger.info("Collector run complete  objects=%d", len(all_stats))
        return all_stats

    def _run_pass(
        self,
        source_cfg: SourceConfig,
        cfg: CollectorConfig,
        nb: Any,
        dry_run: bool,
        pass_idx: int = 0,
        total_passes: int = 1,
    ) -> list[RunStats]:
        """Connect to *source_cfg*, process all object blocks, and disconnect.

        Returns a list of :class:`RunStats` (one per ``object {}`` block).
        """
        if total_passes > 1:
            logger.info(
                "Iterator pass %d/%d  url=%s",
                pass_idx + 1,
                total_passes,
                source_cfg.url,
            )

        source = _get_source_adapter(source_cfg.api_type)
        source.connect(source_cfg)

        base_ctx = RunContext(
            nb=nb,
            source_adapter=source,
            collector_opts=cfg.collector,
            regex_dir=cfg.collector.regex_dir,
            prereqs={},
            source_obj=None,
            parent_nb_obj=None,
            dry_run=dry_run,
        )

        pass_stats: list[RunStats] = []
        try:
            for obj_cfg in cfg.objects:
                stats = self._process_object(obj_cfg, base_ctx)
                pass_stats.append(stats)
                stats.log_summary()
        finally:
            source.close()

        return pass_stats


    # ------------------------------------------------------------------
    # Object-level processing
    # ------------------------------------------------------------------

    def _ensure_sync_tag(self, nb: Any, tag_name: str) -> bool:
        """Create or verify the sync tag in NetBox.

        Returns ``True`` when the tag was successfully created or already
        exists, and ``False`` when creation failed.  Callers that receive
        ``False`` should disable tag injection so that subsequent object
        upserts are not rejected by NetBox with a 400 "Related object not
        found" error.
        """
        slug = slugify(tag_name)
        try:
            nb.upsert(
                "extras.tags",
                {"name": tag_name, "slug": slug, "color": "9e9e9e"},
                lookup_fields=["slug"],
            )
            return True
        except Exception as exc:
            logger.error("Failed to ensure sync tag %r: %s", tag_name, exc)
            return False

    def _process_object(self, obj_cfg: ObjectConfig, ctx: RunContext) -> RunStats:
        stats = RunStats(obj_cfg.name)

        logger.info(
            "Fetching %r (collection=%s) from source",
            obj_cfg.name,
            obj_cfg.source_collection,
        )
        try:
            items = ctx.source_adapter.get_objects(obj_cfg.source_collection)
        except Exception as exc:
            logger.error("Failed to fetch collection %r: %s", obj_cfg.source_collection, exc)
            return stats

        logger.info("Fetched %d items for %r", len(items), obj_cfg.name)

        max_workers = obj_cfg.max_workers or ctx.collector_opts.max_workers or 4
        prereq_runner = PrerequisiteRunner(ctx.nb)

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=obj_cfg.name[:16],
        ) as executor:
            futures = {
                executor.submit(
                    # Copy the context once per item so each worker thread
                    # gets its own Context object.  A single Context cannot
                    # be entered concurrently from multiple threads, which
                    # caused "cannot enter context: ... is already entered".
                    contextvars.copy_context().run,
                    self._process_item,
                    item,
                    obj_cfg,
                    ctx.for_item(item),
                    prereq_runner,
                    stats,
                ): item
                for item in items
            }
            for future in as_completed(futures):
                exc = future.exception()
                if exc:
                    logger.error(
                        "Unhandled error processing item: %s", exc, exc_info=exc
                    )

        return stats

    # ------------------------------------------------------------------
    # Item-level processing
    # ------------------------------------------------------------------

    def _process_item(
        self,
        item: Any,
        obj_cfg: ObjectConfig,
        ctx: RunContext,
        prereq_runner: PrerequisiteRunner,
        stats: RunStats,
    ) -> None:
        resolver = Resolver(ctx)

        # 1. Resolve prerequisites in declaration order
        for prereq_cfg in obj_cfg.prerequisites:
            try:
                result = prereq_runner.run(prereq_cfg, resolver, ctx.dry_run)
                ctx.prereqs[prereq_cfg.name] = result
                # Rebuild resolver so prereq() calls see the new values
                resolver = Resolver(ctx)
            except Exception as exc:
                if prereq_cfg.optional:
                    logger.debug(
                        "Optional prereq %r failed (continuing): %s",
                        prereq_cfg.name, exc,
                    )
                    ctx.prereqs[prereq_cfg.name] = None
                    resolver = Resolver(ctx)
                else:
                    logger.warning(
                        "Required prereq %r failed — skipping item: %s",
                        prereq_cfg.name, exc,
                    )
                    stats.record_error()
                    return

        # 2. Build payload from field blocks
        try:
            payload = self._build_payload(obj_cfg.fields, resolver, ctx)
        except Exception as exc:
            logger.warning("Payload build failed for %r: %s", obj_cfg.name, exc)
            stats.record_error()
            return

        if not payload:
            logger.debug("Empty payload for %r — skipping item", obj_cfg.name)
            stats.record("skipped")
            return

        # 3. Inject sync tag
        self._inject_sync_tag(payload, ctx.collector_opts.sync_tag)

        # 4. Upsert to NetBox
        nb_obj = self._upsert(
            ctx, obj_cfg.netbox_resource, payload, obj_cfg.lookup_by, stats
        )
        if nb_obj is None and not ctx.dry_run:
            return

        # 5. Process nested collections
        try:
            self._process_interfaces(obj_cfg, nb_obj, ctx)
            self._process_inventory_items(obj_cfg, nb_obj, ctx)
            self._process_disks(obj_cfg, nb_obj, ctx)
            self._process_modules(obj_cfg, nb_obj, ctx)
        except Exception as exc:
            logger.error(
                "Nested collection processing failed for %r: %s",
                obj_cfg.name, exc, exc_info=True,
            )

    # ------------------------------------------------------------------
    # Payload builders
    # ------------------------------------------------------------------

    def _build_payload(
        self,
        fields: list[FieldConfig],
        resolver: Resolver,
        ctx: RunContext,
    ) -> dict:
        payload: dict[str, Any] = {}
        for field_cfg in fields:
            try:
                value = self._eval_field(field_cfg, resolver, ctx)
                if value is not None:
                    payload[field_cfg.name] = value
            except Exception as exc:
                logger.debug(
                    "Field %r evaluation error: %s", field_cfg.name, exc
                )
        return payload

    def _eval_field(
        self,
        field_cfg: FieldConfig,
        resolver: Resolver,
        ctx: RunContext,
    ) -> Any:
        """Evaluate a single field and return the value for the payload."""

        # --- tags field ---
        if field_cfg.type == "tags":
            raw = resolver.evaluate(field_cfg.value)
            if not isinstance(raw, list):
                raw = [raw] if raw else []
            # Normalize plain strings to the dict form NetBox expects.
            return [{"name": t} if isinstance(t, str) else t for t in raw if t]

        # --- FK field ---
        if field_cfg.type == "fk":
            lookup: dict[str, Any] = {}
            for k, v in (field_cfg.lookup or {}).items():
                resolved = resolver.evaluate(v) if isinstance(v, str) else v
                if resolved is not None:
                    lookup[k] = resolved
            if not lookup:
                return None
            if ctx.dry_run:
                logger.debug("[DRY-RUN] FK lookup %s %s", field_cfg.resource, lookup)
                return None
            try:
                if field_cfg.ensure:
                    obj = ctx.nb.upsert(
                        field_cfg.resource,
                        lookup,
                        lookup_fields=list(lookup.keys()),
                    )
                else:
                    obj = ctx.nb.get(field_cfg.resource, **lookup)
                return extract_id(obj)
            except Exception as exc:
                logger.debug(
                    "FK lookup failed resource=%s lookup=%s: %s",
                    field_cfg.resource, lookup, exc,
                )
                return None

        # --- scalar field (default) ---
        return resolver.evaluate(field_cfg.value)

    # ------------------------------------------------------------------
    # Sync tag injection
    # ------------------------------------------------------------------

    @staticmethod
    def _inject_sync_tag(payload: dict, sync_tag: str) -> None:
        if not sync_tag:
            return
        tags = payload.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        tag_dict = {"name": sync_tag}
        existing_names = {
            t.get("name") if isinstance(t, dict) else t for t in tags
        }
        if sync_tag not in existing_names:
            tags.append(tag_dict)
        payload["tags"] = tags

    # ------------------------------------------------------------------
    # NetBox write helpers
    # ------------------------------------------------------------------

    def _upsert(
        self,
        ctx: RunContext,
        resource: str,
        payload: dict,
        lookup_fields: list[str],
        stats: Optional[RunStats] = None,
    ) -> Any:
        if ctx.dry_run:
            logger.info(
                "[DRY-RUN] upsert  resource=%-35s  lookup=%s  keys=%s",
                resource,
                lookup_fields,
                sorted(payload.keys()),
            )
            if stats is not None:
                stats.record("skipped")
            return None
        try:
            obj = ctx.nb.upsert(resource, payload, lookup_fields=lookup_fields)
            lookup_display = {k: payload[k] for k in lookup_fields if k in payload}
            logger.info("Upserted  resource=%-30s  %s", resource, lookup_display)
            if stats is not None:
                stats.record("created")
            return obj
        except Exception as exc:
            logger.error(
                "Upsert failed  resource=%s  keys=%s: %s",
                resource, sorted(payload.keys()), exc,
            )
            if stats is not None:
                stats.record_error()
            return None

    # ------------------------------------------------------------------
    # Nested collection processors
    # ------------------------------------------------------------------

    def _process_interfaces(
        self,
        obj_cfg: ObjectConfig,
        parent_nb_obj: Any,
        ctx: RunContext,
    ) -> None:
        parent_resolver = Resolver(ctx)

        for iface_cfg in obj_cfg.interfaces:
            # Check enabled_if gate
            if iface_cfg.enabled_if is not None:
                if not parent_resolver.evaluate(iface_cfg.enabled_if):
                    continue

            items = _get_nested_items(ctx.source_obj, iface_cfg.source_items, parent_resolver)
            if not items:
                continue

            logger.info("Processing %d interface(s) for %r", len(items), obj_cfg.name)

            # Choose correct NetBox resource and parent field name
            if "virtual_machine" in obj_cfg.netbox_resource or obj_cfg.netbox_resource.endswith("virtual_machines"):
                iface_resource = "virtualization.interfaces"
                parent_field = "virtual_machine"
            else:
                iface_resource = "dcim.interfaces"
                parent_field = "device"

            parent_id = extract_id(parent_nb_obj)
            first_primary_ip4_set = False
            first_primary_ip6_set = False
            first_oob_set = False

            for iface_item in items:
                nested_ctx = ctx.for_nested(iface_item, parent_nb_obj)
                resolver = Resolver(nested_ctx)

                payload = self._build_payload(iface_cfg.fields, resolver, nested_ctx)
                if not payload:
                    continue

                if parent_id is not None:
                    payload[parent_field] = parent_id
                self._inject_sync_tag(payload, ctx.collector_opts.sync_tag)

                nb_iface = self._upsert(
                    nested_ctx,
                    iface_resource,
                    payload,
                    ["name", parent_field],
                )

                # Nested IP addresses
                for ip_cfg in iface_cfg.ip_addresses:
                    if ip_cfg.enabled_if is not None:
                        if not resolver.evaluate(ip_cfg.enabled_if):
                            continue

                    ip_items = _get_nested_items(iface_item, ip_cfg.source_items, resolver)
                    if not ip_items:
                        continue

                    first_for_iface = True
                    for ip_item in ip_items:
                        ip_ctx = nested_ctx.for_nested(ip_item, nb_iface)
                        ip_resolver = Resolver(ip_ctx)

                        ip_payload = self._build_payload(ip_cfg.fields, ip_resolver, ip_ctx)
                        if not ip_payload:
                            continue

                        # Attach IP to interface
                        if nb_iface is not None:
                            iface_id = extract_id(nb_iface)
                            if iface_id is not None:
                                ip_payload["assigned_object_type"] = (
                                    "dcim.interface"
                                    if iface_resource == "dcim.interfaces"
                                    else "virtualization.vminterface"
                                )
                                ip_payload["assigned_object_id"] = iface_id

                        self._inject_sync_tag(ip_payload, ctx.collector_opts.sync_tag)
                        nb_ip = self._upsert(ip_ctx, "ipam.ip_addresses", ip_payload, ["address"])

                        # Set primary IPv4 or IPv6 on parent object based on address version
                        if (
                            ip_cfg.primary_if == "first"
                            and first_for_iface
                            and nb_ip is not None
                            and parent_nb_obj is not None
                            and not ip_ctx.dry_run
                        ):
                            raw_address = ip_payload.get("address", "")
                            try:
                                ip_version = ipaddress.ip_interface(raw_address).version
                            except ValueError:
                                logger.debug(
                                    "Could not determine IP version for address %r; skipping primary IP assignment",
                                    raw_address,
                                )
                                ip_version = None

                            ip_id = extract_id(nb_ip)
                            parent_obj_id = extract_id(parent_nb_obj)
                            if ip_id is not None and parent_obj_id is not None:
                                if ip_version == 4 and not first_primary_ip4_set:
                                    try:
                                        ctx.nb.update(
                                            obj_cfg.netbox_resource,
                                            parent_obj_id,
                                            {"primary_ip4": ip_id},
                                        )
                                        first_primary_ip4_set = True
                                    except Exception as exc:
                                        logger.debug(
                                            "Failed to set primary_ip4: %s", exc
                                        )
                                elif ip_version == 6 and not first_primary_ip6_set:
                                    try:
                                        ctx.nb.update(
                                            obj_cfg.netbox_resource,
                                            parent_obj_id,
                                            {"primary_ip6": ip_id},
                                        )
                                        first_primary_ip6_set = True
                                    except Exception as exc:
                                        logger.debug(
                                            "Failed to set primary_ip6: %s", exc
                                        )

                        # Set oob_ip on the parent device for the first IP on
                        # an interface block configured with oob_if = "first".
                        # Guarded to dcim.interfaces so that virtual-machine
                        # interface blocks (virtualization.vminterface) never
                        # trigger an oob_ip write.
                        if (
                            ip_cfg.oob_if == "first"
                            and first_for_iface
                            and not first_oob_set
                            and nb_ip is not None
                            and parent_nb_obj is not None
                            and not ip_ctx.dry_run
                            and iface_resource == "dcim.interfaces"
                        ):
                            ip_id = extract_id(nb_ip)
                            parent_obj_id = extract_id(parent_nb_obj)
                            if ip_id is not None and parent_obj_id is not None:
                                try:
                                    ctx.nb.update(
                                        obj_cfg.netbox_resource,
                                        parent_obj_id,
                                        {"oob_ip": ip_id},
                                    )
                                    first_oob_set = True
                                except Exception as exc:
                                    logger.debug(
                                        "Failed to set oob_ip: %s", exc
                                    )

                        first_for_iface = False

                # Nested tagged VLANs
                if iface_cfg.tagged_vlans:
                    self._process_tagged_vlans(
                        iface_cfg, nb_iface, iface_item, nested_ctx, resolver, iface_resource
                    )

    def _process_tagged_vlans(
        self,
        iface_cfg: InterfaceConfig,
        nb_iface: Any,
        iface_item: Any,
        ctx: RunContext,
        resolver: Any,
        iface_resource: str,
    ) -> None:
        """Find/create VLANs described by *iface_cfg.tagged_vlans* and assign
        them as ``tagged_vlans`` on *nb_iface* in NetBox.

        If VLANs are found the interface ``mode`` is also set to ``"tagged"``.
        Nothing is written when *ctx.dry_run* is ``True``.
        """
        all_vlan_ids: list[int] = []

        for vlan_cfg in iface_cfg.tagged_vlans:
            if vlan_cfg.enabled_if is not None:
                if not resolver.evaluate(vlan_cfg.enabled_if):
                    continue

            vlan_items = _get_nested_items(iface_item, vlan_cfg.source_items, resolver)
            if not vlan_items:
                continue

            for vlan_item in vlan_items:
                vlan_ctx = ctx.for_nested(vlan_item, nb_iface)
                vlan_resolver = Resolver(vlan_ctx)

                vlan_payload = self._build_payload(vlan_cfg.fields, vlan_resolver, vlan_ctx)
                if not vlan_payload:
                    continue

                # Require at least the first lookup field to be present
                primary_key = vlan_cfg.lookup_by[0] if vlan_cfg.lookup_by else None
                if not primary_key or not vlan_payload.get(primary_key):
                    continue

                if ctx.dry_run:
                    logger.info(
                        "[DRY-RUN] tagged_vlan  resource=%s  lookup=%s  payload=%s",
                        vlan_cfg.netbox_resource,
                        vlan_cfg.lookup_by,
                        sorted(vlan_payload.keys()),
                    )
                    continue

                try:
                    lookup_fields = [k for k in vlan_cfg.lookup_by if k in vlan_payload]
                    self._inject_sync_tag(vlan_payload, ctx.collector_opts.sync_tag)
                    # For ipam.vlans with vid lookup, use multi-site aware resolution
                    # to handle the case where the same vid exists across multiple sites.
                    if (
                        vlan_cfg.netbox_resource == "ipam.vlans"
                        and "vid" in lookup_fields
                        and vlan_payload.get("vid") is not None
                    ):
                        nb_vlan = self._find_or_create_vlan_multisite(
                            vlan_payload, ctx
                        )
                    else:
                        nb_vlan = ctx.nb.upsert(
                            vlan_cfg.netbox_resource,
                            vlan_payload,
                            lookup_fields=lookup_fields,
                        )
                    vlan_id = extract_id(nb_vlan)
                    if vlan_id is not None:
                        all_vlan_ids.append(vlan_id)
                except Exception as exc:
                    logger.debug(
                        "Failed to find/create %s %s=%s: %s",
                        vlan_cfg.netbox_resource, primary_key,
                        vlan_payload.get(primary_key), exc,
                    )

        if all_vlan_ids and nb_iface is not None:
            iface_id = extract_id(nb_iface)
            if iface_id is not None:
                try:
                    ctx.nb.update(
                        iface_resource,
                        iface_id,
                        {"mode": "tagged", "tagged_vlans": all_vlan_ids},
                    )
                except Exception as exc:
                    logger.debug(
                        "Failed to set tagged_vlans on interface %s: %s", iface_id, exc
                    )

    def _find_or_create_vlan_multisite(
        self,
        vlan_payload: dict,
        ctx: RunContext,
    ) -> Any:
        """Resolve an ``ipam.vlans`` record when multiple VLANs may share the same vid.

        NetBox allows the same ``vid`` to exist at multiple sites, which causes
        ``get(vid=…)`` to raise "get() returned more than one result."  This method
        uses ``list(vid=…)`` instead and applies the same priority logic as the
        legacy vmware-collector:

        1. A **siteless** VLAN (site=None) is treated as spanning all sites and is
           preferred – the existing record is updated in-place.
        2. A **site-matched** VLAN (site == payload's site) is updated in-place.
           When other-site VLANs also exist the record is kept site-scoped rather
           than promoted to siteless.
        3. When the caller provides **no site** but only site-scoped VLANs exist,
           we refuse to auto-promote to siteless and return ``None``.
        4. When VLANs exist only at **other sites**, a new site-scoped record is
           created for the requested site.
        5. When **no VLANs** exist at all, a new record is created (with or without
           a site, depending on the payload).
        """
        vid = vlan_payload.get("vid")
        site_id = vlan_payload.get("site")

        existing_vlans = ctx.nb.list("ipam.vlans", vid=vid)

        siteless_vlan = None
        site_vlan = None
        other_site_vlans: list[Any] = []

        for existing_vlan in existing_vlans:
            existing_site = getattr(existing_vlan, "site", None)
            existing_site_id: Optional[int] = None
            if existing_site is not None:
                if isinstance(existing_site, dict):
                    existing_site_id = existing_site.get("id")
                elif hasattr(existing_site, "id"):
                    existing_site_id = existing_site.id
                elif isinstance(existing_site, int):
                    existing_site_id = existing_site

            if existing_site_id is None and siteless_vlan is None:
                siteless_vlan = existing_vlan
            elif site_id is not None and existing_site_id == site_id and site_vlan is None:
                site_vlan = existing_vlan
            else:
                other_site_vlans.append(existing_vlan)

        if siteless_vlan is not None:
            # Update the siteless VLAN in-place; remove site so it stays siteless.
            update_payload = {**vlan_payload, "id": extract_id(siteless_vlan)}
            update_payload.pop("site", None)
            return ctx.nb.upsert("ipam.vlans", update_payload, lookup_fields=["id"])

        if site_vlan is not None:
            if other_site_vlans:
                logger.debug(
                    "VLAN vid=%s exists in multiple site-scoped records; "
                    "preserving requested site %s without promoting to siteless",
                    vid, site_id,
                )
            update_payload = {**vlan_payload, "id": extract_id(site_vlan)}
            if site_id is not None:
                update_payload["site"] = site_id
            return ctx.nb.upsert("ipam.vlans", update_payload, lookup_fields=["id"])

        if site_id is None and existing_vlans:
            logger.debug(
                "VLAN vid=%s requested without a site but only site-scoped VLANs "
                "exist; refusing to auto-promote to siteless",
                vid,
            )
            return None

        if other_site_vlans:
            logger.debug(
                "VLAN vid=%s exists at other sites but not site %s; "
                "creating a new site-scoped VLAN for the requested site",
                vid, site_id,
            )

        # Create a new VLAN (with or without site as supplied in payload).
        return ctx.nb.upsert("ipam.vlans", vlan_payload, lookup_fields=[])

    def _process_inventory_items(
        self,
        obj_cfg: ObjectConfig,
        parent_nb_obj: Any,
        ctx: RunContext,
    ) -> None:
        prereq_runner = PrerequisiteRunner(ctx.nb)

        for inv_cfg in obj_cfg.inventory_items:
            resolver = Resolver(ctx)

            if inv_cfg.enabled_if is not None:
                if not resolver.evaluate(inv_cfg.enabled_if):
                    continue

            items = _get_nested_items(ctx.source_obj, inv_cfg.source_items, resolver)
            if not items:
                continue

            logger.info("Processing %d inventory item(s) for %r", len(items), obj_cfg.name)

            parent_id = extract_id(parent_nb_obj)

            # Ensure the inventory item role exists once per block
            role_id: Optional[int] = None
            if inv_cfg.role and not ctx.dry_run:
                try:
                    role_id = prereq_runner._ensure_inventory_item_role(
                        {"name": inv_cfg.role}, dry_run=False
                    )
                except Exception as exc:
                    logger.warning("Failed to ensure inventory role %r: %s", inv_cfg.role, exc)

            seen_dedup_keys: set = set()

            for inv_item in items:
                nested_ctx = ctx.for_nested(inv_item, parent_nb_obj)
                inv_resolver = Resolver(nested_ctx)

                # Deduplication
                if inv_cfg.dedupe_by:
                    dedup_key = inv_resolver.evaluate(inv_cfg.dedupe_by)
                    if dedup_key is not None:
                        if dedup_key in seen_dedup_keys:
                            continue
                        seen_dedup_keys.add(dedup_key)

                payload = self._build_payload(inv_cfg.fields, inv_resolver, nested_ctx)
                if not payload:
                    continue

                if parent_id is not None:
                    payload["device"] = parent_id
                if role_id is not None:
                    payload["role"] = role_id
                self._inject_sync_tag(payload, ctx.collector_opts.sync_tag)

                self._upsert(
                    nested_ctx,
                    "dcim.inventory_items",
                    payload,
                    ["device", "name"],
                )

    def _process_disks(
        self,
        obj_cfg: ObjectConfig,
        parent_nb_obj: Any,
        ctx: RunContext,
    ) -> None:
        for disk_cfg in obj_cfg.disks:
            resolver = Resolver(ctx)

            if disk_cfg.enabled_if is not None:
                if not resolver.evaluate(disk_cfg.enabled_if):
                    continue

            items = _get_nested_items(ctx.source_obj, disk_cfg.source_items, resolver)
            if not items:
                continue

            logger.info("Processing %d disk(s) for %r", len(items), obj_cfg.name)

            parent_id = extract_id(parent_nb_obj)

            for disk_item in items:
                nested_ctx = ctx.for_nested(disk_item, parent_nb_obj)
                disk_resolver = Resolver(nested_ctx)

                payload = self._build_payload(disk_cfg.fields, disk_resolver, nested_ctx)
                if not payload:
                    continue

                if parent_id is not None:
                    payload["virtual_machine"] = parent_id
                self._inject_sync_tag(payload, ctx.collector_opts.sync_tag)

                self._upsert(
                    nested_ctx,
                    "virtualization.virtual_disks",
                    payload,
                    ["virtual_machine", "name"],
                )

    def _process_modules(
        self,
        obj_cfg: ObjectConfig,
        parent_nb_obj: Any,
        ctx: RunContext,
    ) -> None:
        """Sync hardware components as NetBox Modules (ModuleBay + Module).

        For each item in each ``module {}`` block the engine will:

        1. Evaluate the ``bay_name``, ``position``, ``model``, ``serial``, and
           ``manufacturer`` fields from the source data.
        2. Call ``ensure_module_bay_template`` on the device type so that the
           slot is declared on the type template.
        3. Call ``ensure_module_bay`` on the device instance to ensure the
           physical bay record exists.
        4. Call ``ensure_module_type`` (model + manufacturer) to obtain the
           reusable module-type record.
        5. Upsert the ``dcim.modules`` record linking device, bay, and type.
        """
        if not obj_cfg.modules:
            return

        prereq_runner = PrerequisiteRunner(ctx.nb)
        parent_id = extract_id(parent_nb_obj)

        # Derive device_type_id from the parent NetBox device so we can add
        # bay templates without an extra API call.
        device_type_id: Optional[int] = None
        if parent_nb_obj is not None:
            dt = (
                parent_nb_obj.get("device_type")
                if isinstance(parent_nb_obj, dict)
                else getattr(parent_nb_obj, "device_type", None)
            )
            if isinstance(dt, dict):
                device_type_id = dt.get("id")
            elif dt is not None:
                device_type_id = getattr(dt, "id", None)

        for mod_cfg in obj_cfg.modules:
            resolver = Resolver(ctx)

            if mod_cfg.enabled_if is not None:
                if not resolver.evaluate(mod_cfg.enabled_if):
                    continue

            items = _get_nested_items(ctx.source_obj, mod_cfg.source_items, resolver)
            if not items:
                continue

            logger.info("Processing %d module(s) for %r", len(items), obj_cfg.name)

            seen_dedup_keys: set = set()

            for mod_item in items:
                nested_ctx = ctx.for_nested(mod_item, parent_nb_obj)
                mod_resolver = Resolver(nested_ctx)

                # Deduplication guard
                if mod_cfg.dedupe_by:
                    dedup_key = mod_resolver.evaluate(mod_cfg.dedupe_by)
                    if dedup_key is not None:
                        if dedup_key in seen_dedup_keys:
                            continue
                        seen_dedup_keys.add(dedup_key)

                # Evaluate all field expressions for this item
                raw_payload = self._build_payload(mod_cfg.fields, mod_resolver, nested_ctx)
                if not raw_payload:
                    continue

                bay_name = raw_payload.get("bay_name") or raw_payload.get("name")
                position = str(raw_payload.get("position") or "")
                model = raw_payload.get("model")
                serial = raw_payload.get("serial")
                manufacturer_name = raw_payload.get("manufacturer")

                if not bay_name or not model:
                    logger.debug(
                        "Module item missing bay_name or model — skipping (bay=%r model=%r)",
                        bay_name, model,
                    )
                    continue

                if ctx.dry_run:
                    logger.info(
                        "[DRY-RUN] module  bay=%s  model=%s  serial=%s",
                        bay_name, model, serial,
                    )
                    continue

                # 1. Resolve manufacturer ID (optional)
                manufacturer_id: Optional[int] = None
                if manufacturer_name:
                    try:
                        manufacturer_id = prereq_runner._ensure_manufacturer(
                            {"name": manufacturer_name}, dry_run=False
                        )
                    except Exception as exc:
                        logger.debug(
                            "ensure_manufacturer for module %r failed: %s", bay_name, exc
                        )

                # 2. Ensure bay template on device type
                if device_type_id is not None:
                    try:
                        prereq_runner._ensure_module_bay_template(
                            {
                                "device_type": device_type_id,
                                "name": bay_name,
                                "position": position,
                            },
                            dry_run=False,
                        )
                    except Exception as exc:
                        logger.debug(
                            "ensure_module_bay_template %r failed: %s", bay_name, exc
                        )

                # 3. Ensure bay instance on device
                bay_id: Optional[int] = None
                if parent_id is not None:
                    try:
                        bay_id = prereq_runner._ensure_module_bay(
                            {
                                "device": parent_id,
                                "name": bay_name,
                                "position": position,
                            },
                            dry_run=False,
                        )
                    except Exception as exc:
                        logger.debug(
                            "ensure_module_bay %r failed: %s", bay_name, exc
                        )

                if bay_id is None:
                    logger.debug(
                        "Could not obtain module_bay for %r — skipping module install",
                        bay_name,
                    )
                    continue

                # 4. Ensure module type
                module_type_id: Optional[int] = None
                try:
                    # Evaluate ``attribute {}`` field expressions for this item.
                    # These are applied to the ModuleType record (not the Module
                    # instance) after the profile has been committed to NetBox.
                    attrs: dict[str, Any] = {}
                    for attr_cfg in mod_cfg.attributes:
                        try:
                            val = self._eval_field(attr_cfg, mod_resolver, nested_ctx)
                            if val is not None:
                                attrs[attr_cfg.name] = val
                        except Exception as exc:
                            logger.debug(
                                "Module attribute %r evaluation error: %s",
                                attr_cfg.name, exc,
                            )

                    module_type_id = prereq_runner._ensure_module_type(
                        {
                            "model": model,
                            "manufacturer": manufacturer_id,
                            "profile": mod_cfg.profile,
                            "attributes": attrs if attrs else None,
                        },
                        dry_run=False,
                    )
                except Exception as exc:
                    logger.debug("ensure_module_type %r failed: %s", model, exc)

                if module_type_id is None:
                    logger.debug(
                        "Could not obtain module_type for %r — skipping module install",
                        model,
                    )
                    continue

                # 5. Install module
                module_payload: dict[str, Any] = {
                    "device": parent_id,
                    "module_bay": bay_id,
                    "module_type": module_type_id,
                    "status": "active",
                }
                if serial:
                    module_payload["serial"] = str(serial)

                module_record = self._upsert(
                    nested_ctx,
                    "dcim.modules",
                    module_payload,
                    ["device", "module_bay"],
                )

                # 6. Create power input port if configured
                if mod_cfg.power_input is not None and module_record is not None:
                    module_id = extract_id(module_record)
                    if module_id is not None and parent_id is not None:
                        pi_cfg = mod_cfg.power_input
                        pi_name = (
                            mod_resolver.evaluate(pi_cfg.name)
                            if pi_cfg.name
                            else None
                        )
                        pi_type = (
                                mod_resolver.evaluate(pi_cfg.type)
                                if pi_cfg.type
                                else _DEFAULT_POWER_PORT_TYPE
                            ) or _DEFAULT_POWER_PORT_TYPE
                        if pi_name:
                            self._upsert(
                                nested_ctx,
                                "dcim.power_ports",
                                {
                                    "device": parent_id,
                                    "module": module_id,
                                    "name": str(pi_name),
                                    "type": str(pi_type),
                                },
                                ["device", "name"],
                            )
