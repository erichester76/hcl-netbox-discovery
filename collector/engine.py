"""Top-level orchestrator for HCL-driven NetBox syncing.

Usage
-----
from collector.engine import Engine
engine = Engine()
engine.run("mappings/vmware.hcl")
"""

from __future__ import annotations

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
    ObjectConfig,
    load_config,
)
from .context import RunContext
from .field_resolvers import Resolver, walk_path
from .prerequisites import PrerequisiteRunner, extract_id, slugify

logger = logging.getLogger(__name__)


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
        cache_backend=cfg_nb.cache if cfg_nb.cache in ("none", "redis", "sqlite") else "none",
    )
    if cfg_nb.cache == "redis":
        kwargs["redis_url"] = cfg_nb.cache_url or "redis://localhost:6379/0"
    if cfg_nb.cache == "sqlite":
        kwargs["sqlite_path"] = cfg_nb.cache_url or ".nbx_cache.sqlite3"

    return pynetbox.api(**kwargs)


def _get_source_adapter(api_type: str) -> Any:
    """Instantiate the correct DataSource sub-class for *api_type*."""
    from .sources.azure import AzureSource
    from .sources.catc import CatalystCenterSource
    from .sources.ldap import LDAPSource
    from .sources.rest import RestSource
    from .sources.vmware import VMwareSource

    registry = {
        "vmware": VMwareSource,
        "rest":   RestSource,
        "catc":   CatalystCenterSource,
        "ldap":   LDAPSource,
        "azure":  AzureSource,
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
            One ``RunStats`` instance per ``object`` block, in declaration order.
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
        source = _get_source_adapter(cfg.source.api_type)
        source.connect(cfg.source)

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

        if cfg.collector.sync_tag and not dry_run:
            tag_ok = self._ensure_sync_tag(nb, cfg.collector.sync_tag)
            if not tag_ok:
                logger.error(
                    "Sync tag %r could not be created in NetBox; "
                    "tag injection disabled for this run to prevent 400 errors",
                    cfg.collector.sync_tag,
                )
                cfg.collector.sync_tag = ""

        all_stats: list[RunStats] = []
        try:
            for obj_cfg in cfg.objects:
                stats = self._process_object(obj_cfg, base_ctx)
                all_stats.append(stats)
                stats.log_summary()
        finally:
            source.close()
            nb.close()

        logger.info("Collector run complete  objects=%d", len(all_stats))
        return all_stats

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

                        first_for_iface = False

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
