"""Prerequisite evaluation — ensure_* methods and placement resolution.

Each method corresponds to an HCL ``prerequisite.method`` value.  Methods
receive a resolved args dict (values have already been evaluated by the field
resolver) and return a value that is stored in ``RunContext.prereqs`` under
the prerequisite's name.

Return values:
  - Scalar methods (ensure_*) → integer NetBox ID or None
  - resolve_placement          → dict with keys site_id, location_id,
                                 rack_id, rack_position
  - lookup_tenant              → integer NetBox ID or None

Available methods:
  ensure_manufacturer, ensure_device_type, ensure_device_role,
  ensure_site, ensure_location, ensure_rack, ensure_platform,
  ensure_cluster_type, ensure_cluster_group, ensure_cluster,
  ensure_inventory_item_role,
  ensure_tenant_group, ensure_contact_group,
  ensure_region, ensure_vlan_group, ensure_vrf,
  ensure_tenant, lookup_tenant, resolve_placement,
  ensure_module_bay_template, ensure_module_bay,
  ensure_module_type_profile, ensure_module_type
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def slugify(value: str) -> str:
    """Convert *value* to a NetBox-compatible slug (max 100 chars)."""
    slug = str(value).lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    slug = slug.strip("-")
    return slug[:100]


def extract_id(obj: Any) -> int | None:
    """Return the integer ``id`` from a pynetbox record, dict, or None."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("id")
    return getattr(obj, "id", None)


def extract_field(obj: Any, field: str) -> Any:
    """Return *field* from a pynetbox record, dict, or None."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(field)
    return getattr(obj, field, None)


class PrerequisiteArgumentError(ValueError):
    """Raised when a prerequisite method receives invalid required input."""


def require_text_arg(args: dict[str, Any], key: str, method_name: str) -> str:
    """Return a non-empty text argument or raise a clear validation error."""
    value = args.get(key)
    if isinstance(value, str):
        value = value.strip()
    if not value:
        raise PrerequisiteArgumentError(f"{method_name} requires a non-empty {key!r}")
    return str(value)


# ---------------------------------------------------------------------------
# PrerequisiteRunner
# ---------------------------------------------------------------------------

class PrerequisiteRunner:
    """Evaluate prerequisite blocks and cache results per item."""

    def __init__(self, nb: Any) -> None:
        self.nb = nb

    def run(
        self,
        prereq_cfg: Any,   # PrerequisiteConfig
        resolver: Any,     # Resolver — used to evaluate arg expressions
        dry_run: bool,
    ) -> Any:
        """Evaluate *prereq_cfg* and return the resolved value.

        Args expressions are evaluated with *resolver* before being passed to
        the method.
        """
        # Evaluate each arg value through the resolver
        args: dict[str, Any] = {}
        for k, v in prereq_cfg.args.items():
            args[k] = resolver.evaluate(v) if isinstance(v, str) else v

        method_name = f"_{prereq_cfg.method}"
        method = getattr(self, method_name, None)
        if method is None:
            raise ValueError(f"Unknown prerequisite method: {prereq_cfg.method!r}")

        return method(args, dry_run)

    # ------------------------------------------------------------------
    # Individual methods
    # ------------------------------------------------------------------

    def _ensure_manufacturer(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_manufacturer")
        slug = slugify(name)
        if dry_run:
            logger.info("[DRY-RUN] ensure_manufacturer name=%s", name)
            return None
        obj = self.nb.upsert(
            "dcim.manufacturers",
            {"name": name, "slug": slug},
            lookup_fields=["slug"],
        )
        return extract_id(obj)

    def _ensure_device_type(self, args: dict, dry_run: bool) -> int | None:
        model = require_text_arg(args, "model", "ensure_device_type")
        manufacturer_id = args.get("manufacturer")
        slug = slugify(model)
        payload: dict[str, Any] = {"model": model, "slug": slug}
        if manufacturer_id is not None:
            payload["manufacturer"] = manufacturer_id
        if args.get("part_number"):
            payload["part_number"] = args["part_number"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_device_type model=%s manufacturer=%s", model, manufacturer_id)
            return None
        lookup = ["manufacturer", "model"] if manufacturer_id is not None else ["model"]
        obj = self.nb.upsert("dcim.device_types", payload, lookup_fields=lookup)
        return extract_id(obj)

    def _ensure_device_role(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_device_role")
        slug = slugify(name)
        color = args.get("color", "9e9e9e")
        if dry_run:
            logger.info("[DRY-RUN] ensure_device_role name=%s", name)
            return None
        obj = self.nb.upsert(
            "dcim.device_roles",
            {"name": name, "slug": slug, "color": color},
            lookup_fields=["slug"],
        )
        return extract_id(obj)

    def _ensure_site(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_site")
        slug = slugify(name)
        if dry_run:
            logger.info("[DRY-RUN] ensure_site name=%s", name)
            return None
        obj = self.nb.upsert(
            "dcim.sites",
            {"name": name, "slug": slug},
            lookup_fields=["name"],
        )
        return extract_id(obj)

    def _ensure_location(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        site_id = args.get("site_id") or args.get("site")
        slug = slugify(name)
        payload: dict[str, Any] = {"name": name, "slug": slug}
        if site_id is not None:
            payload["site"] = site_id
        lookup = ["name", "site"] if site_id is not None else ["name"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_location name=%s site=%s", name, site_id)
            return None
        obj = self.nb.upsert("dcim.locations", payload, lookup_fields=lookup)
        return extract_id(obj)

    def _ensure_rack(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        site_id = args.get("site_id") or args.get("site")
        location_id = args.get("location_id") or args.get("location")
        payload: dict[str, Any] = {"name": name}
        if site_id is not None:
            payload["site"] = site_id
        if location_id is not None:
            payload["location"] = location_id
        lookup = ["name", "site"] if site_id is not None else ["name"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_rack name=%s site=%s", name, site_id)
            return None
        obj = self.nb.upsert("dcim.racks", payload, lookup_fields=lookup)
        return extract_id(obj)

    def _ensure_platform(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        slug = slugify(name)
        manufacturer_id = args.get("manufacturer_id") or args.get("manufacturer")
        # Allow callers to pass a manufacturer name string; we'll ensure it ourselves.
        if manufacturer_id is None:
            manufacturer_name = args.get("manufacturer_name")
            if manufacturer_name:
                manufacturer_id = self._ensure_manufacturer(
                    {"name": manufacturer_name}, dry_run
                )
        payload: dict[str, Any] = {"name": name, "slug": slug}
        if manufacturer_id is not None:
            payload["manufacturer"] = manufacturer_id
        if dry_run:
            logger.info("[DRY-RUN] ensure_platform name=%s", name)
            return None
        try:
            obj = self.nb.upsert("dcim.platforms", payload, lookup_fields=["slug"])
        except Exception as exc:
            # Race condition: another thread may have created the platform between
            # the GET check and our POST — fall back to a plain GET by slug.
            # Check for a 400 status code with a uniqueness constraint violation.
            exc_str = str(exc)
            if "400" in exc_str and "unique" in exc_str.lower():
                logger.debug("ensure_platform collision for %r — falling back to GET", name)
                try:
                    obj = self.nb.get("dcim.platforms", slug=slug)
                except Exception:
                    return None
            else:
                raise
        return extract_id(obj)

    def _ensure_cluster_type(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_cluster_type")
        slug = slugify(name)
        if dry_run:
            logger.info("[DRY-RUN] ensure_cluster_type name=%s", name)
            return None
        obj = self.nb.upsert(
            "virtualization.cluster_types",
            {"name": name, "slug": slug},
            lookup_fields=["slug"],
        )
        return extract_id(obj)

    def _ensure_cluster_group(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_cluster_group")
        slug = slugify(name)
        if dry_run:
            logger.info("[DRY-RUN] ensure_cluster_group name=%s", name)
            return None
        obj = self.nb.upsert(
            "virtualization.cluster_groups",
            {"name": name, "slug": slug},
            lookup_fields=["slug"],
        )
        return extract_id(obj)

    def _ensure_cluster(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_cluster")
        payload: dict[str, Any] = {"name": name}
        for key in ("type", "group", "site"):
            if args.get(key) is not None:
                payload[key] = args[key]
        if dry_run:
            logger.info("[DRY-RUN] ensure_cluster name=%s", name)
            return None
        obj = self.nb.upsert("virtualization.clusters", payload, lookup_fields=["name"])
        return extract_id(obj)

    def _ensure_inventory_item_role(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_inventory_item_role")
        slug = slugify(name)
        color = args.get("color", "9e9e9e")
        if dry_run:
            logger.info("[DRY-RUN] ensure_inventory_item_role name=%s", name)
            return None
        obj = self.nb.upsert(
            "dcim.inventory_item_roles",
            {"name": name, "slug": slug, "color": color},
            lookup_fields=["slug"],
        )
        return extract_id(obj)

    def _resolve_placement(
        self, args: dict, dry_run: bool
    ) -> dict[str, Any]:
        """Resolve site → location → rack chain.

        Returns a dict with keys: ``site_id``, ``location_id``, ``rack_id``,
        ``rack_position``.  Any key that could not be resolved is ``None``.
        """
        result: dict[str, Any] = {
            "site_id": None,
            "location_id": None,
            "rack_id": None,
            "rack_position": None,
        }

        site_name = args.get("site")
        location_name = args.get("location")
        rack_name = args.get("rack")
        position = args.get("position")

        if site_name:
            result["site_id"] = self._ensure_site({"name": site_name}, dry_run)

        if location_name and result["site_id"] is not None:
            result["location_id"] = self._ensure_location(
                {"name": location_name, "site": result["site_id"]}, dry_run
            )

        if rack_name and result["site_id"] is not None:
            result["rack_id"] = self._ensure_rack(
                {
                    "name": rack_name,
                    "site": result["site_id"],
                    "location": result["location_id"],
                },
                dry_run,
            )

        if position is not None and result["rack_id"] is not None:
            try:
                pos_int = int(position)
                if pos_int > 0:
                    result["rack_position"] = pos_int
            except (TypeError, ValueError):
                result["rack_position"] = position

        return result

    def _ensure_tenant_group(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        slug = slugify(name)
        payload: dict[str, Any] = {"name": name, "slug": slug}
        if args.get("description"):
            payload["description"] = args["description"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_tenant_group name=%s", name)
            return None
        obj = self.nb.upsert("tenancy.tenant_groups", payload, lookup_fields=["slug"])
        return extract_id(obj)

    def _ensure_contact_group(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        slug = slugify(name)
        payload: dict[str, Any] = {"name": name, "slug": slug}
        if args.get("description"):
            payload["description"] = args["description"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_contact_group name=%s", name)
            return None
        obj = self.nb.upsert("tenancy.contact_groups", payload, lookup_fields=["slug"])
        return extract_id(obj)

    def _ensure_region(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        slug = slugify(name)
        payload: dict[str, Any] = {"name": name, "slug": slug}
        if args.get("description"):
            payload["description"] = args["description"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_region name=%s", name)
            return None
        obj = self.nb.upsert("dcim.regions", payload, lookup_fields=["slug"])
        return extract_id(obj)

    def _ensure_vlan_group(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        slug = slugify(name)
        payload: dict[str, Any] = {
            "name": name,
            "slug": slug,
            "min_vid": args.get("min_vid", 1),
            "max_vid": args.get("max_vid", 4094),
        }
        if args.get("description"):
            payload["description"] = args["description"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_vlan_group name=%s", name)
            return None
        obj = self.nb.upsert("ipam.vlan_groups", payload, lookup_fields=["slug"])
        return extract_id(obj)

    def _ensure_vrf(self, args: dict, dry_run: bool) -> int | None:
        name = args.get("name")
        if not name:
            return None
        payload: dict[str, Any] = {"name": name}
        if args.get("rd"):
            payload["rd"] = args["rd"]
        if args.get("description"):
            payload["description"] = args["description"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_vrf name=%s", name)
            return None
        obj = self.nb.upsert("ipam.vrfs", payload, lookup_fields=["name"])
        return extract_id(obj)

    def _ensure_tenant(self, args: dict, dry_run: bool) -> int | None:
        name = require_text_arg(args, "name", "ensure_tenant")
        slug = slugify(name)
        payload: dict[str, Any] = {"name": name, "slug": slug}
        if args.get("description"):
            payload["description"] = args["description"]
        if args.get("group") is not None:
            payload["group"] = args["group"]
        if dry_run:
            logger.info("[DRY-RUN] ensure_tenant name=%s", name)
            return None
        obj = self.nb.upsert("tenancy.tenants", payload, lookup_fields=["slug"])
        return extract_id(obj)

    def _lookup_tenant(self, args: dict, dry_run: bool) -> int | None:
        """Read-only tenant lookup by name.  Never creates the tenant."""
        name = args.get("name")
        if not name:
            return None
        try:
            obj = self.nb.get("tenancy.tenants", name=name)
            return extract_id(obj)
        except Exception as exc:
            logger.debug("lookup_tenant name=%r: %s", name, exc)
            return None

    # ------------------------------------------------------------------
    # Module bay / module type helpers (used by engine._process_modules)
    # ------------------------------------------------------------------

    def _ensure_module_bay_template(self, args: dict, dry_run: bool) -> int | None:
        """Ensure a ModuleBayTemplate exists on a DeviceType."""
        device_type_id = args.get("device_type")
        name = require_text_arg(args, "name", "ensure_module_bay_template")
        position = args.get("position", "")
        if device_type_id is None:
            return None
        payload: dict[str, Any] = {"device_type": device_type_id, "name": name}
        if position:
            payload["position"] = position
        if dry_run:
            logger.info(
                "[DRY-RUN] ensure_module_bay_template device_type=%s name=%s",
                device_type_id, name,
            )
            return None
        obj = self.nb.upsert(
            "dcim.module_bay_templates",
            payload,
            lookup_fields=["device_type", "name"],
        )
        return extract_id(obj)

    def _ensure_module_bay(self, args: dict, dry_run: bool) -> int | None:
        """Ensure a ModuleBay exists on a Device."""
        device_id = args.get("device")
        name = require_text_arg(args, "name", "ensure_module_bay")
        position = args.get("position", "")
        if device_id is None:
            return None
        payload: dict[str, Any] = {"device": device_id, "name": name}
        if position:
            payload["position"] = position
        if dry_run:
            logger.info(
                "[DRY-RUN] ensure_module_bay device=%s name=%s",
                device_id, name,
            )
            return None
        obj = self.nb.upsert(
            "dcim.module_bays",
            payload,
            lookup_fields=["device", "name"],
        )
        return extract_id(obj)

    def _ensure_module_type_profile(self, args: dict, dry_run: bool) -> int | None:
        """Ensure a ModuleTypeProfile exists and return its numeric ID.

        *args* may contain:
          ``name``   — profile name (required)
          ``schema`` — JSON Schema dict attached to the profile so that NetBox
                       does not wipe ``attributes`` to NULL on every module-type
                       save.  When omitted and attribute names are provided via
                       ``attribute_names``, a minimal permissive schema is
                       auto-generated.
          ``attribute_names`` — list of attribute key names used to
                       auto-generate a schema when no explicit schema is given.

        The schema is applied via a dedicated ``nb.update`` (PATCH) call after
        the upsert so it is always written even when the profile already exists
        and the upsert would otherwise skip unchanged fields.
        """
        name = require_text_arg(args, "name", "ensure_module_type_profile")
        slug = slugify(name)
        schema: Any = args.get("schema")
        # Auto-generate a minimal schema from attribute names so that NetBox
        # retains ``attributes`` on every save (a profile with no schema causes
        # NetBox to wipe attributes to NULL).
        if schema is None:
            attr_names = args.get("attribute_names") or []
            if attr_names:
                schema = {
                    "type": "object",
                    "properties": {k: {} for k in attr_names},
                }
        if dry_run:
            logger.info("[DRY-RUN] ensure_module_type_profile name=%s", name)
            return None
        obj = self.nb.upsert(
            "dcim.module_type_profiles",
            {"name": name, "slug": slug},
            lookup_fields=["name"],
        )
        # Apply the schema in a separate PATCH after the upsert so it is
        # always written even when the profile record already existed.
        profile_id = extract_id(obj)
        if profile_id is not None and schema is not None:
            existing_schema = extract_field(obj, "schema")
            if existing_schema == schema:
                return profile_id
            try:
                self.nb.update("dcim.module_type_profiles", profile_id, {"schema": schema})
            except Exception as exc:
                logger.debug(
                    "Could not set schema on module_type_profile %r: %s", name, exc
                )
        return profile_id

    def _ensure_module_type(self, args: dict, dry_run: bool) -> int | None:
        """Ensure a ModuleType exists (model + optional manufacturer + optional profile).

        When *args* includes an ``attributes`` dict the values are applied to
        the module-type record via a dedicated PATCH **after** the profile has
        been committed.  NetBox validates attribute values against the profile's
        JSON Schema; sending both ``profile`` and ``attributes`` in a single
        request causes attributes to be silently ignored on some NetBox
        versions, so the two-step approach is mandatory.
        """
        model = require_text_arg(args, "model", "ensure_module_type")
        slug = slugify(model)
        manufacturer_id = args.get("manufacturer")
        profile_name = args.get("profile")
        attrs: dict[str, Any] = args.get("attributes") or {}
        payload: dict[str, Any] = {"model": model, "slug": slug}
        if manufacturer_id is not None:
            payload["manufacturer"] = manufacturer_id
        if profile_name is not None:
            attr_names = list(attrs.keys()) if attrs else []
            profile_id = self._ensure_module_type_profile(
                {"name": profile_name, "attribute_names": attr_names}, dry_run
            )
            if profile_id is not None:
                payload["profile"] = profile_id
        lookup = ["manufacturer", "model"] if manufacturer_id is not None else ["model"]
        if dry_run:
            logger.info(
                "[DRY-RUN] ensure_module_type model=%s manufacturer=%s profile=%s attributes=%s",
                model, manufacturer_id, profile_name, attrs,
            )
            return None
        # Step 1: create/update the module type with the profile assigned.
        # ``attributes`` is intentionally omitted here so that the profile is
        # committed to NetBox before attributes are applied in step 2.
        obj = self.nb.upsert("dcim.module_types", payload, lookup_fields=lookup)
        module_type_id = extract_id(obj)

        # Step 2: apply attributes via a direct PATCH after the profile has
        # been persisted.  Using ``nb.update`` (PATCH) rather than ``upsert``
        # ensures attributes are always written even when the type record
        # otherwise appears unchanged.
        if module_type_id and attrs:
            clean_attrs = {k: v for k, v in attrs.items() if v is not None}
            if clean_attrs:
                existing_attrs = extract_field(obj, "attributes")
                if existing_attrs == clean_attrs:
                    return module_type_id
                try:
                    self.nb.update(
                        "dcim.module_types", module_type_id, {"attributes": clean_attrs}
                    )
                except Exception as exc:
                    logger.debug(
                        "Could not set attributes on module_type %r: %s", model, exc
                    )

        return module_type_id
