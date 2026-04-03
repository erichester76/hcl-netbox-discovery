"""Field expression evaluator.

Expressions are plain Python strings evaluated with a restricted scope of
helper functions.  No arbitrary builtins are exposed — ``__builtins__`` is
replaced with an empty dict so that only the explicitly provided helpers are
callable.

Supported helpers
-----------------
source(path)
    Walk a dotted path on the current source object.  Handles both plain
    Python dicts (dict.get) and attribute objects (getattr) at each step.

    Path syntax:
      "a.b.c"           – nested attribute/key access
      "list[KEY]"       – filter a list for items matching KEY
                          • dicts: item has a key named KEY
                          • VMware objects: identifierType.key == KEY
      "list[*]"         – flatten/iterate all items in list

env(name, default="")
    get_config(name, default) – DB-backed runtime configuration is authoritative
    for non-startup settings and falls back to *default* when unset.

regex_file(value, filename)
    Apply pattern/replacement pairs from ``regex/<filename>`` to *value*.
    Each line in the file is ``pattern,replacement`` (CSV, first comma splits).
    Returns the first matched replacement, or *value* if nothing matches.

map_value(value, mapping, default=None)
    Dictionary lookup: mapping.get(value, default).

when(condition, true_val, false_val)
    Ternary: true_val if condition else false_val.

coalesce(*args)
    Return the first argument that is not None and not an empty string.
    String arguments that look like plain dotted paths (no parentheses or
    spaces) are treated as source() paths and resolved automatically.

replace(value, old, new)
    str.replace(old, new) on value.

upper(value) / lower(value)
    str.upper() / str.lower().

truncate(value, n)
    value[:n] as a string.

split(value, sep=None)
    value.split(sep) — returns a list of parts.  Use indexing to get a
    specific element, e.g. ``split(source('name'))[0]``.

join(sep, items)
    sep.join(str(i) for i in items if i) — skips falsy items.

to_gb(bytes_value)
    int(bytes_value / 1_073_741_824).

to_mb(kb_value)
    int(kb_value / 1024).

str(value)
    Convert *value* to a string (empty string for None).

int(value, default=0)
    Safely convert *value* to an integer, returning *default* on error.

regex_replace(value, pattern, replacement)
    Apply ``re.sub(pattern, replacement, str(value))``.

regex_extract(value, pattern, group=1)
    Return the captured group *group* from the first match of *pattern* in
    *value*.  Returns ``None`` when there is no match.  Useful for extracting
    tokens from vendor sysDescr strings without complex backreference escaping.

mask_to_prefix(mask)
    Convert a dotted-decimal IPv4 subnet mask (e.g. ``'255.255.255.0'``) to its
    CIDR prefix length integer (e.g. ``24``).  Returns ``None`` on any error.

prereq(name)
    Look up a resolved prerequisite by name.  Use dot notation to access
    attributes on multi-value prerequisites, e.g. prereq("placement.site_id").

getattr(obj, name, default=None)
    Safe attribute access — equivalent to Python's built-in getattr.  Useful
    in list comprehensions to filter objects by attribute presence, e.g.
    ``[d for d in source('devices') if getattr(d, 'capacityInKB', None)]``.

collector.<flag>
    Access a boolean/string flag from the collector {} block.
"""

from __future__ import annotations

import logging
import os
import re as _re
import types
from typing import Any

try:
    from .db import get_config as _get_config
except ImportError:
    def _get_config(key: str, default: str = "") -> str:  # type: ignore[misc]
        return default

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path walker
# ---------------------------------------------------------------------------

def _get_attr(obj: Any, key: str) -> Any:
    """Return obj[key] or getattr(obj, key), preferring dict access."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _matches_filter(item: Any, key: str) -> bool:
    """Return True if *item* matches the filter key.

    Rules:
    1. dicts — the key is present in the dict.
    2. VMware HostSystemIdentificationInfo pattern — item has ``identifierType``
       with a ``key`` attribute equal to *key*.
    3. Fallback — item has an attribute named *key*.
    """
    if isinstance(item, dict):
        return key in item
    id_type = getattr(item, "identifierType", None)
    if id_type is not None and getattr(id_type, "key", None) == key:
        return True
    return hasattr(item, key)


def _walk(obj: Any, parts: list) -> Any:
    """Recursively walk *parts* on *obj*."""
    if not parts:
        return obj
    if obj is None:
        return None

    part = parts[0]
    rest = parts[1:]

    # Bracket expression: "name[FILTER]"
    m = _re.match(r'^([^\[]*)\[([^\]]*)\]$', part)
    if m:
        list_key = m.group(1)
        filter_key = m.group(2)

        # Navigate to the container first if list_key is non-empty
        container = _get_attr(obj, list_key) if list_key else obj
        if container is None:
            return None

        if not hasattr(container, "__iter__") or isinstance(container, (str, bytes)):
            return None

        if filter_key == "*":
            items = list(container)
        else:
            items = [item for item in container if _matches_filter(item, filter_key)]

        if not items:
            return None

        if not rest:
            return items[0] if len(items) == 1 else items

        results = []
        for item in items:
            r = _walk(item, rest)
            if r is None:
                continue
            if isinstance(r, list):
                results.extend(r)      # flatten when parent was [*]
            else:
                results.append(r)

        if not results:
            return None
        return results[0] if len(results) == 1 else results

    # Normal single-step attribute access
    val = _get_attr(obj, part)
    if not rest:
        return val
    return _walk(val, rest)


def walk_path(obj: Any, path: str) -> Any:
    """Walk *path* on *obj*.  Exported for use by the engine's nested-item lookup."""
    if not path or obj is None:
        return None
    parts = path.split(".")
    return _walk(obj, parts)


# ---------------------------------------------------------------------------
# regex_file helper
# ---------------------------------------------------------------------------

def _apply_regex_file(value: Any, regex_dir: str, filename: str) -> Any:
    if not isinstance(value, str):
        value = str(value) if value is not None else ""
    filepath = os.path.join(regex_dir, filename)
    try:
        with open(filepath) as fh:
            patterns = [
                tuple(line.strip().split(",", 1))
                for line in fh
                if "," in line and not line.startswith("#")
            ]
    except FileNotFoundError:
        logger.warning("regex_file: file not found: %s", filepath)
        return value
    except OSError as exc:
        logger.warning("regex_file: error reading %s: %s", filepath, exc)
        return value

    for pattern, replacement in patterns:
        new_val = _re.sub(pattern.strip(), replacement.strip(), value)
        if new_val != value:
            return new_val

    return value


# ---------------------------------------------------------------------------
# Resolver
# ---------------------------------------------------------------------------

class Resolver:
    """Evaluate field expressions in a controlled Python scope."""

    def __init__(self, context: Any) -> None:
        self._ctx = context
        self._scope = self._build_scope()

    def evaluate(self, expression: Any) -> Any:
        """Evaluate *expression* and return the result.

        Non-string values are returned as-is.  Evaluation failures are logged
        at DEBUG level and ``None`` is returned so that the calling field is
        silently skipped rather than crashing the item.
        """
        if not isinstance(expression, str):
            return expression
        try:
            return eval(expression, {"__builtins__": {}}, self._scope)  # noqa: S307
        except Exception as exc:
            logger.debug("Expression eval failed %r: %s", expression, exc)
            return None

    def evaluate_strict(self, expression: Any, label: str = "expression") -> Any:
        """Evaluate *expression* and raise on evaluation errors.

        Non-string values are returned as-is. Unlike :meth:`evaluate`, this
        method preserves evaluation errors so callers can fail loud for
        identity-critical fields.
        """
        if not isinstance(expression, str):
            return expression
        try:
            return eval(expression, {"__builtins__": {}}, self._scope)  # noqa: S307
        except Exception as exc:
            raise ValueError(f"{label} evaluation failed: {exc}") from exc

    def _build_scope(self) -> dict:
        ctx = self._ctx
        opts = ctx.collector_opts

        # ---- source() ----
        def source(path: str) -> Any:
            return walk_path(ctx.source_obj, path)

        # ---- env() ----
        def env(name: str, default: str = "") -> str:
            return _get_config(name, default)

        # ---- regex_file() ----
        def regex_file(value: Any, filename: str) -> Any:
            return _apply_regex_file(value, ctx.regex_dir, filename)

        # ---- map_value() ----
        def map_value(value: Any, mapping: dict, default: Any = None) -> Any:
            return mapping.get(value, default)

        # ---- when() ----
        def when(condition: Any, true_val: Any, false_val: Any) -> Any:
            return true_val if condition else false_val

        # ---- coalesce() ----
        def coalesce(*args: Any) -> Any:
            for arg in args:
                # Plain path strings (no parens/spaces) are auto-resolved via source()
                if isinstance(arg, str) and not any(c in arg for c in "() "):
                    val = walk_path(ctx.source_obj, arg)
                else:
                    val = arg
                if val is not None and val != "" and val != []:
                    return val
            return None

        # ---- string helpers ----
        def replace(value: Any, old: str, new: str) -> Any:
            if not isinstance(value, str):
                return value
            return value.replace(old, new)

        def upper(value: Any) -> str | None:
            return str(value).upper() if value is not None else None

        def lower(value: Any) -> str | None:
            return str(value).lower() if value is not None else None

        def truncate(value: Any, n: int) -> str | None:
            return str(value)[:n] if value is not None else None

        def split(value: Any, sep: str | None = None) -> list:
            if value is None:
                return []
            return str(value).split(sep)

        def join(sep: str, items: Any) -> str:
            if not hasattr(items, "__iter__") or isinstance(items, str):
                return str(items) if items else ""
            return sep.join(str(i) for i in items if i)

        # ---- numeric helpers ----
        def to_gb(bytes_value: Any) -> int | None:
            if bytes_value is None:
                return None
            try:
                return int(int(bytes_value) / 1_073_741_824)
            except (TypeError, ValueError):
                return None

        def to_mb(kb_value: Any) -> int | None:
            if kb_value is None:
                return None
            try:
                return int(int(kb_value) / 1024)
            except (TypeError, ValueError):
                return None

        # ---- type conversion helpers ----
        def str_val(value: Any) -> str:
            """Convert *value* to a string (empty string for None)."""
            return str(value) if value is not None else ""

        def int_val(value: Any, default: int = 0) -> int:
            """Safely convert *value* to an integer, returning *default* on error."""
            try:
                return int(value)
            except (TypeError, ValueError):
                return default

        def float_val(value: Any, default: float = 0.0) -> float:
            """Safely convert *value* to a float, returning *default* on error."""
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        # ---- regex helpers ----
        def regex_replace(value: Any, pattern: str, replacement: str) -> str:
            """Apply a regex substitution to *value*.

            Equivalent to ``re.sub(pattern, replacement, str(value))``.
            """
            if value is None:
                return ""
            return _re.sub(pattern, replacement, str(value))

        def regex_extract(value: Any, pattern: str, group: int = 1) -> str | None:
            """Return a captured group from the first regex match in *value*.

            *group* selects the capture group (default: 1).  Returns ``None``
            when *value* is ``None``, when there is no match, or when the
            requested group does not exist.  Useful in HCL to extract
            vendor-specific tokens from free-text fields such as ``sysDescr``
            without needing backslash-heavy replacement strings.

            Example (extract Juniper model from sysDescr)::

                regex_extract(source('description'),
                              '(?i)Juniper Networks.+?Inc\\\\. (\\\\S+)')
            """
            if value is None:
                return None
            m = _re.search(pattern, str(value))
            if not m:
                return None
            try:
                return m.group(group)
            except IndexError:
                return None

        # ---- network helpers ----
        def mask_to_prefix(mask: Any) -> int | None:
            """Convert a dotted-decimal IPv4 subnet mask to a CIDR prefix length.

            For example ``'255.255.255.0'`` → ``24``.  Returns ``None`` when
            *mask* is ``None`` or cannot be parsed.
            """
            if mask is None:
                return None
            import ipaddress as _ipaddress
            try:
                return _ipaddress.IPv4Network(f"0.0.0.0/{mask}").prefixlen
            except Exception:
                return None

        # ---- prereq() ----
        def prereq(name: str) -> Any:
            parts = name.split(".", 1)
            val = ctx.prereqs.get(parts[0])
            if len(parts) == 2:
                if isinstance(val, dict):
                    return val.get(parts[1])
                return getattr(val, parts[1], None)
            return val

        # ---- collector namespace ----
        col_attrs = {
            "max_workers": opts.max_workers,
            "dry_run": opts.dry_run,
            "sync_tag": opts.sync_tag,
            "regex_dir": opts.regex_dir,
        }
        col_attrs.update(opts.extra_flags)
        collector_ns = types.SimpleNamespace(**col_attrs)

        return {
            # Helpers
            "source": source,
            "env": env,
            "regex_file": regex_file,
            "regex_replace": regex_replace,
            "regex_extract": regex_extract,
            "mask_to_prefix": mask_to_prefix,
            "map_value": map_value,
            "when": when,
            "coalesce": coalesce,
            "replace": replace,
            "upper": upper,
            "lower": lower,
            "truncate": truncate,
            "split": split,
            "join": join,
            "to_gb": to_gb,
            "to_mb": to_mb,
            "str": str_val,
            "int": int_val,
            "float": float_val,
            "prereq": prereq,
            "collector": collector_ns,
            # Attribute access helper (safe: only reads attributes, no side-effects)
            "getattr": getattr,
            # Safe literals
            "None": None,
            "True": True,
            "False": False,
            "true": True,
            "false": False,
        }
