"""Helpers for deriving safe NetBox cache namespaces."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlsplit

from .job_lifecycle import get_code_version

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _normalize_branch_name(branch: str | None) -> str:
    value = (branch or "unknown").strip().lower()
    value = _NON_ALNUM_RE.sub("-", value).strip("-")
    return value or "unknown"


def _normalize_url_scope(url: str | None) -> str:
    if not url:
        return "no-url"
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    hostname = parts.hostname or ""
    path = parts.path.rstrip("/")

    port = parts.port
    default_ports = {"http": 80, "https": 443}
    if port is None or default_ports.get(scheme) == port:
        netloc = hostname
    else:
        netloc = f"{hostname}:{port}"

    normalized = f"{scheme}://{netloc}{path}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def build_effective_cache_key_prefix(
    base_prefix: str,
    *,
    netbox_url: str,
    git_branch: str | None = None,
) -> str:
    """Return the effective cache key prefix for the current runtime context."""

    prefix = base_prefix or "nbx:"
    if not prefix.endswith(":"):
        prefix = f"{prefix}:"

    branch = git_branch or get_code_version().get("git_branch")
    return f"{prefix}{_normalize_branch_name(branch)}:{_normalize_url_scope(netbox_url)}:"
