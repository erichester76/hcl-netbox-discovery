"""Generic LDAP data source adapter.

Connects to an LDAP / Active Directory server using ``ldap3`` and returns
directory entries as plain Python dicts with LDAP attribute names as keys.

Supported collections
---------------------
Any name passed to ``get_objects()`` is accepted — the name is a label used
in the HCL ``source_collection`` attribute and has no effect on the search
behaviour.  All search behaviour is driven by the ``extra`` config values in
the ``source {}`` HCL block.

Source HCL block example::

    source "ldap" {
      api_type      = "ldap"
      url           = env("LDAP_SERVER")          # e.g. ldaps://ldap.example.com
      username      = env("LDAP_USER")
      password      = env("LDAP_PASS")
      verify_ssl    = true

      # Extra configuration
      search_base   = env("LDAP_SEARCH_BASE")
      search_filter = env("LDAP_FILTER", "(objectClass=*)")
      # Comma-separated list of LDAP attributes to fetch.
      # Omit (or leave empty) to fetch all non-operational attributes ("*").
      attributes    = "cn,mail,memberOf"
    }

Each returned dict has LDAP attribute names as keys.  Single-value attributes
are returned as strings; multi-value attributes are returned as lists of
strings.  Absent or empty attributes are returned as empty strings.

Field mapping and any schema-specific normalisation should be done in the HCL
``object {}`` block using the expression helpers (``source()``, ``when()``,
``regex_replace()``, ``upper()``, ``truncate()``, etc.).  See
``mappings/jnsu.hcl`` for a worked example using the Novell eDirectory /
Micro Focus IDM DHCP-lease schema.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)


def _entry_to_dict(entry: Any) -> dict:
    """Convert a single ldap3 Entry into a plain Python dict.

    Single-value attributes are returned as strings.
    Multi-value attributes are returned as lists of strings.
    Absent or empty attributes are returned as empty strings.

    Falls back to ``vars()`` for non-ldap3 objects (e.g. test mocks).
    """
    # Prefer ldap3's own attribute list; fall back to instance dict keys
    if hasattr(entry, "entry_attributes"):
        attr_names = entry.entry_attributes
    else:
        attr_names = [k for k in vars(entry) if not k.startswith("_")]

    result: dict = {}
    for attr_name in attr_names:
        try:
            val = getattr(entry, attr_name, None)
            if val is None:
                result[attr_name] = ""
                continue
            # ldap3 Attribute objects expose a .values list
            if hasattr(val, "values"):
                raw_values = [str(v) for v in val.values if str(v).strip()]
            elif hasattr(val, "__iter__") and not isinstance(val, str):
                raw_values = [str(v) for v in val if str(v).strip()]
            else:
                raw_values = [str(val)] if str(val).strip() else []

            if len(raw_values) == 0:
                result[attr_name] = ""
            elif len(raw_values) == 1:
                result[attr_name] = raw_values[0]
            else:
                result[attr_name] = raw_values
        except Exception:
            result[attr_name] = ""
    return result


# ---------------------------------------------------------------------------
# LDAPSource
# ---------------------------------------------------------------------------

class LDAPSource(DataSource):
    """ldap3-backed generic source adapter for LDAP/AD directories."""

    def __init__(self) -> None:
        self._conn: Optional[Any] = None
        self._config: Optional[Any] = None

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to the LDAP server using settings from *config*."""
        try:
            import ldap3  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "ldap3 is required for the LDAP source adapter. "
                "Install it with: pip install ldap3"
            ) from exc

        self._config = config
        url = config.url or ""
        if not url:
            raise ValueError("LDAPSource: 'url' must be set to the LDAP server URI")

        logger.info("Connecting to LDAP: %s", url)
        server = ldap3.Server(url, get_info=ldap3.ALL)
        self._conn = ldap3.Connection(
            server,
            user=config.username,
            password=config.password,
            auto_bind=True,
        )
        logger.info("LDAP connection established: %s", url)

    def get_objects(self, collection: str) -> list:
        """Return LDAP entries for *collection* as raw attribute dicts.

        The *collection* name is used only as a label; all search behaviour is
        driven by the ``extra`` values in the source HCL block (``search_base``,
        ``search_filter``, ``attributes``).
        """
        if self._conn is None:
            raise RuntimeError("LDAPSource: connect() has not been called")
        return self._get_entries()

    def close(self) -> None:
        """Close the LDAP connection."""
        if self._conn is not None:
            try:
                self._conn.unbind()
            except Exception as exc:
                logger.debug("LDAP unbind error: %s", exc)
            finally:
                self._conn = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_entries(self) -> list[dict]:
        """Perform the configured LDAP search and return entries as dicts."""
        extra = self._config.extra if self._config else {}
        search_base   = extra.get("search_base", "")
        search_filter = extra.get("search_filter", "(objectClass=*)")
        attrs_raw     = extra.get("attributes", "")

        if not search_base:
            raise ValueError(
                "LDAPSource: 'search_base' must be set in source.extra "
                "(e.g. search_base = env('LDAP_SEARCH_BASE'))"
            )

        # Parse comma-separated attribute list; default to all non-operational attrs
        if attrs_raw:
            attributes: Any = [a.strip() for a in str(attrs_raw).split(",") if a.strip()]
        else:
            attributes = ["*"]

        logger.info("LDAP search base=%s filter=%s", search_base, search_filter)
        self._conn.search(
            search_base=search_base,
            search_filter=search_filter,
            attributes=attributes,
        )
        entries = self._conn.entries
        logger.debug("LDAP: %d raw entries retrieved", len(entries))

        records = [_entry_to_dict(e) for e in entries]
        logger.debug("LDAP: returning %d records", len(records))
        return records
