"""LDAP data source adapter.

Connects to an LDAP / Active Directory server using ``ldap3`` and returns
DHCP-lease records as plain Python dicts.

Supported collection
--------------------
``"dhcp_leases"`` — returns IP-address records sourced from DHCP / static
registrations in a Novell eDirectory / Micro Focus IDM directory.

Source HCL block example::

    source "ldap" {
      api_type  = "ldap"
      url       = env("LDAP_SERVER")          # e.g. ldaps://ldap.example.com
      username  = env("LDAP_USER")
      password  = env("LDAP_PASS")
      verify_ssl = true

      # Extra configuration
      search_base   = env("LDAP_SEARCH_BASE")
      search_filter = env("LDAP_FILTER", "(DirXMLjnsuDHCPAddress=*)")
      skip_aps      = "true"
    }

Each returned dict has the following fields:

  address       IP address string.  A prefix length is appended when
                ``default_prefix_length`` is set in source.extra, e.g.
                ``"10.20.30.100/24"``.  Otherwise the bare IP is returned.
  description   Formatted description string (max 64 chars).  Static entries
                use the device description directly; DHCP entries prepend the
                user UPN derived from the distinguished name.
  status        ``"dhcp"`` for DHCP leases, ``"active"`` for static entries.
  mac_address   Uppercase MAC address or empty string.
  device_name   Device name from the directory attribute.
  lease_type    ``"Static"`` or ``"Registered"`` (informational).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)

# Default LDAP attributes to request
_DEFAULT_ATTRIBUTES = [
    "DirXMLjnsuDHCPAddress",
    "DirXMLjnsuDeviceName",
    "DirXMLjnsuHWAddress",
    "DirXMLjnsuDescription",
    "DirXMLjnsuUserDN",
    "DirXMLJnsuDisabled",
    "DirXMLjnsuStaticAddrs",
]

# Patterns for AP detection (access points should be skipped)
_AP_PATTERNS = [re.compile(r"-ap", re.IGNORECASE), re.compile(r"\bWAP\b")]

# Pattern to extract UPN from LDAP distinguished name
_UPN_PATTERN = re.compile(r"^cn=(.+),ou=.+$", re.IGNORECASE)


def _is_ap(description: str) -> bool:
    """Return True if *description* indicates an access point entry."""
    return any(p.search(description) for p in _AP_PATTERNS)


def _format_description(user_dn: str, description: str, lease_type: str) -> str:
    """Return a sanitised description string (max 64 chars)."""
    if lease_type == "Static":
        full = description
    else:
        match = _UPN_PATTERN.match(user_dn or "")
        if match:
            upn = f"{match.group(1)}@clemson.edu".upper()
        else:
            upn = (user_dn or "unknown").upper()
        full = f"{upn}: {description}"

    # Remove newlines and excess whitespace
    full = re.sub(r"[\r\n]|[ ]{2,}", " ", full)
    # Remove "Connected to " prefix
    full = re.sub(r"^Connected to ", "", full)
    return full[:64]


def _attr(entry: Any, name: str) -> str:
    """Return string value of an ldap3 entry attribute (safe)."""
    try:
        val = getattr(entry, name, None)
        if val is None:
            return ""
        return str(val)
    except Exception:
        return ""


def _is_static(entry: Any) -> bool:
    """Return True if the LDAP entry represents a static (not DHCP) lease."""
    try:
        static_addrs = getattr(entry, "DirXMLjnsuStaticAddrs", None)
        if static_addrs is None:
            return False
        # ldap3 represents multi-value attributes as lists
        values = list(static_addrs) if hasattr(static_addrs, "__iter__") and not isinstance(static_addrs, str) else [static_addrs]
        return any(str(v).strip() for v in values)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# LDAPSource
# ---------------------------------------------------------------------------

class LDAPSource(DataSource):
    """ldap3-backed source adapter for LDAP/AD directories."""

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
        """Return records for *collection*."""
        if self._conn is None:
            raise RuntimeError("LDAPSource: connect() has not been called")

        collectors = {
            "dhcp_leases": self._get_dhcp_leases,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"LDAPSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

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
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_dhcp_leases(self) -> list[dict]:
        """Fetch and normalise DHCP/static lease records from LDAP."""
        extra = self._config.extra if self._config else {}
        search_base   = extra.get("search_base", "")
        search_filter = extra.get("search_filter", "(DirXMLjnsuDHCPAddress=*)")
        skip_aps_raw  = extra.get("skip_aps", "true")
        skip_aps      = str(skip_aps_raw).strip().lower() in ("true", "1", "yes")
        prefix_length = extra.get("default_prefix_length", "")

        if not search_base:
            raise ValueError(
                "LDAPSource: 'search_base' must be set in source.extra "
                "(e.g. search_base = env('LDAP_SEARCH_BASE'))"
            )

        logger.info("LDAP search base=%s filter=%s", search_base, search_filter)
        self._conn.search(
            search_base=search_base,
            search_filter=search_filter,
            attributes=_DEFAULT_ATTRIBUTES,
        )
        entries = self._conn.entries
        logger.debug("LDAP: %d raw entries retrieved", len(entries))

        records: list[dict] = []
        for entry in entries:
            record = self._normalise_entry(entry, prefix_length, skip_aps)
            if record is not None:
                records.append(record)

        logger.debug("LDAP: returning %d normalised records", len(records))
        return records

    def _normalise_entry(
        self, entry: Any, prefix_length: str, skip_aps: bool
    ) -> Optional[dict]:
        """Convert a single ldap3 entry into a plain dict, or None to skip."""
        ip         = _attr(entry, "DirXMLjnsuDHCPAddress")
        device_name = _attr(entry, "DirXMLjnsuDeviceName")
        mac_raw    = _attr(entry, "DirXMLjnsuHWAddress")
        description = _attr(entry, "DirXMLjnsuDescription")
        user_dn    = _attr(entry, "DirXMLjnsuUserDN")

        if not ip:
            return None

        if skip_aps and description and _is_ap(description):
            logger.debug("Skipping AP entry: %s", description)
            return None

        lease_type = "Static" if _is_static(entry) else "Registered"
        address = f"{ip}/{prefix_length}" if prefix_length else ip
        formatted_desc = _format_description(user_dn, description, lease_type)
        mac = mac_raw.upper() if mac_raw else ""

        return {
            "address":      address,
            "description":  formatted_desc,
            "status":       "active" if lease_type == "Static" else "dhcp",
            "mac_address":  mac,
            "device_name":  device_name,
            "lease_type":   lease_type,
            # Raw passthrough
            "raw_ip":        ip,
            "raw_user_dn":   user_dn,
        }
