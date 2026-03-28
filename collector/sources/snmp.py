"""SNMP data source adapter for network device discovery.

Connects to one or more network devices via SNMP v2c or v3 and returns
device and interface information as plain Python dicts.

Supported collection
--------------------
``"devices"`` — returns one record per polled host.  Each device dict
contains a nested ``interfaces`` list; each interface dict contains a
nested ``ip_addresses`` list.

Source HCL block example (SNMPv2c)::

    source "juniper" {
      api_type  = "snmp"
      url       = env("SNMP_HOSTS")                  # comma-separated host list
      username  = env("SNMP_COMMUNITY", "public")    # community string
      verify_ssl = false

      # Optional overrides (all go into source.extra)
      version  = env("SNMP_VERSION", "2c")   # 1 | 2c | 3
      port     = env("SNMP_PORT", "161")
      timeout  = env("SNMP_TIMEOUT", "5")
      retries  = env("SNMP_RETRIES", "1")
    }

Source HCL block example (SNMPv3)::

    source "juniper" {
      api_type       = "snmp"
      url            = env("SNMP_HOSTS")
      username       = env("SNMP_V3_USER")
      password       = env("SNMP_V3_AUTH_PASS")
      verify_ssl     = false

      version        = "3"
      auth_protocol  = env("SNMP_V3_AUTH_PROTO", "sha")   # md5|sha|sha256|sha384|sha512
      priv_protocol  = env("SNMP_V3_PRIV_PROTO", "aes")   # des|aes|aes128|aes192|aes256
      priv_password  = env("SNMP_V3_PRIV_PASS", "")
    }

Device dict fields
------------------
  host           The polled host (IP or hostname).
  name           sysName (falls back to *host* when empty).
  description    sysDescr.
  location       sysLocation.
  contact        sysContact.
  serial         Serial number (jnxBoxSerialNo for Juniper, empty otherwise).
  model          Model string (parsed from jnxBoxDescr / sysDescr).
  os_version     OS version string (parsed from sysDescr, e.g. "20.4R3").
  platform       Platform name, e.g. "Junos 20.4R3".
  manufacturer   "Juniper Networks" for Juniper devices, empty otherwise.
  interfaces     List of interface dicts (see below).

Interface dict fields
---------------------
  index          ifIndex integer.
  name           ifName (falls back to ifDescr).
  label          ifAlias (operator-assigned description).
  type           NetBox-compatible interface type slug.
  mac_address    MAC address string (uppercase colon-separated), or "".
  admin_status   "up" | "down" | "testing".
  oper_status    "up" | "down" | "testing" | "unknown" | "dormant" |
                 "notPresent" | "lowerLayerDown".
  speed          Speed in Mbps (from ifHighSpeed; falls back to ifSpeed/1e6).
  mtu            MTU integer.
  ip_addresses   List of IP address dicts (see below).

IP address dict fields
----------------------
  address        "A.B.C.D/<prefixlen>".
  family         4 (IPv4).
  status         "active".
  if_index       ifIndex of the owning interface (used internally for
                 correlation; not usually mapped into NetBox directly).
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import re
from typing import Any, Optional

from .base import DataSource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SNMP OID constants
# ---------------------------------------------------------------------------

# System MIB (RFC 1213 / SNMPv2-MIB)
_OID_SYS_DESCR     = "1.3.6.1.2.1.1.1.0"
_OID_SYS_OBJECT_ID = "1.3.6.1.2.1.1.2.0"
_OID_SYS_CONTACT   = "1.3.6.1.2.1.1.4.0"
_OID_SYS_NAME      = "1.3.6.1.2.1.1.5.0"
_OID_SYS_LOCATION  = "1.3.6.1.2.1.1.6.0"

# Juniper enterprise OIDs (JUNIPER-MIB)
_OID_JNX_BOX_DESCR  = "1.3.6.1.4.1.2636.3.1.2.0"
_OID_JNX_BOX_SERIAL = "1.3.6.1.4.1.2636.3.1.3.0"

# IF-MIB — ifTable (RFC 2863)
_OID_IF_DESCR        = "1.3.6.1.2.1.2.2.1.2"   # ifDescr
_OID_IF_TYPE         = "1.3.6.1.2.1.2.2.1.3"   # ifType
_OID_IF_MTU          = "1.3.6.1.2.1.2.2.1.4"   # ifMtu
_OID_IF_SPEED        = "1.3.6.1.2.1.2.2.1.5"   # ifSpeed (bps, 32-bit gauge)
_OID_IF_PHYS_ADDR    = "1.3.6.1.2.1.2.2.1.6"   # ifPhysAddress
_OID_IF_ADMIN_STATUS = "1.3.6.1.2.1.2.2.1.7"   # ifAdminStatus
_OID_IF_OPER_STATUS  = "1.3.6.1.2.1.2.2.1.8"   # ifOperStatus

# IF-MIB — ifXTable (RFC 2863)
_OID_IF_NAME       = "1.3.6.1.2.1.31.1.1.1.1"   # ifName
_OID_IF_ALIAS      = "1.3.6.1.2.1.31.1.1.1.18"  # ifAlias
_OID_IF_HIGH_SPEED = "1.3.6.1.2.1.31.1.1.1.15"  # ifHighSpeed (Mbps, 64-bit)

# IP Address Table — ipAddrTable (RFC 1213)
_OID_IP_ADDR    = "1.3.6.1.2.1.4.20.1.1"  # ipAdEntAddr
_OID_IP_IF_IDX  = "1.3.6.1.2.1.4.20.1.2"  # ipAdEntIfIndex
_OID_IP_NETMASK = "1.3.6.1.2.1.4.20.1.3"  # ipAdEntNetMask

# Juniper OID tree prefix (used to detect Juniper devices from sysObjectID)
_JNX_OID_PREFIX = "1.3.6.1.4.1.2636"

# ---------------------------------------------------------------------------
# Value mappings
# ---------------------------------------------------------------------------

_ADMIN_STATUS: dict[int, str] = {1: "up", 2: "down", 3: "testing"}
_OPER_STATUS: dict[int, str] = {
    1: "up",
    2: "down",
    3: "testing",
    4: "unknown",
    5: "dormant",
    6: "notPresent",
    7: "lowerLayerDown",
}

# SNMP ifType integer → NetBox interface type slug (RFC 2863 / IANAifType)
_IFTYPE_MAP: dict[int, str] = {
    1:   "other",
    6:   "other",    # ethernetCsmacd — use name/speed for finer mapping
    24:  "virtual",  # softwareLoopback
    53:  "virtual",  # propVirtual
    131: "virtual",  # tunnel
    161: "lag",      # ieee8023adLag
    166: "virtual",  # mpls
}

# Juniper interface-name prefix → NetBox type slug (ordered, first match wins)
_JNX_PREFIX_TYPE: list[tuple[str, str]] = [
    ("ge-",  "1000base-t"),
    ("xe-",  "10gbase-x-sfpp"),
    ("et-",  "100gbase-x-cfp"),
    ("fe-",  "100base-tx"),
    ("ae",   "lag"),
    ("reth", "lag"),
    ("lo",   "virtual"),
    ("irb",  "virtual"),
    ("st0",  "virtual"),
    ("vlan", "virtual"),
    ("pp",   "virtual"),
    ("vme",  "virtual"),
    ("me",   "1000base-t"),
    ("fxp",  "1000base-t"),
    ("em",   "1000base-t"),
]

# ---------------------------------------------------------------------------
# Regex patterns for Juniper sysDescr parsing
# ---------------------------------------------------------------------------

_RE_JNX_MODEL   = re.compile(r"Juniper\s+Networks,\s+Inc\.\s+(\S+)", re.IGNORECASE)
_RE_JUNOS_VER   = re.compile(r"kernel\s+JUNOS\s+(\S+)", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Pure helper functions (testable without SNMP)
# ---------------------------------------------------------------------------


def _netmask_to_prefixlen(netmask: str) -> int:
    """Convert a dotted-decimal subnet mask to a CIDR prefix length."""
    try:
        return ipaddress.IPv4Network(f"0.0.0.0/{netmask}", strict=False).prefixlen
    except Exception:
        return 32


def _mac_bytes_to_str(raw: str) -> str:
    """Convert a raw MAC value returned by pysnmp to ``XX:XX:XX:XX:XX:XX``.

    pysnmp represents ``OctetString`` as a hex-prefixed string such as
    ``0x001122aabbcc`` when the bytes are not printable ASCII.
    """
    if not raw:
        return ""
    cleaned = raw.replace("0x", "").replace(":", "").replace("-", "").replace(" ", "")
    if len(cleaned) == 12 and all(c in "0123456789abcdefABCDEF" for c in cleaned):
        return ":".join(cleaned[i : i + 2] for i in range(0, 12, 2)).upper()
    return ""


def _if_type_to_netbox(if_type_int: int, if_name: str) -> str:
    """Return the NetBox interface type slug for *if_name* / *if_type_int*.

    Juniper interface names are checked first (most specific); the generic
    SNMP ifType map is used as a fallback.
    """
    name_lower = if_name.lower()
    for prefix, nb_type in _JNX_PREFIX_TYPE:
        if name_lower.startswith(prefix):
            return nb_type
    return _IFTYPE_MAP.get(if_type_int, "other")


def _parse_juniper_descr(sys_descr: str) -> tuple[str, str]:
    """Extract (model, os_version) from a Juniper sysDescr string.

    Returns empty strings when the pattern is not found.
    """
    model = ""
    os_version = ""
    m = _RE_JNX_MODEL.search(sys_descr)
    if m:
        model = m.group(1)
    v = _RE_JUNOS_VER.search(sys_descr)
    if v:
        os_version = v.group(1)
    return model, os_version


def _rows_by_index(rows: list[tuple[str, str]]) -> dict[int, str]:
    """Convert ``[(oid, value)]`` walk results into ``{ifIndex: value}``."""
    result: dict[int, str] = {}
    for oid, val in rows:
        try:
            idx = int(oid.rsplit(".", 1)[-1])
            result[idx] = val
        except (ValueError, IndexError):
            pass
    return result


def _ip_suffix(oid: str, base_oid: str) -> str:
    """Return the IP-address suffix of *oid* relative to *base_oid*.

    For ipAddrTable the suffix is the dotted-decimal IP address itself,
    e.g. ``"10.1.1.1"``.
    """
    prefix = base_oid + "."
    return oid[len(prefix):] if oid.startswith(prefix) else ""


# ---------------------------------------------------------------------------
# SNMPSource
# ---------------------------------------------------------------------------


class SNMPSource(DataSource):
    """pysnmp-backed source adapter for SNMP-capable network devices.

    Each host in the comma-separated ``url`` field is polled concurrently
    via asyncio; the results are returned as a flat list of device dicts.
    """

    def __init__(self) -> None:
        self._config: Optional[Any] = None
        self._hosts: list[str] = []
        # SNMPv1/v2c
        self._community: str = "public"
        # Common
        self._version: str = "2c"
        self._port: int = 161
        self._timeout: int = 5
        self._retries: int = 1
        # SNMPv3
        self._v3_user: str = ""
        self._v3_auth_pass: str = ""
        self._v3_auth_proto: str = "sha"
        self._v3_priv_proto: str = "aes"
        self._v3_priv_pass: str = ""

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Store SNMP connection parameters from *config*."""
        try:
            import pysnmp.hlapi.v3arch.asyncio  # noqa: F401 — verify availability
        except ImportError as exc:
            raise RuntimeError(
                "pysnmp is required for the SNMP source adapter. "
                "Install it with: pip install 'pysnmp>=7.1'"
            ) from exc

        self._config = config
        hosts_str = (config.url or "").strip()
        if not hosts_str:
            raise ValueError(
                "SNMPSource: 'url' must be set to a comma-separated list of "
                "host names / IP addresses to poll"
            )
        self._hosts = [h.strip() for h in hosts_str.split(",") if h.strip()]

        extra: dict = config.extra or {}
        self._version = str(extra.get("version", "2c")).strip().lower()
        self._port = int(extra.get("port", 161))
        self._timeout = int(extra.get("timeout", 5))
        self._retries = int(extra.get("retries", 1))

        if self._version in ("1", "2c", "2"):
            self._community = config.username or "public"
        else:
            # SNMPv3
            self._v3_user = config.username or ""
            self._v3_auth_pass = config.password or ""
            self._v3_auth_proto = str(extra.get("auth_protocol", "sha")).lower()
            self._v3_priv_proto = str(extra.get("priv_protocol", "aes")).lower()
            self._v3_priv_pass = str(extra.get("priv_password", ""))

        logger.info(
            "SNMPSource: configured %d host(s), version=%s port=%d",
            len(self._hosts),
            self._version,
            self._port,
        )

    def get_objects(self, collection: str) -> list:
        """Return records for *collection*."""
        if self._config is None:
            raise RuntimeError("SNMPSource: connect() has not been called")

        collectors: dict[str, Any] = {
            "devices": self._get_devices,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"SNMPSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Nothing to release for SNMP (stateless UDP queries)."""
        self._config = None

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_devices(self) -> list[dict]:
        """Poll all configured hosts and return a list of device dicts."""
        return asyncio.run(self._collect_all_hosts())

    # ------------------------------------------------------------------
    # Async orchestration
    # ------------------------------------------------------------------

    async def _collect_all_hosts(self) -> list[dict]:
        """Concurrently collect device data from every configured host."""
        tasks = [self._collect_host(host) for host in self._hosts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        devices: list[dict] = []
        for host, result in zip(self._hosts, results):
            if isinstance(result, Exception):
                logger.error("SNMP collection failed for %s: %s", host, result)
            elif result is not None:
                devices.append(result)
        return devices

    async def _collect_host(self, host: str) -> Optional[dict]:
        """Collect complete device information for a single host."""
        try:
            sys_info = await self._snmp_get_multi(
                host,
                [
                    _OID_SYS_DESCR,
                    _OID_SYS_OBJECT_ID,
                    _OID_SYS_CONTACT,
                    _OID_SYS_NAME,
                    _OID_SYS_LOCATION,
                ],
            )
        except Exception as exc:
            logger.error("SNMP system-info GET failed for %s: %s", host, exc)
            return None

        descr    = sys_info.get(_OID_SYS_DESCR, "")
        obj_id   = sys_info.get(_OID_SYS_OBJECT_ID, "")
        sys_name = sys_info.get(_OID_SYS_NAME, "") or host
        location = sys_info.get(_OID_SYS_LOCATION, "")
        contact  = sys_info.get(_OID_SYS_CONTACT, "")

        is_juniper = _JNX_OID_PREFIX in str(obj_id)

        model, os_version = _parse_juniper_descr(descr)
        serial       = ""
        manufacturer = ""
        platform     = ""

        if is_juniper:
            manufacturer = "Juniper Networks"
            platform = f"Junos {os_version}".strip() if os_version else "Junos"
            try:
                jnx_info = await self._snmp_get_multi(
                    host, [_OID_JNX_BOX_DESCR, _OID_JNX_BOX_SERIAL]
                )
                if not model:
                    model = jnx_info.get(_OID_JNX_BOX_DESCR, "")
                serial = jnx_info.get(_OID_JNX_BOX_SERIAL, "")
            except Exception as exc:
                logger.debug("Juniper-specific OIDs failed for %s: %s", host, exc)

        # Collect interfaces, then attach their IP addresses
        interfaces = await self._collect_interfaces(host)
        ip_entries = await self._collect_ip_addresses(host)
        for iface in interfaces:
            iface["ip_addresses"] = [
                ip for ip in ip_entries if ip["if_index"] == iface["index"]
            ]

        return {
            "host":         host,
            "name":         sys_name,
            "description":  descr,
            "location":     location,
            "contact":      contact,
            "serial":       serial,
            "model":        model,
            "os_version":   os_version,
            "platform":     platform,
            "manufacturer": manufacturer,
            "interfaces":   interfaces,
        }

    async def _collect_interfaces(self, host: str) -> list[dict]:
        """Walk IF-MIB tables and return a list of interface dicts."""
        try:
            descr_rows  = await self._snmp_walk(host, _OID_IF_DESCR)
            type_rows   = await self._snmp_walk(host, _OID_IF_TYPE)
            mtu_rows    = await self._snmp_walk(host, _OID_IF_MTU)
            speed_rows  = await self._snmp_walk(host, _OID_IF_SPEED)
            mac_rows    = await self._snmp_walk(host, _OID_IF_PHYS_ADDR)
            admin_rows  = await self._snmp_walk(host, _OID_IF_ADMIN_STATUS)
            oper_rows   = await self._snmp_walk(host, _OID_IF_OPER_STATUS)
        except Exception as exc:
            logger.error("SNMP ifTable walk failed for %s: %s", host, exc)
            return []

        try:
            name_rows   = await self._snmp_walk(host, _OID_IF_NAME)
            alias_rows  = await self._snmp_walk(host, _OID_IF_ALIAS)
            hspeed_rows = await self._snmp_walk(host, _OID_IF_HIGH_SPEED)
        except Exception as exc:
            logger.debug(
                "SNMP ifXTable walk failed for %s: %s (using ifTable only)", host, exc
            )
            name_rows   = []
            alias_rows  = []
            hspeed_rows = []

        descr_by_idx  = _rows_by_index(descr_rows)
        type_by_idx   = _rows_by_index(type_rows)
        mtu_by_idx    = _rows_by_index(mtu_rows)
        speed_by_idx  = _rows_by_index(speed_rows)
        mac_by_idx    = _rows_by_index(mac_rows)
        admin_by_idx  = _rows_by_index(admin_rows)
        oper_by_idx   = _rows_by_index(oper_rows)
        name_by_idx   = _rows_by_index(name_rows)
        alias_by_idx  = _rows_by_index(alias_rows)
        hspeed_by_idx = _rows_by_index(hspeed_rows)

        interfaces: list[dict] = []
        for idx in sorted(descr_by_idx):
            if_name    = name_by_idx.get(idx) or descr_by_idx.get(idx, "")
            if_type_int = int(type_by_idx.get(idx, 1) or 1)

            try:
                speed = int(hspeed_by_idx.get(idx, 0) or 0)
                if speed == 0:
                    raw_bps = int(speed_by_idx.get(idx, 0) or 0)
                    speed = raw_bps // 1_000_000
            except (ValueError, TypeError):
                speed = 0

            try:
                mtu = int(mtu_by_idx.get(idx, 0) or 0)
            except (ValueError, TypeError):
                mtu = 0

            interfaces.append(
                {
                    "index":        idx,
                    "name":         if_name,
                    "label":        alias_by_idx.get(idx, ""),
                    "type":         _if_type_to_netbox(if_type_int, if_name),
                    "mac_address":  _mac_bytes_to_str(mac_by_idx.get(idx, "")),
                    "admin_status": _ADMIN_STATUS.get(
                        int(admin_by_idx.get(idx, 1) or 1), "up"
                    ),
                    "oper_status":  _OPER_STATUS.get(
                        int(oper_by_idx.get(idx, 2) or 2), "down"
                    ),
                    "speed":        speed,
                    "mtu":          mtu,
                    "ip_addresses": [],  # populated after IP walk
                }
            )

        return interfaces

    async def _collect_ip_addresses(self, host: str) -> list[dict]:
        """Walk the ipAddrTable and return a list of IP address dicts."""
        try:
            addr_rows = await self._snmp_walk(host, _OID_IP_ADDR)
            idx_rows  = await self._snmp_walk(host, _OID_IP_IF_IDX)
            mask_rows = await self._snmp_walk(host, _OID_IP_NETMASK)
        except Exception as exc:
            logger.error("SNMP ipAddrTable walk failed for %s: %s", host, exc)
            return []

        # Build IP-keyed lookup maps (the OID suffix *is* the IP address)
        mask_by_ip: dict[str, str] = {}
        for oid, val in mask_rows:
            ip = _ip_suffix(oid, _OID_IP_NETMASK)
            if ip:
                mask_by_ip[ip] = val

        idx_by_ip: dict[str, str] = {}
        for oid, val in idx_rows:
            ip = _ip_suffix(oid, _OID_IP_IF_IDX)
            if ip:
                idx_by_ip[ip] = val

        ip_records: list[dict] = []
        for oid, ip_val in addr_rows:
            ip = _ip_suffix(oid, _OID_IP_ADDR)
            if not ip:
                continue
            mask       = mask_by_ip.get(ip, "255.255.255.255")
            prefix_len = _netmask_to_prefixlen(mask)
            try:
                if_index = int(idx_by_ip.get(ip, 0) or 0)
            except (ValueError, TypeError):
                if_index = 0

            ip_records.append(
                {
                    "address":  f"{ip}/{prefix_len}",
                    "family":   4,
                    "status":   "active",
                    "if_index": if_index,
                }
            )

        return ip_records

    # ------------------------------------------------------------------
    # Low-level SNMP transport helpers
    # ------------------------------------------------------------------

    def _build_auth(self) -> Any:
        """Return a pysnmp auth-data object for the configured SNMP version."""
        from pysnmp.hlapi.v3arch.asyncio import (
            CommunityData,
            USM_AUTH_HMAC96_MD5,
            USM_AUTH_HMAC96_SHA,
            USM_AUTH_HMAC192_SHA256,
            USM_AUTH_HMAC256_SHA384,
            USM_AUTH_HMAC384_SHA512,
            USM_AUTH_NONE,
            USM_PRIV_CBC56_DES,
            USM_PRIV_CFB128_AES,
            USM_PRIV_CFB192_AES,
            USM_PRIV_CFB256_AES,
            USM_PRIV_NONE,
            UsmUserData,
        )

        if self._version in ("1",):
            return CommunityData(self._community, mpModel=0)
        if self._version in ("2c", "2"):
            return CommunityData(self._community, mpModel=1)

        # SNMPv3
        auth_map = {
            "md5":    USM_AUTH_HMAC96_MD5,
            "sha":    USM_AUTH_HMAC96_SHA,
            "sha256": USM_AUTH_HMAC192_SHA256,
            "sha384": USM_AUTH_HMAC256_SHA384,
            "sha512": USM_AUTH_HMAC384_SHA512,
            "none":   USM_AUTH_NONE,
        }
        priv_map = {
            "des":    USM_PRIV_CBC56_DES,
            "aes":    USM_PRIV_CFB128_AES,
            "aes128": USM_PRIV_CFB128_AES,
            "aes192": USM_PRIV_CFB192_AES,
            "aes256": USM_PRIV_CFB256_AES,
            "none":   USM_PRIV_NONE,
        }
        auth_proto = auth_map.get(self._v3_auth_proto, USM_AUTH_HMAC96_SHA)
        priv_proto = priv_map.get(self._v3_priv_proto, USM_PRIV_CFB128_AES)
        return UsmUserData(
            self._v3_user,
            authKey=self._v3_auth_pass or None,
            privKey=self._v3_priv_pass or None,
            authProtocol=auth_proto,
            privProtocol=priv_proto,
        )

    async def _snmp_get_multi(
        self, host: str, oids: list[str]
    ) -> dict[str, str]:
        """Perform a single SNMP GET for *oids* and return ``{oid: value}``."""
        from pysnmp.hlapi.v3arch.asyncio import (
            ContextData,
            ObjectIdentity,
            ObjectType,
            SnmpEngine,
            UdpTransportTarget,
            get_cmd,
        )

        engine = SnmpEngine()
        auth   = self._build_auth()
        target = await UdpTransportTarget.create(
            (host, self._port),
            timeout=self._timeout,
            retries=self._retries,
        )
        var_binds = [ObjectType(ObjectIdentity(oid)) for oid in oids]

        err_ind, err_status, err_idx, var_binds_out = await get_cmd(
            engine, auth, target, ContextData(), *var_binds, lookupMib=False
        )

        if err_ind:
            raise RuntimeError(f"SNMP GET error ({host}): {err_ind}")
        if err_status:
            at = (
                str(var_binds_out[int(err_idx) - 1][0])
                if err_idx and var_binds_out
                else "?"
            )
            raise RuntimeError(
                f"SNMP GET PDU error ({host}): {err_status.prettyPrint()} at {at}"
            )

        result: dict[str, str] = {}
        for vb in var_binds_out:
            oid_str = str(vb[0]).lstrip(".")
            result[oid_str] = str(vb[1])
        return result

    async def _snmp_walk(
        self, host: str, base_oid: str
    ) -> list[tuple[str, str]]:
        """Walk *base_oid* subtree using GETBULK and return ``[(oid, value)]``."""
        from pysnmp.hlapi.v3arch.asyncio import (
            ContextData,
            ObjectIdentity,
            ObjectType,
            SnmpEngine,
            UdpTransportTarget,
            bulk_walk_cmd,
        )

        engine = SnmpEngine()
        auth   = self._build_auth()
        target = await UdpTransportTarget.create(
            (host, self._port),
            timeout=self._timeout,
            retries=self._retries,
        )

        results: list[tuple[str, str]] = []
        async for err_ind, err_status, _err_idx, var_binds in bulk_walk_cmd(
            engine,
            auth,
            target,
            ContextData(),
            0,
            25,
            ObjectType(ObjectIdentity(base_oid)),
            lexicographicMode=False,
            lookupMib=False,
        ):
            if err_ind:
                logger.debug(
                    "SNMP WALK error (%s, %s): %s", host, base_oid, err_ind
                )
                break
            if err_status:
                logger.debug(
                    "SNMP WALK PDU error (%s, %s): %s", host, base_oid, err_status
                )
                break
            for vb in var_binds:
                results.append((str(vb[0]).lstrip("."), str(vb[1])))

        return results
