"""Tenable One / Nessus data source adapter.

Fetches asset inventory and vulnerability data from Tenable.io (Tenable One)
or from an on-premise Nessus scanner via the Workbenches REST API, which is
available on both platforms.

Supported auth modes (controlled by ``extra.platform``):

``tenable`` (default)
    Tenable.io / Tenable One cloud API.  Set ``username`` to the API access
    key and ``password`` to the API secret key.  The adapter sends these as
    the ``X-ApiKeys`` request header.

``nessus``
    On-premise Nessus Professional / Manager.  Set ``username`` and
    ``password`` to normal credentials.  The adapter authenticates via
    ``POST /session`` and uses the resulting token as the ``X-Cookie`` header.

Supported collections
---------------------
``"assets"``
    All assets known to the scanner.  Each dict contains normalised
    convenience fields (``name``, ``ip_address``, ``os``, ``status``) and
    all raw Tenable fields.

``"vulnerabilities"``
    Unique vulnerabilities (by plugin) across all assets.  Each dict
    contains normalised CVE info (``cve_id``, ``severity``, ``cvss_score``,
    ``plugin_name``) and raw Tenable fields.  Suitable for syncing to the
    NetBox *netbox-security* plugin's ``security.vulnerabilities`` endpoint.

``"findings"``
    One record per (asset, plugin) combination — i.e. a specific
    vulnerability found on a specific asset.  Requires additional API calls
    per asset, so enable only when needed via ``extra.include_asset_details``.
    Suitable for syncing to ``security.findings`` or a similar endpoint.

Extra config options
--------------------
``platform``          ``"tenable"`` (default) or ``"nessus"``
``date_range``        Number of days to look back for activity (default: ``30``)
``include_asset_details``
                      ``"true"`` to fetch per-asset vulnerability details
                      needed by the ``"findings"`` collection (default:
                      ``"false"``).  Adds one HTTP request per asset.
"""

from __future__ import annotations

import ipaddress
import logging
from typing import Any

import requests

from .base import DataSource
from .utils import close_http_session, disable_ssl_warnings, safe_get

logger = logging.getLogger(__name__)

# Tenable severity level integers → human-readable labels.
_SEVERITY_MAP: dict[int, str] = {
    0: "info",
    1: "low",
    2: "medium",
    3: "high",
    4: "critical",
}


def _severity_label(level: Any) -> str:
    """Return a human-readable severity label for a Tenable severity *level*."""
    try:
        return _SEVERITY_MAP.get(int(level), "info")
    except (TypeError, ValueError):
        return "info"


def _first(lst: Any, default: str = "") -> str:
    """Return the first element of *lst*, or *default* if empty/None."""
    if isinstance(lst, list) and lst:
        return str(lst[0])
    return default


def _safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Alias for the shared :func:`~collector.sources.utils.safe_get` helper."""
    return safe_get(obj, key, default)


class TenableSource(DataSource):
    """Tenable One / Nessus REST API-backed source adapter."""

    def __init__(self) -> None:
        self._session: requests.Session | None = None
        self._base_url: str = ""
        self._platform: str = "tenable"
        self._date_range: int = 30
        self._include_asset_details: bool = False

    # ------------------------------------------------------------------
    # DataSource interface
    # ------------------------------------------------------------------

    def connect(self, config: Any) -> None:
        """Connect to Tenable One or Nessus using settings from *config*."""
        url = (config.url or "").strip().rstrip("/")
        if not url:
            url = "https://cloud.tenable.com"
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        self._base_url = url

        verify_ssl = config.verify_ssl
        if not verify_ssl:
            disable_ssl_warnings()

        extra = config.extra or {}
        self._platform = str(extra.get("platform", "tenable")).lower()
        self._date_range = int(extra.get("date_range", 30) or 30)
        self._include_asset_details = (
            str(extra.get("include_asset_details", "false")).lower() == "true"
        )

        if not config.username or not config.password:
            raise RuntimeError(
                "TenableSource: both username and password are required. "
                "For Tenable.io set username=access_key and password=secret_key. "
                "For Nessus set username and password to your login credentials."
            )

        session = requests.Session()
        session.verify = verify_ssl
        session.headers.update({
            "Accept":       "application/json",
            "Content-Type": "application/json",
        })
        self._session = session

        if self._platform == "nessus":
            self._authenticate_nessus(config.username, config.password)
        else:
            # Tenable.io / Tenable One: API key auth via X-ApiKeys header.
            session.headers["X-ApiKeys"] = (
                f"accessKey={config.username}; secretKey={config.password}"
            )

        logger.info(
            "TenableSource connected: %s (platform=%s)", self._base_url, self._platform
        )

    def get_objects(self, collection: str) -> list:
        """Return a flat list of dicts for *collection*."""
        if self._session is None:
            raise RuntimeError("TenableSource: connect() has not been called")

        collectors: dict[str, Any] = {
            "assets":          self._get_assets,
            "vulnerabilities": self._get_vulnerabilities,
            "findings":        self._get_findings,
        }
        fn = collectors.get(collection.lower())
        if fn is None:
            raise ValueError(
                f"TenableSource: unknown collection {collection!r}. "
                f"Supported: {sorted(collectors)}"
            )
        return fn()

    def close(self) -> None:
        """Release the HTTP session.  For Nessus this also deletes the session token."""
        if self._session is not None:
            if self._platform == "nessus":
                try:
                    self._delete("/session")
                except Exception as exc:
                    logger.debug("TenableSource: session logout error: %s", exc)
            self._session = close_http_session(self._session, "TenableSource")

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    def _authenticate_nessus(self, username: str, password: str) -> None:
        """Obtain a Nessus session token via ``POST /session``."""
        url = self._base_url + "/session"
        resp = self._session.post(  # type: ignore[union-attr]
            url,
            json={"username": username, "password": password},
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json().get("token")
        if not token:
            raise RuntimeError(
                f"TenableSource: Nessus authentication succeeded but no token was "
                f"returned from {url}"
            )
        self._session.headers["X-Cookie"] = f"token={token}"  # type: ignore[union-attr]
        logger.debug("TenableSource: Nessus session token obtained")

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> Any:
        """Perform an authenticated GET and return parsed JSON."""
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        logger.debug("TenableSource GET %s params=%s", url, params)
        resp = self._session.get(url, params=params, timeout=60)  # type: ignore[union-attr]
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> None:
        """Perform a DELETE request (used to log out of Nessus)."""
        if not path.startswith("/"):
            path = "/" + path
        url = self._base_url + path
        self._session.delete(url, timeout=10)  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Collection fetchers
    # ------------------------------------------------------------------

    def _get_assets(self) -> list[dict]:
        """Fetch all assets from the Workbenches API."""
        params = {"date_range": self._date_range}
        try:
            data = self._get("/workbenches/assets", params=params)
        except Exception as exc:
            logger.error("TenableSource: failed to fetch assets: %s", exc)
            return []

        raw_assets = _extract_list(data, ("assets", "items", "data"))
        logger.debug("TenableSource: fetched %d raw assets", len(raw_assets))

        assets = [self._enrich_asset(a) for a in raw_assets if isinstance(a, dict)]

        if self._include_asset_details:
            assets = [self._fetch_asset_vulns_summary(a) for a in assets]

        return assets

    def _get_vulnerabilities(self) -> list[dict]:
        """Fetch unique vulnerabilities (by plugin) across all assets."""
        params = {"date_range": self._date_range}
        try:
            data = self._get("/workbenches/vulnerabilities", params=params)
        except Exception as exc:
            logger.error("TenableSource: failed to fetch vulnerabilities: %s", exc)
            return []

        raw_vulns = _extract_list(data, ("vulnerabilities", "items", "data"))
        logger.debug(
            "TenableSource: fetched %d unique vulnerabilities", len(raw_vulns)
        )
        return [self._enrich_vulnerability(v) for v in raw_vulns if isinstance(v, dict)]

    def _get_findings(self) -> list[dict]:
        """Fetch one finding record per (asset, plugin) combination.

        This requires an additional API call per asset to retrieve that asset's
        individual vulnerability list.  Only available when
        ``include_asset_details = true`` in the source config.
        """
        if not self._include_asset_details:
            logger.warning(
                "TenableSource: 'findings' collection requires "
                "extra.include_asset_details = true in the source config. "
                "Returning empty list."
            )
            return []

        params = {"date_range": self._date_range}
        try:
            data = self._get("/workbenches/assets", params=params)
        except Exception as exc:
            logger.error("TenableSource: failed to fetch assets for findings: %s", exc)
            return []

        raw_assets = _extract_list(data, ("assets", "items", "data"))
        findings: list[dict] = []
        for raw_asset in raw_assets:
            if not isinstance(raw_asset, dict):
                continue
            asset = self._enrich_asset(raw_asset)
            asset_id = asset.get("id", "")
            if not asset_id:
                continue
            asset_vulns = self._fetch_asset_vulns(asset_id)
            for vuln in asset_vulns:
                # Merge asset and vulnerability data.  Use the asset's "name"
                # (hostname) for the combined record so it is not overwritten
                # by the vulnerability's plugin_name which also uses "name".
                asset_name = asset.get("name", "")
                finding = {**asset, **vuln, "asset_id": asset_id, "name": asset_name}
                findings.append(finding)

        logger.debug("TenableSource: returning %d findings", len(findings))
        return findings

    # ------------------------------------------------------------------
    # Per-asset helpers
    # ------------------------------------------------------------------

    def _fetch_asset_vulns(self, asset_id: str) -> list[dict]:
        """Return the vulnerability list for a single asset."""
        try:
            data = self._get(
                f"/workbenches/assets/{asset_id}/vulnerabilities",
                params={"date_range": self._date_range},
            )
        except Exception as exc:
            logger.warning(
                "TenableSource: failed to fetch vulns for asset %s: %s",
                asset_id, exc,
            )
            return []
        raw = _extract_list(data, ("vulnerabilities", "items", "data"))
        return [self._enrich_vulnerability(v) for v in raw if isinstance(v, dict)]

    def _fetch_asset_vulns_summary(self, asset: dict) -> dict:
        """Attach a ``vulnerabilities`` key to *asset* with its vulnerability list."""
        asset_id = asset.get("id", "")
        if asset_id:
            asset["vulnerabilities"] = self._fetch_asset_vulns(asset_id)
        else:
            asset["vulnerabilities"] = []
        return asset

    # ------------------------------------------------------------------
    # Data normalisation
    # ------------------------------------------------------------------

    def _enrich_asset(self, raw: dict) -> dict:
        """Return a normalised dict for a single Tenable asset record."""
        asset_id      = _safe_get(raw, "id", "")
        ipv4_list     = _safe_get(raw, "ipv4", []) or []
        ipv6_list     = _safe_get(raw, "ipv6", []) or []
        fqdn_list     = _safe_get(raw, "fqdn", []) or []
        netbios_list  = _safe_get(raw, "netbios_name", []) or []
        os_list       = _safe_get(raw, "operating_system", []) or []
        mac_list      = _safe_get(raw, "mac_address", []) or []
        system_types  = _safe_get(raw, "system_type", []) or []
        last_seen     = _safe_get(raw, "last_seen", "") or ""
        has_agent     = bool(_safe_get(raw, "has_agent", False))
        acr_score     = _safe_get(raw, "acr_score", None)
        exposure_score = _safe_get(raw, "exposure_score", None)

        # Determine the best "name" for the asset.
        name = (
            _first(fqdn_list)
            or _first(netbios_list)
            or _first(ipv4_list)
            or asset_id
            or "Unknown"
        )
        # Strip domain suffix to match NetBox 64-char name convention, but
        # only when the name is not a bare IP address (avoid splitting "10.0.0.1"
        # into "10").
        if name and not _is_ip_address(name):
            short_name = name.split(".")[0][:64] or "Unknown"
        else:
            short_name = name[:64] or "Unknown"

        ip_address = _first(ipv4_list) or _first(ipv6_list) or ""
        operating_system = _first(os_list) or ""
        mac_address = _first(mac_list) or ""

        # Compute a simple per-asset severity summary.
        severities = _safe_get(raw, "severities", []) or []
        critical_count = 0
        high_count = 0
        medium_count = 0
        low_count = 0
        for sev in severities:
            if isinstance(sev, dict):
                lvl = int(sev.get("level", 0))
                cnt = int(sev.get("count", 0))
                if lvl == 4:
                    critical_count += cnt
                elif lvl == 3:
                    high_count += cnt
                elif lvl == 2:
                    medium_count += cnt
                elif lvl == 1:
                    low_count += cnt

        return {
            # --- normalised convenience fields ---
            "id":               asset_id,
            "name":             short_name,
            "fqdn":             _first(fqdn_list),
            "ip_address":       ip_address,
            "ip_addresses":     ipv4_list + ipv6_list,
            "mac_address":      mac_address.upper() if mac_address else "",
            "os":               operating_system,
            "system_type":      _first(system_types),
            "last_seen":        last_seen,
            "has_agent":        has_agent,
            "status":           "active",
            "critical_vulns":   critical_count,
            "high_vulns":       high_count,
            "medium_vulns":     medium_count,
            "low_vulns":        low_count,
            "acr_score":        acr_score,
            "exposure_score":   exposure_score,
            # --- passthrough raw fields ---
            "ipv4":             ipv4_list,
            "ipv6":             ipv6_list,
            "fqdns":            fqdn_list,
            "netbios_names":    netbios_list,
            "operating_systems": os_list,
            "mac_addresses":    mac_list,
            "sources":          _safe_get(raw, "sources", []),
        }

    def _enrich_vulnerability(self, raw: dict) -> dict:
        """Return a normalised dict for a single Tenable vulnerability record."""
        plugin_id     = _safe_get(raw, "plugin_id", 0)
        plugin_name   = _safe_get(raw, "plugin_name", "") or ""
        plugin_family = _safe_get(raw, "plugin_family", "") or ""
        count         = _safe_get(raw, "count", 0)
        vuln_state    = _safe_get(raw, "vulnerability_state", "") or ""

        # CVE IDs may be on the top-level record (per-asset vulns) or absent
        # (aggregate vulns where only plugin info is available).
        cve_list = _safe_get(raw, "cve", []) or []
        if isinstance(cve_list, str):
            cve_list = [cve_list]
        cve_id = _first(cve_list) or f"NESSUS-{plugin_id}"

        # Severity: prefer top-level risk_factor, else derive from counts_by_severity.
        risk_factor = (_safe_get(raw, "risk_factor", "") or "").lower()
        if risk_factor in ("critical", "high", "medium", "low", "info", "none"):
            severity = risk_factor if risk_factor != "none" else "info"
        else:
            # Derive highest severity from counts_by_severity list.
            counts_by_sev = _safe_get(raw, "counts_by_severity", []) or []
            highest_level = 0
            for sev in counts_by_sev:
                if isinstance(sev, dict):
                    lvl = int(sev.get("level", 0))
                    cnt = int(sev.get("count", 0))
                    if cnt > 0 and lvl > highest_level:
                        highest_level = lvl
            severity = _severity_label(highest_level)

        cvss_score = _safe_get(raw, "cvss_base_score", None) or _safe_get(
            raw, "cvss3_base_score", None
        )
        description = _safe_get(raw, "description", "") or ""
        synopsis    = _safe_get(raw, "synopsis", "") or ""
        solution    = _safe_get(raw, "solution", "") or ""

        return {
            # --- normalised convenience fields ---
            "plugin_id":    plugin_id,
            "cve_id":       cve_id,
            "cve_ids":      cve_list,
            "name":         plugin_name,
            "plugin_name":  plugin_name,
            "plugin_family": plugin_family,
            "severity":     severity,
            "cvss_score":   cvss_score,
            "description":  description or synopsis,
            "synopsis":     synopsis,
            "solution":     solution,
            "count":        count,
            "state":        vuln_state,
            # --- passthrough raw fields ---
            "risk_factor":  _safe_get(raw, "risk_factor", ""),
            "cvss3_base_score": _safe_get(raw, "cvss3_base_score", None),
            "counts_by_severity": _safe_get(raw, "counts_by_severity", []),
        }


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _is_ip_address(value: str) -> bool:
    """Return ``True`` if *value* is a valid IPv4 or IPv6 address string."""
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _extract_list(data: Any, keys: tuple) -> list:
    """Extract a list from *data*, trying each key in *keys* in order.

    If *data* is already a list it is returned as-is.  If it is a dict the
    first matching key whose value is a list is used.  Returns an empty list
    when *data* is neither a list nor a dict with a matching key.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if isinstance(value, list):
                return value
        return []
    return []
