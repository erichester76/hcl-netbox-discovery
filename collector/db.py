"""SQLite-backed store for job state, logs, schedules, and runtime settings."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)
_UNSET = object()

# Default DB path. Keep it under ``data/`` so the container can persist the
# SQLite file without requiring a writable repo root.
_DEFAULT_DB = os.path.join(os.path.dirname(__file__), "..", "data", "collector_jobs.sqlite3")


def _db_path() -> str:
    return os.environ.get("COLLECTOR_DB_PATH", os.path.normpath(_DEFAULT_DB))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    hcl_file    TEXT    NOT NULL,
    run_token   TEXT,
    status      TEXT    NOT NULL DEFAULT 'queued',
    stop_requested INTEGER NOT NULL DEFAULT 0,
    dry_run     INTEGER NOT NULL DEFAULT 0,
    debug_mode  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    summary     TEXT,
    artifact_json TEXT,
    runtime_snapshot_json TEXT,
    code_version_json TEXT
);

CREATE TABLE IF NOT EXISTS job_logs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id    INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    timestamp TEXT    NOT NULL,
    level     TEXT    NOT NULL,
    logger    TEXT,
    message   TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);

CREATE TABLE IF NOT EXISTS schedules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL,
    hcl_file    TEXT    NOT NULL,
    cron_expr   TEXT    NOT NULL,
    dry_run     INTEGER NOT NULL DEFAULT 0,
    enabled     INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT    NOT NULL,
    last_run_at TEXT,
    next_run_at TEXT
);

CREATE TABLE IF NOT EXISTS config_settings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    key           TEXT    NOT NULL UNIQUE,
    value         TEXT,
    default_value TEXT,
    description   TEXT,
    group_name    TEXT    NOT NULL DEFAULT 'General',
    created_at    TEXT    NOT NULL,
    updated_at    TEXT    NOT NULL
);
"""

# Seed data for ``config_settings``. Values are inserted with ``INSERT OR IGNORE``
# so new defaults appear without overwriting user-managed runtime settings.
_SETTINGS_SEED: list[tuple[str, str, str, str]] = [
    # --- Web UI startup settings are env-only except for DB-backed API auth ---
    ("WEB_PORT", "5000", "TCP port the web UI listens on (default: 5000)", "Web UI"),
    ("WEB_HOST", "0.0.0.0", "Bind address for the web UI (default: 0.0.0.0)", "Web UI"),
    (
        "WEB_SECRET_KEY",
        "change-me-in-production",
        "Secret key for Flask sessions \u2013 change this in production!",
        "Web UI",
    ),
    (
        "WEB_API_TOKEN",
        "",
        "Optional API token for `/api/*`; supports `Authorization: Bearer` and `X-API-Key`",
        "Web UI",
    ),
    (
        "COLLECTOR_DB_PATH",
        "",
        "Path for the SQLite job-tracking database (default: data/collector_jobs.sqlite3)",
        "Web UI",
    ),
    (
        "FLASK_DEBUG",
        "false",
        'Set to "true" to enable Flask debug mode (never use in production)',
        "Web UI",
    ),
    # --- NetBox ---
    ("NETBOX_URL", "https://netbox.example.com", "", "NetBox"),
    ("NETBOX_TOKEN", "your_netbox_api_token", "", "NetBox"),
    ("NETBOX_CACHE_BACKEND", "none", "Optional caching: none | redis | sqlite", "NetBox"),
    (
        "NETBOX_CACHE_URL",
        "",
        "Redis:  NETBOX_CACHE_URL=redis://redis:6379/0 SQLite: NETBOX_CACHE_URL=/tmp/netbox_cache.db",
        "NetBox",
    ),
    ("NETBOX_CACHE_TTL", "300", "Cache entry TTL in seconds (default: 300)", "NetBox"),
    (
        "NETBOX_CACHE_KEY_PREFIX",
        "nbx:",
        "Cache key prefix used to namespace keys in redis/sqlite backends (default: nbx:)",
        "NetBox",
    ),
    (
        "NETBOX_PREWARM_SENTINEL_TTL",
        "",
        "Prewarm sentinel TTL in seconds; leave empty to use cache TTL (default: unset)",
        "NetBox",
    ),
    (
        "NETBOX_CACHE_DISABLE_ON_FAILURES",
        "5",
        "Number of consecutive Redis failures before the cache is auto-disabled for the run (default: 5)",
        "NetBox",
    ),
    (
        "NETBOX_RATE_LIMIT",
        "0",
        "Maximum requests per second sent to NetBox; 0 = unlimited (default: 0)",
        "NetBox",
    ),
    (
        "NETBOX_RATE_LIMIT_BURST",
        "1",
        "Token-bucket burst size for rate limiting (default: 1)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_ATTEMPTS",
        "3",
        "Number of retry attempts on transient failures (default: 3)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_INITIAL_DELAY",
        "0.3",
        "Initial delay in seconds before the first retry (default: 0.3)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_BACKOFF_FACTOR",
        "2.0",
        "Exponential back-off multiplier applied between retries (default: 2.0)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_MAX_DELAY",
        "15.0",
        "Maximum delay in seconds between retries (default: 15.0)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_JITTER",
        "0.0",
        "Maximum random jitter in seconds added to each retry delay (default: 0.0)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_ON_4XX",
        "408,409,425,429",
        "Comma-separated 4xx HTTP status codes that trigger a retry (default: 408,409,425,429)",
        "NetBox",
    ),
    (
        "NETBOX_RETRY_5XX_COOLDOWN",
        "60.0",
        "Global cooldown in seconds after retryable NetBox 5xx responses (default: 60.0)",
        "NetBox",
    ),
    (
        "NETBOX_BRANCH",
        "",
        "NetBox branch name for branch-aware deployments; leave empty for default branch",
        "NetBox",
    ),
    # --- NetBox Source ---
    (
        "SOURCE_NETBOX_URL",
        "",
        "URL of the source NetBox instance to read objects from",
        "NetBox Source",
    ),
    ("SOURCE_NETBOX_TOKEN", "", "API token for the source NetBox instance", "NetBox Source"),
    (
        "SOURCE_NETBOX_VERIFY_SSL",
        "true",
        'Set to "false" to disable SSL verification for the source NetBox (default: true)',
        "NetBox Source",
    ),
    (
        "SOURCE_NETBOX_FILTERS",
        "",
        'Optional JSON filter dict passed to the source collection, e.g. {"site": "lon01"}',
        "NetBox Source",
    ),
    # --- General collector flags ---
    (
        "DRY_RUN",
        "false",
        'Set to "true" to log payloads without writing anything to NetBox',
        "General collector flags",
    ),
    # LOG_LEVEL is startup-only and therefore not DB-backed.
    # --- VMware vCenter ---
    ("VCENTER_URL", "vcenter.example.com", "", "VMware vCenter"),
    ("VCENTER_USER", "administrator@vsphere.local", "", "VMware vCenter"),
    ("VCENTER_PASS", "changeme", "", "VMware vCenter"),
    # --- Cisco Catalyst Center ---
    ("CATC_HOST", "https://catc.example.com", "", "Cisco Catalyst Center"),
    ("CATC_USER", "admin", "", "Cisco Catalyst Center"),
    ("CATC_PASS", "changeme", "", "Cisco Catalyst Center"),
    ("CATC_VERIFY_SSL", "true", "", "Cisco Catalyst Center"),
    (
        "CATC_FETCH_INTERFACES",
        "true",
        'Set to "true" to fetch per-device interface inventories and embed them in the source payload',
        "Cisco Catalyst Center",
    ),
    (
        "CATC_SITE_ASSIGNMENT_STRATEGY",
        "auto",
        "Site assignment strategy for Catalyst Center hierarchy mapping (default: auto)",
        "Cisco Catalyst Center",
    ),
    (
        "CATC_WAIT_ON_RATE_LIMIT",
        "true",
        "Let dnacentersdk honor Catalyst Center Retry-After rate-limit responses automatically (default: true)",
        "Cisco Catalyst Center",
    ),
    (
        "CATC_RATE_LIMIT_RETRY_ATTEMPTS",
        "3",
        "Additional adapter-level retry attempts when Catalyst Center still raises 429 responses (default: 3)",
        "Cisco Catalyst Center",
    ),
    (
        "CATC_RATE_LIMIT_RETRY_INITIAL_DELAY",
        "1.0",
        "Initial fallback delay in seconds before retrying raised Catalyst Center 429 responses (default: 1.0)",
        "Cisco Catalyst Center",
    ),
    (
        "CATC_RATE_LIMIT_RETRY_MAX_DELAY",
        "30.0",
        "Maximum fallback delay in seconds between raised Catalyst Center 429 retries (default: 30.0)",
        "Cisco Catalyst Center",
    ),
    (
        "CATC_RATE_LIMIT_RETRY_JITTER",
        "0.5",
        "Maximum random jitter in seconds added to fallback Catalyst Center 429 retries (default: 0.5)",
        "Cisco Catalyst Center",
    ),
    # --- Lenovo XClarity ---
    (
        "XCLARITY_HOST",
        "https://xclarity.example.com",
        "XCLARITY_HOST can be just a hostname/IP or a full URL; HTTPS port 443 is always used unless an explicit port is included in the URL.",
        "Lenovo XClarity",
    ),
    ("XCLARITY_USER", "admin", "", "Lenovo XClarity"),
    ("XCLARITY_PASS", "changeme", "", "Lenovo XClarity"),
    ("XCLARITY_VERIFY_SSL", "true", "", "Lenovo XClarity"),
    # --- Microsoft Azure ---
    (
        "AZURE_AUTH_METHOD",
        "default",
        'Use "service_principal" + client_id/secret, or "default" for DefaultAzureCredential (az login, managed identity, etc.)',
        "Microsoft Azure",
    ),
    ("AZURE_TENANT_ID", "", "", "Microsoft Azure"),
    ("AZURE_CLIENT_ID", "", "", "Microsoft Azure"),
    ("AZURE_CLIENT_SECRET", "", "", "Microsoft Azure"),
    (
        "AZURE_SUBSCRIPTION_IDS",
        "",
        "Comma-separated subscription IDs to limit scope (leave empty for all)",
        "Microsoft Azure",
    ),
    # --- LDAP ---
    ("LDAP_SERVER", "ldaps://ldap.example.com:636", "", "LDAP"),
    ("LDAP_USER", "cn=service-account,dc=example,dc=com", "", "LDAP"),
    ("LDAP_PASS", "changeme", "", "LDAP"),
    ("LDAP_SEARCH_BASE", "dc=example,dc=com", "", "LDAP"),
    ("LDAP_FILTER", "(objectClass=person)", "", "LDAP"),
    ("LDAP_PREFIX_LENGTH", "", "", "LDAP"),
    ("LDAP_SKIP_APS", "true", "", "LDAP"),
    # --- Active Directory ---
    ("AD_SERVER", "ldaps://dc01.corp.example.com", "", "Active Directory"),
    (
        "AD_USER",
        "CN=svc-netbox,OU=ServiceAccounts,DC=corp,DC=example,DC=com",
        "",
        "Active Directory",
    ),
    ("AD_PASS", "changeme", "", "Active Directory"),
    (
        "AD_SEARCH_BASE",
        "OU=Computers,DC=corp,DC=example,DC=com",
        "Search base for computer objects",
        "Active Directory",
    ),
    ("AD_DOMAIN", "corp.example.com", "", "Active Directory"),
    # --- Cisco Nexus Dashboard Fabric Controller ---
    ("NDFC_HOST", "ndfc.example.com", "", "Cisco Nexus Dashboard Fabric Controller"),
    ("NDFC_USER", "admin", "", "Cisco Nexus Dashboard Fabric Controller"),
    ("NDFC_PASS", "changeme", "", "Cisco Nexus Dashboard Fabric Controller"),
    ("NDFC_VERIFY_SSL", "true", "", "Cisco Nexus Dashboard Fabric Controller"),
    (
        "NDFC_FETCH_INTERFACES",
        "false",
        'Set to "true" to fetch per-switch interface lists from NDFC',
        "Cisco Nexus Dashboard Fabric Controller",
    ),
    # --- F5 BIG-IP ---
    ("F5_HOST", "f5.example.com", "", "F5 BIG-IP"),
    ("F5_USER", "admin", "", "F5 BIG-IP"),
    ("F5_PASS", "changeme", "", "F5 BIG-IP"),
    ("F5_VERIFY_SSL", "true", "", "F5 BIG-IP"),
    (
        "F5_FETCH_INTERFACES",
        "false",
        'Set to "true" to fetch physical interfaces and self-IPs',
        "F5 BIG-IP",
    ),
    ("F5_SITE", "Default", "NetBox site name to assign the BIG-IP appliance to", "F5 BIG-IP"),
    # --- Prometheus node-exporter ---
    ("PROMETHEUS_URL", "http://prometheus.example.com:9090", "", "Prometheus node-exporter"),
    ("PROMETHEUS_USER", "", "", "Prometheus node-exporter"),
    ("PROMETHEUS_PASS", "", "", "Prometheus node-exporter"),
    ("PROMETHEUS_VERIFY_SSL", "true", "", "Prometheus node-exporter"),
    (
        "PROMETHEUS_FETCH_INTERFACES",
        "true",
        'Set to "true" to fetch per-node network interface info',
        "Prometheus node-exporter",
    ),
    # --- SNMP ---
    (
        "SNMP_HOSTS",
        "router1.example.com,router2.example.com",
        "Comma-separated list of hostnames or IP addresses to poll",
        "SNMP",
    ),
    ("SNMP_COMMUNITY", "public", "", "SNMP"),
    ("SNMP_VERSION", "2c", "", "SNMP"),
    ("SNMP_PORT", "161", "", "SNMP"),
    ("SNMP_TIMEOUT", "5", "", "SNMP"),
    ("SNMP_RETRIES", "1", "", "SNMP"),
    ("SNMP_V3_USER", "", "SNMPv3 (only required when SNMP_VERSION=3)", "SNMP"),
    ("SNMP_V3_AUTH_PASS", "", "", "SNMP"),
    ("SNMP_V3_AUTH_PROTO", "sha", "", "SNMP"),
    ("SNMP_V3_PRIV_PASS", "", "", "SNMP"),
    ("SNMP_V3_PRIV_PROTO", "aes", "", "SNMP"),
    ("LINUX_SITE", "Default", "Linux SNMP specific (mappings/linux-snmp.hcl.example)", "SNMP"),
    # --- Per-source sync flags ---
    ("COLLECTOR_SYNC_INTERFACES", "true", "", "Per-source sync flags"),
    ("COLLECTOR_SYNC_INVENTORY", "true", "", "Per-source sync flags"),
    ("COLLECTOR_SYNC_DISKS", "true", "", "Per-source sync flags"),
    # --- Tenable One / Nessus ---
    (
        "TENABLE_HOST",
        "",
        "For Tenable.io / Tenable One: leave TENABLE_HOST empty (defaults to cloud.tenable.com). For on-prem Nessus: set to e.g. https://nessus.example.com:8834",
        "Tenable One",
    ),
    ("TENABLE_ACCESS_KEY", "", "", "Tenable One"),
    ("TENABLE_SECRET_KEY", "", "", "Tenable One"),
    ("TENABLE_PLATFORM", "tenable", '"tenable" (default) or "nessus"', "Tenable One"),
    (
        "TENABLE_DATE_RANGE",
        "30",
        "Days to look back for asset/vulnerability activity",
        "Tenable One",
    ),
    ("TENABLE_VERIFY_SSL", "true", "", "Tenable One"),
    (
        "TENABLE_INCLUDE_ASSET_DETAILS",
        "false",
        'Set to "true" to enable the "findings" collection',
        "Tenable One",
    ),
]

_STARTUP_CONFIG_KEYS = {
    "COLLECTOR_DB_PATH",
    "FLASK_DEBUG",
    "LOG_LEVEL",
    "WEB_HOST",
    "WEB_PORT",
    "WEB_SECRET_KEY",
}

_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    path = _db_path()
    with _lock:
        con = sqlite3.connect(path, timeout=10, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_db() -> None:
    """Create tables if they do not exist and seed config_settings from _SETTINGS_SEED."""
    path = _db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _conn() as con:
        con.executescript(_SCHEMA)
        # Migration: add dry_run column if it was not present in older DBs
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN dry_run INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass  # column already exists
        # Migration: add debug_mode column if it was not present in older DBs
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN debug_mode INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass  # column already exists
        # Migration: add run_token column if it was not present in older DBs
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN run_token TEXT")
        except Exception:
            pass  # column already exists
        # Migration: add artifact_json column for structured per-job artifacts
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN artifact_json TEXT")
        except Exception:
            pass  # column already exists
        # Migration: add runtime_snapshot_json column for masked effective run metadata
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN runtime_snapshot_json TEXT")
        except Exception:
            pass  # column already exists
        # Migration: add code_version_json column for git/version metadata
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN code_version_json TEXT")
        except Exception:
            pass  # column already exists
        # Migration: add stop_requested column if it was not present in older DBs
        try:
            con.execute("ALTER TABLE jobs ADD COLUMN stop_requested INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass  # column already exists

        # Bootstrap settings remain environment-only and should not appear in
        # the DB-backed runtime settings table or Settings UI.
        con.executemany(
            "DELETE FROM config_settings WHERE key=?",
            [(key,) for key in sorted(_STARTUP_CONFIG_KEYS)],
        )

        # Seed config_settings metadata without treating defaults as persisted
        # overrides. ``value`` is reserved for an explicit DB override set via
        # the Settings UI or ``set_setting()``; when it is NULL the effective
        # value falls back to the declared default.
        #
        # Migration note:
        # Older versions seeded ``value`` with either the current env var or
        # the static default, which meant an auto-seeded row could later mask a
        # changed environment variable. Rows that still have their original
        # timestamps are treated as untouched seed rows and cleared back to
        # NULL so they resume env/default fallback semantics.
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        for key, default_value, description, group_name in _SETTINGS_SEED:
            if key in _STARTUP_CONFIG_KEYS:
                continue
            con.execute(
                "INSERT OR IGNORE INTO config_settings"
                " (key, value, default_value, description, group_name, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (key, None, default_value, description, group_name, now, now),
            )
            con.execute(
                "UPDATE config_settings"
                " SET value=NULL, updated_at=?"
                " WHERE key=? AND value IS NOT NULL AND created_at = updated_at",
                (now, key),
            )
            con.execute(
                "UPDATE config_settings"
                " SET default_value=?, description=?, group_name=? WHERE key=?",
                (default_value, description, group_name, key),
            )


def create_job(
    hcl_file: str,
    dry_run: bool = False,
    debug_mode: bool = False,
    run_token: str | None = None,
) -> int:
    """Insert a new job row and return its *id*."""
    now = _now()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO jobs (hcl_file, run_token, status, dry_run, debug_mode, created_at) "
            "VALUES (?, ?, 'queued', ?, ?, ?)",
            (hcl_file, run_token, int(dry_run), int(debug_mode), now),
        )
        return cur.lastrowid  # type: ignore[return-value]


def start_job(job_id: int) -> None:
    """Mark job as running."""
    with _conn() as con:
        con.execute(
            "UPDATE jobs SET status='running', started_at=?, stop_requested=0 WHERE id=?",
            (_now(), job_id),
        )


def finish_job(
    job_id: int,
    success: bool,
    summary: dict | None = None,
    has_errors: bool = False,
    artifact: dict | None = None,
    *,
    forced_status: str | None = None,
) -> None:
    """Mark a job terminal and store optional summary/artifact payloads.

    Args:
        job_id: Primary key of the job to update.
        success: False if a top-level exception aborted the run (sets *failed*).
        summary: Optional per-object stats dict persisted as JSON.
        has_errors: Set to True when the run completed without a fatal exception
            but at least one item-level error was recorded in RunStats. Causes
            the status to be *partial* instead of *success*.
        artifact: Optional structured artifact dict persisted as JSON.
        forced_status: Optional explicit terminal status override. Valid values
            are terminal job states such as ``"stopped"`` and are intended for
            flows that must preserve a nonstandard terminal outcome even when
            *success* / *has_errors* would otherwise map to success/partial/failed.
    """
    if forced_status is not None:
        status = forced_status
    elif not success:
        status = "failed"
    elif has_errors:
        status = "partial"
    else:
        status = "success"
    summary_json = json.dumps(summary) if summary is not None else None
    artifact_json = json.dumps(artifact) if artifact is not None else None
    with _conn() as con:
        con.execute(
            "UPDATE jobs SET status=?, finished_at=?, summary=?, artifact_json=? WHERE id=?",
            (status, _now(), summary_json, artifact_json, job_id),
        )


def update_job_runtime_metadata(
    job_id: int,
    *,
    runtime_snapshot: dict[str, Any] | None | object = _UNSET,
    code_version: dict[str, Any] | None | object = _UNSET,
) -> None:
    """Persist runtime snapshot and code version metadata for a job before execution finishes."""
    updates: list[str] = []
    params: list[Any] = []

    if runtime_snapshot is not _UNSET:
        updates.append("runtime_snapshot_json=?")
        params.append(json.dumps(runtime_snapshot) if runtime_snapshot is not None else None)

    if code_version is not _UNSET:
        updates.append("code_version_json=?")
        params.append(json.dumps(code_version) if code_version is not None else None)

    if not updates:
        return

    params.append(job_id)
    with _conn() as con:
        con.execute(
            f"UPDATE jobs SET {', '.join(updates)} WHERE id=?",
            params,
        )


def request_job_stop(job_id: int) -> str | None:
    """Request that *job_id* stop, or stop it immediately if still queued.

    Returns:
        ``"requested"`` when a running job was flagged for cooperative stop,
        ``"stopped"`` when a queued job was transitioned directly to stopped,
        or ``None`` when the job does not exist or is already terminal.
    """
    now = _now()
    result: str | None = None
    log_message: str | None = None
    with _conn() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute(
            "SELECT status, stop_requested FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            return None

        status = row[0]
        already_requested = bool(row[1])
        if status == "queued":
            con.execute(
                "UPDATE jobs SET status='stopped', stop_requested=1, finished_at=? WHERE id=?",
                (now, job_id),
            )
            result = "stopped"
            log_message = "Job stopped by operator request before execution started."
        elif status == "running":
            con.execute(
                "UPDATE jobs SET stop_requested=1 WHERE id=?",
                (job_id,),
            )
            result = "requested"
            if not already_requested:
                log_message = "Stop requested by operator."
        else:
            return None

    if log_message is not None:
        add_log(job_id, "INFO", __name__, log_message)
    return result


def job_stop_requested(job_id: int) -> bool:
    """Return True when *job_id* has a pending stop request."""
    with _conn() as con:
        row = con.execute(
            "SELECT stop_requested FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
    return bool(row[0]) if row is not None else False


def reconcile_stale_running_jobs() -> list[int]:
    """Mark orphaned ``running`` jobs as terminal after worker restart.

    Jobs that were actively stopping when the worker died are finalized as
    ``stopped``. All other orphaned running jobs are finalized as ``failed``.

    Returns the list of reconciled job ids, oldest first.
    """
    now = _now()
    reconciled: list[tuple[int, str, str]] = []
    with _conn() as con:
        con.execute("BEGIN IMMEDIATE")
        rows = con.execute(
            "SELECT id, stop_requested FROM jobs WHERE status='running' ORDER BY id ASC"
        ).fetchall()
        for row in rows:
            job_id = int(row[0])
            stop_requested = bool(row[1])
            status = "stopped" if stop_requested else "failed"
            message = (
                "Job was stopping when the worker restarted; marked stopped during startup reconciliation."
                if stop_requested
                else "Job was still running when the worker restarted; marked failed during startup reconciliation."
            )
            con.execute(
                "UPDATE jobs SET status=?, finished_at=? WHERE id=? AND status='running'",
                (status, now, job_id),
            )
            reconciled.append((job_id, status, message))

    for job_id, _, message in reconciled:
        add_log(job_id, "WARNING", __name__, message)

    return [job_id for job_id, _, _ in reconciled]


def add_log(job_id: int, level: str, logger_name: str, message: str) -> None:
    """Append one log record for a job."""
    with _conn() as con:
        con.execute(
            "INSERT INTO job_logs (job_id, timestamp, level, logger, message) VALUES (?,?,?,?,?)",
            (job_id, _now(), level, logger_name, message),
        )


def get_jobs(limit: int = 100) -> list[dict[str, Any]]:
    """Return the *limit* most-recent jobs, newest first."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, hcl_file, run_token, status, stop_requested, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json, runtime_snapshot_json, code_version_json "
            "FROM jobs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def get_jobs_filtered(
    *,
    limit: int = 100,
    after_id: int = 0,
    status: str | None = None,
    hcl_file: str | None = None,
) -> list[dict[str, Any]]:
    """Return filtered jobs, newest first, without heavy detail fields."""
    clauses = ["id > ?"]
    params: list[Any] = [after_id]
    if status is not None:
        clauses.append("status = ?")
        params.append(status)
    if hcl_file is not None:
        clauses.append("hcl_file = ?")
        params.append(hcl_file)
    params.append(limit)
    query = (
        "SELECT id, hcl_file, run_token, status, stop_requested, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json "
        f"FROM jobs WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT ?"
    )
    with _conn() as con:
        rows = con.execute(query, tuple(params)).fetchall()
    return [_row_to_job_summary(r) for r in rows]


def get_running_jobs() -> list[dict[str, Any]]:
    """Return all queued and running jobs, newest first, without heavy detail fields."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, hcl_file, run_token, status, stop_requested, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json "
            "FROM jobs WHERE status IN ('queued', 'running') ORDER BY id DESC"
        ).fetchall()
    return [_row_to_job_summary(r) for r in rows]


def get_job(job_id: int) -> dict[str, Any] | None:
    """Return a single job record or *None* if not found."""
    with _conn() as con:
        row = con.execute(
            "SELECT id, hcl_file, run_token, status, stop_requested, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json, runtime_snapshot_json, code_version_json "
            "FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_job(row)


def get_job_logs(job_id: int, after_id: int = 0) -> list[dict[str, Any]]:
    """Return log records for *job_id* in chronological order."""
    after_id = max(after_id, 0)
    with _conn() as con:
        rows = con.execute(
            "SELECT id, job_id, timestamp, level, logger, message "
            "FROM job_logs WHERE job_id=? AND id>? ORDER BY id ASC",
            (job_id, after_id),
        ).fetchall()
    return [
        {
            "id": r[0],
            "job_id": r[1],
            "timestamp": r[2],
            "level": r[3],
            "logger": r[4],
            "message": r[5],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _row_to_job(row: tuple) -> dict[str, Any]:
    job: dict[str, Any] = {
        "id": row[0],
        "hcl_file": row[1],
        "run_token": row[2],
        "status": row[3],
        "stop_requested": bool(row[4]),
        "created_at": row[5],
        "started_at": row[6],
        "finished_at": row[7],
        "summary": None,
        "dry_run": bool(row[9]) if len(row) > 9 else False,
        "debug_mode": bool(row[10]) if len(row) > 10 else False,
        "artifact": None,
        "runtime_snapshot": None,
        "code_version": None,
    }
    if row[8]:
        try:
            job["summary"] = json.loads(row[8])
        except (json.JSONDecodeError, TypeError):
            job["summary"] = row[8]
    if len(row) > 11 and row[11]:
        try:
            job["artifact"] = json.loads(row[11])
        except (json.JSONDecodeError, TypeError):
            job["artifact"] = row[11]
    if len(row) > 12 and row[12]:
        try:
            job["runtime_snapshot"] = json.loads(row[12])
        except (json.JSONDecodeError, TypeError):
            job["runtime_snapshot"] = row[12]
    if len(row) > 13 and row[13]:
        try:
            job["code_version"] = json.loads(row[13])
        except (json.JSONDecodeError, TypeError):
            job["code_version"] = row[13]
    return job


def _row_to_job_summary(row: tuple) -> dict[str, Any]:
    """Return a lightweight job payload for list and polling endpoints."""
    job: dict[str, Any] = {
        "id": row[0],
        "hcl_file": row[1],
        "run_token": row[2],
        "status": row[3],
        "stop_requested": bool(row[4]),
        "created_at": row[5],
        "started_at": row[6],
        "finished_at": row[7],
        "summary": None,
        "dry_run": bool(row[9]) if len(row) > 9 else False,
        "debug_mode": bool(row[10]) if len(row) > 10 else False,
        "artifact": None,
    }
    if row[8]:
        try:
            job["summary"] = json.loads(row[8])
        except (json.JSONDecodeError, TypeError):
            job["summary"] = row[8]
    if len(row) > 11 and row[11]:
        try:
            job["artifact"] = json.loads(row[11])
        except (json.JSONDecodeError, TypeError):
            job["artifact"] = row[11]
    return job


def get_queued_jobs() -> list[dict[str, Any]]:
    """Return all jobs with status='queued', oldest first (FIFO execution order)."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, hcl_file, run_token, status, stop_requested, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json, runtime_snapshot_json, code_version_json "
            "FROM jobs WHERE status='queued' ORDER BY id ASC"
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def claim_next_queued_job() -> dict[str, Any] | None:
    """Atomically claim the oldest queued job and mark it running.

    Returns the claimed job row (including updated ``status`` and ``started_at``),
    or ``None`` when no queued jobs are available.

    This uses a single SQLite transaction with ``BEGIN IMMEDIATE`` so competing
    scheduler processes cannot claim the same queued row.
    """
    started_at = _now()
    with _conn() as con:
        con.execute("BEGIN IMMEDIATE")

        row = con.execute(
            "SELECT id FROM jobs WHERE status='queued' ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if row is None:
            return None

        job_id = int(row[0])
        cur = con.execute(
            "UPDATE jobs SET status='running', started_at=? WHERE id=? AND status='queued'",
            (started_at, job_id),
        )
        if cur.rowcount != 1:
            # Another process claimed this row first.
            return None

        claimed = con.execute(
            "SELECT id, hcl_file, run_token, status, stop_requested, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json, runtime_snapshot_json, code_version_json "
            "FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if claimed is None:
            return None
        return _row_to_job(claimed)


# ---------------------------------------------------------------------------
# Schedule CRUD
# ---------------------------------------------------------------------------


def create_schedule(
    name: str,
    hcl_file: str,
    cron_expr: str,
    dry_run: bool = False,
    next_run_at: str | None = None,
) -> int:
    """Insert a new schedule and return its *id*."""
    now = _now()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO schedules (name, hcl_file, cron_expr, dry_run, enabled, created_at, next_run_at)"
            " VALUES (?, ?, ?, ?, 1, ?, ?)",
            (name, hcl_file, cron_expr, int(dry_run), now, next_run_at),
        )
        return cur.lastrowid  # type: ignore[return-value]


def get_schedules() -> list[dict[str, Any]]:
    """Return all schedules ordered by name."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, name, hcl_file, cron_expr, dry_run, enabled, created_at, last_run_at, next_run_at"
            " FROM schedules ORDER BY name ASC"
        ).fetchall()
    return [_row_to_schedule(r) for r in rows]


def get_schedule(schedule_id: int) -> dict[str, Any] | None:
    """Return a single schedule or *None* if not found."""
    with _conn() as con:
        row = con.execute(
            "SELECT id, name, hcl_file, cron_expr, dry_run, enabled, created_at, last_run_at, next_run_at"
            " FROM schedules WHERE id=?",
            (schedule_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_schedule(row)


def update_schedule(
    schedule_id: int,
    name: str,
    hcl_file: str,
    cron_expr: str,
    dry_run: bool,
    enabled: bool,
    next_run_at: str | None = None,
) -> None:
    """Update an existing schedule."""
    with _conn() as con:
        con.execute(
            "UPDATE schedules SET name=?, hcl_file=?, cron_expr=?, dry_run=?, enabled=?, next_run_at=?"
            " WHERE id=?",
            (name, hcl_file, cron_expr, int(dry_run), int(enabled), next_run_at, schedule_id),
        )


def delete_schedule(schedule_id: int) -> None:
    """Delete a schedule by id."""
    with _conn() as con:
        con.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))


def get_due_schedules() -> list[dict[str, Any]]:
    """Return enabled schedules whose next_run_at is at or before now."""
    now = _now()
    with _conn() as con:
        rows = con.execute(
            "SELECT id, name, hcl_file, cron_expr, dry_run, enabled, created_at, last_run_at, next_run_at"
            " FROM schedules WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at <= ?",
            (now,),
        ).fetchall()
    return [_row_to_schedule(r) for r in rows]


def update_schedule_run(schedule_id: int, last_run_at: str, next_run_at: str) -> None:
    """Record that a schedule has run and set its next fire time."""
    with _conn() as con:
        con.execute(
            "UPDATE schedules SET last_run_at=?, next_run_at=? WHERE id=?",
            (last_run_at, next_run_at, schedule_id),
        )


def dispatch_next_due_schedule() -> dict[str, Any] | None:
    """Atomically dispatch one due schedule by queueing exactly one job.

    The function performs all steps in one transaction:
    1. Find one enabled due schedule.
    2. Advance its ``last_run_at`` and ``next_run_at``.
    3. Insert a queued job row with the schedule's ``hcl_file`` and ``dry_run``.

    Returns the newly created queued job row, or ``None`` when no due schedule
    exists.  ``debug_mode`` is always ``False`` for schedule-dispatched jobs.

    Concurrency note:
    ``BEGIN IMMEDIATE`` ensures only one competing scheduler process can dispatch
    a given due schedule at a time.
    """
    from datetime import datetime, timezone  # noqa: PLC0415

    from croniter import croniter  # noqa: PLC0415

    now_dt = datetime.now(timezone.utc)
    now_str = now_dt.strftime("%Y-%m-%dT%H:%M:%S")

    with _conn() as con:
        con.execute("BEGIN IMMEDIATE")

        sched_row = con.execute(
            "SELECT id, hcl_file, cron_expr, dry_run, next_run_at "
            "FROM schedules "
            "WHERE enabled=1 AND next_run_at IS NOT NULL AND next_run_at <= ? "
            "ORDER BY next_run_at ASC, id ASC "
            "LIMIT 1",
            (now_str,),
        ).fetchone()
        if sched_row is None:
            return None

        schedule_id = int(sched_row[0])
        hcl_file = str(sched_row[1])
        cron_expr = str(sched_row[2])
        dry_run = bool(sched_row[3])
        observed_next_run_at = sched_row[4]

        cron = croniter(cron_expr, now_dt)
        next_dt = cron.get_next(datetime)
        next_run_str = next_dt.strftime("%Y-%m-%dT%H:%M:%S")

        cur = con.execute(
            "UPDATE schedules SET last_run_at=?, next_run_at=? "
            "WHERE id=? AND enabled=1 AND next_run_at=?",
            (now_str, next_run_str, schedule_id, observed_next_run_at),
        )
        if cur.rowcount != 1:
            # Another process likely dispatched this schedule first.
            return None

        cur = con.execute(
            "INSERT INTO jobs (hcl_file, run_token, status, dry_run, debug_mode, created_at) "
            "VALUES (?, NULL, 'queued', ?, 0, ?)",
            (hcl_file, int(dry_run), now_str),
        )
        job_id = int(cur.lastrowid)

        job_row = con.execute(
            "SELECT id, hcl_file, run_token, status, 0, created_at, started_at, finished_at, summary, dry_run, debug_mode, artifact_json, runtime_snapshot_json, code_version_json "
            "FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if job_row is None:
            return None
        return _row_to_job(job_row)


def _row_to_schedule(row: tuple) -> dict[str, Any]:
    return {
        "id": row[0],
        "name": row[1],
        "hcl_file": row[2],
        "cron_expr": row[3],
        "dry_run": bool(row[4]),
        "enabled": bool(row[5]),
        "created_at": row[6],
        "last_run_at": row[7],
        "next_run_at": row[8],
    }


# ---------------------------------------------------------------------------
# Config settings CRUD
# ---------------------------------------------------------------------------

# Sensitive key name fragments – rendered as password inputs in the UI.
_SENSITIVE_PATTERNS = ("PASS", "TOKEN", "SECRET", "KEY", "CLIENT_SECRET")


def get_config(key: str, default: str = "") -> str:
    """Return the effective config value for *key*.

    Startup keys are env-only. All other keys are DB-backed runtime settings:
    explicit DB config_settings.value → row.default_value → *default*.

    Silently falls back to *default* when the DB is unavailable (e.g. before
    ``init_db()`` has been called, or during unit tests that do not set up the
    DB).
    """
    if key in _STARTUP_CONFIG_KEYS:
        return os.environ.get(key, default)
    try:
        with _conn() as con:
            row = con.execute(
                "SELECT value, default_value FROM config_settings WHERE key=?", (key,)
            ).fetchone()
        if row is not None:
            if row[0] is not None:
                return row[0]
            if row[1] is not None:
                return row[1]
    except Exception:
        pass  # DB not ready – fall through to explicit default
    return default


def get_all_settings() -> list[dict[str, Any]]:
    """Return all config settings ordered by group_name then key."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, key, value, default_value, description, group_name, updated_at"
            " FROM config_settings ORDER BY group_name ASC, key ASC"
        ).fetchall()
    return [_row_to_setting(r) for r in rows]


def get_settings_by_group() -> dict[str, list[dict[str, Any]]]:
    """Return config settings as an ordered dict keyed by group_name."""
    settings = get_all_settings()
    groups: dict[str, list[dict[str, Any]]] = {}
    for s in settings:
        groups.setdefault(s["group_name"], []).append(s)
    return groups


def set_setting(key: str, value: str | None) -> None:
    """Persist *value* for *key* in config_settings.

    Pass ``None`` to clear the DB override and fall back to the environment
    variable (same effect as ``reset_setting``).
    """
    now = _now()
    with _conn() as con:
        con.execute(
            "UPDATE config_settings SET value=?, updated_at=? WHERE key=?",
            (value, now, key),
        )


def reset_setting(key: str) -> None:
    """Clear the DB override for *key*, restoring the env-var / default fallback."""
    set_setting(key, None)


def _row_to_setting(row: tuple) -> dict[str, Any]:
    key = row[1]
    db_value = row[2]
    default_value = row[3] or ""
    effective = db_value if db_value is not None else default_value
    is_sensitive = any(pat in key for pat in _SENSITIVE_PATTERNS)
    return {
        "id": row[0],
        "key": key,
        "value": db_value,
        "default_value": default_value,
        "description": row[4] or "",
        "group_name": row[5] or "General",
        "updated_at": row[6],
        "effective_value": effective,
        "is_sensitive": is_sensitive,
        "is_overridden": db_value is not None,
    }
