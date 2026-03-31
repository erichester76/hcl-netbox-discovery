"""
File: collector/db.py
Purpose: SQLite-backed store for sync job status, log records, schedules, and config settings.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #debug-mode
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DB path – can be overridden via COLLECTOR_DB_PATH env var.
# Stored under data/ so the directory is writable when running as a
# non-root user inside the container (the Dockerfile chowns /app/data/).
# ---------------------------------------------------------------------------
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
    status      TEXT    NOT NULL DEFAULT 'queued',
    dry_run     INTEGER NOT NULL DEFAULT 0,
    debug_mode  INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL,
    started_at  TEXT,
    finished_at TEXT,
    summary     TEXT
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

# ---------------------------------------------------------------------------
# .env.example parser – seeds the config_settings table on first run
# ---------------------------------------------------------------------------

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
_ENV_EXAMPLE = os.path.join(_ROOT, ".env.example")


def _parse_env_example(path: str) -> list[dict[str, str]]:
    """Parse *path* (.env.example) and return a list of setting dicts.

    Each dict has keys: key, default_value, description, group_name.
    Duplicate keys are skipped (first definition wins).
    """
    try:
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
    except OSError:
        logger.debug("Could not read %s – config_settings will not be seeded.", path)
        return []

    settings: list[dict[str, str]] = []
    seen: set[str] = set()
    group = "General"
    pending_desc: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect a three-line section header:
        #   # ----...\n# Group Name (context)\n# ----...
        if re.match(r"^# -{10,}$", line) and i + 2 < len(lines):
            next1 = lines[i + 1]
            next2 = lines[i + 2]
            if next1.startswith("# ") and re.match(r"^# -{10,}$", next2):
                raw = next1[2:].strip()
                # Strip trailing parenthetical context: "Web UI  (web_server.py)"
                raw = re.sub(r"\s*\(.*", "", raw).strip()
                # Keep only the first segment if slash-separated: "SNMP / Linux"
                raw = raw.split("/")[0].strip()
                group = raw or group
                pending_desc = []
                i += 3
                continue

        # Comment line – accumulate as description
        if line.startswith("#"):
            text = line[2:].strip() if len(line) > 1 else ""
            if text and not re.match(r"^-+$", text):
                pending_desc.append(text)
            i += 1
            continue

        # Blank line – clear pending description
        if not line.strip():
            pending_desc = []
            i += 1
            continue

        # Variable assignment: KEY=value
        m = re.match(r"^([A-Z][A-Z0-9_]*)=(.*)$", line)
        if m:
            key, default = m.group(1), m.group(2).strip()
            if key not in seen:
                settings.append(
                    {
                        "key": key,
                        "default_value": default,
                        "description": " ".join(pending_desc).strip(),
                        "group_name": group,
                    }
                )
                seen.add(key)
            pending_desc = []

        i += 1

    return settings

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
    """Create tables if they do not exist and seed config_settings from .env.example."""
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

        # Seed config_settings from .env.example (INSERT OR IGNORE preserves user values)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        for s in _parse_env_example(_ENV_EXAMPLE):
            con.execute(
                "INSERT OR IGNORE INTO config_settings"
                " (key, value, default_value, description, group_name, created_at, updated_at)"
                " VALUES (?, NULL, ?, ?, ?, ?, ?)",
                (s["key"], s["default_value"], s["description"], s["group_name"], now, now),
            )


def create_job(hcl_file: str, dry_run: bool = False, debug_mode: bool = False) -> int:
    """Insert a new job row and return its *id*."""
    now = _now()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO jobs (hcl_file, status, dry_run, debug_mode, created_at) VALUES (?, 'queued', ?, ?, ?)",
            (hcl_file, int(dry_run), int(debug_mode), now),
        )
        return cur.lastrowid  # type: ignore[return-value]


def start_job(job_id: int) -> None:
    """Mark job as running."""
    with _conn() as con:
        con.execute(
            "UPDATE jobs SET status='running', started_at=? WHERE id=?",
            (_now(), job_id),
        )


def finish_job(
    job_id: int,
    success: bool,
    summary: dict | None = None,
    has_errors: bool = False,
) -> None:
    """Mark job as success/partial/failed and store optional summary dict.

    Args:
        job_id: Primary key of the job to update.
        success: False if a top-level exception aborted the run (sets *failed*).
        summary: Optional per-object stats dict persisted as JSON.
        has_errors: Set to True when the run completed without a fatal exception
            but at least one item-level error was recorded in RunStats. Causes
            the status to be *partial* instead of *success*.
    """
    if not success:
        status = "failed"
    elif has_errors:
        status = "partial"
    else:
        status = "success"
    summary_json = json.dumps(summary) if summary else None
    with _conn() as con:
        con.execute(
            "UPDATE jobs SET status=?, finished_at=?, summary=? WHERE id=?",
            (status, _now(), summary_json, job_id),
        )


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
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary, dry_run, debug_mode "
            "FROM jobs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def get_running_jobs() -> list[dict[str, Any]]:
    """Return all queued and running jobs (no limit), newest first."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary, dry_run, debug_mode "
            "FROM jobs WHERE status IN ('queued', 'running') ORDER BY id DESC"
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def get_job(job_id: int) -> dict[str, Any] | None:
    """Return a single job record or *None* if not found."""
    with _conn() as con:
        row = con.execute(
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary, dry_run, debug_mode "
            "FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_job(row)


def get_job_logs(job_id: int) -> list[dict[str, Any]]:
    """Return all log records for *job_id* in chronological order."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, job_id, timestamp, level, logger, message "
            "FROM job_logs WHERE job_id=? ORDER BY id ASC",
            (job_id,),
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
        "status": row[2],
        "created_at": row[3],
        "started_at": row[4],
        "finished_at": row[5],
        "summary": None,
        "dry_run": bool(row[7]) if len(row) > 7 else False,
        "debug_mode": bool(row[8]) if len(row) > 8 else False,
    }
    if row[6]:
        try:
            job["summary"] = json.loads(row[6])
        except (json.JSONDecodeError, TypeError):
            job["summary"] = row[6]
    return job


def get_queued_jobs() -> list[dict[str, Any]]:
    """Return all jobs with status='queued', oldest first (FIFO execution order)."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary, dry_run, debug_mode "
            "FROM jobs WHERE status='queued' ORDER BY id ASC"
        ).fetchall()
    return [_row_to_job(r) for r in rows]


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

    Priority: DB config_settings.value → os.environ[key] → *default*.

    Silently falls back to the environment when the DB is unavailable (e.g.
    before ``init_db()`` has been called, or during unit tests that do not
    set up the DB).
    """
    try:
        with _conn() as con:
            row = con.execute(
                "SELECT value FROM config_settings WHERE key=?", (key,)
            ).fetchone()
        if row is not None and row[0] is not None:
            return row[0]
    except Exception:
        pass  # DB not ready – fall through to env var
    return os.environ.get(key, default)


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
    effective = db_value if db_value is not None else os.environ.get(key, default_value)
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
