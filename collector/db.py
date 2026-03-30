"""
File: collector/db.py
Purpose: SQLite-backed store for sync job status, log records, and schedules.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #141
"""

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
"""

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
    """Create tables if they do not exist."""
    path = _db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with _conn() as con:
        con.executescript(_SCHEMA)


def create_job(hcl_file: str) -> int:
    """Insert a new job row and return its *id*."""
    now = _now()
    with _conn() as con:
        cur = con.execute(
            "INSERT INTO jobs (hcl_file, status, created_at) VALUES (?, 'queued', ?)",
            (hcl_file, now),
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
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary "
            "FROM jobs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def get_running_jobs() -> list[dict[str, Any]]:
    """Return all queued and running jobs (no limit), newest first."""
    with _conn() as con:
        rows = con.execute(
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary "
            "FROM jobs WHERE status IN ('queued', 'running') ORDER BY id DESC"
        ).fetchall()
    return [_row_to_job(r) for r in rows]


def get_job(job_id: int) -> dict[str, Any] | None:
    """Return a single job record or *None* if not found."""
    with _conn() as con:
        row = con.execute(
            "SELECT id, hcl_file, status, created_at, started_at, finished_at, summary "
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
    }
    if row[6]:
        try:
            job["summary"] = json.loads(row[6])
        except (json.JSONDecodeError, TypeError):
            job["summary"] = row[6]
    return job


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
