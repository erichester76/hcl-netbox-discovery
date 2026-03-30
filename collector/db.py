"""
File: collector/db.py
Purpose: SQLite-backed store for sync job status and log records.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #web-ui
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
# Default DB path – can be overridden via COLLECTOR_DB_PATH env var
# ---------------------------------------------------------------------------
_DEFAULT_DB = os.path.join(os.path.dirname(__file__), "..", "collector_jobs.sqlite3")


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


def finish_job(job_id: int, success: bool, summary: dict | None = None) -> None:
    """Mark job as success/failed and store optional summary dict."""
    status = "success" if success else "failed"
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
