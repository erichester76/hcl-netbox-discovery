"""Tests for the web UI Flask application (web.app)."""

from __future__ import annotations

import threading

import pytest

import collector.db as db_module
from collector.db import (
    add_log,
    create_job,
    finish_job,
    init_db,
    start_job,
)


@pytest.fixture()
def app(tmp_path, monkeypatch):
    """Create a Flask test client backed by a temporary DB."""
    db_path = str(tmp_path / "test_web.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setattr(db_module, "_lock", threading.Lock())
    init_db()

    from web.app import create_app  # noqa: PLC0415

    flask_app = create_app()
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def test_index_empty(app):
    resp = app.get("/")
    assert resp.status_code == 200
    assert b"HCL NetBox Discovery" in resp.data


def test_index_shows_jobs(app):
    job_id = create_job("mappings/vmware.hcl")
    start_job(job_id)
    finish_job(job_id, success=True, summary={"devices": {"processed": 3, "created": 1, "updated": 2, "skipped": 0, "errored": 0}})

    resp = app.get("/")
    assert resp.status_code == 200
    assert b"vmware.hcl" in resp.data


# ---------------------------------------------------------------------------
# Job detail
# ---------------------------------------------------------------------------


def test_job_detail_found(app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    add_log(job_id, "INFO", "engine", "Sync started for test")
    finish_job(job_id, success=True)

    resp = app.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert b"Sync started for test" in resp.data


def test_job_detail_not_found(app):
    resp = app.get("/jobs/99999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Job logs JSON endpoint (live poll)
# ---------------------------------------------------------------------------


def test_job_logs_json(app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    add_log(job_id, "INFO", "engine", "first log")
    add_log(job_id, "WARNING", "engine", "second log")

    resp = app.get(f"/jobs/{job_id}/logs")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "running"
    assert len(data["logs"]) == 2


def test_job_logs_json_after_id(app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    add_log(job_id, "INFO", "engine", "first")
    add_log(job_id, "INFO", "engine", "second")
    logs = app.get(f"/jobs/{job_id}/logs").get_json()["logs"]
    first_id = logs[0]["id"]

    resp = app.get(f"/jobs/{job_id}/logs?after_id={first_id}")
    data = resp.get_json()
    assert len(data["logs"]) == 1
    assert data["logs"][0]["message"] == "second"


# ---------------------------------------------------------------------------
# Run job – bad mapping file
# ---------------------------------------------------------------------------


def test_run_job_missing_file(app):
    """Submitting a non-existent HCL path should create a failed job."""
    resp = app.post("/jobs/run", data={"hcl_file": "mappings/nonexistent.hcl"})
    # Should redirect to job_detail
    assert resp.status_code == 302
    location = resp.headers["Location"]
    assert "/jobs/" in location


# ---------------------------------------------------------------------------
# Cache status page
# ---------------------------------------------------------------------------


def test_cache_status_page(app):
    resp = app.get("/cache")
    assert resp.status_code == 200
    assert b"Cache" in resp.data


# ---------------------------------------------------------------------------
# 404 handler
# ---------------------------------------------------------------------------


def test_404(app):
    resp = app.get("/this-does-not-exist")
    assert resp.status_code == 404
