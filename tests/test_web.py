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


def test_job_detail_partial_status(app):
    """A job finished with has_errors=True should show 'partial' badge."""
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    summary = {"devices": {"processed": 5, "created": 3, "updated": 1, "skipped": 0, "errored": 1}}
    finish_job(job_id, success=True, summary=summary, has_errors=True)

    resp = app.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert b"partial" in resp.data


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
# Currently running panel – queued and running jobs
# ---------------------------------------------------------------------------


def test_index_shows_queued_job_in_running_panel(app):
    """A queued job (not yet started) must appear in the 'Currently Running' section."""
    job_id = create_job("mappings/queued.hcl")  # status = queued, never started

    resp = app.get("/")
    assert resp.status_code == 200
    # The running panel should contain the job id and the queued badge
    assert str(job_id).encode() in resp.data
    assert b"queued" in resp.data


def test_index_shows_running_job_in_running_panel(app):
    """A running job must appear in the 'Currently Running' section."""
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)  # status = running

    resp = app.get("/")
    assert resp.status_code == 200
    assert str(job_id).encode() in resp.data
    assert b"running" in resp.data


def test_index_excludes_finished_job_from_running_panel(app):
    """A finished job must NOT appear in the 'Currently Running' section."""
    job_id = create_job("mappings/done.hcl")
    start_job(job_id)
    finish_job(job_id, success=True)

    resp = app.get("/")
    assert resp.status_code == 200
    # The job should be in the history table but not in the running panel body
    # (The running panel should say "No active jobs.")
    assert b"No active jobs" in resp.data


# ---------------------------------------------------------------------------
# /api/running-jobs endpoint
# ---------------------------------------------------------------------------


def test_api_running_jobs_empty(app):
    resp = app.get("/api/running-jobs")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 0
    assert data["jobs"] == []


def test_api_running_jobs_returns_active_jobs(app):
    queued_id = create_job("mappings/a.hcl")          # queued
    running_id = create_job("mappings/b.hcl")
    start_job(running_id)                              # running
    done_id = create_job("mappings/c.hcl")
    start_job(done_id)
    finish_job(done_id, success=True)                  # success – should be excluded

    resp = app.get("/api/running-jobs")
    assert resp.status_code == 200
    data = resp.get_json()
    ids = {j["id"] for j in data["jobs"]}
    assert queued_id in ids
    assert running_id in ids
    assert done_id not in ids
    assert data["count"] == 2


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


# ---------------------------------------------------------------------------
# Scheduler routes
# ---------------------------------------------------------------------------


def test_schedules_page_empty(app):
    resp = app.get("/schedules")
    assert resp.status_code == 200
    assert b"Scheduler" in resp.data or b"Schedules" in resp.data


def test_schedules_page_shows_entry(app):
    from collector.db import create_schedule  # noqa: PLC0415
    create_schedule("nightly-test", "mappings/test.hcl", "0 2 * * *", next_run_at="2099-01-01T02:00:00")
    resp = app.get("/schedules")
    assert resp.status_code == 200
    assert b"nightly-test" in resp.data


def test_schedule_create(app):
    resp = app.post("/schedules/create", data={
        "name": "my-schedule",
        "hcl_file": "mappings/test.hcl",
        "cron_expr": "0 3 * * *",
    })
    assert resp.status_code == 302
    # Should redirect to /schedules
    from collector.db import get_schedules  # noqa: PLC0415
    schedules = get_schedules()
    assert any(s["name"] == "my-schedule" for s in schedules)


def test_schedule_create_missing_fields(app):
    """Creating a schedule without required fields should redirect without creating."""
    resp = app.post("/schedules/create", data={"name": "", "hcl_file": "", "cron_expr": ""})
    assert resp.status_code == 302
    from collector.db import get_schedules  # noqa: PLC0415
    assert get_schedules() == []


def test_schedule_delete(app):
    from collector.db import create_schedule, get_schedules  # noqa: PLC0415
    sid = create_schedule("to-delete", "mappings/x.hcl", "0 * * * *")
    assert len(get_schedules()) == 1
    resp = app.post(f"/schedules/{sid}/delete")
    assert resp.status_code == 302
    assert get_schedules() == []


def test_schedule_toggle(app):
    from collector.db import create_schedule, get_schedule  # noqa: PLC0415
    sid = create_schedule("toggleable", "mappings/x.hcl", "0 * * * *")
    # Initially enabled
    assert get_schedule(sid)["enabled"] is True
    # Toggle to disabled
    resp = app.post(f"/schedules/{sid}/toggle")
    assert resp.status_code == 302
    assert get_schedule(sid)["enabled"] is False
    # Toggle back to enabled
    app.post(f"/schedules/{sid}/toggle")
    assert get_schedule(sid)["enabled"] is True


def test_schedule_edit_get(app):
    from collector.db import create_schedule  # noqa: PLC0415
    sid = create_schedule("editable", "mappings/x.hcl", "0 * * * *")
    resp = app.get(f"/schedules/{sid}/edit")
    assert resp.status_code == 200
    assert b"editable" in resp.data


def test_schedule_edit_post(app):
    from collector.db import create_schedule, get_schedule  # noqa: PLC0415
    sid = create_schedule("old-name", "mappings/x.hcl", "0 * * * *")
    resp = app.post(f"/schedules/{sid}/edit", data={
        "name": "new-name",
        "hcl_file": "mappings/y.hcl",
        "cron_expr": "0 4 * * *",
        "enabled": "1",
    })
    assert resp.status_code == 302
    s = get_schedule(sid)
    assert s["name"] == "new-name"
    assert s["cron_expr"] == "0 4 * * *"


def test_schedule_edit_not_found(app):
    resp = app.get("/schedules/99999/edit")
    assert resp.status_code == 404


def test_schedule_run_now_missing_file(app):
    from collector.db import create_schedule  # noqa: PLC0415
    sid = create_schedule("bad-file", "/nonexistent/path.hcl", "0 * * * *")
    resp = app.post(f"/schedules/{sid}/run-now")
    assert resp.status_code == 302
