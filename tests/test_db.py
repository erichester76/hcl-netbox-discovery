"""Tests for collector.db – SQLite job-tracking store."""

from __future__ import annotations

import os
import tempfile

import pytest

import collector.db as db_module
from collector.db import (
    add_log,
    create_job,
    create_schedule,
    delete_schedule,
    finish_job,
    get_due_schedules,
    get_job,
    get_job_logs,
    get_jobs,
    get_running_jobs,
    get_schedule,
    get_schedules,
    init_db,
    start_job,
    update_schedule,
    update_schedule_run,
)


@pytest.fixture(autouse=True)
def tmp_db(tmp_path, monkeypatch):
    """Redirect the DB to a throwaway temp file for each test."""
    db_path = str(tmp_path / "test_jobs.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    # Reset the module-level lock so tests don't share state
    monkeypatch.setattr(db_module, "_lock", __import__("threading").Lock())
    init_db()
    yield db_path


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------


def test_create_job_returns_id():
    job_id = create_job("mappings/vmware.hcl")
    assert isinstance(job_id, int)
    assert job_id >= 1


def test_get_job_initial_status():
    job_id = create_job("mappings/test.hcl")
    job = get_job(job_id)
    assert job is not None
    assert job["status"] == "queued"
    assert job["hcl_file"] == "mappings/test.hcl"
    assert job["started_at"] is None
    assert job["finished_at"] is None
    assert job["summary"] is None


def test_start_job_sets_running():
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    job = get_job(job_id)
    assert job["status"] == "running"
    assert job["started_at"] is not None


def test_finish_job_success():
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    summary = {"devices": {"processed": 10, "created": 2, "updated": 8, "skipped": 0, "errored": 0}}
    finish_job(job_id, success=True, summary=summary)
    job = get_job(job_id)
    assert job["status"] == "success"
    assert job["finished_at"] is not None
    assert job["summary"] == summary


def test_finish_job_failed():
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    finish_job(job_id, success=False)
    job = get_job(job_id)
    assert job["status"] == "failed"
    assert job["summary"] is None


def test_get_job_not_found():
    assert get_job(99999) is None


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------


def test_add_and_retrieve_logs():
    job_id = create_job("mappings/test.hcl")
    add_log(job_id, "INFO", "collector.engine", "Sync started")
    add_log(job_id, "WARNING", "collector.engine", "Missing field")
    add_log(job_id, "ERROR", "collector.engine", "Connection failed")

    logs = get_job_logs(job_id)
    assert len(logs) == 3
    assert logs[0]["level"] == "INFO"
    assert logs[0]["message"] == "Sync started"
    assert logs[1]["level"] == "WARNING"
    assert logs[2]["level"] == "ERROR"


def test_logs_isolated_between_jobs():
    job1 = create_job("mappings/a.hcl")
    job2 = create_job("mappings/b.hcl")
    add_log(job1, "INFO", "engine", "Job1 log")
    add_log(job2, "INFO", "engine", "Job2 log")

    logs1 = get_job_logs(job1)
    logs2 = get_job_logs(job2)
    assert len(logs1) == 1
    assert logs1[0]["message"] == "Job1 log"
    assert len(logs2) == 1
    assert logs2[0]["message"] == "Job2 log"


# ---------------------------------------------------------------------------
# get_jobs
# ---------------------------------------------------------------------------


def test_get_jobs_newest_first():
    ids = [create_job(f"mappings/{i}.hcl") for i in range(3)]
    jobs = get_jobs()
    returned_ids = [j["id"] for j in jobs]
    # Should be newest first
    assert returned_ids == list(reversed(ids))


def test_get_jobs_limit():
    for i in range(10):
        create_job(f"mappings/{i}.hcl")
    jobs = get_jobs(limit=5)
    assert len(jobs) == 5


# ---------------------------------------------------------------------------
# get_running_jobs
# ---------------------------------------------------------------------------


def test_get_running_jobs_empty():
    assert get_running_jobs() == []


def test_get_running_jobs_includes_queued_and_running():
    queued_id = create_job("mappings/queued.hcl")          # status = queued
    running_id = create_job("mappings/running.hcl")
    start_job(running_id)                                   # status = running
    done_id = create_job("mappings/done.hcl")
    start_job(done_id)
    finish_job(done_id, success=True)                       # status = success

    jobs = get_running_jobs()
    ids = {j["id"] for j in jobs}
    assert queued_id in ids
    assert running_id in ids
    assert done_id not in ids


def test_get_running_jobs_no_limit():
    """get_running_jobs must return all active jobs even when there are more than 50."""
    active_ids = set()
    for i in range(60):
        jid = create_job(f"mappings/{i}.hcl")
        start_job(jid)
        active_ids.add(jid)

    # Finish a few so they drop out of the active list
    for jid in list(active_ids)[:5]:
        finish_job(jid, success=True)
        active_ids.discard(jid)

    running = get_running_jobs()
    assert len(running) == len(active_ids)


# ---------------------------------------------------------------------------
# init_db idempotent
# ---------------------------------------------------------------------------


def test_init_db_idempotent():
    # Calling init_db multiple times must not raise or corrupt data
    job_id = create_job("mappings/stable.hcl")
    init_db()
    init_db()
    job = get_job(job_id)
    assert job is not None
    assert job["hcl_file"] == "mappings/stable.hcl"


# ---------------------------------------------------------------------------
# Schedule CRUD
# ---------------------------------------------------------------------------


def test_create_schedule_returns_id():
    sid = create_schedule("test", "mappings/vmware.hcl", "0 * * * *")
    assert isinstance(sid, int)
    assert sid >= 1


def test_get_schedule_initial_state():
    sid = create_schedule("nightly", "mappings/a.hcl", "0 2 * * *", dry_run=True)
    s = get_schedule(sid)
    assert s is not None
    assert s["name"] == "nightly"
    assert s["hcl_file"] == "mappings/a.hcl"
    assert s["cron_expr"] == "0 2 * * *"
    assert s["dry_run"] is True
    assert s["enabled"] is True
    assert s["last_run_at"] is None


def test_get_schedule_not_found():
    assert get_schedule(99999) is None


def test_get_schedules_empty():
    assert get_schedules() == []


def test_get_schedules_returns_all():
    create_schedule("a", "mappings/a.hcl", "0 * * * *")
    create_schedule("b", "mappings/b.hcl", "0 2 * * *")
    schedules = get_schedules()
    assert len(schedules) == 2
    names = {s["name"] for s in schedules}
    assert names == {"a", "b"}


def test_update_schedule():
    sid = create_schedule("old-name", "mappings/old.hcl", "0 * * * *")
    update_schedule(sid, "new-name", "mappings/new.hcl", "0 2 * * *", True, False)
    s = get_schedule(sid)
    assert s["name"] == "new-name"
    assert s["hcl_file"] == "mappings/new.hcl"
    assert s["cron_expr"] == "0 2 * * *"
    assert s["dry_run"] is True
    assert s["enabled"] is False


def test_delete_schedule():
    sid = create_schedule("to-delete", "mappings/x.hcl", "0 * * * *")
    assert get_schedule(sid) is not None
    delete_schedule(sid)
    assert get_schedule(sid) is None


def test_get_due_schedules_past_next_run():
    # A schedule with next_run_at in the past should be due
    past = "2000-01-01T00:00:00"
    sid = create_schedule("due", "mappings/a.hcl", "0 * * * *", next_run_at=past)
    due = get_due_schedules()
    ids = {s["id"] for s in due}
    assert sid in ids


def test_get_due_schedules_future_next_run():
    # A schedule with next_run_at far in the future should NOT be due
    future = "2099-12-31T23:59:59"
    sid = create_schedule("not-due", "mappings/a.hcl", "0 * * * *", next_run_at=future)
    due = get_due_schedules()
    ids = {s["id"] for s in due}
    assert sid not in ids


def test_get_due_schedules_disabled_not_returned():
    past = "2000-01-01T00:00:00"
    sid = create_schedule("disabled-due", "mappings/a.hcl", "0 * * * *", next_run_at=past)
    update_schedule(sid, "disabled-due", "mappings/a.hcl", "0 * * * *", False, False, past)
    due = get_due_schedules()
    ids = {s["id"] for s in due}
    assert sid not in ids


def test_update_schedule_run():
    sid = create_schedule("runner", "mappings/a.hcl", "0 * * * *")
    update_schedule_run(sid, "2026-01-01T02:00:00", "2026-01-02T02:00:00")
    s = get_schedule(sid)
    assert s["last_run_at"] == "2026-01-01T02:00:00"
    assert s["next_run_at"] == "2026-01-02T02:00:00"
