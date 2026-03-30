"""Tests for collector.db – SQLite job-tracking store."""

from __future__ import annotations

import os
import tempfile

import pytest

import collector.db as db_module
from collector.db import (
    add_log,
    create_job,
    finish_job,
    get_job,
    get_job_logs,
    get_jobs,
    get_running_jobs,
    init_db,
    start_job,
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
