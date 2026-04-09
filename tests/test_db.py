"""Tests for collector.db – SQLite job-tracking store."""

from __future__ import annotations

import sqlite3

import pytest

import collector.db as db_module
from collector.db import (
    add_log,
    create_job,
    create_schedule,
    delete_schedule,
    finish_job,
    get_config,
    get_due_schedules,
    get_job,
    get_job_logs,
    get_jobs,
    get_running_jobs,
    get_schedule,
    get_schedules,
    get_settings_by_group,
    init_db,
    job_stop_requested,
    reconcile_stale_running_jobs,
    request_job_stop,
    reset_setting,
    set_setting,
    start_job,
    update_job_runtime_metadata,
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
    assert job["run_token"] is None
    assert job["started_at"] is None
    assert job["finished_at"] is None
    assert job["summary"] is None
    assert job["artifact"] is None


def test_create_job_persists_run_token():
    job_id = create_job("mappings/test.hcl", run_token="capture-123")

    job = get_job(job_id)

    assert job is not None
    assert job["run_token"] == "capture-123"


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


def test_finish_job_persists_artifact():
    job_id = create_job("mappings/test.hcl", dry_run=True, debug_mode=True, run_token="run-123")
    start_job(job_id)
    artifact = {
        "job_id": job_id,
        "hcl_file": "mappings/test.hcl",
        "dry_run": True,
        "debug_mode": True,
        "success": True,
        "status": "success",
        "has_errors": False,
        "summary": {"devices": {"processed": 1}},
        "error": None,
    }
    finish_job(job_id, success=True, artifact=artifact)
    job = get_job(job_id)
    assert job is not None
    assert job["artifact"] == artifact


def test_update_job_runtime_metadata_persists_snapshot_and_code_version():
    job_id = create_job("mappings/test.hcl", dry_run=True, debug_mode=True, run_token="run-123")
    runtime_snapshot = {
        "job": {"hcl_file": "mappings/test.hcl", "run_token": "run-123"},
        "config": {"source": {"password": "********"}},
    }
    code_version = {
        "version": "0.1.0",
        "git_commit": "abc123",
    }

    update_job_runtime_metadata(
        job_id,
        runtime_snapshot=runtime_snapshot,
        code_version=code_version,
    )

    job = get_job(job_id)
    assert job is not None
    assert job["runtime_snapshot"] == runtime_snapshot
    assert job["code_version"] == code_version


def test_update_job_runtime_metadata_only_updates_requested_field():
    job_id = create_job("mappings/test.hcl", dry_run=True, debug_mode=True, run_token="run-123")
    runtime_snapshot = {"job": {"hcl_file": "mappings/test.hcl"}}
    code_version = {"version": "0.1.0", "git_commit": "abc123"}

    update_job_runtime_metadata(
        job_id,
        runtime_snapshot=runtime_snapshot,
        code_version=code_version,
    )
    update_job_runtime_metadata(job_id, runtime_snapshot={"job": {"hcl_file": "mappings/next.hcl"}})

    job = get_job(job_id)
    assert job is not None
    assert job["runtime_snapshot"] == {"job": {"hcl_file": "mappings/next.hcl"}}
    assert job["code_version"] == code_version


def test_finish_job_failed():
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    finish_job(job_id, success=False)
    job = get_job(job_id)
    assert job["status"] == "failed"
    assert job["summary"] is None


def test_finish_job_partial():
    """A successful run with item-level errors should produce status 'partial'."""
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    summary = {"devices": {"processed": 10, "created": 2, "updated": 7, "skipped": 0, "errored": 1}}
    finish_job(job_id, success=True, summary=summary, has_errors=True)
    job = get_job(job_id)
    assert job["status"] == "partial"
    assert job["finished_at"] is not None
    assert job["summary"] == summary


def test_finish_job_forced_stopped():
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    summary = {"devices": {"processed": 3, "created": 1, "updated": 1, "skipped": 1, "errored": 0}}
    finish_job(job_id, success=True, summary=summary, forced_status="stopped")
    job = get_job(job_id)
    assert job["status"] == "stopped"
    assert job["summary"] == summary


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


def test_request_job_stop_marks_queued_job_stopped():
    job_id = create_job("mappings/queued.hcl")
    action = request_job_stop(job_id)
    job = get_job(job_id)
    logs = get_job_logs(job_id)
    assert action == "stopped"
    assert job is not None
    assert job["status"] == "stopped"
    assert job["stop_requested"] is True
    assert job["finished_at"] is not None
    assert logs[-1]["message"] == "Job stopped by operator request before execution started."


def test_request_job_stop_flags_running_job():
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)
    action = request_job_stop(job_id)
    job = get_job(job_id)
    logs = get_job_logs(job_id)
    assert action == "requested"
    assert job is not None
    assert job["status"] == "running"
    assert job["stop_requested"] is True
    assert job_stop_requested(job_id) is True
    assert logs[-1]["message"] == "Stop requested by operator."


def test_request_job_stop_does_not_duplicate_running_log_message():
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)

    assert request_job_stop(job_id) == "requested"
    assert request_job_stop(job_id) == "requested"

    operator_logs = [
        log for log in get_job_logs(job_id)
        if log["message"] == "Stop requested by operator."
    ]
    assert len(operator_logs) == 1


def test_reconcile_stale_running_jobs_marks_running_jobs_failed():
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)

    reconciled = reconcile_stale_running_jobs()

    job = get_job(job_id)
    logs = get_job_logs(job_id)
    assert reconciled == [job_id]
    assert job is not None
    assert job["status"] == "failed"
    assert job["finished_at"] is not None
    assert logs[-1]["level"] == "WARNING"
    assert "marked failed during startup reconciliation" in logs[-1]["message"]


def test_reconcile_stale_running_jobs_marks_stop_requested_jobs_stopped():
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)
    request_job_stop(job_id)

    reconciled = reconcile_stale_running_jobs()

    job = get_job(job_id)
    logs = get_job_logs(job_id)
    assert reconciled == [job_id]
    assert job is not None
    assert job["status"] == "stopped"
    assert job["finished_at"] is not None
    assert logs[-1]["level"] == "WARNING"
    assert "marked stopped during startup reconciliation" in logs[-1]["message"]


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


def test_init_db_migrates_jobs_table_to_add_artifact_json(tmp_path, monkeypatch):
    db_path = str(tmp_path / "legacy_jobs.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setattr(db_module, "_lock", __import__("threading").Lock())

    con = sqlite3.connect(db_path)
    con.execute(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hcl_file TEXT NOT NULL,
            run_token TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            dry_run INTEGER NOT NULL DEFAULT 0,
            debug_mode INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            summary TEXT
        )
        """
    )
    con.commit()
    con.close()

    init_db()

    con = sqlite3.connect(db_path)
    columns = {
        row[1]
        for row in con.execute("PRAGMA table_info(jobs)").fetchall()
    }
    con.close()

    assert "artifact_json" in columns


def test_runtime_config_defaults_do_not_require_env(monkeypatch):
    monkeypatch.delenv("NETBOX_URL", raising=False)
    assert get_config("NETBOX_URL", "") == "https://netbox.example.com"


def test_runtime_config_ignores_env_fallback(monkeypatch):
    monkeypatch.setenv("NETBOX_URL", "https://env.example.com")
    assert get_config("NETBOX_URL", "") == "https://netbox.example.com"


def test_runtime_config_db_override_wins():
    set_setting("NETBOX_URL", "https://db.example.com")
    assert get_config("NETBOX_URL", "") == "https://db.example.com"
    reset_setting("NETBOX_URL")
    assert get_config("NETBOX_URL", "") == "https://netbox.example.com"


def test_sensitive_setting_is_encrypted_at_rest(monkeypatch):
    monkeypatch.setenv("COLLECTOR_DB_ENCRYPTION_KEY", "unit-test-db-key")

    set_setting("VCENTER_PASS", "super-secret")

    with sqlite3.connect(db_module._db_path()) as con:
        stored = con.execute(
            "SELECT value FROM config_settings WHERE key='VCENTER_PASS'"
        ).fetchone()[0]

    assert stored != "super-secret"
    assert stored.startswith("enc:v1:")
    assert get_config("VCENTER_PASS", "") == "super-secret"


def test_sensitive_setting_requires_bootstrap_key_for_decryption(monkeypatch):
    monkeypatch.setenv("COLLECTOR_DB_ENCRYPTION_KEY", "unit-test-db-key")
    set_setting("VCENTER_PASS", "super-secret")

    monkeypatch.delenv("COLLECTOR_DB_ENCRYPTION_KEY", raising=False)

    with pytest.raises(RuntimeError, match="requires COLLECTOR_DB_ENCRYPTION_KEY"):
        get_config("VCENTER_PASS", "")


def test_sensitive_setting_rejects_wrong_bootstrap_key(monkeypatch):
    monkeypatch.setenv("COLLECTOR_DB_ENCRYPTION_KEY", "unit-test-db-key")
    set_setting("VCENTER_PASS", "super-secret")

    monkeypatch.setenv("COLLECTOR_DB_ENCRYPTION_KEY", "wrong-db-key")

    with pytest.raises(RuntimeError, match="could not be decrypted"):
        get_config("VCENTER_PASS", "")


def test_non_secret_key_setting_does_not_require_encryption_bootstrap(monkeypatch):
    monkeypatch.delenv("COLLECTOR_DB_ENCRYPTION_KEY", raising=False)

    set_setting("NETBOX_CACHE_KEY_PREFIX", "myapp:")

    assert get_config("NETBOX_CACHE_KEY_PREFIX", "") == "myapp:"


def test_catc_runtime_settings_are_seeded():
    settings = {row["key"]: row for row in get_settings_by_group()["Cisco Catalyst Center"]}

    assert settings["CATC_FETCH_INTERFACES"]["default_value"] == "true"
    assert settings["CATC_SITE_ASSIGNMENT_STRATEGY"]["default_value"] == "auto"


def test_startup_config_stays_env_only(monkeypatch):
    monkeypatch.setenv("WEB_PORT", "5999")
    assert get_config("WEB_PORT", "5000") == "5999"


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


# ---------------------------------------------------------------------------
# Issue #224 lifecycle regression coverage
# ---------------------------------------------------------------------------


def _resolve_callable(module, candidates: list[str]):
    """Return the first callable attr found in *module* from *candidates*."""
    for name in candidates:
        fn = getattr(module, name, None)
        if callable(fn):
            return fn
    pytest.fail(
        "Expected one of these DB APIs to exist for issue #224: "
        + ", ".join(candidates)
    )


def _extract_job_id_from_dispatch_result(result) -> int | None:
    """Best-effort extraction of a job id from a dispatch return value."""
    if result is None:
        return None
    if isinstance(result, int):
        return result
    if isinstance(result, dict):
        raw = result.get("job_id", result.get("id"))
        return int(raw) if isinstance(raw, int) else None
    if isinstance(result, tuple) and result:
        first = result[0]
        return first if isinstance(first, int) else None
    return None


def test_atomic_claim_next_queued_job_claims_fifo_and_marks_running():
    """Issue #224: queued job claiming must be atomic and FIFO."""
    claim_next = _resolve_callable(
        db_module,
        [
            "claim_next_queued_job",
            "claim_queued_job",
            "claim_next_job",
        ],
    )

    j1 = create_job("mappings/a.hcl")
    j2 = create_job("mappings/b.hcl")

    first = claim_next()
    second = claim_next()
    third = claim_next()

    first_id = first["id"] if isinstance(first, dict) else first
    second_id = second["id"] if isinstance(second, dict) else second

    assert first_id == j1
    assert second_id == j2
    assert third is None

    assert get_job(j1)["status"] == "running"
    assert get_job(j2)["status"] == "running"


def test_dispatch_due_schedule_transactionally_updates_schedule_and_queues_job():
    """Issue #224: due schedule dispatch should advance schedule + queue job together."""
    dispatch_due = _resolve_callable(
        db_module,
        [
            "dispatch_next_due_schedule",
            "dispatch_due_schedule",
            "claim_due_schedule_and_queue_job",
        ],
    )

    past = "2000-01-01T00:00:00"
    sid = create_schedule(
        "dispatch-me",
        "mappings/dispatch.hcl",
        "*/5 * * * *",
        dry_run=True,
        next_run_at=past,
    )
    jobs_before = len(get_jobs())

    result = dispatch_due()
    queued_job_id = _extract_job_id_from_dispatch_result(result)

    schedule = get_schedule(sid)
    assert schedule is not None
    assert schedule["last_run_at"] is not None
    assert schedule["next_run_at"] is not None
    assert schedule["next_run_at"] != past

    jobs_after = get_jobs()
    assert len(jobs_after) == jobs_before + 1
    newest = jobs_after[0]
    assert newest["status"] == "queued"
    assert newest["hcl_file"] == "mappings/dispatch.hcl"
    assert newest["dry_run"] is True

    if queued_job_id is not None:
        assert newest["id"] == queued_job_id
