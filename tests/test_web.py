"""Tests for the web UI Flask application (web.app)."""

from __future__ import annotations

import threading
from urllib.parse import urlparse

import pytest

import collector.db as db_module
from collector.db import (
    add_log,
    create_job,
    finish_job,
    init_db,
    set_setting,
    start_job,
)


@pytest.fixture()
def app(tmp_path, monkeypatch):
    """Create a Flask test client backed by a temporary DB."""
    db_path = str(tmp_path / "test_web.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setenv("WEB_AUTH_ENABLED", "false")
    monkeypatch.setenv("WEB_SECRET_KEY", "test-secret-key")
    monkeypatch.setattr(db_module, "_lock", threading.Lock())
    init_db()

    from web.app import create_app  # noqa: PLC0415

    flask_app = create_app()
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client


@pytest.fixture()
def secured_app(tmp_path, monkeypatch):
    """Create a Flask test client with web auth and CSRF enabled."""
    db_path = str(tmp_path / "test_web_secured.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setenv("WEB_AUTH_ENABLED", "true")
    monkeypatch.setenv("WEB_USERNAME", "admin")
    monkeypatch.setenv("WEB_PASSWORD", "secret")
    monkeypatch.setenv("WEB_SECRET_KEY", "secured-test-secret")
    monkeypatch.setattr(db_module, "_lock", threading.Lock())
    init_db()

    from web.app import create_app  # noqa: PLC0415

    flask_app = create_app()
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as client:
        yield client


def _login(client, username: str = "admin", password: str = "secret"):
    client.get("/login")
    return client.post(
        "/login",
        data={
            "username": username,
            "password": password,
            "next": "/",
            "csrf_token": _csrf_token(client),
        },
    )


def _csrf_token(client) -> str:
    with client.session_transaction() as sess:
        return sess["csrf_token"]


def _post_with_csrf(client, path: str, data: dict[str, str] | None = None):
    payload = dict(data or {})
    payload.setdefault("csrf_token", _csrf_token(client))
    return client.post(path, data=payload)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def test_index_empty(app):
    resp = app.get("/")
    assert resp.status_code == 200
    assert b"HCL NetBox Discovery" in resp.data


def test_login_page_renders_when_auth_enabled(secured_app):
    resp = secured_app.get("/login")
    assert resp.status_code == 200
    assert b"Web UI Login" in resp.data


def test_dashboard_redirects_to_login_when_auth_enabled(secured_app):
    resp = secured_app.get("/")
    assert resp.status_code == 302
    location = resp.headers["Location"]
    parsed = urlparse(location)
    assert parsed.path == "/login"


def test_api_redirect_becomes_401_when_auth_enabled(secured_app):
    resp = secured_app.get("/api/running-jobs")
    assert resp.status_code == 401
    assert resp.get_json() == {"error": "authentication required"}


def test_api_allows_bearer_token_from_db_setting(secured_app):
    set_setting("WEB_API_TOKEN", "api-secret")
    resp = secured_app.get("/api/running-jobs", headers={"Authorization": "Bearer api-secret"})
    assert resp.status_code == 200
    assert resp.get_json()["jobs"] == []


def test_api_allows_x_api_key_from_db_setting(secured_app):
    set_setting("WEB_API_TOKEN", "api-secret")
    resp = secured_app.get("/api/running-jobs", headers={"X-API-Key": "api-secret"})
    assert resp.status_code == 200
    assert resp.get_json()["jobs"] == []


def test_api_rejects_wrong_token(secured_app):
    set_setting("WEB_API_TOKEN", "api-secret")
    resp = secured_app.get("/api/running-jobs", headers={"Authorization": "Bearer wrong-token"})
    assert resp.status_code == 401
    assert resp.get_json() == {"error": "authentication required"}


def test_api_allows_authenticated_session(secured_app):
    _login(secured_app)
    resp = secured_app.get("/api/running-jobs")
    assert resp.status_code == 200


def test_api_job_logs_allows_token_auth(secured_app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    add_log(job_id, "INFO", "engine", "first")
    set_setting("WEB_API_TOKEN", "api-secret")

    resp = secured_app.get(f"/api/jobs/{job_id}/logs", headers={"Authorization": "Bearer api-secret"})

    assert resp.status_code == 200
    assert resp.get_json()["status"] == "running"
    assert resp.get_json()["logs"][0]["message"] == "first"


def test_create_app_requires_non_default_web_password(tmp_path, monkeypatch):
    db_path = str(tmp_path / "test_web_invalid_auth.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setenv("WEB_AUTH_ENABLED", "true")
    monkeypatch.setenv("WEB_USERNAME", "admin")
    monkeypatch.setenv("WEB_PASSWORD", "change-me-in-production")
    monkeypatch.delenv("WEB_PASSWORD_HASH", raising=False)
    monkeypatch.setenv("WEB_SECRET_KEY", "invalid-auth-secret")
    monkeypatch.setattr(db_module, "_lock", threading.Lock())
    init_db()

    from web.app import create_app  # noqa: PLC0415

    with pytest.raises(RuntimeError, match="default placeholder"):
        create_app()


def test_protected_post_redirects_to_login_when_not_authenticated(secured_app):
    from collector.db import get_jobs  # noqa: PLC0415

    resp = secured_app.post("/jobs/run", data={"hcl_file": "mappings/test.hcl"})

    assert resp.status_code == 302
    assert urlparse(resp.headers["Location"]).path == "/login"
    assert get_jobs() == []


def test_login_rejects_invalid_credentials(secured_app):
    resp = _login(secured_app, password="wrong-password")
    assert resp.status_code == 401
    assert b"Invalid username or password" in resp.data


def test_login_post_requires_csrf(secured_app):
    resp = secured_app.post(
        "/login",
        data={"username": "admin", "password": "secret", "next": "/"},
    )

    assert resp.status_code == 400


def test_login_accepts_valid_credentials(secured_app):
    resp = _login(secured_app)
    assert resp.status_code == 302
    assert urlparse(resp.headers["Location"]).path == "/"


def test_authenticated_post_requires_csrf(secured_app):
    from collector.db import get_jobs  # noqa: PLC0415

    _login(secured_app)
    resp = secured_app.post("/jobs/run", data={"hcl_file": "mappings/test.hcl"})

    assert resp.status_code == 400
    assert get_jobs() == []


def test_authenticated_post_with_csrf_dispatches_job(secured_app):
    from collector.db import get_jobs  # noqa: PLC0415

    _login(secured_app)
    resp = _post_with_csrf(secured_app, "/jobs/run", {"hcl_file": "mappings/test.hcl"})

    assert resp.status_code == 302
    jobs = get_jobs()
    assert len(jobs) == 1
    assert jobs[0]["status"] == "queued"


def test_logout_clears_authenticated_session(secured_app):
    _login(secured_app)

    resp = _post_with_csrf(secured_app, "/logout")

    assert resp.status_code == 302
    assert urlparse(resp.headers["Location"]).path == "/login"
    with secured_app.session_transaction() as sess:
        assert sess.get("authenticated") in {None, False}


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


def test_api_jobs_returns_recent_jobs(app):
    first_id = create_job("mappings/first.hcl")
    second_id = create_job("mappings/second.hcl")

    resp = app.get("/api/jobs")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] >= 2
    assert [job["id"] for job in data["jobs"][:2]] == [second_id, first_id]


def test_api_jobs_supports_after_id_and_hcl_file_filter(app):
    skipped_id = create_job("mappings/skip.hcl")
    matching_id = create_job("mappings/azure.hcl")

    resp = app.get(f"/api/jobs?after_id={skipped_id}&hcl_file=mappings/azure.hcl")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 1
    assert [job["id"] for job in data["jobs"]] == [matching_id]
    assert data["jobs"][0]["hcl_file"] == "mappings/azure.hcl"


def test_api_jobs_supports_status_and_limit_filters(app):
    create_job("mappings/queued.hcl")
    running_id = create_job("mappings/running.hcl")
    done_id = create_job("mappings/done.hcl")
    start_job(running_id)
    start_job(done_id)
    finish_job(done_id, success=True)

    resp = app.get("/api/jobs?status=running&limit=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 1
    assert [job["id"] for job in data["jobs"]] == [running_id]


def test_api_job_artifact_returns_persisted_artifact(app):
    artifact = {
        "job_id": 999,
        "status": "success",
        "summary": {"devices": {"processed": 2}},
    }
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    finish_job(job_id, success=True, artifact=artifact)

    resp = app.get(f"/api/jobs/{job_id}/artifact")
    assert resp.status_code == 200
    assert resp.get_json() == {
        "job_id": job_id,
        "status": "success",
        "artifact": artifact,
    }


def test_api_job_artifact_returns_null_when_missing(app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    finish_job(job_id, success=True)

    resp = app.get(f"/api/jobs/{job_id}/artifact")
    assert resp.status_code == 200
    assert resp.get_json() == {
        "job_id": job_id,
        "status": "success",
        "artifact": None,
    }


def test_api_job_artifact_404(app):
    resp = app.get("/api/jobs/99999/artifact")
    assert resp.status_code == 404
    assert resp.get_json() == {"error": "job not found"}


def test_api_job_logs_returns_incremental_logs(app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    add_log(job_id, "INFO", "engine", "first")
    add_log(job_id, "INFO", "engine", "second")

    resp = app.get(f"/api/jobs/{job_id}/logs")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "running"
    assert [entry["message"] for entry in data["logs"]] == ["first", "second"]

    first_id = data["logs"][0]["id"]
    resp = app.get(f"/api/jobs/{job_id}/logs?after_id={first_id}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert [entry["message"] for entry in data["logs"]] == ["second"]


def test_api_job_logs_clamps_negative_after_id(app):
    job_id = create_job("mappings/test.hcl")
    start_job(job_id)
    add_log(job_id, "INFO", "engine", "first")
    add_log(job_id, "INFO", "engine", "second")

    resp = app.get(f"/api/jobs/{job_id}/logs?after_id=-99")

    assert resp.status_code == 200
    data = resp.get_json()
    assert [entry["message"] for entry in data["logs"]] == ["first", "second"]


def test_api_job_logs_404(app):
    resp = app.get("/api/jobs/99999/logs")
    assert resp.status_code == 404
    assert resp.get_json() == {"error": "job not found"}


def test_stop_queued_job_route_marks_job_stopped(app):
    job_id = create_job("mappings/queued.hcl")

    resp = app.post(f"/jobs/{job_id}/stop")

    assert resp.status_code == 302
    job = db_module.get_job(job_id)
    assert job is not None
    assert job["status"] == "stopped"
    assert job["stop_requested"] is True


def test_stop_running_job_route_sets_stop_requested(app):
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)

    resp = app.post(f"/jobs/{job_id}/stop")

    assert resp.status_code == 302
    job = db_module.get_job(job_id)
    assert job is not None
    assert job["status"] == "running"
    assert job["stop_requested"] is True


def test_stop_running_job_route_requires_login_and_csrf(secured_app):
    job_id = create_job("mappings/running.hcl")
    start_job(job_id)

    login_resp = _login(secured_app)
    assert login_resp.status_code == 302

    resp = _post_with_csrf(secured_app, f"/jobs/{job_id}/stop")

    assert resp.status_code == 302
    job = db_module.get_job(job_id)
    assert job is not None
    assert job["stop_requested"] is True


def test_stop_terminal_job_route_returns_404(app):
    job_id = create_job("mappings/done.hcl")
    start_job(job_id)
    finish_job(job_id, success=True)

    resp = app.post(f"/jobs/{job_id}/stop")

    assert resp.status_code == 404


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
