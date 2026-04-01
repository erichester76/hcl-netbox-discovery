"""
File: web/app.py
Purpose: Flask web application for monitoring and triggering HCL sync jobs.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #debug-mode
"""

from __future__ import annotations

import glob
import hmac
import logging
import os
import secrets
import sys
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

# Ensure the project root is on sys.path so that collector and lib are importable
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
_LIB = os.path.join(_ROOT, "lib")
if _LIB not in sys.path:
    sys.path.insert(0, _LIB)

from collector.db import (  # noqa: E402
    create_job,
    create_schedule,
    delete_schedule,
    get_config,
    get_job,
    get_job_logs,
    get_jobs,
    get_running_jobs,
    get_schedule,
    get_schedules,
    get_settings_by_group,
    init_db,
    reset_setting,
    set_setting,
    update_schedule,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config["SECRET_KEY"] = get_config("WEB_SECRET_KEY", "dev-change-me")
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.environ.get("WEB_SESSION_COOKIE_SECURE", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    # Initialise the job database on first request
    with app.app_context():
        init_db()

    auth_error = _auth_configuration_error()
    if auth_error:
        raise RuntimeError(auth_error)

    # ------------------------------------------------------------------
    # Jinja2 filters
    # ------------------------------------------------------------------

    @app.template_filter("basename")
    def _basename_filter(path: str) -> str:
        return os.path.basename(path)

    @app.context_processor
    def inject_security_helpers() -> dict[str, Any]:
        return {
            "csrf_token": _csrf_token,
            "web_auth_enabled": _auth_enabled(),
            "web_authenticated": _is_authenticated(),
            "web_username": session.get("username", _configured_username()),
        }

    @app.before_request
    def require_web_auth() -> Any | None:
        if not _auth_enabled():
            return None
        is_auth_exempt = _is_auth_exempt(request.endpoint)
        if not is_auth_exempt and not _is_authenticated():
            return redirect(url_for("login", next=_safe_next_target(request.full_path)))
        if request.method == "POST":
            _validate_csrf()
        return None

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if not _auth_enabled():
            return redirect(url_for("index"))

        next_target = _safe_next_target(request.values.get("next", ""))
        if _is_authenticated():
            return redirect(next_target or url_for("index"))
        if request.method == "POST":
            username = request.form.get("username", "")
            password = request.form.get("password", "")
            if _credentials_match(username, password):
                session.clear()
                session["authenticated"] = True
                session["username"] = username
                _csrf_token()
                return redirect(next_target or url_for("index"))
            return render_template(
                "login.html",
                invalid_credentials=True,
                next_target=next_target or url_for("index"),
            ), 401

        return render_template(
            "login.html",
            invalid_credentials=False,
            next_target=next_target or url_for("index"),
        )

    @app.route("/logout", methods=["POST"])
    def logout():
        session.clear()
        return redirect(url_for("login"))

    @app.route("/")
    def index():
        running = get_running_jobs()
        recent = get_jobs(limit=50)
        # Exclude active jobs from the recent history list
        active_ids = {j["id"] for j in running}
        recent = [j for j in recent if j["id"] not in active_ids]
        mapping_files = _discover_mappings()
        return render_template(
            "index.html",
            running=running,
            recent=recent,
            mapping_files=mapping_files,
        )

    @app.route("/jobs/<int:job_id>")
    def job_detail(job_id: int):
        job = get_job(job_id)
        if job is None:
            return render_template("404.html"), 404
        logs = get_job_logs(job_id)
        return render_template("job_detail.html", job=job, logs=logs)

    @app.route("/jobs/<int:job_id>/logs")
    def job_logs_json(job_id: int):
        """Return logs since *after_id* as JSON (used for live-polling)."""
        after_id = request.args.get("after_id", 0, type=int)
        logs = get_job_logs(job_id)
        new_logs = [lg for lg in logs if lg["id"] > after_id]
        job = get_job(job_id)
        return jsonify(
            {
                "logs": new_logs,
                "status": job["status"] if job else "unknown",
            }
        )

    @app.route("/api/running-jobs")
    def api_running_jobs():
        """Return currently queued/running jobs as JSON (used for dashboard polling)."""
        jobs = get_running_jobs()
        return jsonify({"jobs": jobs, "count": len(jobs)})

    @app.route("/jobs/run", methods=["POST"])
    def run_job():
        hcl_file = request.form.get("hcl_file", "").strip()
        dry_run = request.form.get("dry_run") == "1"
        debug_mode = request.form.get("debug_mode") == "1"
        if not hcl_file:
            return redirect(url_for("index"))

        job_id = _dispatch_job(hcl_file, dry_run, debug_mode)
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/cache")
    def cache_status():
        info = _get_cache_info()
        return render_template("cache.html", cache_info=info)

    @app.route("/cache/flush", methods=["POST"])
    def cache_flush():
        resource = request.form.get("resource", "").strip() or None
        _flush_cache(resource)
        return redirect(url_for("cache_status"))

    @app.route("/cache/prewarm", methods=["POST"])
    def cache_prewarm():
        resource = request.form.get("resource", "").strip() or None
        _prewarm_cache(resource)
        return redirect(url_for("cache_status"))

    # ------------------------------------------------------------------
    # Scheduler routes
    # ------------------------------------------------------------------

    @app.route("/schedules")
    def schedules():
        all_schedules = get_schedules()
        mapping_files = _discover_mappings()
        return render_template(
            "schedules.html",
            schedules=all_schedules,
            mapping_files=mapping_files,
        )

    @app.route("/schedules/create", methods=["POST"])
    def schedule_create():
        name = request.form.get("name", "").strip()
        hcl_file = request.form.get("hcl_file", "").strip()
        cron_expr = request.form.get("cron_expr", "").strip()
        dry_run = request.form.get("dry_run") == "1"

        if not name or not hcl_file or not cron_expr:
            return redirect(url_for("schedules"))

        # Resolve relative paths
        if not os.path.isabs(hcl_file):
            hcl_file = os.path.join(_ROOT, hcl_file)

        next_run = _compute_next_run(cron_expr)
        create_schedule(name, hcl_file, cron_expr, dry_run=dry_run, next_run_at=next_run)
        return redirect(url_for("schedules"))

    @app.route("/schedules/<int:schedule_id>/edit", methods=["GET", "POST"])
    def schedule_edit(schedule_id: int):
        sched = get_schedule(schedule_id)
        if sched is None:
            return render_template("404.html"), 404

        if request.method == "POST":
            name = request.form.get("name", "").strip()
            hcl_file = request.form.get("hcl_file", "").strip()
            cron_expr = request.form.get("cron_expr", "").strip()
            dry_run = request.form.get("dry_run") == "1"
            enabled = request.form.get("enabled") == "1"

            if not name or not hcl_file or not cron_expr:
                return redirect(url_for("schedule_edit", schedule_id=schedule_id))

            if not os.path.isabs(hcl_file):
                hcl_file = os.path.join(_ROOT, hcl_file)

            next_run = _compute_next_run(cron_expr)
            update_schedule(schedule_id, name, hcl_file, cron_expr, dry_run, enabled, next_run)
            return redirect(url_for("schedules"))

        mapping_files = _discover_mappings()
        return render_template(
            "schedule_edit.html",
            sched=sched,
            mapping_files=mapping_files,
        )

    @app.route("/schedules/<int:schedule_id>/delete", methods=["POST"])
    def schedule_delete(schedule_id: int):
        delete_schedule(schedule_id)
        return redirect(url_for("schedules"))

    @app.route("/schedules/<int:schedule_id>/toggle", methods=["POST"])
    def schedule_toggle(schedule_id: int):
        sched = get_schedule(schedule_id)
        if sched:
            next_run = _compute_next_run(sched["cron_expr"]) if not sched["enabled"] else sched["next_run_at"]
            update_schedule(
                schedule_id,
                sched["name"],
                sched["hcl_file"],
                sched["cron_expr"],
                sched["dry_run"],
                not sched["enabled"],
                next_run,
            )
        return redirect(url_for("schedules"))

    @app.route("/schedules/<int:schedule_id>/run-now", methods=["POST"])
    def schedule_run_now(schedule_id: int):
        """Trigger an immediate (on-demand) execution of a schedule."""
        sched = get_schedule(schedule_id)
        if sched is None:
            return render_template("404.html"), 404

        job_id = _dispatch_job(sched["hcl_file"], sched.get("dry_run", False))
        return redirect(url_for("job_detail", job_id=job_id))

    # ------------------------------------------------------------------
    # Settings routes
    # ------------------------------------------------------------------

    @app.route("/settings")
    def settings():
        groups = get_settings_by_group()
        return render_template("settings.html", groups=groups)

    @app.route("/settings/update", methods=["POST"])
    def settings_update():
        key = request.form.get("key", "").strip()
        action = request.form.get("action", "save")
        if key:
            if action == "reset":
                reset_setting(key)
            else:
                value = request.form.get("value", "").strip() or None
                set_setting(key, value)
        return redirect(url_for("settings"))

    # ------------------------------------------------------------------
    # 404 handler
    # ------------------------------------------------------------------

    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404

    return app


# ---------------------------------------------------------------------------
# Job dispatcher – creates a queued DB record for the collector to pick up
# ---------------------------------------------------------------------------


def _dispatch_job(hcl_file: str, dry_run: bool = False, debug_mode: bool = False) -> int:
    """Resolve *hcl_file*, create a 'queued' DB job record, and return its ID.

    The actual execution is handled by the collector container's scheduler loop,
    which polls for queued jobs and runs them.  The web container never invokes
    collector code directly.
    """
    if not os.path.isabs(hcl_file):
        hcl_file = os.path.join(_ROOT, hcl_file)

    return create_job(hcl_file, dry_run=dry_run, debug_mode=debug_mode)


def _auth_enabled() -> bool:
    return os.environ.get("WEB_AUTH_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


def _configured_username() -> str:
    return os.environ.get("WEB_USERNAME", "admin").strip() or "admin"


def _configured_password() -> str:
    return os.environ.get("WEB_PASSWORD", "")


def _configured_password_hash() -> str:
    return os.environ.get("WEB_PASSWORD_HASH", "").strip()


def _auth_configuration_error() -> str | None:
    if not _auth_enabled():
        return None
    if _configured_password_hash() or _configured_password():
        return None
    return "WEB auth is enabled but no credentials are configured. Set WEB_PASSWORD or WEB_PASSWORD_HASH."


def _credentials_match(username: str, password: str) -> bool:
    expected_username = _configured_username()
    if not hmac.compare_digest(username, expected_username):
        return False

    password_hash = _configured_password_hash()
    if password_hash:
        return check_password_hash(password_hash, password)

    expected_password = _configured_password()
    return bool(expected_password) and hmac.compare_digest(password, expected_password)


def _is_authenticated() -> bool:
    return bool(session.get("authenticated"))


def _is_auth_exempt(endpoint: str | None) -> bool:
    return endpoint in {"login", "static"}


def _safe_next_target(target: str) -> str:
    cleaned = (target or "").strip()
    if not cleaned:
        return ""
    if cleaned.startswith("http://") or cleaned.startswith("https://") or cleaned.startswith("//"):
        return ""
    if not cleaned.startswith("/"):
        return ""
    if cleaned.endswith("?"):
        cleaned = cleaned[:-1]
    return cleaned or ""


def _csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def _validate_csrf() -> None:
    supplied = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token", "")
    expected = _csrf_token()
    if not supplied or not hmac.compare_digest(supplied, expected):
        abort(400, description="Invalid CSRF token")


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _env_int(name: str, default: int) -> int:
    """Return *name* from config (DB then env) as int, falling back to *default*."""
    raw = get_config(name, "")
    if raw:
        try:
            return int(raw)
        except ValueError:
            logger.warning("Invalid integer value for %s=%r; using default %d", name, raw, default)
    return default


def _cache_client_kwargs() -> dict[str, Any]:
    """Build kwargs for a pynetbox2 client using cache-related config settings."""
    backend = get_config("NETBOX_CACHE_BACKEND", "none")
    cache_url = get_config("NETBOX_CACHE_URL", "")
    kwargs: dict[str, Any] = dict(
        url=get_config("NETBOX_URL", "http://localhost:8080"),
        token=get_config("NETBOX_TOKEN", ""),
        cache_backend=backend,
        cache_ttl_seconds=_env_int("NETBOX_CACHE_TTL", 300),
        cache_key_prefix=get_config("NETBOX_CACHE_KEY_PREFIX", "nbx:"),
    )
    sentinel_ttl = _env_int("NETBOX_PREWARM_SENTINEL_TTL", 0)
    if sentinel_ttl:
        kwargs["prewarm_sentinel_ttl_seconds"] = sentinel_ttl
    if backend == "redis":
        kwargs["redis_url"] = cache_url or "redis://localhost:6379/0"
    if backend == "sqlite":
        kwargs["sqlite_path"] = cache_url or ".nbx_cache.sqlite3"
    return kwargs


@contextmanager
def _cache_client() -> Generator[Any | None, None, None]:
    """Context manager that yields a pynetbox2 API client configured from config settings.

    Yields ``None`` when the cache backend is ``"none"``.
    """
    import pynetbox2 as pynetbox  # type: ignore[import]  # noqa: PLC0415

    backend = get_config("NETBOX_CACHE_BACKEND", "none")
    if backend == "none":
        yield None
        return

    nb = pynetbox.api(**_cache_client_kwargs())
    try:
        yield nb
    finally:
        nb.close()


def _get_cache_info() -> dict[str, Any]:
    """Return a dict describing the current cache backend and entry counts."""
    try:
        backend = get_config("NETBOX_CACHE_BACKEND", "none")
        if backend == "none":
            return {"backend": "none", "entries": {}, "total": 0}

        with _cache_client() as nb:
            stats = nb.cache_stats()
        total = stats.get("total", 0) if stats else 0
        by_resource = stats.get("by_resource", {}) if stats else {}
        return {"backend": backend, "entries": by_resource, "total": total}
    except Exception as exc:
        return {"backend": "error", "entries": {}, "total": 0, "error": str(exc)}


def _flush_cache(resource: str | None) -> None:
    """Flush the cache for *resource* (or all if *None*)."""
    try:
        with _cache_client() as nb:
            if nb is None:
                return
            if resource:
                nb.cache_flush(resource)
            else:
                nb.cache_flush()
    except Exception as exc:
        logger.warning("Cache flush failed: %s", exc)


def _prewarm_cache(resource: str | None) -> None:
    """Pre-warm the cache for *resource* (or all known resources if *None*).

    When no resource is specified, all resources registered in
    _RESOURCE_TO_PRECACHE_OBJECT_TYPE are eligible for prewarming.  Using
    only the current cache_stats() keys would miss resources that have never
    been cached (e.g. dcim.devices on a fresh instance), creating a
    chicken-and-egg problem where those resources could never be prewarmed
    through the UI.  The sentinel mechanism inside prewarm() prevents
    redundant fetches for resources that were recently prewarmed.
    """
    try:
        with _cache_client() as nb:
            if nb is None:
                return
            if resource:
                resources: list[str] = [resource]
            else:
                # 2026-03-31 Issue #cache: was using cache_stats() keys which excluded
                # resources with 0 entries (e.g. dcim.devices on first run).
                resources = sorted(nb._RESOURCE_TO_PRECACHE_OBJECT_TYPE.keys())
            if resources:
                logger.debug("Pre-warming cache for resources: %s", resources)
                nb.prewarm(resources)
    except Exception as exc:
        logger.warning("Cache pre-warm failed: %s", exc)


# ---------------------------------------------------------------------------
# Mapping file discovery
# ---------------------------------------------------------------------------


def _discover_mappings() -> list[str]:
    """Return HCL mapping files available in ``<root>/mappings/``."""
    pattern = os.path.join(_ROOT, "mappings", "*.hcl")
    return sorted(
        os.path.relpath(p, _ROOT) for p in glob.glob(pattern)
    )


def _compute_next_run(cron_expr: str) -> str | None:
    """Return the ISO-formatted next run time for *cron_expr*, or None on error."""
    try:
        from datetime import datetime, timezone  # noqa: PLC0415

        from croniter import croniter  # noqa: PLC0415

        now = datetime.now(timezone.utc)
        cron = croniter(cron_expr, now)
        next_dt = cron.get_next(datetime)
        return next_dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        logger.warning("Invalid cron expression: %r", cron_expr)
        return None
