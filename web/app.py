"""
File: web/app.py
Purpose: Flask web application for monitoring and triggering HCL sync jobs.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #web-ui
"""

from __future__ import annotations

import glob
import logging
import os
import sys
import threading
from typing import Any

from flask import Flask, jsonify, redirect, render_template, request, url_for

# Ensure the project root is on sys.path so that collector and lib are importable
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
_LIB = os.path.join(_ROOT, "lib")
if _LIB not in sys.path:
    sys.path.insert(0, _LIB)

from collector.db import (  # noqa: E402
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
from collector.job_log_handler import JobLogHandler  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates")
    app.config["SECRET_KEY"] = os.environ.get("WEB_SECRET_KEY", "dev-change-me")

    # Initialise the job database on first request
    with app.app_context():
        init_db()

    # ------------------------------------------------------------------
    # Jinja2 filters
    # ------------------------------------------------------------------

    @app.template_filter("basename")
    def _basename_filter(path: str) -> str:
        return os.path.basename(path)

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

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
        if not hcl_file:
            return redirect(url_for("index"))

        # Resolve to an absolute path relative to the project root
        if not os.path.isabs(hcl_file):
            hcl_file = os.path.join(_ROOT, hcl_file)

        if not os.path.isfile(hcl_file):
            # Unknown file – still create a failed job so the error is visible
            job_id = create_job(hcl_file)
            start_job(job_id)
            finish_job(job_id, success=False)
            add_log(
                job_id,
                "ERROR",
                __name__,
                f"Mapping file not found: {hcl_file}",
            )
            return redirect(url_for("job_detail", job_id=job_id))

        job_id = create_job(hcl_file)
        t = threading.Thread(
            target=_run_job_background,
            args=(job_id, hcl_file, dry_run),
            daemon=True,
            name=f"sync-job-{job_id}",
        )
        t.start()
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

    # ------------------------------------------------------------------
    # 404 handler
    # ------------------------------------------------------------------

    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404

    return app


# ---------------------------------------------------------------------------
# Background job runner
# ---------------------------------------------------------------------------


def _run_job_background(job_id: int, hcl_file: str, dry_run: bool = False) -> None:
    """Run an Engine sync in a daemon thread, logging to the DB."""
    start_job(job_id)

    handler = JobLogHandler(job_id)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s")
    )
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    summary: dict[str, Any] = {}
    success = False
    try:
        from collector.engine import Engine  # noqa: PLC0415

        engine = Engine()
        all_stats = engine.run(hcl_file, dry_run_override=dry_run if dry_run else None)
        summary = {
            s.object_name: {
                "processed": s.processed,
                "created": s.created,
                "updated": s.updated,
                "skipped": s.skipped,
                "errored": s.errored,
            }
            for s in all_stats
        }
        success = True
    except Exception as exc:
        logger.exception("Sync job %d failed: %s", job_id, exc)
    finally:
        root_logger.removeHandler(handler)
        finish_job(job_id, success=success, summary=summary if summary else None)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _get_cache_info() -> dict[str, Any]:
    """Return a dict describing the current cache backend and entry counts."""
    try:
        lib_dir = os.path.join(_ROOT, "lib")
        if lib_dir not in sys.path:
            sys.path.insert(0, lib_dir)
        import pynetbox2 as pynetbox  # type: ignore[import]

        backend = os.environ.get("NETBOX_CACHE_BACKEND", "none")
        cache_url = os.environ.get("NETBOX_CACHE_URL", "")

        if backend == "none":
            return {"backend": "none", "entries": {}, "total": 0}

        # Build a temporary client just to inspect the cache
        url = os.environ.get("NETBOX_URL", "http://localhost:8080")
        token = os.environ.get("NETBOX_TOKEN", "")
        kwargs: dict[str, Any] = dict(
            url=url,
            token=token,
            cache_backend=backend,
        )
        if backend == "redis":
            kwargs["redis_url"] = cache_url or "redis://localhost:6379/0"
        if backend == "sqlite":
            kwargs["sqlite_path"] = cache_url or ".nbx_cache.sqlite3"

        nb = pynetbox.api(**kwargs)
        stats = nb.cache_stats() if hasattr(nb, "cache_stats") else {}
        nb.close()
        total = sum(stats.values()) if stats else 0
        return {"backend": backend, "entries": stats, "total": total}
    except Exception as exc:
        return {"backend": "error", "entries": {}, "total": 0, "error": str(exc)}


def _flush_cache(resource: str | None) -> None:
    """Flush the cache for *resource* (or all if *None*)."""
    try:
        lib_dir = os.path.join(_ROOT, "lib")
        if lib_dir not in sys.path:
            sys.path.insert(0, lib_dir)
        import pynetbox2 as pynetbox  # type: ignore[import]

        backend = os.environ.get("NETBOX_CACHE_BACKEND", "none")
        if backend == "none":
            return

        url = os.environ.get("NETBOX_URL", "http://localhost:8080")
        token = os.environ.get("NETBOX_TOKEN", "")
        cache_url = os.environ.get("NETBOX_CACHE_URL", "")
        kwargs: dict[str, Any] = dict(url=url, token=token, cache_backend=backend)
        if backend == "redis":
            kwargs["redis_url"] = cache_url or "redis://localhost:6379/0"
        if backend == "sqlite":
            kwargs["sqlite_path"] = cache_url or ".nbx_cache.sqlite3"

        nb = pynetbox.api(**kwargs)
        if resource:
            nb.cache_flush(resource)
        else:
            nb.cache_flush()
        nb.close()
    except Exception as exc:
        logger.warning("Cache flush failed: %s", exc)


# ---------------------------------------------------------------------------
# Mapping file discovery
# ---------------------------------------------------------------------------


def _discover_mappings() -> list[str]:
    """Return HCL mapping files available in ``<root>/mappings/``."""
    pattern = os.path.join(_ROOT, "mappings", "*.hcl")
    return sorted(
        os.path.relpath(p, _ROOT) for p in glob.glob(pattern)
    )
