#!/usr/bin/env python3
"""
File: main.py
Purpose: Modular NetBox collector — CLI entry point.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #debug-mode

Usage
-----
  python main.py --mapping mappings/vmware.hcl
  python main.py --mapping mappings/xclarity.hcl --dry-run
  python main.py --run-scheduler

Getting started
---------------
  Mapping files ship as ``*.hcl.example`` templates.  Copy and rename to
  ``*.hcl`` before running::

      cp mappings/vmware.hcl.example mappings/vmware.hcl
      # edit mappings/vmware.hcl to add your environment variables

Options
-------
  --mapping PATH        Path to a specific HCL mapping file.  May be repeated
                        to run multiple mappings in sequence.
  --dry-run             Log payloads without writing anything to NetBox.
                        Overrides the dry_run setting in the mapping file.
  --log-level LEVEL     Logging verbosity: DEBUG, INFO, WARNING, ERROR.
                        Defaults to the LOG_LEVEL environment variable, or
                        INFO if that is not set.
  --run-scheduler       Run the scheduler loop, executing HCL jobs per their
                        stored cron schedules.  This is the default mode used
                        by the Docker container.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading
import time
from typing import Any

from collector import setup_logging as _setup_logging


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="HCL-driven modular NetBox collector",
    )
    parser.add_argument(
        "--mapping",
        dest="mappings",
        action="append",
        metavar="PATH",
        help="HCL mapping file to run.  May be specified multiple times.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log payloads without writing to NetBox.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO").upper(),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help=(
            "Logging verbosity (default: LOG_LEVEL env var, or INFO). "
            "Choices: DEBUG, INFO, WARNING, ERROR."
        ),
    )
    parser.add_argument(
        "--run-scheduler",
        action="store_true",
        default=False,
        help=(
            "Run the scheduler loop.  Checks the database for due cron schedules "
            "every 60 seconds and executes them.  Runs until interrupted."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _setup_logging(args.log_level)

    # Ensure lib/ is on sys.path so collector can import pynetbox2
    here = os.path.dirname(os.path.abspath(__file__))
    lib_path = os.path.join(here, "lib")
    if lib_path not in sys.path:
        sys.path.insert(0, lib_path)

    # Initialise the shared job-tracking database so the web UI can observe
    # jobs started from the CLI (same SQLite file used by web_server.py).
    from collector.db import create_job, finish_job, init_db, start_job  # noqa: PLC0415
    from collector.job_log_handler import JobLogHandler, job_context  # noqa: PLC0415

    init_db()

    # ------------------------------------------------------------------
    # Scheduler mode: run continuously, firing jobs per cron schedule.
    # ------------------------------------------------------------------
    if args.run_scheduler:
        return _run_scheduler()

    # ------------------------------------------------------------------
    # Manual mode: run one or more explicitly specified mapping files.
    # ------------------------------------------------------------------
    mapping_paths: list[str] = args.mappings or []
    if not mapping_paths:
        logging.error(
            "No mapping files specified.  Use --mapping PATH to run a specific HCL file, "
            "or --run-scheduler to execute jobs from the schedule database."
        )
        return 1

    from collector.engine import Engine  # noqa: PLC0415

    engine = Engine()
    exit_code = 0

    for path in mapping_paths:
        if not os.path.isfile(path):
            logging.error("Mapping file not found: %s", path)
            exit_code = 1
            continue

        job_id = create_job(path)
        start_job(job_id)

        handler = JobLogHandler(job_id)
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s")
        )
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

        summary: dict[str, Any] = {}
        success = False
        has_errors = False
        try:
            dry_run = True if args.dry_run else None
            with job_context(job_id):
                all_stats = engine.run(path, dry_run_override=dry_run)
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
            has_errors = any(s.errored > 0 for s in all_stats)
        except Exception as exc:
            logging.exception("Collector run failed for %s: %s", path, exc)
            exit_code = 1
        finally:
            root_logger.removeHandler(handler)
            finish_job(
                job_id,
                success=success,
                summary=summary if summary else None,
                has_errors=has_errors,
            )

    return exit_code


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------

# Set of schedule IDs whose jobs are currently running (prevents double-fire).
_active_schedule_ids: set[int] = set()
_active_lock = threading.Lock()

# Set of job IDs (created by the web UI) currently being processed.
_active_queued_job_ids: set[int] = set()
_active_queued_lock = threading.Lock()


def _run_scheduler(poll_interval: int = 60) -> int:
    """Run the scheduler loop indefinitely, checking for due jobs every *poll_interval* seconds."""
    logging.info("Scheduler started.  Polling for due jobs every %d s.", poll_interval)
    while True:
        try:
            _check_and_fire_due_schedules()
            _check_and_run_queued_jobs()
        except Exception as exc:
            logging.exception("Scheduler poll error: %s", exc)
        time.sleep(poll_interval)


def _check_and_fire_due_schedules() -> None:
    from collector.db import get_due_schedules, update_schedule_run  # noqa: PLC0415

    due = get_due_schedules()
    if not due:
        return

    for sched in due:
        sid = sched["id"]
        with _active_lock:
            if sid in _active_schedule_ids:
                continue  # already running
            _active_schedule_ids.add(sid)

        # Advance next_run_at immediately so that concurrent scheduler instances
        # (or a fast second poll) don't double-fire the same schedule.
        now_str, next_str = _advance_schedule(sched)
        update_schedule_run(sid, now_str, next_str)

        t = threading.Thread(
            target=_run_scheduled_job,
            args=(sched,),
            daemon=True,
            name=f"sched-{sid}",
        )
        t.start()
        logging.info(
            "Fired scheduled job for '%s' (%s).  Next run: %s",
            sched["name"],
            sched["hcl_file"],
            next_str,
        )


def _advance_schedule(sched: dict[str, Any]) -> tuple[str, str]:
    """Return (now_str, next_run_str) for the given schedule."""
    from datetime import datetime, timezone  # noqa: PLC0415

    from croniter import croniter  # noqa: PLC0415

    now = datetime.now(timezone.utc)
    now_str = now.strftime("%Y-%m-%dT%H:%M:%S")
    cron = croniter(sched["cron_expr"], now)
    next_dt = cron.get_next(datetime)
    next_str = next_dt.strftime("%Y-%m-%dT%H:%M:%S")
    return now_str, next_str


def _run_scheduled_job(sched: dict[str, Any]) -> None:
    """Execute a single scheduled HCL mapping file, recording the job in the DB."""
    from collector.db import create_job, finish_job, start_job  # noqa: PLC0415
    from collector.job_log_handler import JobLogHandler, job_context  # noqa: PLC0415

    hcl_file = sched["hcl_file"]
    dry_run: bool = sched.get("dry_run", False)

    if not os.path.isfile(hcl_file):
        logging.error("Scheduled mapping file not found: %s", hcl_file)
        with _active_lock:
            _active_schedule_ids.discard(sched["id"])
        return

    job_id = create_job(hcl_file)
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
        with job_context(job_id):
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
        logging.exception("Scheduled job failed for %s: %s", hcl_file, exc)
    finally:
        root_logger.removeHandler(handler)
        finish_job(job_id, success=success, summary=summary if summary else None)
        with _active_lock:
            _active_schedule_ids.discard(sched["id"])


def _check_and_run_queued_jobs() -> None:
    """Pick up any jobs with status='queued' (created by the web UI) and run them."""
    from collector.db import get_queued_jobs  # noqa: PLC0415

    queued = get_queued_jobs()
    if not queued:
        return

    for job in queued:
        job_id = job["id"]
        with _active_queued_lock:
            if job_id in _active_queued_job_ids:
                continue  # already being processed
            _active_queued_job_ids.add(job_id)

        t = threading.Thread(
            target=_run_queued_job,
            args=(job,),
            daemon=True,
            name=f"queued-job-{job_id}",
        )
        t.start()
        logging.info(
            "Running on-demand queued job %d for %s (dry_run=%s, debug_mode=%s)",
            job_id,
            job["hcl_file"],
            job.get("dry_run", False),
            job.get("debug_mode", False),
        )


def _run_queued_job(job: dict[str, Any]) -> None:
    """Execute a single on-demand queued job created by the web UI."""
    from collector.db import add_log, finish_job, start_job  # noqa: PLC0415
    from collector.job_log_handler import JobLogHandler, job_context  # noqa: PLC0415

    job_id = job["id"]
    hcl_file = job["hcl_file"]
    dry_run: bool = job.get("dry_run", False)
    debug_mode: bool = job.get("debug_mode", False)

    start_job(job_id)

    if not os.path.isfile(hcl_file):
        logging.error("Queued mapping file not found: %s", hcl_file)
        add_log(job_id, "ERROR", __name__, f"Mapping file not found: {hcl_file}")
        finish_job(job_id, success=False)
        with _active_queued_lock:
            _active_queued_job_ids.discard(job_id)
        return

    min_level = logging.DEBUG if debug_mode else logging.INFO
    handler = JobLogHandler(job_id, min_level=min_level)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s")
    )
    root_logger = logging.getLogger()
    # Save the root logger's current effective level so it can be restored after
    # the job completes.  When debug_mode is enabled we must lower the root
    # logger's level to DEBUG; otherwise Python's logging framework silently
    # drops DEBUG records before they ever reach any handler, so the
    # JobLogHandler would never see them even though its own level is DEBUG.
    original_root_level = root_logger.level
    if debug_mode:
        root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)

    summary: dict[str, Any] = {}
    success = False
    has_errors = False
    try:
        from collector.engine import Engine  # noqa: PLC0415

        engine = Engine()
        with job_context(job_id):
            all_stats = engine.run(hcl_file, dry_run_override=dry_run or None)
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
        has_errors = any(s.errored > 0 for s in all_stats)
    except Exception as exc:
        logging.exception("Queued job %d failed for %s: %s", job_id, hcl_file, exc)
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(original_root_level)
        finish_job(
            job_id,
            success=success,
            summary=summary if summary else None,
            has_errors=has_errors,
        )
        with _active_queued_lock:
            _active_queued_job_ids.discard(job_id)


if __name__ == "__main__":
    sys.exit(main())