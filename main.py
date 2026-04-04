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
    run_token = os.environ.get("COLLECTOR_RUN_TOKEN")

    # Initialise the shared job-tracking database so the web UI can observe
    # jobs started from the CLI (same SQLite file used by web_server.py).
    from collector.db import create_job, init_db  # noqa: PLC0415

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

    exit_code = 0

    for path in mapping_paths:
        dry_run = bool(args.dry_run)
        job_id = create_job(path, dry_run=dry_run, run_token=run_token)
        if not _execute_job(job_id, path, dry_run=dry_run):
            exit_code = 1

    return exit_code


# ---------------------------------------------------------------------------
# Scheduler loop
# ---------------------------------------------------------------------------

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
    from collector.db import dispatch_next_due_schedule  # noqa: PLC0415

    while True:
        job = dispatch_next_due_schedule()
        if job is None:
            return

        logging.info(
            "Queued scheduled job %d for %s (dry_run=%s)",
            job["id"],
            job["hcl_file"],
            job.get("dry_run", False),
        )


def _check_and_run_queued_jobs() -> None:
    """Pick up any jobs with status='queued' (created by the web UI) and run them."""
    from collector.db import claim_next_queued_job  # noqa: PLC0415

    while True:
        job = claim_next_queued_job()
        if job is None:
            return
        job_id = job["id"]
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
    _execute_job(
        job["id"],
        job["hcl_file"],
        dry_run=bool(job.get("dry_run", False)),
        debug_mode=bool(job.get("debug_mode", False)),
        job_already_started=True,
    )


def _summary_from_stats(all_stats: list[Any]) -> tuple[dict[str, Any], bool]:
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
    has_errors = any(s.errored > 0 for s in all_stats)
    return summary, has_errors


def _execute_job(
    job_id: int,
    hcl_file: str,
    *,
    dry_run: bool = False,
    debug_mode: bool = False,
    job_already_started: bool = False,
) -> bool:
    """Run one job end-to-end and persist final job state.

    Returns True when the run completed without a top-level fatal error.
    Item-level errors still return True and are persisted as ``partial``.
    """
    from collector.db import add_log, finish_job, start_job  # noqa: PLC0415
    from collector.job_log_handler import JobLogHandler, job_context  # noqa: PLC0415

    if not job_already_started:
        start_job(job_id)

    if not os.path.isfile(hcl_file):
        logging.error("Mapping file not found: %s", hcl_file)
        add_log(job_id, "ERROR", __name__, f"Mapping file not found: {hcl_file}")
        finish_job(job_id, success=False)
        return False

    root_logger = logging.getLogger()
    original_root_level = root_logger.level
    capture_debug_logs = debug_mode or root_logger.isEnabledFor(logging.DEBUG)
    handler = JobLogHandler(
        job_id,
        min_level=logging.DEBUG if capture_debug_logs else logging.INFO,
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s")
    )
    if capture_debug_logs:
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
        summary, has_errors = _summary_from_stats(all_stats)
        success = True
    except Exception as exc:
        logging.exception("Job %d failed for %s: %s", job_id, hcl_file, exc)
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(original_root_level)
        finish_job(
            job_id,
            success=success,
            summary=summary if summary else None,
            has_errors=has_errors,
        )

    return success


if __name__ == "__main__":
    sys.exit(main())
