#!/usr/bin/env python3
"""CLI entry point for ad-hoc collector runs and the scheduler worker."""

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
    from collector.db import reconcile_stale_running_jobs  # noqa: PLC0415

    reconciled = reconcile_stale_running_jobs()
    if reconciled:
        logging.warning(
            "Reconciled %d stale running job(s) after worker startup: %s",
            len(reconciled),
            ", ".join(str(job_id) for job_id in reconciled),
        )
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
    from collector.db import add_log, job_stop_requested, start_job  # noqa: PLC0415
    from collector.job_lifecycle import (  # noqa: PLC0415
        build_job_artifact,
        captured_job_logging,
        persist_job_result,
        summary_from_stats,
    )

    if not job_already_started:
        start_job(job_id)

    if not os.path.isfile(hcl_file):
        logging.error("Mapping file not found: %s", hcl_file)
        add_log(job_id, "ERROR", __name__, f"Mapping file not found: {hcl_file}")
        persist_job_result(
            job_id,
            success=False,
            summary=None,
            has_errors=False,
            artifact=build_job_artifact(
                job_id=job_id,
                hcl_file=hcl_file,
                dry_run=dry_run,
                debug_mode=debug_mode,
                success=False,
                has_errors=False,
                summary=None,
                error=f"Mapping file not found: {hcl_file}",
            ),
        )
        return False

    summary: dict[str, Any] = {}
    success = False
    has_errors = False
    error_message: str | None = None
    stopped = False
    try:
        from collector.engine import Engine  # noqa: PLC0415

        engine = Engine()

        def stop_checker() -> bool:
            return job_stop_requested(job_id)

        capture_debug_logs = debug_mode or (
            not job_already_started and logging.getLogger().isEnabledFor(logging.DEBUG)
        )
        with captured_job_logging(job_id, capture_debug_logs=capture_debug_logs):
            all_stats = engine.run(hcl_file, dry_run_override=dry_run or None, stop_requested=stop_checker)
        summary, has_errors = summary_from_stats(all_stats)
        stopped = getattr(engine, "stop_requested", False) is True
        success = True
    except Exception as exc:
        error_message = str(exc)
        logging.exception("Job %d failed for %s: %s", job_id, hcl_file, exc)
    finally:
        persist_job_result(
            job_id,
            success=success,
            summary=summary,
            has_errors=has_errors,
            artifact=build_job_artifact(
                job_id=job_id,
                hcl_file=hcl_file,
                dry_run=dry_run,
                debug_mode=debug_mode,
                success=success,
                has_errors=has_errors,
                summary=summary if summary else None,
                error=error_message,
            ),
            forced_status="stopped" if stopped and success else None,
        )

    return success


if __name__ == "__main__":
    sys.exit(main())
