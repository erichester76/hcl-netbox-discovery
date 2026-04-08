"""Shared helpers for job execution lifecycle, summaries, and log capture."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .db import finish_job
from .job_log_handler import JobLogHandler, job_context


def summary_from_stats(all_stats: list[Any]) -> tuple[dict[str, Any], bool]:
    """Build the persisted summary payload and partial-run flag."""
    summary = {
        s.object_name: {
            "processed": s.processed,
            "created": s.created,
            "updated": s.updated,
            "skipped": s.skipped,
            "errored": s.errored,
            "nested_skipped": dict(sorted(s.nested_skipped.items())),
        }
        for s in all_stats
    }
    has_errors = any(s.errored > 0 for s in all_stats)
    return summary, has_errors


def build_job_artifact(
    *,
    job_id: int,
    hcl_file: str,
    dry_run: bool,
    debug_mode: bool,
    success: bool,
    has_errors: bool,
    summary: dict[str, Any] | None,
    error: str | None = None,
) -> dict[str, Any]:
    """Build the persisted artifact payload for a job run."""
    status = "failed"
    if success:
        status = "partial" if has_errors else "success"
    return {
        "job_id": job_id,
        "hcl_file": hcl_file,
        "dry_run": dry_run,
        "debug_mode": debug_mode,
        "success": success,
        "status": status,
        "has_errors": has_errors,
        "summary": summary or {},
        "error": error,
    }


@contextmanager
def captured_job_logging(job_id: int, *, capture_debug_logs: bool) -> Iterator[None]:
    """Attach the DB log handler and restore the root logger afterwards."""
    root_logger = logging.getLogger()
    original_root_level = root_logger.level
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
    try:
        with job_context(job_id):
            yield
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(original_root_level)


def persist_job_result(
    job_id: int,
    *,
    success: bool,
    summary: dict[str, Any] | None,
    has_errors: bool,
    artifact: dict[str, Any],
    forced_status: str | None = None,
) -> None:
    """Persist terminal job state in one place."""
    finish_job(
        job_id,
        success=success,
        summary=summary if summary else None,
        has_errors=has_errors,
        artifact=artifact,
        forced_status=forced_status,
    )
