"""Shared helpers for job execution lifecycle, summaries, and log capture."""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from .db import finish_job
from .job_log_handler import JobLogHandler, job_context

# Module-level lock and reference count for concurrent debug-capture contexts.
# Only the first debug-capturing job raises the root logger to DEBUG; only the
# last one restores it, so concurrent jobs don't race on the root level.
_debug_capture_lock: threading.Lock = threading.Lock()
_debug_capture_refcount: int = 0
# Actual value is captured from the root logger on the first debug-capture
# context entry (when refcount transitions 0→1) before it is ever read.
_debug_capture_saved_level: int = logging.NOTSET


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
    """Attach the DB log handler and restore the root logger afterwards.

    Thread-safe: multiple concurrent debug-capture contexts share a reference
    count so the root logger level is only raised on the first entry and only
    restored on the last exit.  Non-debug contexts never touch the root level.
    """
    global _debug_capture_refcount, _debug_capture_saved_level  # noqa: PLW0603

    root_logger = logging.getLogger()
    handler = JobLogHandler(
        job_id,
        min_level=logging.DEBUG if capture_debug_logs else logging.INFO,
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s] %(message)s")
    )

    if capture_debug_logs:
        with _debug_capture_lock:
            if _debug_capture_refcount == 0:
                _debug_capture_saved_level = root_logger.level
                root_logger.setLevel(logging.DEBUG)
            _debug_capture_refcount += 1

    root_logger.addHandler(handler)
    try:
        with job_context(job_id):
            yield
    finally:
        root_logger.removeHandler(handler)
        if capture_debug_logs:
            with _debug_capture_lock:
                _debug_capture_refcount -= 1
                if _debug_capture_refcount == 0:
                    root_logger.setLevel(_debug_capture_saved_level)


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
