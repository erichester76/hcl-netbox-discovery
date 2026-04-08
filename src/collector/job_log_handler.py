"""
File: src/collector/job_log_handler.py
Purpose: Logging handler that persists log records to the collector DB.
Created: 2026-03-30
Last Changed: Copilot 2026-03-31 Issue: #debug-logging
"""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Generator
from contextlib import contextmanager

from .db import add_log

# Context variable that tracks which job is active in the current execution
# context.  This propagates automatically to threads started via
# contextvars.copy_context().run(), which is how the engine's
# ThreadPoolExecutor worker threads inherit the job context.
_current_job_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "current_job_id", default=None
)


@contextmanager
def job_context(job_id: int) -> Generator[None, None, None]:
    """Context manager that binds *job_id* to the current execution context.

    Use this around any code that should have its log records associated with
    *job_id*.  The binding is scoped to the current ``contextvars.Context``
    and is automatically cleared when the ``with`` block exits.
    """
    token = _current_job_id.set(job_id)
    try:
        yield
    finally:
        _current_job_id.reset(token)


class JobLogHandler(logging.Handler):
    """Attach to the root logger during a sync run to capture log records.

    By default only records at *INFO* level and above are persisted so that
    noisy DEBUG output does not flood the database.  When a job is created
    with ``debug_mode=True`` the caller must also lower the root logger's
    effective level to DEBUG and pass ``min_level=logging.DEBUG``; otherwise
    Python's logging framework silently drops DEBUG records at the logger
    level before they ever reach any handler.

    Records are additionally filtered to:
    * Only include log entries whose execution context carries a
      ``current_job_id`` that matches this handler's ``job_id``.  This
      ensures that when multiple mapping jobs run concurrently each job's
      handler only captures log lines produced in its own execution context,
      including lines emitted from ``ThreadPoolExecutor`` worker threads when
      the context is propagated via ``contextvars.copy_context().run()``.

    Parameters
    ----------
    job_id:
        The integer job ID from ``collector.db.create_job()``.
    min_level:
        Minimum log level to store (default ``logging.INFO``).
    """

    def __init__(self, job_id: int, min_level: int = logging.INFO) -> None:
        super().__init__(level=min_level)
        self.job_id = job_id

    def emit(self, record: logging.LogRecord) -> None:
        # Skip records from execution contexts belonging to a different job.
        if _current_job_id.get() != self.job_id:
            return
        try:
            msg = self.format(record)
            add_log(
                job_id=self.job_id,
                level=record.levelname,
                logger_name=record.name,
                message=msg,
            )
        except Exception:
            # Never let logging errors interrupt the sync run
            self.handleError(record)
