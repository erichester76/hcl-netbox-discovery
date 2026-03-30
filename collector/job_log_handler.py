"""
File: collector/job_log_handler.py
Purpose: Logging handler that persists INFO+ log records to the collector DB.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #web-ui
"""

from __future__ import annotations

import logging
import threading

from .db import add_log

class JobLogHandler(logging.Handler):
    """Attach to the root logger during a sync run to capture log records.

    Only records at *INFO* level and above are persisted so that noisy DEBUG
    output does not flood the database, while still providing a useful audit
    trail in the UI.

    Records are additionally filtered to:
    * Only include log entries whose logger name starts with ``collector``
      (i.e. originating from the collector package, not from Flask, Redis,
      werkzeug, or any other third-party library running in the same process).
    * Only include log entries emitted from the same OS thread that created
      this handler.  This ensures that when multiple mapping jobs run
      concurrently each job's handler does not accidentally capture log lines
      produced by other in-flight jobs.

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
        # Capture the thread identity at construction time so that only log
        # records emitted from the job's own thread are persisted.
        self._thread_id: int = threading.get_ident()

    def emit(self, record: logging.LogRecord) -> None:
        # Skip records from other threads (concurrent jobs sharing the process).
        if record.thread != self._thread_id:
            return
        # Skip records that do not originate from the collector package.
        if not (record.name == "collector" or record.name.startswith("collector.")):
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
