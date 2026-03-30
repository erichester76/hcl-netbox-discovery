"""
File: collector/job_log_handler.py
Purpose: Logging handler that persists INFO+ log records to the collector DB.
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #web-ui
"""

from __future__ import annotations

import logging

from .db import add_log


class JobLogHandler(logging.Handler):
    """Attach to the root logger during a sync run to capture log records.

    Only records at *INFO* level and above are persisted so that noisy DEBUG
    output does not flood the database, while still providing a useful audit
    trail in the UI.

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
