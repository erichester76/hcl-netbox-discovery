"""Modular NetBox collector framework."""

from __future__ import annotations

import logging
import os
import sys


def setup_logging(level: str | None = None) -> None:
    """Configure root logging with a standard format.

    *level* may be a log-level name such as ``"DEBUG"`` or ``"INFO"``.
    When *level* is ``None`` the ``LOG_LEVEL`` environment variable is
    consulted, defaulting to ``"INFO"``.
    """
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(threadName)s] [%(levelname)s] [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
