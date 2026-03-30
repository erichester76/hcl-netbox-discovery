#!/usr/bin/env python3
"""Modular NetBox collector — CLI entry point.

Usage
-----
  python main.py --mapping mappings/vmware.hcl
  python main.py --mapping mappings/xclarity.hcl --dry-run
  python main.py                              # auto-discovers all mappings/*.hcl

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
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from typing import Any


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
    return parser.parse_args(argv)


def _setup_logging(level: str | None = None) -> None:
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(threadName)s] [%(levelname)s] [%(name)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _discover_mappings() -> list[str]:
    """Return all ``mappings/*.hcl`` files relative to this script's directory."""
    base = os.path.dirname(os.path.abspath(__file__))
    pattern = os.path.join(base, "mappings", "*.hcl")
    found = sorted(glob.glob(pattern))
    return found


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
    from collector.job_log_handler import JobLogHandler  # noqa: PLC0415

    init_db()

    # Determine which mapping files to run
    mapping_paths: list[str] = args.mappings or []
    if not mapping_paths:
        mapping_paths = _discover_mappings()
        if not mapping_paths:
            logging.error(
                "No mapping files found.  Use --mapping PATH or place *.hcl "
                "files in the mappings/ directory.  "
                "(Tip: copy a *.hcl.example template and rename it to *.hcl first.)"
            )
            return 1
        logging.info("Auto-discovered %d mapping(s): %s", len(mapping_paths), mapping_paths)

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
        try:
            dry_run = True if args.dry_run else None
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
        except Exception as exc:
            logging.exception("Collector run failed for %s: %s", path, exc)
            exit_code = 1
        finally:
            root_logger.removeHandler(handler)
            finish_job(job_id, success=success, summary=summary if summary else None)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
