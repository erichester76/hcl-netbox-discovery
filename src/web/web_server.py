#!/usr/bin/env python3
"""
File: src/web/web_server.py
Purpose: Entry point for the hcl-netbox-discovery web UI (Flask dev/production server).
Created: 2026-03-30
Last Changed: Copilot 2026-03-30 Issue: #web-ui

Usage
-----
  python -m web.web_server                   # listens on 0.0.0.0:5000
  python -m web.web_server --port 8080
  WEB_PORT=8080 python -m web.web_server

For production, run behind gunicorn::

    gunicorn -w 2 -b 0.0.0.0:5000 "web.app:create_app()"
"""

from __future__ import annotations

import argparse
import logging
import os

from collector import setup_logging


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m web.web_server",
        description="HCL NetBox Discovery – web monitor",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("WEB_PORT", 5000)),
        help="TCP port to listen on (default: 5000 / WEB_PORT env var).",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("WEB_HOST", "0.0.0.0"),
        help="Bind host (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=os.environ.get("FLASK_DEBUG", "false").lower() == "true",
        help="Enable Flask debug mode.",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO").upper(),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    setup_logging(args.log_level)

    from web.app import create_app  # noqa: PLC0415

    app = create_app()
    logging.getLogger(__name__).info(
        "Starting web UI on http://%s:%d  (debug=%s)",
        args.host,
        args.port,
        args.debug,
    )
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
