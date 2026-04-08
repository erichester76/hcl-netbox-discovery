"""Shared helpers for job execution lifecycle, summaries, and log capture."""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

import tomllib

from .config import build_source_groups, load_config
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
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_MASKED = "********"
_SENSITIVE_KEY_FRAGMENTS = ("password", "pass", "token", "secret", "client_secret", "api_key")


def summary_from_stats(all_stats: list[Any]) -> tuple[dict[str, Any], bool]:
    """Build the persisted summary payload and partial-run flag."""
    summary: dict[str, dict[str, Any]] = {}
    for stat in all_stats:
        bucket = summary.setdefault(
            stat.object_name,
            {
                "processed": 0,
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "nested_skipped": {},
            },
        )
        _accumulate_stat_bucket(bucket, stat)

    has_errors = any(s.errored > 0 for s in all_stats)
    return summary, has_errors


def _accumulate_stat_bucket(bucket: dict[str, Any], stat: Any) -> None:
    """Accumulate counters from one stats object into *bucket*."""
    bucket["processed"] += stat.processed
    bucket["created"] += stat.created
    bucket["updated"] += stat.updated
    bucket["skipped"] += stat.skipped
    bucket["errored"] += stat.errored
    for reason, count in stat.nested_skipped.items():
        bucket["nested_skipped"][reason] = bucket["nested_skipped"].get(reason, 0) + count
    bucket["nested_skipped"] = dict(sorted(bucket["nested_skipped"].items()))


def iterations_from_stats(all_stats: list[Any]) -> dict[str, dict[str, Any]]:
    """Build per-source summary breakdown keyed by source URL."""
    per_source: dict[str, dict[str, Any]] = {}
    for stat in all_stats:
        source_url = getattr(stat, "source_url", None)
        if not isinstance(source_url, str) or not source_url:
            continue

        entry = per_source.setdefault(source_url, {"summary": {}})
        bucket = entry["summary"].setdefault(
            stat.object_name,
            {
                "processed": 0,
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "errored": 0,
                "nested_skipped": {},
            },
        )
        _accumulate_stat_bucket(bucket, stat)

    return per_source


def build_job_artifact(
    *,
    job_id: int,
    hcl_file: str,
    dry_run: bool,
    debug_mode: bool,
    success: bool,
    has_errors: bool,
    summary: dict[str, Any] | None,
    runtime_snapshot: dict[str, Any] | None,
    code_version: dict[str, Any] | None,
    all_stats: list[Any] | None = None,
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
        "iterations": iterations_from_stats(all_stats or []),
        "runtime_snapshot": runtime_snapshot,
        "code_version": code_version,
        "error": error,
    }


def capture_job_runtime_metadata(
    *,
    hcl_file: str,
    dry_run: bool,
    debug_mode: bool,
    run_token: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return masked runtime snapshot and code version metadata for one job."""
    return (
        _build_runtime_snapshot(
            hcl_file=hcl_file,
            dry_run=dry_run,
            debug_mode=debug_mode,
            run_token=run_token,
        ),
        _build_code_version(),
    )


def _build_runtime_snapshot(
    *,
    hcl_file: str,
    dry_run: bool,
    debug_mode: bool,
    run_token: str | None,
) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "job": {
            "hcl_file": hcl_file,
            "dry_run": dry_run,
            "debug_mode": debug_mode,
            "run_token": run_token,
        },
        "mapping": {
            "path": hcl_file,
            "exists": os.path.isfile(hcl_file),
        },
    }

    if not os.path.isfile(hcl_file):
        return snapshot

    try:
        cfg = load_config(hcl_file)
        source_groups = build_source_groups(cfg)
    except Exception as exc:
        logging.exception("Failed to load or build configuration for runtime snapshot: %s", hcl_file)
        snapshot["config_error"] = f"Configuration loading failed ({type(exc).__name__})"
        return snapshot

    snapshot["config"] = _mask_sensitive_values(
        {
            "source": asdict(cfg.source),
            "netbox": asdict(cfg.netbox),
            "collector": asdict(cfg.collector),
            "source_label": cfg.source_label,
        }
    )
    snapshot["execution_plan"] = {
        "source_groups": [
            {
                "max_workers": max_workers,
                "source_urls": [_sanitize_url(source.url) for source in sources],
            }
            for sources, max_workers in source_groups
        ]
    }
    return snapshot


def _build_code_version() -> dict[str, Any]:
    return {
        "version": _read_project_version(),
        "git_commit": _git_output("rev-parse", "HEAD"),
        "git_branch": _git_output("rev-parse", "--abbrev-ref", "HEAD"),
        "git_tag": _git_output("describe", "--tags", "--exact-match"),
    }


def _read_project_version() -> str | None:
    pyproject_path = _PROJECT_ROOT / "pyproject.toml"
    try:
        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        poetry = data.get("tool", {}).get("poetry", {})
        version = poetry.get("version")
        return str(version) if version else None
    except Exception:
        return None


def _git_output(*args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=_PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
    except OSError:
        return None
    value = result.stdout.strip()
    return value or None if result.returncode == 0 else None


def _mask_sensitive_values(value: Any, key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {
            item_key: _mask_sensitive_values(item_value, item_key)
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_mask_sensitive_values(item, key) for item in value]
    if key and any(fragment in key.lower() for fragment in _SENSITIVE_KEY_FRAGMENTS):
        return _MASKED if value not in (None, "") else value
    if isinstance(value, str) and key == "url":
        return _sanitize_url(value)
    return value


def _sanitize_url(value: str) -> str:
    """Redact userinfo and obvious secret query params from URLs."""
    try:
        parts = urlsplit(value)
    except Exception:
        return value

    if not parts.scheme or not parts.netloc:
        return value

    hostname = parts.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    port = f":{parts.port}" if parts.port is not None else ""
    userinfo = ""
    if parts.username:
        userinfo = parts.username
        if parts.password is not None:
            userinfo += f":{_MASKED}"
        userinfo += "@"

    query_pairs = [
        (
            key,
            _MASKED
            if any(fragment in key.lower() for fragment in _SENSITIVE_KEY_FRAGMENTS)
            else val,
        )
        for key, val in parse_qsl(parts.query, keep_blank_values=True)
    ]
    query = "&".join(
        f"{quote(key, safe='')}={quote(val, safe='*')}"
        for key, val in query_pairs
    )

    return urlunsplit((parts.scheme, f"{userinfo}{hostname}{port}", parts.path, query, parts.fragment))


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
                    _debug_capture_saved_level = logging.NOTSET


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
