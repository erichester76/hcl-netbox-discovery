#!/usr/bin/env python3
"""
File: src/capture_sync_job.py
Purpose: Run a collector sync on the Docker host and export a pullable artifact bundle.
Created: 2026-04-04
Author: Codex
Last Changed: Codex Issue: #capture-feedback-loop
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, TextIO
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_ROOT = REPO_ROOT / "run-artifacts"
DEFAULT_COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
CONTAINER_APP_ROOT = PurePosixPath("/app")
CONTAINER_DB_PATH = "/app/data/collector_jobs.sqlite3"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a real collector sync inside the live Docker Compose collector "
            "service and write a completed artifact bundle on the Docker host."
        ),
    )
    parser.add_argument(
        "mapping",
        help="Path to the HCL mapping file on the host.",
    )
    parser.add_argument(
        "--artifact-root",
        default=str(DEFAULT_ARTIFACT_ROOT),
        help="Directory on the Docker host where run artifact bundles are written.",
    )
    parser.add_argument(
        "--compose-file",
        default=str(DEFAULT_COMPOSE_FILE),
        help="Path to docker-compose.yml for the target environment.",
    )
    parser.add_argument(
        "--project-directory",
        default=str(REPO_ROOT),
        help="Docker Compose project directory.",
    )
    parser.add_argument(
        "--service",
        default="collector",
        help="Docker Compose service that runs sync jobs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pass --dry-run through to the collector run.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Collector CLI log level for the captured run.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=7200,
        help="Maximum wall-clock runtime before the capture command aborts.",
    )
    parser.add_argument(
        "--done-marker-name",
        default="DONE",
        help="Filename written last to mark a bundle as complete and ready for pull.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    project_directory = Path(args.project_directory).expanduser().resolve()
    compose_file = Path(args.compose_file).expanduser().resolve()
    artifact_root = Path(args.artifact_root).expanduser().resolve()
    mapping_path = Path(args.mapping).expanduser()
    if not mapping_path.is_absolute():
        mapping_path = (project_directory / mapping_path).resolve()
    else:
        mapping_path = mapping_path.resolve()

    if not compose_file.is_file():
        raise SystemExit(f"Compose file not found: {compose_file}")
    if not mapping_path.is_file():
        raise SystemExit(f"Mapping file not found: {mapping_path}")

    container_mapping_path = _container_mapping_path(project_directory, mapping_path)
    compose_base = _compose_base(project_directory, compose_file)
    _ensure_service_running(compose_base, args.service)

    start_utc = datetime.now(timezone.utc)
    artifact_dir = _artifact_dir(artifact_root, mapping_path, start_utc)
    artifact_dir.mkdir(parents=True, exist_ok=False)

    baseline_job_id = _query_max_job_id(compose_base, args.service)
    env_context = _query_env_context(compose_base, args.service)

    stdout_path = artifact_dir / "stdout.log"
    stderr_path = artifact_dir / "stderr.log"

    collector_command = [
        "python",
        "main.py",
        "--mapping",
        str(container_mapping_path),
        "--log-level",
        args.log_level,
    ]
    if args.dry_run:
        collector_command.append("--dry-run")

    exec_command = [*compose_base, "exec", "-T", args.service, *collector_command]

    exit_code = _run_and_tee(
        exec_command,
        project_directory,
        stdout_path,
        stderr_path,
        timeout_seconds=args.timeout_seconds,
    )

    end_utc = datetime.now(timezone.utc)
    job = _query_job_after(
        compose_base=compose_base,
        service=args.service,
        baseline_job_id=baseline_job_id,
        container_mapping_path=str(container_mapping_path),
    )
    db_slice: dict[str, Any] | None = None
    if job is not None:
        db_slice = _query_db_slice(compose_base, args.service, int(job["id"]))

    manifest = {
        "mapping_host_path": str(mapping_path),
        "mapping_container_path": str(container_mapping_path),
        "artifact_directory": str(artifact_dir),
        "started_at_utc": start_utc.isoformat(),
        "finished_at_utc": end_utc.isoformat(),
        "collector_service": args.service,
        "compose_file": str(compose_file),
        "project_directory": str(project_directory),
        "collector_command": collector_command,
        "docker_exec_command": exec_command,
        "exit_code": exit_code,
        "baseline_job_id": baseline_job_id,
        "job_id": None if job is None else job["id"],
        "job_status": None if job is None else job["status"],
        "job_created_at": None if job is None else job["created_at"],
        "job_started_at": None if job is None else job["started_at"],
        "job_finished_at": None if job is None else job["finished_at"],
        "repo_git_sha": _git_sha(project_directory),
        "dry_run": args.dry_run,
        "log_level": args.log_level,
    }

    _write_json(artifact_dir / "manifest.json", manifest)
    _write_json(artifact_dir / "env-context.json", _build_env_context(env_context, manifest))
    _write_json(artifact_dir / "collector-db-slice.json", db_slice)
    _write_json(artifact_dir / "job-summary.json", None if job is None else job.get("summary"))
    _write_db_log_text(artifact_dir / "job_logs.log", db_slice)
    _write_done_marker(artifact_dir / args.done_marker_name, manifest)

    if job is None:
        print(
            f"Capture finished, but no new DB job was found for {container_mapping_path}. "
            f"Artifacts are in {artifact_dir}",
            file=sys.stderr,
        )
        return exit_code or 1

    print(f"Artifacts written to {artifact_dir}")
    return exit_code


def _container_mapping_path(project_directory: Path, mapping_path: Path) -> PurePosixPath:
    try:
        relative_mapping = mapping_path.relative_to(project_directory)
    except ValueError as exc:
        raise SystemExit(
            "Mapping file must live under the Docker Compose project directory so it is visible "
            "inside the collector container."
        ) from exc

    if relative_mapping.parts[0] != "mappings":
        raise SystemExit(
            "Mapping file must live under the project's mappings/ directory for container capture."
        )
    return CONTAINER_APP_ROOT / PurePosixPath(relative_mapping.as_posix())


def _compose_base(project_directory: Path, compose_file: Path) -> list[str]:
    return [
        "docker",
        "compose",
        "--project-directory",
        str(project_directory),
        "-f",
        str(compose_file),
    ]


def _ensure_service_running(compose_base: list[str], service: str) -> None:
    result = subprocess.run(
        [*compose_base, "ps", "--services", "--status", "running"],
        check=True,
        capture_output=True,
        text=True,
    )
    running_services = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    if service not in running_services:
        raise SystemExit(
            f"Docker Compose service '{service}' is not running. Start it before capturing jobs."
        )


def _query_max_job_id(compose_base: list[str], service: str) -> int:
    payload = _exec_python_json(
        compose_base,
        service,
        """
import json
import os
import sqlite3

db_path = os.environ.get("COLLECTOR_DB_PATH", "/app/data/collector_jobs.sqlite3")
con = sqlite3.connect(db_path)
row = con.execute("SELECT COALESCE(MAX(id), 0) FROM jobs").fetchone()
print(json.dumps({"max_job_id": int(row[0])}))
""",
    )
    return int(payload["max_job_id"])


def _query_env_context(compose_base: list[str], service: str) -> dict[str, Any]:
    return _exec_python_json(
        compose_base,
        service,
        """
import json
import os
import socket
import sys

payload = {
    "container_hostname": socket.gethostname(),
    "python_version": sys.version,
    "collector_db_path": os.environ.get("COLLECTOR_DB_PATH", "/app/data/collector_jobs.sqlite3"),
    "netbox_url": os.environ.get("NETBOX_URL", ""),
    "source_netbox_url": os.environ.get("SOURCE_NETBOX_URL", ""),
    "log_level_env": os.environ.get("LOG_LEVEL", ""),
    "dry_run_env": os.environ.get("DRY_RUN", ""),
}
print(json.dumps(payload))
""",
    )


def _query_job_after(
    compose_base: list[str],
    service: str,
    baseline_job_id: int,
    container_mapping_path: str,
) -> dict[str, Any] | None:
    payload = _exec_python_json(
        compose_base,
        service,
        """
import json
import os
import sqlite3
import sys

baseline_job_id = int(sys.argv[1])
mapping_path = sys.argv[2]
db_path = os.environ.get("COLLECTOR_DB_PATH", "/app/data/collector_jobs.sqlite3")
con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row
row = con.execute(
    "SELECT id, hcl_file, status, dry_run, debug_mode, created_at, started_at, finished_at, summary "
    "FROM jobs WHERE id > ? AND hcl_file = ? ORDER BY id DESC LIMIT 1",
    (baseline_job_id, mapping_path),
).fetchone()
if row is None:
    print("null")
else:
    job = dict(row)
    if job.get("summary"):
        try:
            job["summary"] = json.loads(job["summary"])
        except (TypeError, json.JSONDecodeError):
            pass
    print(json.dumps(job))
""",
        str(baseline_job_id),
        container_mapping_path,
    )
    if payload is None:
        return None
    return dict(payload)


def _query_db_slice(compose_base: list[str], service: str, job_id: int) -> dict[str, Any]:
    return _exec_python_json(
        compose_base,
        service,
        """
import json
import os
import sqlite3
import sys

job_id = int(sys.argv[1])
db_path = os.environ.get("COLLECTOR_DB_PATH", "/app/data/collector_jobs.sqlite3")
con = sqlite3.connect(db_path)
con.row_factory = sqlite3.Row
job_row = con.execute(
    "SELECT id, hcl_file, status, dry_run, debug_mode, created_at, started_at, finished_at, summary "
    "FROM jobs WHERE id = ?",
    (job_id,),
).fetchone()
log_rows = con.execute(
    "SELECT id, job_id, timestamp, level, logger, message FROM job_logs WHERE job_id = ? ORDER BY id ASC",
    (job_id,),
).fetchall()
job = None if job_row is None else dict(job_row)
if job is not None and job.get("summary"):
    try:
        job["summary"] = json.loads(job["summary"])
    except (TypeError, json.JSONDecodeError):
        pass
payload = {
    "job": job,
    "logs": [dict(row) for row in log_rows],
}
print(json.dumps(payload))
""",
        str(job_id),
    )


def _exec_python_json(
    compose_base: list[str],
    service: str,
    script: str,
    *script_args: str,
) -> Any:
    command = [*compose_base, "exec", "-T", service, "python", "-c", script, *script_args]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    stdout = result.stdout.strip()
    if not stdout:
        return None
    return json.loads(stdout)


def _artifact_dir(artifact_root: Path, mapping_path: Path, start_utc: datetime) -> Path:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", mapping_path.stem).strip("-") or "mapping"
    timestamp = start_utc.strftime("%Y%m%dT%H%M%SZ")
    return artifact_root / f"{timestamp}_{slug}"


def _run_and_tee(
    command: list[str],
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    timeout_seconds: int,
) -> int:
    with stdout_path.open("w", encoding="utf-8") as stdout_file, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_file:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        stdout_thread = threading.Thread(
            target=_tee_stream,
            args=(process.stdout, stdout_file, sys.stdout),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_tee_stream,
            args=(process.stderr, stderr_file, sys.stderr),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        try:
            return_code = process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            process.kill()
            return_code = process.wait()
            raise SystemExit(
                f"Collector command exceeded timeout of {timeout_seconds} seconds and was terminated."
            )
        finally:
            stdout_thread.join()
            stderr_thread.join()
    return return_code


def _tee_stream(stream: TextIO | None, output_file: TextIO, mirror: TextIO) -> None:
    if stream is None:
        return
    for line in stream:
        output_file.write(line)
        output_file.flush()
        mirror.write(line)
        mirror.flush()
    stream.close()


def _git_sha(project_directory: Path) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_directory,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _build_env_context(container_env: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "collector_service": manifest["collector_service"],
        "project_directory": manifest["project_directory"],
        "compose_file": manifest["compose_file"],
        "mapping_host_path": manifest["mapping_host_path"],
        "mapping_container_path": manifest["mapping_container_path"],
        "repo_git_sha": manifest["repo_git_sha"],
        "dry_run": manifest["dry_run"],
        "log_level": manifest["log_level"],
        "container": {
            "hostname": container_env.get("container_hostname"),
            "python_version": container_env.get("python_version"),
            "collector_db_path": container_env.get("collector_db_path", CONTAINER_DB_PATH),
            "log_level_env": container_env.get("log_level_env"),
            "dry_run_env": container_env.get("dry_run_env"),
            "netbox_url_host": _host_only(container_env.get("netbox_url", "")),
            "source_netbox_url_host": _host_only(container_env.get("source_netbox_url", "")),
        },
    }


def _host_only(raw_value: str) -> str | None:
    value = raw_value.strip()
    if not value:
        return None
    if "://" not in value:
        value = f"//{value}"
    parsed = urlparse(value)
    return parsed.hostname or raw_value


def _write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _write_db_log_text(path: Path, db_slice: dict[str, Any] | None) -> None:
    lines: list[str] = []
    if db_slice is not None:
        for entry in db_slice.get("logs", []):
            lines.append(
                f"{entry.get('timestamp')} [{entry.get('level')}] "
                f"{entry.get('logger') or 'root'} {entry.get('message')}"
            )
    with path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        if lines:
            handle.write("\n")


def _write_done_marker(path: Path, manifest: dict[str, Any]) -> None:
    payload = {
        "job_id": manifest.get("job_id"),
        "job_status": manifest.get("job_status"),
        "finished_at_utc": manifest.get("finished_at_utc"),
        "exit_code": manifest.get("exit_code"),
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    raise SystemExit(main())
