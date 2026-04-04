#!/usr/bin/env python3
"""
File: src/pull_sync_artifacts.py
Purpose: Pull completed sync artifact bundles from a remote Docker host into local analysis storage.
Created: 2026-04-04
Author: Codex
Last Changed: Codex Issue: #capture-feedback-loop
"""

from __future__ import annotations

import argparse
import json
import shlex
import shutil
import subprocess
import tarfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCAL_ROOT = REPO_ROOT / "run-artifacts" / "inbox"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pull completed sync artifact bundles from a remote host over SSH "
            "into local storage for analysis."
        ),
    )
    parser.add_argument("remote_host", help="SSH target for the remote Docker host.")
    parser.add_argument(
        "remote_root",
        help="Remote directory containing timestamped artifact bundles from capture_sync_job.py.",
    )
    parser.add_argument(
        "--local-root",
        default=str(DEFAULT_LOCAL_ROOT),
        help="Local destination for imported artifact bundles.",
    )
    parser.add_argument(
        "--done-marker-name",
        default="DONE",
        help="Completion-marker filename used by the remote capture job.",
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=22,
        help="SSH port for the remote host.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    local_root = Path(args.local_root).expanduser().resolve()
    local_root.mkdir(parents=True, exist_ok=True)

    bundle_names = _list_remote_bundles(
        remote_host=args.remote_host,
        remote_root=args.remote_root,
        done_marker_name=args.done_marker_name,
        ssh_port=args.ssh_port,
    )

    imported: list[str] = []
    skipped: list[str] = []
    for bundle_name in bundle_names:
        destination = local_root / bundle_name
        if destination.exists():
            skipped.append(bundle_name)
            continue
        _pull_bundle(
            remote_host=args.remote_host,
            remote_root=args.remote_root,
            bundle_name=bundle_name,
            local_root=local_root,
            ssh_port=args.ssh_port,
        )
        imported.append(bundle_name)

    summary = {
        "remote_host": args.remote_host,
        "remote_root": args.remote_root,
        "local_root": str(local_root),
        "imported": imported,
        "skipped_existing": skipped,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _list_remote_bundles(
    remote_host: str,
    remote_root: str,
    done_marker_name: str,
    ssh_port: int,
) -> list[str]:
    script = (
        "import json, pathlib, sys\n"
        "root = pathlib.Path(sys.argv[1])\n"
        "done_name = sys.argv[2]\n"
        "bundles = []\n"
        "if root.exists():\n"
        "    for marker in sorted(root.rglob(done_name)):\n"
        "        if marker.is_file() and marker.parent != root:\n"
        "            bundles.append(str(marker.parent.relative_to(root)))\n"
        "print(json.dumps(bundles))\n"
    )
    result = subprocess.run(
        [
            "ssh",
            "-p",
            str(ssh_port),
            remote_host,
            "python3",
            "-c",
            script,
            remote_root,
            done_marker_name,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = result.stdout.strip()
    if not payload:
        return []
    return list(json.loads(payload))


def _pull_bundle(
    remote_host: str,
    remote_root: str,
    bundle_name: str,
    local_root: Path,
    ssh_port: int,
) -> None:
    parent = local_root / Path(bundle_name).parent
    parent.mkdir(parents=True, exist_ok=True)

    ssh_command = (
        f"cd {shlex.quote(remote_root)} && "
        f"tar -cf - {shlex.quote(bundle_name)}"
    )
    ssh_proc = subprocess.Popen(
        ["ssh", "-p", str(ssh_port), remote_host, ssh_command],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert ssh_proc.stdout is not None
    try:
        with tarfile.open(fileobj=ssh_proc.stdout, mode="r|") as archive:
            archive.extractall(path=local_root, filter="data")
    finally:
        ssh_proc.stdout.close()

    stderr = b""
    if ssh_proc.stderr is not None:
        stderr = ssh_proc.stderr.read()
        ssh_proc.stderr.close()
    return_code = ssh_proc.wait()
    if return_code != 0:
        shutil.rmtree(local_root / bundle_name, ignore_errors=True)
        raise SystemExit(
            f"Failed to pull remote bundle '{bundle_name}' from {remote_host}: "
            f"{stderr.decode('utf-8', errors='replace').strip()}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
