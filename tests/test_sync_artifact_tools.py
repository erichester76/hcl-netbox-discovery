"""
File: tests/test_sync_artifact_tools.py
Purpose: Regression coverage for deterministic sync artifact helper functions.
Created: 2026-04-04
Author: Codex
Last Changed: Codex Issue: #capture-feedback-loop
"""

from __future__ import annotations

import importlib.util
import io
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pytest


def _load_module(module_name: str, relative_path: str):
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / relative_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


capture_sync_job = _load_module("capture_sync_job_test", "src/capture_sync_job.py")
pull_sync_artifacts = _load_module("pull_sync_artifacts_test", "src/pull_sync_artifacts.py")


class TestArtifactDir:
    def test_artifact_dir_uses_timestamp_and_slugged_mapping_name(self, tmp_path):
        started_at = datetime(2026, 4, 4, 15, 30, 0, 123456, tzinfo=timezone.utc)
        mapping_path = tmp_path / "vmware prod!.hcl"

        artifact_dir = capture_sync_job._artifact_dir(tmp_path, mapping_path, started_at)

        assert artifact_dir == tmp_path / "20260404T153000123456Z_vmware-prod"

    def test_artifact_dir_falls_back_to_mapping_when_slug_is_empty(self, tmp_path):
        started_at = datetime(2026, 4, 4, 15, 30, 0, 654321, tzinfo=timezone.utc)
        mapping_path = tmp_path / "!!!.hcl"

        artifact_dir = capture_sync_job._artifact_dir(tmp_path, mapping_path, started_at)

        assert artifact_dir == tmp_path / "20260404T153000654321Z_mapping"


class TestContainerMappingPath:
    def test_container_mapping_path_resolves_project_mapping_to_app_path(self, tmp_path):
        project_directory = tmp_path / "project"
        mapping_path = project_directory / "mappings" / "vmware.hcl"
        mapping_path.parent.mkdir(parents=True)
        mapping_path.write_text("", encoding="utf-8")

        container_path = capture_sync_job._container_mapping_path(project_directory, mapping_path)

        assert container_path == PurePosixPath("/app/mappings/vmware.hcl")

    def test_container_mapping_path_rejects_mapping_outside_project_directory(self, tmp_path):
        project_directory = tmp_path / "project"
        mapping_path = tmp_path / "outside" / "vmware.hcl"
        mapping_path.parent.mkdir(parents=True)
        mapping_path.write_text("", encoding="utf-8")

        with pytest.raises(SystemExit, match="must live under the Docker Compose project directory"):
            capture_sync_job._container_mapping_path(project_directory, mapping_path)

    def test_container_mapping_path_rejects_non_mappings_directory(self, tmp_path):
        project_directory = tmp_path / "project"
        mapping_path = project_directory / "other" / "vmware.hcl"
        mapping_path.parent.mkdir(parents=True)
        mapping_path.write_text("", encoding="utf-8")

        with pytest.raises(SystemExit, match="must live under the project's mappings/ directory"):
            capture_sync_job._container_mapping_path(project_directory, mapping_path)


class TestHostOnly:
    @pytest.mark.parametrize(
        ("raw_value", "expected"),
        [
            ("", None),
            ("netbox.example.com", "netbox.example.com"),
            ("netbox.example.com:8443", "netbox.example.com"),
            ("https://netbox.example.com/api", "netbox.example.com"),
            ("http://10.10.10.5:8080", "10.10.10.5"),
        ],
    )
    def test_host_only_normalizes_host_values(self, raw_value, expected):
        assert capture_sync_job._host_only(raw_value) == expected


class TestBuildEnvContext:
    def test_build_env_context_reduces_urls_to_hostnames(self):
        env_context = {
            "container_hostname": "collector-1",
            "python_version": "3.8.10",
            "collector_db_path": "/app/data/collector_jobs.sqlite3",
            "log_level_env": "INFO",
            "dry_run_env": "false",
            "netbox_url": "https://netbox.example.com/api",
            "source_netbox_url": "source-netbox.example.com:8443",
        }
        manifest = {
            "collector_service": "collector",
            "project_directory": "/srv/hcl-netbox-discovery",
            "compose_file": "/srv/hcl-netbox-discovery/docker-compose.yml",
            "mapping_host_path": "/srv/hcl-netbox-discovery/mappings/vmware.hcl",
            "mapping_container_path": "/app/mappings/vmware.hcl",
            "repo_git_sha": "abc123",
            "dry_run": False,
            "log_level": "INFO",
        }

        payload = capture_sync_job._build_env_context(env_context, manifest)

        assert payload["container"]["netbox_url_host"] == "netbox.example.com"
        assert payload["container"]["source_netbox_url_host"] == "source-netbox.example.com"


class TestPullScriptDefaults:
    def test_pull_script_default_local_root_points_to_inbox(self):
        expected_root = (
            Path(pull_sync_artifacts.REPO_ROOT) / "run-artifacts" / "inbox"
        ).resolve()

        args = pull_sync_artifacts._parse_args(["prod-host", "/var/tmp/hcl-sync-artifacts"])

        assert Path(args.local_root).resolve() == expected_root

    def test_remote_python_command_quotes_script_and_args(self):
        command = pull_sync_artifacts._remote_python_command(
            script="import sys\nprint(sys.argv[1:])\n",
            argv=["/var/tmp/hcl-sync-artifacts", "DONE"],
        )

        assert command.startswith("python3 -c ")
        assert "/var/tmp/hcl-sync-artifacts" in command
        assert "DONE" in command


class TestPullScriptStreamDrain:
    def test_drain_stream_collects_stderr_without_leaking_handle(self):
        stream = io.BytesIO(b"warning line 1\nwarning line 2\n")
        chunks = []

        pull_sync_artifacts._drain_stream(stream, chunks)

        assert b"".join(chunks) == b"warning line 1\nwarning line 2\n"
        assert stream.closed is True
