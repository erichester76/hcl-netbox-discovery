"""Tests for the CLI entry point (main.py)."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import collector.db as db_module
from collector.db import get_jobs, init_db
from collector.job_lifecycle import _mask_sensitive_values, capture_job_runtime_metadata
from main import _parse_args, _setup_logging


class TestParseArgsLogLevel:
    """--log-level defaults and env variable handling."""

    def test_default_is_info_when_no_env(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        args = _parse_args([])
        assert args.log_level == "INFO"

    def test_env_variable_sets_default(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        args = _parse_args([])
        assert args.log_level == "DEBUG"

    def test_env_variable_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "warning")
        args = _parse_args([])
        assert args.log_level == "WARNING"

    def test_cli_flag_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        args = _parse_args(["--log-level", "ERROR"])
        assert args.log_level == "ERROR"

    def test_all_valid_choices(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        for level in ("DEBUG", "INFO", "WARNING", "ERROR"):
            args = _parse_args(["--log-level", level])
            assert args.log_level == level


class TestSetupLogging:
    """_setup_logging applies the requested level to the root logger."""

    def reset_root_logger(self):
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
        root.setLevel(logging.WARNING)

    @pytest.mark.parametrize(
        "level_str, expected_level",
        [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
        ],
    )
    def test_level_applied(self, level_str, expected_level):
        self.reset_root_logger()
        _setup_logging(level_str)
        assert logging.getLogger().level == expected_level

    def test_defaults_to_info_when_no_env_and_no_arg(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        self.reset_root_logger()
        _setup_logging()
        assert logging.getLogger().level == logging.INFO

    def test_reads_env_variable_when_no_arg(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        self.reset_root_logger()
        _setup_logging()
        assert logging.getLogger().level == logging.DEBUG

    def test_explicit_arg_overrides_env(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        self.reset_root_logger()
        _setup_logging("WARNING")
        assert logging.getLogger().level == logging.WARNING


# ---------------------------------------------------------------------------
# CLI job DB integration
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_db(tmp_path, monkeypatch):
    """Point the DB at a temp file and reset the lock for isolation."""
    db_path = str(tmp_path / "test_main.sqlite3")
    monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
    monkeypatch.setattr(db_module, "_lock", threading.Lock())
    init_db()
    yield db_path


def _fake_stat():
    """Return a minimal stats-like object that main() iterates over."""
    s = MagicMock()
    s.object_name = "devices"
    s.processed = 5
    s.created = 1
    s.updated = 3
    s.skipped = 1
    s.errored = 0
    s.nested_skipped = {}
    return s


def _minimal_mapping() -> str:
    return """
source "rest" {
  api_type = "rest"
  url = "https://source.example.com"
  username = "operator"
  password = "super-secret"
}

netbox {
  url = "https://netbox.example.com"
  token = "netbox-secret"
}

collector {}

object "device" {
  source_collection = "devices"
  netbox_resource = "dcim.devices"
}
""".strip()


def test_summary_from_stats_builds_expected_payload():
    from collector.job_lifecycle import summary_from_stats  # noqa: PLC0415

    summary, has_errors = summary_from_stats([_fake_stat()])

    assert has_errors is False
    assert summary == {
        "devices": {
            "processed": 5,
            "created": 1,
            "updated": 3,
            "skipped": 1,
            "errored": 0,
            "nested_skipped": {},
        }
    }


def _wait_for_job_completion(job_id: int, db_module_ref, timeout: float = 5.0) -> None:
    import time as _time  # noqa: PLC0415

    deadline = _time.monotonic() + timeout
    last_status = None
    while _time.monotonic() < deadline:
        job = db_module_ref.get_job(job_id)
        last_status = None if job is None else job["status"]
        if last_status not in {"queued", "running"}:
            return
        _time.sleep(0.05)
    pytest.fail(
        f"Timed out waiting for queued job {job_id} to finish; "
        f"last status was {last_status!r}"
    )


def test_main_creates_db_job_on_success(tmp_path, tmp_db, monkeypatch):
    """main() must create a DB job row and mark it success for a good mapping."""
    hcl = tmp_path / "test.hcl"
    hcl.write_text(_minimal_mapping())

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]

    with patch("collector.engine.Engine", return_value=fake_engine):
        from main import main  # noqa: PLC0415
        rc = main(["--mapping", str(hcl)])

    assert rc == 0
    jobs = get_jobs()
    assert len(jobs) == 1
    job = jobs[0]
    assert job["status"] == "success"
    assert job["hcl_file"] == str(hcl)
    assert job["summary"] is not None
    assert job["summary"]["devices"]["created"] == 1
    assert job["artifact"] is not None
    assert job["artifact"]["status"] == "success"
    assert job["artifact"]["summary"]["devices"]["created"] == 1
    assert job["runtime_snapshot"] is not None
    assert job["runtime_snapshot"]["config"]["source"]["password"] == "********"
    assert job["runtime_snapshot"]["config"]["netbox"]["token"] == "********"
    assert job["runtime_snapshot"]["config"]["source"]["url"] == "https://source.example.com"
    assert job["code_version"] is not None
    assert job["artifact"]["runtime_snapshot"] == job["runtime_snapshot"]
    assert job["artifact"]["code_version"] == job["code_version"]


def test_main_runtime_snapshot_sanitizes_urls(tmp_path, tmp_db):
    hcl = tmp_path / "test.hcl"
    hcl.write_text(
        """
source "rest" {
  api_type = "rest"
  url = "https://api-user:api-pass@source.example.com/path?api_token=top-secret&mode=full"
  username = "operator"
  password = "super-secret"
}

netbox {
  url = "https://netbox.example.com"
  token = "netbox-secret"
}

collector {}

object "device" {
  source_collection = "devices"
  netbox_resource = "dcim.devices"
}
""".strip()
    )

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]

    with patch("collector.engine.Engine", return_value=fake_engine):
        from main import main  # noqa: PLC0415
        rc = main(["--mapping", str(hcl)])

    assert rc == 0
    job = get_jobs()[0]
    assert job["runtime_snapshot"]["config"]["source"]["url"] == (
        "https://api-user:********@source.example.com/path?api_token=********&mode=full"
    )
    assert job["runtime_snapshot"]["execution_plan"]["source_groups"][0]["source_urls"] == [
        "https://api-user:********@source.example.com/path?api_token=********&mode=full"
    ]


def test_mask_sensitive_values_masks_sensitive_lists():
    payload = {"tokens": ["one", "two"], "secrets": ["a"], "nested": {"api_key": ["x"]}}
    masked = _mask_sensitive_values(payload)
    assert masked["tokens"] == ["********", "********"]
    assert masked["secrets"] == ["********"]
    assert masked["nested"]["api_key"] == ["********"]


def test_mask_sensitive_values_preserves_ipv6_url_brackets():
    masked = _mask_sensitive_values(
        {"url": "https://api-user:api-pass@[2001:db8::1]:8443/path?api_token=top-secret"}
    )
    assert masked["url"] == "https://api-user:********@[2001:db8::1]:8443/path?api_token=********"


def test_get_code_version_prefers_baked_env_metadata(monkeypatch):
    import collector.job_lifecycle as job_lifecycle  # noqa: PLC0415

    monkeypatch.setenv("APP_VERSION", "1.1.1")
    monkeypatch.setenv("APP_GIT_COMMIT", "abcdef1234567890")
    monkeypatch.setenv("APP_GIT_BRANCH", "dev")
    monkeypatch.setenv("APP_GIT_TAG", "v1.1.1")
    monkeypatch.setattr(job_lifecycle, "_read_project_version", lambda: "0.0.0")
    monkeypatch.setattr(job_lifecycle, "_git_output", lambda *_args: "fallback")
    monkeypatch.setattr(
        job_lifecycle,
        "_build_component_versions",
        lambda base_version: {"collector": {"version": base_version, "sha256": "collector-hash"}},
    )

    job_lifecycle.get_code_version.cache_clear()
    try:
        assert job_lifecycle.get_code_version() == {
            "version": "1.1.1",
            "git_commit": "abcdef1234567890",
            "git_branch": "dev",
            "git_tag": "v1.1.1",
            "components": {"collector": {"version": "1.1.1", "sha256": "collector-hash"}},
        }
    finally:
        job_lifecycle.get_code_version.cache_clear()


def test_capture_job_runtime_metadata_redacts_config_error_details(tmp_path):
    hcl = tmp_path / "broken.hcl"
    hcl.write_text("broken")

    with patch(
        "collector.job_lifecycle.load_config",
        side_effect=RuntimeError("token=super-secret parse exploded"),
    ):
        runtime_snapshot, _code_version = capture_job_runtime_metadata(
            hcl_file=str(hcl),
            dry_run=False,
            debug_mode=False,
            run_token=None,
        )

    assert runtime_snapshot["config_error"] == "Configuration loading failed (RuntimeError)"
    assert "super-secret" not in runtime_snapshot["config_error"]


def test_capture_job_runtime_metadata_includes_mapping_and_source_fingerprints(tmp_path, monkeypatch):
    hcl = tmp_path / "sample.hcl"
    hcl.write_text(_minimal_mapping())
    example = tmp_path / "sample.hcl.example"
    example.write_text("# example")

    import collector.job_lifecycle as job_lifecycle  # noqa: PLC0415

    monkeypatch.setattr(
        job_lifecycle,
        "get_code_version",
        lambda: {"version": "1.0.48", "components": {"engine": {"version": "1.0.48", "sha256": "engine-hash"}}},
    )
    monkeypatch.setattr(job_lifecycle, "_sha256_file", lambda path: f"hash:{Path(path).name}" if path else None)
    monkeypatch.setattr(job_lifecycle, "_tree_sha256", lambda *_args, **_kwargs: "tree-hash")

    runtime_snapshot, code_version = capture_job_runtime_metadata(
        hcl_file=str(hcl),
        dry_run=False,
        debug_mode=False,
        run_token="run-1",
    )

    assert code_version == {
        "version": "1.0.48",
        "components": {"engine": {"version": "1.0.48", "sha256": "engine-hash"}},
    }
    assert runtime_snapshot["mapping"]["version"] == "1.0.48"
    assert runtime_snapshot["mapping"]["sha256"] == "hash:sample.hcl"
    assert runtime_snapshot["mapping"]["example_version"] == "1.0.48"
    assert runtime_snapshot["mapping"]["example_path"] == str(example)
    assert runtime_snapshot["mapping"]["example_sha256"] == "hash:sample.hcl.example"
    assert runtime_snapshot["component_versions"]["active_source"] == {
        "version": "1.0.48",
        "api_type": "rest",
        "path": "src/collector/sources/rest.py",
        "sha256": "hash:rest.py",
    }


def test_capture_job_runtime_metadata_rejects_unsafe_active_source_path(tmp_path, monkeypatch):
    hcl = tmp_path / "sample.hcl"
    hcl.write_text(_minimal_mapping().replace('api_type = "rest"', 'api_type = "../../escape"'))

    import collector.job_lifecycle as job_lifecycle  # noqa: PLC0415

    monkeypatch.setattr(
        job_lifecycle,
        "get_code_version",
        lambda: {"version": "1.0.48", "components": {}},
    )
    monkeypatch.setattr(job_lifecycle, "_sha256_file", lambda path: f"hash:{Path(path).name}" if path else None)

    runtime_snapshot, _code_version = capture_job_runtime_metadata(
        hcl_file=str(hcl),
        dry_run=False,
        debug_mode=False,
        run_token="run-unsafe",
    )

    assert runtime_snapshot["component_versions"]["active_source"] == {
        "version": "1.0.48",
        "api_type": "../../escape",
        "path": None,
        "sha256": None,
    }


def test_tree_sha256_returns_none_when_a_matched_file_cannot_be_hashed(tmp_path, monkeypatch, caplog):
    import collector.job_lifecycle as job_lifecycle  # noqa: PLC0415

    monkeypatch.setattr(job_lifecycle, "_PROJECT_ROOT", tmp_path)

    tree_root = tmp_path / "collector"
    tree_root.mkdir()
    (tree_root / "ok.py").write_text("print('ok')\n")

    original_sha256_file = job_lifecycle._sha256_file

    def fake_sha256_file(path):
        if path and Path(path).name == "ok.py":
            return None
        return original_sha256_file(path)

    monkeypatch.setattr(job_lifecycle, "_sha256_file", fake_sha256_file)

    with caplog.at_level(logging.WARNING):
        digest = job_lifecycle._tree_sha256(tree_root, "*.py")

    assert digest is None
    assert "Unable to hash file while computing tree sha256" in caplog.text


def test_execute_job_persists_stopped_status_when_engine_stops(tmp_path, tmp_db):
    hcl = tmp_path / "stop.hcl"
    hcl.write_text("")

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]
    fake_engine.stop_requested = True

    from main import _execute_job  # noqa: PLC0415

    job_id = db_module.create_job(str(hcl), dry_run=False)

    with patch("collector.engine.Engine", return_value=fake_engine):
        rc = _execute_job(job_id, str(hcl), dry_run=False)

    assert rc is True
    job = db_module.get_job(job_id)
    assert job is not None
    assert job["status"] == "stopped"
    assert job["summary"] is not None


def test_main_uses_collector_run_token_env(tmp_path, tmp_db, monkeypatch):
    hcl = tmp_path / "test.hcl"
    hcl.write_text("")
    monkeypatch.setenv("COLLECTOR_RUN_TOKEN", "capture-123")

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]

    with patch("collector.engine.Engine", return_value=fake_engine):
        from main import main  # noqa: PLC0415
        rc = main(["--mapping", str(hcl)])

    assert rc == 0
    jobs = get_jobs()
    assert jobs[0]["run_token"] == "capture-123"


def test_summary_from_stats_includes_nested_skips():
    from collector.job_lifecycle import summary_from_stats  # noqa: PLC0415

    stat = _fake_stat()
    stat.nested_skipped = {
        "virtualization.interfaces:virtual_machine": 12,
        "virtualization.virtual_disks:virtual_machine": 7,
    }

    summary, has_errors = summary_from_stats([stat])

    assert has_errors is False
    assert summary["devices"]["nested_skipped"] == {
        "virtualization.interfaces:virtual_machine": 12,
        "virtualization.virtual_disks:virtual_machine": 7,
    }


def test_summary_from_stats_aggregates_duplicate_object_names():
    from collector.job_lifecycle import summary_from_stats  # noqa: PLC0415

    first = _fake_stat()
    first.created = 0
    first.updated = 2
    first.skipped = 1
    first.processed = 3
    first.nested_skipped = {"virtualization.interfaces:virtual_machine": 2}
    first.source_url = "https://cu-xclarity.clemson.edu"

    second = _fake_stat()
    second.created = 0
    second.updated = 1
    second.skipped = 0
    second.errored = 1
    second.processed = 2
    second.nested_skipped = {"virtualization.interfaces:virtual_machine": 1}
    second.source_url = "https://proto-xclarity.clemson.edu"

    summary, has_errors = summary_from_stats([first, second])

    assert has_errors is True
    assert summary["devices"] == {
        "processed": 5,
        "created": 0,
        "updated": 3,
        "skipped": 1,
        "errored": 1,
        "nested_skipped": {
            "virtualization.interfaces:virtual_machine": 3,
        },
    }


def test_main_artifact_includes_per_iteration_breakdown(tmp_path, tmp_db):
    hcl = tmp_path / "test.hcl"
    hcl.write_text("")

    first = _fake_stat()
    first.object_name = "node"
    first.created = 0
    first.updated = 33
    first.skipped = 27
    first.errored = 5
    first.processed = 65
    first.source_url = "https://cu-xclarity.clemson.edu"

    second = _fake_stat()
    second.object_name = "node"
    second.created = 0
    second.updated = 0
    second.skipped = 0
    second.errored = 0
    second.processed = 0
    second.source_url = "https://poole-xclarity.clemson.edu"

    fake_engine = MagicMock()
    fake_engine.run.return_value = [first, second]

    with patch("collector.engine.Engine", return_value=fake_engine):
        from main import main  # noqa: PLC0415

        rc = main(["--mapping", str(hcl)])

    assert rc == 0
    job = get_jobs()[0]
    assert job["summary"]["node"] == {
        "processed": 65,
        "created": 0,
        "updated": 33,
        "skipped": 27,
        "errored": 5,
        "nested_skipped": {},
    }
    assert job["artifact"]["summary"]["node"]["processed"] == 65
    assert job["artifact"]["iterations"] == {
        "https://cu-xclarity.clemson.edu": {
            "summary": {
                "node": {
                    "processed": 65,
                    "created": 0,
                    "updated": 33,
                    "skipped": 27,
                    "errored": 5,
                    "nested_skipped": {},
                }
            }
        },
        "https://poole-xclarity.clemson.edu": {
            "summary": {
                "node": {
                    "processed": 0,
                    "created": 0,
                    "updated": 0,
                    "skipped": 0,
                    "errored": 0,
                    "nested_skipped": {},
                }
            }
        },
    }


def test_main_creates_db_job_on_engine_failure(tmp_path, tmp_db, monkeypatch):
    """main() must mark the DB job as failed when the engine raises."""
    hcl = tmp_path / "test.hcl"
    hcl.write_text("")

    fake_engine = MagicMock()
    fake_engine.run.side_effect = RuntimeError("boom")

    with patch("collector.engine.Engine", return_value=fake_engine):
        from main import main  # noqa: PLC0415
        rc = main(["--mapping", str(hcl)])

    assert rc == 1
    jobs = get_jobs()
    assert len(jobs) == 1
    assert jobs[0]["status"] == "failed"
    assert jobs[0]["artifact"] is not None
    assert jobs[0]["artifact"]["status"] == "failed"
    assert jobs[0]["artifact"]["error"] == "boom"


def test_main_missing_mapping_persists_failed_job(tmp_db):
    """main() must persist a failed DB job for a mapping file that does not exist."""
    from main import main  # noqa: PLC0415
    rc = main(["--mapping", "/nonexistent/path.hcl"])
    assert rc == 1
    jobs = get_jobs()
    assert len(jobs) == 1
    assert jobs[0]["status"] == "failed"
    assert jobs[0]["hcl_file"] == "/nonexistent/path.hcl"
    assert jobs[0]["artifact"] is not None
    assert jobs[0]["artifact"]["status"] == "failed"
    assert "Mapping file not found" in jobs[0]["artifact"]["error"]


def test_main_no_args_returns_error(tmp_db):
    """main() without --mapping and without --run-scheduler must return exit code 1."""
    from main import main  # noqa: PLC0415
    rc = main([])
    assert rc == 1


def test_parse_args_run_scheduler_flag():
    """--run-scheduler flag must be parsed correctly."""
    from main import _parse_args  # noqa: PLC0415
    args = _parse_args(["--run-scheduler"])
    assert args.run_scheduler is True


def test_parse_args_run_scheduler_default():
    """--run-scheduler must default to False."""
    from main import _parse_args  # noqa: PLC0415
    args = _parse_args([])
    assert args.run_scheduler is False


def test_check_and_fire_due_schedules_fires_job(tmp_path, tmp_db):
    """_check_and_fire_due_schedules() must create a job for a due schedule."""
    import collector.db as db_module  # noqa: PLC0415

    hcl = tmp_path / "test.hcl"
    hcl.write_text("")

    # Create a schedule with next_run_at in the past
    past = "2000-01-01T00:00:00"
    db_module.create_schedule("test-sched", str(hcl), "0 * * * *", next_run_at=past)
    from main import _check_and_fire_due_schedules  # noqa: PLC0415
    _check_and_fire_due_schedules()

    jobs = get_jobs()
    assert len(jobs) >= 1
    assert jobs[0]["status"] == "queued"


def test_check_and_run_queued_jobs_picks_up_queued_job(tmp_path, tmp_db):
    """_check_and_run_queued_jobs() must execute jobs with status='queued'."""
    import time as _time  # noqa: PLC0415
    from unittest.mock import patch  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415

    hcl = tmp_path / "test.hcl"
    hcl.write_text("")

    # Create a queued job as the web UI would
    job_id = db_module.create_job(str(hcl), dry_run=False)
    assert db_module.get_job(job_id)["status"] == "queued"

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    # Keep the patch alive until the background thread finishes
    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _time.sleep(0.5)
    finally:
        patcher.stop()

    job = db_module.get_job(job_id)
    assert job["status"] == "success"


def test_check_and_run_queued_jobs_missing_file(tmp_path, tmp_db):
    """_check_and_run_queued_jobs() must mark a job failed when the file is missing."""
    import time as _time  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415

    job_id = db_module.create_job("/nonexistent/path.hcl")
    assert db_module.get_job(job_id)["status"] == "queued"

    from main import _check_and_run_queued_jobs  # noqa: PLC0415
    _check_and_run_queued_jobs()

    _time.sleep(0.3)

    job = db_module.get_job(job_id)
    assert job["status"] == "failed"


def test_check_and_run_queued_jobs_marks_partial_when_stats_have_errors(tmp_path, tmp_db):
    """Queued runs with item-level errors must persist status='partial'."""
    import time as _time  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415

    hcl = tmp_path / "partial.hcl"
    hcl.write_text("")

    stat = _fake_stat()
    stat.errored = 2

    fake_engine = MagicMock()
    fake_engine.run.return_value = [stat]

    job_id = db_module.create_job(str(hcl), dry_run=False)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _time.sleep(0.5)
    finally:
        patcher.stop()

    job = db_module.get_job(job_id)
    assert job["status"] == "partial"


def test_due_schedule_execution_marks_partial_when_stats_have_errors(tmp_path, tmp_db):
    """Scheduled runs should enqueue then execute with status='partial' on item errors."""
    import time as _time  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415

    hcl = tmp_path / "scheduled_partial.hcl"
    hcl.write_text("")

    sid = db_module.create_schedule(
        "scheduled-partial",
        str(hcl),
        "*/10 * * * *",
        next_run_at="2000-01-01T00:00:00",
    )
    sched = db_module.get_schedule(sid)
    assert sched is not None

    stat = _fake_stat()
    stat.errored = 1

    fake_engine = MagicMock()
    fake_engine.run.return_value = [stat]

    from main import _check_and_fire_due_schedules, _check_and_run_queued_jobs  # noqa: PLC0415

    with patch("collector.engine.Engine", return_value=fake_engine):
        _check_and_fire_due_schedules()
        _check_and_run_queued_jobs()
        _time.sleep(0.5)

    jobs = get_jobs()
    assert jobs, "Scheduled run did not create a job record"
    assert jobs[0]["status"] == "partial"


def test_due_schedule_missing_file_persists_failed_job(tmp_db):
    """Missing-file scheduled executions should queue then persist a failed job."""
    import time as _time  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415

    db_module.create_schedule(
        "missing-file-sched",
        "/nonexistent/path.hcl",
        "*/5 * * * *",
        next_run_at="2000-01-01T00:00:00",
    )

    from main import _check_and_fire_due_schedules, _check_and_run_queued_jobs  # noqa: PLC0415
    _check_and_fire_due_schedules()
    _check_and_run_queued_jobs()
    _time.sleep(0.3)

    jobs = get_jobs()
    assert jobs, "Expected a failed job record for missing scheduled mapping"
    assert jobs[0]["status"] == "failed"


def test_run_scheduler_reconciles_stale_running_jobs_before_poll_loop(tmp_db):
    import collector.db as db_module  # noqa: PLC0415

    stale_job_id = db_module.create_job("mappings/stale.hcl")
    db_module.start_job(stale_job_id)

    from main import _run_scheduler  # noqa: PLC0415

    with (
        patch("main._check_and_fire_due_schedules"),
        patch("main._check_and_run_queued_jobs"),
        patch("main.time.sleep", side_effect=KeyboardInterrupt),
    ):
        with pytest.raises(KeyboardInterrupt):
            _run_scheduler(poll_interval=1)

    stale_job = db_module.get_job(stale_job_id)
    assert stale_job is not None
    assert stale_job["status"] == "failed"


def test_run_queued_job_debug_mode_captures_debug_logs(tmp_path, tmp_db):
    """When debug_mode=True, _run_queued_job must persist DEBUG-level records.

    Root cause: the root logger's effective level was left at INFO even when
    debug_mode=True, so DEBUG records were silently dropped before reaching
    the JobLogHandler.  The fix temporarily lowers the root logger level to
    DEBUG and restores it afterwards.
    """
    import time as _time  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415
    from collector.db import get_job_logs  # noqa: PLC0415

    hcl = tmp_path / "debug.hcl"
    hcl.write_text("")

    collector_logger = logging.getLogger("collector.engine")

    def fake_run(*args, **kwargs):
        collector_logger.debug("debug-marker-12345")
        s = MagicMock()
        s.object_name = "devices"
        s.processed = 1
        s.created = 0
        s.updated = 0
        s.skipped = 1
        s.errored = 0
        return [s]

    fake_engine = MagicMock()
    fake_engine.run.side_effect = fake_run

    job_id = db_module.create_job(str(hcl), debug_mode=True)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _time.sleep(0.5)
    finally:
        patcher.stop()

    logs = get_job_logs(job_id)
    debug_logs = [lg for lg in logs if lg["level"] == "DEBUG"]
    assert any("debug-marker-12345" in lg["message"] for lg in debug_logs), (
        "DEBUG log record was not captured in job logs when debug_mode=True"
    )


def test_run_queued_job_debug_mode_captures_non_collector_debug_logs(tmp_path, tmp_db):
    """debug_mode=True must persist DEBUG logs from non-collector loggers too."""
    import collector.db as db_module  # noqa: PLC0415
    from collector.db import get_job_logs  # noqa: PLC0415

    hcl = tmp_path / "thirdparty-debug.hcl"
    hcl.write_text("")

    external_logger = logging.getLogger("tests.thirdparty.debug")
    old_level = external_logger.level
    old_propagate = external_logger.propagate
    external_logger.setLevel(logging.DEBUG)
    external_logger.propagate = True

    def fake_run(*args, **kwargs):
        external_logger.debug("third-party-debug-marker-67890")
        return [_fake_stat()]

    fake_engine = MagicMock()
    fake_engine.run.side_effect = fake_run

    job_id = db_module.create_job(str(hcl), debug_mode=True)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _wait_for_job_completion(job_id, db_module)
    finally:
        patcher.stop()
        external_logger.setLevel(old_level)
        external_logger.propagate = old_propagate

    logs = get_job_logs(job_id)
    assert any(
        log["level"] == "DEBUG"
        and log["logger"] == "tests.thirdparty.debug"
        and "third-party-debug-marker-67890" in log["message"]
        for log in logs
    ), "Non-collector DEBUG log record was not captured in job logs"


def test_run_queued_job_non_debug_mode_drops_debug_logs(tmp_path, tmp_db):
    """When debug_mode=False, DEBUG records must NOT appear in job logs."""
    import collector.db as db_module  # noqa: PLC0415
    from collector.db import get_job_logs  # noqa: PLC0415

    hcl = tmp_path / "nodebug.hcl"
    hcl.write_text("")

    collector_logger = logging.getLogger("collector.engine")

    def fake_run(*args, **kwargs):
        collector_logger.debug("should-not-appear")
        s = MagicMock()
        s.object_name = "devices"
        s.processed = 1
        s.created = 0
        s.updated = 0
        s.skipped = 1
        s.errored = 0
        return [s]

    fake_engine = MagicMock()
    fake_engine.run.side_effect = fake_run

    job_id = db_module.create_job(str(hcl), debug_mode=False)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _wait_for_job_completion(job_id, db_module)
    finally:
        patcher.stop()

    logs = get_job_logs(job_id)
    debug_logs = [lg for lg in logs if lg["level"] == "DEBUG"]
    assert not debug_logs, "DEBUG log records must not appear when debug_mode=False"


def test_run_queued_job_non_debug_mode_drops_non_collector_debug_logs(tmp_path, tmp_db):
    """debug_mode=False must not persist DEBUG logs from non-collector loggers."""
    import collector.db as db_module  # noqa: PLC0415
    from collector.db import get_job_logs  # noqa: PLC0415

    hcl = tmp_path / "thirdparty-nodebug.hcl"
    hcl.write_text("")

    external_logger = logging.getLogger("tests.thirdparty.nodebug")
    old_level = external_logger.level
    old_propagate = external_logger.propagate
    external_logger.setLevel(logging.DEBUG)
    external_logger.propagate = True

    def fake_run(*args, **kwargs):
        external_logger.debug("third-party-debug-should-not-appear")
        return [_fake_stat()]

    fake_engine = MagicMock()
    fake_engine.run.side_effect = fake_run

    job_id = db_module.create_job(str(hcl), debug_mode=False)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _wait_for_job_completion(job_id, db_module)
    finally:
        patcher.stop()
        external_logger.setLevel(old_level)
        external_logger.propagate = old_propagate

    logs = get_job_logs(job_id)
    assert not any(
        log["level"] == "DEBUG" and log["logger"] == "tests.thirdparty.nodebug"
        for log in logs
    ), "Non-collector DEBUG log records must not appear when debug_mode=False"


def test_run_queued_job_debug_mode_restores_root_level(tmp_path, tmp_db):
    """After a debug_mode job, the root logger level must be restored."""
    import collector.db as db_module  # noqa: PLC0415

    hcl = tmp_path / "restore.hcl"
    hcl.write_text("")

    root_logger = logging.getLogger()
    level_before = root_logger.level

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]

    db_module.create_job(str(hcl), debug_mode=True)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _wait_for_job_completion(db_module.get_jobs(limit=1)[0]["id"], db_module)
        assert root_logger.level == level_before, (
            "Root logger level was not restored after debug_mode job completed"
        )
    finally:
        patcher.stop()


def test_captured_job_logging_keeps_root_debug_until_last_overlap_exits(tmp_db):
    """Concurrent debug capture contexts must not restore root level too early."""
    import collector.db as db_module  # noqa: PLC0415
    import collector.job_lifecycle as job_lifecycle  # noqa: PLC0415
    from collector.job_lifecycle import captured_job_logging  # noqa: PLC0415

    root_logger = logging.getLogger()
    original_level = logging.INFO
    root_logger.setLevel(original_level)
    job_lifecycle._debug_capture_refcount = 0
    job_lifecycle._saved_root_level = logging.NOTSET

    entered = threading.Barrier(2)
    first_exited = threading.Event()
    release_second = threading.Event()
    results: dict[str, int] = {}

    def worker(name: str, wait_to_exit: threading.Event | None = None) -> None:
        job_id = db_module.create_job(f"/tmp/{name}.hcl", debug_mode=True)
        with captured_job_logging(job_id, capture_debug_logs=True):
            entered.wait()
            if wait_to_exit is not None:
                wait_to_exit.wait(timeout=2)
            results[f"{name}_inside"] = root_logger.level
        results[f"{name}_after"] = root_logger.level
        if name == "first":
            first_exited.set()

    first = threading.Thread(target=worker, args=("first",))
    second = threading.Thread(target=worker, args=("second", release_second))
    first.start()
    second.start()

    assert first_exited.wait(timeout=2), "first debug capture context did not exit"
    assert root_logger.level == logging.DEBUG
    release_second.set()

    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert results["first_inside"] == logging.DEBUG
    assert results["second_inside"] == logging.DEBUG
    assert results["first_after"] == logging.DEBUG
    assert results["second_after"] == original_level
    assert root_logger.level == original_level
    assert job_lifecycle._debug_capture_refcount == 0
    assert job_lifecycle._saved_root_level == logging.NOTSET


def test_cli_debug_level_captures_debug_logs(tmp_path, tmp_db):
    """Manual CLI runs with --log-level DEBUG should persist DEBUG records."""
    import collector.db as db_module  # noqa: PLC0415
    from collector.db import get_job_logs  # noqa: PLC0415

    hcl = tmp_path / "cli-debug.hcl"
    hcl.write_text("")

    collector_logger = logging.getLogger("collector.engine")

    def fake_run(*args, **kwargs):
        collector_logger.debug("cli-debug-marker")
        return [_fake_stat()]

    fake_engine = MagicMock()
    fake_engine.run.side_effect = fake_run

    job_id = db_module.create_job(str(hcl), debug_mode=False)

    root_logger = logging.getLogger()
    original_level = root_logger.level
    root_logger.setLevel(logging.DEBUG)

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        from main import _execute_job  # noqa: PLC0415

        _execute_job(job_id, str(hcl), dry_run=False, debug_mode=False)
        assert root_logger.level == logging.DEBUG, (
            "Root logger level should be restored to the CLI-configured DEBUG level"
        )
    finally:
        patcher.stop()
        root_logger.setLevel(original_level)

    logs = get_job_logs(job_id)
    assert any(
        log["level"] == "DEBUG" and "cli-debug-marker" in log["message"]
        for log in logs
    ), "CLI debug-level runs must persist DEBUG records even without debug_mode"



def test_run_queued_job_debug_captures_logs_from_executor_threads(tmp_path, tmp_db):
    """DEBUG records emitted from ThreadPoolExecutor workers must be captured.

    Root cause: JobLogHandler previously filtered by thread ID, so records
    from executor worker threads (which run in different threads than the job
    thread) were silently dropped.  The fix uses a contextvars.ContextVar to
    track the active job_id; the context is propagated to worker threads via
    contextvars.copy_context().run() in engine.py.
    """
    import time as _time  # noqa: PLC0415
    from concurrent.futures import ThreadPoolExecutor  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415
    from collector.db import get_job_logs  # noqa: PLC0415

    hcl = tmp_path / "executor_debug.hcl"
    hcl.write_text("")

    collector_logger = logging.getLogger("collector.engine")

    def fake_run(*args, **kwargs):
        # Simulate engine behaviour: submit work to a ThreadPoolExecutor and
        # emit a DEBUG record from inside the worker thread, propagating the
        # current contextvars context as the real engine does.
        import contextvars  # noqa: PLC0415

        current_ctx = contextvars.copy_context()

        def worker():
            collector_logger.debug("executor-debug-marker-99999")

        with ThreadPoolExecutor(max_workers=1) as pool:
            fut = pool.submit(current_ctx.run, worker)
            fut.result()  # wait for worker to finish

        s = MagicMock()
        s.object_name = "devices"
        s.processed = 1
        s.created = 0
        s.updated = 0
        s.skipped = 1
        s.errored = 0
        return [s]

    fake_engine = MagicMock()
    fake_engine.run.side_effect = fake_run

    job_id = db_module.create_job(str(hcl), debug_mode=True)

    from main import _check_and_run_queued_jobs  # noqa: PLC0415

    patcher = patch("collector.engine.Engine", return_value=fake_engine)
    patcher.start()
    try:
        _check_and_run_queued_jobs()
        _time.sleep(0.5)
    finally:
        patcher.stop()

    logs = get_job_logs(job_id)
    debug_logs = [lg for lg in logs if lg["level"] == "DEBUG"]
    assert any("executor-debug-marker-99999" in lg["message"] for lg in debug_logs), (
        "DEBUG log record from executor worker thread was not captured in job logs"
    )


def test_dispatch_job_creates_queued_record(tmp_path, tmp_db, monkeypatch):
    """POSTing /jobs/run must create a 'queued' job without running the engine."""
    from collector.db import get_jobs  # noqa: PLC0415
    from web.app import create_app  # noqa: PLC0415

    monkeypatch.setenv("WEB_AUTH_ENABLED", "false")
    flask_app = create_app()
    flask_app.config["TESTING"] = True

    hcl = tmp_path / "real.hcl"
    hcl.write_text("")

    with flask_app.test_client() as client:
        resp = client.post("/jobs/run", data={"hcl_file": str(hcl)})

    assert resp.status_code == 302
    assert "/jobs/" in resp.headers["Location"]

    jobs = get_jobs()
    assert len(jobs) == 1
    assert jobs[0]["status"] == "queued"  # collector has not run yet
