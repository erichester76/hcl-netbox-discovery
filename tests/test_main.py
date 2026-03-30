"""Tests for the CLI entry point (main.py)."""

from __future__ import annotations

import logging
import threading
from unittest.mock import MagicMock, patch

import pytest

import collector.db as db_module
from collector.db import get_job, get_jobs, init_db
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
    return s


def test_main_creates_db_job_on_success(tmp_path, tmp_db, monkeypatch):
    """main() must create a DB job row and mark it success for a good mapping."""
    hcl = tmp_path / "test.hcl"
    hcl.write_text("")  # file just needs to exist

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


def test_main_missing_mapping_does_not_create_db_job(tmp_db):
    """main() must not create a DB job for a mapping file that does not exist."""
    from main import main  # noqa: PLC0415
    rc = main(["--mapping", "/nonexistent/path.hcl"])
    assert rc == 1
    assert get_jobs() == []


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
    import time as _time  # noqa: PLC0415

    import collector.db as db_module  # noqa: PLC0415

    hcl = tmp_path / "test.hcl"
    hcl.write_text("")

    from datetime import datetime, timezone  # noqa: PLC0415

    # Create a schedule with next_run_at in the past
    past = "2000-01-01T00:00:00"
    db_module.create_schedule("test-sched", str(hcl), "0 * * * *", next_run_at=past)

    fake_engine = MagicMock()
    fake_engine.run.return_value = [_fake_stat()]

    import main as main_mod  # noqa: PLC0415

    # Reset the active schedule set between tests
    main_mod._active_schedule_ids.clear()

    with patch("collector.engine.Engine", return_value=fake_engine):
        from main import _check_and_fire_due_schedules  # noqa: PLC0415
        _check_and_fire_due_schedules()

    # Give the background thread a moment to finish
    _time.sleep(0.5)

    jobs = get_jobs()
    assert len(jobs) >= 1
