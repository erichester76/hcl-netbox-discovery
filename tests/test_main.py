"""Tests for the CLI entry point (main.py)."""

from __future__ import annotations

import logging
import threading
from unittest.mock import MagicMock, patch

import pytest

import collector.db as db_module
from collector.db import get_jobs, init_db
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
    assert job["artifact"] is not None
    assert job["artifact"]["status"] == "success"
    assert job["artifact"]["summary"]["devices"]["created"] == 1


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
    from main import _summary_from_stats  # noqa: PLC0415

    stat = _fake_stat()
    stat.nested_skipped = {
        "virtualization.interfaces:virtual_machine": 12,
        "virtualization.virtual_disks:virtual_machine": 7,
    }

    summary, has_errors = _summary_from_stats([stat])

    assert has_errors is False
    assert summary["devices"]["nested_skipped"] == {
        "virtualization.interfaces:virtual_machine": 12,
        "virtualization.virtual_disks:virtual_machine": 7,
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
