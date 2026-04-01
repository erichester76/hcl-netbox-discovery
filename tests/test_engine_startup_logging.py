"""
File: tests/test_engine_startup_logging.py
Purpose: Tests for config-summary INFO logging emitted at the start of Engine.run().
Created: 2026-04-01
Last Changed: Copilot Issue: #add-info-logging-collector-start
"""
# Covers:
# - NetBox URL and masked token are logged
# - Cache, rate-limit, and retry settings are logged
# - Collector options (max_workers, sync_tag, dry_run) are logged
# - Token masking logic (short token, empty token)

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from collector.config import CollectorConfig, CollectorOptions, NetBoxConfig, SourceConfig
from collector.engine import Engine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_collector_config(
    *,
    nb_url: str = "https://netbox.example.com",
    nb_token: str = "abcdefgh1234",
    nb_cache: str = "none",
    nb_cache_url: str = "",
    nb_cache_ttl: int = 300,
    nb_cache_key_prefix: str = "nbx:",
    nb_rate_limit: float = 0.0,
    nb_rate_limit_burst: int = 1,
    nb_retry_attempts: int = 3,
    nb_retry_initial_delay: float = 0.3,
    nb_retry_backoff_factor: float = 2.0,
    nb_retry_max_delay: float = 15.0,
    nb_retry_jitter: float = 0.0,
    nb_retry_on_4xx: str = "408,409,425,429",
    nb_retry_5xx_cooldown: float = 60.0,
    nb_branch: str | None = None,
    col_max_workers: int = 4,
    col_dry_run: bool = False,
    col_sync_tag: str = "",
    source_api_type: str = "vmware",
) -> CollectorConfig:
    nb_cfg = NetBoxConfig(
        url=nb_url,
        token=nb_token,
        cache=nb_cache,
        cache_url=nb_cache_url,
        cache_ttl=nb_cache_ttl,
        cache_key_prefix=nb_cache_key_prefix,
        rate_limit=nb_rate_limit,
        rate_limit_burst=nb_rate_limit_burst,
        retry_attempts=nb_retry_attempts,
        retry_initial_delay=nb_retry_initial_delay,
        retry_backoff_factor=nb_retry_backoff_factor,
        retry_max_delay=nb_retry_max_delay,
        retry_jitter=nb_retry_jitter,
        retry_on_4xx=nb_retry_on_4xx,
        retry_5xx_cooldown=nb_retry_5xx_cooldown,
        branch=nb_branch,
    )
    col_cfg = CollectorOptions(
        max_workers=col_max_workers,
        dry_run=col_dry_run,
        sync_tag=col_sync_tag,
    )
    src_cfg = SourceConfig(api_type=source_api_type, url="vcenter.example.com")
    return CollectorConfig(
        source=src_cfg,
        netbox=nb_cfg,
        collector=col_cfg,
        objects=[],
        raw_source_body={},
        source_label=source_api_type,
    )


def _run_engine_with_cfg(cfg: CollectorConfig) -> list[str]:
    """Run Engine.run() with a mocked config and return captured log messages."""
    nb_mock = MagicMock()
    nb_mock.close = MagicMock()

    log_records: list[str] = []

    class CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_records.append(self.format(record))

    handler = CapturingHandler()
    engine_logger = logging.getLogger("collector.engine")
    engine_logger.addHandler(handler)
    engine_logger.setLevel(logging.DEBUG)
    try:
        with patch("collector.engine.load_config", return_value=cfg), \
             patch("collector.engine._build_nb_client", return_value=nb_mock), \
             patch.object(Engine, "_run_pass", return_value=[]):
            Engine().run("fake.hcl")
    except Exception:
        pass
    finally:
        engine_logger.removeHandler(handler)

    return log_records


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConfigSummaryLogging:
    """Verify that Engine.run() emits INFO config-summary messages."""

    def _capture_run_logs(self, cfg: CollectorConfig) -> list[str]:
        return _run_engine_with_cfg(cfg)

    def test_netbox_url_logged(self):
        cfg = _make_collector_config(nb_url="https://nb.corp.example.com")
        logs = self._capture_run_logs(cfg)
        assert any("url=https://nb.corp.example.com" in m for m in logs), logs

    def test_token_is_masked_in_log(self):
        cfg = _make_collector_config(nb_token="supersecrettoken")
        logs = self._capture_run_logs(cfg)
        # Full token must not appear
        assert not any("supersecrettoken" in m for m in logs), logs
        # First 4 chars + mask suffix must appear
        assert any("supe****" in m for m in logs), logs

    def test_short_token_fully_masked(self):
        cfg = _make_collector_config(nb_token="ab")
        logs = self._capture_run_logs(cfg)
        assert any("****" in m for m in logs), logs
        assert not any("ab****" in m for m in logs), logs

    def test_empty_token_shows_mask(self):
        cfg = _make_collector_config(nb_token="")
        logs = self._capture_run_logs(cfg)
        assert any("****" in m for m in logs), logs

    def test_cache_backend_logged(self):
        cfg = _make_collector_config(nb_cache="redis", nb_cache_url="redis://redis:6379/0")
        logs = self._capture_run_logs(cfg)
        assert any("redis" in m for m in logs), logs

    def test_rate_limit_logged(self):
        cfg = _make_collector_config(nb_rate_limit=10.0, nb_rate_limit_burst=5)
        logs = self._capture_run_logs(cfg)
        assert any("10.0" in m for m in logs), logs
        assert any("burst=5" in m for m in logs), logs

    def test_retry_attempts_logged(self):
        cfg = _make_collector_config(nb_retry_attempts=7)
        logs = self._capture_run_logs(cfg)
        assert any("attempts=7" in m for m in logs), logs

    def test_collector_max_workers_logged(self):
        cfg = _make_collector_config(col_max_workers=8)
        logs = self._capture_run_logs(cfg)
        assert any("max_workers=8" in m for m in logs), logs

    def test_collector_dry_run_logged(self):
        cfg = _make_collector_config(col_dry_run=True)
        logs = self._capture_run_logs(cfg)
        assert any("dry_run=True" in m for m in logs), logs

    def test_collector_sync_tag_logged(self):
        cfg = _make_collector_config(col_sync_tag="my-sync-tag")
        logs = self._capture_run_logs(cfg)
        assert any("my-sync-tag" in m for m in logs), logs

    def test_branch_logged_when_set(self):
        cfg = _make_collector_config(nb_branch="feature-branch")
        logs = self._capture_run_logs(cfg)
        assert any("feature-branch" in m for m in logs), logs

    def test_branch_default_when_not_set(self):
        cfg = _make_collector_config(nb_branch=None)
        logs = self._capture_run_logs(cfg)
        assert any("(default)" in m for m in logs), logs
