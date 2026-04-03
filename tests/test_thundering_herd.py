"""Tests for the thundering-herd / global cooldown fix.

Validates that:
- RateLimiter.trigger_cooldown() blocks all threads until the cooldown expires.
- BackendAdapter._call() triggers a shared cooldown on 5xx overload codes.
- retry_5xx_cooldown_seconds propagates correctly through the config stack.

Created: 2026-04-01
Author: GitHub Copilot
Last Changed: GitHub Copilot 2026-04-01 Issue: #thundering-herd
"""

from __future__ import annotations

import sys
import os
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import collector.db as db_module

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))

from collector.db import init_db, set_setting
from pynetbox2 import RateLimiter, BackendAdapter, PynetboxAdapter  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _ConcreteAdapter(BackendAdapter):
    """Minimal concrete subclass for testing BackendAdapter._call()."""

    def get(self, resource, **filters):
        pass

    def list(self, resource, **filters):
        return []

    def create(self, resource, data):
        pass

    def update(self, resource, object_id, data):
        pass

    def delete(self, resource, object_id):
        return False


def _make_adapter(cooldown=1.0, retry_attempts=1, jitter=0.0) -> _ConcreteAdapter:
    limiter = RateLimiter()
    return _ConcreteAdapter(
        rate_limiter=limiter,
        retry_attempts=retry_attempts,
        retry_initial_delay_seconds=0.0,
        retry_backoff_factor=1.0,
        retry_max_delay_seconds=0.0,
        retry_jitter_seconds=jitter,
        retry_5xx_cooldown_seconds=cooldown,
    )


def _exc_with_status(code: int) -> Exception:
    """Build a fake exception that _extract_status_code() can decode."""
    exc = Exception(f"HTTP {code}")
    exc.status_code = code  # type: ignore[attr-defined]
    return exc


# ---------------------------------------------------------------------------
# RateLimiter: trigger_cooldown basics
# ---------------------------------------------------------------------------

class TestTriggerCooldown:
    def test_cooldown_blocks_acquire(self):
        """A triggered cooldown should cause acquire() to sleep."""
        limiter = RateLimiter()
        limiter.trigger_cooldown(0.1)
        t0 = time.perf_counter()
        limiter.acquire()
        elapsed = time.perf_counter() - t0
        # Should have waited at least 0.09 s (allow 10 ms tolerance)
        assert elapsed >= 0.09

    def test_trigger_only_extends_never_shortens(self):
        """A shorter trigger must not shorten an existing cooldown."""
        limiter = RateLimiter()
        long_until = time.perf_counter() + 10.0
        with limiter.lock:
            limiter._cooldown_until = long_until
        limiter.trigger_cooldown(0.001)  # much shorter
        with limiter.lock:
            assert limiter._cooldown_until == long_until

    def test_zero_cooldown_is_noop(self):
        """trigger_cooldown(0) must leave _cooldown_until unchanged."""
        limiter = RateLimiter()
        with limiter.lock:
            before = limiter._cooldown_until
        limiter.trigger_cooldown(0)
        with limiter.lock:
            assert limiter._cooldown_until == before

    def test_cooldown_expired_does_not_block(self):
        """After the cooldown has passed, acquire() should return immediately."""
        limiter = RateLimiter()
        # Set a cooldown that already expired
        with limiter.lock:
            limiter._cooldown_until = time.perf_counter() - 1.0
        t0 = time.perf_counter()
        limiter.acquire()
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.1


# ---------------------------------------------------------------------------
# RateLimiter: cross-thread coordination
# ---------------------------------------------------------------------------

class TestCooldownCrossThread:
    def test_all_threads_wait_for_cooldown(self):
        """All threads should block until the shared cooldown expires."""
        limiter = RateLimiter()
        limiter.trigger_cooldown(0.15)

        wake_times: list[float] = []
        lock = threading.Lock()

        def worker():
            limiter.acquire()
            with lock:
                wake_times.append(time.perf_counter())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        t0 = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        # All threads must have woken up at least 0.12 s after t0
        assert len(wake_times) == 4
        for wt in wake_times:
            assert wt - t0 >= 0.12, f"Thread woke too early: {wt - t0:.3f}s"

    def test_cooldown_from_one_thread_blocks_others(self):
        """A cooldown triggered in one thread must block threads started after it."""
        limiter = RateLimiter()

        wake_times: list[float] = []
        lock = threading.Lock()

        def slow_worker():
            # This thread triggers the cooldown then finishes
            limiter.trigger_cooldown(0.15)

        def fast_worker():
            limiter.acquire()
            with lock:
                wake_times.append(time.perf_counter())

        t0 = time.perf_counter()
        st = threading.Thread(target=slow_worker)
        st.start()
        st.join()

        threads = [threading.Thread(target=fast_worker) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        assert len(wake_times) == 3
        for wt in wake_times:
            assert wt - t0 >= 0.12


# ---------------------------------------------------------------------------
# BackendAdapter._call(): triggers cooldown on 5xx
# ---------------------------------------------------------------------------

class TestBackendAdapterCooldown:
    def test_503_triggers_global_cooldown(self):
        """_call() must call trigger_cooldown() when a 503 is received."""
        adapter = _make_adapter(cooldown=5.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(503)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        # attempt=0 → cooldown = 5.0 * (1.0 ** 0) = 5.0
        adapter.rate_limiter.trigger_cooldown.assert_called_with(5.0)

    def test_504_triggers_global_cooldown(self):
        """_call() must call trigger_cooldown() when a 504 is received."""
        adapter = _make_adapter(cooldown=5.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(504)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_called_with(5.0)

    def test_cooldown_scales_with_attempt(self):
        """Each successive 5xx must trigger a longer cooldown (backoff_factor=2)."""
        adapter = _ConcreteAdapter(
            rate_limiter=MagicMock(spec=RateLimiter),
            retry_attempts=2,
            retry_initial_delay_seconds=0.0,
            retry_backoff_factor=2.0,
            retry_max_delay_seconds=0.0,
            retry_5xx_cooldown_seconds=10.0,
        )
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def always_fails():
            raise _exc_with_status(503)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(always_fails)

        calls = [c.args[0] for c in adapter.rate_limiter.trigger_cooldown.call_args_list]
        # attempt 0 (retried): 10 * 2^0 = 10.0
        # attempt 1 (retried): 10 * 2^1 = 20.0
        # attempt 2 (final, re-raises immediately): no cooldown triggered
        assert calls == pytest.approx([10.0, 20.0])

    def test_4xx_does_not_trigger_cooldown(self):
        """A 404 error must NOT trigger the global cooldown."""
        adapter = _make_adapter(cooldown=5.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(404)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_not_called()

    def test_zero_cooldown_skips_trigger(self):
        """If retry_5xx_cooldown_seconds=0, trigger_cooldown must not be called."""
        adapter = _make_adapter(cooldown=0.0)
        adapter.rate_limiter = MagicMock(spec=RateLimiter)
        adapter.rate_limiter.acquire = MagicMock(return_value=0.0)

        def fail_once():
            raise _exc_with_status(503)

        with patch("time.sleep"):
            with pytest.raises(Exception):
                adapter._call(fail_once)

        adapter.rate_limiter.trigger_cooldown.assert_not_called()

    def test_succeeds_after_cooldown_clears(self):
        """After the cooldown expires, _call() should succeed normally."""
        limiter = RateLimiter()
        adapter = _ConcreteAdapter(
            rate_limiter=limiter,
            retry_attempts=1,
            retry_initial_delay_seconds=0.0,
            retry_backoff_factor=1.0,
            retry_max_delay_seconds=0.0,
            retry_5xx_cooldown_seconds=0.1,
        )

        calls = []

        def sometimes_fails():
            calls.append(1)
            if len(calls) == 1:
                raise _exc_with_status(503)
            return "ok"

        result = adapter._call(sometimes_fails)
        assert result == "ok"
        assert len(calls) == 2


class TestPynetboxListRetryOwnership:
    def test_list_page_fetch_uses_only_backend_retry_loop(self):
        with patch("pynetbox2.pynetbox.api"):
            adapter = PynetboxAdapter(
                url="http://nb.example.com",
                token="token",
                rate_limiter=RateLimiter(),
                retry_attempts=1,
                retry_initial_delay_seconds=0.0,
                retry_backoff_factor=1.0,
                retry_max_delay_seconds=0.0,
                retry_jitter_seconds=0.0,
                retry_5xx_cooldown_seconds=0.0,
            )

        endpoint = MagicMock()
        endpoint.filter.side_effect = lambda **kwargs: (
            SimpleNamespace(count=1001)
            if kwargs["limit"] == 0
            else (_ for _ in ()).throw(_exc_with_status(503))
        )

        with patch.object(adapter, "_endpoint", return_value=endpoint), patch("time.sleep") as mock_sleep:
            with pytest.raises(Exception):
                adapter.list("dcim.sites")

        # One count request plus two page attempts from BackendAdapter._call().
        assert endpoint.filter.call_count == 3
        assert mock_sleep.call_count == 1


# ---------------------------------------------------------------------------
# Config: retry_5xx_cooldown propagates from HCL / env-var
# ---------------------------------------------------------------------------

class TestRetry5xxCooldownConfig:
    def test_default_cooldown_in_netbox_config(self):
        from collector.config import NetBoxConfig
        cfg = NetBoxConfig(url="http://nb", token="x")
        assert cfg.retry_5xx_cooldown == pytest.approx(60.0)

    def test_runtime_setting_sets_cooldown(self, monkeypatch, tmp_path):
        db_path = str(tmp_path / "thundering_herd.sqlite3")
        monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
        monkeypatch.setattr(db_module, "_lock", threading.Lock())
        init_db()
        set_setting("NETBOX_RETRY_5XX_COOLDOWN", "45.0")
        hcl = """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url   = "https://nb.example.com"
              token = "tok"
            }
        """
        with tempfile.NamedTemporaryFile(suffix=".hcl", mode="w", delete=False) as f:
            f.write(hcl)
            path = f.name
        try:
            from collector.config import load_config
            cfg = load_config(path)
        finally:
            os.unlink(path)
        assert cfg.netbox.retry_5xx_cooldown == pytest.approx(45.0)

    def test_hcl_setting_overrides_runtime_setting(self, monkeypatch, tmp_path):
        db_path = str(tmp_path / "thundering_herd_override.sqlite3")
        monkeypatch.setenv("COLLECTOR_DB_PATH", db_path)
        monkeypatch.setattr(db_module, "_lock", threading.Lock())
        init_db()
        set_setting("NETBOX_RETRY_5XX_COOLDOWN", "99.0")
        hcl = """
            source "vmware" {
              api_type = "vmware"
              url      = "vc.example.com"
            }
            netbox {
              url                = "https://nb.example.com"
              token              = "tok"
              retry_5xx_cooldown = 20
            }
        """
        with tempfile.NamedTemporaryFile(suffix=".hcl", mode="w", delete=False) as f:
            f.write(hcl)
            path = f.name
        try:
            from collector.config import load_config
            cfg = load_config(path)
        finally:
            os.unlink(path)
        assert cfg.netbox.retry_5xx_cooldown == pytest.approx(20.0)

    def test_passed_to_pynetbox2_client(self, monkeypatch):
        """retry_5xx_cooldown_seconds must be forwarded to the NetBoxExtendedClient."""
        from pynetbox2 import NetBoxExtendedClient
        client = NetBoxExtendedClient(
            "http://nb.example.com",
            "token",
            retry_5xx_cooldown_seconds=77.0,
        )
        assert client.config.retry_5xx_cooldown_seconds == pytest.approx(77.0)
        assert client.adapter.retry_5xx_cooldown_seconds == pytest.approx(77.0)

    def test_api_factory_forwards_retry_5xx_cooldown(self):
        """api() factory must accept and forward retry_5xx_cooldown_seconds.

        Regression test: the parameter was present on NetBoxExtendedClient but
        was missing from the api() wrapper, causing a TypeError at runtime.
        """
        import pynetbox2 as pynetbox
        client = pynetbox.api(
            "http://nb.example.com",
            "token",
            retry_5xx_cooldown_seconds=33.0,
        )
        assert client.config.retry_5xx_cooldown_seconds == pytest.approx(33.0)
        assert client.adapter.retry_5xx_cooldown_seconds == pytest.approx(33.0)
