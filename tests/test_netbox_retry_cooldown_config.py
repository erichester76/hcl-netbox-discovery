"""Integration tests for retry_5xx_cooldown wiring from HCL/config into the wrapper."""

from __future__ import annotations

import os
import tempfile
import threading

import pytest

import collector.db as db_module
from collector.db import init_db, set_setting


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
        with tempfile.NamedTemporaryFile(suffix=".hcl", mode="w", delete=False) as handle:
            handle.write(hcl)
            path = handle.name
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
        with tempfile.NamedTemporaryFile(suffix=".hcl", mode="w", delete=False) as handle:
            handle.write(hcl)
            path = handle.name
        try:
            from collector.config import load_config

            cfg = load_config(path)
        finally:
            os.unlink(path)
        assert cfg.netbox.retry_5xx_cooldown == pytest.approx(20.0)
