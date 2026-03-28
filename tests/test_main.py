"""Tests for the CLI entry point (main.py)."""

from __future__ import annotations

import logging

import pytest

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
