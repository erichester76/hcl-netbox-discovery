"""Tests for derived NetBox cache key prefixes."""

from __future__ import annotations

import sys
import types

from collector.cache_keys import build_effective_cache_key_prefix
from collector.config import NetBoxConfig
from collector.engine import _build_nb_client
from web.app import _cache_client_kwargs


def test_build_effective_cache_key_prefix_scopes_by_branch_and_url() -> None:
    prefix_one = build_effective_cache_key_prefix(
        "nbx:",
        netbox_url="https://netbox-a.example.com/api/",
        git_branch="dev",
    )
    prefix_two = build_effective_cache_key_prefix(
        "nbx:",
        netbox_url="https://netbox-b.example.com/api/",
        git_branch="dev",
    )

    assert prefix_one.startswith("nbx:dev:")
    assert prefix_two.startswith("nbx:dev:")
    assert prefix_one != prefix_two


def test_build_effective_cache_key_prefix_normalizes_prefix_and_branch() -> None:
    prefix = build_effective_cache_key_prefix(
        "custom",
        netbox_url="https://netbox.example.com",
        git_branch="Feature/Add Cache Scope",
    )

    assert prefix.startswith("custom:feature-add-cache-scope:")
    assert prefix.endswith(":")


def test_web_cache_client_kwargs_uses_effective_prefix(monkeypatch) -> None:
    config_values = {
        "NETBOX_CACHE_BACKEND": "redis",
        "NETBOX_CACHE_URL": "redis://redis:6379/0",
        "NETBOX_URL": "https://netbox.example.com",
        "NETBOX_TOKEN": "token",
        "NETBOX_CACHE_KEY_PREFIX": "nbx:",
        "NETBOX_CACHE_TTL": "300",
        "NETBOX_PREWARM_SENTINEL_TTL": "",
        "NETBOX_USE_TURBOBULK": "true",
    }

    monkeypatch.setattr("web.app.get_config", lambda key, default="": config_values.get(key, default))
    monkeypatch.setattr(
        "collector.cache_keys.get_code_version",
        lambda: {"git_branch": "dev"},
    )

    kwargs = _cache_client_kwargs()

    assert kwargs["cache_key_prefix"].startswith("nbx:dev:")
    assert kwargs["url"] == "https://netbox.example.com"
    assert kwargs["turbobulk_export_for_prewarm"] is True


def test_web_cache_client_kwargs_accepts_truthy_turbobulk_values(monkeypatch) -> None:
    config_values = {
        "NETBOX_CACHE_BACKEND": "redis",
        "NETBOX_CACHE_URL": "redis://redis:6379/0",
        "NETBOX_URL": "https://netbox.example.com",
        "NETBOX_TOKEN": "token",
        "NETBOX_CACHE_KEY_PREFIX": "nbx:",
        "NETBOX_CACHE_TTL": "300",
        "NETBOX_PREWARM_SENTINEL_TTL": "",
        "NETBOX_USE_TURBOBULK": "yes",
    }

    monkeypatch.setattr("web.app.get_config", lambda key, default="": config_values.get(key, default))
    monkeypatch.setattr(
        "collector.cache_keys.get_code_version",
        lambda: {"git_branch": "dev"},
    )

    kwargs = _cache_client_kwargs()

    assert kwargs["turbobulk_export_for_prewarm"] is True


def test_engine_build_nb_client_uses_effective_prefix(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_api(**kwargs):
        captured.update(kwargs)
        return object()

    fake_module = types.SimpleNamespace(api=fake_api)
    monkeypatch.setitem(sys.modules, "pynetbox2", fake_module)
    monkeypatch.setattr(
        "collector.cache_keys.get_code_version",
        lambda: {"git_branch": "dev"},
    )

    cfg = NetBoxConfig(
        url="https://netbox.example.com",
        token="token",
        cache="redis",
        cache_url="redis://redis:6379/0",
    )

    _build_nb_client(cfg)

    assert captured["cache_key_prefix"].startswith("nbx:dev:")
    assert captured["url"] == "https://netbox.example.com"
    assert captured["turbobulk_export_for_prewarm"] is False
