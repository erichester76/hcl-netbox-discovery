"""Authentication and CSRF helpers for the Flask web application."""

from __future__ import annotations

import hmac
import os
import secrets

from flask import abort, request, session
from werkzeug.security import check_password_hash

from collector.db import get_config


def auth_enabled() -> bool:
    return os.environ.get("WEB_AUTH_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}


def configured_username() -> str:
    return os.environ.get("WEB_USERNAME", "admin").strip() or "admin"


def configured_password() -> str:
    return os.environ.get("WEB_PASSWORD", "")


def configured_password_hash() -> str:
    return os.environ.get("WEB_PASSWORD_HASH", "").strip()


def configured_api_token() -> str:
    return get_config("WEB_API_TOKEN", "").strip()


def auth_configuration_error() -> str | None:
    if not auth_enabled():
        return None
    if configured_password_hash():
        return None
    password = configured_password()
    if not password:
        return "WEB auth is enabled but no credentials are configured. Set WEB_PASSWORD or WEB_PASSWORD_HASH."
    if password == "change-me-in-production":
        return "WEB_PASSWORD must be changed from the default placeholder before starting the web UI."
    return None


def credentials_match(username: str, password: str) -> bool:
    expected_username = configured_username()
    if not hmac.compare_digest(username, expected_username):
        return False

    password_hash = configured_password_hash()
    if password_hash:
        return check_password_hash(password_hash, password)

    expected_password = configured_password()
    return bool(expected_password) and hmac.compare_digest(password, expected_password)


def is_authenticated() -> bool:
    return bool(session.get("authenticated"))


def is_auth_exempt(endpoint: str | None) -> bool:
    return endpoint in {"login", "static"}


def request_api_token() -> str:
    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return request.headers.get("X-API-Key", "").strip()


def api_token_matches_request() -> bool:
    expected_token = configured_api_token()
    supplied_token = request_api_token()
    return bool(expected_token and supplied_token) and hmac.compare_digest(supplied_token, expected_token)


def safe_next_target(target: str) -> str:
    cleaned = (target or "").strip()
    if not cleaned:
        return ""
    if cleaned.startswith("http://") or cleaned.startswith("https://") or cleaned.startswith("//"):
        return ""
    if not cleaned.startswith("/"):
        return ""
    if cleaned.endswith("?"):
        cleaned = cleaned[:-1]
    return cleaned or ""


def csrf_token() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def validate_csrf() -> None:
    supplied = request.form.get("csrf_token") or request.headers.get("X-CSRF-Token", "")
    expected = csrf_token()
    if not supplied or not hmac.compare_digest(supplied, expected):
        abort(400, description="Invalid CSRF token")
