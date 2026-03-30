"""
File: collector/sources/utils.py
Purpose: Shared utility helpers used across source adapters.
Created: 2026-03-30
Last Changed: Copilot Issue: #cleanup
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Speed-string suffix map (value = Mbps multiplier)
# ---------------------------------------------------------------------------

_SPEED_SUFFIX_MAP: list[tuple[str, int]] = [
    ("gbps", 1000),
    ("gbit", 1000),
    ("g",    1000),
    ("mbps", 1),
    ("mbit", 1),
    ("m",    1),
    ("kbps", 0),
    ("kbit", 0),
    ("k",    0),
]


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def safe_get(obj: Any, key: str, default: Any = None) -> Any:
    """Return ``obj[key]`` (dict) or ``getattr(obj, key)`` (object) or *default*."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def parse_speed_mbps(speed_str: str, *, numeric_is_bps: bool = False) -> Optional[int]:
    """Parse a speed string into Mbps.

    Handles human-readable formats such as ``"10G"``, ``"10GBPS"``,
    ``"1 Gbps"`` and ``"100 Mbps"``, as well as bare numeric values.

    When *numeric_is_bps* is ``True`` a bare integer string is interpreted as
    bits-per-second (as returned by Cisco DNAC); otherwise it is treated as
    already being in Mbps (Nexus / generic convention).

    Returns ``None`` when the value is empty, zero, or cannot be parsed.
    """
    if not speed_str:
        return None
    s = str(speed_str).strip()

    # Human-readable suffix (e.g. "10G", "10GBPS", "1 Gbps", "100 Mbps")
    lower = s.lower().replace(" ", "")
    m = re.match(r"^(\d+(?:\.\d+)?)([a-z].*)$", lower)
    if m:
        value = float(m.group(1))
        suffix = m.group(2)
        for key, multiplier in _SPEED_SUFFIX_MAP:
            if suffix.startswith(key):
                return int(value * multiplier)
        return None  # unrecognised suffix

    # Bare integer
    if re.match(r"^\d+$", s):
        n = int(s)
        if n == 0:
            return None
        if numeric_is_bps:
            return max(1, n // 1_000_000)
        return n

    return None


def disable_ssl_warnings() -> None:
    """Suppress urllib3 InsecureRequestWarning for self-signed certificates."""
    import urllib3  # noqa: PLC0415
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def close_http_session(
    session: Optional[requests.Session],
    source_name: str = "",
) -> None:
    """Close *session* with standard error handling and return ``None``.

    The return value is always ``None`` so callers can reassign their
    session attribute in a single expression::

        self._session = close_http_session(self._session, "MySource")

    This achieves the same effect as the typical try/except/finally close
    pattern without repeating it in every source adapter.
    """
    if session is not None:
        try:
            session.close()
        except Exception as exc:
            logger.debug("%s session close error: %s", source_name, exc)
    return None
