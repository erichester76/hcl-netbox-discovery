"""Tests for prerequisite methods in collector/prerequisites.py.

Covers:
- _ensure_platform race condition: uniqueness error falls back to GET (Fix 3)
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from collector.prerequisites import PrerequisiteRunner


class TestEnsurePlatformRaceCondition:
    """_ensure_platform should recover from a uniqueness collision."""

    def _make_runner(self, nb: MagicMock) -> PrerequisiteRunner:
        return PrerequisiteRunner(nb)

    def test_returns_id_on_success(self):
        nb = MagicMock()
        platform_obj = MagicMock(id=42)
        nb.upsert.return_value = platform_obj

        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

        assert result == 42

    def test_falls_back_to_get_on_uniqueness_error(self):
        """If upsert raises a 'unique' error, _ensure_platform falls back to GET."""
        nb = MagicMock()
        nb.upsert.side_effect = Exception(
            "The request failed with code 400 Bad Request: "
            "{'__all__': ['Platform name must be unique.']}"
        )
        platform_obj = MagicMock(id=99)
        nb.get.return_value = platform_obj

        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

        assert result == 99
        nb.get.assert_called_once_with("dcim.platforms", slug="vmware-esxi-70")

    def test_returns_none_when_fallback_get_also_fails(self):
        nb = MagicMock()
        nb.upsert.side_effect = Exception("Platform name must be unique.")
        nb.get.side_effect = Exception("not found")

        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

        assert result is None

    def test_re_raises_non_uniqueness_errors(self):
        nb = MagicMock()
        nb.upsert.side_effect = Exception("Network timeout")

        runner = self._make_runner(nb)
        with pytest.raises(Exception, match="Network timeout"):
            runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=False)

    def test_dry_run_returns_none_without_network_calls(self):
        nb = MagicMock()
        runner = self._make_runner(nb)
        result = runner._ensure_platform({"name": "VMware ESXi 7.0"}, dry_run=True)

        assert result is None
        nb.upsert.assert_not_called()
        nb.get.assert_not_called()

    def test_passes_manufacturer_id_to_upsert(self):
        nb = MagicMock()
        platform_obj = MagicMock(id=10)
        nb.upsert.return_value = platform_obj

        runner = self._make_runner(nb)
        runner._ensure_platform({"name": "ESXi", "manufacturer": 5}, dry_run=False)

        nb.upsert.assert_called_once()
        payload = nb.upsert.call_args[0][1]
        assert payload.get("manufacturer") == 5
