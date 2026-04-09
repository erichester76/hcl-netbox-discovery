"""Tests for the version bump helper."""

from __future__ import annotations

import pytest

from scripts.bump_version import bump_version, main, parse_version, read_pyproject_version


def test_parse_version_rejects_non_semver():
    with pytest.raises(ValueError, match="Invalid semantic version"):
        parse_version("1.2")


@pytest.mark.parametrize(
    ("current", "bump_type", "expected"),
    [
        ("1.2.3", "patch", "1.2.4"),
        ("1.2.3", "minor", "1.3.0"),
        ("1.2.3", "major", "2.0.0"),
    ],
)
def test_bump_version(current, bump_type, expected):
    assert bump_version(current, bump_type) == expected


def test_main_bumps_patch_version(tmp_path, capsys):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.poetry]\nversion = "1.2.3"\n', encoding="utf-8")

    rc = main(["patch", "--pyproject", str(pyproject)])

    assert rc == 0
    assert read_pyproject_version(pyproject) == "1.2.4"
    assert capsys.readouterr().out.strip() == "1.2.4"


def test_main_sets_explicit_version(tmp_path, capsys):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.poetry]\nversion = "1.2.3"\n', encoding="utf-8")

    rc = main(["--set", "2.0.0", "--pyproject", str(pyproject)])

    assert rc == 0
    assert read_pyproject_version(pyproject) == "2.0.0"
    assert capsys.readouterr().out.strip() == "2.0.0"


def test_main_print_only_preserves_file(tmp_path, capsys):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.poetry]\nversion = "1.2.3"\n', encoding="utf-8")

    rc = main(["minor", "--print-only", "--pyproject", str(pyproject)])

    assert rc == 0
    assert read_pyproject_version(pyproject) == "1.2.3"
    assert capsys.readouterr().out.strip() == "1.3.0"
