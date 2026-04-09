#!/usr/bin/env python3
"""Bump the Poetry project version using semantic versioning rules."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

VERSION_RE = re.compile(r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$")
PYPROJECT_VERSION_RE = re.compile(r'^(version\s*=\s*")(?P<version>[^"]+)(")$', re.MULTILINE)


def parse_version(value: str) -> tuple[int, int, int]:
    match = VERSION_RE.fullmatch(value)
    if not match:
        raise ValueError(f"Invalid semantic version: {value!r}")
    return tuple(int(match.group(part)) for part in ("major", "minor", "patch"))


def format_version(version: tuple[int, int, int]) -> str:
    return ".".join(str(part) for part in version)


def bump_version(current: str, bump_type: str) -> str:
    major, minor, patch = parse_version(current)
    if bump_type == "major":
        return format_version((major + 1, 0, 0))
    if bump_type == "minor":
        return format_version((major, minor + 1, 0))
    if bump_type == "patch":
        return format_version((major, minor, patch + 1))
    raise ValueError(f"Unsupported bump type: {bump_type}")


def read_pyproject_version(pyproject_path: Path) -> str:
    text = pyproject_path.read_text(encoding="utf-8")
    match = PYPROJECT_VERSION_RE.search(text)
    if not match:
        raise RuntimeError(f"Could not read Poetry version from {pyproject_path}")
    return match.group("version")


def update_pyproject_version(pyproject_path: Path, new_version: str) -> None:
    parse_version(new_version)
    text = pyproject_path.read_text(encoding="utf-8")

    def replace(match: re.Match[str]) -> str:
        return f'{match.group(1)}{new_version}{match.group(3)}'

    updated, count = PYPROJECT_VERSION_RE.subn(replace, text, count=1)
    if count != 1:
        raise RuntimeError(f"Could not update Poetry version in {pyproject_path}")
    pyproject_path.write_text(updated, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bump",
        nargs="?",
        choices=("major", "minor", "patch"),
        help="Semantic version component to bump.",
    )
    parser.add_argument(
        "--set",
        dest="set_version",
        help="Set an explicit semantic version instead of bumping.",
    )
    parser.add_argument(
        "--pyproject",
        default="pyproject.toml",
        help="Path to pyproject.toml (default: pyproject.toml).",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print the computed version without writing it back.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if bool(args.bump) == bool(args.set_version):
        parser.error("Provide exactly one of a bump type or --set.")

    pyproject_path = Path(args.pyproject)
    current_version = read_pyproject_version(pyproject_path)
    new_version = args.set_version or bump_version(current_version, args.bump)
    parse_version(new_version)

    if args.print_only:
        print(new_version)
        return 0

    update_pyproject_version(pyproject_path, new_version)
    print(new_version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
