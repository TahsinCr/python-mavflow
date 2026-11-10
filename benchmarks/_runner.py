"""Shared pyperf runner setup and Mavflow source provenance."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyperf

ROOT = Path(__file__).resolve().parents[1]


def positive_integer(value: str) -> int:
    """Parse a positive integer for a benchmark workload option."""
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be a positive integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return number


def source_metadata(root: Path = ROOT) -> dict[str, str]:
    """Return pyperf metadata for source identity and the local Git state."""
    source_paths = sorted((root / "mavflow").rglob("*.py"))
    source_paths.extend(sorted((root / "benchmarks").glob("*.py")))
    source_paths.append(root / "pyproject.toml")

    digest = hashlib.sha256()
    for path in source_paths:
        if path.is_file():
            digest.update(path.relative_to(root).as_posix().encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")

    revision = _git_value(root, "rev-parse", "HEAD")
    dirty_status = _git_value(
        root, "status", "--porcelain", "--", "mavflow", "benchmarks", "pyproject.toml"
    )
    return {
        "mavflow_source_fingerprint_sha256": digest.hexdigest(),
        "mavflow_git_revision": revision or "unavailable",
        "mavflow_source_dirty": (
            "unknown" if dirty_status is None else str(bool(dirty_status)).lower()
        ),
    }


def create_runner(module_name: str) -> pyperf.Runner:
    """Create one pyperf runner with worker-safe workload and provenance flags."""
    import pyperf

    return pyperf.Runner(
        metadata=source_metadata(),
        program_args=["-m", module_name],
        add_cmdline_args=_append_workload_arguments,
    )


def _append_workload_arguments(command: list[str], args: argparse.Namespace) -> None:
    for attribute, option in (
        ("messages", "--messages"),
        ("subscribers", "--subscribers"),
        ("payload_bytes", "--payload-bytes"),
    ):
        value: Any = getattr(args, attribute, None)
        if value is not None:
            command.extend((option, str(value)))


def _git_value(root: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip()
