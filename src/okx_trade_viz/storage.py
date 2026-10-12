from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def private_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


def write_private_json(path: Path, value: Any) -> None:
    private_directory(path.parent)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=False)
        handle.write("\n")


def write_private_text(path: Path, value: str) -> None:
    private_directory(path.parent)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(value)
        if not value.endswith("\n"):
            handle.write("\n")


def write_private_text_preserving_parent(path: Path, value: str) -> None:
    """Write a private file without changing an existing parent directory's mode."""

    parent_exists = path.parent.exists()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not parent_exists:
        path.parent.chmod(0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
    except OSError:
        os.close(descriptor)
        raise
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(value)
        if not value.endswith("\n"):
            handle.write("\n")


def append_private_jsonl(path: Path, value: Any) -> None:
    private_directory(path.parent)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
        json.dump(
            value,
            handle,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        handle.write("\n")
