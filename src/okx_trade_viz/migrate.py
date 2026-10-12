"""One-time, non-destructive import of the original okx-analysis evidence archive.

Only the two account databases, referenced raw snapshots, and public candle
cache are imported. Source SQLite databases are read through the backup API so
uncheckpointed WAL transactions are included in a consistent snapshot.
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any


RAW_PROOF_KEYS = frozenset({
    "metadata", "account_config", "fills", "bills", "orders", "algo_orders",
    "fill_order_details", "positions", "instruments", "snapshot_times", "quality",
})


@dataclass(frozen=True)
class MigrationResult:
    source_root: Path
    destination_root: Path
    raw_files: int
    candle_files: int


def _private_directory(path: Path, base: Path) -> None:
    """Create every directory beneath our private staging root with mode 0700."""
    current = base
    for component in path.relative_to(base).parts:
        current /= component
        current.mkdir(mode=0o700, exist_ok=True)
        current.chmod(0o700)


def _private_copy(source: Path, destination: Path) -> None:
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with source.open("rb") as readable, os.fdopen(descriptor, "wb") as writable:
        shutil.copyfileobj(readable, writable)


def _private_raw_copy(source: Path, destination: Path) -> None:
    with source.open(encoding="utf-8") as readable:
        payload = json.load(readable)
    if not isinstance(payload, dict):
        raise ValueError(f"Raw snapshot is not a JSON object: {source}")
    if not isinstance(payload.get("metadata"), dict) or not isinstance(
        payload.get("quality"), dict
    ):
        raise ValueError(f"Raw snapshot lacks metadata/quality proof: {source}")
    selected: dict[str, Any] = {
        key: value for key, value in payload.items() if key in RAW_PROOF_KEYS
    }
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as writable:
        json.dump(selected, writable, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        writable.write("\n")


def _raw_relative(value: str, source_root: Path, source_data: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = source_root / path
    resolved = path.resolve(strict=True)
    if resolved.name != "raw.json" or not resolved.is_file() or not resolved.is_relative_to(
        source_data
    ):
        raise ValueError(f"Referenced raw snapshot is outside {source_data}: {value}")
    return resolved.relative_to(source_data)


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _references(
    archive_db: Path, orders_db: Path, source_root: Path, source_data: Path
) -> tuple[list[tuple[int, Path]], list[tuple[int, Path, dict[str, Any]]]]:
    collection: list[tuple[int, Path]] = []
    imports: list[tuple[int, Path, dict[str, Any]]] = []
    with closing(_connect_readonly(archive_db)) as db:
        for row in db.execute(
            "SELECT id, raw_path FROM collection_runs WHERE raw_path IS NOT NULL"
        ):
            collection.append((
                int(row["id"]), _raw_relative(str(row["raw_path"]), source_root, source_data)
            ))
    with closing(_connect_readonly(orders_db)) as db:
        for row in db.execute(
            "SELECT id, details_json FROM sync_runs WHERE kind = 'raw_import'"
        ):
            try:
                details = json.loads(row["details_json"])
                if not isinstance(details, dict) or not isinstance(details.get("raw_path"), str):
                    raise ValueError("missing raw_path")
                relative = _raw_relative(details["raw_path"], source_root, source_data)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid raw_import run {row['id']}: {exc}") from exc
            imports.append((int(row["id"]), relative, details))
    return collection, imports


def _cache_files(source_data: Path) -> list[tuple[Path, Path]]:
    cache_root = source_data / "market_candles"
    if not cache_root.exists():
        return []
    if cache_root.is_symlink() or not cache_root.is_dir():
        raise ValueError(f"Unsafe candle cache directory: {cache_root}")
    files: list[tuple[Path, Path]] = []
    for path in sorted(cache_root.rglob("*")):
        if path.is_symlink() or not path.resolve(strict=True).is_relative_to(source_data):
            raise ValueError(f"Candle cache path escapes source data: {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError(f"Unsupported candle cache item: {path}")
        files.append((path, path.relative_to(source_data)))
    return files


def _backup(source: Path, destination: Path) -> None:
    with closing(_connect_readonly(source)) as readable:
        with closing(sqlite3.connect(destination)) as writable:
            destination.chmod(0o600)
            readable.backup(writable)
            writable.commit()


def _rebase_archive(path: Path, references: list[tuple[int, Path]], final_data: Path) -> None:
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA journal_mode=DELETE")
        with db:
            db.executemany(
                "UPDATE collection_runs SET raw_path = ? WHERE id = ?",
                ((str(final_data / relative), run_id) for run_id, relative in references),
            )
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Migrated fill archive failed SQLite integrity check")


def _rebase_orders(
    path: Path, references: list[tuple[int, Path, dict[str, Any]]], final_data: Path
) -> None:
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA journal_mode=DELETE")
        with db:
            for run_id, relative, details in references:
                migrated = dict(details)
                migrated["raw_path"] = str(final_data / relative)
                db.execute(
                    "UPDATE sync_runs SET details_json = ? WHERE id = ?",
                    (json.dumps(migrated, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":"), allow_nan=False), run_id),
                )
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Migrated order archive failed SQLite integrity check")


def migrate_data(source_root: Path, destination_root: Path) -> MigrationResult:
    """Copy only chart evidence into an empty private destination; leave source intact.

    The destination is published only when the staged import is complete. A
    failed import removes its private staging directory and does not touch the
    original archive or an existing empty destination.
    """
    source_root = source_root.expanduser().resolve(strict=True)
    source_data = source_root / "data"
    if source_data.is_symlink() or not source_data.is_dir():
        raise ValueError(f"Expected a real data directory under {source_root}")
    source_data = source_data.resolve(strict=True)
    requested_destination = destination_root.expanduser()
    if requested_destination.is_symlink():
        raise ValueError(f"Migration destination must not be a symlink: {requested_destination}")
    destination_root = requested_destination.resolve()
    if destination_root == source_root or destination_root.is_relative_to(source_root):
        raise ValueError("Destination must not be inside the source repository")
    if destination_root.exists() and (
            not destination_root.is_dir() or any(destination_root.iterdir())
    ):
        raise ValueError(f"Migration destination must be an empty directory: {destination_root}")

    archive_db = source_data / "archive.sqlite3"
    orders_db = source_data / "orders.sqlite3"
    for database in (archive_db, orders_db):
        if database.is_symlink() or not database.is_file():
            raise ValueError(f"Missing or unsafe source database: {database}")
    # Fail early on obviously invalid source references, then read the backed-up
    # databases again: a concurrent old collector might add a run between the
    # preflight and SQLite backup.
    _references(archive_db, orders_db, source_root, source_data)
    candle_files = _cache_files(source_data)

    destination_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".{destination_root.name}.migration-", dir=destination_root.parent
    ))
    try:
        staging.chmod(0o700)
        stage_data = staging / "data"
        _private_directory(stage_data, staging)
        _backup(archive_db, stage_data / "archive.sqlite3")
        _backup(orders_db, stage_data / "orders.sqlite3")
        collection, imports = _references(
            stage_data / "archive.sqlite3", stage_data / "orders.sqlite3", source_root, source_data
        )
        raw_files = sorted({relative for _, relative in collection} | {
            relative for _, relative, _ in imports
        })
        for relative in raw_files:
            target = stage_data / relative
            _private_directory(target.parent, staging)
            _private_raw_copy(source_data / relative, target)
        for original, relative in candle_files:
            target = stage_data / relative
            _private_directory(target.parent, staging)
            _private_copy(original, target)
        final_data = destination_root / "data"
        _rebase_archive(stage_data / "archive.sqlite3", collection, final_data)
        _rebase_orders(stage_data / "orders.sqlite3", imports, final_data)
        for path in staging.rglob("*"):
            path.chmod(0o700 if path.is_dir() else 0o600)
        os.replace(staging, destination_root)
    except Exception:
        shutil.rmtree(staging)
        raise
    return MigrationResult(
        source_root=source_root,
        destination_root=destination_root,
        raw_files=len(raw_files),
        candle_files=len(candle_files),
    )
