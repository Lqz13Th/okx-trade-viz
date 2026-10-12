"""One-time import must preserve proof, privacy, and original archives."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
import unittest
from contextlib import closing, redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from okx_trade_viz import render
from okx_trade_viz.migrate import migrate_data


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(base: Path) -> tuple[Path, Path, Path, Path]:
    source = base / "old"
    data = source / "data"
    data.mkdir(parents=True)
    first = data / "run-a" / "raw.json"
    second = data / "run-b" / "raw.json"
    unrelated = data / "unreferenced" / "raw.json"
    for path in (first, second, unrelated):
        path.parent.mkdir()
        path.write_text(
            json.dumps(
                {
                    "metadata": {"account_fingerprint": "account", "environment": "LIVE"},
                    "quality": {"orders": {"api_complete": True}},
                    "fills": [{"fillId": "123"}],
                    "orders": [{"ordId": "abc"}],
                    "positions": [{"instId": "BTC-USDT-SWAP"}],
                    "snapshot_times": {"positions_as_of": "2026-10-12T00:00:00+00:00"},
                    "balances": [{"secret": "omit-me"}],
                    "positions_history": [{"secret": "omit-me"}],
                    "subtypes": {"secret": "omit-me"},
                }
            ),
            encoding="utf-8",
        )
    archive = data / "archive.sqlite3"
    with closing(sqlite3.connect(archive)) as db, db:
        db.execute("CREATE TABLE collection_runs (id INTEGER PRIMARY KEY, raw_path TEXT)")
        db.execute("CREATE TABLE marker (value TEXT)")
        db.execute("INSERT INTO collection_runs VALUES (1, ?)", (str(first),))
        db.execute("INSERT INTO marker VALUES ('fills-preserved')")
    orders = data / "orders.sqlite3"
    with closing(sqlite3.connect(orders)) as db, db:
        db.execute("CREATE TABLE sync_runs (id INTEGER PRIMARY KEY, kind TEXT, details_json TEXT)")
        db.execute("CREATE TABLE marker (value TEXT)")
        db.execute(
            "INSERT INTO sync_runs VALUES (1, 'raw_import', ?)",
            (json.dumps({"raw_path": str(second), "source": "historical"}),),
        )
        db.execute(
            "INSERT INTO sync_runs VALUES (2, 'ordinary_scan', ?)",
            (json.dumps({"note": "unchanged"}),),
        )
        db.execute("INSERT INTO marker VALUES ('orders-preserved')")
    candle = data / "market_candles" / "BTC" / "15m.json"
    candle.parent.mkdir(parents=True)
    candle.write_text('{"bars":[]}', encoding="utf-8")
    return source, first, second, candle


class MigrationTests(unittest.TestCase):
    def test_cli_import_from_runs_before_offline_render_without_network(self) -> None:
        class AfterImport(Exception):
            pass

        with TemporaryDirectory() as directory:
            base = Path(directory)
            source, *_ = _fixture(base)
            destination = base / "new"
            output = StringIO()
            with (
                patch.object(render, "_refresh_account", side_effect=AssertionError("network")),
                patch.object(render, "_archive_window", side_effect=AfterImport),
                redirect_stdout(output),
                self.assertRaises(AfterImport),
            ):
                render.main(
                    [
                        "--import-from",
                        str(source),
                        "--offline",
                        "--data-dir",
                        str(destination),
                        "--days",
                        "3",
                    ]
                )
            self.assertIn("IMPORTED: 2 raw snapshots, 1 candle caches", output.getvalue())
            self.assertTrue((destination / "data" / "archive.sqlite3").is_file())
            self.assertTrue((destination / "data" / "orders.sqlite3").is_file())

    def test_copies_only_referenced_raws_and_rebases_both_databases(self) -> None:
        with TemporaryDirectory() as directory:
            base = Path(directory)
            source, first, second, candle = _fixture(base)
            original_hashes = {
                path: _sha256(path)
                for path in (
                    first,
                    second,
                    candle,
                    source / "data" / "archive.sqlite3",
                    source / "data" / "orders.sqlite3",
                )
            }
            destination = base / "new"
            destination.mkdir()
            result = migrate_data(source, destination)
            self.assertEqual(result.raw_files, 2)
            self.assertEqual(result.candle_files, 1)
            self.assertEqual(result.destination_root, destination.resolve())
            data = destination / "data"
            migrated_first = data / "run-a" / "raw.json"
            migrated_second = data / "run-b" / "raw.json"
            self.assertTrue(migrated_first.is_file())
            self.assertTrue(migrated_second.is_file())
            self.assertFalse((data / "unreferenced" / "raw.json").exists())
            self.assertEqual(
                (data / "market_candles" / "BTC" / "15m.json").read_bytes(), candle.read_bytes()
            )
            migrated = json.loads(migrated_first.read_text(encoding="utf-8"))
            self.assertEqual(migrated["fills"], [{"fillId": "123"}])
            self.assertIn("quality", migrated)
            self.assertIn("positions", migrated)
            self.assertNotIn("balances", migrated)
            self.assertNotIn("positions_history", migrated)
            self.assertNotIn("subtypes", migrated)
            with closing(sqlite3.connect(data / "archive.sqlite3")) as db:
                self.assertEqual(
                    db.execute("SELECT raw_path FROM collection_runs WHERE id = 1").fetchone()[0],
                    str(migrated_first.resolve()),
                )
                self.assertEqual(
                    db.execute("SELECT value FROM marker").fetchone()[0], "fills-preserved"
                )
            with closing(sqlite3.connect(data / "orders.sqlite3")) as db:
                details = json.loads(
                    db.execute("SELECT details_json FROM sync_runs WHERE id = 1").fetchone()[0]
                )
                self.assertEqual(
                    details, {"raw_path": str(migrated_second.resolve()), "source": "historical"}
                )
                self.assertEqual(
                    json.loads(
                        db.execute("SELECT details_json FROM sync_runs WHERE id = 2").fetchone()[0]
                    ),
                    {"note": "unchanged"},
                )
                self.assertEqual(
                    db.execute("SELECT value FROM marker").fetchone()[0], "orders-preserved"
                )
            for path in (
                destination,
                data,
                migrated_first.parent,
                migrated_second.parent,
                data / "market_candles",
                data / "market_candles" / "BTC",
            ):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700)
            for path in (
                migrated_first,
                migrated_second,
                data / "archive.sqlite3",
                data / "orders.sqlite3",
                data / "market_candles" / "BTC" / "15m.json",
            ):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            for path, digest in original_hashes.items():
                self.assertEqual(_sha256(path), digest, f"Source changed: {path}")

    def test_rejects_nonempty_destination_without_touching_it(self) -> None:
        with TemporaryDirectory() as directory:
            base = Path(directory)
            source, *_ = _fixture(base)
            destination = base / "new"
            destination.mkdir()
            sentinel = destination / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "empty directory"):
                migrate_data(source, destination)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_rejects_raw_reference_outside_source_data_before_writing(self) -> None:
        with TemporaryDirectory() as directory:
            base = Path(directory)
            source, *_ = _fixture(base)
            outside = base / "outside" / "raw.json"
            outside.parent.mkdir()
            outside.write_text("{}", encoding="utf-8")
            with closing(sqlite3.connect(source / "data" / "archive.sqlite3")) as db, db:
                db.execute("UPDATE collection_runs SET raw_path = ? WHERE id = 1", (str(outside),))
            destination = base / "new"
            with self.assertRaisesRegex(ValueError, "outside"):
                migrate_data(source, destination)
            self.assertFalse(destination.exists())

    def test_rejects_candle_cache_symlink_before_writing(self) -> None:
        with TemporaryDirectory() as directory:
            base = Path(directory)
            source, *_ = _fixture(base)
            outside = base / "outside.txt"
            outside.write_text("private", encoding="utf-8")
            (source / "data" / "market_candles" / "leak.json").symlink_to(outside)
            destination = base / "new"
            with self.assertRaisesRegex(ValueError, "escapes source data"):
                migrate_data(source, destination)
            self.assertFalse(destination.exists())

    def test_sqlite_backup_includes_uncheckpointed_wal(self) -> None:
        with TemporaryDirectory() as directory:
            base = Path(directory)
            source, *_ = _fixture(base)
            archive = source / "data" / "archive.sqlite3"
            with closing(sqlite3.connect(archive)) as writer, writer:
                writer.execute("PRAGMA journal_mode=WAL")
                writer.execute("INSERT INTO marker VALUES ('wal-row')")
                writer.commit()
                destination = base / "new"
                migrate_data(source, destination)
            with closing(sqlite3.connect(destination / "data" / "archive.sqlite3")) as db:
                self.assertEqual(
                    [row[0] for row in db.execute("SELECT value FROM marker ORDER BY rowid")],
                    ["fills-preserved", "wal-row"],
                )


if __name__ == "__main__":
    unittest.main()
