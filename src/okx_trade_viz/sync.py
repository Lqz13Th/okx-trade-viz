"""Incrementally collect the read-only OKX evidence needed by the charts."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import order_archive_cli
from .archive import archive_bundle, common_curve_coverage, default_archive_path
from .client import ReadOnlyOKXClient
from .config import Settings
from .fetch import TimeWindow, collect_account_data
from .order_archive import OrderArchive, discover_raw_identity
from .storage import private_directory, write_private_json
from .trades import _coverage_gaps, verified_raw_order_coverage


TZ = ZoneInfo("Asia/Shanghai")
DAY_MS = 86_400_000
OVERLAP_MS = 6 * 60 * 60_000


def client_from_env(env_file: Path) -> ReadOnlyOKXClient:
    return ReadOnlyOKXClient(Settings.from_env_file(env_file))


def _first_fill_gap(path: Path, start_ms: int, end_ms: int) -> int | None:
    if not path.is_file():
        return start_ms
    gaps = _coverage_gaps(common_curve_coverage(path), start_ms, end_ms)
    return gaps[0]["start_ms"] if gaps else None


def _first_order_gap(path: Path, start_ms: int, end_ms: int) -> int | None:
    if not path.is_file():
        return start_ms
    gaps = verified_raw_order_coverage(path, start_ms, end_ms)["gaps"]
    return gaps[0]["start_ms"] if gaps else None


def refresh_account(root: Path, env_file: Path, days: int, now: datetime) -> Path:
    """Refresh account and order archives, returning the new position snapshot."""
    now_ms = round(now.timestamp() * 1000)
    start_ms = now_ms - days * DAY_MS
    candidates = [max(0, now_ms - OVERLAP_MS)]
    for gap in (
        _first_fill_gap(root / "data" / "archive.sqlite3", start_ms, now_ms),
        _first_order_gap(root / "data" / "orders.sqlite3", start_ms, now_ms),
    ):
        if gap is not None:
            candidates.append(gap)
    sync_start = min(candidates)
    if sync_start < now_ms - 89 * DAY_MS:
        raise ValueError("Account archive gap exceeds OKX's safe 89-day recovery window")

    start = datetime.fromtimestamp(sync_start / 1000, UTC).astimezone(TZ)
    end = now.astimezone(TZ)
    window = TimeWindow(start=start, end=end, timezone=str(TZ))
    client = client_from_env(env_file)
    bundle = collect_account_data(client, window)
    run_id = start.strftime("%Y%m%dT%H%M%S%z") + "_to_" + end.strftime("%Y%m%dT%H%M%S%z")
    raw_path = private_directory(root / "data" / run_id) / "raw.json"
    write_private_json(raw_path, bundle)
    archive_update = archive_bundle(default_archive_path(root), bundle, raw_path=raw_path)
    print(f"WINDOW: {window.start.isoformat()} -> {window.end.isoformat()}", flush=True)
    print(f"FILLS: {len(bundle['fills'])}", flush=True)
    print(f"ORDERS: {len(bundle['orders'])}", flush=True)
    print(f"ALGO_ORDERS: {len(bundle['algo_orders'])}", flush=True)
    print(f"BILLS: {len(bundle['bills'])}", flush=True)
    print(f"RAW_DATA: {raw_path}", flush=True)
    print(f"CURVE_ARCHIVE: {archive_update.path}", flush=True)
    print(f"CURVE_ARCHIVE_RUN: {archive_update.run_id}", flush=True)

    order_db = root / "data" / "orders.sqlite3"
    fingerprint, environment = discover_raw_identity([raw_path])
    imported = OrderArchive(
        order_db, account_fingerprint=fingerprint, environment=environment
    ).import_raw_snapshot(raw_path)
    print(f"ORDER_ARCHIVE_IMPORTED: {imported}", flush=True)
    if (
        order_archive_cli.main(["once", "--env-file", str(env_file), "--database", str(order_db)])
        != 0
    ):
        raise RuntimeError("Current order scan failed; no images were rendered")
    return raw_path
