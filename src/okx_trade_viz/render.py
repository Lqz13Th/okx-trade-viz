"""Refresh OKX account data and render one dark square chart per recent position.

Selection is by an opening or closing fill in the trailing archive window.
Public candles are fetched only when a validated local cache is missing.
Use --offline to render a historical local snapshot without any API calls.
No exchange write endpoints are used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import stat
from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import chart, sync
from .candles import BAR_MS, bar_floor, load_or_fetch_candles
from .fetch import TimeWindow, fetch_fills
from .migrate import migrate_data
from .storage import private_directory, write_private_text
from .trades import (
    _coverage_gaps,
    load_timeline,
    verified_raw_order_coverage,
)


DEFAULT_DATA_DIR = Path.home() / ".local" / "share" / "okx-trade-viz"
DEFAULT_ENV_FILE = Path.home() / ".config" / "okx-trade-viz" / ".env"
TZ = ZoneInfo("Asia/Shanghai")
DAY_MS = 86_400_000
MAX_SNAPSHOT_AGE_MS = 10 * 60_000
POSITION_TOLERANCE = Decimal("0.00000001")
MAX_VIEW_CANDLES = 720
BAR_ORDER = ("5m", "15m", "1H", "4H", "1D")
EXPLICIT_VIEWS = {
    "5m": ("5m-6h", 72),
    "15m": ("15m-24h", 96),
    "1H": ("1h-4d", 96),
    "4H": ("4h-16d", 96),
    "1D": ("1d-30d", 30),
}
STRUCTURE_VIEWS = (
    ("15m-6h", "15m", 24),
    ("15m-12h", "15m", 48),
    ("15m-24h", "15m", 96),
    ("15m-48h", "15m", 192),
    ("1h-48h", "1H", 48),
    ("1h-4d", "1H", 96),
    ("4h-16d", "4H", 96),
    ("4h-28d", "4H", 168),
)


class ViewLengthError(ValueError):
    """The requested bar would need too many complete candles to show through now."""


def _refresh_account(root: Path, env_file: Path, days: int, now: datetime) -> Path:
    return sync.refresh_account(root, env_file, days, now)


def _live_snapshot(raw_path: Path, archive_end_ms: int) -> dict:
    """Load only the just-fetched position snapshot, never an older raw file."""
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    if raw.get("metadata", {}).get("source_kind") != "okx_api":
        raise ValueError("Position snapshot is not from the OKX API")
    positions = raw.get("positions")
    if not isinstance(positions, list):
        raise ValueError("The refreshed account snapshot has no positions list")
    asof = raw.get("snapshot_times", {}).get("positions_as_of")
    if not isinstance(asof, str):
        raise ValueError("The refreshed positions have no as-of timestamp")
    instant = datetime.fromisoformat(asof)
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("The positions as-of timestamp has no UTC offset")
    asof_ms = round(instant.timestamp() * 1000)
    age_ms = round(datetime.now(UTC).timestamp() * 1000) - asof_ms
    if age_ms < -60_000 or age_ms > MAX_SNAPSHOT_AGE_MS:
        raise ValueError("The refreshed position snapshot is too old to label current")
    if asof_ms < archive_end_ms or asof_ms - archive_end_ms > MAX_SNAPSHOT_AGE_MS:
        raise ValueError("Fill archive and position snapshot have incompatible as-of times")
    return {
        "positions": positions,
        "positions_as_of": asof,
        "positions_as_of_ms": asof_ms,
        "raw_path": str(raw_path),
        "raw_sha256": _sha256(raw_path),
    }


def _verify_fill_gap(env_file: Path, archive_end_ms: int, snapshot: dict) -> dict:
    """Prove no trade changed the account between fill and position snapshots."""
    asof_ms = int(snapshot["positions_as_of_ms"])
    if asof_ms < archive_end_ms:
        raise ValueError("Position snapshot predates the fill archive")
    window = TimeWindow(
        start=datetime.fromtimestamp(archive_end_ms / 1000, UTC),
        end=datetime.fromtimestamp((asof_ms + 1) / 1000, UTC),
        timezone="UTC",
    )
    fills, quality = fetch_fills(sync.client_from_env(env_file), window)
    if not quality.get("complete_for_window"):
        raise ValueError("Could not verify fills between archive and position snapshots")
    if fills:
        raise ValueError(
            f"{len(fills)} fill(s) occurred between archive and position snapshots; "
            "rerun to reconcile the live position"
        )
    return {
        "start": _stamp(archive_end_ms),
        "end": snapshot["positions_as_of"],
        "fills": 0,
        "api_complete": True,
    }


def _quantity(value: object) -> Decimal:
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid position quantity: {value!r}") from exc
    if not quantity.is_finite():
        raise ValueError("Position quantity must be finite")
    return quantity


def _reconcile_position_snapshot(
    instrument: str, trades: list[dict], positions: list[dict]
) -> dict:
    """Compare account position by side; no per-parent live position is inferred."""
    inferred = {"long": Decimal(0), "short": Decimal(0)}
    for trade in trades:
        remaining = _quantity(trade.get("remaining_size"))
        if remaining < 0 or trade["side"] not in inferred:
            raise ValueError(f"Invalid reconstructed position for {instrument}")
        inferred[trade["side"]] += remaining
    live = {"long": Decimal(0), "short": Decimal(0)}
    for row in positions:
        if row.get("instId") != instrument:
            continue
        amount = _quantity(row.get("pos"))
        side = row.get("posSide") or "net"
        if side == "net":
            live["long" if amount >= 0 else "short"] += abs(amount)
        elif side in live:
            live[side] += abs(amount)
        else:
            raise ValueError(f"Unknown OKX position side for {instrument}: {side!r}")
    for side in ("long", "short"):
        if abs(inferred[side] - live[side]) > POSITION_TOLERANCE:
            raise ValueError(
                f"Position mismatch for {instrument} {side}: "
                f"fills={inferred[side]}, OKX snapshot={live[side]}"
            )
    return {
        "instrument": instrument,
        "long_contracts": str(live["long"]),
        "short_contracts": str(live["short"]),
        "matched": True,
    }


def _same_quote_amount(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= max(Decimal("0.0001"), abs(right) * Decimal("0.000001"))


def _set_total_r(instrument: str, trades: list[dict], snapshot: dict | None) -> None:
    for trade in trades:
        trade["total_r"] = trade.get("net_r") if trade["status"] == "closed" else None
        trade["total_r_note"] = (
            "final_verified_net" if trade["total_r"] is not None
            else "final_net_unavailable" if trade["status"] == "closed"
            else "live_snapshot_unavailable"
        )
        trade["total_r_asof"] = None
        trade["total_pnl_quote"] = trade.get("net_pnl_quote") if trade["status"] == "closed" else None
        trade["unrealized_pnl_quote"] = None

    if snapshot is None:
        return
    active = [trade for trade in trades if _quantity(trade["remaining_size"]) > 0]
    if len(active) != 1:
        for trade in active:
            trade["total_r_note"] = "multiple_open_parents"
        return
    trade = active[0]
    rows = [
        row for row in snapshot["positions"]
        if row.get("instId") == instrument and _quantity(row.get("pos")) != 0
    ]
    if len(rows) != 1:
        trade["total_r_note"] = "live_position_not_unique"
        return
    row = rows[0]
    try:
        position = _quantity(row.get("pos"))
        remaining = _quantity(trade["remaining_size"])
        entry = _quantity(trade["entry_price"])
        stop = _quantity(trade["sl_price"])
        initial_size = _quantity(trade["size"])
        risk = _quantity(trade.get("initial_risk_quote"))
        archived_realized = _quantity(trade.get("realized_net_quote"))
        realized = _quantity(row.get("realizedPnl"))
        unrealized = _quantity(row.get("upl"))
        mark = _quantity(row.get("markPx"))
        average = _quantity(row.get("avgPx"))
        gross = _quantity(row.get("pnl"))
        position_created = _quantity(row.get("cTime"))
    except ValueError:
        trade["total_r_note"] = "missing_or_invalid_position_evidence"
        return
    quote = instrument.split("-")[1] if "-" in instrument else ""
    pos_side = row.get("posSide") or "net"
    matching_side = (
        (pos_side == "net" and ((position > 0) == (trade["side"] == "long")))
        or pos_side == trade["side"]
    )
    if (
        quote not in {"USDT", "USDC"}
        or row.get("ccy") != quote
        or not matching_side
        or abs(abs(position) - remaining) > POSITION_TOLERANCE
        or risk <= 0
        or entry == stop
        or initial_size <= 0
        or not trade.get("entry_fills")
    ):
        trade["total_r_note"] = "position_identity_or_risk_mismatch"
        return
    first_fill_ms = min(int(fill["time_ms"]) for fill in trade["entry_fills"])
    if (
        abs(position_created - first_fill_ms) > 1_000
        or abs(average - entry) > max(Decimal("0.00000001"), abs(entry) * Decimal("0.000001"))
        or not _same_quote_amount(realized, archived_realized)
    ):
        trade["total_r_note"] = "position_lifecycle_or_realized_pnl_mismatch"
        return
    multiplier = risk / (abs(entry - stop) * initial_size)
    direction = Decimal(1) if trade["side"] == "long" else Decimal(-1)
    expected_gross = sum(
        (
            (_quantity(fill["price"]) - entry)
            * direction
            * _quantity(fill["size"])
            * multiplier
            for fill in trade["exit_fills"]
        ),
        Decimal(0),
    )
    expected_unrealized = (mark - entry) * direction * remaining * multiplier
    if not _same_quote_amount(gross, expected_gross) or not _same_quote_amount(
        unrealized, expected_unrealized
    ):
        trade["total_r_note"] = "position_gross_or_unrealized_pnl_mismatch"
        return
    trade["total_pnl_quote"] = float(realized + unrealized)
    trade["unrealized_pnl_quote"] = float(unrealized)
    trade["total_r"] = float((realized + unrealized) / risk)
    trade["total_r_asof"] = snapshot["positions_as_of"]
    trade["total_r_note"] = "live_position_mark_and_verified_realized_bills"


def _has_recent_activity(trade: dict, start_ms: int, end_ms: int) -> bool:
    return (
        start_ms <= int(trade["entry_ms"]) < end_ms
        or (
            trade.get("exit_ms") is not None
            and start_ms <= int(trade["exit_ms"]) < end_ms
        )
        or any(start_ms <= int(fill["time_ms"]) < end_ms for fill in trade["exit_fills"])
    )


def _view_window(
    trade: dict, bar: str, pre_bars: int, asof_ms: int
) -> tuple[int, int, int, int]:
    bar_ms = BAR_MS[bar]
    entry_ms = int(trade["entry_ms"])
    event_end = (
        int(trade["exit_ms"])
        if trade.get("status") == "closed" and trade.get("exit_ms") is not None
        else asof_ms
    )
    if event_end < entry_ms or event_end > asof_ms:
        raise ValueError("Trade event time is outside its archive window")
    start_ms = bar_floor(bar, entry_ms) - pre_bars * bar_ms
    # A closed position stops its shaded exposure at exit, but market candles
    # continue through the most recent complete bar at the archive snapshot.
    fetch_asof_ms = bar_floor(bar, asof_ms)
    count = (fetch_asof_ms - start_ms) // bar_ms
    if count < pre_bars or count > MAX_VIEW_CANDLES:
        raise ViewLengthError(
            f"{count} complete {bar} candles (maximum {MAX_VIEW_CANDLES})"
        )
    plot_end_ms = asof_ms + max(bar_ms, round((asof_ms - start_ms) * 0.12))
    return start_ms, plot_end_ms, count, fetch_asof_ms


def _metric_label(trade: dict) -> str:
    return chart._single_trade_metric(trade)


def _archive_window(fills_db: Path, days: int) -> tuple[int, int, int, list[str]]:
    with sqlite3.connect(f"{fills_db.resolve().as_uri()}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        end_ms = db.execute(
            "SELECT MAX(end_ms) FROM coverage WHERE entity = 'fills'"
        ).fetchone()[0]
        first_fill_ms = db.execute(
            "SELECT MIN(timestamp_ms) FROM fills WHERE active = 1"
        ).fetchone()[0]
        if end_ms is None or first_fill_ms is None:
            raise ValueError("The local fill archive is empty")
        start_ms = int(end_ms) - days * DAY_MS
        coverage = [
            (int(row[0]), int(row[1]))
            for row in db.execute(
                "SELECT start_ms, end_ms FROM coverage WHERE entity = 'fills'"
            )
        ]
        if _coverage_gaps(coverage, start_ms, int(end_ms)):
            raise ValueError("The requested window has gaps in the fill archive")
        instruments = [
            str(row[0])
            for row in db.execute(
                "SELECT DISTINCT json_extract(payload_json, '$.instId') FROM fills "
                "WHERE active = 1 AND timestamp_ms >= ? AND timestamp_ms < ? "
                "ORDER BY 1",
                (start_ms, int(end_ms)),
            )
            if row[0]
        ]
    return start_ms, int(end_ms), int(first_fill_ms), instruments


def _views(trade: dict, asof_ms: int) -> list[tuple[str, str, int]]:
    holding_ms = (
        int(trade["exit_ms"])
        if trade.get("status") == "closed" and trade.get("exit_ms") is not None
        else asof_ms
    ) - int(trade["entry_ms"])
    setup = ("setup-5m-6h", "5m", 72) if holding_ms < 2 * 3_600_000 else (
        "setup-15m-24h", "15m", 96
    )
    context = ("context-4h-16d", "4H", 96) if holding_ms >= 4 * DAY_MS else (
        "context-1h-4d", "1H", 96
    )
    if holding_ms >= 6 * DAY_MS:
        setup = ("setup-1h-48h", "1H", 48)
    fitted = []
    for name, bar, pre_bars in (setup, context):
        pre_ms = pre_bars * BAR_MS[bar]
        for candidate in BAR_ORDER[BAR_ORDER.index(bar):]:
            candidate_ms = BAR_MS[candidate]
            candidate_pre_bars = (pre_ms + candidate_ms - 1) // candidate_ms
            start_ms = bar_floor(candidate, int(trade["entry_ms"])) - (
                candidate_pre_bars * candidate_ms
            )
            count = (bar_floor(candidate, asof_ms) - start_ms) // candidate_ms
            if count <= MAX_VIEW_CANDLES:
                prefix, _, pre_label = name.rpartition("-")
                view_slug = (
                    name if candidate == bar
                    else f"{prefix.rsplit('-', 1)[0]}-{candidate.lower()}-{pre_label}"
                )
                fitted.append((view_slug, candidate, candidate_pre_bars))
                break
        else:
            raise ValueError(f"No legible candle interval can cover {name} through now")
    return fitted


def _explicit_views(bars: list[str]) -> list[tuple[str, str, int]]:
    """Keep each requested native OKX interval, without silently coarsening it."""
    return [
        (EXPLICIT_VIEWS[bar][0], bar, EXPLICIT_VIEWS[bar][1])
        for bar in dict.fromkeys(bars)
    ]


def _stamp(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).astimezone(TZ).strftime(
        "%Y-%m-%d %H:%M:%S %z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _private_output_directory(path: Path) -> Path:
    if path.exists() and stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ValueError(f"Output directory is not private (mode must be 700): {path}")
    return private_directory(path)


def _load_candles(
    instrument: str,
    bar: str,
    count: int,
    fetch_asof_ms: int,
    cache: Path,
    *,
    offline: bool,
    relay: str | None,
) -> list[dict]:
    try:
        return load_or_fetch_candles(instrument, bar, count, fetch_asof_ms, cache)
    except RuntimeError as exc:
        if offline or "No sufficient validated local candle cache" not in str(exc):
            raise
        return load_or_fetch_candles(
            instrument, bar, count, fetch_asof_ms, cache, refresh=True, relay=relay
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="okx-trade-viz",
        description="Read-only OKX trade charts with verified whole-position R",
    )
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument(
        "--data-dir", type=Path, default=DEFAULT_DATA_DIR,
        help=f"private archive root (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--env-file", type=Path, default=DEFAULT_ENV_FILE,
        help=f"chmod 600 OKX credentials (default: {DEFAULT_ENV_FILE})",
    )
    parser.add_argument(
        "--import-from", type=Path, metavar="SOURCE_DIR",
        help="one-time copy of an existing okx-analysis archive into an empty --data-dir",
    )
    parser.add_argument("--output-dir", type=Path)
    views = parser.add_mutually_exclusive_group()
    views.add_argument(
        "--bars", nargs="+", choices=BAR_ORDER,
        help="render one image per requested OKX interval; default is eight structure views",
    )
    views.add_argument(
        "--structure", action="store_true",
        help="render the default eight pre-entry structure views (15m/1H/4H)",
    )
    parser.add_argument(
        "--instrument", action="append", metavar="INST_ID",
        help="Only render this recently traded instrument (repeatable)",
    )
    parser.add_argument(
        "--offline", action="store_true",
        help="Use local account archives and candle caches; results are not current",
    )
    parser.add_argument("--relay", help="Read-only SSH relay for public OKX candles")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error("--days must be positive")
    root = args.data_dir.expanduser().resolve()
    if args.import_from is not None:
        imported = migrate_data(args.import_from, root)
        print(
            f"IMPORTED: {imported.raw_files} raw snapshots, "
            f"{imported.candle_files} candle caches -> {imported.destination_root}",
            flush=True,
        )
    raw_path = None
    if not args.offline:
        env_file = args.env_file.expanduser().resolve()
        raw_path = _refresh_account(root, env_file, args.days, datetime.now(UTC))
    fills_db = root / "data" / "archive.sqlite3"
    orders_db = root / "data" / "orders.sqlite3"
    start_ms, end_ms, first_fill_ms, instruments = _archive_window(fills_db, args.days)
    if verified_raw_order_coverage(orders_db, start_ms, end_ms)["gaps"]:
        raise ValueError("The requested window has gaps in verified account-wide order scans")
    if not instruments:
        raise ValueError("No fill activity in the requested archive window")
    requested_instruments = {inst.upper() for inst in args.instrument or []}
    unknown_instruments = requested_instruments.difference(instruments)
    if unknown_instruments:
        parser.error(
            "No recent fill activity for: " + ", ".join(sorted(unknown_instruments))
        )
    snapshot = _live_snapshot(raw_path, end_ms) if raw_path is not None else None
    fill_gap_check = (
        _verify_fill_gap(env_file, end_ms, snapshot) if snapshot is not None else None
    )
    prepared = []
    reconciliations = []
    for instrument in instruments:
        recent = load_timeline(orders_db, fills_db, instrument, start_ms, end_ms)
        if not recent["quality"]["reconstruction_complete"]:
            raise ValueError(f"Incomplete recent trade reconstruction for {instrument}")
        history = load_timeline(orders_db, fills_db, instrument, first_fill_ms, end_ms)
        if snapshot is not None:
            reconciliations.append(
                _reconcile_position_snapshot(instrument, history["trades"], snapshot["positions"])
            )
        _set_total_r(instrument, history["trades"], snapshot)
        trades = [
            trade for trade in history["trades"] if _has_recent_activity(trade, start_ms, end_ms)
        ]
        if not requested_instruments or instrument in requested_instruments:
            prepared.append((instrument, trades))
    end_key = datetime.fromtimestamp(end_ms / 1000, UTC).astimezone(TZ).strftime(
        "%Y%m%dT%H%M%S%z"
    )
    output_dir = (
        args.output_dir or root / "reports" / "trade_rr" / f"recent-{args.days}d-{end_key}"
    ).resolve()
    _private_output_directory(output_dir)
    manifest: dict = {
        "selection": "opening or closing fill in trailing archive window; end exclusive",
        "view_selection": "bars" if args.bars else "structure",
        "requested_bars": list(dict.fromkeys(args.bars)) if args.bars else None,
        "requested_instruments": sorted(requested_instruments) if requested_instruments else None,
        "window_start": _stamp(start_ms),
        "archive_asof": _stamp(end_ms),
        "account_sources": {
            "fills_db": str(fills_db),
            "fills_db_sha256": _sha256(fills_db),
            "orders_db": str(orders_db),
            "orders_db_sha256": _sha256(orders_db),
            "mode": "live_refreshed" if snapshot is not None else "offline_archive_only",
            "position_snapshot": snapshot["raw_path"] if snapshot is not None else None,
            "position_snapshot_sha256": snapshot["raw_sha256"] if snapshot is not None else None,
            "positions_as_of": snapshot["positions_as_of"] if snapshot is not None else None,
            "fill_gap_check": fill_gap_check,
            "position_reconciliation": reconciliations,
        },
        "interpretation": {
            "bands": (
                "Recorded TP/SL price-distance references share time boundaries; opacity "
                "falls with remaining size after each exit and nested targets darken toward entry. "
                "Shading stops at the final exit but continues for an open runner. "
                "Bands do not verify that child orders stayed active or unchanged."
            ),
            "markers": "Exit diamonds mark actual fills grouped by order and price",
            "guides": "Full-width dashed levels are price references, not live order durations",
            "r": (
                "All labels use original planned quote risk. Closed trades show final verified "
                "net R. A sole live parent shows (verified realized net plus OKX snapshot "
                "unrealized PnL) / original risk at positions_as_of; future exit costs are "
                "excluded. Missing or ambiguous evidence gives R UNAVAILABLE."
            ),
            "candles": (
                "completed public OKX candles through archive_asof, including after a full "
                "exit; candle timestamps are placed at their close time"
            ),
        },
        "trades": [],
    }
    index = [
        f"# Recent {args.days}-day trades",
        "",
        f"Window: {_stamp(start_ms)} to {_stamp(end_ms)} (end exclusive).",
        (
            f"OKX positions verified at {snapshot['positions_as_of']}."
            if snapshot is not None else "Offline archive only; current positions were not checked."
        ),
        (
            "One position per image. Paired bands show recorded TP/SL price distances; "
            "their shared opacity fades on partial exits and stops after a full exit. "
            "Diamonds mark actual exits. "
            "Order lifetimes are not verified live state, and later-observed plans "
            "are price references rather than proof of a live order at entry."
        ),
        "Open-position R is marked to the verified position snapshot, not the last plotted candle.",
        "Structure views show four 15m, two 1H and two 4H pre-entry windows.",
        "Each chart stops at its last complete candle; OKX 1D closes at Beijing midnight.",
        "",
    ]
    for instrument, trades in prepared:
        for trade in trades:
            local_entry = datetime.fromtimestamp(trade["entry_ms"] / 1000, UTC).astimezone(TZ)
            slug = f"{local_entry:%Y%m%dT%H%M%S}-{trade['order_id']}"
            trade_dir = output_dir / instrument.lower() / slug
            private_directory(trade_dir)
            item = {
                "instrument": instrument,
                "order_id": trade["order_id"],
                "entry_time": _stamp(trade["entry_ms"]),
                "exit_time": _stamp(trade["exit_ms"]) if trade["exit_ms"] else None,
                "side": trade["side"],
                "status": trade["status"],
                "metric": _metric_label(trade),
                "size": trade["size"],
                "remaining_size": trade["remaining_size"],
                "total_r": trade["total_r"],
                "total_r_note": trade["total_r_note"],
                "total_r_asof": trade["total_r_asof"],
                "total_pnl_quote": trade["total_pnl_quote"],
                "realized_net_quote": trade.get("realized_net_quote"),
                "unrealized_pnl_quote": trade["unrealized_pnl_quote"],
                "realized_price_r": trade["realized_price_r"],
                "net_r": trade["net_r"],
                "plan_provenance": trade["plan_provenance"],
                "entry_price": trade["entry_price"],
                "tp_price": trade["tp_price"],
                "recorded_sl_reference": trade["sl_price"],
                "plan_first_observed_ms": trade["plan_first_observed_ms"],
                "tp_orders": trade["tp_orders"],
                "sl_orders": trade["sl_orders"],
                "entry_fills": trade["entry_fills"],
                "exit_fills": trade["exit_fills"],
                "views": [],
                "skipped_views": [],
            }
            view_specs = _explicit_views(args.bars) if args.bars else STRUCTURE_VIEWS
            for view_slug, bar, pre_bars in view_specs:
                try:
                    plot_start, plot_end, count, fetch_asof = _view_window(
                        trade, bar, pre_bars, end_ms
                    )
                except ViewLengthError as exc:
                    item["skipped_views"].append({
                        "name": view_slug, "bar": bar, "reason": str(exc),
                    })
                    print(f"SKIPPED {instrument} {slug} {bar}: {exc}", flush=True)
                    continue
                cache = (
                    root / "data" / "market_candles" / "recent_trade_rr" / slug
                    / f"{view_slug}.json"
                )
                candles = _load_candles(
                    instrument, bar, count, fetch_asof, cache,
                    offline=args.offline, relay=args.relay,
                )
                image_path = trade_dir / f"{view_slug}.png"
                chart.render_chart(
                    instrument, bar, plot_start, plot_end, candles, [trade], image_path,
                    aspect="1:1", candle_anchor="close", single_trade=True,
                    view_label=f"PRE {view_slug.rsplit('-', 1)[-1].upper()}",
                    evidence_end_ms=(
                        int(trade["exit_ms"])
                        if trade["status"] == "closed" else end_ms
                    ),
                )
                image_path.chmod(0o600)
                item["views"].append({
                    "name": view_slug,
                    "bar": bar,
                    "pre_bars": pre_bars,
                    "candle_count": len(candles),
                    "first_candle_open": _stamp(candles[0]["ts"]),
                    "last_complete_close": _stamp(candles[-1]["ts"] + BAR_MS[bar]),
                    "cache": str(cache),
                    "cache_sha256": _sha256(cache),
                    "image": str(image_path),
                    "image_sha256": _sha256(image_path),
                })
                print(image_path, flush=True)
            manifest["trades"].append(item)
            index.extend([
                f"## {instrument} | {local_entry:%m-%d %H:%M} | "
                f"{trade['side'].upper()} | {item['metric']}",
                "",
                f"Order `{trade['order_id']}` | entry `{trade['entry_price']}` | "
                f"recorded TP `{trade['tp_price']}` | SL reference `{trade['sl_price']}`",
                "",
                *(
                    f"![{view['name']}]({Path(view['image']).relative_to(output_dir).as_posix()})"
                    for view in item["views"]
                ),
                *(
                    f"Skipped {view['bar']}: {view['reason']}"
                    for view in item["skipped_views"]
                ),
                "",
            ])
    manifest_path = output_dir / "manifest.json"
    write_private_text(manifest_path, json.dumps(manifest, indent=2) + "\n")
    write_private_text(output_dir / "index.md", "\n".join(index) + "\n")
    print(f"TRADES: {len(manifest['trades'])}")
    print(f"INDEX: {output_dir / 'index.md'}")
    print(f"MANIFEST: {manifest_path}")


if __name__ == "__main__":
    main()
