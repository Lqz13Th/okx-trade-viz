"""Assemble one instrument's recorded order plans and actual fills.

FIFO allocation is an accounting reconstruction, not an exchange-provided
parent/exit link. Independent TP and Algo SL orders are associated only when
their exchange IDs link to an allocated exit or exactly one compatible position
was open. An association is not proof of the trader's original intent.
"""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict, deque
from contextlib import closing
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ZERO = Decimal(0)
STANDARD_ORDER_TYPES = frozenset({"SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION", "EVENTS"})


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _positive(value: Any) -> Decimal | None:
    parsed = _decimal(value)
    return parsed if parsed is not None and parsed > ZERO else None


def _integer(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _true(value: Any) -> bool:
    return value is True or str(value).lower() == "true"


def _open_readonly(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise FileNotFoundError(path)
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _coverage_gaps(
    coverage: list[tuple[int, int]], start_ms: int, end_ms: int
) -> list[dict[str, int]]:
    cursor = start_ms
    gaps: list[dict[str, int]] = []
    for left, right in sorted(coverage):
        if right <= cursor or left >= end_ms:
            continue
        if left > cursor:
            gaps.append({"start_ms": cursor, "end_ms": min(left, end_ms)})
        cursor = max(cursor, right)
        if cursor >= end_ms:
            break
    if cursor < end_ms:
        gaps.append({"start_ms": cursor, "end_ms": end_ms})
    return gaps


def _verified_raw_order_windows(
    connection: sqlite3.Connection, start_ms: int, end_ms: int
) -> list[tuple[int, int]]:
    """Find account-wide order-history windows proven by imported OKX raw snapshots.

    ``raw_import.complete`` only means the import succeeded, not that the OKX
    order scan was complete. Check the source snapshot, its account identity,
    observed time, and pagination quality before using its window as evidence.
    """
    tables = {
        str(row["name"])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name IN ('archive_meta', 'sync_runs')"
        )
    }
    if tables != {"archive_meta", "sync_runs"}:
        return []
    identity = {
        str(row["key"]): str(row["value"])
        for row in connection.execute(
            "SELECT key, value FROM archive_meta "
            "WHERE key IN ('account_fingerprint', 'environment')"
        )
    }
    fingerprint = identity.get("account_fingerprint")
    environment = identity.get("environment")
    if not fingerprint or environment not in {"LIVE", "DEMO"}:
        return []

    windows: set[tuple[int, int]] = set()
    for run in connection.execute(
        "SELECT observed_ms, order_count, details_json FROM sync_runs "
        "WHERE kind = 'raw_import' AND complete = 1 AND error_json IS NULL "
        "AND observed_ms IS NOT NULL"
    ):
        try:
            details = json.loads(run["details_json"])
            raw_path = Path(details["raw_path"])
            if not raw_path.is_absolute() or not raw_path.is_file():
                continue
            with raw_path.open(encoding="utf-8") as source:
                snapshot = json.load(source)
            metadata = snapshot["metadata"]
            quality = snapshot["quality"]
            if not isinstance(metadata, dict) or not isinstance(quality, dict):
                continue
            left = int(metadata["start_ms"])
            right = int(metadata["end_ms"])
            observed_at = datetime.fromisoformat(metadata["collected_at"])
            observed_ms = int(observed_at.timestamp() * 1000)
            orders_quality = quality["orders"]
            if not isinstance(orders_quality, dict):
                continue
            endpoint_pages = orders_quality["by_endpoint"]
            if not isinstance(endpoint_pages, dict):
                continue
            standard_orders = snapshot["orders"]
            detail_orders = snapshot.get("fill_order_details", [])
            algo_orders = snapshot.get("algo_orders", [])
            if not all(
                isinstance(rows, list) for rows in (standard_orders, detail_orders, algo_orders)
            ):
                continue
            imported_count = len(
                {
                    (str(row["instId"]), str(row["ordId"]))
                    for row in (*standard_orders, *detail_orders)
                }
            ) + len({(str(row["instId"]), str(row["algoId"])) for row in algo_orders})
            pages_complete = bool(endpoint_pages) and all(
                isinstance(endpoint, dict)
                and bool(endpoint)
                and STANDARD_ORDER_TYPES.issubset(endpoint)
                and all(
                    isinstance(page, dict) and page.get("complete") is True
                    for page in endpoint.values()
                )
                for endpoint in endpoint_pages.values()
            )
        except (OSError, ValueError, TypeError, KeyError):
            continue
        if (
            metadata.get("source_kind") != "okx_api"
            or metadata.get("account_fingerprint") != fingerprint
            or metadata.get("environment") != environment
            or "excluded_instruments" in metadata
            or observed_at.tzinfo is None
            or observed_ms != int(run["observed_ms"])
            or right > observed_ms
            or right <= left
            or right <= start_ms
            or left >= end_ms
            or quality.get("order_metadata_complete") is not True
            or not isinstance(quality.get("order_details"), dict)
            or quality["order_details"].get("complete") is not True
            or orders_quality.get("api_complete") is not True
            or orders_quality.get("complete") is not True
            or orders_quality.get("errors") not in (None, [])
            or not pages_complete
            or imported_count != int(run["order_count"])
        ):
            continue
        windows.add((left, right))
    return sorted(windows)


def verified_raw_order_coverage(order_db: Path, start_ms: int, end_ms: int) -> dict[str, Any]:
    """Return verified account-wide history-scan windows and gaps for one interval.

    The evidence covers API-visible standard orders, not zero-fill cancellations
    that OKX stopped retaining before collection.
    """
    if end_ms <= start_ms:
        raise ValueError("end_ms must exceed start_ms")
    with closing(_open_readonly(Path(order_db))) as connection:
        windows = _verified_raw_order_windows(connection, start_ms, end_ms)
    return {"windows": windows, "gaps": _coverage_gaps(windows, start_ms, end_ms)}


def _recorded_bracket(
    revision: dict[str, Any] | None, direction: str, entry_price: Decimal
) -> tuple[float | None, float | None, str, str | None]:
    if revision is None:
        return None, None, "not_recorded", "missing_parent_revision"
    attached = revision["payload"].get("attachAlgoOrds") or []
    if not isinstance(attached, list) or not attached:
        return None, None, "not_recorded", "missing_attached_bracket"
    if len(attached) != 1 or not isinstance(attached[0], dict):
        return None, None, "not_recorded", "ambiguous_attached_bracket"
    row = attached[0]
    target = _positive(row.get("tpTriggerPx")) or _positive(row.get("tpOrdPx"))
    stop = _positive(row.get("slTriggerPx")) or _positive(row.get("slOrdPx"))
    if target is None or stop is None:
        return (
            float(target) if target is not None else None,
            float(stop) if stop is not None else None,
            "recorded_parent_attached_bracket",
            "incomplete_attached_bracket",
        )
    valid = target > entry_price > stop if direction == "long" else target < entry_price < stop
    if not valid:
        return None, None, "not_recorded", "invalid_attached_bracket_geometry"
    return float(target), float(stop), "recorded_parent_attached_bracket", None


def _position_side(value: Any) -> str:
    return str(value or "net").lower()


def _matching_trade(
    trades: list[dict[str, Any]],
    *,
    exit_order_id: str,
    created_ms: int,
    side: str,
    position_side: str,
    size: Decimal | None,
    price: Decimal,
    kind: str,
) -> tuple[dict[str, Any] | None, str]:
    """Link by an allocated exit ID first, otherwise require a unique position.

    A stop's nominal size may remain at the original position size after a
    partial TP. We therefore compare it with the initial size, not remaining
    size. Missing size is allowed but never resolves an overlap by itself.
    """
    direct = [
        trade
        for trade in trades
        if exit_order_id and any(fill["order_id"] == exit_order_id for fill in trade["exit_fills"])
    ]
    if len(direct) == 1:
        return direct[0], "direct_exit_order"
    if len(direct) > 1:
        return None, "ambiguous_direct_exit"

    candidates = []
    for trade in trades:
        if side != ("sell" if trade["side"] == "long" else "buy"):
            continue
        if position_side != trade["position_side"]:
            continue
        if created_ms < trade["entry_ms"]:
            continue
        if trade["status"] == "closed" and created_ms > trade["exit_ms"]:
            continue
        entry = Decimal(str(trade["entry_price"]))
        direction = 1 if trade["side"] == "long" else -1
        if kind == "tp" and (price - entry) * direction <= ZERO:
            continue
        if size is not None and size > Decimal(str(trade["size"])) * Decimal("1.000001"):
            continue
        candidates.append(trade)
    if len(candidates) == 1:
        return candidates[0], "unique_active_position_inferred"
    return None, "ambiguous_active_position" if candidates else "no_compatible_position"


def _independent_plan_events(
    orders: dict[str, dict[str, Any]],
    algos: dict[str, dict[str, Any]],
    trades: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attach observable TP/SL events; leave competing positions unresolved."""
    issues: list[dict[str, Any]] = []
    candidates: list[tuple[str, str, dict[str, Any], Decimal]] = []
    for order_id, order in orders.items():
        payload = order["payload"]
        if _true(payload.get("reduceOnly")) and _true(payload.get("isTpLimit")):
            price = _positive(payload.get("px"))
            if price is not None:
                candidates.append(("tp", order_id, order, price))
    for algo_id, algo in algos.items():
        payload = algo["payload"]
        for kind, key in (("tp", "tpTriggerPx"), ("sl", "slTriggerPx")):
            price = _positive(payload.get(key))
            if price is not None:
                candidates.append((kind, algo_id, algo, price))

    for kind, event_id, record, price in sorted(
        candidates,
        key=lambda row: (_integer(row[2].get("created_ms")) or 0, row[1], row[0]),
    ):
        payload = record["payload"]
        created_ms = _integer(record.get("created_ms"))
        if created_ms is None:
            issues.append({"kind": "plan_order_missing_creation_time", "event_id": event_id})
            continue
        triggered_order = str(payload.get("ordId") or "") if "algo_id" in record else event_id
        size = _positive(payload.get("sz"))
        trade, association = _matching_trade(
            trades,
            exit_order_id=triggered_order,
            created_ms=created_ms,
            side=str(payload.get("side") or "").lower(),
            position_side=_position_side(payload.get("posSide")),
            size=size,
            price=price,
            kind=kind,
        )
        if trade is None:
            issues.append(
                {
                    "kind": association,
                    "event_id": event_id,
                    "time_ms": created_ms,
                    "plan_kind": kind,
                }
            )
            continue
        event = {
            "kind": kind,
            "id": event_id,
            "source": "algo_order" if "algo_id" in record else "ordinary_order",
            "price": float(price),
            "size": float(size) if size is not None else None,
            "created_ms": created_ms,
            "first_observed_ms": _integer(record.get("first_observed_ms")),
            "state": str(record.get("exchange_state") or ""),
            "terminal_ms": _integer(record.get("terminal_ms")),
            "trigger_order_id": triggered_order if "algo_id" in record else None,
            "association": association,
            "timing_note": (
                "Exchange cTime is known; prior in-place edits and the effective "
                "cancellation time are not reconstructed from a terminal snapshot"
            ),
        }
        trade["tp_orders" if kind == "tp" else "sl_orders"].append(event)
    return issues


def _attach_net_r(
    trades: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    bills: list[dict[str, Any]],
    bill_coverage: list[tuple[int, int]],
    instrument: dict[str, Any] | None,
    asof_ms: int | None = None,
) -> None:
    """Attribute realized net PnL; final R still requires a closed position.

    Bill ``balChg`` already includes the execution PnL and fee. A funding bill
    without an order ID can only be allocated if exactly one position is open
    in this instrument at that instant. Overlaps are deliberately left blank.
    """
    for trade in trades:
        trade["net_r"] = None
        trade["net_r_note"] = "unavailable_or_open"
        trade["realized_net_quote"] = None
        trade["realized_net_note"] = "unavailable"
    if not instrument:
        return
    base, *rest = str(instrument.get("instId") or "").split("-")
    quote = str(instrument.get("settleCcy") or "")
    multiplier = _positive(instrument.get("ctVal"))
    if (
        len(rest) < 2
        or quote not in {"USDT", "USDC"}
        or instrument.get("ctValCcy") != base
        or multiplier is None
    ):
        return

    total_fill_size: dict[str, Decimal] = defaultdict(lambda: ZERO)
    fill_by_bill_id: dict[str, tuple[str, int]] = {}
    for fill in fills:
        order_id = str(fill["payload"].get("ordId") or "")
        size = _positive(fill["payload"].get("fillSz"))
        if order_id and size is not None:
            total_fill_size[order_id] += size
            bill_id = str(fill["payload"].get("billId") or "")
            if bill_id:
                fill_by_bill_id[bill_id] = (order_id, fill["time_ms"])
    order_bills: dict[str, list[tuple[int, int, Decimal]]] = defaultdict(list)
    funding: list[tuple[int, Decimal]] = []
    for bill in bills:
        payload = bill["payload"]
        if str(payload.get("ccy") or "") != quote:
            continue
        change = _decimal(payload.get("balChg"))
        if change is None:
            continue
        order_id = str(payload.get("ordId") or "")
        if order_id:
            matched_fill = fill_by_bill_id.get(str(payload.get("billId") or ""))
            effective_ms = (
                matched_fill[1] if matched_fill and matched_fill[0] == order_id else bill["time_ms"]
            )
            order_bills[order_id].append((effective_ms, bill["time_ms"], change))
        elif str(payload.get("type")) == "8" and str(payload.get("subType")) in {"173", "174"}:
            funding.append((bill["time_ms"], change))

    for trade in trades:
        if trade["sl_price"] is None:
            continue
        closed = trade["status"] == "closed"
        if not closed and asof_ms is None:
            continue
        accounting_end_ms = trade["exit_ms"] + 1 if closed else asof_ms
        if _coverage_gaps(bill_coverage, trade["entry_ms"], accounting_end_ms):
            trade["realized_net_note"] = "bill_coverage_gap"
            if closed:
                trade["net_r_note"] = "bill_coverage_gap"
            continue
        entry = Decimal(str(trade["entry_price"]))
        stop = Decimal(str(trade["sl_price"]))
        risk = abs(entry - stop) * Decimal(str(trade["size"])) * multiplier
        if risk <= ZERO:
            continue
        allocated: dict[str, Decimal] = defaultdict(lambda: ZERO)
        for fill in (*trade["entry_fills"], *trade["exit_fills"]):
            allocated[fill["order_id"]] += Decimal(str(fill["size"]))
        attributable_order_bills = {
            order_id: sum(
                (
                    change
                    for time_ms, bill_ms, change in order_bills[order_id]
                    if time_ms < accounting_end_ms and (asof_ms is None or bill_ms < asof_ms)
                ),
                ZERO,
            )
            for order_id in allocated
        }
        if any(
            not any(
                time_ms < accounting_end_ms and (asof_ms is None or bill_ms < asof_ms)
                for time_ms, bill_ms, _ in order_bills[order_id]
            )
            or total_fill_size[order_id] <= ZERO
            or quantity > total_fill_size[order_id] * Decimal("1.000001")
            for order_id, quantity in allocated.items()
        ):
            trade["realized_net_note"] = "missing_or_inconsistent_order_bills"
            if closed:
                trade["net_r_note"] = "missing_or_inconsistent_order_bills"
            continue
        net = sum(
            (
                attributable_order_bills[order_id] * quantity / total_fill_size[order_id]
                for order_id, quantity in allocated.items()
            ),
            ZERO,
        )
        ambiguous_funding = False
        for time_ms, change in funding:
            if not trade["entry_ms"] <= time_ms < accounting_end_ms:
                continue
            active = [
                candidate
                for candidate in trades
                if candidate["entry_ms"] <= time_ms
                and (candidate["status"] != "closed" or time_ms <= candidate["exit_ms"])
            ]
            if len(active) != 1 or active[0] is not trade:
                ambiguous_funding = True
                break
            net += change
        if ambiguous_funding:
            trade["realized_net_note"] = "funding_allocation_ambiguous"
            if closed:
                trade["net_r_note"] = "funding_allocation_ambiguous"
            continue
        trade["initial_risk_quote"] = float(risk)
        trade["realized_net_quote"] = float(net)
        trade["realized_net_note"] = "verified_order_bills_and_uniquely_allocated_funding"
        if closed:
            trade["net_pnl_quote"] = float(net)
            trade["net_r"] = float(net / risk)
            trade["net_r_note"] = trade["realized_net_note"]


def _route(side: str, pos_side: str) -> tuple[str | None, str | None]:
    """Return FIFO queue to close, then FIFO queue to open, if allowed."""
    if pos_side in ("", "net"):
        if side == "buy":
            return "net_short", "net_long"
        if side == "sell":
            return "net_long", "net_short"
    elif pos_side == "long":
        if side == "buy":
            return None, "hedge_long"
        if side == "sell":
            return "hedge_long", None
    elif pos_side == "short":
        if side == "sell":
            return None, "hedge_short"
        if side == "buy":
            return "hedge_short", None
    return None, None


def load_timeline(
    order_db: Path, fills_db: Path, instrument: str, start_ms: int, end_ms: int
) -> dict[str, Any]:
    """Return recorded plans and FIFO-allocated actual fills for one instrument.

    Selection is by the *first opening fill* in ``[start_ms, end_ms)``. The
    replay reads all available fills before and after that interval so partial
    entries, reversals, and later exits are not cut off at the display window.
    All timestamps are Unix milliseconds; callers choose Beijing boundaries.
    """
    if not instrument or end_ms <= start_ms:
        raise ValueError("instrument must be nonempty and end_ms must exceed start_ms")
    order_db, fills_db = Path(order_db), Path(fills_db)
    omissions: list[dict[str, Any]] = []

    with (
        closing(_open_readonly(order_db)) as orders_connection,
        closing(_open_readonly(fills_db)) as fills_connection,
    ):
        order_rows = orders_connection.execute(
            "SELECT order_id, created_ms, last_seen_ms, exchange_state, payload_json "
            "FROM orders WHERE inst_id = ?",
            (instrument,),
        ).fetchall()
        orders = {
            str(row["order_id"]): {**dict(row), "payload": json.loads(row["payload_json"])}
            for row in order_rows
        }
        revision_rows = orders_connection.execute(
            "SELECT order_id, first_observed_ms, source, payload_json "
            "FROM order_revisions WHERE inst_id = ? "
            "ORDER BY first_observed_ms, id",
            (instrument,),
        ).fetchall()
        earliest_revisions: dict[str, dict[str, Any]] = {}
        for row in revision_rows:
            earliest_revisions.setdefault(
                str(row["order_id"]),
                {
                    "first_observed_ms": int(row["first_observed_ms"]),
                    "source": str(row["source"]),
                    "payload": json.loads(row["payload_json"]),
                },
            )
        for order_id, order in orders.items():
            revision = earliest_revisions.get(order_id)
            order["first_observed_ms"] = (
                revision["first_observed_ms"] if revision is not None else None
            )
            if order["exchange_state"] in {"filled", "canceled", "mmp_canceled"}:
                order["terminal_ms"] = _integer(order["payload"].get("uTime"))

        algo_tables = {
            str(row["name"])
            for row in orders_connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name IN ('algo_orders', 'algo_order_revisions')"
            )
        }
        algos: dict[str, dict[str, Any]] = {}
        if "algo_orders" in algo_tables:
            for row in orders_connection.execute(
                "SELECT algo_id, created_ms, exchange_state, payload_json "
                "FROM algo_orders WHERE inst_id = ?",
                (instrument,),
            ):
                payload = json.loads(row["payload_json"])
                algos[str(row["algo_id"])] = {
                    "algo_id": str(row["algo_id"]),
                    "created_ms": row["created_ms"],
                    "exchange_state": row["exchange_state"],
                    "terminal_ms": (
                        _integer(payload.get("uTime"))
                        if row["exchange_state"] in {"effective", "canceled", "order_failed"}
                        else None
                    ),
                    "payload": payload,
                }
        if "algo_order_revisions" in algo_tables:
            for row in orders_connection.execute(
                "SELECT algo_id, MIN(first_observed_ms) AS first_observed_ms "
                "FROM algo_order_revisions WHERE inst_id = ? GROUP BY algo_id",
                (instrument,),
            ):
                if str(row["algo_id"]) in algos:
                    algos[str(row["algo_id"])]["first_observed_ms"] = int(row["first_observed_ms"])

        fill_rows = fills_connection.execute(
            "SELECT identity, timestamp_ms, payload_json FROM fills "
            "WHERE active = 1 AND json_extract(payload_json, '$.instId') = ? "
            "ORDER BY timestamp_ms, identity",
            (instrument,),
        ).fetchall()
        coverage = [
            (int(row[0]), int(row[1]))
            for row in fills_connection.execute(
                "SELECT start_ms, end_ms FROM coverage WHERE entity = 'fills'"
            )
        ]
        bill_coverage = [
            (int(row[0]), int(row[1]))
            for row in fills_connection.execute(
                "SELECT start_ms, end_ms FROM coverage WHERE entity = 'bills'"
            )
        ]
        fills_tables = {
            str(row["name"])
            for row in fills_connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name IN ('bills', 'instruments')"
            )
        }
        bill_rows = (
            fills_connection.execute(
                "SELECT timestamp_ms, payload_json FROM bills WHERE active = 1 "
                "AND json_extract(payload_json, '$.instId') = ? ORDER BY timestamp_ms",
                (instrument,),
            ).fetchall()
            if "bills" in fills_tables
            else []
        )
        instrument_row = (
            fills_connection.execute(
                "SELECT payload_json FROM instruments WHERE inst_id = ?", (instrument,)
            ).fetchone()
            if "instruments" in fills_tables
            else None
        )
        order_windows = _verified_raw_order_windows(orders_connection, start_ms, end_ms)

    fills = [
        {
            "identity": str(row["identity"]),
            "time_ms": int(row["timestamp_ms"]),
            "payload": json.loads(row["payload_json"]),
        }
        for row in fill_rows
    ]
    bills = [
        {"time_ms": int(row["timestamp_ms"]), "payload": json.loads(row["payload_json"])}
        for row in bill_rows
    ]
    instrument_payload = json.loads(instrument_row["payload_json"]) if instrument_row else None
    fill_ids_by_order: dict[str, int] = defaultdict(int)
    for fill in fills:
        fill_ids_by_order[str(fill["payload"].get("ordId") or "")] += 1

    canceled_unfilled_orders = 0
    canceled_unfilled_parent_orders = 0
    for order in orders.values():
        created_ms = _integer(order["created_ms"])
        if created_ms is None or not start_ms <= created_ms < end_ms:
            continue
        if order["exchange_state"] != "canceled":
            continue
        if fill_ids_by_order.get(str(order["order_id"]), 0) > 0:
            continue
        if _positive(order["payload"].get("accFillSz")) is not None:
            omissions.append(
                {
                    "kind": "canceled_order_missing_archived_fill",
                    "order_id": str(order["order_id"]),
                    "time_ms": created_ms,
                }
            )
            continue
        canceled_unfilled_orders += 1
        if not _true(order["payload"].get("reduceOnly")):
            canceled_unfilled_parent_orders += 1

    # A lot is an opening slice from one parent order. FIFO exits may consume
    # several lots, and one reversal fill may both close old lots and open new.
    queues: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
    trades_by_order: dict[str, dict[str, Any]] = {}
    for fill in fills:
        payload = fill["payload"]
        order_id = str(payload.get("ordId") or "")
        order = orders.get(order_id)
        time_ms = fill["time_ms"]
        size = _positive(payload.get("fillSz"))
        price = _positive(payload.get("fillPx"))
        side = str(payload.get("side") or "").lower()
        pos_side = str(payload.get("posSide") or "net").lower()
        close_key, open_key = _route(side, pos_side)
        if (
            not order_id
            or size is None
            or price is None
            or (close_key is None and open_key is None)
        ):
            omissions.append(
                {
                    "kind": "invalid_fill",
                    "order_id": order_id,
                    "time_ms": time_ms,
                    "fill_identity": fill["identity"],
                }
            )
            continue
        if order is None:
            omissions.append(
                {
                    "kind": "fill_without_order_metadata",
                    "order_id": order_id,
                    "time_ms": time_ms,
                    "fill_identity": fill["identity"],
                }
            )
            continue

        reduce_only = _true(order["payload"].get("reduceOnly"))
        if reduce_only and open_key is not None and close_key is None:
            omissions.append(
                {
                    "kind": "reduce_only_open_side",
                    "order_id": order_id,
                    "time_ms": time_ms,
                    "fill_identity": fill["identity"],
                }
            )
            continue
        remainder = size
        closed = ZERO
        if close_key is not None:
            queue = queues[close_key]
            while queue and remainder > ZERO:
                lot = queue[0]
                allocated = min(remainder, lot["remaining"])
                lot["remaining"] -= allocated
                remainder -= allocated
                closed += allocated
                trade = lot["trade"]
                trade["exit_fills"].append(
                    {
                        "time_ms": time_ms,
                        "price": float(price),
                        "size": float(allocated),
                        "order_id": order_id,
                    }
                )
                trade["_exit_notional"] += allocated * price
                trade["_exit_size"] += allocated
                trade["_remaining"] -= allocated
                if lot["remaining"] == ZERO:
                    queue.popleft()

        if remainder == ZERO:
            continue
        if reduce_only or open_key is None:
            omissions.append(
                {
                    "kind": "exit_without_archived_open_lot",
                    "order_id": order_id,
                    "time_ms": time_ms,
                    "fill_identity": fill["identity"],
                    "size": float(remainder),
                }
            )
            continue
        # A positive realized fillPnl with no known closing lot indicates a
        # pre-archive position. Its opening/closing split cannot be inferred.
        fill_pnl = _decimal(payload.get("fillPnl")) or ZERO
        if pos_side in ("", "net") and closed == ZERO and fill_pnl != ZERO:
            omissions.append(
                {
                    "kind": "unresolved_prearchive_position",
                    "order_id": order_id,
                    "time_ms": time_ms,
                    "fill_identity": fill["identity"],
                    "size": float(remainder),
                }
            )
            continue

        direction = "long" if open_key.endswith("long") else "short"
        trade = trades_by_order.get(order_id)
        if trade is None:
            revision = earliest_revisions.get(order_id)
            trade = {
                "order_id": order_id,
                "created_ms": _integer(order["created_ms"]),
                "entry_ms": time_ms,
                "entry_price": None,
                "side": direction,
                "position_side": _position_side(pos_side),
                "size": None,
                "entry_fills": [],
                "tp_price": None,
                "sl_price": None,
                "tp_orders": [],
                "sl_orders": [],
                "exit_fills": [],
                "exit_ms": None,
                "exit_price": None,
                "status": None,
                "plan_provenance": "not_recorded",
                "plan_first_observed_ms": (
                    revision["first_observed_ms"] if revision is not None else None
                ),
                "plan_revision_source": revision["source"] if revision is not None else None,
                "entry_order_state": str(order["exchange_state"]),
                "_entry_notional": ZERO,
                "_entry_size": ZERO,
                "_exit_notional": ZERO,
                "_exit_size": ZERO,
                "_remaining": ZERO,
            }
            trades_by_order[order_id] = trade
        elif trade["side"] != direction:
            omissions.append(
                {
                    "kind": "parent_order_opposite_open_directions",
                    "order_id": order_id,
                    "time_ms": time_ms,
                    "fill_identity": fill["identity"],
                }
            )
            continue
        trade["entry_fills"].append(
            {
                "time_ms": time_ms,
                "price": float(price),
                "size": float(remainder),
                "order_id": order_id,
            }
        )
        trade["_entry_notional"] += remainder * price
        trade["_entry_size"] += remainder
        trade["_remaining"] += remainder
        queues[open_key].append({"trade": trade, "remaining": remainder})

    selected: list[dict[str, Any]] = []
    for trade in trades_by_order.values():
        entry_size: Decimal = trade.pop("_entry_size")
        entry_notional: Decimal = trade.pop("_entry_notional")
        exit_size: Decimal = trade.pop("_exit_size")
        exit_notional: Decimal = trade.pop("_exit_notional")
        remaining: Decimal = trade.pop("_remaining")
        entry_price = entry_notional / entry_size
        trade["entry_price"] = float(entry_price)
        trade["size"] = float(entry_size)
        trade["remaining_size"] = float(remaining)
        if trade["created_ms"] is None:
            trade["created_ms"] = trade["entry_ms"]
            omissions.append(
                {
                    "kind": "missing_parent_creation_time",
                    "order_id": trade["order_id"],
                    "time_ms": trade["entry_ms"],
                }
            )
        if exit_size > ZERO:
            trade["exit_ms"] = max(row["time_ms"] for row in trade["exit_fills"])
            trade["exit_price"] = float(exit_notional / exit_size)
        trade["entry_exit_interleaved"] = bool(
            trade["exit_fills"]
            and len(trade["entry_fills"]) > 1
            and min(row["time_ms"] for row in trade["exit_fills"])
            <= max(row["time_ms"] for row in trade["entry_fills"])
        )
        trade["status"] = (
            "closed"
            if remaining == ZERO
            else "partially_closed_in_archive"
            if exit_size > ZERO
            else "unclosed_in_archive"
        )
        revision = earliest_revisions.get(trade["order_id"])
        tp, sl, provenance, issue = _recorded_bracket(revision, trade["side"], entry_price)
        trade["tp_price"] = tp
        trade["sl_price"] = sl
        trade["plan_provenance"] = provenance
        trade["_parent_plan_issue"] = issue
        if start_ms <= trade["entry_ms"] < end_ms:
            selected.append(trade)

    plan_issues = _independent_plan_events(orders, algos, list(trades_by_order.values()))
    omissions.extend(plan_issues)
    for trade in selected:
        trade["tp_orders"].sort(key=lambda row: (row["created_ms"], row["id"]))
        trade["sl_orders"].sort(key=lambda row: (row["created_ms"], row["id"]))
        if trade["tp_orders"]:
            trade["tp_price"] = trade["tp_orders"][0]["price"]
        if trade["sl_orders"]:
            entry = Decimal(str(trade["entry_price"]))
            side = 1 if trade["side"] == "long" else -1
            initial = next(
                (
                    event
                    for event in trade["sl_orders"]
                    if (Decimal(str(event["price"])) - entry) * side < ZERO
                ),
                None,
            )
            if initial is not None:
                trade["sl_price"] = initial["price"]
        if trade["tp_orders"] or trade["sl_orders"]:
            trade["plan_provenance"] = "associated_independent_orders"
        parent_issue = trade.pop("_parent_plan_issue")
        if parent_issue is not None and (trade["tp_price"] is None or trade["sl_price"] is None):
            missing = [
                kind
                for kind, price in (("tp", trade["tp_price"]), ("sl", trade["sl_price"]))
                if price is None
            ]
            omissions.append(
                {
                    "kind": "unrecovered_" + "_and_".join(missing),
                    "parent_revision_issue": parent_issue,
                    "order_id": trade["order_id"],
                    "time_ms": trade["entry_ms"],
                }
            )
        if trade["tp_price"] is not None and trade["sl_price"] is not None:
            entry = Decimal(str(trade["entry_price"]))
            target = Decimal(str(trade["tp_price"]))
            stop = Decimal(str(trade["sl_price"]))
            risk = abs(entry - stop)
            direction = 1 if trade["side"] == "long" else -1
            trade["target_price_r"] = float((target - entry) * direction / risk)
            entry_size = Decimal(str(trade["size"]))
            realized = sum(
                (Decimal(str(fill["price"])) - entry) * direction * Decimal(str(fill["size"]))
                for fill in trade["exit_fills"]
            )
            trade["realized_price_r"] = float(realized / (risk * entry_size))
        else:
            trade["target_price_r"] = None
            trade["realized_price_r"] = None
    _attach_net_r(
        list(trades_by_order.values()), fills, bills, bill_coverage, instrument_payload, end_ms
    )
    selected.sort(key=lambda row: (row["entry_ms"], row["order_id"]))

    for order in orders.values():
        created_ms = _integer(order["created_ms"])
        if created_ms is None or not start_ms <= created_ms < end_ms:
            continue
        if _true(order["payload"].get("reduceOnly")):
            continue
        if _positive(order["payload"].get("accFillSz")) is not None and not fill_ids_by_order.get(
            str(order["order_id"]), 0
        ):
            omissions.append(
                {
                    "kind": "filled_parent_missing_archived_fill",
                    "order_id": str(order["order_id"]),
                    "time_ms": created_ms,
                }
            )

    fill_gaps = _coverage_gaps(coverage, start_ms, end_ms)
    archive_end = max((right for _, right in coverage), default=None)
    order_latest_observed = max((int(row["last_seen_ms"]) for row in order_rows), default=None)
    order_gaps: list[dict[str, Any]] = [
        {**gap, "reason": "no_verified_account_order_scan"}
        for gap in _coverage_gaps(order_windows, start_ms, end_ms)
    ]
    # A complete account-wide scan can cover a quiet instrument even if that
    # instrument has no order near the end of the requested interval.
    for fill in fills:
        if start_ms <= fill["time_ms"] < end_ms:
            order_id = str(fill["payload"].get("ordId") or "")
            if order_id not in orders:
                order_gaps.append(
                    {
                        "start_ms": fill["time_ms"],
                        "end_ms": fill["time_ms"] + 1,
                        "reason": "fill_without_order_metadata",
                        "order_id": order_id,
                    }
                )
    selected_order_ids = {trade["order_id"] for trade in selected}
    relevant_omissions = [
        row
        for row in omissions
        if start_ms <= int(row.get("time_ms") or 0) < end_ms
        or row.get("order_id") in selected_order_ids
    ]
    severe_omission_kinds = {
        "filled_parent_missing_archived_fill",
        "invalid_fill",
        "fill_without_order_metadata",
        "parent_order_opposite_open_directions",
        "exit_without_archived_open_lot",
        "unresolved_prearchive_position",
        "reduce_only_open_side",
    }
    quality = {
        "coverage_complete": not fill_gaps and not order_gaps,
        "coverage_gaps": {"fills": fill_gaps, "orders": order_gaps},
        "coverage_scope": (
            "active_fills_and_verified_account_wide_api_order_history_scans; "
            "zero_fill_cancellation_count_is_a_lower_bound_due_to_okx_retention"
        ),
        "fill_window_covered": not fill_gaps,
        "fill_coverage_gaps": fill_gaps,
        "archive_covered_through_ms": archive_end,
        "order_archive_latest_ms": order_latest_observed,
        "order_archive_latest_seen_ms": order_latest_observed,
        "verified_order_scan_windows": [
            {"start_ms": left, "end_ms": right} for left, right in order_windows
        ],
        "reconstruction_complete": (
            not fill_gaps
            and not order_gaps
            and not any(row["kind"] in severe_omission_kinds for row in relevant_omissions)
        ),
        "matching_method": "fifo_by_instrument_and_position_side",
        "initial_position_verified": False,
        "selected_trade_count": len(selected),
        "selected_missing_complete_bracket_count": sum(
            trade["tp_price"] is None or trade["sl_price"] is None for trade in selected
        ),
        "bracket_coverage": (
            "not_guaranteed: independent zero-fill TP cancellations can expire from "
            "OKX history; one terminal Algo snapshot may not reveal in-place edits"
        ),
        "associated_independent_plan_count": sum(
            bool(trade["tp_orders"] or trade["sl_orders"]) for trade in selected
        ),
        "entry_exit_interleaved_trade_count": sum(
            trade["entry_exit_interleaved"] for trade in selected
        ),
        "entry_price_note": (
            "entry_price is final fill-weighted average; if entry_exit_interleaved "
            "is true, use entry_fills and exit_fills for event-time geometry"
        ),
        "omission_count": len(relevant_omissions),
    }
    return {
        "instrument": instrument,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "trades": selected,
        "canceled_unfilled_orders": canceled_unfilled_orders,
        "canceled_unfilled_parent_orders": canceled_unfilled_parent_orders,
        "quality": quality,
        "omissions": relevant_omissions,
        "source": {
            "orders": str(order_db),
            "fills": str(fills_db),
            "plan": "parent_revision_or_independent_ordinary_and_algo_orders",
            "actual": "active_archived_fill_events",
        },
    }
