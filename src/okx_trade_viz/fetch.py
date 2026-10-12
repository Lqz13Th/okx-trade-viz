from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .client import OKXAPIError, ReadOnlyOKXClient


STANDARD_INST_TYPES = ("SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION", "EVENTS")
FILL_HISTORY_INST_TYPES = ("SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION")
RECENT_FILLS_AGE = timedelta(days=3)
RECENT_BILLS_AGE = timedelta(days=7)
RECENT_ORDERS_AGE = timedelta(days=7)
MAX_ROLLING_HISTORY_AGE = timedelta(days=90)
ALGO_ORDER_TYPES = "conditional,oco"
ALGO_HISTORY_STATES = ("effective", "canceled", "order_failed")
ALGO_HISTORY_RETENTION = timedelta(days=90)
ALGO_REQUEST_INTERVAL_SECONDS = 0.11


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime
    timezone: str

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("start 和 end 必须带时区")
        if self.start >= self.end:
            raise ValueError("start 必须早于 end")

    @property
    def start_ms(self) -> int:
        return int(self.start.timestamp() * 1000)

    @property
    def end_ms(self) -> int:
        return int(self.end.timestamp() * 1000)

    @classmethod
    def since_yesterday(cls, timezone_name: str, now: datetime | None = None) -> "TimeWindow":
        zone = ZoneInfo(timezone_name)
        local_now = now.astimezone(zone) if now else datetime.now(zone)
        yesterday = local_now.date() - timedelta(days=1)
        start = datetime.combine(yesterday, datetime.min.time(), tzinfo=zone)
        return cls(start=start, end=local_now, timezone=timezone_name)


def parse_datetime(value: str, timezone_name: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed


def _stable_id(row: Mapping[str, Any], cursor_field: str) -> str:
    cursor = str(row.get(cursor_field, ""))
    if cursor:
        return f"{cursor_field}:{cursor}"
    return "fallback:" + "|".join(
        str(row.get(key, ""))
        for key in ("instId", "tradeId", "ordId", "fillTime", "ts", "subType")
    )


def paginate_backwards(
    client: ReadOnlyOKXClient,
    path: str,
    params: Mapping[str, Any],
    *,
    cursor_field: str,
    authenticated: bool = True,
    max_pages: int = 10_000,
    interval_seconds: float = 0.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if interval_seconds < 0:
        raise ValueError("分页间隔不能为负数")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor: str | None = None
    pages = 0
    duplicates = 0
    termination_reason = ""

    for page_number in range(1, max_pages + 1):
        page_params = dict(params)
        if cursor:
            page_params["after"] = cursor
        page = client.get(path, page_params, authenticated=authenticated)
        pages = page_number
        if not page:
            termination_reason = "empty_page"
            break

        for row in page:
            row_id = _stable_id(row, cursor_field)
            if row_id in seen:
                duplicates += 1
                continue
            seen.add(row_id)
            rows.append(row)

        next_cursor = str(page[-1].get(cursor_field, ""))
        if not next_cursor:
            raise RuntimeError(f"{path} 最后一条记录缺少分页游标 {cursor_field}")
        if next_cursor == cursor:
            raise RuntimeError(f"{path} 分页游标停滞在 {cursor_field}={cursor}")
        cursor = next_cursor
        if interval_seconds:
            time.sleep(interval_seconds)
    else:
        raise RuntimeError(f"{path} 分页超过安全上限 {max_pages}")

    return rows, {
        "pages": pages,
        "rows_after_dedup": len(rows),
        "duplicates_removed": duplicates,
        "complete": termination_reason == "empty_page",
        "termination_reason": termination_reason,
    }


def fetch_pending_algo_orders(
    client: ReadOnlyOKXClient,
    *,
    inst_type: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch every pending conditional/OCO algo order visible to the account."""
    params: dict[str, Any] = {
        "ordType": ALGO_ORDER_TYPES,
        "limit": 100,
    }
    if inst_type:
        params["instType"] = inst_type
    rows, pagination = paginate_backwards(
        client,
        "/api/v5/trade/orders-algo-pending",
        params,
        cursor_field="algoId",
        interval_seconds=ALGO_REQUEST_INTERVAL_SECONDS,
    )
    missing_algo_id = sum(not str(row.get("algoId", "")).strip() for row in rows)
    quality = {
        "ord_type": ALGO_ORDER_TYPES,
        "inst_type": inst_type,
        "request_interval_seconds": ALGO_REQUEST_INTERVAL_SECONDS,
        "pagination": pagination,
        "rows": len(rows),
        "rows_missing_algo_id": missing_algo_id,
        "complete": pagination.get("complete") is True and missing_algo_id == 0,
    }
    return rows, quality


def _fetch_algo_history_state(
    client: ReadOnlyOKXClient,
    state: str,
    *,
    inst_type: str | None,
    minimum_c_time_ms: int,
    max_pages: int = 10_000,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor: str | None = None
    pages = 0
    duplicates = 0
    missing_c_time = 0
    missing_algo_id = 0
    termination_reason = ""

    for page_number in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "ordType": ALGO_ORDER_TYPES,
            "state": state,
            "limit": 100,
        }
        if inst_type:
            params["instType"] = inst_type
        if cursor:
            params["after"] = cursor
        try:
            page = client.get("/api/v5/trade/orders-algo-history", params)
        finally:
            # The endpoint is limited to 20 requests per two seconds. Sleeping
            # after every attempt also spaces transitions between state streams.
            time.sleep(ALGO_REQUEST_INTERVAL_SECONDS)
        pages = page_number
        if not page:
            termination_reason = "empty_page"
            break

        page_c_times: list[int] = []
        page_has_invalid_c_time = False
        for row in page:
            c_time = str(row.get("cTime", ""))
            if c_time.isdigit():
                page_c_times.append(int(c_time))
            else:
                page_has_invalid_c_time = True
                missing_c_time += 1
            if not str(row.get("algoId", "")).strip():
                missing_algo_id += 1
            row_id = _stable_id(row, "algoId")
            if row_id in seen:
                duplicates += 1
                continue
            seen.add(row_id)
            rows.append(row)

        next_cursor = str(page[-1].get("algoId", ""))
        if not next_cursor:
            raise RuntimeError(
                "/api/v5/trade/orders-algo-history 最后一条记录缺少分页游标 algoId"
            )
        if next_cursor == cursor:
            raise RuntimeError(
                "/api/v5/trade/orders-algo-history 分页游标停滞在 "
                f"state={state}, algoId={cursor}"
            )
        cursor = next_cursor

        if (
            page_c_times
            and not page_has_invalid_c_time
            and all(value < minimum_c_time_ms for value in page_c_times)
        ):
            termination_reason = "page_before_c_time_cutoff"
            break
    else:
        raise RuntimeError(
            "/api/v5/trade/orders-algo-history "
            f"state={state} 分页超过安全上限 {max_pages}"
        )

    return rows, {
        "pages": pages,
        "rows_after_dedup": len(rows),
        "duplicates_removed": duplicates,
        "rows_missing_c_time": missing_c_time,
        "rows_missing_algo_id": missing_algo_id,
        "minimum_c_time_ms": minimum_c_time_ms,
        "complete": termination_reason == "empty_page",
        "complete_for_window": (
            termination_reason in {"empty_page", "page_before_c_time_cutoff"}
            and missing_c_time == 0
            and missing_algo_id == 0
        ),
        "termination_reason": termination_reason,
    }


def fetch_algo_order_history(
    client: ReadOnlyOKXClient,
    window: TimeWindow,
    *,
    inst_type: str | None = None,
    server_now: datetime | None = None,
    minimum_c_time_ms: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch terminal conditional/OCO algos created in a half-open time window."""
    if minimum_c_time_ms is not None and (
        isinstance(minimum_c_time_ms, bool)
        or not isinstance(minimum_c_time_ms, int)
        or minimum_c_time_ms < 0
    ):
        raise ValueError("minimum_c_time_ms 必须是非负整数")
    as_of = server_now or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        raise ValueError("server_now 必须带时区")
    effective_start_ms = max(window.start_ms, minimum_c_time_ms or window.start_ms)
    if effective_start_ms >= window.end_ms:
        raise ValueError("minimum_c_time_ms 必须早于窗口结束时间")
    if effective_start_ms < int((as_of - ALGO_HISTORY_RETENTION).timestamp() * 1000):
        raise ValueError(
            "OKX algo 历史接口只承诺最近 3 个月；"
            "当前按 90 天上限拒绝更早窗口"
        )

    all_rows: list[tuple[str, dict[str, Any]]] = []
    by_state: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    for state in ALGO_HISTORY_STATES:
        try:
            state_rows, state_quality = _fetch_algo_history_state(
                client,
                state,
                inst_type=inst_type,
                minimum_c_time_ms=effective_start_ms,
            )
        except OKXAPIError as exc:
            errors.append({"state": state, "code": exc.code})
            continue
        all_rows.extend((state, row) for row in state_rows)
        by_state[state] = state_quality

    deduped: dict[str, dict[str, Any]] = {}
    cross_state_duplicates = 0
    for _, row in all_rows:
        row_id = _stable_id(row, "algoId")
        if row_id in deduped:
            cross_state_duplicates += 1
            continue
        deduped[row_id] = row

    available = list(deduped.values())
    missing_c_time = sum(not str(row.get("cTime", "")).isdigit() for row in available)
    missing_algo_id = sum(not str(row.get("algoId", "")).strip() for row in available)
    exact = [
        row
        for row in available
        if str(row.get("cTime", "")).isdigit()
        and effective_start_ms <= int(row["cTime"]) < window.end_ms
    ]
    exact.sort(key=lambda row: (int(row.get("cTime", 0)), str(row.get("algoId", ""))))
    state_requests_complete = not errors and set(by_state) == set(ALGO_HISTORY_STATES)
    fully_exhausted = state_requests_complete and all(
        item.get("complete") is True for item in by_state.values()
    )
    window_complete = state_requests_complete and all(
        item.get("complete_for_window") is True for item in by_state.values()
    )
    quality = {
        "ord_type": ALGO_ORDER_TYPES,
        "states_requested": list(ALGO_HISTORY_STATES),
        "inst_type": inst_type,
        "window_start_ms": window.start_ms,
        "effective_start_ms": effective_start_ms,
        "window_end_ms": window.end_ms,
        "minimum_c_time_ms": minimum_c_time_ms,
        "filter_semantics": "effective_start_ms <= cTime < window_end_ms",
        "timestamp_basis": "cTime (algo creation time)",
        "retention_note": "OKX orders-algo-history promises the most recent 3 months only",
        "request_interval_seconds": ALGO_REQUEST_INTERVAL_SECONDS,
        "by_state": by_state,
        "errors": errors,
        "rows_examined_after_cross_state_dedup": len(available),
        "rows_in_exact_c_time_window": len(exact),
        "cross_state_duplicates_removed": cross_state_duplicates,
        "rows_missing_c_time": missing_c_time,
        "rows_missing_algo_id": missing_algo_id,
        "api_complete": state_requests_complete,
        "complete": fully_exhausted and missing_c_time == 0 and missing_algo_id == 0,
        "complete_for_window": (
            window_complete and missing_c_time == 0 and missing_algo_id == 0
        ),
    }
    return exact, quality


def fetch_algo_order_detail(
    client: ReadOnlyOKXClient,
    *,
    algo_id: str | None = None,
    algo_client_order_id: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch one algo order by exactly one server or client-side identifier."""
    normalized_algo_id = (algo_id or "").strip()
    normalized_client_id = (algo_client_order_id or "").strip()
    if bool(normalized_algo_id) == bool(normalized_client_id):
        raise ValueError("必须且只能提供 algo_id 或 algo_client_order_id 其中一个")
    if normalized_algo_id:
        selector = "algoId"
        value = normalized_algo_id
    else:
        selector = "algoClOrdId"
        value = normalized_client_id
    rows = client.get("/api/v5/trade/order-algo", {selector: value})
    return rows, {
        "selector": selector,
        "requested_id": value,
        "rows": len(rows),
        "found": bool(rows),
        "complete": True,
    }


def _recent_fills(
    client: ReadOnlyOKXClient,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return paginate_backwards(
        client,
        "/api/v5/trade/fills",
        {"limit": 100},
        cursor_field="billId",
        interval_seconds=0.04,
    )


def _historical_fills(
    client: ReadOnlyOKXClient,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_inst_type: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    for inst_type in FILL_HISTORY_INST_TYPES:
        try:
            page_rows, page_quality = paginate_backwards(
                client,
                "/api/v5/trade/fills-history",
                {"instType": inst_type, "limit": 100},
                cursor_field="billId",
                interval_seconds=0.21,
            )
        except OKXAPIError as exc:
            errors.append({"instType": inst_type, "code": exc.code})
            continue
        rows.extend(page_rows)
        by_inst_type[inst_type] = page_quality
    return rows, {
        "by_inst_type": by_inst_type,
        "errors": errors,
        "complete": not errors
        and all(item.get("complete") is True for item in by_inst_type.values()),
    }


def fetch_fills(
    client: ReadOnlyOKXClient,
    window: TimeWindow,
    *,
    server_now: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    as_of = server_now or datetime.now(timezone.utc)
    recent_cutoff_ms = int((as_of - RECENT_FILLS_AGE).timestamp() * 1000)
    source_rows: list[tuple[str, list[dict[str, Any]]]] = []
    sources: dict[str, Any] = {}
    if window.start_ms < recent_cutoff_ms:
        history_rows, history_quality = _historical_fills(client)
        source_rows.append(("fills-history", history_rows))
        sources["fills-history"] = history_quality
    if window.end_ms > recent_cutoff_ms:
        recent_rows, recent_quality = _recent_fills(client)
        source_rows.append(("fills", recent_rows))
        sources["fills"] = recent_quality

    # Historical rows are loaded first and the 3-day endpoint overwrites matching
    # billIds, so the API explicitly recommended for the hot window has priority.
    available_by_id: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for source, rows in source_rows:
        source_counts[source] = len(rows)
        for row in rows:
            available_by_id[_stable_id(row, "billId")] = row
    available = list(available_by_id.values())
    exact: list[dict[str, Any]] = []
    missing_fill_time = 0
    missing_bill_id = 0
    for row in available:
        if not str(row.get("billId", "")).strip():
            missing_bill_id += 1
        value = str(row.get("fillTime", ""))
        if not value.isdigit():
            missing_fill_time += 1
            continue
        fill_time = int(value)
        if window.start_ms <= fill_time < window.end_ms:
            exact.append(row)
    exact.sort(key=lambda row: (int(row.get("fillTime", 0)), str(row.get("billId", ""))))
    pagination_complete = bool(sources) and all(
        source.get("complete") is True for source in sources.values()
    )
    quality = {
        "sources": sources,
        "source_rows": source_counts,
        "source_priority": "fills (last 3 days) over fills-history on duplicate billId",
        "rows_after_cross_source_dedup": len(available),
        "rows_in_exact_fill_time_window": len(exact),
        "rows_missing_fill_time": missing_fill_time,
        "rows_missing_bill_id": missing_bill_id,
        "filter_semantics": "start <= fillTime < end",
        "complete": pagination_complete,
        "complete_for_window": (
            pagination_complete and missing_fill_time == 0 and missing_bill_id == 0
        ),
    }
    return exact, quality


def fetch_bills(
    client: ReadOnlyOKXClient,
    window: TimeWindow,
    *,
    server_now: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    as_of = server_now or datetime.now(timezone.utc)
    recent_cutoff_ms = int((as_of - RECENT_BILLS_AGE).timestamp() * 1000)
    source_rows: list[tuple[str, list[dict[str, Any]]]] = []
    sources: dict[str, Any] = {}
    if window.start_ms < recent_cutoff_ms:
        archive_end = min(window.end_ms, recent_cutoff_ms)
        rows, page_quality = paginate_backwards(
            client,
            "/api/v5/account/bills-archive",
            {
                "begin": window.start_ms,
                "end": archive_end - 1,
                "limit": 100,
            },
            cursor_field="billId",
            interval_seconds=0.41,
        )
        source_rows.append(("bills-archive", rows))
        sources["bills-archive"] = page_quality
    if window.end_ms > recent_cutoff_ms:
        rows, page_quality = paginate_backwards(
            client,
            "/api/v5/account/bills",
            {"limit": 100},
            cursor_field="billId",
            interval_seconds=0.21,
        )
        source_rows.append(("bills", rows))
        sources["bills"] = page_quality

    available_by_id: dict[str, dict[str, Any]] = {}
    source_counts: dict[str, int] = {}
    for source, rows in source_rows:
        source_counts[source] = len(rows)
        for row in rows:
            available_by_id[_stable_id(row, "billId")] = row
    available = list(available_by_id.values())
    invalid_ts = sum(not str(row.get("ts", "")).isdigit() for row in available)
    missing_bill_id = sum(not str(row.get("billId", "")).strip() for row in available)
    exact = [
        row
        for row in available
        if str(row.get("ts", "")).isdigit()
        and window.start_ms <= int(row["ts"]) < window.end_ms
    ]
    exact.sort(key=lambda row: (int(row.get("ts", 0)), str(row.get("billId", ""))))
    pagination_complete = bool(sources) and all(
        source.get("complete") is True for source in sources.values()
    )
    quality = {
        "sources": sources,
        "source_rows": source_counts,
        "source_priority": "bills (last 7 days) over bills-archive on duplicate billId",
        "rows_after_cross_source_dedup": len(available),
        "rows_in_exact_ts_window": len(exact),
        "rows_invalid_ts": invalid_ts,
        "rows_missing_bill_id": missing_bill_id,
        "filter_semantics": "start <= ts < end",
        "complete": pagination_complete,
        "complete_for_window": (
            pagination_complete and invalid_ts == 0 and missing_bill_id == 0
        ),
    }
    return exact, quality


def fetch_orders(
    client: ReadOnlyOKXClient,
    window: TimeWindow,
    observed_inst_types: set[str],
    *,
    server_now: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    inst_types = list(STANDARD_INST_TYPES)
    for value in sorted(observed_inst_types):
        if value and value not in inst_types:
            inst_types.append(value)

    as_of = server_now or datetime.now(timezone.utc)
    recent_cutoff_ms = int((as_of - RECENT_ORDERS_AGE).timestamp() * 1000)
    endpoint_windows: list[tuple[str, int, int, float]] = []
    if window.start_ms < recent_cutoff_ms:
        endpoint_windows.append(
            (
                "/api/v5/trade/orders-history-archive",
                window.start_ms,
                min(window.end_ms, recent_cutoff_ms),
                0.11,
            )
        )
    if window.end_ms > recent_cutoff_ms:
        endpoint_windows.append(
            (
                "/api/v5/trade/orders-history",
                max(window.start_ms, recent_cutoff_ms),
                window.end_ms,
                0.06,
            )
        )

    all_orders: list[tuple[str, dict[str, Any]]] = []
    quality: dict[str, Any] = {
        "by_endpoint": {},
        "errors": [],
        "zero_fill_cancellations_complete": False,
        "cancellation_retention_note": (
            "The last-7-days endpoint explicitly retains canceled unfilled orders for only "
            "about two hours; scheduled pending-order snapshots are still required"
        ),
    }
    for path, start_ms, end_ms, interval_seconds in endpoint_windows:
        endpoint_quality: dict[str, Any] = {}
        for inst_type in inst_types:
            try:
                rows, page_quality = paginate_backwards(
                    client,
                    path,
                    {
                        "instType": inst_type,
                        "begin": start_ms,
                        "end": end_ms - 1,
                        "limit": 100,
                    },
                    cursor_field="ordId",
                    interval_seconds=interval_seconds,
                )
            except OKXAPIError as exc:
                quality["errors"].append(
                    {"endpoint": path, "instType": inst_type, "code": exc.code}
                )
                continue
            all_orders.extend((path, row) for row in rows)
            endpoint_quality[inst_type] = page_quality
        quality["by_endpoint"][path] = endpoint_quality

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for _, order in all_orders:
        key = (str(order.get("instId", "")), str(order.get("ordId", "")))
        deduped[key] = order
    result = sorted(
        deduped.values(),
        key=lambda row: (int(row.get("cTime", 0) or 0), str(row.get("ordId", ""))),
    )
    quality["rows_after_dedup"] = len(result)
    quality["api_complete"] = not quality["errors"]
    quality["complete"] = quality["api_complete"] and all(
        page.get("complete") is True
        for endpoint in quality["by_endpoint"].values()
        for page in endpoint.values()
    )
    return result, quality


def fill_missing_order_details(
    client: ReadOnlyOKXClient,
    fills: list[dict[str, Any]],
    orders: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    existing = {
        (str(order.get("instId", "")), str(order.get("ordId", ""))) for order in orders
    }
    wanted = sorted(
        {
            (str(fill.get("instId", "")), str(fill.get("ordId", "")))
            for fill in fills
            if fill.get("instId") and fill.get("ordId")
        }
        - existing
    )
    fetched: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for inst_id, ord_id in wanted:
        try:
            rows = client.get(
                "/api/v5/trade/order", {"instId": inst_id, "ordId": ord_id}
            )
        except OKXAPIError as exc:
            errors.append({"instId": inst_id, "ordId": ord_id, "code": exc.code})
            continue
        fetched.extend(rows)
        time.sleep(0.04)
    deduped = {
        (str(row.get("instId", "")), str(row.get("ordId", ""))): row for row in fetched
    }
    return list(deduped.values()), {
        "missing_order_details_requested": len(wanted),
        "missing_order_details_fetched": len(fetched),
        "errors": errors,
        "complete": not errors,
    }


def fetch_instruments(
    client: ReadOnlyOKXClient, fills: list[dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], list[dict[str, str]]]:
    keys = sorted(
        {
            (str(row.get("instType", "")), str(row.get("instId", "")))
            for row in fills
            if row.get("instType") and row.get("instId")
        }
    )
    instruments: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    for inst_type, inst_id in keys:
        try:
            rows = client.get(
                "/api/v5/public/instruments",
                {"instType": inst_type, "instId": inst_id},
                authenticated=False,
            )
        except OKXAPIError as exc:
            errors.append({"instId": inst_id, "code": exc.code})
            continue
        if rows:
            instruments[inst_id] = rows[0]
        time.sleep(0.11)
    return instruments, errors


def collect_account_data(
    client: ReadOnlyOKXClient, window: TimeWindow
) -> dict[str, Any]:
    clock_offset_ms = client.sync_clock()
    server_now = datetime.now(timezone.utc) + timedelta(milliseconds=clock_offset_ms)
    if window.end > server_now + timedelta(minutes=5):
        raise ValueError("分析结束时间不能晚于 OKX 服务器当前时间")
    if window.start < server_now - MAX_ROLLING_HISTORY_AGE:
        raise ValueError(
            "OKX 滚动成交/账单接口最多只承诺最近 3 个月；"
            "当前安全实现按 90 天上限拒绝更早窗口"
        )
    config_rows = client.get("/api/v5/account/config")
    config = config_rows[0] if config_rows else {}
    permission_set = {
        part.strip().lower() for part in str(config.get("perm", "")).split(",")
    }
    if "withdraw" in permission_set:
        raise ValueError("拒绝使用带 Withdraw 权限的 API key；请立即撤销并重建只读 key")
    config_summary = {
        "perm": config.get("perm", ""),
        "acctLv": config.get("acctLv", ""),
        "posMode": config.get("posMode", ""),
        "ip_whitelist_bound": bool(config.get("ip")),
    }
    identity_parts = [
        str(config.get(field) or "").strip()
        for field in ("mainUid", "uid", "subAcct")
    ]
    stable_identity = "|".join(part for part in identity_parts if part)
    if not stable_identity:
        stable_identity = f"api-key:{client.settings.api_key}"
    account_fingerprint = hashlib.sha256(stable_identity.encode("utf-8")).hexdigest()

    fills, fills_quality = fetch_fills(client, window, server_now=server_now)
    bills, bills_quality = fetch_bills(client, window, server_now=server_now)
    observed_types = {str(row.get("instType", "")) for row in fills}
    orders, orders_quality = fetch_orders(
        client,
        window,
        observed_types,
        server_now=server_now,
    )
    algo_orders, algo_orders_quality = fetch_algo_order_history(
        client,
        window,
        server_now=server_now,
    )
    fill_order_details, order_detail_quality = fill_missing_order_details(
        client, fills, orders
    )
    instruments, instrument_errors = fetch_instruments(client, fills)

    positions_requested_at = datetime.now(timezone.utc).isoformat()
    positions = client.get("/api/v5/account/positions")
    positions_as_of = datetime.now(timezone.utc).isoformat()

    return {
        "metadata": {
            "window_start": window.start.isoformat(),
            "window_end": window.end.isoformat(),
            "timezone": window.timezone,
            "start_ms": window.start_ms,
            "end_ms": window.end_ms,
            "collected_at": datetime.now(window.start.tzinfo).isoformat(),
            "environment": "DEMO" if client.settings.demo else "LIVE",
            "source_kind": "okx_api",
            "account_fingerprint": account_fingerprint,
            "clock_offset_ms": clock_offset_ms,
        },
        "account_config": config_summary,
        "fills": fills,
        "bills": bills,
        "orders": orders,
        "algo_orders": algo_orders,
        "fill_order_details": fill_order_details,
        "positions": positions,
        "instruments": instruments,
        "snapshot_times": {
            "positions_requested_at": positions_requested_at,
            "positions_as_of": positions_as_of,
            "note": "current position snapshot; not a historical window-end value",
        },
        "quality": {
            "critical_sources_complete": bool(
                fills_quality.get("complete_for_window")
                and bills_quality.get("complete_for_window")
            ),
            "order_metadata_complete": bool(
                orders_quality.get("api_complete")
                and order_detail_quality.get("complete")
            ),
            "algo_order_metadata_complete": bool(
                algo_orders_quality.get("complete_for_window")
            ),
            "bracket_metadata_complete": bool(
                orders_quality.get("api_complete")
                and order_detail_quality.get("complete")
                and algo_orders_quality.get("complete_for_window")
            ),
            "fills": fills_quality,
            "bills": bills_quality,
            "orders": orders_quality,
            "algo_orders": algo_orders_quality,
            "order_details": order_detail_quality,
            "instrument_errors": instrument_errors,
        },
    }
