from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from .client import OKXAPIError, ReadOnlyOKXClient
from .config import ConfigurationError, Settings
from .fetch import (
    TimeWindow,
    fetch_algo_order_detail,
    fetch_algo_order_history,
    fetch_orders,
    fetch_pending_algo_orders,
)
from .order_archive import (
    OrderArchive,
    discover_raw_identity,
    load_archive_identity,
)
from .storage import private_directory


PENDING_INST_TYPES = ("SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION")
FATAL_AUTH_CODES = frozenset(
    {"50101", "50103", "50105", "50106", "50107", "50110", "50111", "50113", "50114"}
)


def fetch_server_time_ms(client: ReadOnlyOKXClient) -> int:
    """Read the exchange clock used to timestamp an archive observation."""
    rows = client.get("/api/v5/public/time", authenticated=False)
    if not rows:
        raise OKXAPIError("INVALID_TIME", "OKX 时间接口返回空数据")
    timestamp = str(rows[0].get("ts") or "")
    if not timestamp.isdigit() or int(timestamp) <= 0:
        raise OKXAPIError("INVALID_TIME", "OKX 时间接口缺少有效 ts")
    return int(timestamp)


def fetch_pending_orders(
    client: ReadOnlyOKXClient,
    *,
    inst_types: Sequence[str] = PENDING_INST_TYPES,
    max_pages_per_type: int = 100,
) -> list[dict[str, Any]]:
    """Collect ordinary pending orders without importing the trading guard."""
    orders: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for inst_type in inst_types:
        after = ""
        for _ in range(max_pages_per_type):
            params = {"instType": inst_type, "limit": "100"}
            if after:
                params["after"] = after
            page = client.get("/api/v5/trade/orders-pending", params)
            for row in page:
                order_id = str(row.get("ordId") or "").strip()
                if order_id and order_id not in seen_ids:
                    seen_ids.add(order_id)
                    orders.append(row)
            if len(page) < 100:
                break
            next_after = str(page[-1].get("ordId") or "").strip()
            if not next_after or next_after == after:
                raise ValueError("挂单分页游标没有前进，安全中止")
            after = next_after
        else:
            raise ValueError("挂单分页超过安全上限")
    return orders


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="okx-order-archive",
        description="Append-only, read-only OKX standard and algo-order archive",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    once = subparsers.add_parser("once", help="collect one complete pending snapshot")
    once.add_argument("--env-file", type=Path, default=Path.cwd() / ".env")
    once.add_argument(
        "--database",
        type=Path,
        default=Path.cwd() / "data" / "orders.sqlite3",
    )
    once.add_argument(
        "--history-interval-seconds",
        type=float,
        default=300.0,
        help="minimum time between complete terminal-history scans",
    )
    once.add_argument(
        "--history-lookback-seconds",
        type=float,
        default=10_800.0,
        help="cTime overlap used to capture short-lived orders",
    )
    once.add_argument(
        "--algo-history-interval-seconds",
        type=float,
        default=300.0,
        help="minimum time between complete algo-history scans",
    )
    once.add_argument(
        "--algo-history-lookback-seconds",
        type=float,
        default=10_800.0,
        help="cTime overlap used to capture terminal conditional/OCO orders",
    )
    once.add_argument("--max-reconcile-per-run", type=int, default=10)
    once.add_argument("--max-algo-reconcile-per-run", type=int, default=10)
    once.add_argument("--detail-request-interval-seconds", type=float, default=0.25)

    status = subparsers.add_parser("status", help="read local archive status")
    status.add_argument(
        "--database",
        type=Path,
        default=Path.cwd() / "data" / "orders.sqlite3",
    )

    importer = subparsers.add_parser(
        "import-raw", help="seed terminal orders from existing raw.json snapshots"
    )
    importer.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="project root containing data/*/raw.json",
    )
    importer.add_argument(
        "--database",
        type=Path,
        default=Path.cwd() / "data" / "orders.sqlite3",
    )
    return parser


def _validate_once_args(args: argparse.Namespace) -> None:
    numeric = {
        "--history-interval-seconds": args.history_interval_seconds,
        "--history-lookback-seconds": args.history_lookback_seconds,
        "--algo-history-interval-seconds": args.algo_history_interval_seconds,
        "--algo-history-lookback-seconds": args.algo_history_lookback_seconds,
        "--detail-request-interval-seconds": args.detail_request_interval_seconds,
    }
    for name, value in numeric.items():
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} 必须是大于 0 的有限数字")
    if args.history_interval_seconds < 60:
        raise ValueError("--history-interval-seconds 不能低于 60 秒")
    if args.algo_history_interval_seconds < 60:
        raise ValueError("--algo-history-interval-seconds 不能低于 60 秒")
    if not 7_200 <= args.history_lookback_seconds <= 7 * 24 * 3600:
        raise ValueError("--history-lookback-seconds 必须在 2 小时到 7 天之间")
    if not 7_200 <= args.algo_history_lookback_seconds <= 90 * 24 * 3600:
        raise ValueError("--algo-history-lookback-seconds 必须在 2 小时到 90 天之间")
    if not 1 <= args.max_reconcile_per_run <= 20:
        raise ValueError("--max-reconcile-per-run 必须在 1 到 20 之间")
    if not 1 <= args.max_algo_reconcile_per_run <= 20:
        raise ValueError("--max-algo-reconcile-per-run 必须在 1 到 20 之间")
    if args.detail_request_interval_seconds < 0.2:
        raise ValueError("--detail-request-interval-seconds 不能低于 0.2 秒")


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _safe_error(exc: Exception) -> dict[str, str]:
    result = {"type": type(exc).__name__, "message": str(exc)}
    if isinstance(exc, OKXAPIError):
        result["code"] = exc.code
    return result


def _account_context(
    client: ReadOnlyOKXClient, settings: Settings
) -> tuple[str, str, dict[str, Any]]:
    rows = client.get("/api/v5/account/config")
    if not rows:
        raise ValueError("账户配置返回空数据")
    config = rows[0]
    permissions = {
        value.strip().lower() for value in str(config.get("perm") or "").split(",") if value.strip()
    }
    if "withdraw" in permissions:
        raise ValueError("拒绝使用含 Withdraw 权限的 key 运行订单归档")
    identity_parts = [
        str(config.get(field) or "").strip() for field in ("mainUid", "uid", "subAcct")
    ]
    stable_identity = "|".join(value for value in identity_parts if value)
    if not stable_identity:
        stable_identity = f"api-key:{settings.api_key}"
    fingerprint = hashlib.sha256(stable_identity.encode("utf-8")).hexdigest()
    environment = "DEMO" if settings.demo else "LIVE"
    public = {
        "environment": environment,
        "permissions": sorted(permissions),
        "position_mode": str(config.get("posMode") or ""),
        "ip_whitelist_reported": bool(config.get("ip")),
    }
    return fingerprint, environment, public


def _acquire_lock(database: Path):
    database = database.expanduser().resolve()
    private_directory(database.parent)
    lock_path = database.with_suffix(database.suffix + ".lock")
    descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT, 0o600)
    handle = os.fdopen(descriptor, "w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise ValueError("已有另一个订单归档进程正在使用该数据库") from None
    handle.seek(0)
    handle.truncate()
    handle.write(f"{os.getpid()}\n")
    handle.flush()
    return handle


def _fetch_detail_rows(
    client: ReadOnlyOKXClient,
    requested: Sequence[tuple[str, str]],
    *,
    interval_seconds: float,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, str]]]:
    rows: list[dict[str, Any]] = []
    errors: dict[tuple[str, str], dict[str, str]] = {}
    last_request_at = 0.0
    for inst_id, order_id in requested:
        elapsed = time.monotonic() - last_request_at
        remaining = interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
        last_request_at = time.monotonic()
        try:
            response = client.get("/api/v5/trade/order", {"instId": inst_id, "ordId": order_id})
            match = next(
                (
                    row
                    for row in response
                    if str(row.get("instId") or "") == inst_id
                    and str(row.get("ordId") or "") == order_id
                ),
                None,
            )
            if match is None:
                raise ValueError("订单详情返回空数据或身份不匹配")
            rows.append(match)
        except (OKXAPIError, ConnectionError, ValueError) as exc:
            errors[(inst_id, order_id)] = _safe_error(exc)
            if isinstance(exc, OKXAPIError) and exc.code in FATAL_AUTH_CODES:
                break
    return rows, errors


def _fetch_algo_detail_rows(
    client: ReadOnlyOKXClient,
    requested: Sequence[tuple[str, str]],
    *,
    interval_seconds: float,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], dict[str, str]]]:
    rows: list[dict[str, Any]] = []
    errors: dict[tuple[str, str], dict[str, str]] = {}
    last_request_at = 0.0
    for inst_id, algo_id in requested:
        elapsed = time.monotonic() - last_request_at
        remaining = interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
        last_request_at = time.monotonic()
        try:
            response, _ = fetch_algo_order_detail(client, algo_id=algo_id)
            match = next(
                (
                    row
                    for row in response
                    if str(row.get("instId") or "") == inst_id
                    and str(row.get("algoId") or "") == algo_id
                ),
                None,
            )
            if match is None:
                raise ValueError("Algo 订单详情返回空数据或身份不匹配")
            rows.append(match)
        except (OKXAPIError, ConnectionError, ValueError) as exc:
            errors[(inst_id, algo_id)] = _safe_error(exc)
            if isinstance(exc, OKXAPIError) and exc.code in FATAL_AUTH_CODES:
                break
    return rows, errors


def _status_dict(archive: OrderArchive) -> dict[str, Any]:
    status = archive.status()
    return {
        "path": str(status.path),
        "orders": status.order_count,
        "terminal": status.terminal_count,
        "pending": status.pending_count,
        "unresolved": status.unresolved_count,
        "revisions": status.revision_count,
        "pending_scans": status.pending_scan_count,
        "last_pending_complete_ms": status.last_pending_complete_ms,
        "last_history_complete_ms": status.last_history_complete_ms,
        "algo_orders": status.algo_order_count,
        "algo_terminal": status.algo_terminal_count,
        "algo_pending": status.algo_pending_count,
        "algo_unresolved": status.algo_unresolved_count,
        "algo_revisions": status.algo_revision_count,
        "algo_pending_scans": status.algo_pending_scan_count,
        "last_algo_pending_complete_ms": status.last_algo_pending_complete_ms,
        "last_algo_history_complete_ms": status.last_algo_history_complete_ms,
    }


def _run_once(args: argparse.Namespace) -> int:
    _validate_once_args(args)
    settings = Settings.from_env_file(args.env_file)
    client = ReadOnlyOKXClient(settings)
    client.sync_clock()
    fingerprint, environment, public_account = _account_context(client, settings)
    archive = OrderArchive(
        args.database,
        account_fingerprint=fingerprint,
        environment=environment,
    )

    pending_started_ms = _now_ms()
    try:
        pending_rows = fetch_pending_orders(client, inst_types=PENDING_INST_TYPES)
        pending_observed_ms = fetch_server_time_ms(client)
        pending_result = archive.record_pending_scan(
            pending_rows,
            started_ms=pending_started_ms,
            observed_ms=pending_observed_ms,
            completed_ms=_now_ms(),
        )
    except (OKXAPIError, ConnectionError, ValueError) as exc:
        archive.record_failure(
            kind="pending",
            started_ms=pending_started_ms,
            completed_ms=_now_ms(),
            error=_safe_error(exc),
        )
        raise

    algo_pending_started_ms = _now_ms()
    try:
        algo_pending_rows, algo_pending_quality = fetch_pending_algo_orders(client)
        algo_pending_observed_ms = fetch_server_time_ms(client)
        algo_pending_result = archive.record_algo_pending_scan(
            algo_pending_rows,
            started_ms=algo_pending_started_ms,
            observed_ms=algo_pending_observed_ms,
            completed_ms=_now_ms(),
        )
    except (OKXAPIError, ConnectionError, ValueError, RuntimeError) as exc:
        archive.record_failure(
            kind="algo_pending",
            started_ms=algo_pending_started_ms,
            completed_ms=_now_ms(),
            error=_safe_error(exc),
        )
        raise

    detail_requested = archive.due_reconciliations(
        observed_ms=pending_observed_ms,
        limit=args.max_reconcile_per_run,
    )
    detail_error_count = 0
    if detail_requested:
        detail_started_ms = _now_ms()
        detail_rows, detail_errors = _fetch_detail_rows(
            client,
            detail_requested,
            interval_seconds=args.detail_request_interval_seconds,
        )
        detail_observed_ms = fetch_server_time_ms(client)
        archive.record_detail_scan(
            detail_rows,
            detail_errors,
            requested=detail_requested,
            started_ms=detail_started_ms,
            observed_ms=detail_observed_ms,
            completed_ms=_now_ms(),
        )
        detail_error_count = len(detail_errors)

    algo_detail_requested = archive.due_algo_reconciliations(
        observed_ms=algo_pending_observed_ms,
        limit=args.max_algo_reconcile_per_run,
    )
    algo_detail_error_count = 0
    if algo_detail_requested:
        algo_detail_started_ms = _now_ms()
        algo_detail_rows, algo_detail_errors = _fetch_algo_detail_rows(
            client,
            algo_detail_requested,
            interval_seconds=args.detail_request_interval_seconds,
        )
        algo_detail_observed_ms = fetch_server_time_ms(client)
        archive.record_algo_detail_scan(
            algo_detail_rows,
            algo_detail_errors,
            requested=algo_detail_requested,
            started_ms=algo_detail_started_ms,
            observed_ms=algo_detail_observed_ms,
            completed_ms=_now_ms(),
        )
        algo_detail_error_count = len(algo_detail_errors)

    history_ran = False
    history_complete: bool | None = None
    history_count = 0
    if archive.history_due(
        observed_ms=pending_observed_ms,
        interval_ms=int(args.history_interval_seconds * 1000),
    ):
        history_ran = True
        history_started_ms = _now_ms()
        end = datetime.fromtimestamp(pending_observed_ms / 1000, timezone.utc)
        start = end - timedelta(seconds=args.history_lookback_seconds)
        history_rows, history_quality = fetch_orders(
            client,
            TimeWindow(start=start, end=end, timezone="UTC"),
            set(PENDING_INST_TYPES),
            server_now=end,
        )
        history_complete = history_quality.get("complete") is True
        archive.record_history_scan(
            history_rows,
            started_ms=history_started_ms,
            observed_ms=fetch_server_time_ms(client),
            completed_ms=_now_ms(),
            complete=history_complete,
            details=history_quality,
        )
        history_count = len(history_rows)

    algo_history_ran = False
    algo_history_complete: bool | None = None
    algo_history_count = 0
    if archive.algo_history_due(
        observed_ms=algo_pending_observed_ms,
        interval_ms=int(args.algo_history_interval_seconds * 1000),
    ):
        algo_history_ran = True
        algo_history_started_ms = _now_ms()
        algo_end = datetime.fromtimestamp(algo_pending_observed_ms / 1000, timezone.utc)
        algo_start = algo_end - timedelta(seconds=args.algo_history_lookback_seconds)
        algo_history_rows, algo_history_quality = fetch_algo_order_history(
            client,
            TimeWindow(start=algo_start, end=algo_end, timezone="UTC"),
            server_now=algo_end,
        )
        algo_history_complete = algo_history_quality.get("complete_for_window") is True
        archive.record_algo_history_scan(
            algo_history_rows,
            started_ms=algo_history_started_ms,
            observed_ms=fetch_server_time_ms(client),
            completed_ms=_now_ms(),
            complete=algo_history_complete,
            details=algo_history_quality,
        )
        algo_history_count = len(algo_history_rows)

    summary = {
        "event": "order_archive_cycle",
        "account": public_account,
        "pending": pending_result.pending_count,
        "disappeared": len(pending_result.disappeared),
        "algo_pending": algo_pending_result.pending_count,
        "algo_disappeared": len(algo_pending_result.disappeared),
        "algo_pending_complete": algo_pending_quality.get("complete") is True,
        "detail_requested": len(detail_requested),
        "detail_errors": detail_error_count,
        "algo_detail_requested": len(algo_detail_requested),
        "algo_detail_errors": algo_detail_error_count,
        "history_ran": history_ran,
        "history_complete": history_complete,
        "history_orders": history_count,
        "algo_history_ran": algo_history_ran,
        "algo_history_complete": algo_history_complete,
        "algo_history_orders": algo_history_count,
        "archive": _status_dict(archive),
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")), flush=True)
    return 0


def _run_status(args: argparse.Namespace) -> int:
    fingerprint, environment = load_archive_identity(args.database)
    archive = OrderArchive(
        args.database,
        account_fingerprint=fingerprint,
        environment=environment,
    )
    print(json.dumps(_status_dict(archive), ensure_ascii=False, indent=2))
    return 0


def _run_import(args: argparse.Namespace) -> int:
    root = args.root.expanduser().resolve()
    paths = sorted((root / "data").glob("*/raw.json"))
    if not paths:
        paths = sorted(root.glob("*/raw.json"))
    if not paths:
        raise ValueError(f"{root} 下找不到 data/*/raw.json")
    fingerprint, environment = discover_raw_identity(paths)
    archive = OrderArchive(
        args.database,
        account_fingerprint=fingerprint,
        environment=environment,
    )
    imported_rows = 0
    imported_files = 0
    for path in paths:
        count = archive.import_raw_snapshot(path)
        if count:
            imported_files += 1
            imported_rows += count
    result = {
        "raw_files_seen": len(paths),
        "raw_files_imported": imported_files,
        "order_rows_imported": imported_rows,
        "archive": _status_dict(archive),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    lock_handle = None
    try:
        if args.command in {"once", "import-raw"}:
            lock_handle = _acquire_lock(args.database)
        if args.command == "once":
            return _run_once(args)
        if args.command == "status":
            return _run_status(args)
        if args.command == "import-raw":
            return _run_import(args)
        raise AssertionError("unreachable")
    except (ConfigurationError, OKXAPIError, ConnectionError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if lock_handle is not None:
            lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
