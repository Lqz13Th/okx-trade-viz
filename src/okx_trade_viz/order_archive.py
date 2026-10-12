from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .storage import private_directory


SCHEMA_VERSION = 2
TERMINAL_ORDER_STATES = frozenset({"canceled", "mmp_canceled", "filled"})
TERMINAL_ALGO_ORDER_STATES = frozenset({"effective", "canceled", "order_failed"})
SOURCE_PRIORITY = {
    "raw_backfill": 1,
    "pending": 2,
    "algo_pending": 2,
    "history": 3,
    "algo_history": 3,
    "detail": 4,
    "algo_detail": 4,
}


@dataclass(frozen=True)
class PendingScanResult:
    run_id: int
    pending_count: int
    disappeared: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class AlgoPendingScanResult:
    run_id: int
    pending_count: int
    disappeared: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class ArchiveStatus:
    path: Path
    order_count: int
    terminal_count: int
    pending_count: int
    unresolved_count: int
    revision_count: int
    pending_scan_count: int
    last_pending_complete_ms: int | None
    last_history_complete_ms: int | None
    algo_order_count: int
    algo_terminal_count: int
    algo_pending_count: int
    algo_unresolved_count: int
    algo_revision_count: int
    algo_pending_scan_count: int
    last_algo_pending_complete_ms: int | None
    last_algo_history_complete_ms: int | None


def _canonical_payload(row: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(row),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _payload_hash(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


def _optional_ms(value: Any) -> int | None:
    text = _nonempty(value)
    if not text:
        return None
    if not text.isdigit():
        raise ValueError(f"无效毫秒时间戳：{text}")
    parsed = int(text)
    if parsed <= 0:
        raise ValueError(f"无效毫秒时间戳：{text}")
    return parsed


def _order_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    inst_id = _nonempty(row.get("instId"))
    order_id = _nonempty(row.get("ordId"))
    if not inst_id or not order_id:
        raise ValueError("订单缺少 instId 或 ordId")
    return inst_id, order_id


def _algo_order_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    inst_id = _nonempty(row.get("instId"))
    algo_id = _nonempty(row.get("algoId"))
    if not inst_id or not algo_id:
        raise ValueError("Algo 订单缺少 instId 或 algoId")
    return inst_id, algo_id


def _observed_ms_from_metadata(metadata: Mapping[str, Any]) -> int:
    collected_at = _nonempty(metadata.get("collected_at"))
    if collected_at:
        try:
            parsed = datetime.fromisoformat(collected_at)
        except ValueError:
            parsed = None
        if parsed is not None and parsed.tzinfo is not None:
            return int(parsed.timestamp() * 1000)
    end_ms = _optional_ms(metadata.get("end_ms"))
    if end_ms is None:
        raise ValueError("raw.json metadata 缺少 collected_at/end_ms")
    return end_ms


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS archive_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    version_row = connection.execute(
        "SELECT value FROM archive_meta WHERE key = 'schema_version'"
    ).fetchone()
    existing_version = int(version_row["value"]) if version_row is not None else None
    if existing_version not in {None, 1, SCHEMA_VERSION}:
        raise ValueError(f"不支持的订单归档版本 {existing_version}，当前只支持 {SCHEMA_VERSION}")

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            started_ms INTEGER NOT NULL,
            observed_ms INTEGER,
            completed_ms INTEGER NOT NULL,
            complete INTEGER NOT NULL,
            order_count INTEGER NOT NULL DEFAULT 0,
            disappeared_count INTEGER NOT NULL DEFAULT 0,
            error_json TEXT,
            details_json TEXT
        );

        CREATE INDEX IF NOT EXISTS sync_runs_kind_completed
            ON sync_runs(kind, complete, completed_ms);

        CREATE TABLE IF NOT EXISTS orders (
            inst_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            inst_type TEXT NOT NULL DEFAULT '',
            client_order_id TEXT NOT NULL DEFAULT '',
            created_ms INTEGER,
            updated_ms INTEGER,
            exchange_state TEXT NOT NULL DEFAULT '',
            terminal INTEGER NOT NULL DEFAULT 0,
            terminal_ms INTEGER,
            first_seen_ms INTEGER NOT NULL,
            last_seen_ms INTEGER NOT NULL,
            last_pending_seen_ms INTEGER,
            is_pending INTEGER NOT NULL DEFAULT 0,
            missing_since_ms INTEGER,
            reconcile_attempts INTEGER NOT NULL DEFAULT 0,
            next_reconcile_ms INTEGER,
            last_reconcile_error TEXT,
            latest_source TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (inst_id, order_id)
        );

        CREATE INDEX IF NOT EXISTS orders_state
            ON orders(terminal, is_pending, exchange_state);

        CREATE INDEX IF NOT EXISTS orders_reconcile_due
            ON orders(next_reconcile_ms, missing_since_ms)
            WHERE terminal = 0 AND missing_since_ms IS NOT NULL;

        CREATE TABLE IF NOT EXISTS order_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inst_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            first_observed_ms INTEGER NOT NULL,
            last_observed_ms INTEGER NOT NULL,
            seen_count INTEGER NOT NULL DEFAULT 1,
            source TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '',
            updated_ms INTEGER,
            payload_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            first_run_id INTEGER REFERENCES sync_runs(id),
            last_run_id INTEGER REFERENCES sync_runs(id),
            UNIQUE (inst_id, order_id, payload_hash),
            FOREIGN KEY (inst_id, order_id) REFERENCES orders(inst_id, order_id)
        );

        CREATE INDEX IF NOT EXISTS revisions_observed
            ON order_revisions(first_observed_ms, inst_id, order_id);

        CREATE TABLE IF NOT EXISTS pending_membership (
            run_id INTEGER NOT NULL REFERENCES sync_runs(id),
            inst_id TEXT NOT NULL,
            order_id TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            PRIMARY KEY (run_id, inst_id, order_id),
            FOREIGN KEY (inst_id, order_id) REFERENCES orders(inst_id, order_id)
        );

        CREATE TABLE IF NOT EXISTS algo_orders (
            inst_id TEXT NOT NULL,
            algo_id TEXT NOT NULL,
            inst_type TEXT NOT NULL DEFAULT '',
            algo_client_order_id TEXT NOT NULL DEFAULT '',
            order_id TEXT NOT NULL DEFAULT '',
            order_type TEXT NOT NULL DEFAULT '',
            created_ms INTEGER,
            updated_ms INTEGER,
            exchange_state TEXT NOT NULL DEFAULT '',
            terminal INTEGER NOT NULL DEFAULT 0,
            terminal_ms INTEGER,
            first_seen_ms INTEGER NOT NULL,
            last_seen_ms INTEGER NOT NULL,
            last_pending_seen_ms INTEGER,
            is_pending INTEGER NOT NULL DEFAULT 0,
            missing_since_ms INTEGER,
            reconcile_attempts INTEGER NOT NULL DEFAULT 0,
            next_reconcile_ms INTEGER,
            last_reconcile_error TEXT,
            latest_source TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            PRIMARY KEY (inst_id, algo_id)
        );

        CREATE INDEX IF NOT EXISTS algo_orders_state
            ON algo_orders(terminal, is_pending, exchange_state);

        CREATE INDEX IF NOT EXISTS algo_orders_created
            ON algo_orders(created_ms, inst_id, algo_id);

        CREATE TABLE IF NOT EXISTS algo_order_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inst_id TEXT NOT NULL,
            algo_id TEXT NOT NULL,
            first_observed_ms INTEGER NOT NULL,
            last_observed_ms INTEGER NOT NULL,
            seen_count INTEGER NOT NULL DEFAULT 1,
            source TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT '',
            updated_ms INTEGER,
            payload_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            first_run_id INTEGER REFERENCES sync_runs(id),
            last_run_id INTEGER REFERENCES sync_runs(id),
            UNIQUE (inst_id, algo_id, payload_hash),
            FOREIGN KEY (inst_id, algo_id)
                REFERENCES algo_orders(inst_id, algo_id)
        );

        CREATE INDEX IF NOT EXISTS algo_revisions_observed
            ON algo_order_revisions(first_observed_ms, inst_id, algo_id);
        """
    )
    algo_columns = {
        str(row["name"]) for row in connection.execute("PRAGMA table_info(algo_orders)")
    }
    algo_reconciliation_columns = {
        "reconcile_attempts": "INTEGER NOT NULL DEFAULT 0",
        "next_reconcile_ms": "INTEGER",
        "last_reconcile_error": "TEXT",
    }
    for name, declaration in algo_reconciliation_columns.items():
        if name not in algo_columns:
            connection.execute(f"ALTER TABLE algo_orders ADD COLUMN {name} {declaration}")
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS algo_orders_reconcile_due
        ON algo_orders(next_reconcile_ms, missing_since_ms)
        WHERE terminal = 0 AND missing_since_ms IS NOT NULL
        """
    )
    if existing_version is None:
        connection.execute(
            "INSERT INTO archive_meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
    elif existing_version < SCHEMA_VERSION:
        connection.execute(
            "UPDATE archive_meta SET value = ? WHERE key = 'schema_version'",
            (str(SCHEMA_VERSION),),
        )


def _connect(path: Path) -> sqlite3.Connection:
    path = path.expanduser().resolve()
    private_directory(path.parent)
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    path.chmod(0o600)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = DELETE")
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute("PRAGMA busy_timeout = 30000")
    _create_schema(connection)
    return connection


def _set_identity(
    connection: sqlite3.Connection,
    *,
    account_fingerprint: str,
    environment: str,
) -> None:
    values = {
        "account_fingerprint": account_fingerprint,
        "environment": environment.upper(),
    }
    if not account_fingerprint:
        raise ValueError("账户指纹不能为空")
    if values["environment"] not in {"LIVE", "DEMO"}:
        raise ValueError("environment 必须为 LIVE 或 DEMO")
    for key, value in values.items():
        existing = connection.execute(
            "SELECT value FROM archive_meta WHERE key = ?", (key,)
        ).fetchone()
        if existing is None:
            connection.execute("INSERT INTO archive_meta(key, value) VALUES(?, ?)", (key, value))
        elif str(existing["value"]) != value:
            raise ValueError(f"订单归档 {key} 与当前账户不一致，拒绝混写")


def _insert_run(
    connection: sqlite3.Connection,
    *,
    kind: str,
    started_ms: int,
    observed_ms: int | None,
    completed_ms: int,
    complete: bool,
    order_count: int,
    disappeared_count: int = 0,
    error: Mapping[str, Any] | None = None,
    details: Mapping[str, Any] | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO sync_runs(
            kind, started_ms, observed_ms, completed_ms, complete,
            order_count, disappeared_count, error_json, details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            kind,
            started_ms,
            observed_ms,
            completed_ms,
            int(complete),
            order_count,
            disappeared_count,
            _canonical_payload(error) if error is not None else None,
            _canonical_payload(details) if details is not None else None,
        ),
    )
    return int(cursor.lastrowid)


def _source_priority(source: str) -> int:
    return SOURCE_PRIORITY.get(source, 0)


def _upsert_order(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    observed_ms: int,
    source: str,
    run_id: int,
    pending: bool,
) -> str:
    inst_id, order_id = _order_identity(row)
    payload = _canonical_payload(row)
    payload_hash = _payload_hash(payload)
    state = _nonempty(row.get("state")).lower()
    terminal = state in TERMINAL_ORDER_STATES
    created_ms = _optional_ms(row.get("cTime"))
    updated_ms = _optional_ms(row.get("uTime"))
    inst_type = _nonempty(row.get("instType"))
    client_order_id = _nonempty(row.get("clOrdId"))
    existing = connection.execute(
        "SELECT * FROM orders WHERE inst_id = ? AND order_id = ?",
        (inst_id, order_id),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO orders(
                inst_id, order_id, inst_type, client_order_id,
                created_ms, updated_ms, exchange_state, terminal, terminal_ms,
                first_seen_ms, last_seen_ms, last_pending_seen_ms, is_pending,
                missing_since_ms, reconcile_attempts, next_reconcile_ms,
                last_reconcile_error, latest_source, payload_hash, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL, NULL, ?, ?, ?)
            """,
            (
                inst_id,
                order_id,
                inst_type,
                client_order_id,
                created_ms,
                updated_ms,
                state,
                int(terminal),
                (updated_ms or observed_ms) if terminal else None,
                observed_ms,
                observed_ms,
                observed_ms if pending else None,
                int(pending and not terminal),
                source,
                payload_hash,
                payload,
            ),
        )
        existing_terminal = False
        should_replace = True
    else:
        existing_terminal = bool(existing["terminal"])
        existing_inst_type = str(existing["inst_type"] or "")
        if inst_type and existing_inst_type and inst_type != existing_inst_type:
            raise ValueError(f"订单 {order_id} 的 instType 发生冲突")
        existing_created_ms = existing["created_ms"]
        if created_ms and existing_created_ms and created_ms != int(existing_created_ms):
            raise ValueError(f"订单 {order_id} 的 cTime 发生冲突")

        existing_updated_ms = int(existing["updated_ms"] or 0)
        incoming_updated_ms = int(updated_ms or 0)
        should_replace = False
        if terminal and not existing_terminal:
            should_replace = True
        elif existing_terminal and not terminal:
            should_replace = False
        elif incoming_updated_ms > existing_updated_ms:
            should_replace = True
        elif incoming_updated_ms == existing_updated_ms:
            should_replace = _source_priority(source) >= _source_priority(
                str(existing["latest_source"])
            )

        updates: dict[str, Any] = {
            "last_seen_ms": max(int(existing["last_seen_ms"]), observed_ms),
            "first_seen_ms": min(int(existing["first_seen_ms"]), observed_ms),
        }
        if pending:
            updates["last_pending_seen_ms"] = max(
                int(existing["last_pending_seen_ms"] or 0), observed_ms
            )
            if not existing_terminal and not terminal:
                updates["is_pending"] = 1
                updates["missing_since_ms"] = None
                updates["next_reconcile_ms"] = None
                updates["last_reconcile_error"] = None
        if terminal:
            updates.update(
                {
                    "terminal": 1,
                    "terminal_ms": updated_ms or observed_ms,
                    "is_pending": 0,
                    "missing_since_ms": None,
                    "next_reconcile_ms": None,
                    "last_reconcile_error": None,
                }
            )
        if should_replace:
            updates.update(
                {
                    "inst_type": inst_type or existing_inst_type,
                    "client_order_id": client_order_id or str(existing["client_order_id"] or ""),
                    "created_ms": created_ms or existing_created_ms,
                    "updated_ms": updated_ms or existing["updated_ms"],
                    "exchange_state": state,
                    "latest_source": source,
                    "payload_hash": payload_hash,
                    "payload_json": payload,
                }
            )
        assignments = ", ".join(f"{key} = ?" for key in updates)
        connection.execute(
            f"UPDATE orders SET {assignments} WHERE inst_id = ? AND order_id = ?",
            (*updates.values(), inst_id, order_id),
        )

    revision = connection.execute(
        """
        SELECT id, last_observed_ms, seen_count
        FROM order_revisions
        WHERE inst_id = ? AND order_id = ? AND payload_hash = ?
        """,
        (inst_id, order_id, payload_hash),
    ).fetchone()
    if revision is None:
        connection.execute(
            """
            INSERT INTO order_revisions(
                inst_id, order_id, first_observed_ms, last_observed_ms,
                seen_count, source, state, updated_ms, payload_hash, payload_json,
                first_run_id, last_run_id
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                inst_id,
                order_id,
                observed_ms,
                observed_ms,
                source,
                state,
                updated_ms,
                payload_hash,
                payload,
                run_id,
                run_id,
            ),
        )
    else:
        connection.execute(
            """
            UPDATE order_revisions
            SET last_observed_ms = ?, seen_count = ?, last_run_id = ?
            WHERE id = ?
            """,
            (
                max(int(revision["last_observed_ms"]), observed_ms),
                int(revision["seen_count"]) + 1,
                run_id,
                int(revision["id"]),
            ),
        )
    return payload_hash


def _upsert_algo_order(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    observed_ms: int,
    source: str,
    run_id: int,
    pending: bool,
) -> str:
    inst_id, algo_id = _algo_order_identity(row)
    payload = _canonical_payload(row)
    payload_hash = _payload_hash(payload)
    state = _nonempty(row.get("state")).lower()
    terminal = state in TERMINAL_ALGO_ORDER_STATES
    created_ms = _optional_ms(row.get("cTime"))
    updated_ms = _optional_ms(row.get("uTime"))
    inst_type = _nonempty(row.get("instType"))
    algo_client_order_id = _nonempty(row.get("algoClOrdId"))
    order_id = _nonempty(row.get("ordId"))
    order_type = _nonempty(row.get("ordType"))
    existing = connection.execute(
        "SELECT * FROM algo_orders WHERE inst_id = ? AND algo_id = ?",
        (inst_id, algo_id),
    ).fetchone()

    if existing is None:
        connection.execute(
            """
            INSERT INTO algo_orders(
                inst_id, algo_id, inst_type, algo_client_order_id, order_id,
                order_type, created_ms, updated_ms, exchange_state, terminal,
                terminal_ms, first_seen_ms, last_seen_ms, last_pending_seen_ms,
                is_pending, missing_since_ms, latest_source, payload_hash,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                inst_id,
                algo_id,
                inst_type,
                algo_client_order_id,
                order_id,
                order_type,
                created_ms,
                updated_ms,
                state,
                int(terminal),
                (updated_ms or observed_ms) if terminal else None,
                observed_ms,
                observed_ms,
                observed_ms if pending else None,
                int(pending and not terminal),
                source,
                payload_hash,
                payload,
            ),
        )
    else:
        existing_terminal = bool(existing["terminal"])
        existing_inst_type = str(existing["inst_type"] or "")
        if inst_type and existing_inst_type and inst_type != existing_inst_type:
            raise ValueError(f"Algo 订单 {algo_id} 的 instType 发生冲突")
        existing_created_ms = existing["created_ms"]
        if created_ms and existing_created_ms and created_ms != int(existing_created_ms):
            raise ValueError(f"Algo 订单 {algo_id} 的 cTime 发生冲突")

        existing_updated_ms = int(existing["updated_ms"] or 0)
        incoming_updated_ms = int(updated_ms or 0)
        should_replace = False
        if terminal and not existing_terminal:
            should_replace = True
        elif existing_terminal and not terminal:
            should_replace = False
        elif incoming_updated_ms > existing_updated_ms:
            should_replace = True
        elif incoming_updated_ms == existing_updated_ms:
            should_replace = _source_priority(source) >= _source_priority(
                str(existing["latest_source"])
            )

        updates: dict[str, Any] = {
            "last_seen_ms": max(int(existing["last_seen_ms"]), observed_ms),
            "first_seen_ms": min(int(existing["first_seen_ms"]), observed_ms),
        }
        if pending:
            updates["last_pending_seen_ms"] = max(
                int(existing["last_pending_seen_ms"] or 0), observed_ms
            )
            if not existing_terminal and not terminal:
                updates["is_pending"] = 1
                updates["missing_since_ms"] = None
                updates["next_reconcile_ms"] = None
                updates["last_reconcile_error"] = None
        if terminal:
            updates.update(
                {
                    "terminal": 1,
                    "is_pending": 0,
                    "missing_since_ms": None,
                    "next_reconcile_ms": None,
                    "last_reconcile_error": None,
                }
            )
            if not existing_terminal:
                updates["terminal_ms"] = updated_ms or observed_ms
            elif updated_ms is not None:
                updates["terminal_ms"] = updated_ms
        if should_replace:
            updates.update(
                {
                    "inst_type": inst_type or existing_inst_type,
                    "algo_client_order_id": algo_client_order_id
                    or str(existing["algo_client_order_id"] or ""),
                    "order_id": order_id or str(existing["order_id"] or ""),
                    "order_type": order_type or str(existing["order_type"] or ""),
                    "created_ms": created_ms or existing_created_ms,
                    "updated_ms": updated_ms or existing["updated_ms"],
                    "exchange_state": state,
                    "latest_source": source,
                    "payload_hash": payload_hash,
                    "payload_json": payload,
                }
            )
        assignments = ", ".join(f"{key} = ?" for key in updates)
        connection.execute(
            f"UPDATE algo_orders SET {assignments} WHERE inst_id = ? AND algo_id = ?",
            (*updates.values(), inst_id, algo_id),
        )

    revision = connection.execute(
        """
        SELECT id, last_observed_ms, seen_count
        FROM algo_order_revisions
        WHERE inst_id = ? AND algo_id = ? AND payload_hash = ?
        """,
        (inst_id, algo_id, payload_hash),
    ).fetchone()
    if revision is None:
        connection.execute(
            """
            INSERT INTO algo_order_revisions(
                inst_id, algo_id, first_observed_ms, last_observed_ms,
                seen_count, source, state, updated_ms, payload_hash, payload_json,
                first_run_id, last_run_id
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                inst_id,
                algo_id,
                observed_ms,
                observed_ms,
                source,
                state,
                updated_ms,
                payload_hash,
                payload,
                run_id,
                run_id,
            ),
        )
    else:
        connection.execute(
            """
            UPDATE algo_order_revisions
            SET last_observed_ms = ?, seen_count = ?, last_run_id = ?
            WHERE id = ?
            """,
            (
                max(int(revision["last_observed_ms"]), observed_ms),
                int(revision["seen_count"]) + 1,
                run_id,
                int(revision["id"]),
            ),
        )
    return payload_hash


class OrderArchive:
    def __init__(
        self,
        path: Path,
        *,
        account_fingerprint: str,
        environment: str,
    ):
        self.path = path.expanduser().resolve()
        self.account_fingerprint = account_fingerprint
        self.environment = environment.upper()
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            _set_identity(
                connection,
                account_fingerprint=self.account_fingerprint,
                environment=self.environment,
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        self.path.chmod(0o600)

    def record_failure(
        self,
        *,
        kind: str,
        started_ms: int,
        completed_ms: int,
        error: Mapping[str, Any],
        observed_ms: int | None = None,
    ) -> int:
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run_id = _insert_run(
                connection,
                kind=kind,
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=False,
                order_count=0,
                error=error,
            )
            connection.commit()
            return run_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def record_pending_scan(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        started_ms: int,
        observed_ms: int,
        completed_ms: int,
    ) -> PendingScanResult:
        identities = [_order_identity(row) for row in rows]
        if len(set(identities)) != len(identities):
            raise ValueError("pending 快照含重复 ordId")
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            previously_pending = {
                (str(row["inst_id"]), str(row["order_id"]))
                for row in connection.execute(
                    "SELECT inst_id, order_id FROM orders WHERE is_pending = 1"
                )
            }
            run_id = _insert_run(
                connection,
                kind="pending",
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=True,
                order_count=len(rows),
            )
            for row in rows:
                inst_id, order_id = _order_identity(row)
                payload_hash = _upsert_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="pending",
                    run_id=run_id,
                    pending=True,
                )
                connection.execute(
                    """
                    INSERT INTO pending_membership(run_id, inst_id, order_id, payload_hash)
                    VALUES (?, ?, ?, ?)
                    """,
                    (run_id, inst_id, order_id, payload_hash),
                )

            current = set(identities)
            disappeared = tuple(sorted(previously_pending - current))
            for inst_id, order_id in disappeared:
                connection.execute(
                    """
                    UPDATE orders
                    SET is_pending = 0,
                        missing_since_ms = COALESCE(missing_since_ms, ?),
                        next_reconcile_ms = COALESCE(next_reconcile_ms, ?)
                    WHERE inst_id = ? AND order_id = ? AND terminal = 0
                    """,
                    (observed_ms, observed_ms, inst_id, order_id),
                )
            connection.execute(
                "UPDATE sync_runs SET disappeared_count = ? WHERE id = ?",
                (len(disappeared), run_id),
            )
            connection.execute(
                """
                INSERT INTO archive_meta(key, value)
                VALUES('last_pending_complete_ms', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(observed_ms),),
            )
            connection.commit()
            return PendingScanResult(
                run_id=run_id,
                pending_count=len(rows),
                disappeared=disappeared,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def record_algo_pending_scan(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        started_ms: int,
        observed_ms: int,
        completed_ms: int,
    ) -> AlgoPendingScanResult:
        identities = [_algo_order_identity(row) for row in rows]
        if len(set(identities)) != len(identities):
            raise ValueError("Algo pending 快照含重复 algoId")
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            previously_pending = {
                (str(row["inst_id"]), str(row["algo_id"]))
                for row in connection.execute(
                    "SELECT inst_id, algo_id FROM algo_orders WHERE is_pending = 1"
                )
            }
            run_id = _insert_run(
                connection,
                kind="algo_pending",
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=True,
                order_count=len(rows),
            )
            for row in rows:
                _upsert_algo_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="algo_pending",
                    run_id=run_id,
                    pending=True,
                )

            current = set(identities)
            disappeared = tuple(sorted(previously_pending - current))
            for inst_id, algo_id in disappeared:
                connection.execute(
                    """
                    UPDATE algo_orders
                    SET is_pending = 0,
                        missing_since_ms = COALESCE(missing_since_ms, ?),
                        next_reconcile_ms = COALESCE(next_reconcile_ms, ?)
                    WHERE inst_id = ? AND algo_id = ? AND terminal = 0
                    """,
                    (observed_ms, observed_ms, inst_id, algo_id),
                )
            connection.execute(
                "UPDATE sync_runs SET disappeared_count = ? WHERE id = ?",
                (len(disappeared), run_id),
            )
            connection.execute(
                """
                INSERT INTO archive_meta(key, value)
                VALUES('last_algo_pending_complete_ms', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (str(observed_ms),),
            )
            connection.commit()
            return AlgoPendingScanResult(
                run_id=run_id,
                pending_count=len(rows),
                disappeared=disappeared,
            )
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def due_algo_reconciliations(self, *, observed_ms: int, limit: int) -> list[tuple[str, str]]:
        if limit <= 0:
            return []
        connection = _connect(self.path)
        try:
            return [
                (str(row["inst_id"]), str(row["algo_id"]))
                for row in connection.execute(
                    """
                    SELECT inst_id, algo_id
                    FROM algo_orders
                    WHERE terminal = 0
                      AND missing_since_ms IS NOT NULL
                      AND (next_reconcile_ms IS NULL OR next_reconcile_ms <= ?)
                    ORDER BY COALESCE(next_reconcile_ms, 0), missing_since_ms, algo_id
                    LIMIT ?
                    """,
                    (observed_ms, limit),
                )
            ]
        finally:
            connection.close()

    def record_algo_detail_scan(
        self,
        rows: Sequence[Mapping[str, Any]],
        errors: Mapping[tuple[str, str], Mapping[str, Any]],
        *,
        requested: Sequence[tuple[str, str]],
        started_ms: int,
        observed_ms: int,
        completed_ms: int,
    ) -> int:
        returned = {_algo_order_identity(row) for row in rows}
        requested_set = set(requested)
        if not returned <= requested_set:
            raise ValueError("Algo detail 返回了未请求的订单")
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            complete = not errors and returned == requested_set
            run_id = _insert_run(
                connection,
                kind="algo_detail",
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=complete,
                order_count=len(rows),
                error={
                    f"{inst_id}|{algo_id}": dict(value)
                    for (inst_id, algo_id), value in errors.items()
                }
                if errors
                else None,
                details={"requested_count": len(requested)},
            )
            terminal_returned: set[tuple[str, str]] = set()
            for row in rows:
                identity = _algo_order_identity(row)
                _upsert_algo_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="algo_detail",
                    run_id=run_id,
                    pending=False,
                )
                if _nonempty(row.get("state")).lower() in TERMINAL_ALGO_ORDER_STATES:
                    terminal_returned.add(identity)

            unresolved = requested_set - terminal_returned
            for inst_id, algo_id in unresolved:
                existing = connection.execute(
                    """
                    SELECT reconcile_attempts
                    FROM algo_orders WHERE inst_id = ? AND algo_id = ?
                    """,
                    (inst_id, algo_id),
                ).fetchone()
                if existing is None:
                    continue
                attempts = int(existing["reconcile_attempts"]) + 1
                error = errors.get((inst_id, algo_id))
                message = _canonical_payload(error) if error is not None else "non_terminal"
                connection.execute(
                    """
                    UPDATE algo_orders
                    SET reconcile_attempts = ?, next_reconcile_ms = ?,
                        last_reconcile_error = ?
                    WHERE inst_id = ? AND algo_id = ? AND terminal = 0
                    """,
                    (
                        attempts,
                        observed_ms + self._retry_delay_ms(attempts),
                        message,
                        inst_id,
                        algo_id,
                    ),
                )
            connection.commit()
            return run_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def due_reconciliations(self, *, observed_ms: int, limit: int) -> list[tuple[str, str]]:
        if limit <= 0:
            return []
        connection = _connect(self.path)
        try:
            return [
                (str(row["inst_id"]), str(row["order_id"]))
                for row in connection.execute(
                    """
                    SELECT inst_id, order_id
                    FROM orders
                    WHERE terminal = 0
                      AND missing_since_ms IS NOT NULL
                      AND (next_reconcile_ms IS NULL OR next_reconcile_ms <= ?)
                    ORDER BY COALESCE(next_reconcile_ms, 0), missing_since_ms, order_id
                    LIMIT ?
                    """,
                    (observed_ms, limit),
                )
            ]
        finally:
            connection.close()

    @staticmethod
    def _retry_delay_ms(attempts: int) -> int:
        delays = (60, 120, 300, 900, 3600)
        return delays[min(max(attempts - 1, 0), len(delays) - 1)] * 1000

    def record_detail_scan(
        self,
        rows: Sequence[Mapping[str, Any]],
        errors: Mapping[tuple[str, str], Mapping[str, Any]],
        *,
        requested: Sequence[tuple[str, str]],
        started_ms: int,
        observed_ms: int,
        completed_ms: int,
    ) -> int:
        returned = {_order_identity(row) for row in rows}
        requested_set = set(requested)
        if not returned <= requested_set:
            raise ValueError("detail 返回了未请求的订单")
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            complete = not errors and returned == requested_set
            run_id = _insert_run(
                connection,
                kind="detail",
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=complete,
                order_count=len(rows),
                error={
                    f"{inst_id}|{order_id}": dict(value)
                    for (inst_id, order_id), value in errors.items()
                }
                if errors
                else None,
                details={"requested_count": len(requested)},
            )
            terminal_returned: set[tuple[str, str]] = set()
            for row in rows:
                identity = _order_identity(row)
                _upsert_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="detail",
                    run_id=run_id,
                    pending=False,
                )
                if _nonempty(row.get("state")).lower() in TERMINAL_ORDER_STATES:
                    terminal_returned.add(identity)

            unresolved = requested_set - terminal_returned
            for inst_id, order_id in unresolved:
                existing = connection.execute(
                    """
                    SELECT reconcile_attempts
                    FROM orders WHERE inst_id = ? AND order_id = ?
                    """,
                    (inst_id, order_id),
                ).fetchone()
                if existing is None:
                    continue
                attempts = int(existing["reconcile_attempts"]) + 1
                error = errors.get((inst_id, order_id))
                message = _canonical_payload(error) if error is not None else "non_terminal"
                connection.execute(
                    """
                    UPDATE orders
                    SET reconcile_attempts = ?, next_reconcile_ms = ?,
                        last_reconcile_error = ?
                    WHERE inst_id = ? AND order_id = ? AND terminal = 0
                    """,
                    (
                        attempts,
                        observed_ms + self._retry_delay_ms(attempts),
                        message,
                        inst_id,
                        order_id,
                    ),
                )
            connection.commit()
            return run_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def history_due(self, *, observed_ms: int, interval_ms: int) -> bool:
        connection = _connect(self.path)
        try:
            row = connection.execute(
                "SELECT value FROM archive_meta WHERE key = 'last_history_complete_ms'"
            ).fetchone()
            return row is None or observed_ms - int(row["value"]) >= interval_ms
        finally:
            connection.close()

    def record_history_scan(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        started_ms: int,
        observed_ms: int,
        completed_ms: int,
        complete: bool,
        details: Mapping[str, Any] | None = None,
    ) -> int:
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run_id = _insert_run(
                connection,
                kind="history",
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=complete,
                order_count=len(rows),
                details=details,
            )
            for row in rows:
                _upsert_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="history",
                    run_id=run_id,
                    pending=False,
                )
            if complete:
                connection.execute(
                    """
                    INSERT INTO archive_meta(key, value)
                    VALUES('last_history_complete_ms', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (str(observed_ms),),
                )
            connection.commit()
            return run_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def algo_history_due(self, *, observed_ms: int, interval_ms: int) -> bool:
        connection = _connect(self.path)
        try:
            row = connection.execute(
                "SELECT value FROM archive_meta WHERE key = 'last_algo_history_complete_ms'"
            ).fetchone()
            return row is None or observed_ms - int(row["value"]) >= interval_ms
        finally:
            connection.close()

    def record_algo_history_scan(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        started_ms: int,
        observed_ms: int,
        completed_ms: int,
        complete: bool,
        details: Mapping[str, Any] | None = None,
    ) -> int:
        identities = [_algo_order_identity(row) for row in rows]
        if len(set(identities)) != len(identities):
            raise ValueError("Algo history 快照含重复 algoId")
        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            run_id = _insert_run(
                connection,
                kind="algo_history",
                started_ms=started_ms,
                observed_ms=observed_ms,
                completed_ms=completed_ms,
                complete=complete,
                order_count=len(rows),
                details=details,
            )
            for row in rows:
                _upsert_algo_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="algo_history",
                    run_id=run_id,
                    pending=False,
                )
            if complete:
                connection.execute(
                    """
                    INSERT INTO archive_meta(key, value)
                    VALUES('last_algo_history_complete_ms', ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (str(observed_ms),),
                )
            connection.commit()
            return run_id
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def load_algo_orders(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        if start_ms is not None and start_ms < 0:
            raise ValueError("start_ms 不能小于 0")
        if end_ms is not None and end_ms <= 0:
            raise ValueError("end_ms 必须大于 0")
        if start_ms is not None and end_ms is not None and start_ms >= end_ms:
            raise ValueError("start_ms 必须小于 end_ms")

        clauses: list[str] = []
        parameters: list[int] = []
        event_ms = "COALESCE(created_ms, updated_ms, first_seen_ms)"
        if start_ms is not None:
            clauses.append(f"{event_ms} >= ?")
            parameters.append(start_ms)
        if end_ms is not None:
            clauses.append(f"{event_ms} < ?")
            parameters.append(end_ms)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        connection = _connect(self.path)
        try:
            result: list[dict[str, Any]] = []
            for row in connection.execute(
                f"""
                SELECT payload_json
                FROM algo_orders
                {where}
                ORDER BY {event_ms}, inst_id, algo_id
                """,
                parameters,
            ):
                payload = json.loads(str(row["payload_json"]))
                if not isinstance(payload, dict):
                    raise ValueError("Algo 订单归档 payload 不是对象")
                result.append(payload)
            return result
        finally:
            connection.close()

    def import_raw_snapshot(self, path: Path) -> int:
        path = path.expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError(f"{path} 顶层必须是对象")
        metadata = payload.get("metadata")
        if not isinstance(metadata, Mapping):
            raise ValueError(f"{path} 缺少 metadata")
        environment = _nonempty(metadata.get("environment")).upper()
        fingerprint = _nonempty(metadata.get("account_fingerprint"))
        if environment and environment != self.environment:
            raise ValueError(f"{path} environment 与订单归档不一致")
        if fingerprint and fingerprint != self.account_fingerprint:
            raise ValueError(f"{path} 账户指纹与订单归档不一致")
        observed_ms = _observed_ms_from_metadata(metadata)
        rows: dict[tuple[str, str], Mapping[str, Any]] = {}
        for key in ("orders", "fill_order_details"):
            values = payload.get(key, [])
            if not isinstance(values, list):
                raise ValueError(f"{path} 的 {key} 必须是数组")
            for row in values:
                if not isinstance(row, Mapping):
                    continue
                rows[_order_identity(row)] = row
        algo_rows: dict[tuple[str, str], Mapping[str, Any]] = {}
        raw_algo_rows = payload.get("algo_orders", [])
        if not isinstance(raw_algo_rows, list):
            raise ValueError(f"{path} 的 algo_orders 必须是数组")
        for row in raw_algo_rows:
            if not isinstance(row, Mapping):
                continue
            algo_rows[_algo_order_identity(row)] = row

        connection = _connect(self.path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            imported = connection.execute(
                "SELECT 1 FROM sync_runs WHERE kind = 'raw_import' AND details_json = ?",
                (_canonical_payload({"raw_path": str(path)}),),
            ).fetchone()
            if imported is not None:
                connection.rollback()
                return 0
            run_id = _insert_run(
                connection,
                kind="raw_import",
                started_ms=observed_ms,
                observed_ms=observed_ms,
                completed_ms=observed_ms,
                complete=True,
                order_count=len(rows) + len(algo_rows),
                details={"raw_path": str(path)},
            )
            for row in rows.values():
                _upsert_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="raw_backfill",
                    run_id=run_id,
                    pending=False,
                )
            for row in algo_rows.values():
                _upsert_algo_order(
                    connection,
                    row,
                    observed_ms=observed_ms,
                    source="raw_backfill",
                    run_id=run_id,
                    pending=False,
                )
            connection.commit()
            return len(rows) + len(algo_rows)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def status(self) -> ArchiveStatus:
        connection = _connect(self.path)
        try:
            counts = connection.execute(
                """
                SELECT
                    COUNT(*) AS order_count,
                    COALESCE(SUM(terminal), 0) AS terminal_count,
                    COALESCE(SUM(is_pending), 0) AS pending_count,
                    COALESCE(SUM(
                        CASE WHEN terminal = 0 AND missing_since_ms IS NOT NULL
                             THEN 1 ELSE 0 END
                    ), 0) AS unresolved_count
                FROM orders
                """
            ).fetchone()
            revision_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM order_revisions").fetchone()[
                    "count"
                ]
            )
            pending_scan_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM sync_runs "
                    "WHERE kind = 'pending' AND complete = 1"
                ).fetchone()["count"]
            )
            algo_counts = connection.execute(
                """
                SELECT
                    COUNT(*) AS order_count,
                    COALESCE(SUM(terminal), 0) AS terminal_count,
                    COALESCE(SUM(is_pending), 0) AS pending_count,
                    COALESCE(SUM(
                        CASE WHEN terminal = 0 AND missing_since_ms IS NOT NULL
                             THEN 1 ELSE 0 END
                    ), 0) AS unresolved_count
                FROM algo_orders
                """
            ).fetchone()
            algo_revision_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM algo_order_revisions").fetchone()[
                    "count"
                ]
            )
            algo_pending_scan_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM sync_runs "
                    "WHERE kind = 'algo_pending' AND complete = 1"
                ).fetchone()["count"]
            )

            def meta_ms(key: str) -> int | None:
                row = connection.execute(
                    "SELECT value FROM archive_meta WHERE key = ?", (key,)
                ).fetchone()
                return int(row["value"]) if row is not None else None

            return ArchiveStatus(
                path=self.path,
                order_count=int(counts["order_count"]),
                terminal_count=int(counts["terminal_count"]),
                pending_count=int(counts["pending_count"]),
                unresolved_count=int(counts["unresolved_count"]),
                revision_count=revision_count,
                pending_scan_count=pending_scan_count,
                last_pending_complete_ms=meta_ms("last_pending_complete_ms"),
                last_history_complete_ms=meta_ms("last_history_complete_ms"),
                algo_order_count=int(algo_counts["order_count"]),
                algo_terminal_count=int(algo_counts["terminal_count"]),
                algo_pending_count=int(algo_counts["pending_count"]),
                algo_unresolved_count=int(algo_counts["unresolved_count"]),
                algo_revision_count=algo_revision_count,
                algo_pending_scan_count=algo_pending_scan_count,
                last_algo_pending_complete_ms=meta_ms("last_algo_pending_complete_ms"),
                last_algo_history_complete_ms=meta_ms("last_algo_history_complete_ms"),
            )
        finally:
            connection.close()


def discover_raw_identity(paths: Sequence[Path]) -> tuple[str, str]:
    fingerprints: set[str] = set()
    environments: set[str] = set()
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        metadata = payload.get("metadata") if isinstance(payload, Mapping) else None
        if not isinstance(metadata, Mapping):
            continue
        fingerprint = _nonempty(metadata.get("account_fingerprint"))
        environment = _nonempty(metadata.get("environment")).upper()
        if fingerprint:
            fingerprints.add(fingerprint)
        if environment:
            environments.add(environment)
    if len(fingerprints) != 1:
        raise ValueError("raw 快照必须提供唯一账户指纹")
    if len(environments) != 1:
        raise ValueError("raw 快照必须来自唯一 LIVE/DEMO 环境")
    return next(iter(fingerprints)), next(iter(environments))


def load_archive_identity(path: Path) -> tuple[str, str]:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"找不到订单归档：{path}")
    connection = _connect(path)
    try:
        values = {
            str(row["key"]): str(row["value"])
            for row in connection.execute(
                "SELECT key, value FROM archive_meta "
                "WHERE key IN ('account_fingerprint', 'environment')"
            )
        }
    finally:
        connection.close()
    fingerprint = values.get("account_fingerprint", "")
    environment = values.get("environment", "")
    if not fingerprint or environment not in {"LIVE", "DEMO"}:
        raise ValueError("订单归档缺少账户身份信息")
    return fingerprint, environment


def ensure_private_database(path: Path) -> None:
    path = path.expanduser().resolve()
    if path.exists():
        os.chmod(path, 0o600)
