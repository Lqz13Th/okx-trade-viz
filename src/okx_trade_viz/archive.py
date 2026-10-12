from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from .storage import private_directory


SCHEMA_VERSION = 1
ARCHIVE_FILENAME = "archive.sqlite3"
CURVE_ENTITIES = ("fills", "bills")


@dataclass(frozen=True)
class ArchiveUpdate:
    path: Path
    run_id: int
    inserted: dict[str, int]
    replaced: dict[str, int]
    retired: dict[str, int]
    skipped: tuple[str, ...]


def default_archive_path(root: Path) -> Path:
    return root.expanduser().resolve() / "data" / ARCHIVE_FILENAME


def _connect(path: Path) -> sqlite3.Connection:
    private_directory(path.parent)
    connection = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    path.chmod(0o600)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = DELETE")
    connection.execute("PRAGMA synchronous = FULL")
    _create_schema(connection)
    return connection


def _create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS archive_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS collection_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bundle_hash TEXT NOT NULL UNIQUE,
            source_kind TEXT NOT NULL,
            observed_ms INTEGER NOT NULL,
            collected_at TEXT NOT NULL,
            environment TEXT NOT NULL,
            timezone TEXT NOT NULL,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            raw_path TEXT,
            critical_complete INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS coverage (
            entity TEXT NOT NULL,
            start_ms INTEGER NOT NULL,
            end_ms INTEGER NOT NULL,
            run_id INTEGER NOT NULL REFERENCES collection_runs(id),
            observed_ms INTEGER NOT NULL,
            PRIMARY KEY (entity, run_id)
        );

        CREATE TABLE IF NOT EXISTS fills (
            identity TEXT PRIMARY KEY,
            timestamp_ms INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            observed_ms INTEGER NOT NULL,
            source_run_id INTEGER NOT NULL REFERENCES collection_runs(id),
            active INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS fills_time_active
            ON fills(active, timestamp_ms);

        CREATE TABLE IF NOT EXISTS bills (
            identity TEXT PRIMARY KEY,
            timestamp_ms INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            observed_ms INTEGER NOT NULL,
            source_run_id INTEGER NOT NULL REFERENCES collection_runs(id),
            active INTEGER NOT NULL DEFAULT 1
        );

        CREATE INDEX IF NOT EXISTS bills_time_active
            ON bills(active, timestamp_ms);

        CREATE TABLE IF NOT EXISTS event_revisions (
            entity TEXT NOT NULL,
            identity TEXT NOT NULL,
            observed_ms INTEGER NOT NULL,
            payload_hash TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            source_run_id INTEGER NOT NULL REFERENCES collection_runs(id),
            PRIMARY KEY (entity, identity, observed_ms, payload_hash)
        );

        CREATE TABLE IF NOT EXISTS instruments (
            inst_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            observed_ms INTEGER NOT NULL,
            source_run_id INTEGER NOT NULL REFERENCES collection_runs(id)
        );
        """
    )
    row = connection.execute(
        "SELECT value FROM archive_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        connection.execute(
            "INSERT INTO archive_meta(key, value) VALUES('schema_version', ?)",
            (str(SCHEMA_VERSION),),
        )
    elif int(row["value"]) != SCHEMA_VERSION:
        raise ValueError(
            f"不支持的增量归档版本 {row['value']}，当前只支持 {SCHEMA_VERSION}"
        )


def _metadata(
    bundle: Mapping[str, Any],
) -> tuple[int, int, str, str, str, str, str, int]:
    metadata = bundle.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ValueError("raw bundle 缺少 metadata")
    try:
        start_ms = int(metadata["start_ms"])
        end_ms = int(metadata["end_ms"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("raw bundle 缺少有效 start_ms/end_ms") from exc
    if start_ms >= end_ms:
        raise ValueError("raw bundle start_ms 必须早于 end_ms")
    timezone_name = str(metadata.get("timezone") or "UTC")
    ZoneInfo(timezone_name)
    environment = str(metadata.get("environment") or "UNKNOWN").upper()
    source_kind = str(metadata.get("source_kind") or "")
    if source_kind != "okx_api":
        raise ValueError("增量归档只接受直接来自 OKX API 的完整批次")
    account_fingerprint = str(metadata.get("account_fingerprint") or "").strip()
    if not account_fingerprint:
        raise ValueError("raw bundle 缺少账户指纹，拒绝混入长期归档")
    collected_at = str(metadata.get("collected_at") or "")
    if collected_at:
        try:
            observed_at = datetime.fromisoformat(collected_at)
        except ValueError as exc:
            raise ValueError("metadata.collected_at 不是有效 ISO-8601 时间") from exc
        if observed_at.tzinfo is None:
            raise ValueError("metadata.collected_at 必须带时区")
        observed_ms = int(observed_at.timestamp() * 1000)
    else:
        observed_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        collected_at = datetime.now(timezone.utc).isoformat()
    return (
        start_ms,
        end_ms,
        timezone_name,
        environment,
        source_kind,
        account_fingerprint,
        collected_at,
        observed_ms,
    )


def _quality_complete(bundle: Mapping[str, Any], entity: str) -> bool:
    quality = bundle.get("quality")
    if not isinstance(quality, Mapping):
        return False
    entity_quality = quality.get(entity)
    return (
        isinstance(entity_quality, Mapping)
        and entity_quality.get("complete") is True
        and entity_quality.get("complete_for_window") is True
    )


def _event_timestamp(row: Mapping[str, Any], entity: str) -> int:
    raw = (row.get("fillTime") or row.get("ts")) if entity == "fills" else row.get("ts")
    try:
        return int(str(raw))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{entity} 记录缺少有效事件时间") from exc


def _event_identity(row: Mapping[str, Any], entity: str) -> str:
    bill_id = str(row.get("billId") or "").strip()
    if bill_id:
        return f"billId:{bill_id}"
    raise ValueError(f"{entity} 记录缺少 OKX billId，无法安全增量去重")


def _canonical_payload(row: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(row),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _validate_rows(
    rows: Any,
    *,
    entity: str,
    start_ms: int,
    end_ms: int,
) -> list[tuple[str, int, str]]:
    if not isinstance(rows, list):
        raise ValueError(f"raw bundle 的 {entity} 必须是列表")
    normalized: dict[str, tuple[int, str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError(f"{entity} 记录不是对象")
        timestamp_ms = _event_timestamp(row, entity)
        if not start_ms <= timestamp_ms < end_ms:
            raise ValueError(f"{entity} 记录不在 bundle 的半开时间窗口内")
        identity = _event_identity(row, entity)
        payload = _canonical_payload(row)
        previous = normalized.get(identity)
        if previous is not None and previous != (timestamp_ms, payload):
            raise ValueError(f"同一 API 批次中的 {entity} {identity} 内容冲突")
        normalized[identity] = (timestamp_ms, payload)
    return [
        (identity, timestamp_ms, payload)
        for identity, (timestamp_ms, payload) in sorted(normalized.items())
    ]


def _set_archive_identity(
    connection: sqlite3.Connection,
    *,
    environment: str,
    timezone_name: str,
    account_fingerprint: str,
) -> None:
    for key, value in (
        ("environment", environment),
        ("account_fingerprint", account_fingerprint),
    ):
        row = connection.execute(
            "SELECT value FROM archive_meta WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO archive_meta(key, value) VALUES(?, ?)", (key, value)
            )
        elif row["value"] != value:
            raise ValueError(
                f"增量归档的 {key}={row['value']}，不能混入 {value}"
            )
    connection.execute(
        "INSERT OR IGNORE INTO archive_meta(key, value) VALUES('timezone', ?)",
        (timezone_name,),
    )


def archive_bundle(
    path: Path,
    bundle: Mapping[str, Any],
    *,
    raw_path: Path | None = None,
) -> ArchiveUpdate:
    (
        start_ms,
        end_ms,
        timezone_name,
        environment,
        source_kind,
        account_fingerprint,
        collected_at,
        observed_ms,
    ) = _metadata(bundle)
    quality = bundle.get("quality")
    critical_complete = bool(
        isinstance(quality, Mapping) and quality.get("critical_sources_complete") is True
    )
    if not critical_complete:
        raise ValueError("关键 API 数据源不完整，不推进增量归档")

    complete_entities = [
        entity for entity in CURVE_ENTITIES if _quality_complete(bundle, entity)
    ]
    if set(complete_entities) != set(CURVE_ENTITIES):
        raise ValueError("fills 与 bills 必须同时声明完整，才可推进曲线增量归档")
    skipped = tuple(entity for entity in CURVE_ENTITIES if entity not in complete_entities)
    normalized = {
        entity: _validate_rows(
            bundle.get(entity),
            entity=entity,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        for entity in complete_entities
    }
    bundle_hash = hashlib.sha256(
        _canonical_payload(
            {
                "source_kind": source_kind,
                "account_fingerprint": account_fingerprint,
                "environment": environment,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "observed_ms": observed_ms,
                "events": normalized,
                "instruments": bundle.get("instruments", {}),
            }
        ).encode("utf-8")
    ).hexdigest()

    path = path.expanduser().resolve()
    connection = _connect(path)
    inserted: dict[str, int] = {}
    replaced: dict[str, int] = {}
    retired: dict[str, int] = {}
    try:
        connection.execute("BEGIN IMMEDIATE")
        try:
            _set_archive_identity(
                connection,
                environment=environment,
                timezone_name=timezone_name,
                account_fingerprint=account_fingerprint,
            )
            duplicate = connection.execute(
                "SELECT id FROM collection_runs WHERE bundle_hash = ?",
                (bundle_hash,),
            ).fetchone()
            if duplicate is not None:
                connection.rollback()
                return ArchiveUpdate(
                    path=path,
                    run_id=int(duplicate["id"]),
                    inserted={entity: 0 for entity in complete_entities},
                    replaced={entity: 0 for entity in complete_entities},
                    retired={entity: 0 for entity in complete_entities},
                    skipped=skipped,
                )
            for entity in complete_entities:
                newer = connection.execute(
                    """
                    SELECT MAX(observed_ms) AS observed_ms
                    FROM coverage
                    WHERE entity = ? AND start_ms < ? AND end_ms > ?
                    """,
                    (entity, end_ms, start_ms),
                ).fetchone()
                if newer is not None and newer["observed_ms"] is not None:
                    if int(newer["observed_ms"]) >= observed_ms:
                        raise ValueError(
                            f"拒绝用不更新的 {entity} 快照覆盖较新的重叠 API 区间"
                        )
            cursor = connection.execute(
                """
                INSERT INTO collection_runs(
                    bundle_hash, source_kind, observed_ms, collected_at,
                    environment, timezone,
                    start_ms, end_ms, raw_path, critical_complete
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bundle_hash,
                    source_kind,
                    observed_ms,
                    collected_at,
                    environment,
                    timezone_name,
                    start_ms,
                    end_ms,
                    str(raw_path.expanduser().resolve()) if raw_path else None,
                    1,
                ),
            )
            run_id = int(cursor.lastrowid)

            for entity in complete_entities:
                rows = normalized[entity]
                incoming_ids = {identity for identity, _, _ in rows}
                existing = {
                    row["identity"]: row["payload_json"]
                    for row in connection.execute(
                        f"SELECT identity, payload_json FROM {entity} WHERE active = 1 "
                        "AND timestamp_ms >= ? AND timestamp_ms < ?",
                        (start_ms, end_ms),
                    )
                }
                known = {
                    row["identity"]: row["payload_json"]
                    for row in connection.execute(
                        f"SELECT identity, payload_json FROM {entity}"
                    )
                    if row["identity"] in incoming_ids
                }
                connection.execute(
                    f"UPDATE {entity} SET active = 0 "
                    "WHERE active = 1 AND timestamp_ms >= ? AND timestamp_ms < ? "
                    "AND observed_ms <= ?",
                    (start_ms, end_ms, observed_ms),
                )
                retired[entity] = len(set(existing) - incoming_ids)
                inserted[entity] = sum(identity not in known for identity, _, _ in rows)
                replaced[entity] = sum(
                    identity in known and known[identity] != payload
                    for identity, _, payload in rows
                )
                for identity, timestamp_ms, payload in rows:
                    payload_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO event_revisions(
                            entity, identity, observed_ms, payload_hash,
                            payload_json, source_run_id
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (entity, identity, observed_ms, payload_hash, payload, run_id),
                    )
                    connection.execute(
                        f"""
                        INSERT INTO {entity}(
                            identity, timestamp_ms, payload_json,
                            observed_ms, source_run_id, active
                        ) VALUES (?, ?, ?, ?, ?, 1)
                        ON CONFLICT(identity) DO UPDATE SET
                            timestamp_ms = excluded.timestamp_ms,
                            payload_json = excluded.payload_json,
                            observed_ms = excluded.observed_ms,
                            source_run_id = excluded.source_run_id,
                            active = 1
                        WHERE excluded.observed_ms > {entity}.observed_ms
                        """,
                        (identity, timestamp_ms, payload, observed_ms, run_id),
                    )
                connection.execute(
                    """
                    INSERT INTO coverage(entity, start_ms, end_ms, run_id, observed_ms)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (entity, start_ms, end_ms, run_id, observed_ms),
                )

            instruments = bundle.get("instruments")
            if isinstance(instruments, Mapping):
                for inst_id, row in instruments.items():
                    if not isinstance(row, Mapping):
                        continue
                    payload = _canonical_payload(row)
                    connection.execute(
                        """
                        INSERT INTO instruments(inst_id, payload_json, observed_ms, source_run_id)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(inst_id) DO UPDATE SET
                            payload_json = excluded.payload_json,
                            observed_ms = excluded.observed_ms,
                            source_run_id = excluded.source_run_id
                        WHERE excluded.observed_ms >= instruments.observed_ms
                        """,
                        (str(inst_id), payload, observed_ms, run_id),
                    )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    finally:
        connection.close()
    path.chmod(0o600)
    return ArchiveUpdate(
        path=path,
        run_id=run_id,
        inserted=inserted,
        replaced=replaced,
        retired=retired,
        skipped=skipped,
    )


def _merge_intervals(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for start_ms, end_ms in sorted(intervals):
        if start_ms >= end_ms:
            continue
        if not merged or start_ms > merged[-1][1]:
            merged.append([start_ms, end_ms])
        else:
            merged[-1][1] = max(merged[-1][1], end_ms)
    return [(start_ms, end_ms) for start_ms, end_ms in merged]


def _intersect_intervals(
    left: Sequence[tuple[int, int]],
    right: Sequence[tuple[int, int]],
) -> list[tuple[int, int]]:
    result: list[tuple[int, int]] = []
    left_index = 0
    right_index = 0
    while left_index < len(left) and right_index < len(right):
        start_ms = max(left[left_index][0], right[right_index][0])
        end_ms = min(left[left_index][1], right[right_index][1])
        if start_ms < end_ms:
            result.append((start_ms, end_ms))
        if left[left_index][1] <= right[right_index][1]:
            left_index += 1
        else:
            right_index += 1
    return _merge_intervals(result)


def common_curve_coverage(path: Path) -> list[tuple[int, int]]:
    path = path.expanduser().resolve()
    if not path.is_file():
        return []
    connection = _connect(path)
    try:
        by_entity: dict[str, list[tuple[int, int]]] = {}
        for entity in CURVE_ENTITIES:
            by_entity[entity] = _merge_intervals(
                (int(row["start_ms"]), int(row["end_ms"]))
                for row in connection.execute(
                    "SELECT start_ms, end_ms FROM coverage WHERE entity = ?",
                    (entity,),
                )
            )
        return _intersect_intervals(by_entity["fills"], by_entity["bills"])
    finally:
        connection.close()


def _select_coverage(
    intervals: Sequence[tuple[int, int]],
    *,
    start_ms: int | None,
    end_ms: int | None,
) -> tuple[int, int]:
    if not intervals:
        raise ValueError("增量归档还没有 fills 与 bills 的共同完整覆盖区间")
    if start_ms is None and end_ms is None:
        return max(intervals, key=lambda interval: (interval[1], interval[1] - interval[0]))
    if start_ms is None:
        candidates = [interval for interval in intervals if interval[0] < int(end_ms) <= interval[1]]
        if not candidates:
            raise ValueError("请求终点不在增量归档的完整覆盖区间内")
        interval = max(candidates, key=lambda value: value[0])
        return interval[0], int(end_ms)
    if end_ms is None:
        candidates = [interval for interval in intervals if interval[0] <= start_ms < interval[1]]
        if not candidates:
            raise ValueError("请求起点不在增量归档的完整覆盖区间内")
        interval = max(candidates, key=lambda value: value[1])
        return start_ms, interval[1]
    if start_ms >= end_ms:
        raise ValueError("曲线请求起点必须早于终点")
    if not any(left <= start_ms and end_ms <= right for left, right in intervals):
        raise ValueError("请求窗口跨越增量归档缺口，拒绝生成误导曲线")
    return start_ms, end_ms


def load_curve_bundle(
    path: Path,
    *,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> dict[str, Any]:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"找不到增量归档：{path}")
    coverage = common_curve_coverage(path)
    selected_start, selected_end = _select_coverage(
        coverage,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    connection = _connect(path)
    try:
        meta = {
            row["key"]: row["value"]
            for row in connection.execute("SELECT key, value FROM archive_meta")
        }
        timezone_name = meta.get("timezone", "UTC")
        environment = meta.get("environment", "UNKNOWN")
        zone = ZoneInfo(timezone_name)

        rows_by_entity: dict[str, list[dict[str, Any]]] = {}
        for entity in CURVE_ENTITIES:
            rows_by_entity[entity] = [
                json.loads(row["payload_json"])
                for row in connection.execute(
                    f"SELECT payload_json FROM {entity} "
                    "WHERE active = 1 AND timestamp_ms >= ? AND timestamp_ms < ? "
                    "ORDER BY timestamp_ms, identity",
                    (selected_start, selected_end),
                )
            ]
        instruments = {
            row["inst_id"]: json.loads(row["payload_json"])
            for row in connection.execute(
                "SELECT inst_id, payload_json FROM instruments ORDER BY inst_id"
            )
        }
        run_count = int(
            connection.execute("SELECT COUNT(*) AS count FROM collection_runs").fetchone()[
                "count"
            ]
        )
    finally:
        connection.close()

    start = datetime.fromtimestamp(selected_start / 1000, zone)
    end = datetime.fromtimestamp(selected_end / 1000, zone)
    return {
        "metadata": {
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "timezone": timezone_name,
            "start_ms": selected_start,
            "end_ms": selected_end,
            "environment": environment,
            "archive_path": str(path),
        },
        "fills": rows_by_entity["fills"],
        "bills": rows_by_entity["bills"],
        "instruments": instruments,
        "quality": {
            "critical_sources_complete": True,
            "archive": {
                "schema_version": SCHEMA_VERSION,
                "api_priority": True,
                "collection_run_count": run_count,
                "common_coverage": [
                    {"start_ms": left, "end_ms": right} for left, right in coverage
                ],
            },
        },
    }


def archive_status(path: Path) -> dict[str, Any]:
    path = path.expanduser().resolve()
    coverage = common_curve_coverage(path)
    if not path.is_file():
        return {"path": str(path), "exists": False, "common_coverage": []}
    connection = _connect(path)
    try:
        meta = {
            row["key"]: row["value"]
            for row in connection.execute("SELECT key, value FROM archive_meta")
        }
        counts = {
            entity: int(
                connection.execute(
                    f"SELECT COUNT(*) AS count FROM {entity} WHERE active = 1"
                ).fetchone()["count"]
            )
            for entity in CURVE_ENTITIES
        }
        run_count = int(
            connection.execute("SELECT COUNT(*) AS count FROM collection_runs").fetchone()[
                "count"
            ]
        )
    finally:
        connection.close()
    return {
        "path": str(path),
        "exists": True,
        "environment": meta.get("environment"),
        "timezone": meta.get("timezone"),
        "run_count": run_count,
        "active_counts": counts,
        "common_coverage": [
            {"start_ms": start_ms, "end_ms": end_ms}
            for start_ms, end_ms in coverage
        ],
    }
