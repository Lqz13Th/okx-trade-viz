"""Load complete OKX candles from a local cache or explicitly refresh it.

Rendering is offline by default.  A caller must opt into ``refresh=True`` to
make requests to OKX; this also makes the provenance of a newly drawn chart
obvious at the command line.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import subprocess
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MARKET_ENDPOINT = "https://openapi.okx.com/api/v5/market/history-candles"
BAR_MS = {
    "1D": 24 * 60 * 60_000,
    "4H": 4 * 60 * 60_000,
    "1H": 60 * 60_000,
    "15m": 15 * 60_000,
    "5m": 5 * 60_000,
}
DAY_OPEN_UTC_OFFSET_MS = 16 * 60 * 60_000  # OKX 1D opens at 00:00 Asia/Shanghai.
PAGE_LIMIT = 300
MIN_REQUEST_INTERVAL_SECONDS = 0.25  # Well below OKX's public market request limit.
USER_AGENT = "okx-trade-viz/0.1"

_rate_lock = threading.Lock()
_last_request_at = 0.0


def bar_floor(bar: str, timestamp_ms: int) -> int:
    """Return the opening time of the OKX bar containing ``timestamp_ms``."""
    duration = BAR_MS[bar]
    offset = DAY_OPEN_UTC_OFFSET_MS if bar == "1D" else 0
    return ((timestamp_ms - offset) // duration) * duration + offset


def _rate_limit() -> None:
    global _last_request_at
    with _rate_lock:
        elapsed = time.monotonic() - _last_request_at
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)
        _last_request_at = time.monotonic()


def _request_page(inst_id: str, bar: str, after_ms: int, relay: str | None) -> list[Any]:
    params = urllib.parse.urlencode(
        {"instId": inst_id, "bar": bar, "after": str(after_ms), "limit": str(PAGE_LIMIT)}
    )
    url = f"{MARKET_ENDPOINT}?{params}"
    error: Exception | None = None
    for attempt in range(4):
        try:
            _rate_limit()
            if relay is None:
                request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(request, timeout=30) as response:
                    body = response.read()
            else:
                # OpenSSH executes a remote shell command. Quote every argument,
                # and reject option-like or shell-special host strings.
                if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@:-]*", relay):
                    raise ValueError("relay must be an SSH host or user@host without shell syntax")
                remote_command = shlex.join(
                    [
                        "curl",
                        "--fail",
                        "--silent",
                        "--show-error",
                        "--max-time",
                        "30",
                        "--user-agent",
                        USER_AGENT,
                        "--",
                        url,
                    ]
                )
                result = subprocess.run(
                    [
                        "ssh",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "StrictHostKeyChecking=yes",
                        "-o",
                        "ConnectTimeout=10",
                        relay,
                        remote_command,
                    ],
                    check=True,
                    capture_output=True,
                    timeout=45,
                )
                body = result.stdout
            payload = json.loads(body)
            if str(payload.get("code")) != "0":
                raise RuntimeError(f"OKX {payload.get('code')}: {payload.get('msg')}")
            rows = payload.get("data")
            if not isinstance(rows, list):
                raise TypeError("OKX history-candles returned a non-list data field")
            return rows
        except (OSError, TypeError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            if isinstance(exc, ValueError) and "relay must be" in str(exc):
                raise
            error = exc
            if attempt < 3:
                time.sleep(0.5 * 2**attempt)
    raise RuntimeError(f"Could not fetch {inst_id} {bar} candles from OKX: {error}") from error


def _normalize_candle(raw: Any, bar_ms: int, asof_ms: int) -> dict[str, int | float] | None:
    if not isinstance(raw, list) or len(raw) < 9:
        raise ValueError("OKX history-candles row is incomplete")
    if str(raw[8]) != "1":
        return None
    ts = int(raw[0])
    if ts + bar_ms > asof_ms:
        return None
    values = [float(raw[index]) for index in (1, 2, 3, 4, 5)]
    if not all(math.isfinite(value) for value in values):
        raise ValueError(f"Non-finite OKX candle at {ts}")
    open_, high, low, close, volume = values
    if ts < 0 or low > min(open_, close) or high < max(open_, close) or volume < 0:
        raise ValueError(f"Invalid OKX candle at {ts}")
    return {
        "ts": ts,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _validate_window(
    candles: list[dict[str, Any]], bar: str, count: int, asof_ms: int
) -> list[dict[str, Any]]:
    """Return exactly the requested, contiguous, last-complete window."""
    bar_ms = BAR_MS[bar]
    last_open_ms = bar_floor(bar, asof_ms) - bar_ms
    by_ts: dict[int, dict[str, Any]] = {}
    for candle in candles:
        ts = int(candle["ts"])
        if ts in by_ts and by_ts[ts] != candle:
            raise ValueError(f"Conflicting duplicate candle at {ts}")
        by_ts[ts] = candle
    needed = [last_open_ms - offset * bar_ms for offset in range(count - 1, -1, -1)]
    missing = [ts for ts in needed if ts not in by_ts]
    if missing:
        raise ValueError(
            f"Missing {len(missing)} complete {bar} candles in requested window "
            f"({missing[0]} through {missing[-1]})"
        )
    return [by_ts[ts] for ts in needed]


def _load_cache(cache_path: Path, inst_id: str, bar: str, asof_ms: int) -> list[dict[str, Any]]:
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Cache root is not an object")
    if payload.get("inst_id") != inst_id or payload.get("bar") != bar:
        raise ValueError("Cache instrument/bar does not match the request")
    if payload.get("source") != MARKET_ENDPOINT or payload.get("complete_only") is not True:
        raise ValueError("Cache source or completion metadata is invalid")
    if not isinstance(payload.get("fetched_at_utc"), str):
        raise TypeError("Cache is missing fetched_at_utc")
    raw_candles = payload.get("candles")
    if not isinstance(raw_candles, list):
        raise TypeError("Cache candles is not a list")
    normalized: list[dict[str, Any]] = []
    for raw in raw_candles:
        if not isinstance(raw, dict):
            raise TypeError("Cache contains a non-object candle")
        ts = int(raw["ts"])
        candle = {
            "ts": ts,
            "open": float(raw["open"]),
            "high": float(raw["high"]),
            "low": float(raw["low"]),
            "close": float(raw["close"]),
            "volume": float(raw["volume"]),
        }
        if ts + BAR_MS[bar] > asof_ms:
            # A cache can cover later dates; these rows are not part of this
            # historical setup and cannot influence its displayed context.
            continue
        if not all(math.isfinite(float(value)) for value in candle.values()):
            raise ValueError(f"Non-finite cached candle at {ts}")
        if candle["low"] > min(candle["open"], candle["close"]) or candle["high"] < max(
            candle["open"], candle["close"]
        ):
            raise ValueError(f"Invalid cached candle at {ts}")
        normalized.append(candle)
    return normalized


def _write_cache(cache_path: Path, inst_id: str, bar: str, candles: list[dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "inst_id": inst_id,
        "bar": bar,
        "source": MARKET_ENDPOINT,
        "fetched_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "complete_only": True,
        "candles": candles,
    }
    # A failed refresh must not leave a half-written cache behind.
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=cache_path.parent,
        prefix=f".{cache_path.name}.",
        delete=False,
    ) as temp:
        temp_path = Path(temp.name)
        json.dump(payload, temp, ensure_ascii=False, separators=(",", ":"))
        temp.write("\n")
    try:
        os.replace(temp_path, cache_path)
    finally:
        temp_path.unlink(missing_ok=True)


def load_or_fetch_candles(
    inst_id: str,
    bar: str,
    count: int,
    asof_ms: int,
    cache_path: Path,
    *,
    refresh: bool = False,
    relay: str | None = None,
) -> list[dict]:
    """Return ``count`` consecutive completed candles ending before ``asof_ms``.

    When ``refresh`` is false, this function never contacts the network. The
    cache must cover the exact requested window; otherwise the error directs
    the caller to rerun with ``--refresh``. A refresh fetches only public OKX
    market data, replaces the cache atomically, and returns the same shape.
    """
    if bar not in BAR_MS:
        raise ValueError(f"Unsupported bar {bar!r}; choose one of {', '.join(BAR_MS)}")
    if not inst_id or not isinstance(inst_id, str):
        raise ValueError("inst_id must be a nonempty string")
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise ValueError("count must be a positive integer")
    if not isinstance(asof_ms, int) or isinstance(asof_ms, bool) or asof_ms < BAR_MS[bar]:
        raise ValueError("asof_ms must be a millisecond timestamp after one full bar")
    cache_path = Path(cache_path)

    if not refresh:
        try:
            cached = _load_cache(cache_path, inst_id, bar, asof_ms)
            return _validate_window(cached, bar, count, asof_ms)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"No sufficient validated local candle cache at {cache_path}: {exc}. "
                "Rerun with --refresh to fetch public OKX candles."
            ) from exc

    bar_ms = BAR_MS[bar]
    cursor = bar_floor(bar, asof_ms)
    candles: dict[int, dict[str, Any]] = {}
    max_pages = (count + PAGE_LIMIT - 1) // PAGE_LIMIT + 3
    for _ in range(max_pages):
        page = _request_page(inst_id, bar, cursor, relay)
        if not page:
            break
        oldest: int | None = None
        for raw in page:
            if not isinstance(raw, list) or not raw:
                raise ValueError("OKX history-candles row is incomplete")
            ts = int(raw[0])
            oldest = ts if oldest is None else min(oldest, ts)
            candle = _normalize_candle(raw, bar_ms, asof_ms)
            if candle is not None:
                prior = candles.get(ts)
                if prior is not None and prior != candle:
                    raise ValueError(f"Conflicting OKX candle at {ts}")
                candles[ts] = candle
        if oldest is None or oldest >= cursor:
            raise RuntimeError(f"OKX candle pagination stalled at {cursor}")
        try:
            selected = _validate_window(list(candles.values()), bar, count, asof_ms)
        except ValueError:
            cursor = oldest
            continue
        _write_cache(cache_path, inst_id, bar, sorted(candles.values(), key=lambda row: row["ts"]))
        return selected

    # Do not write an incomplete or gapped series as a supposedly usable cache.
    try:
        _validate_window(list(candles.values()), bar, count, asof_ms)
    except ValueError as exc:
        raise RuntimeError(
            f"OKX did not provide a contiguous {count}-bar {bar} window: {exc}"
        ) from exc
    raise RuntimeError(f"OKX candle pagination exceeded its {max_pages}-page safety limit")
