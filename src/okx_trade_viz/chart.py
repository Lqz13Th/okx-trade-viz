"""Render the original dark OKX single-position R chart.

Only the one-position square layout is carried over from okx-analysis.
Recorded plan bands are price references, not proof of continuous live orders.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageColor, ImageDraw, ImageFont

from . import style
from .candles import BAR_MS

TZ = ZoneInfo("Asia/Shanghai")
SIZE = 2400
SINGLE_PLOT = (130, 365, 2270, 2200)
SINGLE_GAIN_FILL = (16, 132, 105, 112)
SINGLE_LOSS_FILL = (165, 48, 66, 120)
SINGLE_REFERENCE_ALPHA = 84
SINGLE_TITLE: ImageFont.ImageFont = style._font(105, bold=True)
SINGLE_METRIC: ImageFont.ImageFont = style._font(105, bold=True)
SINGLE_DETAIL: ImageFont.ImageFont = style._font(80, bold=True)
SINGLE_CAPTION: ImageFont.ImageFont = style._font(73)


def _initial_size(trade: dict) -> float | None:
    for key in ("size", "entry_size"):
        size = float(trade.get(key) or 0)
        if math.isfinite(size) and size > 0:
            return size
    fills = trade.get("entry_fills") or trade.get("exit_fills") or []
    size = sum(float(fill.get("size") or 0) for fill in fills)
    return size if math.isfinite(size) and size > 0 else None


def _single_trade_metric(trade: dict) -> str:
    if trade.get("status") != "closed" and not trade.get("exit_fills"):
        return ""
    value = trade.get("total_r")
    if value is None and trade.get("status") == "closed":
        value = trade.get("net_r")
    return f"{float(value):+.3f}R" if value is not None else "R UNAVAILABLE"


def _partial_exit_percent(trade: dict) -> int | None:
    if trade.get("status") != "partially_closed_in_archive":
        return None
    initial = _initial_size(trade)
    remaining = trade.get("remaining_size")
    if initial is None or remaining is None:
        return None
    open_fraction = min(1.0, max(0.0, float(remaining) / initial))
    return 100 - round(100 * open_fraction)


def _draw_dashed(
    draw: ImageDraw.ImageDraw, left: int, right: int, yy: int, color, width: int
) -> None:
    for cursor in range(left, right, 31):
        draw.line((cursor, yy, min(cursor + 18, right), yy), fill=color, width=width)


def _single_plan_levels(trade: dict, kind: str) -> list[float]:
    """Distinct recorded price references, ordered nearest the entry first."""
    entry = float(trade["entry_price"])
    side = 1 if trade["side"] == "long" else -1
    direction = side if kind == "tp" else -side
    levels = {
        float(event["price"])
        for event in trade.get(f"{kind}_orders") or []
        if (float(event["price"]) - entry) * direction > 0
    }
    reference = trade.get(f"{kind}_price")
    if reference is not None and (float(reference) - entry) * direction > 0:
        levels.add(float(reference))
    return sorted(levels, key=lambda level: abs(level - entry))


def _single_zone_spans(trade: dict, horizon_ms: int) -> list[tuple[int, int, float]]:
    initial = _initial_size(trade)
    if initial is None or initial <= 0 or trade.get("entry_exit_interleaved"):
        return []
    cursor = int(trade["entry_ms"])
    remaining = initial
    spans = []
    for fill in sorted(trade.get("exit_fills") or [], key=lambda item: int(item["time_ms"])):
        fill_ms = int(fill["time_ms"])
        if fill_ms > horizon_ms:
            break
        if fill_ms > cursor and remaining > 0:
            spans.append((cursor, fill_ms, remaining / initial))
        cursor = max(cursor, fill_ms)
        remaining = max(0.0, remaining - float(fill.get("size") or 0))
    if horizon_ms > cursor and remaining > 0:
        spans.append((cursor, horizon_ms, remaining / initial))
    return spans


def _draw_single_zone_span(
    draw: ImageDraw.ImageDraw,
    levels: list[float],
    entry: float,
    color: tuple[int, int, int, int],
    x1: int,
    x2: int,
    fraction: float,
    y,
) -> None:
    """Composite nested entry-to-level rectangles without changing their time width."""
    if not levels or x2 <= x1 or fraction <= 0:
        return
    per_layer = color[3] * min(1.0, fraction) / len(levels) / 255
    boundaries = [entry, *levels]
    for index in range(len(levels)):
        near, far = boundaries[index], boundaries[index + 1]
        depth = len(levels) - index
        alpha = round(255 * (1 - (1 - per_layer) ** depth))
        draw.rectangle(
            (x1, min(y(near), y(far)), x2, max(y(near), y(far))),
            fill=(*color[:3], alpha),
        )


def _single_tp_caption(trade: dict, tp: float, index: int, total: int) -> str:
    entry = float(trade["entry_price"])
    stop = trade.get("sl_price")
    risk = abs(entry - float(stop)) if stop is not None else 0.0
    side = 1 if trade["side"] == "long" else -1
    prefix = f"TP{index}" if total > 1 else "TP"
    caption = f"{prefix} +{(tp - entry) * side / risk:.2f}R" if risk > 0 else prefix
    orders = trade.get("tp_orders") or []
    matching = [item for item in orders if math.isclose(float(item["price"]), tp)]
    if len(matching) != 1:
        return caption
    order = matching[0]
    initial = _initial_size(trade)
    size = order.get("size")
    if initial is None or size is None:
        return caption
    share = min(100, round(100 * float(size) / initial))
    ids = {str(value) for value in (order.get("id"), order.get("trigger_order_id")) if value}
    filled = sum(
        float(fill.get("size") or 0)
        for fill in trade.get("exit_fills", [])
        if fill.get("order_id") is not None and str(fill["order_id"]) in ids
    )
    if filled >= float(size) - 1e-9:
        state = "FILLED"
    elif filled > 0:
        state = "PARTIAL"
    elif order.get("state") in {"canceled", "cancelled", "order_failed"}:
        state = "UNFILLED"
    elif order.get("state") == "live":
        state = "OPEN"
    else:
        state = "RECORDED"
    return f"{caption} / {share}% {state}"


def _single_exit_groups(trade: dict) -> list[dict]:
    groups: dict[tuple[str, float], dict] = {}
    for fill in trade.get("exit_fills") or []:
        price = float(fill["price"])
        order_id = str(fill.get("order_id") or f"at:{fill['time_ms']}")
        group = groups.setdefault(
            (order_id, price),
            {"time_ms": 0, "price": price, "size": 0.0},
        )
        group["time_ms"] = max(group["time_ms"], int(fill["time_ms"]))
        group["size"] += float(fill.get("size") or 0)
    return sorted(groups.values(), key=lambda item: (item["time_ms"], item["price"]))


def _render_single_trade_chart(
    instrument: str,
    bar: str,
    start_ms: int,
    end_ms: int,
    evidence_end_ms: int,
    candles: list[dict],
    trade: dict,
    output: Path,
    *,
    candle_anchor: str,
    view_label: str | None,
) -> None:
    left, top, right, bottom = SINGLE_PLOT
    entry = float(trade["entry_price"])
    tp_levels = _single_plan_levels(trade, "tp")
    sl_levels = _single_plan_levels(trade, "sl")
    stop = float(trade["sl_price"]) if trade.get("sl_price") is not None else None
    side = 1 if trade["side"] == "long" else -1
    horizon_ms = (
        min(evidence_end_ms, int(trade["exit_ms"]))
        if trade.get("status") == "closed" and trade.get("exit_ms") is not None
        else evidence_end_ms
    )
    zone_end_ms = (
        end_ms
        if trade.get("status") != "closed" and float(trade.get("remaining_size") or 0) > 0
        else horizon_ms
    )
    prices = [entry, *(float(row[key]) for row in candles for key in ("high", "low"))]
    prices.extend(tp_levels)
    prices.extend(sl_levels)
    prices.extend(
        float(event["price"])
        for kind in ("tp", "sl")
        for event in trade.get(f"{kind}_orders") or []
    )
    prices.extend(float(fill["price"]) for fill in trade.get("exit_fills", []))
    raw_low, raw_high = min(prices), max(prices)
    risk = abs(entry - stop) if stop is not None else 0.0
    pad = max((raw_high - raw_low) * 0.055, risk * 0.18, raw_high * 0.001)
    low, high = raw_low - pad, raw_high + pad

    def x(ms: float) -> int:
        return round(left + (ms - start_ms) / (end_ms - start_ms) * (right - left))

    def y(price: float) -> int:
        return round(bottom - (price - low) / (high - low) * (bottom - top))

    image = Image.new("RGBA", (SIZE, SIZE), style.X_BG)
    draw = ImageDraw.Draw(image)
    pair = " / ".join(instrument.split("-")[:2])
    metric = _single_trade_metric(trade)
    draw.text((130, 60), pair, font=SINGLE_TITLE, fill=style.X_TEXT)
    if metric:
        metric_width = draw.textbbox((0, 0), metric, font=SINGLE_METRIC)[2]
        draw.text((SIZE - 130 - metric_width, 65), metric, font=SINGLE_METRIC, fill=style.X_TEXT)
    detail = (
        f"{bar.upper()} / {view_label}   |   {trade['side'].upper()}"
        if view_label
        else (f"{bar.upper()}   |   {trade['side'].upper()}")
    )
    draw.text((132, 205), detail, font=SINGLE_DETAIL, fill=style.X_MUTED)
    if trade.get("total_r_asof"):
        asof = datetime.fromisoformat(trade["total_r_asof"]).astimezone(TZ)
        mark_label = f"MARK {asof:%m/%d %H:%M}"
        mark_width = draw.textbbox((0, 0), mark_label, font=SINGLE_CAPTION)[2]
        mark_x = SIZE - 130 - mark_width
        if mark_x > 132 + draw.textbbox((0, 0), detail, font=SINGLE_DETAIL)[2] + 80:
            draw.text((mark_x, 205), mark_label, font=SINGLE_CAPTION, fill=style.X_MUTED)
    draw.line((130, 310, SIZE - 130, 310), fill=style.X_GRID, width=3)
    draw.rectangle(SINGLE_PLOT, fill=style.X_PLOT_BG)
    for fraction in (0.25, 0.5, 0.75):
        yy = round(top + fraction * (bottom - top))
        draw.line((left, yy, right, yy), fill=style.X_GRID, width=2)

    overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
    shade = ImageDraw.Draw(overlay)
    entry_x, zone_end_x = x(int(trade["entry_ms"])), x(zone_end_ms)
    if tp_levels and sl_levels:
        for span_start, span_end, fraction in _single_zone_spans(trade, zone_end_ms):
            for levels, color in ((tp_levels, SINGLE_GAIN_FILL), (sl_levels, SINGLE_LOSS_FILL)):
                _draw_single_zone_span(
                    shade,
                    levels,
                    entry,
                    color,
                    x(span_start),
                    x(span_end),
                    fraction,
                    y,
                )
    for levels, color in ((tp_levels, style.X_TARGET), (sl_levels, style.X_RISK)):
        for level in levels:
            _draw_dashed(
                shade,
                left,
                right,
                y(level),
                (*ImageColor.getrgb(color), SINGLE_REFERENCE_ALPHA),
                4,
            )
    image.alpha_composite(overlay)
    draw = ImageDraw.Draw(image)

    bar_ms = BAR_MS[bar]
    spacing = (right - left) * bar_ms / (end_ms - start_ms)
    half = max(2, min(13, round(spacing * 0.32)))
    wick_width = 4 if spacing > 15 else 2
    for candle in candles:
        candle_ms = int(candle["ts"]) + (bar_ms if candle_anchor == "close" else bar_ms // 2)
        cx = x(candle_ms)
        rising = candle["close"] >= candle["open"]
        color = style.X_UP if rising else style.X_DOWN
        draw.line((cx, y(candle["high"]), cx, y(candle["low"])), fill=color, width=wick_width)
        upper = y(max(candle["open"], candle["close"]))
        lower = y(min(candle["open"], candle["close"]))
        body = (cx - half, upper, cx + half, max(upper + 4, lower))
        if rising:
            draw.rectangle(body, fill=style.X_PLOT_BG, outline=color, width=2)
        else:
            draw.rectangle(body, fill=color)

    draw.line((entry_x, top, entry_x, bottom), fill=style.X_GRID, width=3)
    exit_groups = _single_exit_groups(trade)
    for group in exit_groups:
        draw.line(
            (x(group["time_ms"]), top, x(group["time_ms"]), bottom), fill=style.X_GRID, width=3
        )
    draw.line((entry_x, y(entry), zone_end_x, y(entry)), fill=style.X_TEXT, width=5)
    for event in trade.get("tp_orders") or []:
        first = max(int(trade["entry_ms"]), int(event["created_ms"]))
        last = min(horizon_ms, int(event.get("terminal_ms") or horizon_ms))
        if last > first:
            draw.line(
                (x(first), y(float(event["price"])), x(last), y(float(event["price"]))),
                fill=style.X_TARGET,
                width=5,
            )
    for index, event in enumerate(trade.get("sl_orders") or []):
        first = max(int(trade["entry_ms"]), int(event["created_ms"]))
        last = min(horizon_ms, int(event.get("terminal_ms") or horizon_ms))
        if last > first:
            level = y(float(event["price"]))
            color = style.X_RISK if index == 0 else style.X_MUTED
            _draw_dashed(draw, x(first), x(last), level, color, 5 if index == 0 else 4)
            if index:
                draw.ellipse((x(first) - 9, level - 9, x(first) + 9, level + 9), fill=color)

    entry_y = y(entry)
    draw.ellipse(
        (entry_x - 20, entry_y - 20, entry_x + 20, entry_y + 20),
        fill=style.X_PLOT_BG,
        outline=style.X_TEXT,
        width=6,
    )
    for group in exit_groups:
        exit_x, exit_y = x(group["time_ms"]), y(group["price"])
        favorable = (group["price"] - entry) * side > 0
        diamond = (
            (exit_x, exit_y - 24),
            (exit_x + 24, exit_y),
            (exit_x, exit_y + 24),
            (exit_x - 24, exit_y),
        )
        draw.polygon(diamond, fill="#24463e" if favorable else "#572c38")
        draw.line(
            (*diamond, diamond[0]),
            fill="#b9d8cc" if favorable else "#e4a3ab",
            width=6,
            joint="curve",
        )
    captions = []
    for index, tp in enumerate(tp_levels, 1):
        captions.append(
            (
                (left + 24, max(top + 16, y(tp) - 102)),
                _single_tp_caption(trade, tp, index, len(tp_levels)),
                style.X_TARGET,
            )
        )
    if stop is not None and stop in sl_levels:
        captions.append(((left + 24, min(bottom - 94, y(stop) + 16)), "SL -1R", style.X_RISK))
    captions.append(
        (
            (
                entry_x + 32 if right - entry_x > 360 else entry_x - 300,
                min(bottom - 94, entry_y + 34),
            ),
            "ENTRY",
            style.X_TEXT,
        )
    )
    caption_boxes = [
        draw.textbbox(position, caption, font=SINGLE_CAPTION, stroke_width=5)
        for position, caption, _ in captions
    ]

    def exit_caption_position(exit_x: int, exit_y: int, caption: str) -> tuple[int, int]:
        text_box = draw.textbbox((0, 0), caption, font=SINGLE_CAPTION, stroke_width=5)
        width, height = text_box[2] - text_box[0], text_box[3] - text_box[1]
        candidates = (
            (exit_x - 230, exit_y - 105),
            (exit_x + 32, exit_y + 34),
            (exit_x - 230, exit_y + 34),
            (exit_x + 32, exit_y - 105),
            (exit_x - 230, exit_y - 200),
        )
        for candidate_x, candidate_y in candidates:
            px = max(left + 24, min(right - width - 24, candidate_x))
            py = max(top + 16, min(bottom - height - 24, candidate_y))
            box = draw.textbbox((px, py), caption, font=SINGLE_CAPTION, stroke_width=5)
            if all(
                box[2] + 16 < occupied[0]
                or occupied[2] + 16 < box[0]
                or box[3] + 16 < occupied[1]
                or occupied[3] + 16 < box[1]
                for occupied in caption_boxes
            ):
                caption_boxes.append(box)
                return px, py
        px = max(left + 24, min(right - width - 24, exit_x - 230))
        py = max(top + 16, min(bottom - height - 24, exit_y - 105))
        caption_boxes.append(draw.textbbox((px, py), caption, font=SINGLE_CAPTION, stroke_width=5))
        return px, py

    initial_size = _initial_size(trade)
    for group in exit_groups:
        exit_x, exit_y = x(group["time_ms"]), y(group["price"])
        percent = round(100 * group["size"] / initial_size) if initial_size else None
        exit_caption = (
            f"EXIT {percent}%"
            if percent is not None and (len(exit_groups) > 1 or trade.get("status") != "closed")
            else "EXIT"
        )
        captions.append(
            (
                exit_caption_position(exit_x, exit_y, exit_caption),
                exit_caption,
                "#b9d8cc" if (group["price"] - entry) * side > 0 else "#e4a3ab",
            )
        )
    for position, caption, color in captions:
        draw.text(
            position,
            caption,
            font=SINGLE_CAPTION,
            fill=color,
            stroke_width=5,
            stroke_fill=style.X_PLOT_BG,
        )
    first_date = datetime.fromtimestamp(start_ms / 1000, UTC).astimezone(TZ).strftime("%m/%d")
    last_complete_close_ms = int(candles[-1]["ts"]) + BAR_MS[bar]
    last_date = (
        datetime.fromtimestamp(last_complete_close_ms / 1000, UTC).astimezone(TZ).strftime("%m/%d")
    )
    draw.text((left, bottom + 20), first_date, font=SINGLE_CAPTION, fill=style.X_MUTED)
    date_width = draw.textbbox((0, 0), last_date, font=SINGLE_CAPTION)[2]
    last_date_x = x(last_complete_close_ms)
    if last_date_x - date_width / 2 > left + 250:
        draw.text(
            (last_date_x - date_width / 2, bottom + 20),
            last_date,
            font=SINGLE_CAPTION,
            fill=style.X_MUTED,
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(output, optimize=True)


def render_chart(
    instrument: str,
    bar: str,
    start_ms: int,
    end_ms: int,
    candles: list[dict],
    trades: list[dict],
    output: Path,
    *,
    aspect: str = "1:1",
    candle_anchor: str = "center",
    single_trade: bool = True,
    evidence_end_ms: int | None = None,
    view_label: str | None = None,
) -> None:
    """Render exactly one 1:1 position image; matches the original single-trade path."""
    if not candles:
        raise ValueError("No complete candles in the requested interval")
    if bar not in BAR_MS:
        raise ValueError(f"Unsupported bar: {bar}")
    if aspect != "1:1":
        raise ValueError("Single-trade layout requires 1:1 aspect")
    if candle_anchor not in {"center", "close"}:
        raise ValueError(f"Unsupported candle time anchor: {candle_anchor}")
    if not single_trade or len(trades) != 1:
        raise ValueError("Single-trade layout needs exactly one trade")
    if evidence_end_ms is None:
        evidence_end_ms = end_ms
    if not start_ms < evidence_end_ms <= end_ms:
        raise ValueError("Evidence end must fall inside the chart time window")
    _render_single_trade_chart(
        instrument,
        bar,
        start_ms,
        end_ms,
        evidence_end_ms,
        candles,
        trades[0],
        output,
        candle_anchor=candle_anchor,
        view_label=view_label,
    )
