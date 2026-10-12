"""Palette and font fallback for the OKX single-position chart."""

from __future__ import annotations

import math

from PIL import ImageFont

X_BG = "#080808"
X_PLOT_BG = "#101010"
X_GRID = "#303030"
X_TEXT = "#f2f2f2"
X_MUTED = "#a0a0a0"
X_TARGET = "#54c4a2"
X_RISK = "#e17b89"
X_UP = "#f2f2f2"
X_DOWN = "#b8b8b8"


def _font(size: int, *, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = (
        "/System/Library/Fonts/STHeiti Medium.ttc"
        if bold
        else "/System/Library/Fonts/Hiragino Sans GB.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"
        if bold
        else "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    )
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def nice_tick_step(span: float, intervals: int = 6) -> float:
    raw = max(span / intervals, 0.01)
    order = 10 ** math.floor(math.log10(raw))
    for scale in (1, 2, 2.5, 5, 10):
        candidate = scale * order
        if candidate >= raw:
            return candidate
    return 10 * order
