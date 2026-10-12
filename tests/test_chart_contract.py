"""Protect the one-position chart's risk and partial-exit semantics."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from PIL import Image

from okx_trade_viz import chart


class ChartContractTests(unittest.TestCase):
    def test_header_uses_whole_position_r_not_price_only_r(self) -> None:
        closed = {
            "status": "closed",
            "net_r": 1.07934,
            "realized_price_r": 1.71,
        }
        self.assertEqual(chart._single_trade_metric(closed), "+1.079R")
        open_trade = {
            "status": "partially_closed_in_archive",
            "total_r": 2.63428,
            "realized_price_r": 1.71,
        }
        self.assertEqual(chart._single_trade_metric(open_trade), "+2.634R")

    def test_missing_verified_r_never_falls_back_to_price_r(self) -> None:
        for status in ("closed", "open", "partially_closed_in_archive"):
            with self.subTest(status=status):
                self.assertEqual(
                    chart._single_trade_metric({"status": status, "realized_price_r": 1.71}),
                    "R UNAVAILABLE",
                )

    def test_shading_fades_after_partial_exit_and_ends_after_final_exit(self) -> None:
        trade = {
            "entry_ms": 0,
            "size": 100,
            "exit_fills": [
                {"time_ms": 10, "size": 75},
                {"time_ms": 20, "size": 25},
            ],
        }
        self.assertEqual(
            chart._single_zone_spans(trade, 30),
            [
                (0, 10, 1.0),
                (10, 20, 0.25),
            ],
        )

    def test_partial_exit_label_uses_filled_size(self) -> None:
        self.assertEqual(
            chart._partial_exit_percent(
                {
                    "status": "partially_closed_in_archive",
                    "size": 100,
                    "remaining_size": 25,
                }
            ),
            75,
        )

    def test_chart_renders_a_square_png_from_one_trade(self) -> None:
        bar_ms = 15 * 60_000
        candles = [
            {
                "ts": i * bar_ms,
                "open": 100 + i * 0.2,
                "high": 101 + i * 0.2,
                "low": 99 + i * 0.2,
                "close": 100.2 + i * 0.2,
            }
            for i in range(4)
        ]
        trade = {
            "side": "long",
            "status": "closed",
            "entry_ms": bar_ms,
            "exit_ms": 3 * bar_ms,
            "entry_price": 100.0,
            "tp_price": 102.0,
            "sl_price": 99.0,
            "size": 100,
            "remaining_size": 0,
            "total_r": 1.5,
            "exit_fills": [
                {"time_ms": 3 * bar_ms, "size": 100, "price": 102.0, "order_id": "tp-1"}
            ],
        }
        with TemporaryDirectory() as directory:
            output = Path(directory) / "trade.png"
            chart.render_chart(
                "BTC-USDT-SWAP",
                "15m",
                0,
                5 * bar_ms,
                candles,
                [trade],
                output,
                candle_anchor="close",
                evidence_end_ms=3 * bar_ms,
            )
            with Image.open(output) as rendered:
                self.assertEqual(rendered.size, (2400, 2400))
                self.assertEqual(rendered.mode, "RGB")


if __name__ == "__main__":
    unittest.main()
