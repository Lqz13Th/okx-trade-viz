"""Keep the published eight-view structure preset stable."""

from __future__ import annotations

import stat
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from okx_trade_viz import render


class RenderContractTests(unittest.TestCase):
    def test_default_structure_has_eight_pre_entry_views(self) -> None:
        self.assertEqual(
            render.STRUCTURE_VIEWS,
            (
                ("15m-6h", "15m", 24),
                ("15m-12h", "15m", 48),
                ("15m-24h", "15m", 96),
                ("15m-48h", "15m", 192),
                ("1h-48h", "1H", 48),
                ("1h-4d", "1H", 96),
                ("4h-16d", "4H", 96),
                ("4h-28d", "4H", 168),
            ),
        )

    def test_closed_position_keeps_candles_through_snapshot(self) -> None:
        bar_ms = render.BAR_MS["15m"]
        trade = {"entry_ms": 100 * bar_ms, "exit_ms": 110 * bar_ms, "status": "closed"}
        start, plot_end, count, fetch_asof = render._view_window(trade, "15m", 24, 200 * bar_ms)
        self.assertLess(start, trade["entry_ms"])
        self.assertGreater(plot_end, 200 * bar_ms)
        self.assertEqual(fetch_asof, 200 * bar_ms)
        self.assertGreater(count, 24)

    def test_existing_shared_output_directory_is_not_chmodded(self) -> None:
        with TemporaryDirectory() as directory:
            output = Path(directory) / "shared"
            output.mkdir(mode=0o755)
            output.chmod(0o755)
            with self.assertRaisesRegex(ValueError, "not private"):
                render._private_output_directory(output)
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o755)


if __name__ == "__main__":
    unittest.main()
