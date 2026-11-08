"""Protect platform-specific startup and rendering behavior."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from okx_trade_viz import order_archive_cli, style


class WindowsCompatibilityTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "nt", "Windows font fallback contract")
    def test_windows_font_fallback_preserves_requested_size(self) -> None:
        regular = style._font(105)
        bold = style._font(105, bold=True)
        self.assertEqual(getattr(regular, "size", None), 105)
        self.assertEqual(getattr(bold, "size", None), 105)

    def test_archive_lock_rejects_a_second_writer(self) -> None:
        with TemporaryDirectory() as directory:
            database = Path(directory) / "orders.sqlite3"
            first = order_archive_cli._acquire_lock(database)
            try:
                with self.assertRaisesRegex(ValueError, "已有另一个订单归档进程"):
                    order_archive_cli._acquire_lock(database)
            finally:
                first.close()


if __name__ == "__main__":
    unittest.main()
