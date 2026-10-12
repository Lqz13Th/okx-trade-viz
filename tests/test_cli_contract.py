"""Smoke-test the installed command without credentials or network access."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from importlib.metadata import distribution


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    for key in ("OKX_API_KEY", "OKX_API_SECRET", "OKX_API_PASSPHRASE"):
        environment.pop(key, None)
    with tempfile.TemporaryDirectory() as directory:
        return subprocess.run(
            [
                sys.executable,
                "-c",
                "from okx_trade_viz.cli import main; main()",
                *args,
            ],
            cwd=directory,
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )


class CLIContractTests(unittest.TestCase):
    def test_package_exposes_one_console_command(self) -> None:
        scripts = {
            entry.name: entry.value
            for entry in distribution("okx-trade-viz").entry_points
            if entry.group == "console_scripts"
        }
        self.assertEqual(scripts, {"okx-trade-viz": "okx_trade_viz.cli:main"})

    def test_help_works_without_credential_file_or_data_directory(self) -> None:
        result = _run_cli("--help")
        self.assertEqual(result.returncode, 0, result.stderr)
        for option in (
            "--days",
            "--instrument",
            "--offline",
            "--env-file",
            "--data-dir",
            "--import-from",
            "--output-dir",
            "--bars",
            "--structure",
        ):
            self.assertIn(option, result.stdout)

    def test_exact_bars_and_structure_cannot_be_combined(self) -> None:
        result = _run_cli("--offline", "--bars", "15m", "--structure")
        self.assertEqual(result.returncode, 2)
        self.assertIn("not allowed with argument", result.stderr)

    def test_nonpositive_day_count_is_rejected_before_data_access(self) -> None:
        result = _run_cli("--offline", "--days", "0")
        self.assertEqual(result.returncode, 2)
        self.assertIn("--days", result.stderr)


if __name__ == "__main__":
    unittest.main()
