"""Regression checks for holding-period policy imports in both CLI layouts."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = PROJECT_ROOT / "core"


def _clean_python_env() -> dict[str, str]:
    """Avoid PYTHONPATH masking the import paths used by the two entry points."""
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return env


def test_holding_policy_imports_as_project_package() -> None:
    """The package-qualified import must work from the repository root."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from core.utils.holding_period_policy import "
                "holding_period_gate_reason; "
                "assert holding_period_gate_reason("
                "has_holding_period=0, holding_days=None, "
                "fund_name='普通债券基金') is None"
            ),
        ],
        cwd=PROJECT_ROOT,
        env=_clean_python_env(),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_core_cli_help_imports_policy_from_core_working_directory() -> None:
    """The real CLI entry point must import when launched from ``core/``."""
    result = subprocess.run(
        [sys.executable, "main.py", "--help"],
        cwd=CORE_ROOT,
        env=_clean_python_env(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "usage:" in result.stdout.lower()


def test_core_screening_and_lazy_holding_helper_use_cli_import_layout() -> None:
    """Exercise screening imports and the report helper's deferred import."""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from bond_fund_screening.core.filtering_engine import FilteringEngine; "
                "from bond_fund_screening.core.enhanced_scoring_engine import EnhancedScoringEngine; "
                "from bond_fund_screening.tasks.final_screening_common_helpers "
                "import has_fixed_holding_period; "
                "assert has_fixed_holding_period('测试180天持有期债券A', holding_keywords=[]); "
                "assert not has_fixed_holding_period('测试30天持有期债券A', holding_keywords=[]); "
                "assert not has_fixed_holding_period('测试普通债券A', holding_keywords=[])"
            ),
        ],
        cwd=CORE_ROOT,
        env=_clean_python_env(),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
