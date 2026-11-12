"""Extreme finite volatility needs source review; it is not mathematically invalid."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.audit_data_truthfulness import _risk_metric_truth


def risk_database(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "risk.db"
    fields = (
        "fcode", "period", "volatility", "max_drawdown", "var_95", "var_99",
        "annual_return", "positive_ratio",
    )
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE fund_risk_metrics (
            fcode TEXT, period TEXT, volatility REAL, max_drawdown REAL,
            var_95 REAL, var_99 REAL, annual_return REAL, positive_ratio REAL
        )""")
        conn.executemany(
            "INSERT INTO fund_risk_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(row.get(field) for field in fields) for row in rows],
        )
    return path


def test_real_high_volatility_is_retained_as_source_review_warning(tmp_path: Path) -> None:
    fixture = json.loads((ROOT / "tests/fixtures/risk_volatility_outliers_20260908.json").read_text())
    path = risk_database(tmp_path, fixture["rows"])
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    result = _risk_metric_truth(SimpleNamespace(database_path=path))

    assert result["blockers"] == []
    assert result["warnings"] == ["volatility_above_one_requires_source_review"]
    assert "volatility_above_one" not in result["invalid_counts"]
    assert result["extreme_value_counts"]["volatility_above_one"] == 2
    samples = result["extreme_value_samples"]["volatility_above_one"]
    assert {(row["fcode"], row["period"], row["volatility"]) for row in samples} == {
        (row["fcode"], row["period"], row["volatility"]) for row in fixture["rows"]
    }
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


@pytest.mark.parametrize("volatility", [None, 0.0, 1.0])
def test_missing_zero_and_one_are_not_extreme_observations(tmp_path: Path, volatility) -> None:
    # Isolated mathematical boundary input; never a published fund record.
    path = risk_database(tmp_path, [{"fcode": "boundary_fixture", "period": "1y", "volatility": volatility}])
    result = _risk_metric_truth(SimpleNamespace(database_path=path))
    assert result["blockers"] == result["warnings"] == []
    assert result["extreme_value_counts"]["volatility_above_one"] == 0
    assert result["extreme_value_samples"]["volatility_above_one"] == []
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT volatility FROM fund_risk_metrics").fetchone()[0] == volatility


@pytest.mark.parametrize("field,value,blocker", [
    ("volatility", -0.01, "negative_volatility"),
    ("volatility", float("inf"), "non_finite_volatility"),
    ("volatility", float("-inf"), "non_finite_volatility"),
    ("volatility", "invalid", "non_numeric_volatility"),
    ("max_drawdown", -0.01, "negative_max_drawdown"),
    ("max_drawdown", 1.01, "max_drawdown_above_one"),
])
def test_invalid_values_still_block(tmp_path: Path, field: str, value, blocker: str) -> None:
    path = risk_database(tmp_path, [{"fcode": "boundary_fixture", "period": "1y", field: value}])
    result = _risk_metric_truth(SimpleNamespace(database_path=path))
    assert blocker in result["blockers"]
    assert result["invalid_counts"][blocker] == 1
    assert result["extreme_value_counts"]["volatility_above_one"] == 0


def test_extreme_sample_limit_does_not_reduce_observed_count(tmp_path: Path) -> None:
    # Repeated actual values exercise diagnostic pagination, not additional funds.
    fixture = json.loads((ROOT / "tests/fixtures/risk_volatility_outliers_20260908.json").read_text())
    path = risk_database(tmp_path, fixture["rows"] * 15)
    result = _risk_metric_truth(SimpleNamespace(database_path=path))
    assert result["extreme_value_counts"]["volatility_above_one"] == 30
    assert len(result["extreme_value_samples"]["volatility_above_one"]) == 20
    assert result["extreme_value_sample_limit"] == 20
