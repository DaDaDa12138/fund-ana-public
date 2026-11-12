from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.score_dedup_shadow import (
    SHADOW_SCORE_COMPONENTS,
    attach_score_differences,
    build_duplicate_return_correlation,
    build_membership_comparison,
    build_ranking_stability,
    build_risk_exposure_comparison,
    build_shadow_contract,
    calculate_scores,
    validate_score_components,
)


def _sample_frame() -> pd.DataFrame:
    rows = []
    for index, code in enumerate(("000001", "000002", "000003"), start=1):
        rows.append(
            {
                "基金代码": code,
                "基金名称": f"样本{index}",
                "债基风格标签": "综合纯债",
                "正式投资评分": float(60 + index * 10),
                "近期得分": float(58 + index * 10),
                "长期得分": float(62 + index * 10),
                "return_6m": float(index),
                "return_1y": float(index * 2),
                "annual_return": float(index * 2 + 0.1),
                "return_3y": float(index * 4),
                "sharpe_ratio": float(index),
                "sortino_ratio": float(index + 0.5),
                "calmar_ratio": float(index + 0.2),
                "max_drawdown": float(0.04 - index * 0.005),
                "volatility": float(0.05 - index * 0.005),
                "positive_ratio": float(50 + index * 5),
                "fee_total_numeric": float(0.6 - index * 0.05),
                "scale_billion": float(index * 10),
            }
        )
    return pd.DataFrame(rows)


def test_shadow_components_sum_to_one_and_separate_duplicate_return_roles() -> None:
    validate_score_components(SHADOW_SCORE_COMPONENTS)
    short_metrics = {metric for metric, _weight, _direction in SHADOW_SCORE_COMPONENTS["short_term"]}
    long_metrics = {metric for metric, _weight, _direction in SHADOW_SCORE_COMPONENTS["long_term"]}
    assert "return_1y" in short_metrics
    assert "return_1y" not in long_metrics
    assert "annual_return" in long_metrics
    assert "annual_return" not in short_metrics


def test_invalid_component_weights_fail_closed() -> None:
    with pytest.raises(ValueError, match="score_component_weight_not_one"):
        validate_score_components(
            {
                "short_term": [("return_1y", 0.8, True)],
                "long_term": [("annual_return", 1.0, True)],
            }
        )


def test_calculate_scores_preserves_style_percentile_and_missing_component_fallback() -> None:
    frame = _sample_frame()
    frame.loc[:, [metric for metric, _weight, _direction in SHADOW_SCORE_COMPONENTS["short_term"]]] = float("nan")
    scored = calculate_scores(
        frame,
        components=SHADOW_SCORE_COMPONENTS,
        short_term_weight=0.45,
        long_term_weight=0.55,
    )
    assert scored["shadow_short_score"].isna().all()
    assert scored["shadow_formal_score"].equals(scored["shadow_long_score"])
    assert scored.sort_values("shadow_formal_score")["基金代码"].tolist() == [
        "000001",
        "000002",
        "000003",
    ]


def test_score_difference_and_ranking_audit_are_per_fund() -> None:
    scored = calculate_scores(
        _sample_frame(),
        components=SHADOW_SCORE_COMPONENTS,
        short_term_weight=0.45,
        long_term_weight=0.55,
    )
    detailed = attach_score_differences(scored)
    ranking = build_ranking_stability(detailed)
    assert detailed["formal_score_delta"].notna().all()
    assert detailed["production_style_rank"].notna().all()
    assert detailed["shadow_style_rank"].notna().all()
    assert ranking["overall_comparable_count"] == 3
    assert ranking["overall_spearman"] == 1.0


def test_duplicate_return_correlation_does_not_require_scipy() -> None:
    audit = build_duplicate_return_correlation(_sample_frame())
    assert audit["overall"]["comparable_count"] == 3
    assert audit["overall"]["pearson"] == 1.0
    assert audit["overall"]["spearman"] == 1.0


def test_membership_and_risk_exposure_keep_added_removed_semantics() -> None:
    scored = calculate_scores(
        _sample_frame(),
        components=SHADOW_SCORE_COMPONENTS,
        short_term_weight=0.45,
        long_term_weight=0.55,
    )
    detailed = attach_score_differences(scored)
    baseline = detailed[detailed["基金代码"].isin(["000001", "000002"])].copy()
    shadow = detailed[detailed["基金代码"].isin(["000002", "000003"])].copy()
    membership, detail = build_membership_comparison(detailed, baseline, shadow)
    exposure = build_risk_exposure_comparison(baseline, shadow)
    assert membership["added_codes"] == ["000003"]
    assert membership["removed_codes"] == ["000001"]
    assert membership["jaccard"] == pytest.approx(1 / 3, abs=1e-6)
    status_by_code = detail.set_index("基金代码")["membership_change"].to_dict()
    assert status_by_code["000001"] == "shadow_removed"
    assert status_by_code["000003"] == "shadow_added"
    assert exposure["mean_deltas"]["sharpe_ratio"]["shadow_direction_preferred"] is True
    assert exposure["mean_deltas"]["max_drawdown"]["shadow_direction_preferred"] is True


def test_contract_explicitly_forbids_production_mutation() -> None:
    production = {
        "short_term": [("return_1y", 1.0, True)],
        "long_term": [("annual_return", 1.0, True)],
    }
    contract = build_shadow_contract(
        production_components=production,
        short_term_weight=0.45,
        long_term_weight=0.55,
    )
    assert contract["production_score_mutated"] is False
    assert contract["role_boundary"]["return_1y"].startswith("short_term")
    assert "正式" in contract["rollback"]


def test_workflow_runs_score_dedup_shadow_after_annual_shadow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from backend.workflow import WorkflowManager

    calls: list[tuple[str, list[str]]] = []
    logs: list[str] = []

    def _fake_run_structured_subprocess(*, task_name, cmd, **_kwargs):
        calls.append((task_name, list(cmd)))
        return SimpleNamespace(
            success=True,
            return_code=0,
            meta_path=tmp_path / f"{task_name}.json",
            stdout_tail=[],
            stderr_tail=[],
        )

    monkeypatch.setattr("backend.core_runner.run_structured_subprocess", _fake_run_structured_subprocess)
    monkeypatch.setattr("backend.core_runner.build_subprocess_log_index", lambda **_kwargs: {})
    manager = object.__new__(WorkflowManager)
    manager.settings = SimpleNamespace(project_root=tmp_path, runs_dir=tmp_path / "runs")
    manager.store = SimpleNamespace(append_log=logs.append)

    manager._run_annual_return_gate_shadow_audit("20260711_195604_sync")

    assert [task_name for task_name, _cmd in calls] == [
        "annual-return-gate-shadow",
        "score-dedup-shadow",
    ]
    assert calls[1][1][-3:] == ["scripts/audit_score_dedup_shadow.py", "--run-id", "20260711_195604_sync"]
    assert logs[-1] == "[score-dedup-shadow] 审计完成；生产评分保持不变"
