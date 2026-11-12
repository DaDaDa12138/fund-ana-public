from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings
from backend.report_inputs import normalize_code, resolve_runtime_artifact_path
from bond_fund_screening.core.enhanced_scoring_engine import EnhancedScoringEngine
from scripts.audit_runtime_utils import resolve_run_payload


SCORE_COLUMNS = {
    "收益能力分": "return_score",
    "风险控制分": "risk_score",
    "稳定性分": "stability_score",
    "流动性分": "liquidity_score",
    "综合评分": "comprehensive_score",
}
RISK_PERIOD_COVERAGE_GRACE_DAYS = 7
RISK_PERIOD_REQUIRED_OBSERVATIONS = 31


def _tracker_auxiliary_rebuild_required(score_summaries: dict[str, Any]) -> bool:
    """Only request a rebuild when the stored auxiliary risk score materially drifts."""
    risk_summary = score_summaries.get("risk_score") if isinstance(score_summaries, dict) else {}
    try:
        return int((risk_summary or {}).get("changed_gt_0_01_count") or 0) > 0
    except (TypeError, ValueError):
        return False


def _read_tracker(path: Path) -> pd.DataFrame:
    tracker = pd.read_excel(path, sheet_name="完整数据", header=1, dtype=str)
    if tracker.empty or "基金代码" not in tracker.columns:
        raise RuntimeError(f"Tracker 完整数据为空或缺少基金代码：{path}")
    tracker["基金代码"] = tracker["基金代码"].map(normalize_code)
    return tracker[tracker["基金代码"].astype(str).str.strip().ne("")].copy()


def _load_metric_input_frame(database_path: Path, tracker_codes: set[str]) -> pd.DataFrame:
    query = """
    WITH nav_window AS (
        SELECT
            nav.fcode,
            MIN(nav.nav_date) AS nav_window_start_date,
            MAX(nav.nav_date) AS nav_window_end_date,
            COUNT(*) AS nav_window_rows
        FROM fund_nav_history nav
        JOIN tracker_target_codes target ON target.fcode = nav.fcode
        JOIN fund_returns returns ON returns.fcode = nav.fcode
        WHERE nav.nav_date BETWEEN date(returns.latest_nav_date, '-365 day') AND returns.latest_nav_date
        GROUP BY nav.fcode
    )
    SELECT
        funds.fcode,
        risk.end_date AS risk_end_date,
        risk.calculated_at AS risk_calculated_at,
        risk.sharpe_ratio,
        risk.sortino_ratio,
        risk.calmar_ratio,
        risk.max_drawdown,
        risk.volatility,
        risk.var_95,
        risk.annual_return,
        risk.positive_ratio,
        funds.scale_billion,
        funds.has_holding_period,
        funds.holding_days,
        funds.is_buyable,
        funds.purchase_status,
        funds.estab_date,
        returns.latest_nav_date,
        nav_window.nav_window_start_date,
        nav_window.nav_window_end_date,
        nav_window.nav_window_rows,
        CAST(
            COALESCE(
                julianday(nav_window.nav_window_end_date) - julianday(nav_window.nav_window_start_date),
                0
            ) AS INTEGER
        ) AS nav_window_coverage_days
    FROM tracker_target_codes target
    JOIN funds ON funds.fcode = target.fcode
    LEFT JOIN fund_risk_metrics risk
      ON risk.fcode = funds.fcode
     AND risk.period = '1y'
    LEFT JOIN fund_returns returns ON returns.fcode = funds.fcode
    LEFT JOIN nav_window ON nav_window.fcode = funds.fcode
    """
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute("CREATE TEMP TABLE tracker_target_codes (fcode TEXT PRIMARY KEY)")
        connection.executemany(
            "INSERT INTO tracker_target_codes (fcode) VALUES (?)",
            [(code,) for code in sorted(tracker_codes)],
        )
        connection.execute("PRAGMA query_only = ON")
        frame = pd.read_sql_query(query, connection)
    finally:
        connection.close()
    frame["fcode"] = frame["fcode"].map(normalize_code)
    frame = frame[frame["fcode"].isin(tracker_codes)].copy()
    frame["estab_date"] = pd.to_datetime(frame.get("estab_date"), errors="coerce")
    today = pd.Timestamp.now().normalize()
    frame["establish_days"] = (today - frame["estab_date"]).dt.days
    return frame


def _classify_risk_snapshot_row(
    row: pd.Series | dict[str, Any],
    *,
    expected_risk_end: str,
    holding_days_threshold: int,
) -> str:
    def _clean_text(value: Any) -> str:
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    risk_end = _clean_text(row.get("risk_end_date"))
    latest_nav_date = _clean_text(row.get("latest_nav_date"))
    try:
        has_holding_period = int(float(row.get("has_holding_period") or 0)) == 1
    except (TypeError, ValueError):
        has_holding_period = False
    try:
        holding_days = int(float(row.get("holding_days") or 0))
    except (TypeError, ValueError):
        holding_days = 0
    try:
        coverage_days = int(float(row.get("nav_window_coverage_days") or 0))
    except (TypeError, ValueError):
        coverage_days = 0
    try:
        nav_rows = int(float(row.get("nav_window_rows") or 0))
    except (TypeError, ValueError):
        nav_rows = 0

    if risk_end and risk_end == expected_risk_end:
        return "aligned"
    if has_holding_period and holding_days > holding_days_threshold:
        return "policy_excluded_holding_period"
    if not latest_nav_date:
        return "source_nav_missing"

    source_snapshot_stale = bool(expected_risk_end and latest_nav_date != expected_risk_end)
    insufficient_history = (
        coverage_days < (365 - RISK_PERIOD_COVERAGE_GRACE_DAYS)
        or nav_rows < RISK_PERIOD_REQUIRED_OBSERVATIONS
    )
    if source_snapshot_stale:
        if risk_end and risk_end == latest_nav_date:
            return "source_nav_stale_risk_aligned"
        if not risk_end and insufficient_history:
            return "source_nav_stale_insufficient_1y_history"
        if not risk_end:
            return "source_nav_stale_risk_missing_unexplained"
        return "source_nav_stale_risk_misaligned"
    if not risk_end and insufficient_history:
        return "insufficient_1y_history"
    if not risk_end:
        return "risk_missing_unexplained"
    return "risk_snapshot_misaligned"


def _numeric_stats(series: pd.Series) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return {
        "valid_count": int(len(values.index)),
        "median": round(float(values.median()), 6) if not values.empty else None,
        "p95": round(float(values.quantile(0.95)), 6) if not values.empty else None,
        "max": round(float(values.max()), 6) if not values.empty else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="核验累积 Tracker 四维辅助评分及风险快照新鲜度。")
    parser.add_argument("--run-id", default="", help="指定 run；默认 current published/latest canonical。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    run_id, payload, artifacts, replay_precision = resolve_run_payload(
        settings,
        run_id=args.run_id,
        project_root=project_root,
    )
    core_artifacts = ((payload.get("artifacts") or {}).get("core") or {})
    tracker_path = resolve_runtime_artifact_path(
        core_artifacts.get("accumulation_tracker_excel"),
        project_root=project_root,
    )
    if tracker_path is None or not tracker_path.exists():
        raise RuntimeError("目标 run 缺少 Core 累积 Tracker。")

    tracker = _read_tracker(tracker_path)
    tracker_codes = set(tracker["基金代码"].tolist())
    inputs = _load_metric_input_frame(Path(settings.database_path), tracker_codes)
    engine = EnhancedScoringEngine(risk_profile="balanced", fund_category="bond")
    scored = engine.calculate_scores(inputs.copy())

    risk_audit_columns = [
        "risk_end_date",
        "risk_calculated_at",
        "latest_nav_date",
        "has_holding_period",
        "holding_days",
        "is_buyable",
        "purchase_status",
        "nav_window_start_date",
        "nav_window_end_date",
        "nav_window_rows",
        "nav_window_coverage_days",
    ]
    comparison = tracker.merge(
        scored[
            [
                "fcode",
                *SCORE_COLUMNS.values(),
                *[column for column in risk_audit_columns if column in scored.columns],
            ]
        ],
        left_on="基金代码",
        right_on="fcode",
        how="left",
    )
    score_summaries: dict[str, Any] = {}
    for display_column, recomputed_column in SCORE_COLUMNS.items():
        stored = pd.to_numeric(comparison.get(display_column), errors="coerce")
        recomputed = pd.to_numeric(comparison.get(recomputed_column), errors="coerce")
        delta = recomputed - stored
        comparison[f"{display_column}_重算值"] = recomputed.round(4)
        comparison[f"{display_column}_变化"] = delta.round(4)
        score_summaries[recomputed_column] = {
            "stored": _numeric_stats(stored),
            "recomputed": _numeric_stats(recomputed),
            "delta": _numeric_stats(delta.abs()),
            "changed_gt_0_01_count": int(delta.abs().gt(0.01).sum()),
            "changed_gt_1_count": int(delta.abs().gt(1.0).sum()),
        }

    stored_risk = pd.to_numeric(comparison.get("风险控制分"), errors="coerce")
    recomputed_risk = pd.to_numeric(comparison.get("risk_score"), errors="coerce")
    comparable = stored_risk.notna() & recomputed_risk.notna()
    spearman = (
        float(stored_risk[comparable].rank(method="average").corr(
            recomputed_risk[comparable].rank(method="average")
        ))
        if int(comparable.sum()) >= 2
        else None
    )
    risk_end_counts = {
        str(key): int(value)
        for key, value in comparison.get("risk_end_date", pd.Series(dtype=object))
        .fillna("")
        .astype(str)
        .replace("", "missing")
        .value_counts()
        .to_dict()
        .items()
    }
    expected_risk_end = str(
        (((payload.get("database_freshness") or {}).get("risk_1y_freshness") or {}).get("risk_1y_effective_end_date"))
        or (((payload.get("database_freshness") or {}).get("effective_nav_snapshot") or {}).get("risk_1y_effective_end_date"))
        or ""
    ).strip()
    aligned_risk_count = int(
        comparison.get("risk_end_date", pd.Series(dtype=object)).fillna("").astype(str).eq(expected_risk_end).sum()
    )
    missing_risk_count = int(comparison.get("risk_end_date", pd.Series(dtype=object)).isna().sum())
    comparison["risk_snapshot_status"] = comparison.apply(
        lambda row: _classify_risk_snapshot_row(
            row,
            expected_risk_end=expected_risk_end,
            holding_days_threshold=int(settings.exclude_holding_days_gt),
        ),
        axis=1,
    )
    risk_snapshot_status_counts = {
        str(key): int(value)
        for key, value in comparison["risk_snapshot_status"].value_counts().to_dict().items()
    }
    attention_statuses = {
        "source_nav_stale_risk_missing_unexplained",
        "source_nav_stale_risk_misaligned",
        "risk_missing_unexplained",
        "risk_snapshot_misaligned",
    }
    risk_snapshot_attention_rows = comparison[
        comparison["risk_snapshot_status"].isin(attention_statuses)
    ].copy()
    risk_snapshot_exception_preview_columns = [
        "基金代码",
        "基金名称",
        "risk_snapshot_status",
        "risk_end_date",
        "latest_nav_date",
        "has_holding_period",
        "holding_days",
        "nav_window_coverage_days",
        "nav_window_rows",
    ]
    risk_snapshot_exception_preview = (
        comparison[comparison["risk_snapshot_status"].ne("aligned")]
        [[column for column in risk_snapshot_exception_preview_columns if column in comparison.columns]]
        .fillna("")
        .to_dict(orient="records")
    )

    largest_changes = comparison.copy()
    largest_changes["risk_score_abs_delta"] = (
        pd.to_numeric(largest_changes.get("risk_score"), errors="coerce")
        - pd.to_numeric(largest_changes.get("风险控制分"), errors="coerce")
    ).abs()
    preview_columns = [
        "基金代码",
        "基金名称",
        "风险控制分",
        "风险控制分_重算值",
        "风险控制分_变化",
        "综合评分",
        "综合评分_重算值",
        "综合评分_变化",
        "risk_end_date",
    ]
    preview = (
        largest_changes.sort_values(
            ["risk_score_abs_delta", "基金代码"],
            ascending=[False, True],
            kind="mergesort",
        )[[column for column in preview_columns if column in largest_changes.columns]]
        .head(30)
        .fillna("")
        .to_dict(orient="records")
    )

    tracker_rebuild_required = _tracker_auxiliary_rebuild_required(score_summaries)
    summary = {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": run_id,
        "replay_precision": replay_precision,
        "tracker_path": str(tracker_path),
        "tracker_row_count": int(len(tracker.index)),
        "metric_input_count": int(len(inputs.index)),
        "expected_risk_end_date": expected_risk_end,
        "aligned_risk_end_count": aligned_risk_count,
        "missing_risk_count": missing_risk_count,
        "risk_end_date_counts": risk_end_counts,
        "risk_snapshot_status_counts": risk_snapshot_status_counts,
        "risk_snapshot_attention_count": int(len(risk_snapshot_attention_rows.index)),
        "risk_snapshot_exception_preview": risk_snapshot_exception_preview,
        "risk_free_rate": engine.risk_free_rate,
        "risk_free_rate_source": engine.risk_free_rate_source,
        "score_summaries": score_summaries,
        "risk_score_spearman_stored_vs_recomputed": round(spearman, 6) if spearman is not None else None,
        "largest_change_preview": preview,
        "interpretation": {
            "auxiliary_only": True,
            "production_formal_score_affected": False,
            "current_tracker_contains_pre_fix_auxiliary_scores": tracker_rebuild_required,
            "next_workflow_rebuild_required": tracker_rebuild_required,
            "risk_snapshot_anomaly_requires_action": bool(len(risk_snapshot_attention_rows.index)),
        },
    }

    audit_dir = settings.reports_dir / "audit" / "tracker_auxiliary_scores"
    audit_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    stem = f"tracker_auxiliary_scores_{run_id}_{timestamp}"
    json_path = audit_dir / f"{stem}.json"
    csv_path = audit_dir / f"{stem}_comparison.csv"
    md_path = audit_dir / f"{stem}.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    comparison.to_csv(csv_path, index=False, encoding="utf-8-sig")
    md_path.write_text(
        "\n".join(
            [
                "# Tracker 四维辅助评分审计",
                "",
                f"- run：`{run_id}`",
                f"- Tracker 行数：`{len(tracker.index)}`",
                f"- 风险快照日对齐：`{aligned_risk_count}`；缺失：`{missing_risk_count}`；目标：`{expected_risk_end}`",
                f"- 风险快照分类：`{json.dumps(risk_snapshot_status_counts, ensure_ascii=False)}`",
                f"- 无策略解释且需处理的风险快照：`{len(risk_snapshot_attention_rows.index)}`",
                f"- 风险控制分变化 >0.01：`{score_summaries['risk_score']['changed_gt_0_01_count']}`",
                f"- 风险控制分变化 >1：`{score_summaries['risk_score']['changed_gt_1_count']}`",
                f"- 新旧风险控制分 Spearman：`{summary['risk_score_spearman_stored_vs_recomputed']}`",
                (
                    "- 该四维分是累积 Tracker 辅助列，不是正式投资评分；"
                    + (
                        "当前仍存在实质差异，下一次真实 workflow 需重建。"
                        if tracker_rebuild_required
                        else "当前 run 已写入修复后的值，无需额外重建。"
                    )
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    print(str(json_path))
    print(str(md_path))
    print(str(csv_path))
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
