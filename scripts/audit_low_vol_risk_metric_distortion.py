from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings
from backend.audit_support import write_compressed_audit_csv
from backend.frozen_strategy_audit import FROZEN_STRATEGY_CONTRACT, bind_frozen_report_sources
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline, write_baseline_verification
from backend.report_builder import LOW_VOL_STYLE_TAGS
from backend.publish_pipeline import (
    compute_low_vol_monthly_shadow_metrics as _compute_low_vol_monthly_shadow_metrics,
    normalize_code as _normalize_code,
)
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload as _resolve_run_payload

CONSISTENCY_DELTA_ALERT = 0.01
POSITIVE_RATIO_GAP_ALERT_PCT = 15.0


def compute_monthly_returns_by_calendar(daily_returns: pd.Series) -> list[float]:
    if daily_returns is None or len(daily_returns) == 0:
        return []
    idx = daily_returns.index
    if not isinstance(idx, pd.DatetimeIndex):
        try:
            idx = pd.to_datetime(idx, errors="raise")
        except Exception:
            return []
    series = pd.Series(daily_returns.to_numpy(copy=False), index=idx)
    grouped = series.groupby(idx.to_period("M"))
    return [float((1 + chunk).prod() - 1) for _label, chunk in grouped]


def compute_monthly_returns_from_daily(
    daily_returns: pd.Series,
    trading_days_per_month: int = 20,
    mode: str = "trading20",
) -> list[float]:
    if mode == "calendar":
        return compute_monthly_returns_by_calendar(daily_returns)
    n_months = len(daily_returns) // trading_days_per_month
    monthly_returns: list[float] = []
    for index in range(n_months):
        start_idx = index * trading_days_per_month
        end_idx = start_idx + trading_days_per_month
        month_data = daily_returns.iloc[start_idx:end_idx]
        monthly_returns.append(float((1 + month_data).prod() - 1))
    return monthly_returns


def _split_reason_codes(value: Any) -> list[str]:
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


def _safe_median(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return round(float(numeric.median()), 6)


def _safe_p90(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return round(float(numeric.quantile(0.9)), 6)


def _safe_count(mask: pd.Series | Iterable[bool]) -> int:
    if isinstance(mask, pd.Series):
        return int(mask.fillna(False).astype(bool).sum())
    return int(sum(1 for item in mask if bool(item)))


def _load_style_threshold_detail_df(payload: dict[str, Any]) -> pd.DataFrame:
    rows = (
        (((payload.get("project_publish_summary") or {}).get("threshold_comparison") or {}).get("style_thresholds"))
        or []
    )
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame()
    detail_df = pd.DataFrame(rows)
    if detail_df.empty or "债基风格标签" not in detail_df.columns:
        return pd.DataFrame()
    detail_df["债基风格标签"] = detail_df["债基风格标签"].fillna("").astype(str)
    detail_df = detail_df[detail_df["债基风格标签"] != "全局基准"].drop_duplicates(subset=["债基风格标签"]).reset_index(drop=True)
    return detail_df


def _empty_monthly_metrics() -> dict[str, Any]:
    return {"trading20_month_count": None, "calendar_month_count": None,
            "consistency_trading20": None, "consistency_calendar": None,
            "positive_month_ratio_trading20_pct": None, "positive_month_ratio_calendar_pct": None,
            "consistency_delta": None, "positive_month_ratio_gap_pct": None}


def _compute_monthly_shadow_metrics_from_series(daily_returns: pd.Series) -> dict[str, Any]:
    """Input is daily growth in percent, the same unit as the stored source."""
    series = pd.to_numeric(daily_returns, errors="coerce")
    series = pd.Series(series.values, index=daily_returns.index).dropna()
    if not all(math.isfinite(value) for value in series):
        return _empty_monthly_metrics()
    if len(series.index) < 21:
        return {**_empty_monthly_metrics(), "trading20_month_count": 0, "calendar_month_count": 0}

    # 统一复用主流程报告中的自然月 shadow 口径，避免报告/API 与审计脚本漂移。
    shared_metrics = _compute_low_vol_monthly_shadow_metrics(series)
    normalized_series = series / 100.0
    normalized_series = normalized_series.iloc[1:]
    trading20_months = compute_monthly_returns_from_daily(normalized_series, trading_days_per_month=20)
    calendar_months = compute_monthly_returns_by_calendar(normalized_series)

    def _consistency(monthly_returns: list[float]) -> float | None:
        if len(monthly_returns) < 2:
            return None
        return round(float(np.std(monthly_returns, ddof=1) * np.sqrt(12)), 6)

    def _positive_month_ratio_pct(monthly_returns: list[float]) -> float | None:
        if len(monthly_returns) < 2:
            return None
        positive_count = sum(1 for item in monthly_returns if item > 0)
        return round(float(positive_count / len(monthly_returns) * 100), 6)

    consistency_trading20 = _consistency(trading20_months)
    consistency_calendar = _consistency(calendar_months)
    positive_ratio_trading20_pct = _positive_month_ratio_pct(trading20_months)
    positive_ratio_calendar_pct = _positive_month_ratio_pct(calendar_months)
    return {
        "trading20_month_count": int(len(trading20_months)),
        "calendar_month_count": int(shared_metrics.get("calendar_month_count") or 0),
        "consistency_trading20": consistency_trading20,
        "consistency_calendar": consistency_calendar,
        "positive_month_ratio_trading20_pct": positive_ratio_trading20_pct,
        "positive_month_ratio_calendar_pct": shared_metrics.get("positive_month_ratio_calendar_pct"),
        "consistency_delta": (
            round(float(consistency_calendar - consistency_trading20), 6)
            if consistency_trading20 is not None and consistency_calendar is not None
            else None
        ),
        "positive_month_ratio_gap_pct": (
            round(float(shared_metrics.get("positive_month_ratio_calendar_pct") - positive_ratio_trading20_pct), 6)
            if positive_ratio_trading20_pct is not None and shared_metrics.get("positive_month_ratio_calendar_pct") is not None
            else None
        ),
    }


def _build_monthly_shadow_frame(
    *, focus_df: pd.DataFrame, risk_window_df: pd.DataFrame, nav_history_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = ["基金代码", "monthly_status", "risk_window_start_date", "risk_window_end_date",
               "risk_data_points", "risk_metric_rowid", "observed_nav_row_count", "missing_daily_growth_count",
               *_empty_monthly_metrics()]
    if focus_df.empty:
        return pd.DataFrame(columns=columns)
    risk = risk_window_df.copy()
    nav = nav_history_df.copy()
    if not risk.empty:
        risk["fcode"] = risk["fcode"].map(_normalize_code)
        if risk["fcode"].duplicated().any():
            raise ValueError("duplicate_frozen_risk_window")
        for field in ("start_date", "end_date"):
            risk[field] = pd.to_datetime(risk[field], errors="coerce")
    if not nav.empty:
        nav["fcode"] = nav["fcode"].map(_normalize_code)
        nav["nav_date"] = pd.to_datetime(nav["nav_date"], errors="coerce")
        if nav["nav_date"].isna().any() or nav.duplicated(["fcode", "nav_date"]).any():
            raise ValueError("invalid_or_duplicate_frozen_nav_date")
        nav["daily_growth"] = pd.to_numeric(nav["daily_growth"], errors="coerce")
    windows = {row["fcode"]: row for row in risk.to_dict(orient="records")}
    groups = {code: group.sort_values("nav_date", kind="stable") for code, group in nav.groupby("fcode")} if not nav.empty else {}
    rows = []
    for code in focus_df["基金代码"].map(_normalize_code):
        row = {column: None for column in columns}
        row.update(基金代码=code, monthly_status="missing_frozen_risk_window", **_empty_monthly_metrics())
        window = windows.get(code)
        if window is None:
            rows.append(row)
            continue
        start, end = window["start_date"], window["end_date"]
        if pd.isna(start) or pd.isna(end) or start > end:
            raise ValueError("invalid_frozen_risk_window_dates")
        row.update(risk_window_start_date=start.date().isoformat(), risk_window_end_date=end.date().isoformat())
        for field, source in (("risk_data_points", "data_points"), ("risk_metric_rowid", "risk_metric_rowid")):
            value = pd.to_numeric(window.get(source), errors="coerce")
            row[field] = int(value) if value is not None and pd.notna(value) and math.isfinite(value) else None
        group = groups.get(code)
        current = group[group["nav_date"].between(start, end)] if group is not None else pd.DataFrame()
        if current.empty:
            row.update(monthly_status="missing_frozen_nav_rows", observed_nav_row_count=0)
        else:
            daily_growth_pct = pd.Series(current["daily_growth"].to_numpy(copy=False), index=current["nav_date"])
            row.update(observed_nav_row_count=len(current), missing_daily_growth_count=int(daily_growth_pct.isna().sum()))
            if not all(math.isfinite(value) for value in daily_growth_pct.dropna()):
                row["monthly_status"] = "nonfinite_daily_growth"
            else:
                metrics = _compute_monthly_shadow_metrics_from_series(daily_growth_pct)
                row.update(metrics)
                row["monthly_status"] = "computed" if metrics["calendar_month_count"] else "insufficient_observed_returns"
        rows.append(row)
    return pd.DataFrame(rows, columns=columns)


def _frozen_monthly_inputs(context: dict[str, Any], focus_codes: set[str]) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    frames = context["frames"]
    names = ("low_vol_risk_windows", "low_vol_nav_history")
    if not all(name in frames for name in names):
        return None
    contract = context["snapshot"]["inputs"].get("low_vol_monthly_input_contract") or {}
    if (contract.get("version") != "low-vol-monthly-inputs-2026-09-11.1"
            or contract.get("daily_growth_unit") != "percent" or contract.get("risk_period") != "1y"
            or contract.get("first_return_policy") != "exclude_window_first_observed_return"
            or contract.get("source") != "actual_in_memory_report_query_results"):
        raise ValueError("invalid_frozen_monthly_input_contract")
    scope = contract.get("scope_codes")
    if not isinstance(scope, list) or not focus_codes <= set(scope):
        raise ValueError("frozen_monthly_scope_incomplete")
    risk, nav = (frames[name] for name in names)
    if not {"fcode", "start_date", "end_date"} <= set(risk.columns) or not {"fcode", "nav_date", "daily_growth"} <= set(nav.columns):
        raise ValueError("frozen_monthly_columns_missing")
    return risk, nav


def _build_style_summary_rows(
    *,
    focus_df: pd.DataFrame,
    official_codes: set[str],
    threshold_detail_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    if focus_df.empty:
        return []

    threshold_map = (
        threshold_detail_df.set_index("债基风格标签").to_dict(orient="index")
        if not threshold_detail_df.empty
        else {}
    )
    rows: list[dict[str, Any]] = []
    grouped = focus_df.groupby("债基风格标签", dropna=False)
    for style, group in grouped:
        current = group.copy()
        style_name = str(style or "").strip() or "未知"
        detail = threshold_map.get(style_name, {})
        base_gate_mask = current.get("基础门禁通过", pd.Series(False, index=current.index)).fillna(False).astype(bool)
        dynamic_blocked_mask = base_gate_mask & current.get(
            "dynamic_threshold_verdict",
            pd.Series("", index=current.index, dtype=object),
        ).fillna("").astype(str).ne("正式推荐")
        mdd_gate_mask = current.get("reason_has_mdd", pd.Series(False, index=current.index)).fillna(False).astype(bool)
        vol_gate_mask = current.get("reason_has_volatility", pd.Series(False, index=current.index)).fillna(False).astype(bool)
        positive_ratio_gate_mask = current.get("reason_has_positive_ratio", pd.Series(False, index=current.index)).fillna(False).astype(bool)
        mdd_or_vol_mask = mdd_gate_mask | vol_gate_mask
        mdd_or_vol_only_mask = current.get("mdd_or_vol_only", pd.Series(False, index=current.index)).fillna(False).astype(bool)
        official_count = _safe_count(
            current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code).isin(official_codes)
        )
        rows.append(
            {
                "债基风格标签": style_name,
                "样本数": int(len(current.index)),
                "基础门禁通过数": _safe_count(base_gate_mask),
                "动态拦截数": _safe_count(dynamic_blocked_mask),
                "正式主榜数": official_count,
                "命中正收益比率门槛数": _safe_count(positive_ratio_gate_mask),
                "命中最大回撤门槛数": _safe_count(mdd_gate_mask),
                "命中波动率门槛数": _safe_count(vol_gate_mask),
                "命中回撤或波动门槛数": _safe_count(mdd_or_vol_mask),
                "仅回撤/波动门槛拦截数": _safe_count(mdd_or_vol_only_mask),
                "月度shadow可复算数": _safe_count(current.get("calendar_month_count", pd.Series(dtype=float)).fillna(0).gt(0)),
                "月桶数量不一致数": _safe_count(
                    current["trading20_month_count"].notna() & current["calendar_month_count"].notna()
                    & current["trading20_month_count"].ne(current["calendar_month_count"])
                ),
                "月桶数量可比较数": _safe_count(current["trading20_month_count"].notna() & current["calendar_month_count"].notna()),
                "一致性差可比较数": int(pd.to_numeric(current["consistency_delta"], errors="coerce").notna().sum()),
                "日频正收益率中位数(%)": _safe_median(current.get("1年-正收益比率", pd.Series(dtype=float))),
                "自然月正收益月占比中位数(%)": _safe_median(current.get("positive_month_ratio_calendar_pct", pd.Series(dtype=float))),
                "自然月vs20日正收益月占比差中位数(%)": _safe_median(current.get("positive_month_ratio_gap_pct", pd.Series(dtype=float))),
                "自然月vs日频正收益率差中位数(%)": _safe_median(current.get("positive_ratio_daily_vs_calendar_gap_pct", pd.Series(dtype=float))),
                "20日一致性中位数": _safe_median(current.get("consistency_trading20", pd.Series(dtype=float))),
                "自然月一致性中位数": _safe_median(current.get("consistency_calendar", pd.Series(dtype=float))),
                "自然月vs20日一致性差中位数": _safe_median(current.get("consistency_delta", pd.Series(dtype=float))),
                "自然月vs20日一致性差P90(abs)": _safe_p90(
                    pd.to_numeric(current.get("consistency_delta"), errors="coerce").abs()
                ),
                "自然月正收益差>=15pct样本数": _safe_count(
                    pd.to_numeric(current.get("positive_ratio_daily_vs_calendar_gap_pct"), errors="coerce").abs().ge(POSITIVE_RATIO_GAP_ALERT_PCT)
                ),
                "自然月一致性差>=0.01样本数": _safe_count(
                    pd.to_numeric(current.get("consistency_delta"), errors="coerce").abs().ge(CONSISTENCY_DELTA_ALERT)
                ),
                "回撤阈值上限": detail.get("1年最大回撤上限"),
                "波动率阈值上限": detail.get("1年波动率上限"),
                "正收益率阈值下限": detail.get("1年正收益率下限"),
                "阈值来源": detail.get("阈值来源"),
                "阈值样本数": detail.get("样本数"),
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            -int(item.get("命中回撤或波动门槛数") or 0),
            -int(item.get("自然月正收益差>=15pct样本数") or 0),
            str(item.get("债基风格标签") or ""),
        ),
    )


def _write_markdown(*, summary: dict[str, Any], output_path: Path) -> None:
    display = lambda value: "未计算" if value is None else str(value)
    lines = ["# 低波债基风险指标口径失真审计", "", f"- 批次：`{summary['run_id']}`",
             f"- 状态：`{summary['status']}`",
             f"- 冻结候选：{display(summary['focus_sample_count'])}；月度可复算：{display(summary['monthly_shadow_ready_count'])}",
             f"- 正收益占比差 >= 15pct：{display(summary['positive_ratio_gap_alert_count'])}",
             f"- 一致性差 >= 0.01：{display(summary['consistency_delta_alert_count'])}", "",
             "百分比日增长率只转换一次；一致性为月收益样本标准差乘以 sqrt(12)。",
             "自然月包括窗口首尾的部分月份，20日分桶舍弃末尾不足20日的片段；这两个样本定义并不等价。",
             "历史未冻结的净值窗口不能使用当前数据库补造，缺失结果不用于判断指标没有影响。", "",
             *summary.get("recommendation", []), ""]
    if summary.get("error"):
        lines.append(summary["error"])
    with output_path.open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="从冻结净值窗口审计低波债基风险指标口径失真。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="")
    source.add_argument("--report", type=Path)
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            payload = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not payload.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = payload["run_id"]
        else:
            run_id, payload, _, _ = _resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2
    summary = {"schema_version": 2, "input_contract": FROZEN_STRATEGY_CONTRACT,
               "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": run_id,
               "source_publication_control": payload.get("publication_control"),
               "status": "baseline_not_verified", "passed": False, "baseline_verification": {},
               "source_verified_after_replay": False, "scope": "frozen_low_vol_monthly_window_observation",
               "later_database_reads": False, "current_upstream_queries": False,
               "wall_clock_used_for_replayed_gates": False, "production_formulas_changed": False,
               "upstream_collection_and_core_risk_recalculation_verified": False,
               "low_vol_styles": list(LOW_VOL_STYLE_TAGS), "focus_sample_count": None,
               "monthly_shadow_ready_count": None, "monthly_comparison_count": None,
               "positive_ratio_gap_alert_count": None, "consistency_delta_alert_count": None,
               "mdd_or_vol_gate_hit_count": None, "mdd_or_vol_only_count": None,
               "style_summary": None, "recommendation": [],
               "formula_contract": {"daily_growth_input_unit": "percent", "compounded_return_unit": "decimal_ratio",
                   "unit_conversion_count": 1, "consistency": "sample_std(monthly_returns, ddof=1) * sqrt(12)",
                   "first_return_policy": "exclude_window_first_observed_return", "missing_daily_growth": "drop_before_first_return_removal",
                   "calendar_partial_month_policy": "include_observed_fragment", "trading20_remainder_policy": "exclude_incomplete_block",
                   "comparison_interpretation": "different_month_buckets_and_boundary_samples_not_equivalent_full_months",
                   "code_sha256": file_sha256(Path(__file__))}}
    exports = {}
    try:
        binding = bind_frozen_report_sources(settings, payload)
        summary["source_binding"] = binding
        baseline, context = verify_frozen_baseline(settings, payload)
        summary["baseline_verification"] = baseline
        if not baseline.get("baseline_verified"):
            raise ValueError(f"frozen_baseline_not_verified:{baseline.get('status')}")
        if args.baseline_only:
            summary.update(status="baseline_verified", passed=True)
            exports["baseline_members"] = context["frames"]["formal_after_all_filters"]
        else:
            candidate = context["result"]["candidate"]
            focus = candidate[candidate["债基风格标签"].isin(LOW_VOL_STYLE_TAGS)].copy()
            focus["基金代码"] = focus["基金代码"].map(_normalize_code)
            reasons = focus["dynamic_threshold_reason_codes"].fillna("").map(_split_reason_codes)
            for column, code in (("reason_has_positive_ratio", "below_positive_ratio_floor"),
                                 ("reason_has_mdd", "mdd_too_high"), ("reason_has_volatility", "volatility_too_high")):
                focus[column] = reasons.map(lambda items, key=code: key in items)
            focus["mdd_or_vol_only"] = reasons.map(lambda items: bool(items) and set(items) <= {"mdd_too_high", "volatility_too_high"})
            summary.update(focus_sample_count=len(focus), mdd_or_vol_gate_hit_count=int((focus["reason_has_mdd"] | focus["reason_has_volatility"]).sum()),
                           mdd_or_vol_only_count=int(focus["mdd_or_vol_only"].sum()))
            inputs = _frozen_monthly_inputs(context, set(focus["基金代码"]))
            if focus.empty:
                summary.update(status="empty_scope", passed=True, monthly_shadow_ready_count=0,
                               monthly_comparison_count=0, positive_ratio_gap_alert_count=0, consistency_delta_alert_count=0,
                               style_summary=[])
            elif inputs is None:
                summary.update(status="missing_frozen_nav_window", missing_frozen_nav_codes=sorted(set(focus["基金代码"])),
                               recommendation=["本报告没有保存当时的低波风险窗口和日增长率，月度比较未完成。"])
                exports["missing_inputs"] = pd.DataFrame({"基金代码": sorted(set(focus["基金代码"])), "缺失阶段": "当时的低波风险窗口及日增长率"})
            else:
                monthly = _build_monthly_shadow_frame(focus_df=focus, risk_window_df=inputs[0], nav_history_df=inputs[1])
                focus = focus.merge(monthly, on="基金代码", how="left", validate="one_to_one")
                focus["positive_ratio_daily_vs_calendar_gap_pct"] = (
                    pd.to_numeric(focus["positive_month_ratio_calendar_pct"], errors="coerce")
                    - pd.to_numeric(focus["1年-正收益比率"], errors="coerce")).round(6)
                official_codes = set(context["frames"]["formal_after_all_filters"]["基金代码"].map(_normalize_code))
                styles = _build_style_summary_rows(focus_df=focus, official_codes=official_codes,
                                                  threshold_detail_df=_load_style_threshold_detail_df(payload))
                comparable = pd.to_numeric(focus["consistency_delta"], errors="coerce").notna()
                ready = pd.to_numeric(focus["calendar_month_count"], errors="coerce").gt(0)
                summary.update(status="computed" if ready.all() else "computed_with_missing_observations", passed=True,
                               monthly_shadow_ready_count=int(ready.sum()), monthly_comparison_count=int(comparable.sum()),
                               monthly_status_counts=focus["monthly_status"].value_counts().to_dict(),
                               positive_ratio_comparison_count=int(focus["positive_ratio_daily_vs_calendar_gap_pct"].notna().sum()),
                               positive_ratio_gap_alert_count=int(focus["positive_ratio_daily_vs_calendar_gap_pct"].abs().ge(POSITIVE_RATIO_GAP_ALERT_PCT).sum()),
                               consistency_delta_alert_count=int(pd.to_numeric(focus["consistency_delta"], errors="coerce").abs().ge(CONSISTENCY_DELTA_ALERT).sum()),
                               style_summary=styles, frozen_monthly_input_contract=context["snapshot"]["inputs"]["low_vol_monthly_input_contract"],
                               recommendation=["差异计数只包含对应指标可比较的样本；保留各项覆盖数和缺失原因。",
                                               "月桶边界、样本数量与收益定义须同时评估；该审计不授权生产阈值切换。"])
                exports.update(samples=focus, style_summary=pd.DataFrame(styles))
        if binding != bind_frozen_report_sources(settings, payload):
            raise ValueError("frozen_source_changed_during_replay")
        summary["source_verified_after_replay"] = True
        summary["baseline_verification_artifact"] = str(write_baseline_verification(settings, baseline))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary.update(status="frozen_audit_failed", passed=False, error=f"{type(exc).__name__}: {exc}",
                       source_verified_after_replay=False, focus_sample_count=None, monthly_shadow_ready_count=None,
                       monthly_comparison_count=None, positive_ratio_gap_alert_count=None, consistency_delta_alert_count=None,
                       mdd_or_vol_gate_hit_count=None, mdd_or_vol_only_count=None, style_summary=None)
        for key in ("monthly_status_counts", "positive_ratio_comparison_count", "frozen_monthly_input_contract"):
            summary.pop(key, None)
        exports = {}
    path = build_audit_artifact_path(settings.reports_dir / "audit/low_vol_risk_metric_distortion",
                                     "low_vol_risk_metric_distortion", run_id=run_id)
    summary["artifacts"] = {}
    for name, frame in exports.items():
        output = path.with_name(f"{path.stem}_{name}.csv.gz")
        summary["artifacts"][name] = {"path": str(output.relative_to(settings.project_root)),
                                      **write_compressed_audit_csv(output, frame)}
    with path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    _write_markdown(summary=summary, output_path=path.with_suffix(".md"))
    print(json.dumps({"artifact_path": str(path), "artifact_sha256": file_sha256(path), "run_id": run_id,
                      "passed": summary["passed"], "status": summary["status"],
                      "monthly_shadow_ready_count": summary["monthly_shadow_ready_count"]}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
