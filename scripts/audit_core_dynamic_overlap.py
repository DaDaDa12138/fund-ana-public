from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from backend.config import load_settings
from core.utils.core_csv_storage import artifact_is_file, read_csv_artifact
from backend.audit_support import write_compressed_audit_csv
from backend.frozen_strategy_audit import (
    FROZEN_STRATEGY_CONTRACT, bind_frozen_report_sources, missing_scenario_inputs,
    run_frozen_strategy_scenario, scenario_gap_frame,
)
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline, write_baseline_verification
from backend.diagnosis_contract import diagnosis_categories
from backend.project_publish_decisions import AUTO_DIAGNOSIS_CATEGORY_COLUMN
from backend.report_inputs import resolve_runtime_artifact_path as _resolve_runtime_artifact_path
from backend.project_quality_thresholds import (
    combined_quality_floor,
)
from backend.publish_pipeline import (
    normalize_code as _normalize_code,
)
from scripts.audit_core_publish_overlap import _load_core_selected_records
from scripts.audit_runtime_utils import build_audit_artifact_path, resolve_run_payload as _resolve_run_payload

CORE_LONG_COLUMNS = [
    "fcode",
    "period_count",
    "avg_combo_score",
    "avg_sharpe",
    "min_return",
    "score_std",
    "comprehensive_score",
]

CORE_SHORT_COLUMNS = [
    "fcode",
    "period_count",
    "avg_combo_score",
    "avg_sharpe",
    "min_return",
    "score_std",
    "comprehensive_score",
]

OVERLAP_REASON_TOKENS = [
    "综合加权得分",
    "近期得分",
    "长期得分",
    "1年夏普比率",
]

DISTINCT_REASON_TOKENS = [
    "1年年化收益",
    "1年索提诺比率",
    "1年卡玛比率",
    "1年正收益比率",
    "1年最大回撤",
    "1年波动率",
    "费率合计",
    "基金规模",
]

METRIC_ORDER = [
    "annual_return",
    "score",
    "sharpe",
    "sortino",
    "calmar",
    "positive_ratio",
    "max_drawdown",
    "volatility",
    "fee",
    "scale",
]

REASON_CODE_TO_METRIC = {
    "below_annual_floor": "annual_return",
    "below_weighted_floor": "score",
    "below_short_floor": "score",
    "below_long_floor": "score",
    "below_sharpe_floor": "sharpe",
    "below_sortino_floor": "sortino",
    "below_calmar_floor": "calmar",
    "below_positive_ratio_floor": "positive_ratio",
    "mdd_too_high": "max_drawdown",
    "volatility_too_high": "volatility",
    "fee_too_high": "fee",
    "scale_too_small": "scale",
}

FOCUS_COMBO_SPECS = [
    ("annual_return_only", ("annual_return",), "annual_return only"),
    ("annual_return_plus_score", ("annual_return", "score"), "annual_return + score"),
    ("annual_return_plus_sharpe", ("annual_return", "sharpe"), "annual_return + sharpe"),
    (
        "annual_return_plus_sharpe_plus_score",
        ("annual_return", "score", "sharpe"),
        "annual_return + sharpe + score",
    ),
]


FOCUS_STYLE_BUCKETS = ["中短债", "综合纯债", "固收指数"]
FOCUS_STYLE_OVERLAP_METRICS = ("annual_return", "score", "sharpe")


def _load_intersection_quality_filters() -> dict[str, float]:
    try:
        from bond_fund_screening.config import INTERSECTION_QUALITY_FILTERS as imported_filters  # type: ignore

        return {str(key): float(value) for key, value in dict(imported_filters).items()}
    except Exception:
        config_path = PROJECT_ROOT / "core" / "bond_fund_screening" / "config.py"
        source = config_path.read_text(encoding="utf-8")
        marker = "INTERSECTION_QUALITY_FILTERS ="
        start = source.find(marker)
        if start == -1:
            raise RuntimeError("无法在 core 配置中解析 INTERSECTION_QUALITY_FILTERS。")
        brace_start = source.find("{", start)
        if brace_start == -1:
            raise RuntimeError("INTERSECTION_QUALITY_FILTERS 缺少字典起始符。")
        depth = 0
        brace_end = -1
        for index in range(brace_start, len(source)):
            char = source[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    brace_end = index
                    break
        if brace_end == -1:
            raise RuntimeError("INTERSECTION_QUALITY_FILTERS 缺少字典结束符。")
        literal = source[brace_start : brace_end + 1]
        payload = ast.literal_eval(literal)
        return {str(key): float(value) for key, value in dict(payload).items()}


def _load_core_side_frame(
    path_value: str | None,
    prefix: str,
    *,
    project_root: Path = PROJECT_ROOT,
) -> pd.DataFrame:
    if not path_value:
        return pd.DataFrame()
    path = _resolve_runtime_artifact_path(path_value, project_root=project_root)
    if path is None:
        return pd.DataFrame()
    if not artifact_is_file(path):
        return pd.DataFrame()
    df = read_csv_artifact(path, dtype={"fcode": str})
    if "fcode" not in df:
        raise ValueError("frozen_core_side_fcode_missing")
    df["基金代码"] = df["fcode"].astype(str).map(_normalize_code)
    if df["基金代码"].eq("").any() or df["基金代码"].duplicated().any():
        raise ValueError("frozen_core_side_identity_invalid")
    rename_map = {column: f"{prefix}_{column}" for column in CORE_LONG_COLUMNS if column != "fcode" and column in df.columns}
    keep_columns = ["基金代码"] + [column for column in CORE_LONG_COLUMNS if column in df.columns and column != "fcode"]
    return df[keep_columns].rename(columns=rename_map)


def _classify_dynamic_reason(reason_text: str) -> str:
    text = str(reason_text or "").strip()
    if not text:
        return "unknown"
    has_overlap = any(token in text for token in OVERLAP_REASON_TOKENS)
    has_distinct = any(token in text for token in DISTINCT_REASON_TOKENS)
    if has_overlap and not has_distinct:
        return "overlap_only"
    if has_distinct:
        return "distinct_metric_involved"
    return "other"


def _reason_metric_flags(reason_text: str) -> dict[str, bool]:
    text = str(reason_text or "").strip()
    return {
        "annual_return": "1年年化收益" in text,
        "score": any(token in text for token in ["综合加权得分", "近期得分", "长期得分"]),
        "sharpe": "1年夏普比率" in text,
        "sortino": "1年索提诺比率" in text,
        "calmar": "1年卡玛比率" in text,
        "positive_ratio": "1年正收益比率" in text,
        "max_drawdown": "1年最大回撤" in text,
        "volatility": "1年波动率" in text,
        "fee": "费率合计" in text,
        "scale": "基金规模" in text,
    }


def _split_reason_codes(value: Any) -> list[str]:
    return [item.strip() for item in str(value or "").split("|") if item.strip()]


def _reason_metric_flags_from_row(row: pd.Series | dict[str, Any]) -> dict[str, bool]:
    if isinstance(row, pd.Series):
        row = row.to_dict()
    flags = _reason_metric_flags(str(row.get("dynamic_threshold_reason") or ""))
    for code in _split_reason_codes(row.get("dynamic_threshold_reason_codes")):
        metric = REASON_CODE_TO_METRIC.get(code)
        if metric:
            flags[metric] = True
    return flags


def _metric_combo_tuple(flags: dict[str, bool]) -> tuple[str, ...]:
    return tuple(metric for metric in METRIC_ORDER if bool(flags.get(metric)))


def _metric_combo_label(metrics: tuple[str, ...]) -> str:
    return " + ".join(metrics) if metrics else "unknown"


def _code_set(df: pd.DataFrame) -> set[str]:
    if df.empty or "基金代码" not in df.columns:
        return set()
    return {
        _normalize_code(code)
        for code in df["基金代码"].astype(str).tolist()
        if _normalize_code(code)
    }


def _load_style_threshold_detail_df(latest_payload: dict[str, Any]) -> pd.DataFrame:
    rows = (
        (((latest_payload.get("project_publish_summary") or {}).get("threshold_comparison") or {}).get("style_thresholds"))
        or []
    )
    if not isinstance(rows, list) or not rows:
        return pd.DataFrame()
    detail_df = pd.DataFrame(rows)
    if detail_df.empty or "债基风格标签" not in detail_df.columns:
        return pd.DataFrame()
    detail_df["债基风格标签"] = detail_df["债基风格标签"].fillna("").astype(str)
    return detail_df[detail_df["债基风格标签"] != "全局基准"].drop_duplicates(subset=["债基风格标签"]).reset_index(drop=True)


def _run_release_shadow(
    *,
    frozen_context: dict[str, Any],
    target_codes: set[str],
) -> dict[str, Any]:
    targets = sorted({_normalize_code(code) for code in target_codes})
    if "" in targets:
        raise ValueError("empty_dynamic_release_code")
    candidate = frozen_context["result"]["candidate"]
    eligible = candidate["基础门禁通过"] & candidate["dynamic_threshold_verdict"].ne("正式推荐")
    if not set(targets) <= _code_set(candidate.loc[eligible]):
        raise ValueError("dynamic_release_targets_not_threshold_blocked")
    release_id = hashlib.sha256(json.dumps(targets, separators=(",", ":")).encode()).hexdigest()[:20]
    cache = frozen_context.setdefault("release_results", {})
    if release_id not in cache:
        result = run_frozen_strategy_scenario(
            frozen_context, scored_universe=frozen_context["frames"]["scored_universe_before_holding_gate"],
            dynamic_release_codes=set(targets),
        )
        cache[release_id] = {"target_codes": targets, "result": result}
    cached = cache[release_id]
    if cached["target_codes"] != targets:
        raise ValueError("dynamic_release_identity_collision")
    result = cached["result"]
    impact = {"release_id": release_id, "status": result["status"], "targeted_count": len(targets),
              "target_codes": targets, "formal_count": None, "direct_final_impact_count": None,
              "added_official_count": None, "removed_official_count": None, "net_final_count_delta": None,
              "direct_final_codes": None, "added_codes": None, "removed_codes": None,
              **missing_scenario_inputs(result)}
    if result["status"] == "computed":
        baseline = _code_set(frozen_context["frames"]["formal_after_all_filters"])
        codes = _code_set(result["official"])
        impact.update(formal_count=len(codes), direct_final_impact_count=len(codes & set(targets)),
                      added_official_count=len(codes - baseline), removed_official_count=len(baseline - codes),
                      net_final_count_delta=len(codes) - len(baseline), direct_final_codes=sorted(codes & set(targets)),
                      added_codes=sorted(codes - baseline), removed_codes=sorted(baseline - codes))
    frozen_context.setdefault("release_requests", []).append(impact)
    return impact


def _release_exports(frozen_context: dict[str, Any]) -> dict[str, pd.DataFrame]:
    exports = {}
    for release_id, item in frozen_context.get("release_results", {}).items():
        result = item["result"]
        if result["status"] == "computed":
            exports[f"release_{release_id}_members"] = result["official"]
        else:
            exports[f"release_{release_id}_missing_inputs"] = scenario_gap_frame(result)
    return exports


def _build_metric_combo_summary(dynamic_blocked_df: pd.DataFrame) -> tuple[dict[str, int], list[dict[str, Any]], dict[str, set[str]]]:
    if dynamic_blocked_df.empty:
        combo_counts = {key: 0 for key, _, _ in FOCUS_COMBO_SPECS}
        return combo_counts, [], {key: set() for key, _, _ in FOCUS_COMBO_SPECS}

    current = dynamic_blocked_df.copy()
    current["metric_combo_tuple"] = current.apply(_reason_metric_flags_from_row, axis=1).map(_metric_combo_tuple)
    current["metric_combo_label"] = current["metric_combo_tuple"].map(_metric_combo_label)
    combo_counter = current["metric_combo_label"].value_counts()
    combo_rows = [
        {
            "combo_label": str(label),
            "count": int(count),
            "share_pct": round(float(count) / len(current.index) * 100, 2),
        }
        for label, count in combo_counter.head(20).items()
    ]

    focus_combo_counts: dict[str, int] = {}
    focus_combo_codes: dict[str, set[str]] = {}
    normalized_codes = current.get("基金代码", pd.Series(dtype=object)).astype(str).map(_normalize_code)
    for focus_key, metrics, _ in FOCUS_COMBO_SPECS:
        mask = current["metric_combo_tuple"] == tuple(metrics)
        focus_combo_counts[focus_key] = int(mask.sum())
        focus_combo_codes[focus_key] = {
            code for code in normalized_codes[mask].tolist() if code
        }
    return focus_combo_counts, combo_rows, focus_combo_codes


def _build_focus_combo_impact_rows(
    *,
    focus_combo_codes: dict[str, set[str]],
    frozen_context: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    label_map = {key: label for key, _, label in FOCUS_COMBO_SPECS}
    for focus_key, _, _ in FOCUS_COMBO_SPECS:
        impact = _run_release_shadow(
            frozen_context=frozen_context,
            target_codes=focus_combo_codes.get(focus_key, set()),
        )
        rows.append(
            {
                "focus_combo_key": focus_key,
                "focus_combo_label": label_map[focus_key],
                **impact,
            }
        )
    return rows


def _build_style_threshold_impact_rows(
    *,
    compare_df: pd.DataFrame,
    dynamic_blocked_df: pd.DataFrame,
    style_threshold_detail_df: pd.DataFrame,
    frozen_context: dict[str, Any],
) -> list[dict[str, Any]]:
    if dynamic_blocked_df.empty:
        return []

    core_selected_style_counts = (
        compare_df.get("债基风格标签", pd.Series(dtype=object))
        .fillna("未知")
        .astype(str)
        .value_counts()
        .to_dict()
    )
    blocked_by_style = (
        dynamic_blocked_df.copy()
        .assign(债基风格标签=dynamic_blocked_df.get("债基风格标签", pd.Series(dtype=object)).fillna("未知").astype(str))
        .groupby("债基风格标签")
    )
    threshold_detail_map = (
        style_threshold_detail_df.set_index("债基风格标签").to_dict(orient="index")
        if not style_threshold_detail_df.empty
        else {}
    )

    rows: list[dict[str, Any]] = []
    for style, style_df in blocked_by_style:
        impact = _run_release_shadow(
            frozen_context=frozen_context,
            target_codes=_code_set(style_df),
        )
        detail = threshold_detail_map.get(str(style), {})
        rows.append(
            {
                "债基风格标签": str(style),
                "core_selected_count": int(core_selected_style_counts.get(str(style), 0)),
                "dynamic_blocked_count": int(len(style_df.index)),
                "threshold_sample_size": _optional_number(detail.get("样本数"), 0),
                "threshold_source": str(detail.get("阈值来源") or ""),
                **impact,
            }
        )
    return sorted(
        rows,
        key=lambda item: (
            -int(item.get("dynamic_blocked_count") or 0),
            -int(item.get("direct_final_impact_count") or 0),
            str(item.get("债基风格标签") or ""),
        ),
    )


def _metric_flag_series(df: pd.DataFrame, metric: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    column = f"reason_has_{metric}"
    if column not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    return df[column].fillna(False).astype(bool)


def _focus_overlap_only_mask(df: pd.DataFrame, focus_metrics: tuple[str, ...]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    focus_any = pd.Series(False, index=df.index, dtype=bool)
    other_any = pd.Series(False, index=df.index, dtype=bool)
    for metric in focus_metrics:
        focus_any = focus_any | _metric_flag_series(df, metric)
    for metric in METRIC_ORDER:
        if metric in focus_metrics:
            continue
        other_any = other_any | _metric_flag_series(df, metric)
    return focus_any & ~other_any


def _optional_number(value: Any, digits: int = 2) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    return round(float(numeric), digits) if numeric is not None and math.isfinite(numeric) else None


def _threshold_delta(current: float | None, baseline: float | None, digits: int = 2) -> float | None:
    if current is None or baseline is None:
        return None
    return round(float(current) - float(baseline), digits)


def _threshold_origin_from_floor(current: float | None, floor_value: float | None) -> str:
    if current is None or floor_value is None:
        return "unknown"
    if abs(float(current) - float(floor_value)) < 1e-9:
        return "style_floor"
    if float(current) > float(floor_value):
        return "style_quantile_uplift"
    return "below_style_floor"


def _score_threshold_origin_from_global(
    *,
    weighted_delta_vs_global: float | None,
    short_delta_vs_global: float | None,
    long_delta_vs_global: float | None,
) -> str:
    deltas = [value for value in [weighted_delta_vs_global, short_delta_vs_global, long_delta_vs_global] if value is not None]
    if not deltas:
        return "unknown"
    if any(value > 0 for value in deltas):
        return "partially_stricter_than_global"
    return "not_stricter_than_global"


def _impact_brief(prefix: str, impact: dict[str, Any]) -> dict[str, Any]:
    return {
        f"{prefix}_status": impact.get("status"),
        f"{prefix}_release_id": impact.get("release_id"),
        f"{prefix}_targeted_count": impact.get("targeted_count"),
        f"{prefix}_direct_final_impact_count": impact.get("direct_final_impact_count"),
        f"{prefix}_net_final_count_delta": impact.get("net_final_count_delta"),
        f"{prefix}_missing_inputs": missing_scenario_inputs(impact),
    }


def _build_focus_style_responsibility_rows(
    *,
    dynamic_blocked_df: pd.DataFrame,
    style_threshold_detail_df: pd.DataFrame,
    frozen_context: dict[str, Any],
) -> list[dict[str, Any]]:
    style_thresholds = frozen_context["result"]["style_thresholds"]
    global_thresholds = frozen_context["result"]["global_thresholds"]
    detail_map = (
        style_threshold_detail_df.set_index("债基风格标签").to_dict(orient="index")
        if not style_threshold_detail_df.empty
        else {}
    )
    combo_map = {focus_key: tuple(metrics) for focus_key, metrics, _ in FOCUS_COMBO_SPECS}
    rows: list[dict[str, Any]] = []

    current = dynamic_blocked_df.copy()
    if not current.empty and "metric_combo_tuple" not in current.columns:
        current["metric_combo_tuple"] = current.apply(_reason_metric_flags_from_row, axis=1).map(_metric_combo_tuple)
    if not current.empty:
        current["债基风格标签"] = current.get("债基风格标签", pd.Series(dtype=object)).fillna("未知").astype(str)

    for style in FOCUS_STYLE_BUCKETS:
        style_df = current[current["债基风格标签"] == style].copy() if not current.empty else pd.DataFrame()
        style_threshold = style_thresholds.get(style) or global_thresholds
        style_floor = combined_quality_floor(style)
        detail = detail_map.get(style, {})

        annual_hit_count = int(_metric_flag_series(style_df, "annual_return").sum()) if not style_df.empty else 0
        score_hit_count = int(_metric_flag_series(style_df, "score").sum()) if not style_df.empty else 0
        sharpe_hit_count = int(_metric_flag_series(style_df, "sharpe").sum()) if not style_df.empty else 0

        overlap_only_mask = _focus_overlap_only_mask(style_df, FOCUS_STYLE_OVERLAP_METRICS) if not style_df.empty else pd.Series(dtype=bool)
        overlap_only_df = style_df[overlap_only_mask].copy() if not style_df.empty else pd.DataFrame()
        overlap_only_impact = _run_release_shadow(
            frozen_context=frozen_context,
            target_codes=_code_set(overlap_only_df),
        )

        weighted_delta_vs_global = _threshold_delta(
            style_threshold.get("weighted_score_min"),
            global_thresholds.get("weighted_score_min"),
        )
        short_delta_vs_global = _threshold_delta(
            style_threshold.get("short_score_min"),
            global_thresholds.get("short_score_min"),
        )
        long_delta_vs_global = _threshold_delta(
            style_threshold.get("long_score_min"),
            global_thresholds.get("long_score_min"),
        )
        annual_delta_vs_style_floor = _threshold_delta(
            style_threshold.get("annual_return_min"),
            style_floor.get("annual_return_min"),
        )
        sharpe_delta_vs_style_floor = _threshold_delta(
            style_threshold.get("sharpe_min"),
            style_floor.get("sharpe_min"),
        )

        row = {
            "债基风格标签": style,
            "threshold_source": str(detail.get("阈值来源") or ""),
            "threshold_sample_size": _optional_number(detail.get("样本数"), 0),
            "dynamic_blocked_count": int(len(style_df.index)),
            "annual_hit_count": annual_hit_count,
            "score_hit_count": score_hit_count,
            "sharpe_hit_count": sharpe_hit_count,
            "annual_score_sharpe_only_count": int(len(overlap_only_df.index)),
            "distinct_metric_involved_count": int(len(style_df.index) - len(overlap_only_df.index)),
            "weighted_score_min": _optional_number(style_threshold.get("weighted_score_min")),
            "short_score_min": _optional_number(style_threshold.get("short_score_min")),
            "long_score_min": _optional_number(style_threshold.get("long_score_min")),
            "annual_return_min": _optional_number(style_threshold.get("annual_return_min")),
            "sharpe_min": _optional_number(style_threshold.get("sharpe_min")),
            "weighted_score_delta_vs_global": weighted_delta_vs_global,
            "short_score_delta_vs_global": short_delta_vs_global,
            "long_score_delta_vs_global": long_delta_vs_global,
            "annual_return_delta_vs_style_floor": annual_delta_vs_style_floor,
            "sharpe_delta_vs_style_floor": sharpe_delta_vs_style_floor,
            "annual_return_threshold_origin": _threshold_origin_from_floor(
                style_threshold.get("annual_return_min"),
                style_floor.get("annual_return_min"),
            ),
            "sharpe_threshold_origin": _threshold_origin_from_floor(
                style_threshold.get("sharpe_min"),
                style_floor.get("sharpe_min"),
            ),
            "score_threshold_origin": _score_threshold_origin_from_global(
                weighted_delta_vs_global=weighted_delta_vs_global,
                short_delta_vs_global=short_delta_vs_global,
                long_delta_vs_global=long_delta_vs_global,
            ),
            **_impact_brief("annual_score_sharpe_only", overlap_only_impact),
        }

        for focus_key, _, _ in FOCUS_COMBO_SPECS:
            combo_tuple = combo_map[focus_key]
            combo_df = style_df[style_df["metric_combo_tuple"] == combo_tuple].copy() if not style_df.empty else pd.DataFrame()
            combo_impact = _run_release_shadow(
                frozen_context=frozen_context,
                target_codes=_code_set(combo_df),
            )
            row[f"{focus_key}_count"] = int(len(combo_df.index))
            row.update(_impact_brief(focus_key, combo_impact))

        rows.append(row)

    return rows


def _safe_median(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    return round(float(numeric.median()), 6)


def _build_metric_snapshot(df: pd.DataFrame, prefix: str) -> dict[str, Any]:
    return {
        f"{prefix}_long_avg_combo_score_median": _safe_median(df.get("long_avg_combo_score", pd.Series(dtype=float))),
        f"{prefix}_long_avg_sharpe_median": _safe_median(df.get("long_avg_sharpe", pd.Series(dtype=float))),
        f"{prefix}_long_min_return_median": _safe_median(df.get("long_min_return", pd.Series(dtype=float))),
        f"{prefix}_long_score_std_median": _safe_median(df.get("long_score_std", pd.Series(dtype=float))),
        f"{prefix}_short_avg_combo_score_median": _safe_median(df.get("short_avg_combo_score", pd.Series(dtype=float))),
        f"{prefix}_short_avg_sharpe_median": _safe_median(df.get("short_avg_sharpe", pd.Series(dtype=float))),
        f"{prefix}_short_min_return_median": _safe_median(df.get("short_min_return", pd.Series(dtype=float))),
        f"{prefix}_short_score_std_median": _safe_median(df.get("short_score_std", pd.Series(dtype=float))),
    }


def _build_compare_frame(
    *,
    core_selected_df: pd.DataFrame,
    long_df: pd.DataFrame,
    short_df: pd.DataFrame,
    publish_track_df: pd.DataFrame,
    official_df: pd.DataFrame,
) -> pd.DataFrame:
    compare = core_selected_df.copy()
    compare["基金代码"] = compare["基金代码"].map(_normalize_code)

    compare = compare.merge(long_df, on="基金代码", how="left")
    compare = compare.merge(short_df, on="基金代码", how="left")

    publish = publish_track_df.copy()
    publish[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = diagnosis_categories(publish)
    publish["基金代码"] = publish["基金代码"].map(_normalize_code)
    publish_keep = [
        "基金代码",
        "项目推荐结论",
        AUTO_DIAGNOSIS_CATEGORY_COLUMN,
        "推荐理由",
        "dynamic_threshold_reason",
        "dynamic_threshold_reason_codes",
        "dynamic_threshold_verdict",
        "dynamic_threshold_source",
        "基础推荐理由",
        "基础门禁通过",
        "综合加权得分",
        "债基风格标签",
    ]
    publish = publish[[column for column in publish_keep if column in publish.columns]].drop_duplicates(subset=["基金代码"])
    compare = compare.merge(publish, on="基金代码", how="left")

    official = official_df.copy()
    official["基金代码"] = official["基金代码"].map(_normalize_code)
    official["进入最终正式主榜"] = True
    compare = compare.merge(
        official[[column for column in ["基金代码", "进入最终正式主榜"] if column in official.columns]].drop_duplicates(subset=["基金代码"]),
        on="基金代码",
        how="left",
    )

    for column in ["基础门禁通过", "进入最终正式主榜"]:
        if column in compare.columns:
            compare[column] = compare[column].where(compare[column].notna(), False).infer_objects(copy=False).astype(bool)
    compare["项目推荐结论"] = compare.get("项目推荐结论", "").fillna("").astype(str)
    compare["dynamic_threshold_verdict"] = compare.get("dynamic_threshold_verdict", "").fillna("").astype(str)
    compare["core_side_membership"] = compare.apply(
        lambda row: (
            "双侧通过"
            if pd.notna(row.get("long_avg_combo_score")) and pd.notna(row.get("short_avg_combo_score"))
            else "仅长期通过"
            if pd.notna(row.get("long_avg_combo_score"))
            else "仅近期通过"
            if pd.notna(row.get("short_avg_combo_score"))
            else "未知"
        ),
        axis=1,
    )
    compare["核心到发布状态"] = compare.apply(
        lambda row: (
            "被基础门禁拦截"
            if not bool(row.get("基础门禁通过"))
            else "被动态阈值拦截"
            if str(row.get("dynamic_threshold_verdict") or "").strip() != "正式推荐"
            else "被发布后复核拦截"
            if str(row.get("项目推荐结论") or "").strip() != "正式推荐"
            else "进入最终正式主榜"
            if bool(row.get("进入最终正式主榜"))
            else "通过发布后复核但未入最终主榜"
        ),
        axis=1,
    )
    compare["dynamic_reason_family"] = compare.get("dynamic_threshold_reason", "").fillna("").astype(str).map(_classify_dynamic_reason)
    reason_flags = compare.apply(_reason_metric_flags_from_row, axis=1)
    for key in METRIC_ORDER:
        compare[f"reason_has_{key}"] = reason_flags.map(lambda item, metric=key: bool(item.get(metric)))
    compare["metric_combo_tuple"] = reason_flags.map(_metric_combo_tuple)
    compare["metric_combo_label"] = compare["metric_combo_tuple"].map(_metric_combo_label)
    return compare.sort_values(
        by=["核心到发布状态", "core_side_membership", "核心类别", "基金代码"],
        ascending=[True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def _bind_dynamic_sources(settings, report: dict[str, Any]) -> dict[str, Any]:
    binding = bind_frozen_report_sources(settings, report)
    directory = (settings.project_root / binding["files"]["report_json"]["path"]).parent / "core_artifacts"
    core = (report.get("artifacts") or {}).get("core") or {}
    for key in ("long_csv", "short_csv"):
        path = _resolve_runtime_artifact_path(core.get(key), project_root=settings.project_root)
        if path is None or not path.is_file():
            raise ValueError(f"frozen_core_side_missing:{key}")
        if not path.resolve().is_relative_to(directory.resolve()):
            raise ValueError(f"frozen_core_side_revision_mismatch:{key}")
        binding["files"][f"core_{key}"] = {"path": str(path.relative_to(settings.project_root)),
                                              "sha256": file_sha256(path)}
    return binding


def _write_markdown(*, run_id: str, summary: dict[str, Any], output_path: Path) -> None:
    display = lambda value: "未计算" if value is None else str(value)
    lines = ["# 核心多周期质量过滤与动态阈值重叠审计", "", f"- 基线批次：`{run_id}`",
             f"- 状态：`{summary['status']}`",
             f"- 已完成情景：{summary['completed_scenario_count']} / {summary['requested_scenario_count']}",
             f"- 核心主榜：{display(summary['core_selected_total'])}；动态门槛拦截：{display(summary['dynamic_blocked_count'])}；最终正式：{display(summary['final_official_count'])}",
             "- 仅释放指定候选的动态阈值；持有期、基础门禁、增强门禁、冻结时钟、Tracker、AutoStock和新增样本复核继续适用。", "",
             "| 重点组合 | 状态 | 候选数 | 直达正式 | 净变化 |", "| --- | --- | ---: | ---: | ---: |"]
    for item in summary.get("dynamic_focus_combo_impact") or []:
        lines.append(f"| {item['focus_combo_label']} | {item['status']} | {item['targeted_count']} | "
                     f"{display(item['direct_final_impact_count'])} | {display(item['net_final_count_delta'])} |")
    lines.extend(["", *summary.get("recommendation", []), ""])
    if summary.get("error"):
        lines.append(summary["error"])
    with output_path.open("x", encoding="utf-8") as stream:
        stream.write("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description="使用同一修订的冻结输入审计核心质量过滤与动态阈值的重叠。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="")
    source.add_argument("--report", type=Path)
    parser.add_argument("--baseline-only", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    project_root = settings.project_root
    try:
        if args.report:
            payload = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not payload.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = payload["run_id"]
        else:
            run_id, payload, _, _ = _resolve_run_payload(settings, run_id=args.run_id, project_root=project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2

    summary = {"schema_version": 2, "input_contract": FROZEN_STRATEGY_CONTRACT,
               "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": run_id,
               "source_publication_control": payload.get("publication_control"),
               "status": "baseline_not_verified", "passed": False, "available": False,
               "baseline_verification": {}, "source_verified_after_replay": False,
               "scope": "frozen_core_decisions_and_report_selection",
               "later_database_reads": False, "current_upstream_queries": False,
               "wall_clock_used_for_replayed_gates": False, "production_formulas_changed": False,
               "upstream_collection_and_core_risk_recalculation_verified": False,
               "core_selected_total": None, "dynamic_blocked_count": None, "base_gate_blocked_count": None,
               "final_official_count": None, "dynamic_focus_combo_impact": [],
               "requested_scenario_count": 0, "completed_scenario_count": 0, "scenarios": [],
               "strategy_comparison_allowed": False, "recommendation": []}
    exports = {}
    try:
        binding = _bind_dynamic_sources(settings, payload)
        summary["source_binding"] = binding
        baseline, context = verify_frozen_baseline(settings, payload)
        summary["baseline_verification"] = baseline
        if not baseline.get("baseline_verified"):
            raise ValueError(f"frozen_baseline_not_verified:{baseline.get('status')}")
        result = context["result"]
        candidate_df, official_df = result["candidate"], result["official"]
        if args.baseline_only:
            summary.update(status="baseline_verified", passed=True)
            exports["baseline_members"] = official_df
        else:
            core_selected = _load_core_selected_records(context["snapshot"]["inputs"]["decision_map"])
            core_paths = binding["files"]
            long_df = _load_core_side_frame(core_paths["core_long_csv"]["path"], "long", project_root=project_root)
            short_df = _load_core_side_frame(core_paths["core_short_csv"]["path"], "short", project_root=project_root)
            compare = _build_compare_frame(core_selected_df=core_selected, long_df=long_df, short_df=short_df,
                                           publish_track_df=result["publish_track"], official_df=official_df)
            blocked = compare[compare["核心到发布状态"].eq("被动态阈值拦截")].copy()
            formal = compare[compare["核心到发布状态"].eq("进入最终正式主榜")].copy()
            detail = _load_style_threshold_detail_df(payload)
            combo_counts, combo_rows, combo_codes = _build_metric_combo_summary(blocked)
            combo_impacts = _build_focus_combo_impact_rows(focus_combo_codes=combo_codes, frozen_context=context)
            style_impacts = _build_style_threshold_impact_rows(compare_df=compare, dynamic_blocked_df=blocked,
                style_threshold_detail_df=detail, frozen_context=context)
            responsibilities = _build_focus_style_responsibility_rows(dynamic_blocked_df=blocked,
                style_threshold_detail_df=detail, frozen_context=context)
            scenarios = context["release_requests"]
            completed = sum(item["status"] == "computed" for item in scenarios)
            complete = completed == len(scenarios)
            side_status = compare.groupby(["core_side_membership", "核心到发布状态"]).size().reset_index(name="count")
            summary.update(status="computed" if complete else "scenario_inputs_incomplete", passed=complete,
                available=True, strategy_comparison_allowed=complete, core_selected_total=len(compare),
                dynamic_blocked_count=len(blocked), base_gate_blocked_count=int(compare["核心到发布状态"].eq("被基础门禁拦截").sum()),
                final_official_count=len(formal), side_membership_counts=compare["core_side_membership"].value_counts().to_dict(),
                status_counts=compare["核心到发布状态"].value_counts().to_dict(),
                dynamic_reason_family_counts=blocked["dynamic_reason_family"].value_counts().to_dict(),
                dynamic_reason_metric_counts={metric: int(blocked[f"reason_has_{metric}"].sum()) for metric in METRIC_ORDER},
                dynamic_focus_combo_counts=combo_counts, dynamic_metric_combo_summary=combo_rows,
                dynamic_focus_combo_impact=combo_impacts, style_threshold_impact=style_impacts,
                focus_style_responsibility=responsibilities, side_status_summary=side_status.to_dict(orient="records"),
                metric_snapshot={**_build_metric_snapshot(compare, "core_selected"),
                                 **_build_metric_snapshot(blocked, "dynamic_blocked"),
                                 **_build_metric_snapshot(formal, "final_official")},
                scenarios=scenarios, requested_scenario_count=len(scenarios), completed_scenario_count=completed,
                unique_release_input_count=len(context["release_results"]),
                reference_assumptions={"core_thresholds": _load_intersection_quality_filters(),
                    "source": "current_code_reference_not_historical_configuration",
                    "style_floor_reference": {style: combined_quality_floor(style) for style in FOCUS_STYLE_BUCKETS},
                    "code_sha256": file_sha256(PROJECT_ROOT / "core/bond_fund_screening/config.py")},
                recommendation=["指标同时命中只能说明关联，不能证明筛选职责重复或阈值应该放松。",
                    "缺少历史输入的情景保持未完成；先取得完整门禁证据，再用成熟前瞻表现评估策略。"])
            exports.update(compare=compare, side_status=side_status, scenario_impacts=pd.DataFrame(scenarios))
            exports.update(_release_exports(context))
        if binding != _bind_dynamic_sources(settings, payload):
            raise ValueError("frozen_source_changed_during_replay")
        summary["source_verified_after_replay"] = True
        summary["baseline_verification_artifact"] = str(write_baseline_verification(settings, baseline))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        summary.update(status="frozen_audit_failed", passed=False, available=False, error=f"{type(exc).__name__}: {exc}",
                       strategy_comparison_allowed=False, source_verified_after_replay=False,
                       core_selected_total=None, dynamic_blocked_count=None, base_gate_blocked_count=None,
                       final_official_count=None, scenarios=[], dynamic_focus_combo_impact=[], completed_scenario_count=0)
        for key in ("style_threshold_impact", "focus_style_responsibility", "metric_snapshot", "side_status_summary",
                    "dynamic_reason_family_counts", "dynamic_reason_metric_counts", "dynamic_focus_combo_counts",
                    "dynamic_metric_combo_summary", "side_membership_counts", "status_counts"):
            summary.pop(key, None)
        exports = {}
    path = build_audit_artifact_path(settings.reports_dir / "audit/core_dynamic_overlap", "core_dynamic_overlap", run_id=run_id)
    summary["artifacts"] = {}
    for name, frame in exports.items():
        output = path.with_name(f"{path.stem}_{name}.csv.gz")
        summary["artifacts"][name] = {"path": str(output.relative_to(project_root)),
                                      **write_compressed_audit_csv(output, frame)}
    with path.open("x", encoding="utf-8") as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    _write_markdown(run_id=run_id, summary=summary, output_path=path.with_suffix(".md"))
    print(json.dumps({"artifact_path": str(path), "artifact_sha256": file_sha256(path), "run_id": run_id,
                      "passed": summary["passed"], "status": summary["status"],
                      "completed_scenario_count": summary["completed_scenario_count"],
                      "requested_scenario_count": summary["requested_scenario_count"]}, ensure_ascii=False), flush=True)
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
