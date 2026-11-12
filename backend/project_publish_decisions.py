from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from core.utils.buyability import buyability_label, normalize_buyability
from backend.bond_style_classifier import classify_bond_style
from backend.manager_metrics import coverage_note, coverage_text
from backend.fee_semantics import (
    evaluate_fee_semantics,
    fee_semantics_ready,
    resolve_fee_screening_value,
)
from backend.scale_semantics import resolve_scale_for_thresholds
from core.utils.holding_period_policy import (
    combine_holding_period_names,
    holding_period_gate_reason,
    has_unresolved_holding_signal,
    normalize_holding_days,
    resolve_holding_period_days,
)


PROJECT_RECOMMENDATION_REASONS = {
    "not_buyable": "当前不可申购",
    "buyability_unknown": "申购状态未知，尚未确认可申购",
    "soft_warning": "AutoStock 存在软告警",
    "hard_block": "AutoStock 硬拦截",
    "holding_period_limit": "持有期超过90天",
    "holding_days_unknown": "结构化持有期数据缺失或无效",
    "holding_period_name_unparsed": "名称未识别持有期限制",
    "stale_nav": "净值日期过旧或缺失",
    "missing_key_metrics": "关键收益风险指标缺失",
    "period_return_unready": "区间收益基准日缺失或与净值日期不一致",
    "below_weighted_floor": "综合加权得分低于项目门槛",
    "below_short_floor": "近期得分低于项目门槛",
    "below_long_floor": "长期得分低于项目门槛",
    "below_annual_floor": "1年年化收益低于项目门槛",
    "below_sharpe_floor": "1年夏普比率低于项目门槛",
    "below_sortino_floor": "1年索提诺比率低于项目门槛",
    "below_calmar_floor": "1年卡玛比率低于项目门槛",
    "below_positive_ratio_floor": "1年正收益比率低于项目门槛",
    "fee_too_high": "费率合计高于项目门槛",
    "fee_data_missing": "费率组件缺失或口径未确认",
    "mdd_too_high": "1年最大回撤高于项目门槛",
    "volatility_too_high": "1年波动率高于项目门槛",
    "scale_too_small": "基金规模低于项目门槛",
    "low_publish_confidence": "质量边际过窄，暂不进入正式推荐",
    "manager_data_missing": "基金经理从业天数缺失",
    "manager_tenure_short": "基金经理从业时间偏短",
    "manager_score_low": "基金经理综合评分偏低",
    "manager_score_missing": "基金经理综合评分缺失",
    "manager_stability_watch": "基金经理从业或评分未达门槛，转入自动诊断",
    "scale_report_stale": "基金规模数据过旧",
    "incremental_safety_watch": "新增正式样本即时体检偏弱，转入自动诊断",
    "final_selection_removed": "通过发布门禁，但未进入最终精选名单",
    "final_selection_stability_cap": "正式主榜稳定化收敛，转入自动诊断",
    "score_insufficient": "评分不足",
    "long_term_history_missing_neutral": "长期稳定性历史暂缺，按中性处理",
    "holding_period_unknown": "名称未识别持有期限制",
}

REASON_CODE_GROUPS = {
    "not_buyable": "可投资性",
    "buyability_unknown": "数据与校验",
    "hard_block": "可投资性",
    "soft_warning": "数据与校验",
    "holding_period_limit": "结构性约束",
    "holding_days_unknown": "数据与校验",
    "holding_period_name_unparsed": "结构性约束",
    "holding_period_unknown": "结构性约束",
    "stale_nav": "数据与校验",
    "missing_key_metrics": "数据与校验",
    "period_return_unready": "数据与校验",
    "below_weighted_floor": "质量评分",
    "below_short_floor": "质量评分",
    "below_long_floor": "质量评分",
    "below_annual_floor": "质量评分",
    "below_sharpe_floor": "质量评分",
    "below_sortino_floor": "质量评分",
    "below_calmar_floor": "质量评分",
    "below_positive_ratio_floor": "质量评分",
    "fee_too_high": "摩擦与承载",
    "fee_data_missing": "数据与校验",
    "mdd_too_high": "风险约束",
    "volatility_too_high": "风险约束",
    "scale_too_small": "摩擦与承载",
    "low_publish_confidence": "自动发布门禁",
    "manager_data_missing": "自动发布门禁",
    "manager_tenure_short": "自动发布门禁",
    "manager_score_low": "自动发布门禁",
    "manager_score_missing": "自动发布门禁",
    "manager_stability_watch": "自动发布门禁",
    "scale_report_stale": "自动发布门禁",
    "incremental_safety_watch": "自动发布门禁",
    "final_selection_removed": "自动发布门禁",
    "final_selection_stability_cap": "自动发布门禁",
    "score_insufficient": "质量评分",
    "long_term_history_missing_neutral": "数据与校验",
}

HOLDING_PERIOD_NAME_UNPARSED_STATUS = "名称未识别持有期限制"
HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE = "holding_period_name_unparsed"
HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS = "通用占位持有期"
HOLDING_PERIOD_NO_SIGNAL_STATUS = "未见持有期限制信号"
HOLDING_PERIOD_DATA_UNKNOWN_STATUS = "结构化持有期未知"

AUTO_DIAGNOSIS_VERDICT = "自动诊断"
AUTO_DIAGNOSIS_CATEGORY_COLUMN = "自动诊断分类"
AUTO_DIAGNOSIS_POOL_LABEL = "自动诊断池"
NOT_BUYABLE_DIAGNOSIS = "不可申购诊断"
VALIDATION_BLOCK_DIAGNOSIS = "校验拦截诊断"
QUALITY_WARNING_DIAGNOSIS = "质量告警诊断"
ABSOLUTE_QUALITY_DIAGNOSIS = "绝对质量诊断"
DYNAMIC_QUALITY_DIAGNOSIS = "动态质量诊断"
MANAGER_STABILITY_DIAGNOSIS = "经理从业与评分诊断"
AUTO_PUBLISH_GATE_DIAGNOSIS = "自动发布门禁诊断"
FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS = "最终精选未入围"

DYNAMIC_NAV_FRESHNESS_DAYS = 10

PUBLISH_CONFIDENCE_CONFIG = {
    "weighted_high_margin": 6.0,
    "weighted_mid_margin": 2.5,
    "score_high_margin": 6.0,
    "score_mid_margin": 2.0,
    "fee_high_margin": 0.10,
    "fee_mid_margin": 0.05,
    "mdd_high_margin": 0.0030,
    "mdd_mid_margin": 0.0010,
    "scale_high_margin": 8.0,
    "scale_mid_margin": 2.0,
    "high_score_min": 5.0,
    "mid_score_min": 3.0,
}

MANAGER_STABILITY_THRESHOLDS = {
    "min_total_days": 365,
    "avg_score_overall_min": 7.20,
}

MANAGER_SCORE_NEAR_THRESHOLD_RELIEF = {
    "min_weighted_score": 82.5,
    "max_score_shortfall": 0.15,
    "min_total_days": 730,
}

MANAGER_TENURE_NEAR_THRESHOLD_RELIEF = {
    "min_weighted_score": 78.0,
    "max_days_shortfall": 10,
}

ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS = {
    "一级债增强": {
        "annual_return_min": 4.50,
        "sharpe_min": 1.10,
        "calmar_min": 2.60,
        "max_drawdown_max": 0.0800,
        "positive_ratio_min": 55.00,
        "fee_total_max": 1.10,
        "scale_billion_min": 2.0,
        "manager_min_total_days": 365,
        "manager_avg_score_min": 7.10,
    },
    "二级债增强": {
        "annual_return_min": 5.50,
        "sharpe_min": 1.10,
        "calmar_min": 2.60,
        "max_drawdown_max": 0.0800,
        "positive_ratio_min": 55.00,
        "fee_total_max": 1.25,
        "scale_billion_min": 2.0,
        "manager_min_total_days": 365,
        "manager_avg_score_min": 7.10,
    },
    "转债增强": {
        "annual_return_min": 4.00,
        "sharpe_min": 1.10,
        "calmar_min": 3.20,
        "max_drawdown_max": 0.0300,
        "positive_ratio_min": 54.00,
        "fee_total_max": 0.70,
        "scale_billion_min": 2.0,
        "manager_min_total_days": 365,
        "manager_avg_score_min": 7.10,
    },
}

SCALE_DATA_THRESHOLDS = {
    "max_report_age_days": 200,
}

DEFAULT_SCALE_BILLION_MIN = 2.0


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _safe_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        timestamp = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(timestamp):
        return None
    if hasattr(timestamp, "to_pydatetime"):
        return timestamp.to_pydatetime()
    return None


def _is_primary_share_candidate(row: dict[str, Any]) -> bool:
    share_count = _safe_float(row.get("关联份额数") or row.get("share_class_count"))
    if share_count is not None and share_count > 1:
        return False

    fund_name = str(row.get("基金名称") or "").strip()
    if not fund_name:
        return True
    suffix = fund_name[-1]
    if suffix in {"A", "B", "C", "D", "E", "F", "H", "I", "R", "Y"}:
        return suffix == "A"
    return True


def _is_manager_score_near_relief_candidate(
    row: dict[str, Any],
    *,
    manager_avg_score_required: float,
) -> bool:
    weighted_score = _safe_float(row.get("综合加权得分"))
    manager_avg_score = _safe_float(row.get("manager_avg_score_overall"))
    manager_min_days = _safe_float(row.get("manager_min_total_days"))

    if weighted_score is None or weighted_score < float(MANAGER_SCORE_NEAR_THRESHOLD_RELIEF["min_weighted_score"]):
        return False
    if manager_avg_score is None or manager_avg_score < float(manager_avg_score_required) - float(MANAGER_SCORE_NEAR_THRESHOLD_RELIEF["max_score_shortfall"]):
        return False
    if manager_min_days is None or manager_min_days < float(MANAGER_SCORE_NEAR_THRESHOLD_RELIEF["min_total_days"]):
        return False
    if not _is_primary_share_candidate(row):
        return False
    return True


def _is_manager_tenure_near_relief_candidate(
    row: dict[str, Any],
    *,
    manager_min_days_required: int,
) -> bool:
    weighted_score = _safe_float(row.get("综合加权得分"))
    manager_min_days = _safe_float(row.get("manager_min_total_days"))

    if weighted_score is None or weighted_score < float(MANAGER_TENURE_NEAR_THRESHOLD_RELIEF["min_weighted_score"]):
        return False
    if manager_min_days is None or manager_min_days < float(manager_min_days_required) - float(MANAGER_TENURE_NEAR_THRESHOLD_RELIEF["max_days_shortfall"]):
        return False
    if not _is_primary_share_candidate(row):
        return False
    return True


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def reason_code_group(reason_code: str) -> str:
    normalized = str(reason_code or "").strip()
    if not normalized:
        return ""
    return REASON_CODE_GROUPS.get(normalized, "其他")


def summarize_reason_groups(reason_codes: list[str]) -> tuple[str, str]:
    groups: list[str] = []
    for code in reason_codes:
        group = reason_code_group(code)
        if group and group not in groups:
            groups.append(group)
    primary = groups[0] if groups else ""
    joined = "；".join(groups)
    return primary, joined


def normalize_autostock_status(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    mapping = {
        "未校验": "not_checked",
        "not_checked": "not_checked",
        "unknown": "",
        "none": "",
        "null": "",
        "nan": "",
        "<na>": "",
        "n/a": "",
        "通过": "strict_match",
        "严格一致": "strict_match",
        "通过（含软告警）": "soft_pass_with_warnings",
        "soft_pass_with_warnings": "soft_pass_with_warnings",
        "strict_match": "strict_match",
        "拦截": "hard_block",
        "hard_block": "hard_block",
    }
    return mapping.get(lowered, mapping.get(text, text))


DATA_VALIDATION_COLUMNS = (
    "数据校验原始状态", "数据校验状态", "数据校验备注", "数据校验告警原因",
    "数据校验来源", "数据校验契约版本",
)


def validation_status_value(row: Any) -> Any:
    """New-source missing states must never inherit a legacy AutoStock pass."""
    source = str(row.get("数据校验来源") or row.get("validation_source") or "").strip()
    if source and source != "autostock":
        if "数据校验原始状态" in row:
            return row.get("数据校验原始状态")
        if "数据校验状态" in row:
            return row.get("数据校验状态")
        return row.get("validation_status") or row.get("autostock_validation_status") or "not_checked"
    if normalize_autostock_status(row.get("数据校验原始状态")):
        return row.get("数据校验原始状态")
    if normalize_autostock_status(row.get("数据校验状态")):
        return row.get("数据校验状态")
    return row.get("AutoStock原始校验状态") or row.get("AutoStock校验状态") or row.get("autostock_validation_status")


def validation_status_series(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(index=frame.index, dtype=object)
    return frame.apply(validation_status_value, axis=1).map(normalize_autostock_status)


def format_autostock_display_status(value: Any) -> str:
    status = normalize_autostock_status(value)
    if status == "strict_match":
        return "通过"
    if status == "soft_pass_with_warnings":
        return "通过（含软告警）"
    if status == "hard_block":
        return "拦截"
    return "未校验"


def format_autostock_note_flag(value: Any) -> str:
    status = normalize_autostock_status(value)
    if status == "strict_match":
        return "严格校验通过"
    if status == "soft_pass_with_warnings":
        return "存在校验告警"
    if status == "hard_block":
        return "校验拦截"
    return ""


def build_recommendation_note(row: pd.Series | dict[str, Any]) -> str:
    note_parts: list[str] = []
    buyable_text = str(row.get("是否可申购") or "").strip()
    if buyable_text:
        note_parts.append(buyable_text)
    holding_period_status = str(row.get("持有期状态") or "").strip()
    if holding_period_status and holding_period_status != HOLDING_PERIOD_NO_SIGNAL_STATUS:
        note_parts.append(holding_period_status)
    style_tag = str(row.get("债基风格标签") or "").strip()
    if style_tag:
        note_parts.append(style_tag)
    latest_nav_date = _safe_datetime(row.get("最新净值日期") or row.get("latest_nav_date"))
    if latest_nav_date is not None:
        note_parts.append(f"净值日期 {latest_nav_date.date().isoformat()}")
    note_flag = format_autostock_note_flag(validation_status_value(row))
    if note_flag:
        note_parts.append(note_flag)
    fee_total = _safe_float(row.get("费率合计(%)"))
    fee_lower_bound = _safe_float(row.get("费率下界(%)"))
    if fee_total is not None:
        note_parts.append(f"费率合计 {fee_total:.2f}%")
    elif fee_lower_bound is not None:
        note_parts.append(f"费率下界 {fee_lower_bound:.2f}%")
    stability = str(row.get("长期稳定性标签") or "").strip()
    if stability in {"长期历史部分缺失", "长期历史缺失"}:
        note_parts.append("长周期特征未完整")
    return "；".join(note_parts)


def resolve_holding_period_status(
    *,
    has_holding_period: int,
    holding_days: int | None,
    threshold_days: int = 90,
    holding_period_source: str | None = None,
    holding_keywords: str | None = None,
    fund_name: str | None = None,
) -> str:
    """Resolve concrete name/metadata terms; keep no-signal NULL as ordinary."""
    raw_holding_days = normalize_holding_days(holding_days)
    name = combine_holding_period_names(fund_name)
    holding_days = resolve_holding_period_days(
        holding_days=raw_holding_days,
        fund_name=name,
        holding_period_source=holding_period_source,
        holding_keywords=holding_keywords,
    )
    if holding_days in (None, 0) and has_unresolved_holding_signal(
        has_holding_period=has_holding_period, holding_days=raw_holding_days,
        holding_period_source=holding_period_source, holding_keywords=holding_keywords,
        fund_name=name,
    ):
        return HOLDING_PERIOD_NAME_UNPARSED_STATUS
    if holding_days is None:
        if (
            str(holding_period_source or "").strip() == "no_signal_reviewed"
            and int(_safe_float(has_holding_period) or 0) == 0
        ):
            return HOLDING_PERIOD_NO_SIGNAL_STATUS
        if name:
            return HOLDING_PERIOD_NO_SIGNAL_STATUS
        return HOLDING_PERIOD_DATA_UNKNOWN_STATUS
    if holding_days == 0 and int(_safe_float(has_holding_period) or 0) == 1:
        return HOLDING_PERIOD_DATA_UNKNOWN_STATUS
    if holding_days > 0:
        if holding_days > threshold_days:
            return f"已确认>{threshold_days}天"
        return f"已确认<={threshold_days}天"
    return HOLDING_PERIOD_NO_SIGNAL_STATUS


def _verified_holding_days(value: Any) -> int | None:
    return normalize_holding_days(value)


def is_holding_period_name_unparsed_status(value: Any) -> bool:
    return str(value or "").strip() == HOLDING_PERIOD_NAME_UNPARSED_STATUS


def apply_holding_period_name_unparsed_confidence_penalty(
    *,
    holding_period_status: str,
    label: str,
    score: float,
    note: str,
    high_score_min: float,
    mid_score_min: float,
) -> tuple[str, float, str]:
    if not is_holding_period_name_unparsed_status(holding_period_status):
        return label, round(float(score), 2), note

    note_parts = [item for item in [str(note or "").strip(), "持有期限制未确认，正式发布须先确认期限"] if item]
    # No calibrated confidence-point penalty exists for this evidence state.
    # Keep the numeric evidence score and label unchanged; publication gating
    # handles unresolved restriction signals separately.
    return label, round(float(score), 2), "；".join(note_parts)


def refresh_recommendation_notes(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    if current.empty or "推荐备注" not in current.columns:
        return current
    current["推荐备注"] = [build_recommendation_note(row) for _, row in current.iterrows()]
    return current


def append_project_decision_columns(
    df: pd.DataFrame,
    *,
    decision_map: dict[str, dict[str, Any]],
    soft_warning_map: dict[str, dict[str, Any]],
    evaluated_at: datetime | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    enriched = df.copy()
    project_verdicts: list[str] = []
    watch_buckets: list[str] = []
    watch_reason_texts: list[str] = []
    base_gate_passes: list[bool] = []
    buyable_texts: list[str] = []
    holding_period_statuses: list[str] = []
    style_tags: list[str] = []
    benchmark_families: list[str] = []
    exposure_texts: list[str] = []
    fee_totals: list[float | None] = []
    fee_screening_values: list[float | None] = []
    fee_lower_bounds: list[float | None] = []
    fee_statuses: list[str] = []
    fee_ready_flags: list[bool] = []
    fee_floor_flags: list[bool] = []
    autostock_statuses: list[str] = []
    autostock_warning_texts: list[str] = []
    neutral_status_rows: list[dict[str, Any] | None] = []
    recommendation_notes: list[str] = []
    base_reason_codes_texts: list[str] = []
    investable_statuses: list[str] = []
    data_readiness_statuses: list[str] = []
    quality_statuses: list[str] = []
    publish_statuses: list[str] = []
    primary_reason_groups: list[str] = []
    all_reason_groups: list[str] = []
    now_ts = evaluated_at if evaluated_at is not None else datetime.now()

    for row in enriched.to_dict(orient="records"):
        code = _normalize_code(row.get("fcode") or row.get("基金代码"))
        decision = decision_map.get(code, {})
        soft_warning = soft_warning_map.get(code, {})
        style_info = classify_bond_style(
            ftype=row.get("ftype") or row.get("基金类型"),
            sub_type=row.get("sub_type"),
            benchmark=row.get("benchmark"),
            investment_target=row.get("investment_target"),
            investment_strategy=row.get("investment_strategy"),
        )

        if str(row.get("费率口径") or "").strip():
            fee_status = str(row.get("费率口径") or "").strip()
            fee_total = _safe_float(row.get("费率合计(%)"))
            fee_screening_value = resolve_fee_screening_value(row)
            fee_lower_bound = _safe_float(row.get("费率下界(%)"))
            fee_ready = fee_semantics_ready(row)
            fee_uses_floor = bool(row.get("费率筛选使用下界"))
        else:
            fee_semantics = evaluate_fee_semantics(
                fcode=code,
                mgr_fee_rate=row.get("mgr_fee_rate"),
                trust_fee_rate=row.get("trust_fee_rate"),
                sales_fee_rate=row.get("sales_fee_rate"),
            )
            fee_status = str(fee_semantics["status"])
            fee_total = fee_semantics["exact_total"]
            fee_screening_value = fee_semantics["screening_value"]
            fee_lower_bound = fee_semantics["lower_bound"]
            fee_ready = bool(fee_semantics["screening_ready"])
            fee_uses_floor = bool(fee_semantics["uses_floor"])
        is_buyable = normalize_buyability(row.get("is_buyable"))
        has_holding_period = int(_safe_float(row.get("has_holding_period")) or 0)
        holding_days = normalize_holding_days(row.get("holding_days"))
        latest_nav_date = _safe_datetime(row.get("latest_nav_date") or row.get("最新净值日期"))
        has_stale_nav = (
            latest_nav_date is None
            or (now_ts - latest_nav_date).days > DYNAMIC_NAV_FRESHNESS_DAYS
        )
        weighted_score = _safe_float(row.get("综合加权得分"))
        short_score = _safe_float(row.get("近期得分"))
        long_score = _safe_float(row.get("长期得分"))
        annual_return = _safe_float(row.get("1年-年化收益(%)"))
        sharpe_ratio = _safe_float(row.get("1年-夏普比率"))
        max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
        has_missing_key_metrics = any(
            value is None
            for value in [weighted_score, short_score, long_score, annual_return, sharpe_ratio, max_drawdown]
        )
        # Frozen pre-migration reports have no status and keep their original
        # replay contract. Current database projections always carry it.
        period_unready = "period_return_status" in row and row["period_return_status"] != "dated"
        validation_source = str(decision.get("validation_source") or row.get("数据校验来源")
                                or row.get("validation_source") or "").strip()
        neutral_source = bool(validation_source and validation_source != "autostock")
        autostock_status = (validation_status_value(row) if "数据校验原始状态" in row else
                           decision.get("validation_status") or decision.get("autostock_validation_status")
                           or validation_status_value(row) or "")
        autostock_status = normalize_autostock_status(autostock_status)
        autostock_warning = str(
            decision.get("validation_warning_display")
            or decision.get("autostock_warning_display")
            or row.get("数据校验告警原因")
            or soft_warning.get("warning_display")
            or ""
        ).strip()
        holding_name = combine_holding_period_names(
            row.get("基金名称"), row.get("shortname"), row.get("fund_name"),
            row.get("fullname"), row.get("基金全称"),
        )
        holding_period_status = resolve_holding_period_status(
            has_holding_period=has_holding_period,
            holding_days=holding_days,
            threshold_days=90,
            holding_period_source=row.get("holding_period_source"),
            fund_name=holding_name,
            holding_keywords=row.get("持有期关键词") or row.get("holding_keywords"),
        )

        reason_codes: list[str] = []
        holding_reason = holding_period_gate_reason(
            has_holding_period=has_holding_period,
            holding_days=holding_days,
            holding_period_source=row.get("holding_period_source"),
            threshold_days=90,
            fund_name=holding_name,
            holding_keywords=row.get("持有期关键词") or row.get("holding_keywords"),
        )
        if holding_reason:
            reason_codes.append(holding_reason)
        if is_buyable is None:
            reason_codes.append("buyability_unknown")
        elif is_buyable is False:
            reason_codes.append("not_buyable")
        if has_stale_nav:
            reason_codes.append("stale_nav")
        if has_missing_key_metrics:
            reason_codes.append("missing_key_metrics")
        if period_unready:
            reason_codes.append("period_return_unready")
        if not fee_ready:
            reason_codes.append("fee_data_missing")
        if autostock_status == "hard_block":
            reason_codes.append("hard_block")
        has_soft_warning = autostock_status == "soft_pass_with_warnings"

        if "not_buyable" in reason_codes:
            watch_bucket = NOT_BUYABLE_DIAGNOSIS
        elif "hard_block" in reason_codes:
            watch_bucket = VALIDATION_BLOCK_DIAGNOSIS
        elif "holding_days_unknown" in reason_codes or "holding_period_limit" in reason_codes or "holding_period_name_unparsed" in reason_codes or "buyability_unknown" in reason_codes or "stale_nav" in reason_codes or "missing_key_metrics" in reason_codes or "fee_data_missing" in reason_codes or period_unready:
            watch_bucket = QUALITY_WARNING_DIAGNOSIS
        else:
            watch_bucket = ""

        reason_text = "；".join(PROJECT_RECOMMENDATION_REASONS[code_name] for code_name in reason_codes)
        verdict = "待定" if not reason_codes else AUTO_DIAGNOSIS_VERDICT
        primary_reason_group, all_reason_group = summarize_reason_groups(reason_codes)

        if "not_buyable" in reason_codes or "hard_block" in reason_codes or "holding_period_limit" in reason_codes:
            investable_status = "不可投资"
        elif "buyability_unknown" in reason_codes or "holding_days_unknown" in reason_codes or "holding_period_name_unparsed" in reason_codes:
            investable_status = "待确认"
        else:
            investable_status = "可投资"
        if "holding_days_unknown" in reason_codes or "holding_period_name_unparsed" in reason_codes or "buyability_unknown" in reason_codes or "stale_nav" in reason_codes or "missing_key_metrics" in reason_codes or "fee_data_missing" in reason_codes or period_unready:
            data_readiness_status = "待补齐"
        else:
            data_readiness_status = "已就绪"
        quality_status = "待评估" if holding_reason or is_buyable is None or has_missing_key_metrics or not fee_ready or period_unready else "已通过基础门禁"
        publish_status = "基础门禁拦截" if reason_codes else "进入后续主链"

        note_parts = [buyability_label(is_buyable)]
        if holding_period_status and holding_period_status != HOLDING_PERIOD_NO_SIGNAL_STATUS:
            note_parts.append(holding_period_status)
        if style_info["style_tag"]:
            note_parts.append(style_info["style_tag"])
        if latest_nav_date is not None:
            note_parts.append(f"净值日期 {latest_nav_date.date().isoformat()}")
        note_flag = format_autostock_note_flag(autostock_status)
        if note_flag:
            note_parts.append(note_flag)
        if has_soft_warning and autostock_warning:
            note_parts.append(autostock_warning)
        if fee_total is not None:
            note_parts.append(f"费率合计 {fee_total:.2f}%")
        elif fee_lower_bound is not None:
            note_parts.append(f"费率下界 {fee_lower_bound:.2f}%")

        project_verdicts.append(verdict)
        watch_buckets.append(watch_bucket)
        watch_reason_texts.append(reason_text)
        base_gate_passes.append(not reason_codes)
        buyable_texts.append(buyability_label(is_buyable))
        holding_period_statuses.append(holding_period_status)
        style_tags.append(style_info["style_tag"])
        benchmark_families.append(style_info["benchmark_family"])
        exposure_texts.append("是" if style_info["has_equity_convertible_exposure"] else "否")
        fee_totals.append(fee_total)
        fee_screening_values.append(fee_screening_value)
        fee_lower_bounds.append(fee_lower_bound)
        fee_statuses.append(fee_status)
        fee_ready_flags.append(fee_ready)
        fee_floor_flags.append(fee_uses_floor)
        autostock_statuses.append(str(row.get("AutoStock原始校验状态") or "") if neutral_source else autostock_status or "unknown")
        autostock_warning_texts.append(str(row.get("AutoStock告警原因") or "") if neutral_source else autostock_warning)
        neutral_status_rows.append({
            "数据校验原始状态": autostock_status or "not_checked",
            "数据校验状态": format_autostock_display_status(autostock_status),
            "数据校验告警原因": autostock_warning,
            "数据校验备注": autostock_warning,
            "数据校验来源": validation_source,
            "数据校验契约版本": decision.get("validation_contract") or row.get("数据校验契约版本") or row.get("validation_contract") or "",
        } if neutral_source else None)
        recommendation_notes.append("；".join(note_parts))
        base_reason_codes_texts.append("|".join(reason_codes))
        investable_statuses.append(investable_status)
        data_readiness_statuses.append(data_readiness_status)
        quality_statuses.append(quality_status)
        publish_statuses.append(publish_status)
        primary_reason_groups.append(primary_reason_group)
        all_reason_groups.append(all_reason_group)

    enriched["项目推荐结论"] = project_verdicts
    enriched[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = watch_buckets
    enriched["推荐理由"] = watch_reason_texts
    enriched["基础门禁通过"] = base_gate_passes
    enriched["基础观察分类"] = watch_buckets
    enriched["基础推荐理由"] = watch_reason_texts
    enriched["基础推荐原因代码"] = base_reason_codes_texts
    enriched["可投资性状态"] = investable_statuses
    enriched["数据就绪状态"] = data_readiness_statuses
    enriched["质量结论状态"] = quality_statuses
    enriched["发布结论状态"] = publish_statuses
    enriched["主原因分组"] = primary_reason_groups
    enriched["原因分组汇总"] = all_reason_groups
    enriched["是否可申购"] = buyable_texts
    enriched["持有期状态"] = holding_period_statuses
    enriched["债基风格标签"] = style_tags
    enriched["基准归类"] = benchmark_families
    enriched["是否含权益/转债增强"] = exposure_texts
    enriched["费率合计(%)"] = fee_totals
    enriched["费率筛选值(%)"] = fee_screening_values
    enriched["费率下界(%)"] = fee_lower_bounds
    enriched["费率口径"] = fee_statuses
    enriched["费率筛选就绪"] = fee_ready_flags
    enriched["费率筛选使用下界"] = fee_floor_flags
    enriched["AutoStock原始校验状态"] = autostock_statuses
    enriched["AutoStock告警原因"] = autostock_warning_texts
    if any(item is not None for item in neutral_status_rows):
        for column in DATA_VALIDATION_COLUMNS:
            enriched[column] = [item.get(column, "") if item is not None else "" for item in neutral_status_rows]
        neutral_mask = pd.Series([item is not None for item in neutral_status_rows], index=enriched.index)
        for column in ("推荐理由", "基础推荐理由", "推荐备注"):
            if column in enriched:
                enriched.loc[neutral_mask, column] = enriched.loc[neutral_mask, column].str.replace("AutoStock", "数据校验", regex=False)
    enriched["推荐备注"] = recommendation_notes
    return enriched


def confidence_from_thresholds(
    row: dict[str, Any],
    *,
    thresholds: dict[str, float],
) -> tuple[str, float, str]:
    weighted_score = _safe_float(row.get("综合加权得分"))
    short_score = _safe_float(row.get("近期得分"))
    long_score = _safe_float(row.get("长期得分"))
    annual_return = _safe_float(row.get("1年-年化收益(%)"))
    sharpe_ratio = _safe_float(row.get("1年-夏普比率"))
    sortino_ratio = _safe_float(row.get("1年-索提诺比率"))
    calmar_ratio = _safe_float(row.get("1年-卡玛比率"))
    positive_ratio = _safe_float(row.get("1年-正收益比率"))
    fee_total = resolve_fee_screening_value(row)
    max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
    volatility = _safe_float(row.get("1年-波动率"))
    scale_billion = resolve_scale_for_thresholds(row)

    score = 0.0
    notes: list[str] = []

    def _score_high_mid(delta: float | None, *, high: float, mid: float, positive_label: str) -> None:
        nonlocal score
        if delta is None:
            notes.append(f"{positive_label}缺失")
            return
        if delta >= high:
            score += 1.0
            notes.append(f"{positive_label}边际充足")
        elif delta >= mid:
            score += 0.5
            notes.append(f"{positive_label}边际一般")
        else:
            notes.append(f"{positive_label}贴线")

    weighted_margin = None if weighted_score is None else weighted_score - float(thresholds["weighted_score_min"])
    short_margin = None if short_score is None else short_score - float(thresholds["short_score_min"])
    long_margin = None if long_score is None else long_score - float(thresholds["long_score_min"])
    ann_margin = None if annual_return is None else annual_return - float(thresholds["annual_return_min"])
    sharpe_margin = None if sharpe_ratio is None else sharpe_ratio - float(thresholds["sharpe_min"])
    sortino_margin = None if sortino_ratio is None else sortino_ratio - float(thresholds["sortino_min"])
    calmar_margin = None if calmar_ratio is None else calmar_ratio - float(thresholds["calmar_min"])
    positive_margin = None if positive_ratio is None else positive_ratio - float(thresholds["positive_ratio_min"])
    fee_margin = None if fee_total is None else float(thresholds["fee_total_max"]) - fee_total
    mdd_margin = None if max_drawdown is None else float(thresholds["max_drawdown_max"]) - max_drawdown
    volatility_margin = None if volatility is None else float(thresholds["volatility_max"]) - volatility
    scale_margin = None if scale_billion is None else scale_billion - float(thresholds.get("scale_billion_min") or DEFAULT_SCALE_BILLION_MIN)

    _score_high_mid(
        weighted_margin,
        high=PUBLISH_CONFIDENCE_CONFIG["weighted_high_margin"],
        mid=PUBLISH_CONFIDENCE_CONFIG["weighted_mid_margin"],
        positive_label="综合得分",
    )
    _score_high_mid(
        short_margin,
        high=PUBLISH_CONFIDENCE_CONFIG["score_high_margin"],
        mid=PUBLISH_CONFIDENCE_CONFIG["score_mid_margin"],
        positive_label="近期得分",
    )
    _score_high_mid(
        long_margin,
        high=PUBLISH_CONFIDENCE_CONFIG["score_high_margin"],
        mid=PUBLISH_CONFIDENCE_CONFIG["score_mid_margin"],
        positive_label="长期得分",
    )
    _score_high_mid(ann_margin, high=1.00, mid=0.30, positive_label="年化收益")
    _score_high_mid(sharpe_margin, high=0.35, mid=0.10, positive_label="夏普")
    _score_high_mid(sortino_margin, high=0.35, mid=0.10, positive_label="索提诺")
    _score_high_mid(calmar_margin, high=1.20, mid=0.40, positive_label="卡玛")
    _score_high_mid(positive_margin, high=3.00, mid=1.00, positive_label="正收益率")
    _score_high_mid(
        fee_margin,
        high=PUBLISH_CONFIDENCE_CONFIG["fee_high_margin"],
        mid=PUBLISH_CONFIDENCE_CONFIG["fee_mid_margin"],
        positive_label="费率",
    )
    _score_high_mid(
        mdd_margin,
        high=PUBLISH_CONFIDENCE_CONFIG["mdd_high_margin"],
        mid=PUBLISH_CONFIDENCE_CONFIG["mdd_mid_margin"],
        positive_label="回撤",
    )
    _score_high_mid(volatility_margin, high=0.0080, mid=0.0030, positive_label="波动")
    _score_high_mid(
        scale_margin,
        high=PUBLISH_CONFIDENCE_CONFIG["scale_high_margin"],
        mid=PUBLISH_CONFIDENCE_CONFIG["scale_mid_margin"],
        positive_label="规模",
    )

    if score >= 7.5:
        label = "高信心"
    elif score >= 4.5:
        label = "中信心"
    else:
        label = "边界观察"
    return apply_holding_period_name_unparsed_confidence_penalty(
        holding_period_status=str(row.get("持有期状态") or "").strip(),
        label=label,
        score=score,
        note="；".join(notes),
        high_score_min=7.5,
        mid_score_min=4.5,
    )


def confidence_from_enhanced_metrics(
    row: dict[str, Any],
    *,
    thresholds: dict[str, float],
) -> tuple[str, float, str]:
    annual_return = _safe_float(row.get("1年-年化收益(%)"))
    sharpe_ratio = _safe_float(row.get("1年-夏普比率"))
    calmar_ratio = _safe_float(row.get("1年-卡玛比率"))
    positive_ratio = _safe_float(row.get("1年-正收益比率"))
    max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
    fee_total = resolve_fee_screening_value(row)
    scale_billion = resolve_scale_for_thresholds(row)

    score = 0.0
    notes: list[str] = []

    def _score_high_mid(delta: float | None, *, high: float, mid: float, positive_label: str) -> None:
        nonlocal score
        if delta is None:
            notes.append(f"{positive_label}缺失")
            return
        if delta >= high:
            score += 1.0
            notes.append(f"{positive_label}边际充足")
        elif delta >= mid:
            score += 0.5
            notes.append(f"{positive_label}边际一般")
        else:
            notes.append(f"{positive_label}贴线")

    ann_margin = None if annual_return is None else annual_return - float(thresholds["annual_return_min"])
    sharpe_margin = None if sharpe_ratio is None else sharpe_ratio - float(thresholds["sharpe_min"])
    calmar_margin = None if calmar_ratio is None else calmar_ratio - float(thresholds["calmar_min"])
    pos_margin = None if positive_ratio is None else positive_ratio - float(thresholds["positive_ratio_min"])
    mdd_margin = None if max_drawdown is None else float(thresholds["max_drawdown_max"]) - max_drawdown
    fee_margin = None if fee_total is None else float(thresholds["fee_total_max"]) - fee_total
    scale_margin = None if scale_billion is None else scale_billion - float(thresholds["scale_billion_min"])

    _score_high_mid(ann_margin, high=1.50, mid=0.50, positive_label="年化收益")
    _score_high_mid(sharpe_margin, high=0.40, mid=0.15, positive_label="夏普")
    _score_high_mid(calmar_margin, high=1.50, mid=0.50, positive_label="卡玛")
    _score_high_mid(pos_margin, high=4.00, mid=1.50, positive_label="正收益率")
    _score_high_mid(mdd_margin, high=0.0040, mid=0.0015, positive_label="回撤")
    _score_high_mid(fee_margin, high=0.10, mid=0.05, positive_label="费率")
    _score_high_mid(scale_margin, high=8.0, mid=2.0, positive_label="规模")

    if score >= 5.0:
        label = "高信心"
    elif score >= 3.0:
        label = "中信心"
    else:
        label = "边界观察"
    return apply_holding_period_name_unparsed_confidence_penalty(
        holding_period_status=str(row.get("持有期状态") or "").strip(),
        label=label,
        score=score,
        note="；".join(notes),
        high_score_min=5.0,
        mid_score_min=3.0,
    )


def project_confidence_label(
    row: dict[str, Any],
    *,
    occurrences: int = 1,
    current_active: bool = True,
) -> str:
    weighted_score = _safe_float(row.get("综合加权得分")) or 0.0
    short_score = _safe_float(row.get("近期得分")) or 0.0
    long_score = _safe_float(row.get("长期得分")) or 0.0
    fee_total = resolve_fee_screening_value(row)
    max_drawdown = _safe_float(row.get("1年-最大回撤(%)"))
    scale_billion = resolve_scale_for_thresholds(row)
    annual_return = _safe_float(row.get("1年-年化收益(%)") or row.get("最新年化收益(%)"))
    sharpe_ratio = _safe_float(row.get("1年-夏普比率") or row.get("最新夏普比率"))
    calmar_ratio = _safe_float(row.get("1年-卡玛比率"))
    positive_ratio = _safe_float(row.get("1年-正收益比率"))

    score = 0.0
    score += 2.0 if weighted_score >= 68 else 1.0 if weighted_score >= 63 else 0.0
    score += 1.0 if short_score >= 68 else 0.5 if short_score >= 63 else 0.0
    score += 1.0 if long_score >= 68 else 0.5 if long_score >= 63 else 0.0
    if annual_return is not None:
        score += 1.5 if annual_return >= 5.0 else 1.0 if annual_return >= 4.0 else 0.5 if annual_return >= 3.0 else 0.0
    if sharpe_ratio is not None:
        score += 1.0 if sharpe_ratio >= 1.8 else 0.5 if sharpe_ratio >= 1.3 else 0.0
    if calmar_ratio is not None:
        score += 1.0 if calmar_ratio >= 5.0 else 0.5 if calmar_ratio >= 3.5 else 0.0
    if positive_ratio is not None:
        score += 0.5 if positive_ratio >= 60.0 else 0.25 if positive_ratio >= 56.0 else 0.0
    if fee_total is not None:
        score += 1.0 if fee_total <= 0.35 else 0.5 if fee_total <= 0.45 else 0.0
    if max_drawdown is not None:
        score += 1.0 if max_drawdown <= 0.005 else 0.5 if max_drawdown <= 0.010 else 0.0
    if scale_billion is not None:
        score += 1.0 if scale_billion >= 10 else 0.5 if scale_billion >= 5 else 0.0
    score += 1.5 if occurrences >= 3 else 0.8 if occurrences >= 2 else 0.0
    score += 0.5 if current_active else 0.0

    if score >= 6.0:
        return "高信心"
    if score >= 4.0:
        return "中信心"
    return "边界观察"


def apply_publish_post_filters(
    df: pd.DataFrame,
    *,
    threshold_mode: str,
    style_thresholds: dict[str, dict[str, float]],
    global_thresholds: dict[str, float],
    evaluated_at: datetime | None = None,
) -> pd.DataFrame:
    current = df.copy()
    if current.empty:
        return current

    verdicts: list[str] = []
    buckets: list[str] = []
    reasons: list[str] = []
    confidence_labels: list[str] = []
    confidence_scores: list[float] = []
    confidence_notes: list[str] = []
    manager_statuses: list[str] = []
    manager_days: list[str] = []
    manager_scores: list[str] = []
    manager_score_coverage: list[str] = []
    manager_days_coverage: list[str] = []
    scale_statuses: list[str] = []
    scale_age_values: list[str] = []
    threshold_modes: list[str] = []
    admission_modes: list[str] = []
    reason_codes_texts: list[str] = []
    investable_statuses: list[str] = []
    data_readiness_statuses: list[str] = []
    quality_statuses: list[str] = []
    publish_statuses: list[str] = []
    primary_reason_groups: list[str] = []
    all_reason_groups: list[str] = []

    now_ts = evaluated_at if evaluated_at is not None else datetime.now()
    source_verdict_col = "dynamic_threshold_verdict" if threshold_mode == "dynamic" else "fixed_threshold_verdict"
    source_bucket_col = "dynamic_threshold_watch_bucket" if threshold_mode == "dynamic" else "fixed_threshold_watch_bucket"
    source_reason_col = "dynamic_threshold_reason" if threshold_mode == "dynamic" else "fixed_threshold_reason"
    source_reason_codes_col = "dynamic_threshold_reason_codes" if threshold_mode == "dynamic" else "fixed_threshold_reason_codes"

    for row in current.to_dict(orient="records"):
        verdict = str(row.get(source_verdict_col) or "").strip()
        bucket = str(row.get(source_bucket_col) or "").strip()
        reason_text = str(row.get(source_reason_col) or "").strip()
        source_reason_codes = [
            item.strip()
            for item in str(row.get(source_reason_codes_col) or "").split("|")
            if item.strip()
        ]

        manager_min_days = _safe_float(row.get("manager_min_total_days"))
        manager_avg_score = _safe_float(row.get("manager_avg_score_overall"))
        manager_count = _safe_float(row.get("manager_count"))
        manager_days_count = _safe_float(row.get("manager_total_days_count"))
        manager_scored_count = _safe_float(row.get("manager_scored_count"))
        scale_report_date = _safe_datetime(row.get("scale_report_date"))
        scale_age_days = (now_ts - scale_report_date).days if scale_report_date is not None else None

        style = str(row.get("债基风格标签") or "未知").strip() or "未知"
        supplemental_pass = bool(row.get("enhanced_supplemental_pass")) and threshold_mode == "dynamic"
        supplemental_thresholds = ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS.get(style) if supplemental_pass else None
        thresholds = global_thresholds
        if threshold_mode == "dynamic":
            thresholds = style_thresholds.get(style) or global_thresholds
        if supplemental_pass and supplemental_thresholds:
            confidence_label, confidence_score, confidence_note = confidence_from_enhanced_metrics(
                row,
                thresholds=supplemental_thresholds,
            )
        else:
            confidence_label, confidence_score, confidence_note = confidence_from_thresholds(row, thresholds=thresholds)

        extra_reason_codes: list[str] = []
        holding_period_status = str(row.get("持有期状态") or "").strip()
        if is_holding_period_name_unparsed_status(holding_period_status):
            extra_reason_codes.append(HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE)
        manager_status = "已知数据达标"
        manager_min_days_required = int((supplemental_thresholds or {}).get("manager_min_total_days") or MANAGER_STABILITY_THRESHOLDS["min_total_days"])
        manager_avg_score_required = float((supplemental_thresholds or {}).get("manager_avg_score_min") or MANAGER_STABILITY_THRESHOLDS["avg_score_overall_min"])
        if (manager_min_days is None or
                (manager_count is not None and manager_days_count is not None
                 and manager_days_count < manager_count)):
            manager_status = "数据缺失"
            extra_reason_codes.append("manager_data_missing")
        else:
            if manager_min_days < manager_min_days_required:
                manager_status = "从业偏短"
                extra_reason_codes.append("manager_tenure_short")
            if manager_avg_score is None:
                manager_status = "评分缺失" if manager_status == "已知数据达标" else f"{manager_status}/评分缺失"
                extra_reason_codes.append("manager_score_missing")
            elif manager_avg_score < manager_avg_score_required:
                manager_status = "评分偏低" if manager_status == "已知数据达标" else f"{manager_status}/评分偏低"
                extra_reason_codes.append("manager_score_low")

        scale_status = "新鲜"
        if scale_age_days is None or scale_age_days > SCALE_DATA_THRESHOLDS["max_report_age_days"]:
            scale_status = "过旧"
            extra_reason_codes.append("scale_report_stale")

        if confidence_label == "边界观察":
            extra_reason_codes.append("low_publish_confidence")

        if len(extra_reason_codes) == 1:
            if extra_reason_codes == ["manager_score_low"] and _is_manager_score_near_relief_candidate(
                row,
                manager_avg_score_required=manager_avg_score_required,
            ):
                extra_reason_codes = []
                manager_status = "近门槛缓释"
            elif extra_reason_codes == ["manager_tenure_short"] and _is_manager_tenure_near_relief_candidate(
                row,
                manager_min_days_required=manager_min_days_required,
            ):
                extra_reason_codes = []
                manager_status = "近门槛缓释"

        if verdict == "正式推荐" and extra_reason_codes:
            verdict = AUTO_DIAGNOSIS_VERDICT
            manager_related_codes = {"manager_data_missing", "manager_tenure_short", "manager_score_low", "manager_score_missing"}
            non_manager_codes = [code for code in extra_reason_codes if code not in manager_related_codes]
            if any(code in manager_related_codes for code in extra_reason_codes) and not non_manager_codes:
                bucket = MANAGER_STABILITY_DIAGNOSIS
                merged_reason_text = PROJECT_RECOMMENDATION_REASONS["manager_stability_watch"]
            else:
                bucket = AUTO_PUBLISH_GATE_DIAGNOSIS
                merged_reason_text = "；".join(PROJECT_RECOMMENDATION_REASONS[code] for code in extra_reason_codes)
            merged_reasons = [item for item in [reason_text, merged_reason_text] if item]
            reason_text = "；".join(merged_reasons)
            source_reason_codes = [*source_reason_codes, *extra_reason_codes]

        primary_reason_group, all_reason_group = summarize_reason_groups(source_reason_codes)
        investable_blocked = any(code in {"not_buyable", "hard_block"} for code in source_reason_codes)
        data_blocked = any(code in {"buyability_unknown", "stale_nav", "missing_key_metrics", "soft_warning", "period_return_unready"} for code in source_reason_codes)
        quality_blocked = any(
            code in {
                "below_weighted_floor",
                "below_short_floor",
                "below_long_floor",
                "below_annual_floor",
                "below_sharpe_floor",
                "below_sortino_floor",
                "below_calmar_floor",
                "below_positive_ratio_floor",
                "fee_too_high",
                "mdd_too_high",
                "volatility_too_high",
                "scale_too_small",
                "score_insufficient",
            }
            for code in source_reason_codes
        )
        publish_blocked = any(
            code in {
                "manager_data_missing",
                "manager_tenure_short",
                "manager_score_low",
                "manager_score_missing",
                "holding_period_name_unparsed",
                "scale_report_stale",
                "low_publish_confidence",
            }
            for code in source_reason_codes
        )

        investable_status = (
            "不可投资" if investable_blocked else
            "待确认" if "buyability_unknown" in source_reason_codes else "可投资"
        )
        data_readiness_status = "待补齐" if data_blocked else "已就绪"
        if "buyability_unknown" in source_reason_codes:
            quality_status = "待评估"
        elif supplemental_pass and verdict == "正式推荐":
            quality_status = "质量通过（补充准入）"
        else:
            quality_status = "质量未通过" if quality_blocked else "质量通过"
        if verdict == "正式推荐":
            publish_status = "正式发布"
        elif publish_blocked:
            publish_status = "自动发布门禁拦截"
        elif quality_blocked:
            publish_status = "质量筛选拦截"
        elif investable_blocked:
            publish_status = "可投资性拦截"
        elif data_blocked:
            publish_status = "数据就绪拦截"
        else:
            publish_status = AUTO_DIAGNOSIS_VERDICT

        verdicts.append(verdict)
        buckets.append(bucket)
        reasons.append(reason_text)
        confidence_labels.append(confidence_label)
        confidence_scores.append(confidence_score)
        confidence_notes.append(confidence_note)
        note = coverage_note(row)
        manager_statuses.append(f"{manager_status}/{note}" if note else manager_status)
        manager_days.append("" if manager_min_days is None else str(int(manager_min_days)))
        manager_scores.append("" if manager_avg_score is None else f"{manager_avg_score:.2f}")
        manager_score_coverage.append(coverage_text(row.get("manager_scored_count"), row.get("manager_count")))
        manager_days_coverage.append(coverage_text(row.get("manager_total_days_count"), row.get("manager_count")))
        scale_statuses.append(scale_status)
        scale_age_values.append("" if scale_age_days is None else str(int(scale_age_days)))
        reason_codes_texts.append("|".join(source_reason_codes))
        investable_statuses.append(investable_status)
        data_readiness_statuses.append(data_readiness_status)
        quality_statuses.append(quality_status)
        publish_statuses.append(publish_status)
        primary_reason_groups.append(primary_reason_group)
        all_reason_groups.append(all_reason_group)
        if supplemental_pass:
            threshold_modes.append("动态阈值 + 增强补充准入 + 自动发布门禁")
            admission_modes.append("增强债基补充准入")
        else:
            threshold_modes.append("动态阈值" if threshold_mode == "dynamic" else "固定阈值")
            admission_modes.append("标准动态阈值" if threshold_mode == "dynamic" else "固定阈值审计")

    current["项目推荐结论"] = verdicts
    current[AUTO_DIAGNOSIS_CATEGORY_COLUMN] = buckets
    current["推荐理由"] = reasons
    current["项目准入方式"] = admission_modes
    current["项目质量信心"] = confidence_labels
    current["质量边际得分"] = confidence_scores
    current["质量边际说明"] = confidence_notes
    current["经理从业与评分状态"] = manager_statuses
    current["经理最短从业天数"] = manager_days
    current["经理平均综合评分"] = manager_scores
    current["经理评分覆盖"] = manager_score_coverage
    current["经理从业数据覆盖"] = manager_days_coverage
    current["规模数据状态"] = scale_statuses
    current["规模数据账龄(天)"] = scale_age_values
    current["发布阈值模式"] = threshold_modes
    current["项目推荐原因代码"] = reason_codes_texts
    current["可投资性状态"] = investable_statuses
    current["数据就绪状态"] = data_readiness_statuses
    current["质量结论状态"] = quality_statuses
    current["发布结论状态"] = publish_statuses
    current["主原因分组"] = primary_reason_groups
    current["原因分组汇总"] = all_reason_groups
    return current


__all__ = [
    "DYNAMIC_NAV_FRESHNESS_DAYS",
    "ABSOLUTE_QUALITY_DIAGNOSIS",
    "AUTO_DIAGNOSIS_CATEGORY_COLUMN",
    "AUTO_DIAGNOSIS_POOL_LABEL",
    "AUTO_DIAGNOSIS_VERDICT",
    "AUTO_PUBLISH_GATE_DIAGNOSIS",
    "DYNAMIC_QUALITY_DIAGNOSIS",
    "ENHANCED_STYLE_SUPPLEMENTAL_THRESHOLDS",
    "FINAL_SELECTION_NOT_SELECTED_DIAGNOSIS",
    "HOLDING_PERIOD_GENERIC_PLACEHOLDER_STATUS",
    "HOLDING_PERIOD_NO_SIGNAL_STATUS",
    "HOLDING_PERIOD_NAME_UNPARSED_REASON_CODE",
    "HOLDING_PERIOD_NAME_UNPARSED_STATUS",
    "MANAGER_STABILITY_DIAGNOSIS",
    "MANAGER_SCORE_NEAR_THRESHOLD_RELIEF",
    "MANAGER_TENURE_NEAR_THRESHOLD_RELIEF",
    "MANAGER_STABILITY_THRESHOLDS",
    "NOT_BUYABLE_DIAGNOSIS",
    "PROJECT_RECOMMENDATION_REASONS",
    "PUBLISH_CONFIDENCE_CONFIG",
    "QUALITY_WARNING_DIAGNOSIS",
    "SCALE_DATA_THRESHOLDS",
    "VALIDATION_BLOCK_DIAGNOSIS",
    "append_project_decision_columns",
    "apply_holding_period_name_unparsed_confidence_penalty",
    "apply_publish_post_filters",
    "build_recommendation_note",
    "combine_holding_period_names",
    "confidence_from_enhanced_metrics",
    "confidence_from_thresholds",
    "format_autostock_display_status",
    "format_autostock_note_flag",
    "is_holding_period_name_unparsed_status",
    "normalize_autostock_status",
    "project_confidence_label",
    "reason_code_group",
    "refresh_recommendation_notes",
    "resolve_holding_period_status",
    "summarize_reason_groups",
]
