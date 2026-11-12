"""Manager career experience and score coverage, using stored upstream values.

TOTALDAYS is career experience, not continuous tenure on a particular fund.
Neither an absent score nor an absent historical coverage record is a zero.
"""
from __future__ import annotations

from copy import deepcopy
import math
from typing import Any

import pandas as pd


MANAGER_METRIC_CONTRACT = {
    "schema_version": 1,
    "career_days": {
        "source": "fundMSNMangerInfo.TOTALDAYS -> fund_managers.total_days",
        "definition": "Minimum known career days among the fund's recorded manager IDs",
        "unit": "days",
        "continuous_fund_tenure": False,
    },
    "average_score": {
        "source": "fundMSNMangerInfo.MGOLD -> fund_managers.score_overall",
        "definition": "Arithmetic mean of available upstream scores, rounded to 4 decimals",
        "missing_score_imputation": False,
        "display_decimals": 2,
    },
    "coverage": {
        "denominator": "Distinct recorded manager IDs; missing profiles remain in the denominator",
        "score_numerator": "Recorded managers with an available numeric score, including a real zero",
        "career_days_numerator": "Recorded managers with available numeric career days, including a real zero",
        "legacy_missing_coverage": "unknown; never reconstructed from a later database",
    },
    "time_basis": {
        "profile_last_updated_at": "Stored profile update time; not an upstream metric valuation date",
        "field_observation_date": None,
        "career_days_extrapolation": False,
    },
    "policy": "Existing known-subset mean, career-day gate and reason codes are unchanged",
    "legacy_names": {
        "manager_tenure_short": "Historical reason code for the career-day gate",
        "manager_stability": "Historical metric key for career-day and score gates",
    },
}

MANAGER_METRIC_COLUMNS = [
    "fcode", "manager_count", "manager_profile_count", "manager_total_days_count",
    "manager_scored_count", "manager_min_total_days", "manager_avg_score_overall",
]
MANAGER_DISPLAY_COLUMNS = [
    "经理从业与评分状态", "经理最短从业天数", "经理平均综合评分",
    "经理评分覆盖", "经理从业数据覆盖",
]
MANAGER_COLUMN_DESCRIPTIONS = {
    "经理从业与评分状态": "现有从业天数与已知评分门禁的结果，不代表本基金连续任职稳定性。",
    "经理最短从业天数": "绑定经理中已知 TOTALDAYS 的最小值；TOTALDAYS 为上游从业天数，不是本基金任职天数，未按当前日期外推。",
    "经理平均综合评分": "仅对有评分的绑定经理计算 MGOLD 算术均值；原始均值保留4位小数，此处显示2位。缺失评分不补0，请结合评分覆盖读取。",
    "经理评分覆盖": "有评分经理人数 / 已记录的唯一绑定经理人数。真实0分计入有评分人数；未记录表示该历史产物缺少覆盖证据。",
    "经理从业数据覆盖": "有从业天数的经理人数 / 已记录的唯一绑定经理人数。未记录绑定不代表已确认基金没有经理。",
}
LEGACY_MANAGER_COLUMNS = {
    "经理稳定性": "经理从业与评分状态",
    "经理最短任职天数": "经理最短从业天数",
    "最新经理稳定性": "最新经理从业与评分状态",
    "最新经理最短任职天数": "最新经理最短从业天数",
}


def manager_metric_contract() -> dict[str, Any]:
    return deepcopy(MANAGER_METRIC_CONTRACT)


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _text(value: Any) -> str:
    if value is None or value is pd.NA or value is pd.NaT:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def manager_ids(value: Any) -> list[str]:
    return list(dict.fromkeys(item.strip() for item in _text(value).replace("|", ",").split(",")
                             if item.strip()))


def build_manager_metrics(metadata_df: pd.DataFrame, manager_df: pd.DataFrame) -> pd.DataFrame:
    if metadata_df.empty or "fcode" not in metadata_df:
        return pd.DataFrame(columns=MANAGER_METRIC_COLUMNS)
    profiles = {}
    for row in manager_df.to_dict(orient="records"):
        key = _text(row.get("mgr_id"))
        if not key:
            continue
        if key in profiles:
            raise ValueError(f"manager_profile_duplicate_id:{key}")
        profiles[key] = row
    rows = []
    for fund in metadata_df.to_dict(orient="records"):
        text = _text(fund.get("fcode"))
        digits = "".join(ch for ch in text if ch.isdigit())
        code = digits.zfill(6) if digits else text
        if not code:
            continue
        ids = manager_ids(fund.get("manager_ids"))
        present = [profiles[key] for key in ids if key in profiles]
        days = [value for row in present if (value := _number(row.get("total_days"))) is not None]
        scores = [value for row in present if (value := _number(row.get("score_overall"))) is not None]
        rows.append({
            "fcode": code, "manager_count": len(ids), "manager_profile_count": len(present),
            "manager_total_days_count": len(days), "manager_scored_count": len(scores),
            "manager_min_total_days": min(days) if days else None,
            "manager_avg_score_overall": round(sum(scores) / len(scores), 4) if scores else None,
        })
    return pd.DataFrame(rows, columns=MANAGER_METRIC_COLUMNS)


def coverage_text(available: Any, total: Any) -> str:
    numerator, denominator = _number(available), _number(total)
    if (numerator is None or denominator is None or not numerator.is_integer()
            or not denominator.is_integer() or not 0 <= numerator <= denominator):
        return "未记录"
    if denominator == 0:
        return "未记录绑定"
    return f"{int(numerator)}/{int(denominator)}"


def coverage_note(row: dict[str, Any]) -> str:
    total = _number(row.get("manager_count"))
    if total is None or total <= 0:
        return ""
    notes = []
    for key, label in (("manager_total_days_count", "从业数据"), ("manager_scored_count", "评分")):
        count = _number(row.get(key))
        if count is not None and 0 < count < total:
            notes.append(f"{label}覆盖不全")
    return "/".join(notes)


def normalize_manager_status(value: Any) -> Any:
    # Translate only known manager statuses, never unrelated stability measures.
    if not isinstance(value, str):
        return value
    if value == "稳定":
        return "已知数据达标"
    return value.replace("任职偏短", "从业偏短")


def normalize_manager_record(row: dict[str, Any]) -> dict[str, Any]:
    """Read legacy report labels without changing immutable files or adding data."""
    result = dict(row)
    for old, new in LEGACY_MANAGER_COLUMNS.items():
        if old in result:
            if new not in result:
                result[new] = result[old]
            del result[old]
    for key in ("经理从业与评分状态", "最新经理从业与评分状态"):
        if key in result:
            result[key] = normalize_manager_status(result[key])
    return result


def normalize_manager_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for old, new in LEGACY_MANAGER_COLUMNS.items():
        if old in result:
            if new not in result:
                result = result.rename(columns={old: new})
            else:
                result = result.drop(columns=[old])
    for key in ("经理从业与评分状态", "最新经理从业与评分状态"):
        if key in result:
            result[key] = result[key].map(normalize_manager_status)
    return result


def with_manager_display_columns(frame: pd.DataFrame, *, default_status: str = "未记录") -> pd.DataFrame:
    """Project available aggregates; never look up missing historical values."""
    result = normalize_manager_frame(frame)
    rows = result.to_dict(orient="records")
    values = {
        "经理从业与评分状态": [default_status for _ in rows],
        "经理最短从业天数": ["" if (value := _number(row.get("manager_min_total_days"))) is None
                            else str(int(value)) for row in rows],
        "经理平均综合评分": ["" if (value := _number(row.get("manager_avg_score_overall"))) is None
                            else f"{value:.2f}" for row in rows],
        "经理评分覆盖": [coverage_text(row.get("manager_scored_count"), row.get("manager_count")) for row in rows],
        "经理从业数据覆盖": [coverage_text(row.get("manager_total_days_count"), row.get("manager_count")) for row in rows],
    }
    for column, entries in values.items():
        if column not in result:
            result[column] = entries
    return result
