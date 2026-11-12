"""Explain known undefined ratios without treating an incomplete row as ready."""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import date
from typing import Any


RISK_RATIO_FIELDS = {
    "sharpe_ratio": "1年-夏普比率",
    "sortino_ratio": "1年-索提诺比率",
    "calmar_ratio": "1年-卡玛比率",
}
RISK_DISPLAY_FIELDS = (
    "1年-波动率", "1年-最大回撤(%)", "1年-年化收益(%)",
    *RISK_RATIO_FIELDS.values(), "1年-VaR95(%)", "1年-VaR99(%)", "1年-正收益比率",
)
_REASONS = {
    "sharpe_ratio": ("zero_or_negligible_volatility", "窗口内波动率为零或低于计算精度，夏普比率无法定义"),
    "sortino_ratio": ("zero_or_negligible_downside_deviation", "窗口内下行偏差及现行替代分母为零或低于计算精度，索提诺比率无法定义"),
    "calmar_ratio": ("zero_drawdown", "窗口内最大回撤为零，标准卡玛比率无法定义"),
}


def _finite(value: Any) -> bool:
    if value is None or isinstance(value, bool) or value == "":
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def prepare_risk_display_row(row: dict[str, Any]) -> dict[str, Any]:
    """Preserve a computed window's values; unknown incomplete rows stay missing.

    Publication and screening readiness still require the original non-null
    risk fields. This function is only used to display a database snapshot.
    """
    result = dict(row)
    raw = row.get("__risk_undefined_reasons_json")
    try:
        reasons = json.loads(raw) if isinstance(raw, str) else {}
    except (ValueError, TypeError):
        reasons = {}
    if not isinstance(reasons, dict):
        reasons = {}
    end_date = str(row.get("__risk_end_date") or "").strip()
    try:
        date.fromisoformat(end_date)
        valid_end_date = True
    except ValueError:
        valid_end_date = False
    unavailability = {}
    for metric, field in RISK_RATIO_FIELDS.items():
        reason, label = _REASONS[metric]
        if valid_end_date and reasons.get(metric) == reason and not _finite(row.get(field)):
            unavailability[field] = {
                "reason_code": reason, "label": label, "end_date": end_date or None,
                "source": "database_risk_calculation",
            }
    base_complete = all(_finite(row.get(field)) for field in RISK_DISPLAY_FIELDS if field not in RISK_RATIO_FIELDS.values())
    known_ratios = all(_finite(row.get(field)) or field in unavailability for field in RISK_RATIO_FIELDS.values())
    known_window = (
        bool(unavailability) and base_complete and known_ratios
        and _finite(row.get("__risk_data_points")) and float(row["__risk_data_points"]) > 0
        and valid_end_date
    )
    if row.get("__risk_snapshot_ready") not in (True, 1) and not known_window:
        for field in RISK_DISPLAY_FIELDS:
            result[field] = None
        unavailability = {}
    if unavailability:
        result["__risk_metric_unavailability"] = unavailability
    return result


def overlay_risk_unavailability(row: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    """Explain missing values or correct a legacy zero with a matching NAV date."""
    result = dict(row)
    attached = dict(result.get("__risk_metric_unavailability") or {})
    snapshot_date = str(row.get("最新净值日期") or "").strip()[:10]
    for field, status in (snapshot.get("__risk_metric_unavailability") or {}).items():
        if field not in RISK_RATIO_FIELDS.values() or not isinstance(status, dict):
            continue
        missing = not _finite(row.get(field))
        same_date = bool(snapshot_date) and snapshot_date == status.get("end_date")
        legacy_zero = not missing and float(row[field]) == 0.0
        if missing or (same_date and legacy_zero):
            result[field] = ""
            attached[field] = dict(status)
    if attached:
        result["__risk_metric_unavailability"] = attached
    return result


def load_undefined_risk_windows(connection: sqlite3.Connection) -> dict[tuple[str, str], dict[str, Any]]:
    """Read validated explanations for latest computed windows, without making them ready."""
    from backend.effective_nav import _latest_risk_rowid_predicate, _risk_metric_ready_predicate

    fields = {
        "volatility": "1年-波动率", "max_drawdown": "1年-最大回撤(%)", "annual_return": "1年-年化收益(%)",
        **RISK_RATIO_FIELDS, "var_95": "1年-VaR95(%)", "var_99": "1年-VaR99(%)", "positive_ratio": "1年-正收益比率",
    }
    columns = {row[1] for row in connection.execute("PRAGMA table_info(fund_risk_metrics)")}
    required = {*fields, "fcode", "period", "end_date", "data_points", "undefined_metric_reasons_json"}
    if not required <= columns:
        return {}
    expressions = []
    for key, label in fields.items():
        expression = f"COALESCE(rm.raw_{key}, rm.{key})" if key in {"sharpe_ratio", "sortino_ratio"} and f"raw_{key}" in columns else f"rm.{key}"
        expressions.append(f'{expression} AS "{label}"')
    predicate = _latest_risk_rowid_predicate("rm", "latest")
    ready = _risk_metric_ready_predicate(connection, "rm")
    cursor = connection.execute(f"""
        SELECT rm.fcode, rm.period, rm.end_date AS __risk_end_date,
               rm.data_points AS __risk_data_points,
               rm.undefined_metric_reasons_json AS __risk_undefined_reasons_json,
               CASE WHEN {ready} THEN 1 ELSE 0 END AS __risk_snapshot_ready,
               {','.join(expressions)}
        FROM fund_risk_metrics rm
        WHERE rm.undefined_metric_reasons_json IS NOT NULL AND {predicate}
    """)
    names = [item[0] for item in cursor.description]
    result = {}
    for values in cursor:
        row = prepare_risk_display_row(dict(zip(names, values)))
        statuses = row.get("__risk_metric_unavailability") or {}
        if statuses:
            result[(row["fcode"], row["period"])] = {
                "end_date": row["__risk_end_date"],
                "undefined_metric_reasons": {key: statuses[label]["reason_code"] for key, label in RISK_RATIO_FIELDS.items() if label in statuses},
                "risk_ready": bool(row["__risk_snapshot_ready"]),
                "calculation_status": "computed_ratio_undefined",
            }
    return result
