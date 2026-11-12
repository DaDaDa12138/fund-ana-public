"""Fail closed on new publications whose selected inputs cannot be verified."""
from __future__ import annotations

from datetime import date, datetime
import json
import math
from typing import Any

from backend.project_publish_decisions import combine_holding_period_names
from core.bond_fund_screening.core.metric_formula_contract import VERSION
from core.utils.holding_period_policy import holding_period_gate_reason


def publication_truthfulness_checks(summary: dict[str, Any], inputs: dict[str, Any], *,
                                    max_snapshot_age_days: int = 10) -> list[dict[str, Any]]:
    checks = []

    def add(name: str, issues: list[dict[str, Any]]) -> None:
        checks.append({"name": name, "aligned": not issues, "detail": "新发布的数据日期与完整计算输入校验",
                       "violation_count": len(issues), "samples": issues[:30]})

    snapshot = ((summary.get("database_freshness") or {}).get("effective_nav_snapshot") or {}).get("candidate_date")
    age_issues = []
    try:
        source_date = date.fromisoformat(str(snapshot))
        report_date = datetime.fromisoformat(str(summary.get("generated_at"))).date()
        age = (report_date - source_date).days
        if not 0 <= age <= max_snapshot_age_days:
            age_issues.append({"reason": "snapshot_stale_or_future", "age_days": age})
    except (TypeError, ValueError):
        age_issues.append({"reason": "snapshot_date_unverified"})
    add("publication_source_date_verified", age_issues)

    def code(row: dict[str, Any]) -> str:
        return str(row.get("fcode") or row.get("基金代码") or "").strip().zfill(6)

    official = [*(inputs.get("official_rows") or []), *(inputs.get("dynamic_rows") or [])]
    metadata = {code(row): row for row in inputs.get("fund_metadata_rows") or []}
    risks = {code(row): row for row in inputs.get("risk_snapshot_rows") or [] if row.get("period") == "1y"}
    issues = []
    if not official:
        issues.append({"reason": "selected_inputs_missing"})
    for row in official:
        fcode = code(row)
        fund_metadata = metadata.get(fcode, {})
        holding_reason = holding_period_gate_reason(
            has_holding_period=fund_metadata.get("has_holding_period"),
            holding_days=fund_metadata.get("holding_days"),
            holding_period_source=fund_metadata.get("holding_period_source"),
            threshold_days=90,
            fund_name=combine_holding_period_names(
                fund_metadata.get("shortname"), fund_metadata.get("fund_name"),
                fund_metadata.get("fullname"),
            ),
            holding_keywords=fund_metadata.get("holding_keywords"),
        )
        if holding_reason == "holding_days_unknown":
            issues.append({"fcode": fcode, "reason": "holding_days_unknown"})
        elif holding_reason == "holding_period_limit":
            issues.append({"fcode": fcode, "reason": "holding_days_over_limit"})
        risk = risks.get(fcode) or {}
        try:
            contract = json.loads(risk.get("formula_contract_json") or "null")
        except (TypeError, ValueError):
            contract = None
        if (not isinstance(contract, dict) or contract.get("version") != VERSION
                or contract.get("engine") != "database"
                or contract.get("return_source") != "daily_growth"
                or contract.get("return_observations_verified") is not True):
            issues.append({"fcode": fcode, "reason": "risk_total_return_inputs_unverified"})
        if str(risk.get("end_date") or "") != str(snapshot or ""):
            issues.append({"fcode": fcode, "reason": "selected_risk_date_mismatch"})
        for field in ("最新单位净值", "最新累计净值", "1年-年化收益(%)", "1年-最大回撤(%)", "1年-波动率"):
            try:
                value = float(row.get(field))
                valid = math.isfinite(value)
                if "净值" in field:
                    valid = valid and value > 0
            except (TypeError, ValueError, OverflowError):
                valid = False
            if not valid:
                issues.append({"fcode": fcode, "reason": "critical_field_missing", "field": field})
    add("publication_selected_inputs_verified", issues)
    return checks
