"""Pure collection-log summaries shared by scheduling and historical audits."""
from __future__ import annotations

import json
from typing import Any

from utils.manager_collection_summary import (
    manager_collection_source_errors,
    parse_manager_collection_summary,
)
from utils.period_return_snapshot import period_collection_blocking_count
from utils.detail_collection_summary import detail_collection_source_errors, parse_detail_collection_summary


def classify_collection_log_severity(logs: list[str]) -> dict[str, Any]:
    current_stage = ""
    degraded_stage = ""
    manager_failure_lines = [
        line for line in logs if ("获取经理" in line or ("采集基金" in line and "经理列表" in line)) and "失败" in line
    ]
    manager_retry_lines = [
        line for line in manager_failure_lines if "准备重试" in line
    ]
    manager_terminal_failure_lines = [
        line for line in manager_failure_lines if "准备重试" not in line
    ]
    datas_type_errors = [line for line in logs if "Datas 类型异常" in line]
    summary_by_stage: dict[str, dict[str, int]] = {}
    summary_fields_seen: dict[str, set[str]] = {}
    source_evidence_by_source: dict[str, dict[str, Any]] = {}
    for raw in logs:
        line = str(raw).strip()
        if "api_source_evidence=" in line:
            try:
                evidence = json.loads(line.split("api_source_evidence=", 1)[1])
                if isinstance(evidence, dict) and evidence.get("source") in {
                    "fundMNMangerList", "fundMSNMangerInfo", "fundMNDetailInformation", "companyApi2",
                }:
                    source_evidence_by_source[evidence["source"]] = evidence
                if isinstance(evidence, dict) and evidence.get("task") == "collect-period-returns":
                    source_evidence_by_source["period-returns"] = evidence
            except (ValueError, TypeError):
                pass
        if line.startswith("[") and "] $" in line:
            current_stage = line.split("]", 1)[0].lstrip("[")
            summary_by_stage.setdefault(
                current_stage,
                {"summary_total": 0, "summary_success": 0, "summary_failed": 0},
            )
            continue
        if line.startswith("总任务数:"):
            try:
                value = int(line.split(":")[-1].strip().replace(",", ""))
            except ValueError:
                value = 0
            summary_by_stage.setdefault(
                current_stage or "unknown",
                {"summary_total": 0, "summary_success": 0, "summary_failed": 0},
            )["summary_total"] = value
            summary_fields_seen[current_stage or "unknown"] = set()
        elif line.startswith("成功:"):
            if "success" in summary_fields_seen.setdefault(current_stage or "unknown", set()):
                continue
            summary_fields_seen[current_stage or "unknown"].add("success")
            try:
                value = int(line.split(":")[-1].strip().replace(",", ""))
            except ValueError:
                value = 0
            summary_by_stage.setdefault(
                current_stage or "unknown",
                {"summary_total": 0, "summary_success": 0, "summary_failed": 0},
            )["summary_success"] = value
        elif line.startswith("失败:"):
            if "failed" in summary_fields_seen.setdefault(current_stage or "unknown", set()):
                continue
            summary_fields_seen[current_stage or "unknown"].add("failed")
            try:
                value = int(line.split(":")[-1].strip().replace(",", ""))
            except ValueError:
                value = 0
            summary_by_stage.setdefault(
                current_stage or "unknown",
                {"summary_total": 0, "summary_success": 0, "summary_failed": 0},
            )["summary_failed"] = value
        elif line.startswith("跳过:"):
            if "skipped" in summary_fields_seen.setdefault(current_stage or "unknown", set()):
                continue
            summary_fields_seen[current_stage or "unknown"].add("skipped")
            try:
                skipped = int(line.split(":")[-1].strip().replace(",", ""))
            except ValueError:
                continue
            summary_by_stage.setdefault(current_stage or "unknown", {})["summary_skipped"] = skipped

    manager_source_counts = []
    manager_summary_source = "log_summary"
    for source in ("fundMNMangerList", "fundMSNMangerInfo"):
        evidence = source_evidence_by_source.get(source)
        if not evidence:
            continue
        try:
            total = int(evidence["target_count"])
            if total == 0 and int(evidence.get("request_count") or 0) == 0:
                success = failed = skipped = 0
            else:
                success = int(evidence["success_count"])
                failed = int(evidence["failed_count"])
                skipped = int(evidence.get("skipped_count") or 0)
            if min(total, success, failed, skipped) < 0 or success + failed + skipped != total:
                break
        except (KeyError, ValueError, TypeError):
            break
        manager_source_counts.append({
            "source": source, "summary_total": total, "summary_success": success,
            "summary_failed": failed, "summary_skipped": skipped,
        })
    else:
        if manager_source_counts:
            summary_by_stage["collect-managers"] = {
                field: sum(item[field] for item in manager_source_counts)
                for field in ("summary_total", "summary_success", "summary_failed", "summary_skipped")
            }
            manager_summary_source = "api_source_evidence"

    manager_terminal, manager_summary_errors = parse_manager_collection_summary(logs)
    if manager_terminal is not None:
        manager_summary_errors.extend(manager_collection_source_errors(manager_terminal, [
            item for source, item in source_evidence_by_source.items()
            if source in {"fundMNMangerList", "fundMSNMangerInfo"}
        ]))
        summary_by_stage["collect-managers"] = {
            "summary_total": manager_terminal["target_count"],
            "summary_success": manager_terminal["success_count"],
            "summary_failed": manager_terminal["failed_count"],
            "summary_skipped": manager_terminal["skipped_count"],
        }
        manager_summary_source = "manager_collection_summary"

    company_evidence = source_evidence_by_source.get("companyApi2")
    company_summary_invalid = False
    company_cooldown = 0
    if company_evidence is not None:
        try:
            total, success, failed, skipped = (
                int(company_evidence[key])
                for key in ("target_count", "success_count", "failed_count", "skipped_count")
            )
            if min(total, success, failed, skipped) < 0 or success + failed + skipped != total:
                raise ValueError("company target outcomes do not balance")
            company_cooldown = int(company_evidence.get("cooldown_count") or 0)
            if company_cooldown < 0:
                raise ValueError("negative company cooldown count")
            summary_by_stage["collect-companies"] = {
                "summary_total": total, "summary_success": success,
                "summary_failed": failed, "summary_skipped": skipped,
            }
        except (KeyError, ValueError, TypeError):
            company_summary_invalid = True

    detail_terminal, detail_summary_errors = parse_detail_collection_summary(logs)
    if detail_terminal is not None:
        detail_summary_errors.extend(detail_collection_source_errors(
            detail_terminal, source_evidence_by_source.get("fundMNDetailInformation")))
        if not detail_summary_errors:
            summary_by_stage["collect-details"] = {
                "summary_total": detail_terminal["target_count"], "summary_success": detail_terminal["success_count"],
                "summary_failed": detail_terminal["failed_count"], "summary_skipped": detail_terminal["skipped_count"],
            }
    detail_degraded = bool(detail_summary_errors or (detail_terminal or {}).get("status") == "degraded")
    company_summary = summary_by_stage.get("collect-companies") or {}
    period_evidence = source_evidence_by_source.get("period-returns")
    period_invalid = False
    period_failed = 0
    period_cooldown = 0
    if period_evidence is not None:
        try:
            total, success, period_failed = (period_evidence[key] for key in
                                            ("target_count", "saved_fund_count", "failed_count"))
            if (any(type(value) is not int or value < 0 for value in (total, success, period_failed))
                    or success + period_failed != total):
                raise ValueError("period_return_outcomes_do_not_balance")
            period_cooldown = period_evidence["cooldown_count"]
            if type(period_cooldown) is not int or period_cooldown < 0:
                raise ValueError("period_return_cooldown_invalid")
            period_collection_blocking_count(period_evidence)
            summary_by_stage["collect-period-returns"] = {
                "summary_total": total, "summary_success": success, "summary_failed": period_failed,
                "pending_count": period_evidence.get("pending_count"),
            }
        except (KeyError, TypeError, ValueError):
            period_invalid = True
    company_total = int(company_summary.get("summary_total") or 0)
    company_success = int(company_summary.get("summary_success") or 0)
    company_failed = int(company_summary.get("summary_failed") or 0)
    company_skipped = int(company_summary.get("summary_skipped") or 0)
    company_degraded = company_failed > 0 or company_cooldown > 0 or company_summary_invalid
    manager_summary = summary_by_stage.get("collect-managers") or {}
    detail_summary = summary_by_stage.get("collect-details") or {}
    summary_total = int(manager_summary.get("summary_total") or 0)
    summary_success = int(manager_summary.get("summary_success") or 0)
    summary_failed = int(manager_summary.get("summary_failed") or 0)
    summary_skipped = int(manager_summary.get("summary_skipped") or 0)
    detail_summary_total = int(detail_summary.get("summary_total") or 0)
    detail_summary_success = int(detail_summary.get("summary_success") or 0)
    detail_summary_failed = int(detail_summary.get("summary_failed") or 0)
    if detail_summary_errors:
        # Invalid new evidence cannot fall back to an old/fetch-only success.
        detail_summary_total = detail_summary_success = detail_summary_failed = None
        summary_by_stage["collect-details"] = {
            field: None for field in ("summary_total", "summary_success", "summary_failed", "summary_skipped")
        }
    if manager_terminal is not None or manager_summary_errors:
        manager_degraded = bool(manager_summary_errors or (manager_terminal or {}).get("status") == "degraded")
        manager_reason = ("manager_collection_summary_invalid" if manager_summary_errors
                          else manager_terminal["reason"] if manager_degraded else "")
    else:
        manager_degraded = bool(
            summary_failed > 0
            or (summary_total > 0 and summary_success <= 0 and summary_skipped < summary_total)
            or manager_terminal_failure_lines
        )
        manager_reason = ("manager_collection_failed" if summary_failed > 0 or manager_terminal_failure_lines
                          else "manager_collection_no_usable_records")
    if manager_degraded:
        degraded_stage = "collect-managers"
    elif company_degraded:
        degraded_stage = "collect-companies"
    elif detail_degraded:
        degraded_stage = "collect-details"
    elif period_failed or period_cooldown or period_invalid:
        degraded_stage = "collect-period-returns"
    degraded = manager_degraded or company_degraded or detail_degraded or bool(period_failed or period_cooldown) or period_invalid
    result = {
        "degraded": degraded,
        "degraded_stage": degraded_stage,
        "summary_total": summary_total + company_total,
        "summary_success": summary_success + company_success,
        "summary_failed": summary_failed + company_failed,
        "summary_skipped": summary_skipped + company_skipped,
        "manager_source_counts": manager_source_counts,
        "manager_summary_source": manager_summary_source,
        "detail_summary_total": detail_summary_total,
        "detail_summary_success": detail_summary_success,
        "detail_summary_failed": detail_summary_failed,
        "datas_type_error_count": len(datas_type_errors),
        "manager_failure_count": len(manager_failure_lines),
        "manager_retry_count": len(manager_retry_lines),
        "manager_terminal_failure_count": len(manager_terminal_failure_lines),
        "summary_by_stage": summary_by_stage,
        "reason": (
            manager_reason if manager_degraded
            else "company_collection_summary_invalid" if company_summary_invalid
            else "company_collection_failed" if company_failed > 0
            else "company_collection_retry_cooldown" if company_cooldown > 0
            else "detail_collection_summary_invalid" if detail_summary_errors
            else detail_terminal["reason"] if detail_degraded
            else "period_return_collection_summary_invalid" if period_invalid
            else "period_return_refresh_superseded" if period_evidence and period_evidence.get("screening_blocked") is False and period_evidence.get("superseded_refresh_fcodes")
            else "period_return_initialization_placeholder_quarantined" if period_evidence and period_evidence.get("screening_blocked") is False and period_evidence.get("initialization_placeholder_fcodes")
            else "period_return_date_lag_quarantined" if period_evidence and period_evidence.get("screening_blocked") is False and period_evidence.get("quarantined_date_lag_fcodes")
            else "period_return_unavailable_without_returns" if period_evidence and period_evidence.get("screening_blocked") is False and (period_failed or period_cooldown)
            else "period_return_collection_incomplete" if period_failed
            else "period_return_collection_retry_cooldown" if period_cooldown else ""
        ),
    }
    if manager_terminal is not None or manager_summary_errors:
        result.update({
            "manager_collection_summary": manager_terminal,
            "manager_collection_summary_errors": manager_summary_errors,
        })
    if detail_terminal is not None or detail_summary_errors:
        result.update({
            "detail_collection_summary": detail_terminal,
            "detail_collection_summary_errors": detail_summary_errors,
            "detail_summary_source": "detail_collection_summary",
            "detail_summary_skipped": None if detail_summary_errors else detail_terminal["skipped_count"],
        })
    # Older immutable task explanations predate company collection. Do not add
    # unobserved company counters to their exact log-reproduction contract.
    if company_evidence is not None or "collect-companies" in summary_by_stage:
        result.update({
            "company_summary_total": company_total,
            "company_summary_success": company_success,
            "company_summary_failed": company_failed,
            "company_cooldown_count": company_cooldown,
        })
    if period_evidence is not None:
        result["period_return_collection"] = period_evidence
    return result
