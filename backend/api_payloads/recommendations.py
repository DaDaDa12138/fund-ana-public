from __future__ import annotations

from datetime import datetime
from io import BytesIO
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from backend.api_payloads.portfolio_scope import (
    build_portfolio_display_scope,
    build_publish_layer_semantics_summary,
)
from backend.api_payloads.positive_ratio import (
    build_positive_ratio_semantics_summary,
    normalize_low_vol_positive_ratio_shadow_watch,
)
from backend.api_payloads.dashboard import (
    _build_tracker_summary_runtime_scope,
    _build_low_vol_positive_ratio_shadow_watch_runtime_scope,
    _build_portfolio_group_brief_runtime_scope,
    _build_portfolio_display_scope_runtime_scope,
    _build_positive_ratio_semantics_runtime_scope,
    _build_publish_funnel_runtime_scope,
    _build_publish_layer_semantics_runtime_scope,
    _build_report_timing_runtime_scope,
)
from backend.project_publish_decisions import DATA_VALIDATION_COLUMNS
from backend.metric_units import metric_unit_metadata
from backend.manager_metrics import manager_metric_contract, normalize_manager_record
from core.utils.drawdown_duration import (
    ANNUAL_DRAWDOWN_OBSERVATIONS,
    normalize_drawdown_duration_record,
)
from backend.report_workbook import _add_sheet_with_style
from backend.tracker_occurrence import tracker_occurrence_fields


def _normalize_sidecar_row(row: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        field = str(key or "").strip()
        if not field:
            continue
        normalized[field] = "" if value is None else str(value).strip()
    return normalize_drawdown_duration_record(normalize_manager_record(normalized))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _manager_score_text(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return ""
    return f"{number:.2f}" if math.isfinite(number) else ""


def load_tracker_drawdown_duration_values(
    payload: dict[str, Any], *, settings: Any, codes: set[str],
) -> dict[str, Any]:
    """Read an omitted preview field from the same immutable published detail."""
    if not codes:
        return {}
    artifacts = payload.get("artifacts") or {}
    name = artifacts.get("project_tracker_current_detail_json") or artifacts.get("run_local_project_tracker_current_detail_json")
    run_id = str(payload.get("run_id") or "").strip()
    if not name or not run_id:
        return {}
    try:
        path = (settings.project_root / str(name)).resolve()
        if not path.is_relative_to(settings.project_root.resolve()):
            return {}
        revision = (payload.get("publication_control") or {}).get("revision")
        if revision and (path.parent.name != revision or path.parent.parent.name != "revisions"
                         or path.parent.parent.parent.name != run_id):
            return {}
        sidecar = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    if (not isinstance(sidecar, dict) or sidecar.get("run_id") != run_id
            or sidecar.get("artifact_type") != "project_tracker_current_detail_sidecar"
            or not isinstance(sidecar.get("rows"), list)):
        return {}
    values = {}
    for item in sidecar["rows"]:
        if not isinstance(item, dict):
            continue
        code = str(item.get("基金代码") or "").strip()
        if code not in codes:
            continue
        if code in values:
            return {}
        values[code] = normalize_drawdown_duration_record(item).get(ANNUAL_DRAWDOWN_OBSERVATIONS)
    return values


def _load_tracker_current_detail_sidecar(
    payload: dict[str, Any],
    *,
    settings: Any,
) -> dict[str, Any] | None:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    sidecar_file = (
        artifacts.get("project_tracker_current_detail_json")
        or artifacts.get("run_local_project_tracker_current_detail_json")
    )
    if not sidecar_file:
        return None

    sidecar_path = settings.project_root / str(sidecar_file)
    if not sidecar_path.exists():
        return None
    try:
        sidecar_payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(sidecar_payload, dict):
        return None
    if str(sidecar_payload.get("artifact_type") or "").strip() != "project_tracker_current_detail_sidecar":
        return None

    raw_rows = sidecar_payload.get("rows")
    if not isinstance(raw_rows, list):
        return None

    tracker_records: list[dict[str, str]] = []
    detail_records_by_code: dict[str, dict[str, str]] = {}
    group_records_by_code: dict[str, dict[str, str]] = {}
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        row = _normalize_sidecar_row(item)
        code = str(row.get("基金代码") or "").strip()
        if not code:
            continue
        tracker_records.append(row)
        detail_records_by_code[code] = {
            "基金代码": code,
            "债基风格标签": row.get("债基风格标签", ""),
            "长期稳定性标签": row.get("长期稳定性标签", ""),
        }
        group_records_by_code[code] = row

    source = sidecar_payload.get("source") if isinstance(sidecar_payload.get("source"), dict) else {}
    return {
        "tracker_file": source.get("project_tracker_excel") or artifacts.get("project_tracker_excel"),
        "tracker_records": tracker_records,
        "detail_records_by_code": detail_records_by_code,
        "group_records_by_code": group_records_by_code,
        "dataset_source": "project_tracker_current_detail_json",
        "sidecar_file": sidecar_file,
        "sidecar_schema_version": _safe_int(sidecar_payload.get("schema_version")),
        "sidecar_row_count": _safe_int(sidecar_payload.get("row_count"), len(tracker_records)),
    }


def load_tracker_dataset(
    payload: dict[str, Any],
    *,
    settings: Any,
    read_excel_cached: Any,
    read_tracker_workbook_sheet: Any,
) -> dict[str, Any]:
    sidecar_dataset = _load_tracker_current_detail_sidecar(payload, settings=settings)
    if sidecar_dataset is not None:
        return sidecar_dataset

    tracker_sheet_usecols_options = [
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "当前是否在榜",
            "基金公司",
            "债基风格标签",
            "基金规模(亿)",
            "数据来源",
        ],
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "当前是否在榜",
            "基金公司",
            "债基风格标签",
            "最新基金规模(亿)",
            "数据来源",
        ],
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "当前是否在榜",
            "基金公司",
            "债基风格标签",
            "基金规模(亿)",
        ],
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "当前是否在榜",
            "基金公司",
            "债基风格标签",
            "最新基金规模(亿)",
        ],
    ]
    tracker_current_sheet_usecols_options = [
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "基金规模(亿)",
            "数据来源",
        ],
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "最新基金规模(亿)",
            "数据来源",
        ],
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "基金规模(亿)",
        ],
        [
            "基金代码",
            "基金名称",
            "正式榜分组",
            "正式投资评分",
            "出现次数",
            "出现频率(%)",
            "最新基金规模(亿)",
        ],
    ]
    tracker_group_usecols_options = [
        [
            "基金代码",
            "基金公司",
            "基金规模(亿)",
            "近1周(%)",
            "近1月(%)",
            "近3月(%)",
            "1年-波动率",
            "1年-最大回撤(%)",
            "1年-年化收益(%)",
            "1年-夏普比率",
            "最新净值日期",
            "数据来源",
        ],
        [
            "基金代码",
            "基金公司",
            "基金规模(亿)",
            "近1周(%)",
            "近1月(%)",
            "近3月(%)",
            "1年-波动率",
            "1年-最大回撤(%)",
            "1年-年化收益(%)",
            "1年-夏普比率",
            "最新净值日期",
        ],
    ]
    # Keep the optional duration in both current and immutable legacy workbooks.
    # Canonical columns are tried first, including when their values are missing.
    tracker_group_usecols_options = [
        [*fields, duration_field]
        for duration_field in (ANNUAL_DRAWDOWN_OBSERVATIONS, "1年-最大回撤持续期(天)")
        for fields in tracker_group_usecols_options
    ] + tracker_group_usecols_options
    tracker_file = (payload.get("artifacts") or {}).get("project_tracker_excel")
    filtered_file = (payload.get("artifacts") or {}).get("filtered_excel")
    tracker_records: list[dict[str, Any]] = []
    if tracker_file:
        tracker_path = settings.project_root / str(tracker_file)
        if tracker_path.exists():
            try:
                tracker_df = None
                for usecols in tracker_current_sheet_usecols_options:
                    try:
                        tracker_df = read_tracker_workbook_sheet(
                            tracker_path,
                            sheet_name="当前在榜",
                            usecols=usecols,
                        )
                        break
                    except ValueError:
                        continue
                if tracker_df is None:
                    raise ValueError("当前在榜 sheet 缺少预期列")
            except ValueError:
                tracker_df = None
                for usecols in tracker_sheet_usecols_options:
                    try:
                        tracker_df = read_tracker_workbook_sheet(
                            tracker_path,
                            sheet_name="完整数据",
                            usecols=usecols,
                        )
                        break
                    except ValueError:
                        continue
                if tracker_df is None:
                    raise ValueError("完整数据 sheet 缺少预期列")
            tracker_records = tracker_df.astype(str).fillna("").to_dict(orient="records")

    detail_records_by_code: dict[str, dict[str, Any]] = {}
    group_records_by_code: dict[str, dict[str, Any]] = {}
    if filtered_file:
        filtered_path = settings.project_root / str(filtered_file)
        if filtered_path.exists():
            detail_sheet = read_excel_cached(
                filtered_path,
                sheet_name="推荐理由与风格",
                usecols=None,
            )
            for row in detail_sheet.astype(str).fillna("").to_dict(orient="records"):
                row = normalize_drawdown_duration_record(normalize_manager_record(row))
                code = str(row.get("基金代码") or "").strip()
                if code:
                    detail_records_by_code[code] = row
            for sheet_name in ["纯债稳健榜", "增强债弹性榜"]:
                group_df = None
                for usecols in tracker_group_usecols_options:
                    try:
                        group_df = read_excel_cached(
                            filtered_path,
                            sheet_name=sheet_name,
                            usecols=usecols,
                        )
                        break
                    except ValueError:
                        continue
                if group_df is None:
                    continue
                for row in group_df.astype(str).fillna("").to_dict(orient="records"):
                    row = normalize_drawdown_duration_record(row)
                    code = str(row.get("基金代码") or "").strip()
                    if code:
                        group_records_by_code[code] = row

    return {
        "tracker_file": tracker_file,
        "tracker_records": tracker_records,
        "detail_records_by_code": detail_records_by_code,
        "group_records_by_code": group_records_by_code,
        "dataset_source": "project_tracker_workbook",
    }


def load_tracker_occurrence_map(
    payload: dict[str, Any],
    *,
    settings: Any,
    normalize_code_text: Any,
    read_tracker_workbook_sheet: Any,
) -> dict[str, dict[str, str]]:
    tracker_file = (payload.get("artifacts") or {}).get("project_tracker_excel")
    if not tracker_file:
        return {}
    tracker_path = settings.project_root / str(tracker_file)
    if not tracker_path.exists():
        return {}

    tracker_df = None
    for usecols in (
        ["基金代码", "出现次数", "出现频率(%)"],
        ["基金代码", "出现次数", "出现频率(%)", "当前是否在榜"],
    ):
        try:
            tracker_df = read_tracker_workbook_sheet(
                tracker_path,
                sheet_name="完整数据",
                usecols=usecols,
            )
            break
        except ValueError:
            continue
    if tracker_df is None or tracker_df.empty:
        return {}

    result: dict[str, dict[str, str]] = {}
    for row in tracker_df.fillna("").astype(str).to_dict(orient="records"):
        code = normalize_code_text(row.get("基金代码"))
        if not code:
            continue
        result[code] = {
            "出现次数": str(row.get("出现次数") or "").strip(),
            "出现频率(%)": str(row.get("出现频率(%)") or "").strip(),
        }
    return result


def overlay_portfolio_occurrence_metrics(
    portfolio_summary: dict[str, Any] | None,
    occurrence_by_code: dict[str, dict[str, str]] | None,
    *,
    normalize_code_text: Any,
    normalize_number_text: Any,
    to_float: Any,
    format_tracker_occurrence_text: Any,
) -> dict[str, Any]:
    summary = dict(portfolio_summary or {})
    groups = summary.get("groups") or []
    if not groups:
        return summary
    occurrence_by_code = occurrence_by_code or {}

    normalized_groups: list[dict[str, Any]] = []
    normalized_funds: list[dict[str, Any]] = []
    for group in groups:
        normalized_group = dict(group or {})
        funds = []
        for fund in (group.get("funds") or []):
            normalized_fund = dict(fund or {})
            code = normalize_code_text(normalized_fund.get("基金代码"))
            normalized_fund.update(tracker_occurrence_fields(occurrence_by_code.get(code)))
            funds.append(normalized_fund)
            normalized_funds.append(normalized_fund)
        normalized_group["funds"] = funds
        normalized_groups.append(normalized_group)

    summary["groups"] = normalized_groups
    summary["funds"] = normalized_funds
    return summary


def build_tracker_current_response(
    payload: dict[str, Any],
    *,
    load_tracker_dataset_fn: Any,
    normalize_portfolio_groups: Any,
    normalize_number_text: Any,
    format_tracker_occurrence_text: Any,
    apply_since_inception_annualized_return_to_funds_fn: Any,
) -> dict[str, Any]:
    dataset = load_tracker_dataset_fn(payload)
    counts = payload.get("counts") or {}
    publish_summary = payload.get("project_publish_summary") or {}
    tracker_summary = publish_summary.get("project_tracker_summary") or {}
    portfolio_summary = normalize_portfolio_groups(publish_summary.get("portfolio_suggestions") or {})
    tracker_rows = []
    tracker_records = dataset.get("tracker_records") or []
    detail_by_code = dataset.get("detail_records_by_code") or {}
    group_by_code = dataset.get("group_records_by_code") or {}
    for item in tracker_records:
        item = normalize_drawdown_duration_record(normalize_manager_record(item))
        if "当前是否在榜" in item and str(item.get("当前是否在榜") or "").strip() != "是":
            continue
        code = str(item.get("基金代码") or "").strip()
        detail_record = detail_by_code.get(code, {})
        manager_record = {**normalize_manager_record(detail_record), **item}
        group_detail = normalize_drawdown_duration_record(group_by_code.get(code, {}))
        scale_text = group_detail.get("基金规模(亿)") or item.get("基金规模(亿)") or item.get("最新基金规模(亿)") or ""
        tracker_rows.append(
            {
                "基金代码": code,
                "基金名称": item.get("基金名称") or "",
                "正式榜分组": item.get("正式榜分组") or "",
                "债基风格标签": detail_record.get("债基风格标签") or "",
                "长期稳定性标签": detail_record.get("长期稳定性标签") or "",
                "基金公司": group_detail.get("基金公司") or item.get("基金公司") or "",
                "数据来源": group_detail.get("数据来源") or item.get("数据来源") or "",
                "正式投资评分": normalize_number_text(item.get("正式投资评分"), digits=4),
                "出现次数": normalize_number_text(item.get("出现次数"), digits=0),
                "出现频率(%)": normalize_number_text(item.get("出现频率(%)"), digits=2),
                "出现次数&出现频率(%)": format_tracker_occurrence_text(
                    item.get("出现次数"),
                    item.get("出现频率(%)"),
                ),
                "基金规模(亿)": normalize_number_text(scale_text, digits=2),
                "近1周(%)": normalize_number_text(group_detail.get("近1周(%)"), digits=2),
                "近1月(%)": normalize_number_text(group_detail.get("近1月(%)"), digits=2),
                "近3月(%)": normalize_number_text(group_detail.get("近3月(%)"), digits=2),
                "1年-波动率": normalize_number_text(group_detail.get("1年-波动率"), digits=4),
                "1年-最大回撤(%)": normalize_number_text(group_detail.get("1年-最大回撤(%)"), digits=4),
                ANNUAL_DRAWDOWN_OBSERVATIONS: normalize_number_text(
                    group_detail.get(ANNUAL_DRAWDOWN_OBSERVATIONS), digits=0,
                ),
                "1年-年化收益(%)": normalize_number_text(group_detail.get("1年-年化收益(%)"), digits=2),
                "1年-夏普比率": normalize_number_text(group_detail.get("1年-夏普比率"), digits=4),
                "最新净值日期": group_detail.get("最新净值日期") or "",
                "经理从业与评分状态": manager_record.get("经理从业与评分状态", "未记录"),
                "经理最短从业天数": normalize_number_text(manager_record.get("经理最短从业天数"), digits=0),
                "经理平均综合评分": _manager_score_text(manager_record.get("经理平均综合评分")),
                "经理评分覆盖": manager_record.get("经理评分覆盖", "未记录"),
                "经理从业数据覆盖": manager_record.get("经理从业数据覆盖", "未记录"),
            }
        )

    tracker_rows = apply_since_inception_annualized_return_to_funds_fn(tracker_rows)

    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "tracker_file": dataset.get("tracker_file"),
        "tracker_summary": tracker_summary,
        "portfolio_suggestions": portfolio_summary,
        "metric_units": metric_unit_metadata(),
        "manager_metric_semantics": manager_metric_contract(),
        "row_count": int(len(tracker_rows)),
        "payload_scope": {
            "mode": "detail",
            "source": dataset.get("dataset_source") or "project_tracker_workbook",
            "sidecar_file": dataset.get("sidecar_file"),
            "sidecar_schema_version": dataset.get("sidecar_schema_version"),
            "sidecar_row_count": dataset.get("sidecar_row_count"),
        },
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "project_tracker_total_periods": int(counts.get("project_tracker_total_periods") or 0),
            "project_tracker_fund_count": int(counts.get("project_tracker_fund_count") or 0),
            "project_tracker_repeated_fund_count": int(counts.get("project_tracker_repeated_fund_count") or 0),
            "portfolio_group_count": int(portfolio_summary.get("group_count") or 0),
            "portfolio_fund_count": int(portfolio_summary.get("fund_count") or 0),
        },
        "rows": tracker_rows,
        "funds": tracker_rows,
    }


def build_recommendations_response(
    payload: dict[str, Any],
    *,
    settings: Any,
    normalize_portfolio_groups: Any,
    load_tracker_occurrence_map_fn: Any,
    overlay_portfolio_occurrence_metrics_fn: Any,
    apply_since_inception_annualized_return_fn: Any,
    build_portfolio_group_brief: Any,
    build_publish_funnel_summary: Any,
) -> dict[str, Any]:
    counts = payload.get("counts") or {}
    publish_summary = payload.get("project_publish_summary") or {}
    tracker_summary = publish_summary.get("project_tracker_summary") or {}
    report_timing_summary = publish_summary.get("report_timing_summary") or {}
    portfolio_summary = normalize_portfolio_groups(publish_summary.get("portfolio_suggestions") or {})
    portfolio_summary = overlay_portfolio_occurrence_metrics_fn(
        portfolio_summary,
        load_tracker_occurrence_map_fn(payload),
    )
    portfolio_summary = apply_since_inception_annualized_return_fn(portfolio_summary)
    normalized_run_id = str(payload.get("run_id") or "").strip() or None
    tracker_summary = _build_tracker_summary_runtime_scope(
        tracker_summary,
        run_id=normalized_run_id,
    )
    portfolio_display_scope = build_portfolio_display_scope(counts, portfolio_summary)
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        build_publish_funnel_summary(payload),
        run_id=normalized_run_id,
    )
    publish_layer_semantics_summary = build_publish_layer_semantics_summary(
        counts,
        publish_funnel_summary,
        portfolio_display_scope,
        portfolio_summary,
    )
    portfolio_group_brief = build_portfolio_group_brief(portfolio_summary)
    generic_holding_placeholder_watch = publish_summary.get("generic_holding_placeholder_watch") or {}
    low_vol_positive_ratio_shadow_watch = normalize_low_vol_positive_ratio_shadow_watch(
        publish_summary, report_identity=payload, settings=settings,
    )
    positive_ratio_semantics = build_positive_ratio_semantics_summary(
        publish_summary,
        low_vol_positive_ratio_shadow_watch,
    )
    portfolio_display_scope = _build_portfolio_display_scope_runtime_scope(
        portfolio_display_scope,
        run_id=normalized_run_id,
    )
    publish_layer_semantics_summary = _build_publish_layer_semantics_runtime_scope(
        publish_layer_semantics_summary,
        run_id=normalized_run_id,
    )
    positive_ratio_semantics = _build_positive_ratio_semantics_runtime_scope(
        positive_ratio_semantics,
        run_id=normalized_run_id,
    )
    portfolio_group_brief = _build_portfolio_group_brief_runtime_scope(
        portfolio_group_brief,
        run_id=normalized_run_id,
    )
    report_timing_summary = _build_report_timing_runtime_scope(
        report_timing_summary,
        run_id=normalized_run_id,
    )
    low_vol_positive_ratio_shadow_watch = _build_low_vol_positive_ratio_shadow_watch_runtime_scope(
        low_vol_positive_ratio_shadow_watch,
        run_id=normalized_run_id,
    )
    artifacts = payload.get("artifacts") or {}
    recommendation_rows = portfolio_summary.get("funds") or []
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "report_timing_summary": report_timing_summary,
        "tracker_summary": tracker_summary,
        "portfolio_suggestions": portfolio_summary,
        "rows": recommendation_rows,
        "funds": recommendation_rows,
        "metric_units": metric_unit_metadata(),
        "portfolio_display_scope": portfolio_display_scope,
        "publish_layer_semantics_summary": publish_layer_semantics_summary,
        "portfolio_group_brief": portfolio_group_brief,
        "generic_holding_placeholder_watch": generic_holding_placeholder_watch,
        "low_vol_positive_ratio_shadow_watch": low_vol_positive_ratio_shadow_watch,
        "positive_ratio_semantics": positive_ratio_semantics,
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
            "portfolio_group_count": int(portfolio_summary.get("group_count") or 0),
            "portfolio_fund_count": int(portfolio_summary.get("fund_count") or 0),
            "generic_holding_placeholder_excluded_count": int(
                counts.get("generic_holding_placeholder_excluded_count")
                or generic_holding_placeholder_watch.get("count")
                or 0
            ),
            "generic_holding_placeholder_buyable_count": int(
                counts.get("generic_holding_placeholder_buyable_count")
                or generic_holding_placeholder_watch.get("buyable_count")
                or 0
            ),
            "generic_holding_placeholder_high_score_count": int(
                counts.get("generic_holding_placeholder_high_score_count")
                or generic_holding_placeholder_watch.get("high_score_count")
                or 0
            ),
            "low_vol_positive_ratio_focus_count": int(
                counts.get("low_vol_positive_ratio_focus_count")
                or low_vol_positive_ratio_shadow_watch.get("focus_sample_count")
                or 0
            ),
            "low_vol_positive_ratio_gate_hit_count": int(
                counts.get("low_vol_positive_ratio_gate_hit_count")
                or low_vol_positive_ratio_shadow_watch.get("positive_ratio_gate_hit_count")
                or 0
            ),
            "low_vol_positive_ratio_gap_alert_count": int(
                counts.get("low_vol_positive_ratio_gap_alert_count")
                or low_vol_positive_ratio_shadow_watch.get("positive_ratio_gap_alert_count")
                or 0
            ),
        },
        "artifacts": {
            "filtered_excel": artifacts.get("filtered_excel"),
            "report_markdown": artifacts.get("report_markdown"),
            "project_tracker_excel": artifacts.get("project_tracker_excel"),
        },
    }


def _reduce_portfolio_suggestions_for_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(summary or {})
    compression_audit = payload.get("compression_audit") if isinstance(payload.get("compression_audit"), dict) else {}
    groups = []
    for item in list(payload.get("groups") or []):
        group = dict(item or {})
        funds = list(group.get("funds") or [])
        groups.append(
            {
                "name": group.get("name"),
                "description": group.get("description"),
                "count": int(group.get("count") or len(funds)),
                "downgrade_info": group.get("downgrade_info") or {},
            }
        )
    return {
        "available": bool(payload.get("available")),
        "source_candidate_count": int(payload.get("source_candidate_count") or 0),
        "candidate_count": int(payload.get("candidate_count") or 0),
        "target_count": int(payload.get("target_count") or 0),
        "fund_count": int(payload.get("fund_count") or 0),
        "group_count": int(payload.get("group_count") or 0),
        "company_cap": int(payload.get("company_cap") or 0),
        "share_class_unique_count": int(payload.get("share_class_unique_count") or 0),
        "selection_policy": payload.get("selection_policy"),
        "quality_gate": payload.get("quality_gate") or {},
        "core_quality_gate": payload.get("core_quality_gate") or {},
        "target_policy": payload.get("target_policy") or {},
        "compression_audit": {
            "candidate_count_before_target": int(compression_audit.get("candidate_count_before_target") or 0),
            "selected_count": int(compression_audit.get("selected_count") or 0),
            "trimmed_by_target_count": int(compression_audit.get("trimmed_by_target_count") or 0),
            "target_compression_ratio_pct": compression_audit.get("target_compression_ratio_pct"),
            "selection_pressure": compression_audit.get("selection_pressure") or {},
            "company_cap": int(compression_audit.get("company_cap") or payload.get("company_cap") or 0),
            "share_class_unique_count": int(
                compression_audit.get("share_class_unique_count")
                or payload.get("share_class_unique_count")
                or 0
            ),
        },
        "groups": groups,
    }


def _omit_heavy_preview_payloads(value: Any) -> Any:
    heavy_keys = {
        "preview",
        "sample",
        "samples",
        "rows",
        "funds",
        "records",
        "details",
        "detail_rows",
    }
    if isinstance(value, dict):
        compact: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key or "").strip()
            normalized_key = key.lower()
            if normalized_key in heavy_keys or normalized_key.endswith("_preview") or normalized_key.endswith("_sample"):
                continue
            compact[key] = _omit_heavy_preview_payloads(raw_value)
        return compact
    if isinstance(value, list):
        return [_omit_heavy_preview_payloads(item) for item in value[:4]]
    return value


def build_recommendations_summary_response(
    payload: dict[str, Any],
    *,
    settings: Any,
    normalize_portfolio_groups: Any,
    build_portfolio_group_brief: Any,
    build_publish_funnel_summary: Any,
) -> dict[str, Any]:
    counts = payload.get("counts") or {}
    publish_summary = payload.get("project_publish_summary") or {}
    tracker_summary = publish_summary.get("project_tracker_summary") or {}
    report_timing_summary = publish_summary.get("report_timing_summary") or {}
    portfolio_summary = normalize_portfolio_groups(publish_summary.get("portfolio_suggestions") or {})
    normalized_run_id = str(payload.get("run_id") or "").strip() or None

    tracker_summary = _build_tracker_summary_runtime_scope(
        tracker_summary,
        run_id=normalized_run_id,
    )
    portfolio_display_scope = build_portfolio_display_scope(counts, portfolio_summary)
    publish_funnel_summary = _build_publish_funnel_runtime_scope(
        build_publish_funnel_summary(payload),
        run_id=normalized_run_id,
    )
    publish_layer_semantics_summary = build_publish_layer_semantics_summary(
        counts,
        publish_funnel_summary,
        portfolio_display_scope,
        portfolio_summary,
    )
    portfolio_group_brief = build_portfolio_group_brief(portfolio_summary)
    generic_holding_placeholder_watch = _omit_heavy_preview_payloads(
        publish_summary.get("generic_holding_placeholder_watch") or {}
    )
    low_vol_positive_ratio_shadow_watch = _omit_heavy_preview_payloads(
        normalize_low_vol_positive_ratio_shadow_watch(publish_summary, report_identity=payload, settings=settings)
    )
    positive_ratio_semantics = build_positive_ratio_semantics_summary(
        publish_summary,
        low_vol_positive_ratio_shadow_watch,
    )
    portfolio_display_scope = _build_portfolio_display_scope_runtime_scope(
        portfolio_display_scope,
        run_id=normalized_run_id,
    )
    publish_layer_semantics_summary = _build_publish_layer_semantics_runtime_scope(
        publish_layer_semantics_summary,
        run_id=normalized_run_id,
    )
    positive_ratio_semantics = _build_positive_ratio_semantics_runtime_scope(
        positive_ratio_semantics,
        run_id=normalized_run_id,
    )
    portfolio_group_brief = _build_portfolio_group_brief_runtime_scope(
        portfolio_group_brief,
        run_id=normalized_run_id,
    )
    report_timing_summary = _build_report_timing_runtime_scope(
        report_timing_summary,
        run_id=normalized_run_id,
    )
    low_vol_positive_ratio_shadow_watch = _build_low_vol_positive_ratio_shadow_watch_runtime_scope(
        low_vol_positive_ratio_shadow_watch,
        run_id=normalized_run_id,
    )
    artifacts = payload.get("artifacts") or {}
    return {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "payload_scope": {
            "mode": "summary",
            "summary_endpoint": "/api/recommendations/latest",
            "detail_endpoint": "/api/recommendations/latest/detail",
            "omitted_fields": [
                "portfolio_suggestions.funds",
                "portfolio_suggestions.groups[].funds",
                "rows",
                "funds",
                "style_metric_references",
            ],
        },
        "report_timing_summary": report_timing_summary,
        "tracker_summary": tracker_summary,
        "portfolio_suggestions": _reduce_portfolio_suggestions_for_summary(portfolio_summary),
        "metric_units": metric_unit_metadata(),
        "portfolio_display_scope": portfolio_display_scope,
        "publish_layer_semantics_summary": publish_layer_semantics_summary,
        "portfolio_group_brief": portfolio_group_brief,
        "generic_holding_placeholder_watch": generic_holding_placeholder_watch,
        "low_vol_positive_ratio_shadow_watch": low_vol_positive_ratio_shadow_watch,
        "positive_ratio_semantics": positive_ratio_semantics,
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "dynamic_final_count": int(counts.get("dynamic_final_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
            "portfolio_group_count": int(portfolio_summary.get("group_count") or 0),
            "portfolio_fund_count": int(portfolio_summary.get("fund_count") or 0),
            "generic_holding_placeholder_excluded_count": int(
                counts.get("generic_holding_placeholder_excluded_count")
                or generic_holding_placeholder_watch.get("count")
                or 0
            ),
            "generic_holding_placeholder_buyable_count": int(
                counts.get("generic_holding_placeholder_buyable_count")
                or generic_holding_placeholder_watch.get("buyable_count")
                or 0
            ),
            "generic_holding_placeholder_high_score_count": int(
                counts.get("generic_holding_placeholder_high_score_count")
                or generic_holding_placeholder_watch.get("high_score_count")
                or 0
            ),
            "low_vol_positive_ratio_focus_count": int(
                counts.get("low_vol_positive_ratio_focus_count")
                or low_vol_positive_ratio_shadow_watch.get("focus_sample_count")
                or 0
            ),
            "low_vol_positive_ratio_gate_hit_count": int(
                counts.get("low_vol_positive_ratio_gate_hit_count")
                or low_vol_positive_ratio_shadow_watch.get("positive_ratio_gate_hit_count")
                or 0
            ),
            "low_vol_positive_ratio_gap_alert_count": int(
                counts.get("low_vol_positive_ratio_gap_alert_count")
                or low_vol_positive_ratio_shadow_watch.get("positive_ratio_gap_alert_count")
                or 0
            ),
        },
        "artifacts": {
            "filtered_excel": artifacts.get("filtered_excel"),
            "report_markdown": artifacts.get("report_markdown"),
            "project_tracker_excel": artifacts.get("project_tracker_excel"),
        },
    }


def recommendation_export_rows(
    funds: list[dict[str, Any]],
    *,
    include_group_name: str | None = None,
    normalize_code_text: Any,
    normalize_number_text: Any,
    to_float: Any,
    format_tracker_occurrence_text: Any,
    export_columns: list[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index, fund in enumerate(funds or [], start=1):
        occurrence = tracker_occurrence_fields(fund)
        row = {
            "序号": index,
            "基金代码": normalize_code_text(fund.get("基金代码")),
            "基金名称": str(fund.get("基金名称") or "").strip(),
            "建议权重": str(fund.get("建议权重") or "").strip(),
            "实时估值": "",
            "出现次数&出现频率(%)": occurrence["出现次数&出现频率(%)"],
            "1年-年化收益(%)": normalize_number_text(fund.get("1年-年化收益(%)"), digits=2),
            "平台成立以来收益年化(%)": normalize_number_text(fund.get("平台成立以来收益年化(%)"), digits=2),
            "累计净值比年化(%)": normalize_number_text(fund.get("累计净值比年化(%)"), digits=2),
            "近1月(%)": normalize_number_text(fund.get("近1月(%)"), digits=2),
            "近3月(%)": normalize_number_text(fund.get("近3月(%)"), digits=2),
            "1年-夏普比率": normalize_number_text(fund.get("1年-夏普比率"), digits=4),
            "1年-最大回撤(%)": normalize_number_text(fund.get("1年-最大回撤(%)"), digits=4),
            "1年-波动率": normalize_number_text(fund.get("1年-波动率"), digits=4),
            "投资优先池得分": normalize_number_text(fund.get("投资优先池得分"), digits=4),
            "正式投资评分": normalize_number_text(fund.get("正式投资评分"), digits=4),
            "最新净值日期": str(fund.get("最新净值日期") or "").strip(),
            "基金规模(亿)": normalize_number_text(fund.get("基金规模(亿)"), digits=2),
            "正式榜分组": str(fund.get("正式榜分组") or "").strip(),
            "债基风格标签": str(fund.get("债基风格标签") or "").strip(),
            "项目质量信心": str(fund.get("项目质量信心") or "").strip(),
            **{key: fund.get(key) for key in DATA_VALIDATION_COLUMNS if key in fund},
            "AutoStock原始校验状态": str(fund.get("AutoStock原始校验状态") or "").strip(),
            "基金公司": str(fund.get("基金公司") or "").strip(),
            "费率合计(%)": normalize_number_text(fund.get("费率合计(%)"), digits=2),
            "长期稳定性标签": str(fund.get("长期稳定性标签") or "").strip(),
            "入选原因": str(fund.get("入选原因") or "").strip(),
            "风险提示": str(fund.get("风险提示") or "").strip(),
        }
        if include_group_name is not None:
            row = {"推荐分组": include_group_name, **row}
        rows.append(row)
    columns = ["推荐分组", *export_columns] if include_group_name is not None else export_columns
    return pd.DataFrame(rows, columns=columns)


def build_recommendation_audit_export_rows(
    payload: dict[str, Any],
    portfolio_summary: dict[str, Any],
    *,
    normalize_number_text: Any,
    to_float: Any,
) -> pd.DataFrame:
    publish_summary = payload.get("project_publish_summary") or {}
    target_policy = portfolio_summary.get("target_policy") or {}
    compression_audit = portfolio_summary.get("compression_audit") or {}
    screening_audit = publish_summary.get("standard_screening_audit") or {}
    rows: list[dict[str, Any]] = []

    def append_row(section: str, metric: str, value: object, detail: object = "") -> None:
        value_text = str(value).strip() if value is not None else ""
        detail_text = str(detail).strip() if detail is not None else ""
        if not value_text and not detail_text:
            return
        rows.append(
            {
                "分区": section,
                "指标": metric,
                "数值": value_text,
                "说明": detail_text,
            }
        )

    append_row(
        "目标池策略",
        "基础目标数",
        int(target_policy.get("base_target_count") or 0),
        f"候选 {int(target_policy.get('candidate_count') or portfolio_summary.get('candidate_count') or 0)}",
    )
    append_row(
        "目标池策略",
        "当前目标数",
        int(target_policy.get("target_count") or portfolio_summary.get("target_count") or 0),
        "已扩容" if target_policy.get("expanded") else "未扩容",
    )
    append_row("目标池策略", "扩容原因", str(target_policy.get("expansion_reason") or "").strip() or "-", "")
    append_row(
        "目标池策略",
        "扩容触发阈值",
        normalize_number_text(target_policy.get("expansion_ratio_trigger"), digits=2),
        "候选/基础目标倍数",
    )
    append_row(
        "目标池策略",
        "基础比例",
        normalize_number_text(to_float(target_policy.get("base_target_ratio")), digits=2),
        "",
    )
    append_row(
        "目标池策略",
        "扩容比例",
        normalize_number_text(to_float(target_policy.get("expanded_target_ratio")), digits=2),
        "",
    )
    append_row(
        "展示压缩",
        "展示压缩率(%)",
        normalize_number_text(compression_audit.get("target_compression_ratio_pct"), digits=2),
        f"裁剪 {int(compression_audit.get('trimmed_by_target_count') or 0)}",
    )
    append_row(
        "展示压缩",
        "候选/目标比",
        normalize_number_text((compression_audit.get("selection_pressure") or {}).get("candidate_per_target_ratio"), digits=2),
        "",
    )
    append_row(
        "展示压缩",
        "公司覆盖率(%)",
        normalize_number_text((compression_audit.get("selection_pressure") or {}).get("company_coverage_ratio_pct"), digits=2),
        "",
    )
    append_row("展示压缩", "单公司上限", int(compression_audit.get("company_cap") or portfolio_summary.get("company_cap") or 0), "")
    append_row(
        "展示压缩",
        "份额唯一数",
        int(compression_audit.get("share_class_unique_count") or portfolio_summary.get("share_class_unique_count") or 0),
        "",
    )

    for item in (compression_audit.get("group_candidate_rows") or [])[:4]:
        append_row("候选分组", str(item.get("group_label") or "").strip() or "-", int(item.get("count") or 0), f"{normalize_number_text(item.get('share_pct'), digits=2)}%")
    for item in (compression_audit.get("group_selected_rows") or [])[:4]:
        append_row("入选分组", str(item.get("group_label") or "").strip() or "-", int(item.get("count") or 0), f"{normalize_number_text(item.get('share_pct'), digits=2)}%")
    for item in (compression_audit.get("bucket_selected_rows") or [])[:5]:
        append_row("入选风格桶", str(item.get("bucket_key") or "").strip() or "-", int(item.get("count") or 0), f"{normalize_number_text(item.get('share_pct'), digits=2)}%")
    for item in (compression_audit.get("company_selected_rows") or [])[:5]:
        append_row("入选公司", str(item.get("company_name") or "").strip() or "-", int(item.get("count") or 0), f"{normalize_number_text(item.get('share_pct'), digits=2)}%")

    append_row("正式筛选", "高分摩擦样本", int(screening_audit.get("high_score_friction_count") or 0), "")
    for item in (screening_audit.get("reason_group_preview") or [])[:4]:
        append_row("正式筛选主原因", str(item.get("主原因分组") or "").strip() or "-", int(item.get("数量") or 0), f"{normalize_number_text(item.get('占未入榜样本比例(%)'), digits=2)}%")
    for item in (screening_audit.get("quality_factor_group_preview") or [])[:4]:
        append_row("质量因子组", str(item.get("质量因子组") or "").strip() or "-", int(item.get("数量") or 0), f"{normalize_number_text(item.get('占质量拦截样本比例(%)'), digits=2)}%")
    for item in (screening_audit.get("quality_factor_detail_preview") or [])[:5]:
        append_row("质量细项", str(item.get("质量细分因子") or "").strip() or "-", int(item.get("命中样本数") or 0), str(item.get("所属因子组") or "").strip())
    for item in (screening_audit.get("quality_overlap_combo_preview") or [])[:3]:
        append_row("质量重叠", str(item.get("重叠组合") or "").strip() or "-", int(item.get("样本数") or 0), f"{int(item.get('因子数') or 0)} 因子")
    for item in (screening_audit.get("high_score_friction_reason_preview") or [])[:3]:
        append_row("高分摩擦主因", str(item.get("主排除原因") or "").strip() or "-", int(item.get("样本数") or 0), f"{normalize_number_text(item.get('占高分摩擦样本比例(%)'), digits=2)}%")
    for item in (screening_audit.get("status_split_preview") or [])[:4]:
        append_row("发布状态", str(item.get("状态值") or "").strip() or "-", int(item.get("数量") or 0), str(item.get("状态维度") or "").strip())

    return pd.DataFrame(rows, columns=["分区", "指标", "数值", "说明"])


def build_recommendations_export_workbook(
    payload: dict[str, Any],
    *,
    normalize_portfolio_groups: Any,
    load_tracker_occurrence_map_fn: Any,
    overlay_portfolio_occurrence_metrics_fn: Any,
    apply_since_inception_annualized_return_fn: Any,
    build_recommendation_audit_export_rows_fn: Any,
    recommendation_export_rows_fn: Any,
    export_columns: list[str],
    export_column_widths: dict[str, int],
) -> tuple[BytesIO, str]:
    from openpyxl import Workbook

    publish_summary = payload.get("project_publish_summary") or {}
    portfolio_summary = normalize_portfolio_groups(publish_summary.get("portfolio_suggestions") or {})
    portfolio_summary = overlay_portfolio_occurrence_metrics_fn(
        portfolio_summary,
        load_tracker_occurrence_map_fn(payload),
    )
    portfolio_summary = apply_since_inception_annualized_return_fn(portfolio_summary)
    groups = portfolio_summary.get("groups") or []
    workbook = Workbook()
    workbook.remove(workbook.active)

    summary_rows = pd.DataFrame(
        [
            {"指标": "当前主版本", "数值": str(payload.get("run_id") or "").strip()},
            {"指标": "最新有效净值日期", "数值": str((publish_summary.get("project_tracker_summary") or {}).get("latest_nav_snapshot_date") or "").strip()},
            {"指标": "推荐分组数", "数值": int(portfolio_summary.get("group_count") or 0)},
            {"指标": "推荐基金数", "数值": int(portfolio_summary.get("fund_count") or 0)},
            {"指标": "统一候选数", "数值": int(portfolio_summary.get("candidate_count") or 0)},
            {"指标": "目标池规模", "数值": int(portfolio_summary.get("target_count") or 0)},
            {"指标": "单公司上限", "数值": int(portfolio_summary.get("company_cap") or 0)},
            {"指标": "选池规则", "数值": str(portfolio_summary.get("selection_policy") or "").strip()},
        ]
    )
    _add_sheet_with_style(
        workbook,
        title="导出摘要",
        df=summary_rows,
        left_align_column="指标",
        column_widths={"指标": 18, "数值": 42},
    )
    _add_sheet_with_style(
        workbook,
        title="推荐解释摘要",
        df=build_recommendation_audit_export_rows_fn(payload, portfolio_summary),
        left_align_column="指标",
        column_widths={"分区": 14, "指标": 26, "数值": 14, "说明": 56},
    )

    all_rows_frames: list[pd.DataFrame] = []
    for group in groups:
        group_name = str(group.get("name") or "推荐分组").strip() or "推荐分组"
        group_funds = group.get("funds") or []
        group_df = recommendation_export_rows_fn(group_funds)
        _add_sheet_with_style(
            workbook,
            title=group_name[:31],
            df=group_df,
            left_align_column="基金名称",
            column_widths=export_column_widths,
        )
        all_rows_frames.append(recommendation_export_rows_fn(group_funds, include_group_name=group_name))

    all_rows_df = (
        pd.concat(all_rows_frames, ignore_index=True)
        if all_rows_frames
        else pd.DataFrame(columns=["推荐分组", *export_columns])
    )
    _add_sheet_with_style(
        workbook,
        title="全部推荐基金",
        df=all_rows_df,
        left_align_column="基金名称",
        column_widths=export_column_widths,
    )

    buffer = BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    filename = f"优质基金推荐_{str(payload.get('run_id') or datetime.now().strftime('%Y%m%d_%H%M%S'))}.xlsx"
    return buffer, filename
