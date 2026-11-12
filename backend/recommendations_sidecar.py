from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from backend.config import Settings
from backend.project_publish_decisions import validation_status_value
from backend.api_payloads.portfolio_scope import (
    build_portfolio_display_scope,
    build_publish_layer_semantics_summary,
)
from backend.api_payloads.positive_ratio import (
    build_positive_ratio_semantics_summary,
    normalize_low_vol_positive_ratio_shadow_watch,
)
from backend.metric_units import metric_unit_metadata
from backend.tracker_occurrence import tracker_occurrence_fields, tracker_occurrence_reason


RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION = 3
RECOMMENDATIONS_DETAIL_SIDECAR_META_LABEL = "__recommendations_detail_sidecar"
SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL = "平台成立以来收益年化(%)"
SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL = "成立以来年度收益明细"
STYLE_METRIC_REFERENCES_LABEL = "__style_metric_references"


def _normalize_code(value: object) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _normalize_number_text(value: object, digits: int = 4) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = float(text)
    except (TypeError, ValueError):
        return text
    normalized = f"{number:.{digits}f}"
    if digits > 0:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized if normalized != "-0" else "0"


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _path_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    try:
        stat = Path(path).stat()
    except OSError:
        return "missing"
    return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def _file_content_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    current = Path(path)
    try:
        data = current.read_bytes()
    except OSError:
        return "missing"
    return f"{len(data)}:{hashlib.sha1(data).hexdigest()}"


def _sqlite_file_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    base = Path(path)
    wal = Path(f"{base}-wal")
    wal_token = "empty_or_missing_wal"
    try:
        wal_stat = wal.stat()
        if int(wal_stat.st_size) > 0:
            wal_token = f"{int(wal_stat.st_mtime_ns)}:{int(wal_stat.st_size)}"
    except OSError:
        pass
    return "::".join([_path_cache_token(base), wal_token])


def _project_path(settings: Settings, raw_path: object) -> Path | None:
    text = str(raw_path or "").strip()
    if not text:
        return None
    path = Path(text)
    return path if path.is_absolute() else settings.project_root / path


def _project_relative(settings: Settings, path: Path | str | None) -> str | None:
    if path is None:
        return None
    current = Path(path)
    try:
        return str(current.relative_to(settings.project_root))
    except ValueError:
        return str(current)


def _safe_json_load(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_file = path.parent / f"{path.name}.{uuid4().hex}.tmp"
    temp_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_file.replace(path)


def _runtime_scope(payload: dict[str, Any] | None, run_id: str) -> dict[str, Any]:
    current = dict(payload or {})
    current.setdefault("run_id", run_id)
    current.setdefault("latest_canonical_run_id", run_id)
    current.setdefault("replay_precision_status", "missing")
    current.setdefault("replay_precision_status_label", "非精确回放")
    current.setdefault("auxiliary_scope", "same_run_report_artifact")
    current.setdefault("auxiliary_scope_label", "同 run 报告产物")
    return current


def recommendations_detail_sidecar_path(settings: Settings, payload: dict[str, Any]) -> Path | None:
    run_id = str(payload.get("run_id") or "").strip()
    if not run_id:
        return None
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if any(ch not in allowed for ch in run_id):
        return None
    advertised = _project_path(settings, (payload.get("artifacts") or {}).get("recommendations_detail_json"))
    if advertised is not None:
        allowed_root = settings.runs_dir / run_id
        if not advertised.resolve().is_relative_to(allowed_root.resolve()):
            return None
        return advertised
    return settings.runs_dir / run_id / "recommendations_detail.json"


def build_recommendations_detail_cache_basis(
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    tracker_excel_path = _project_path(settings, artifacts.get("project_tracker_excel"))
    if (payload.get("publication_control") or {}).get("commit_protocol"):
        return {
            "schema_version": RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
            "basis_kind": "immutable_publication_revision",
            "run_id": str(payload.get("run_id") or ""),
            "generated_at": str(payload.get("generated_at") or ""),
            "revision": (payload.get("publication_control") or {}).get("revision"),
            "project_tracker_excel": _file_content_cache_token(tracker_excel_path),
            "project_tracker_current_detail_json": _file_content_cache_token(_project_path(settings, artifacts.get("project_tracker_current_detail_json"))),
            "publication_basis_json": _file_content_cache_token(_project_path(settings, artifacts.get("publication_basis_json"))),
        }
    return {
        "schema_version": RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
        "run_id": str(payload.get("run_id") or "").strip(),
        "generated_at": str(payload.get("generated_at") or "").strip(),
        "latest_report_file": _file_content_cache_token(settings.latest_report_file),
        "database_file": _sqlite_file_cache_token(settings.database_path),
        "project_tracker_excel": _path_cache_token(tracker_excel_path),
    }


def build_recommendations_detail_source_contract(cache_basis: dict[str, Any]) -> dict[str, Any]:
    basis_fields = {
        "run_id": "canonical report run_id",
        "latest_report_file": "reports/latest.json content fingerprint",
        "database_file": "SQLite database file and WAL fingerprint",
        "project_tracker_excel": "run-local tracker workbook stat token",
    }
    if cache_basis.get("basis_kind") == "immutable_publication_revision":
        basis_fields = {
            "run_id": "committed report run_id",
            "generated_at": "committed report timestamp",
            "revision": "immutable report revision identifier",
            "project_tracker_excel": "frozen workbook content fingerprint",
            "project_tracker_current_detail_json": "frozen tracker detail content fingerprint",
            "publication_basis_json": "frozen source values and provenance content fingerprint",
        }
    return {
        "schema_version": 1,
        "source": "run_local_recommendations_detail",
        "truth_policy": "real_api_database_or_runtime_artifact_only",
        "missing_value_policy": "preserve_missing_semantics",
        "business_value_synthesis": "forbidden",
        "basis_fields": basis_fields,
        "cache_basis_schema_version": cache_basis.get("schema_version"),
    }


def attach_recommendations_detail_source_contract(
    response: dict[str, Any],
    cache_basis: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(response, dict):
        return response
    basis = dict(cache_basis or {})
    response["cache_basis"] = basis
    response["source_basis"] = dict(basis)
    response["source_contract"] = build_recommendations_detail_source_contract(basis)
    payload_scope = response.get("payload_scope") if isinstance(response.get("payload_scope"), dict) else {}
    response["payload_scope"] = {
        **payload_scope,
        "source_contract": {
            "source": "run_local_recommendations_detail",
            "cache_basis_present": bool(basis),
            "source_basis_matches_cache_basis": True,
            "truth_policy": "real_api_database_or_runtime_artifact_only",
            "missing_value_policy": "preserve_missing_semantics",
        },
    }
    return response


def _load_tracker_detail_payload(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    tracker_path = _project_path(
        settings,
        artifacts.get("project_tracker_current_detail_json")
        or artifacts.get("run_local_project_tracker_current_detail_json"),
    )
    tracker = _safe_json_load(tracker_path)
    if str(tracker.get("run_id") or "") != str(payload.get("run_id") or ""):
        return {}
    publication = payload.get("publication_control") or {}
    if publication.get("commit_protocol"):
        revision_root = settings.runs_dir / str(payload.get("run_id") or "") / "revisions" / str(publication.get("revision") or "")
        if tracker_path is None or not tracker_path.resolve().is_relative_to(revision_root.resolve()):
            return {}
    return tracker


def _tracker_rows_by_code(tracker_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = tracker_payload.get("rows")
    if not isinstance(rows, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        code = _normalize_code(item.get("基金代码"))
        if code:
            result[code] = dict(item)
    return result


def overlay_recommendations_tracker_occurrences(
    settings: Settings,
    payload: dict[str, Any],
    response: dict[str, Any],
    *,
    tracker_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Correct new and cached responses from the same publication's Tracker only."""
    tracker = _load_tracker_detail_payload(settings, payload) if tracker_payload is None else tracker_payload
    rows = _tracker_rows_by_code(tracker)
    result = deepcopy(response)
    missing_codes: set[str] = set()
    total_periods = ((payload.get("project_publish_summary") or {}).get("project_tracker_summary") or {}).get("total_periods")

    def overlay(funds: Any) -> list[dict[str, Any]]:
        normalized = []
        for fund in funds if isinstance(funds, list) else []:
            if not isinstance(fund, dict):
                continue
            code = _normalize_code(fund.get("基金代码"))
            values = tracker_occurrence_fields(rows.get(code))
            if values["出现次数"] is None or values["出现频率(%)"] is None:
                missing_codes.add(code)
            current = {**fund, **values}
            if "入选原因" in current:
                current["入选原因"] = tracker_occurrence_reason(current["入选原因"], values, total_periods)
            normalized.append(current)
        return normalized

    for field in ("rows", "funds"):
        if field in result:
            result[field] = overlay(result[field])
    portfolio = result.get("portfolio_suggestions")
    if isinstance(portfolio, dict):
        if "funds" in portfolio:
            portfolio["funds"] = overlay(portfolio["funds"])
        for group in portfolio.get("groups") or []:
            if isinstance(group, dict):
                group["funds"] = overlay(group.get("funds"))
        brief = result.get("portfolio_group_brief")
        if isinstance(brief, dict):
            for name, values in _build_portfolio_group_brief(portfolio).items():
                if isinstance(brief.get(name), dict):
                    brief[name]["reason_counts"] = values["reason_counts"]
                    if "top_reason_tags" in brief[name]:
                        brief[name]["top_reason_tags"] = [
                            {"label": label, "count": count}
                            for label, count in sorted(values["reason_counts"].items(), key=lambda item: -item[1])[:4]
                        ]
    result["tracker_occurrence_basis"] = {
        "schema_version": 1,
        "source": "same_revision_project_tracker_current_detail_json",
        "run_id": payload.get("run_id"),
        "revision": (payload.get("publication_control") or {}).get("revision"),
        "total_periods": total_periods,
        "available": bool(rows),
        "missing_codes": sorted(missing_codes),
        "missing_value_policy": "preserve_missing_semantics",
        "selection_scores_source": "committed_report_portfolio_suggestions",
        "observation_reasons_source": "same_revision_project_tracker_current_detail_json",
    }
    return result


def recommendations_display_report(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    """Make a response-only view for summary and Excel; never rewrite the report."""
    summary = payload.get("project_publish_summary") or {}
    overlay = overlay_recommendations_tracker_occurrences(
        settings, payload, {"portfolio_suggestions": summary.get("portfolio_suggestions") or {}},
    )
    return {
        **payload,
        "project_publish_summary": {**summary, "portfolio_suggestions": overlay["portfolio_suggestions"]},
        "tracker_occurrence_basis": overlay["tracker_occurrence_basis"],
    }


def _split_semicolon_text(value: Any, *, limit: int | None = None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    parts = [item.strip() for item in text.replace(";", "；").split("；") if item.strip()]
    return parts[:limit] if limit is not None else parts


def _build_portfolio_group_brief(portfolio_summary: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for group in list(portfolio_summary.get("groups") or []):
        if not isinstance(group, dict):
            continue
        group_name = str(group.get("name") or "").strip()
        funds = [dict(item) for item in list(group.get("funds") or []) if isinstance(item, dict)]
        if not group_name:
            continue
        style_labels = sorted({
            str(item.get("债基风格标签") or "").strip()
            for item in funds
            if str(item.get("债基风格标签") or "").strip()
        })
        confidence_counts: dict[str, int] = {}
        autostock_counts: dict[str, int] = {}
        stability_counts: dict[str, int] = {}
        holding_status_counts: dict[str, int] = {}
        risk_counts: dict[str, int] = {}
        reason_counts: dict[str, int] = {}
        for fund in funds:
            for source, target in (
                (fund.get("项目质量信心"), confidence_counts),
                (validation_status_value(fund), autostock_counts),
                (fund.get("长期稳定性标签"), stability_counts),
                (fund.get("持有期状态"), holding_status_counts),
            ):
                key = str(source or "").strip()
                if key:
                    target[key] = target.get(key, 0) + 1
            for item in _split_semicolon_text(fund.get("风险提示"), limit=3):
                if item and item != "当前未见额外显式风险提示":
                    risk_counts[item] = risk_counts.get(item, 0) + 1
            for item in _split_semicolon_text(fund.get("入选原因"), limit=4):
                if item:
                    reason_counts[item] = reason_counts.get(item, 0) + 1
        result[group_name] = {
            "fund_count": len(funds),
            "style_count": len(style_labels),
            "style_labels": style_labels,
            "confidence_counts": confidence_counts,
            "validation_counts": autostock_counts,
            "autostock_counts": autostock_counts,
            "stability_counts": stability_counts,
            "holding_status_counts": holding_status_counts,
            "risk_counts": risk_counts,
            "reason_counts": reason_counts,
        }
    return _runtime_scope(result, "")


def _build_publish_funnel_summary(payload: dict[str, Any]) -> dict[str, Any]:
    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    publish_summary = payload.get("project_publish_summary") if isinstance(payload.get("project_publish_summary"), dict) else {}
    threshold_comparison = (
        publish_summary.get("threshold_comparison")
        if isinstance(publish_summary.get("threshold_comparison"), dict)
        else {}
    )
    layers = payload.get("coverage_layers") if isinstance(payload.get("coverage_layers"), dict) else {}
    bond_universe = layers.get("bond_universe_coverage") if isinstance(layers.get("bond_universe_coverage"), dict) else {}
    stage_b = layers.get("stage_b_candidate_coverage") or layers.get("publishable_universe_coverage") or {}
    if not isinstance(stage_b, dict):
        stage_b = {}
    dynamic_reference_count = _to_int(
        threshold_comparison.get("dynamic_reference_export_count")
        or counts.get("dynamic_final_count")
        or threshold_comparison.get("dynamic_official_count")
    )
    return {
        "screenable_universe_count": _to_int(
            bond_universe.get("screenable_universe_count") or counts.get("screenable_universe_count")
        ),
        "governance_candidate_pool_count": _to_int(
            stage_b.get("governance_candidate_pool_count")
            or stage_b.get("stage_b_candidate_pool_count")
            or stage_b.get("publishable_pool_count")
            or counts.get("stage_b_candidate_pool_count")
            or counts.get("publishable_pool_count")
        ),
        "cross_validated_candidate_count": _to_int(
            counts.get("autostock_passed_count") or counts.get("autostock_validated_count")
        ),
        "autostock_filtered_count": _to_int(counts.get("autostock_filtered_count")),
        "official_publish_count": _to_int(counts.get("final_count")),
        "dynamic_reference_count": dynamic_reference_count,
        "dynamic_reference_export_count": dynamic_reference_count,
        "final_selection_removed_count": _to_int(
            (publish_summary.get("final_selection_contraction") or {}).get("removed_count")
            if isinstance(publish_summary.get("final_selection_contraction"), dict)
            else 0
        ),
    }


def _extract_style_threshold_payload(payload: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    threshold_comparison = (
        (payload.get("project_publish_summary") or {}).get("threshold_comparison")
        if isinstance(payload.get("project_publish_summary"), dict)
        else {}
    )
    threshold_rows = (
        threshold_comparison.get("style_thresholds")
        if isinstance(threshold_comparison, dict)
        else []
    )
    if not isinstance(threshold_rows, list):
        threshold_rows = []
    relevant_styles = {
        str(row.get("债基风格标签") or "").strip()
        for row in rows
        if str(row.get("债基风格标签") or "").strip()
    }
    styles: list[dict[str, Any]] = []
    global_entry: dict[str, Any] = {}
    seen_styles: set[str] = set()
    for item in threshold_rows:
        if not isinstance(item, dict):
            continue
        style = str(item.get("债基风格标签") or "").strip()
        if style == "全局基准":
            global_entry = dict(item)
            continue
        if style and style in relevant_styles:
            styles.append(dict(item))
            seen_styles.add(style)
    return {
        "styles": styles,
        "global": global_entry,
        "missing_styles": sorted(relevant_styles - seen_styles),
    }


def _normalize_group_fund(
    fund: dict[str, Any],
    *,
    tracker_row: dict[str, Any],
    data_source_by_code: dict[str, Any],
    annualized_return_by_code: dict[str, Any],
    annual_return_breakdown_by_code: dict[str, Any],
    style_metric_references: dict[str, Any],
) -> dict[str, Any]:
    current = dict(fund or {})
    current.pop("成立以来年化收益率(%)", None)
    code = _normalize_code(current.get("基金代码"))
    if code:
        current["基金代码"] = code

    data_source = (
        current.get("数据来源")
        or data_source_by_code.get(code)
        or tracker_row.get("数据来源")
        or ""
    )
    current["数据来源"] = "" if data_source is None else str(data_source).strip()
    annualized_return = current.get(SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL)
    if annualized_return is None or str(annualized_return).strip() == "":
        annualized_return = annualized_return_by_code.get(code)
    current[SINCE_INCEPTION_ANNUALIZED_RETURN_LABEL] = _normalize_number_text(annualized_return)
    current[SINCE_INCEPTION_ANNUAL_RETURN_BREAKDOWN_LABEL] = (
        annual_return_breakdown_by_code.get(code)
        if isinstance(annual_return_breakdown_by_code.get(code), list)
        else []
    )
    if isinstance(style_metric_references.get(code), dict):
        current[STYLE_METRIC_REFERENCES_LABEL] = style_metric_references.get(code) or {}

    # Preserve report values, but normalize core display fields the frontend/audit
    # expects to exist. Missing values intentionally remain empty.
    for field, digits in {
        "基金规模(亿)": 2,
        "费率合计(%)": 2,
        "正式投资评分": 4,
        "投资优先池得分": 4,
        "出现频率(%)": 2,
        "1年-年化收益(%)": 2,
        "近1月(%)": 2,
        "近3月(%)": 2,
        "1年-夏普比率": 4,
        "1年-最大回撤(%)": 4,
        "1年-波动率": 4,
    }.items():
        if field in current:
            current[field] = _normalize_number_text(current.get(field), digits=digits)
    return current


def build_recommendations_detail_response(
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    publish_summary = payload.get("project_publish_summary") if isinstance(payload.get("project_publish_summary"), dict) else {}
    portfolio_summary = (
        deepcopy(publish_summary.get("portfolio_suggestions"))
        if isinstance(publish_summary.get("portfolio_suggestions"), dict)
        else {}
    )
    tracker_payload = _load_tracker_detail_payload(settings, payload)
    tracker_rows = _tracker_rows_by_code(tracker_payload)
    derived_fields = tracker_payload.get("derived_fields") if isinstance(tracker_payload.get("derived_fields"), dict) else {}
    data_source_by_code = derived_fields.get("data_source_by_code") if isinstance(derived_fields.get("data_source_by_code"), dict) else {}
    annualized_return_by_code = derived_fields.get("annualized_return_by_code")
    # Earlier schemas used this map for an ambiguous local NAV-ratio return.
    # Only schema 2 establishes the platform-performance definition.
    if derived_fields.get("schema_version") != 2 or not isinstance(annualized_return_by_code, dict):
        annualized_return_by_code = {}
    annual_return_breakdown_by_code = (
        derived_fields.get("annual_return_breakdown_by_code")
        if isinstance(derived_fields.get("annual_return_breakdown_by_code"), dict)
        else {}
    )
    style_metric_references = (
        derived_fields.get("style_metric_references")
        if isinstance(derived_fields.get("style_metric_references"), dict)
        else {}
    )

    normalized_groups: list[dict[str, Any]] = []
    all_funds: list[dict[str, Any]] = []
    for group in list(portfolio_summary.get("groups") or []):
        if not isinstance(group, dict):
            continue
        normalized_funds: list[dict[str, Any]] = []
        for fund in list(group.get("funds") or []):
            if not isinstance(fund, dict):
                continue
            code = _normalize_code(fund.get("基金代码"))
            normalized = _normalize_group_fund(
                fund,
                tracker_row=tracker_rows.get(code) or {},
                data_source_by_code=data_source_by_code,
                annualized_return_by_code=annualized_return_by_code,
                annual_return_breakdown_by_code=annual_return_breakdown_by_code,
                style_metric_references=style_metric_references,
            )
            normalized["累计净值比年化(%)"] = (derived_fields.get("accum_nav_ratio_annualized_by_code") or {}).get(code, "")
            normalized["成立以来收益口径"] = (derived_fields.get("lifetime_return_observations") or {}).get(code) or {"status": "legacy_definition_unknown"}
            normalized_funds.append(normalized)
            all_funds.append(normalized)
        current_group = dict(group)
        current_group["funds"] = normalized_funds
        current_group["count"] = int(group.get("count") or len(normalized_funds))
        normalized_groups.append(current_group)

    portfolio_summary["groups"] = normalized_groups
    portfolio_summary["funds"] = all_funds
    portfolio_summary["fund_count"] = int(len(all_funds))
    portfolio_summary["group_count"] = int(len(normalized_groups))

    counts = payload.get("counts") if isinstance(payload.get("counts"), dict) else {}
    artifacts = payload.get("artifacts") if isinstance(payload.get("artifacts"), dict) else {}
    run_id = str(payload.get("run_id") or "").strip()
    publish_funnel_summary = _build_publish_funnel_summary(payload)
    portfolio_display_scope = _runtime_scope(
        build_portfolio_display_scope(counts, portfolio_summary),
        run_id,
    )
    publish_layer_semantics = _runtime_scope(
        build_publish_layer_semantics_summary(
            counts,
            publish_funnel_summary,
            portfolio_display_scope,
            portfolio_summary,
        ),
        run_id,
    )
    low_vol_watch = _runtime_scope(
        normalize_low_vol_positive_ratio_shadow_watch(publish_summary, report_identity=payload, settings=settings),
        run_id,
    )
    positive_ratio_semantics = _runtime_scope(
        build_positive_ratio_semantics_summary(publish_summary, low_vol_watch),
        run_id,
    )
    style_threshold_payload = _extract_style_threshold_payload(payload, all_funds)
    portfolio_group_brief = _build_portfolio_group_brief(portfolio_summary)
    portfolio_group_brief.update(
        {
            "run_id": run_id,
            "latest_canonical_run_id": run_id,
            "replay_precision_status": "missing",
            "replay_precision_status_label": "非精确回放",
            "auxiliary_scope": "same_run_report_artifact",
            "auxiliary_scope_label": "同 run 报告产物",
            "scope_detail": "当前摘要由同一报告中的 portfolio_suggestions 分组结果聚合得出。",
        }
    )
    response = {
        "run_id": payload.get("run_id"),
        "generated_at": payload.get("generated_at"),
        "report_timing_summary": _runtime_scope(
            publish_summary.get("report_timing_summary") if isinstance(publish_summary.get("report_timing_summary"), dict) else {},
            run_id,
        ),
        "tracker_summary": publish_summary.get("project_tracker_summary") or {},
        "portfolio_suggestions": portfolio_summary,
        "rows": all_funds,
        "funds": all_funds,
        "metric_units": metric_unit_metadata(),
        "portfolio_display_scope": portfolio_display_scope,
        "publish_layer_semantics_summary": publish_layer_semantics,
        "portfolio_group_brief": portfolio_group_brief,
        "generic_holding_placeholder_watch": _runtime_scope(
            publish_summary.get("generic_holding_placeholder_watch") if isinstance(publish_summary.get("generic_holding_placeholder_watch"), dict) else {},
            run_id,
        ),
        "low_vol_positive_ratio_shadow_watch": low_vol_watch,
        "positive_ratio_semantics": positive_ratio_semantics,
        "style_thresholds": style_threshold_payload.get("styles") or [],
        "global_style_threshold": style_threshold_payload.get("global") or {},
        "missing_style_thresholds": style_threshold_payload.get("missing_styles") or [],
        "style_metric_references": {
            code: value
            for code, value in style_metric_references.items()
            if code in {_normalize_code(item.get("基金代码")) for item in all_funds}
        },
        "counts": {
            "final_count": int(counts.get("final_count") or 0),
            "stable_bond_final_count": int(counts.get("stable_bond_final_count") or 0),
            "enhanced_bond_final_count": int(counts.get("enhanced_bond_final_count") or 0),
            "project_tracker_current_active_count": int(counts.get("project_tracker_current_active_count") or 0),
            "portfolio_group_count": int(len(normalized_groups)),
            "portfolio_fund_count": int(len(all_funds)),
        },
        "artifacts": {
            "filtered_excel": artifacts.get("filtered_excel"),
            "report_markdown": artifacts.get("report_markdown"),
            "project_tracker_excel": artifacts.get("project_tracker_excel"),
            "project_tracker_current_detail_json": artifacts.get("project_tracker_current_detail_json"),
        },
        "payload_scope": {
            "mode": "detail",
            "summary_endpoint": "/api/recommendations/latest",
            "detail_endpoint": "/api/recommendations/latest/detail",
            "source": "run_local_recommendations_detail_sidecar",
        },
    }
    return overlay_recommendations_tracker_occurrences(settings, payload, response, tracker_payload=tracker_payload)


def build_recommendations_detail_sidecar_payload(
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    cache_basis = build_recommendations_detail_cache_basis(settings, payload)
    response = attach_recommendations_detail_source_contract(
        build_recommendations_detail_response(settings, payload),
        cache_basis,
    )
    return {
        "artifact_type": "recommendations_detail_sidecar",
        "schema_version": RECOMMENDATIONS_DETAIL_SIDECAR_SCHEMA_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": payload.get("run_id"),
        "cache_basis": cache_basis,
        "response": response,
    }


def materialize_recommendations_detail_sidecar(
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    sidecar_path = recommendations_detail_sidecar_path(settings, payload)
    if sidecar_path is None:
        return {"written": False, "reason": "missing_or_invalid_run_id"}
    if (sidecar_path.parent / "validated.json").exists():
        return {"written": False, "reason": "immutable_validated_publication_revision", "path": str(sidecar_path)}
    sidecar_payload = build_recommendations_detail_sidecar_payload(settings, payload)
    _write_json_atomic(sidecar_path, sidecar_payload)
    response = sidecar_payload.get("response") if isinstance(sidecar_payload.get("response"), dict) else {}
    rows = response.get("rows") if isinstance(response.get("rows"), list) else []
    return {
        "written": True,
        "path": str(sidecar_path),
        "project_path": _project_relative(settings, sidecar_path),
        "run_id": sidecar_payload.get("run_id"),
        "row_count": len(rows),
        "portfolio_fund_count": int(((response.get("portfolio_suggestions") or {}).get("fund_count") or 0)),
    }
