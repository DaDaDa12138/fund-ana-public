"""Bounded pre-publication checks against actual run artifacts and fund metadata."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

from backend.fee_semantics import evaluate_fee_semantics, load_sales_fee_validation_classification_map
from backend.metric_units import format_metric_display_text, is_decimal_ratio_percent_field
from backend.project_publish_decisions import combine_holding_period_names, normalize_autostock_status, validation_status_value, DATA_VALIDATION_COLUMNS
from backend.project_report_exports import PERIOD_RETURN_SOURCE_COLUMNS
from core.utils.drawdown_duration import ANNUAL_DRAWDOWN_OBSERVATIONS, normalize_drawdown_duration_record
from core.utils.holding_period_policy import holding_period_gate_reason

FULL_NUMERIC_FIELDS = (
    "正式投资评分", "基金规模(亿)", "费率合计(%)",
    "近1周(%)", "近1月(%)", "近3月(%)", "近6月(%)", "近1年(%)", "近2年(%)", "近3年(%)", "今年来(%)", "成立来(%)",
    "1年-年化收益(%)", "1年-波动率", "1年-最大回撤(%)", "1年-夏普比率", "1年-索提诺比率", "1年-卡玛比率",
    "1年-VaR95(%)", "1年-VaR99(%)", "1年-正收益比率", ANNUAL_DRAWDOWN_OBSERVATIONS,
    "最新日增长率(%)", "最新单位净值", "最新累计净值",
)
FULL_TEXT_FIELDS = ("基金代码", "基金名称", "正式榜分组", "最新净值日期", "持有期状态", "基金类型", "AutoStock校验状态", *DATA_VALIDATION_COLUMNS)
SOURCE_METADATA_FIELDS = (
    "fcode", "shortname", "fullname", "holding_days", "has_holding_period", "holding_period_source", "holding_keywords", "mgr_fee_rate", "trust_fee_rate", "sales_fee_rate",
    "is_buyable", "purchase_status", "data_source", "data_source_governance_flag",
)
DRAWDOWN_SOURCE_CONTRACT = {
    'schema_version': 1,
    'field': ANNUAL_DRAWDOWN_OBSERVATIONS,
    'source': 'risk_snapshot_rows.max_drawdown_duration',
    'unit': 'nav_observations',
    'period': '1y',
    'includes_unrecovered': True,
}


def _input_signature(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def write_publication_basis(settings: Any, summary: dict[str, Any], *, official_records: list[dict[str, Any]],
                            risk_records: list[dict[str, Any]],
                            dynamic_records: list[dict[str, Any]] | None = None) -> Path:
    """Freeze the real selected inputs before export validation, preserving NULL."""
    truth_required = bool((summary.get("publication_control") or {}).get("truthfulness_checks_required"))
    source_records = [*official_records, *(dynamic_records or [])] if truth_required else official_records
    codes = sorted({_code(row.get("基金代码")) for row in source_records})
    code_set = set(codes)
    conn = sqlite3.connect(f"{settings.database_path.resolve().as_uri()}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    metadata = []
    try:
        conn.execute("BEGIN")
        available_columns = {str(item[1]) for item in conn.execute("PRAGMA table_info(funds)")}
        metadata_select = ", ".join(
            field if field in available_columns else f"NULL AS {field}"
            for field in SOURCE_METADATA_FIELDS
        )
        for offset in range(0, len(codes), 500):
            batch = codes[offset:offset + 500]
            placeholders = ",".join("?" for _ in batch)
            metadata.extend(dict(row) for row in conn.execute(
                f"SELECT {metadata_select} FROM funds WHERE fcode IN ({placeholders}) ORDER BY fcode", batch))
    finally:
        conn.close()
    inputs = {"official_rows": sorted(official_records, key=lambda row: _code(row.get("基金代码"))),
              "fund_metadata_rows": metadata,
              "risk_snapshot_rows": sorted((row for row in risk_records if _code(row.get("fcode")) in code_set), key=lambda row: _code(row.get("fcode")))}
    if truth_required:
        inputs["dynamic_rows"] = sorted(dynamic_records or [], key=lambda row: _code(row.get("基金代码")))
    if (summary.get("publication_control") or {}).get("csv_return_provenance_checks_required"):
        if dynamic_records is None:
            raise ValueError("dynamic_csv_return_provenance_inputs_missing")
        source_fields = ("基金代码", *PERIOD_RETURN_SOURCE_COLUMNS)
        for track, rows in (("formal", official_records), ("dynamic", dynamic_records)):
            if any(any(field not in row for field in source_fields) for row in rows):
                raise ValueError(f"{track}_csv_return_provenance_columns_missing")
        inputs["dynamic_csv_return_provenance_rows"] = sorted(
            ({field: row[field] for field in source_fields} for row in dynamic_records),
            key=lambda row: _code(row["基金代码"]),
        )
    from backend.metric_units import metric_unit_metadata

    path = Path(summary["artifacts"]["report_json"])
    path = (settings.project_root / path).parent / "publication_basis.json"
    payload = {"schema_version": 1, "run_id": summary["run_id"], "generated_at": summary["generated_at"],
               "scope": "selected_in_memory_report_inputs_and_uncoalesced_fund_metadata",
               "numeric_fields": list(FULL_NUMERIC_FIELDS), "text_fields": list(FULL_TEXT_FIELDS),
               "metric_units": metric_unit_metadata(), "inputs": inputs, "inputs_sha256": _input_signature(inputs),
               "source": {"database": str(settings.database_path), "tables": ["funds", "fund_risk_metrics"],
                          "risk_snapshot": "report_candidate_universe_context.risk_df", "official_rows": "materialized_export_context.official_export_full"}}
    if (summary.get('publication_control') or {}).get('drawdown_source_checks_required'):
        payload['drawdown_source_contract'] = dict(DRAWDOWN_SOURCE_CONTRACT)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    tracker_path = settings.project_root / summary["artifacts"]["project_tracker_current_detail_json"]
    tracker = json.loads(tracker_path.read_text(encoding="utf-8"))
    tracker.setdefault("source", {}).update({"publication_basis_json": str(path.relative_to(settings.project_root)),
                                             "publication_basis_sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    tracker.setdefault("derived_fields", {})["data_source_governance_flag_by_code"] = {
        _code(row["fcode"]): row["data_source_governance_flag"] for row in metadata}
    tracker_path.write_text(json.dumps(tracker, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    return path


def _code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _missing(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _same_number(left: Any, right: Any, *, allow_missing: bool) -> bool:
    if _missing(left) or _missing(right):
        return allow_missing and _missing(left) and _missing(right)
    a, b = _number(left), _number(right)
    return a is not None and b is not None and math.isclose(a, b, rel_tol=1e-8, abs_tol=1e-6)


def _drawdown_source_mismatches(
    expected: dict[str, dict[str, Any]], risk_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Check the risk source itself, not just copies of one exported value."""
    mismatches = []
    sources: dict[str, dict[str, Any]] = {}
    missing_count = 0
    for source in risk_rows:
        code = _code(source.get('fcode'))
        if code in sources:
            mismatches.append({'fcode': code, 'reason': 'duplicate_risk_source'})
        sources[code] = source
    for code, row in expected.items():
        source = sources.get(code, {})
        duration = source.get('max_drawdown_duration')
        if ('max_drawdown_duration' not in source or ANNUAL_DRAWDOWN_OBSERVATIONS not in row
                or not _same_number(row.get(ANNUAL_DRAWDOWN_OBSERVATIONS), duration, allow_missing=True)):
            mismatches.append({'fcode': code, 'reason': 'risk_duration_value_or_presence',
                               'risk_value': duration, 'report_value': row.get(ANNUAL_DRAWDOWN_OBSERVATIONS)})
        if (source.get('period') != '1y' or not source.get('end_date')
                or str(source.get('end_date')) != str(row.get('最新净值日期'))):
            mismatches.append({'fcode': code, 'reason': 'risk_period_or_nav_date'})
        if _missing(duration):
            missing_count += 1
            continue
        number = _number(duration)
        points = _number(source.get('data_points'))
        if (isinstance(duration, bool) or number is None or not number.is_integer()
                or number < 0 or points is None or number > points):
            mismatches.append({'fcode': code, 'reason': 'invalid_observation_count'})
        try:
            start = date.fromisoformat(str(source.get('start_date')))
            end = date.fromisoformat(str(source.get('end_date')))
            datetime.fromisoformat(str(source.get('calculated_at')))
            signature = source.get('nav_snapshot_signature')
            # build_nav_snapshot_signature uses SHA-1 hex truncated to 24
            # characters; it is distinct from the publication file SHA-256.
            if (start > end or not isinstance(signature, str)
                    or re.fullmatch(r'[0-9a-f]{24}', signature) is None):
                raise ValueError('invalid_risk_source_binding')
        except (TypeError, ValueError):
            mismatches.append({'fcode': code, 'reason': 'risk_source_binding_missing_or_invalid'})
    return mismatches, missing_count


def build_publication_member_checks(settings: Any, summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Check membership, units and hard gates before a report can be promoted.

    Do not use preview rows: these checks read the full formal CSV and the
    exact run-local Tracker JSON used by the API. Missing evidence fails closed.
    """
    checks: list[dict[str, Any]] = []
    artifacts = summary.get("artifacts") or {}
    counts = summary.get("counts") or {}
    run_id = str(summary.get("run_id") or "")
    csv_fields: dict[str, list[str]] = {}

    def add(name: str, aligned: bool, detail: str, **evidence: Any) -> None:
        checks.append({"name": name, "aligned": bool(aligned), "detail": detail, **evidence})

    def path_for(key: str) -> Path:
        value = artifacts.get(key)
        if not value:
            raise ValueError(f"missing artifact: {key}")
        path = Path(value)
        if not path.is_absolute():
            path = settings.project_root / path
        path = path.resolve()
        if not path.is_relative_to((settings.runs_dir / run_id).resolve()):
            raise ValueError(f"artifact is not run-local: {key}")
        return path

    def read_csv(key: str) -> list[dict[str, Any]]:
        with path_for(key).open(encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            csv_fields[key] = reader.fieldnames or []
            return list(reader)

    def index_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            row = normalize_drawdown_duration_record(row)
            code = _code(row.get("基金代码"))
            if len(code) != 6 or not code.isdigit() or code in result:
                raise ValueError(f"invalid or duplicate fund code: {code!r}")
            result[code] = row
        return result

    try:
        drawdown_checks_required = bool((summary.get('publication_control') or {}).get('drawdown_source_checks_required'))
        if (((summary.get("publication_control") or {}).get("csv_return_provenance_checks_required") or drawdown_checks_required)
                and not (summary.get("publication_control") or {}).get("full_value_checks_required")):
            if (summary.get('publication_control') or {}).get('csv_return_provenance_checks_required'):
                raise ValueError('csv_return_provenance_requires_full_value_checks')
            raise ValueError('drawdown_provenance_requires_full_value_checks')
        formal = index_rows(read_csv("filtered_csv"))
        dynamic = index_rows(read_csv("dynamic_filtered_csv"))
        tracker_payload = json.loads(path_for("project_tracker_current_detail_json").read_text(encoding="utf-8"))
        tracker = index_rows(tracker_payload["rows"])
        add(
            "formal_csv_tracker_membership",
            set(formal) == set(tracker)
            and len(formal) == counts.get("final_count")
            and len(tracker) == tracker_payload.get("row_count")
            and tracker_payload.get("run_id") == run_id,
            "正式 CSV 与当前 Tracker 的完整成员、数量和批次一致",
            formal_count=len(formal), tracker_count=len(tracker),
            missing_in_tracker=sorted(set(formal) - set(tracker)),
            unexpected_in_tracker=sorted(set(tracker) - set(formal)),
        )
        add("dynamic_csv_count", len(dynamic) == counts.get("dynamic_final_count"),
            "动态参考 CSV 数量与报告一致", actual_count=len(dynamic))

        numeric_fields = ("正式投资评分", "1年-年化收益(%)", "1年-波动率", "1年-最大回撤(%)", "1年-夏普比率")
        mismatches: list[dict[str, str]] = []
        for code in set(formal) & set(tracker):
            for field in (*numeric_fields, "最新净值日期", "正式榜分组"):
                left, right = formal[code].get(field), tracker[code].get(field)
                if field in numeric_fields:
                    if is_decimal_ratio_percent_field(field):
                        # CSV is human-readable percent points (2 decimals);
                        # Tracker/API retain the underlying decimal ratio.
                        right = format_metric_display_text(field, right)
                    a, b = _number(left), _number(right)
                    aligned = a is not None and b is not None and math.isclose(a, b, rel_tol=1e-8, abs_tol=1e-4)
                else:
                    aligned = bool(left) and left == right
                if not aligned:
                    mismatches.append({"fcode": code, "field": field})
        add("formal_tracker_metric_values", not mismatches,
            "正式 CSV 与 Tracker 的评分、收益、风险单位、日期及分组一致",
            mismatch_count=len(mismatches), samples=mismatches[:20])

        if (summary.get("publication_control") or {}).get("full_value_checks_required"):
            basis = json.loads(path_for("publication_basis_json").read_text(encoding="utf-8"))
            inputs = basis["inputs"]
            expected = index_rows(inputs["official_rows"])
            signature_ok = basis.get("schema_version") == 1 and basis.get("run_id") == run_id and basis.get("inputs_sha256") == _input_signature(inputs)
            signature_ok = signature_ok and (tracker_payload.get("source") or {}).get("publication_basis_sha256") == hashlib.sha256(path_for("publication_basis_json").read_bytes()).hexdigest()
            add("publication_input_signature", signature_ok and set(expected) == set(formal),
                "冻结的筛选输出与真实元数据输入签名、完整成员一致", inputs_sha256=basis.get("inputs_sha256"))
            if drawdown_checks_required:
                source_mismatches, missing_count = _drawdown_source_mismatches(expected, inputs['risk_snapshot_rows'])
                if basis.get('drawdown_source_contract') != DRAWDOWN_SOURCE_CONTRACT:
                    source_mismatches.append({'reason': 'drawdown_source_contract_missing_or_invalid'})
                if ANNUAL_DRAWDOWN_OBSERVATIONS not in csv_fields['filtered_csv']:
                    source_mismatches.append({'reason': 'canonical_csv_duration_column_missing'})
                if any(ANNUAL_DRAWDOWN_OBSERVATIONS not in row for row in tracker_payload['rows']):
                    source_mismatches.append({'reason': 'canonical_tracker_duration_column_missing'})
                add('formal_drawdown_matches_current_risk_source', not source_mismatches,
                    '最长回撤观测数逐值绑定当前年度风险来源、净值日期和单位；缺失单独计数',
                    checked_count=len(expected), missing_duration_count=missing_count,
                    mismatch_count=len(source_mismatches), samples=source_mismatches[:30])
            if (summary.get("publication_control") or {}).get("csv_return_provenance_checks_required"):
                dynamic_sources = index_rows(inputs["dynamic_csv_return_provenance_rows"])
                source_mismatches = []
                for artifact, rows, sources in (("filtered_csv", formal, expected),
                                                ("dynamic_filtered_csv", dynamic, dynamic_sources)):
                    for field in PERIOD_RETURN_SOURCE_COLUMNS:
                        if field not in csv_fields[artifact]:
                            source_mismatches.append({"artifact": artifact, "field": field, "reason": "column_missing"})
                    if set(rows) != set(sources):
                        source_mismatches.append({"artifact": artifact, "reason": "source_membership_mismatch"})
                    for code in sorted(set(rows) & set(sources)):
                        for field in PERIOD_RETURN_SOURCE_COLUMNS:
                            wanted, value = sources[code].get(field), rows[code].get(field)
                            aligned = (field in sources[code] and field in rows[code]
                                       and ((_missing(wanted) and _missing(value))
                                            or (isinstance(wanted, str) and wanted == value)))
                            if not aligned:
                                source_mismatches.append({"artifact": artifact, "fcode": code, "field": field,
                                                          "expected": wanted, "actual": value})
                add("csv_return_provenance_matches_frozen_inputs", not source_mismatches,
                    "正式及动态 CSV 的收益日期、来源与来源 SHA 逐值对应各自冻结输入",
                    mismatch_count=len(source_mismatches), samples=source_mismatches[:30],
                    fields=list(PERIOD_RETURN_SOURCE_COLUMNS), formal_count=len(formal), dynamic_count=len(dynamic))
            all_mismatches = []
            from backend.project_tracker import _read_project_tracker_export_sheet, _read_project_tracker_process_sheet

            workbook = index_rows(_read_project_tracker_export_sheet(path_for("project_tracker_excel"), "当前在榜").to_dict(orient="records"))
            add("formal_workbook_complete_membership", set(workbook) == set(expected),
                "Tracker Excel 当前在榜工作表与冻结正式名单的完整成员一致",
                workbook_count=len(workbook), expected_count=len(expected))
            tracker_metadata = json.loads(path_for("project_tracker_excel_meta").read_text(encoding="utf-8"))
            tracker_summary = tracker_metadata.get("accumulation_summary") or {}
            report_tracker_summary = (summary.get("project_publish_summary") or {}).get("project_tracker_summary") or {}
            process_rows = _read_project_tracker_process_sheet(path_for("project_tracker_excel")).to_dict(orient="records")
            observation_keys = {
                ("nav", str(row["净值快照日期"]).strip()) if str(row.get("净值快照日期") or "").strip()
                else ("signature", str(row.get("快照签名") or "").strip())
                for row in process_rows
                if str(row.get("净值快照日期") or "").strip() or str(row.get("快照签名") or "").strip()
            }
            observed_periods = len(observation_keys)
            reported_periods = {
                "report_counts": counts.get("project_tracker_total_periods"),
                "report_summary": report_tracker_summary.get("total_periods"),
                "report_unique": report_tracker_summary.get("unique_snapshot_count"),
                "metadata_summary": tracker_summary.get("total_periods"),
                "metadata_unique": tracker_summary.get("unique_snapshot_count"),
            }
            add("tracker_observation_period_counts",
                observed_periods > 0 and all(_number(value) == observed_periods for value in reported_periods.values())
                and _number(tracker_summary.get("processed_run_count")) == len(process_rows),
                "累计观察期数对应处理记录中的唯一净值快照，重复生成不计为新增观察期",
                observed_periods=observed_periods, processed_row_count=len(process_rows), reported_periods=reported_periods)
            for code in set(formal) & set(tracker) & set(expected):
                for field in (*FULL_NUMERIC_FIELDS, *FULL_TEXT_FIELDS):
                    source = expected[code].get(field)
                    for target_name, row in (("tracker", tracker[code]), ("csv", formal[code]), ("workbook", workbook.get(code, {}))):
                        if target_name == "csv" and field not in row:
                            continue  # Compact CSV intentionally excludes fees and detailed metadata.
                        value = row.get(field)
                        wanted = source
                        if field in FULL_NUMERIC_FIELDS:
                            if target_name == "csv" and is_decimal_ratio_percent_field(field) and not _missing(source):
                                wanted = format_metric_display_text(field, source)
                            aligned = field in row and _same_number(wanted, value, allow_missing=True)
                        else:
                            aligned = field in row and str(wanted or "") == str(value or "")
                        if not aligned:
                            all_mismatches.append({"fcode": code, "field": field, "target": target_name,
                                                   "expected": wanted, "actual": value})
            add("formal_all_values_from_frozen_inputs", not all_mismatches,
                "全部正式收益、风险、NAV、费用与文本字段逐值对应冻结输入，缺失与真实零分开",
                mismatch_count=len(all_mismatches), samples=all_mismatches[:30], fields=list(FULL_NUMERIC_FIELDS))
            source_rows = {_code(row.get("fcode")): row for row in inputs["fund_metadata_rows"]}
            source_by_code = (tracker_payload.get("derived_fields") or {}).get("data_source_by_code") or {}
            governance_by_code = (tracker_payload.get("derived_fields") or {}).get("data_source_governance_flag_by_code") or {}
            source_mismatches = []
            for code, row in tracker.items():
                raw = source_rows.get(code) or {}
                actual_source = source_by_code.get(code)
                if (not raw.get("data_source") or str(raw.get("data_source")) != str(actual_source)
                        or raw.get("data_source_governance_flag") != governance_by_code.get(code)):
                    source_mismatches.append({"fcode": code, "field": "data_source"})
                fees = [raw.get(key) for key in ("mgr_fee_rate", "trust_fee_rate", "sales_fee_rate")]
                if all(_number(value) is not None for value in fees):
                    if not _same_number(sum(float(value) for value in fees), row.get("费率合计(%)"), allow_missing=False):
                        source_mismatches.append({"fcode": code, "field": "fee_total"})
                elif not _missing(row.get("费率合计(%)")):
                    source_mismatches.append({"fcode": code, "field": "missing_fee_rendered_as_total"})
            add("formal_fee_and_source_provenance", not source_mismatches,
                "Tracker 费用来自真实组件合计，来源标签对应冻结的原始元数据",
                mismatch_count=len(source_mismatches), samples=source_mismatches[:30])

        if (summary.get("publication_control") or {}).get("truthfulness_checks_required"):
            from backend.truthfulness_gate import publication_truthfulness_checks
            if not (summary.get("publication_control") or {}).get("full_value_checks_required"):
                raise ValueError("truthfulness_checks_require_frozen_inputs")
            checks.extend(publication_truthfulness_checks(summary, inputs,
                max_snapshot_age_days=getattr(settings, "publication_max_snapshot_age_days", 10)))

        selected = {**dynamic, **formal}
        validation_map = load_sales_fee_validation_classification_map(audit_dir=settings.project_root / "core" / "audit")
        conn = sqlite3.connect(f"{settings.database_path.resolve().as_uri()}?mode=ro", uri=True, timeout=10)
        conn.row_factory = sqlite3.Row
        metadata: dict[str, dict[str, Any]] = {}
        try:
            conn.execute("BEGIN")
            available_columns = {str(item[1]) for item in conn.execute("PRAGMA table_info(funds)")}
            metadata_select = ", ".join(
                field if field in available_columns else f"NULL AS {field}"
                for field in ("fcode", "shortname", "fullname", "holding_days", "has_holding_period",
                              "holding_period_source", "holding_keywords", "mgr_fee_rate",
                              "trust_fee_rate", "sales_fee_rate")
            )
            codes = sorted(selected)
            for offset in range(0, len(codes), 500):
                batch = codes[offset:offset + 500]
                placeholders = ",".join("?" for _ in batch)
                for row in conn.execute(
                    f"SELECT {metadata_select} FROM funds WHERE fcode IN ({placeholders})",
                    batch,
                ):
                    metadata[_code(row["fcode"])] = dict(row)
        finally:
            conn.close()
        violations: list[dict[str, str]] = []
        holding_limit = min(90, int(getattr(settings, "exclude_holding_days_gt", 90)))
        for code, row in selected.items():
            fund = metadata.get(code)
            if fund is None:
                violations.append({"fcode": code, "reason": "fund_metadata_missing"})
                continue
            holding_reason = holding_period_gate_reason(
                has_holding_period=fund.get("has_holding_period"),
                holding_days=fund.get("holding_days"),
                holding_period_source=fund.get("holding_period_source"),
                threshold_days=holding_limit,
                fund_name=combine_holding_period_names(fund.get("shortname"), fund.get("fullname")),
                holding_keywords=fund.get("holding_keywords"),
            )
            if holding_reason == "holding_period_limit" or (
                (summary.get("publication_control") or {}).get("truthfulness_checks_required")
                and holding_reason
            ):
                violations.append({"fcode": code, "reason": holding_reason})
            fee = evaluate_fee_semantics(
                fcode=code, mgr_fee_rate=fund["mgr_fee_rate"], trust_fee_rate=fund["trust_fee_rate"],
                sales_fee_rate=fund["sales_fee_rate"], validation_map=validation_map,
            )
            if not fee["screening_ready"]:
                violations.append({"fcode": code, "reason": "fee_data_missing"})
            statuses = [
                validation_status_value(item)
                for item in (formal.get(code), dynamic.get(code), tracker.get(code)) if item is not None
            ]
            if any(normalize_autostock_status(status) == "hard_block" for status in statuses):
                violations.append({"fcode": code, "reason": "data_validation_hard_block" if (summary.get("data_validation_audit") or {}).get("required") else "autostock_hard_block"})
            if (summary.get("data_validation_audit") or {}).get("required"):
                if any(normalize_autostock_status(status) not in {"strict_match", "soft_pass_with_warnings"} for status in statuses):
                    violations.append({"fcode": code, "reason": "data_validation_unverified"})
                for item in (formal.get(code), dynamic.get(code), tracker.get(code)):
                    if item is not None and (item.get("数据校验来源") != "eastmoney_cross_endpoint"
                                             or not item.get("数据校验契约版本")):
                        violations.append({"fcode": code, "reason": "data_validation_source_missing"})
        add("publication_member_hard_gates", not violations,
            "正式及动态名单复核结构化持有期、费率完整性和数据校验硬阻断",
            checked_count=len(selected), violation_count=len(violations), samples=violations[:20])
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, AttributeError) as exc:
        add("publication_member_evidence_readable", False, f"{type(exc).__name__}: {exc}")
    return checks
