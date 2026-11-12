"""Fresh two-endpoint fund checks without AutoStock or publication cache reuse.

FundValuationLast supplies percentage returns; FundMNFInfo confirms the row's
identity, date and ordinary NAV. Both belong to Eastmoney/Tiantian. This is an
endpoint consistency contract, never evidence of independent suppliers.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import gzip
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import requests

PROVIDER = "eastmoney_cross_endpoint"
SOURCE = PROVIDER
CONTRACT = "fund-endpoint-cross-validation-2026-10-13.1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
RETURN_MAP = {"return_1w": "SYL_Z", "return_1m": "SYL_Y", "return_3m": "SYL_3Y",
              "return_6m": "SYL_6Y", "return_1y": "SYL_1N"}
LOCAL_RETURN_MAP = {"return_1w": "近1周(%)", "return_1m": "近1月(%)", "return_3m": "近3月(%)",
                    "return_6m": "近6月(%)", "return_1y": "近1年(%)"}
FIELDS = ",".join(("FCODE", "SHORTNAME", "FUNDTYPE", "NAV", "PDATE", *RETURN_MAP.values()))
HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0",
           "Referer": "https://h5.1234567.com.cn/", "Content-Type": "application/x-www-form-urlencoded"}
INFO_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0",
                "Referer": "https://fund.eastmoney.com/"}
DEFAULT_CONFIG = {"nav_relative_tolerance_pct": 1.0, "nav_absolute_tolerance": 0.001,
                  "return_relative_tolerance_pct": 5.0, "return_absolute_tolerance": 0.5,
                  "return_small_base_threshold": 1.0, "nav_date_soft_lag_days": 5,
                  "residual_missing_max_count": 2, "residual_missing_max_ratio": 0.002}
TYPE_LABELS = {"001": "股票型", "002": "混合型", "003": "债券型", "005": "货币型", "007": "QDII"}


def _code(value: Any) -> str | None:
    return value if isinstance(value, str) and len(value) == 6 and value.isascii() and value.isdigit() else None


def _text(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip() or value.strip() in {"--", "-"}:
        return None
    return value.strip()


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _field(row: Mapping[str, Any], field: str) -> tuple[float | None, str]:
    if field not in row:
        return None, "field_not_returned"
    raw = row[field]
    if raw is None or isinstance(raw, str) and raw.strip() in {"", "--", "-", "null"}:
        return None, "upstream_null"
    value = _number(raw)
    return value, "parse_failed" if value is None else "real_zero" if value == 0 else "value"


def _date(value: Any) -> str | None:
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat() if parsed.isoformat() == value else None


def _request(
    send: Callable[..., Any], endpoint: str, params: dict[str, Any], *, method: str,
    clock: datetime, audit_dir: Path, kind: str,
) -> tuple[list[dict[str, Any]] | None, dict[str, Any], int | None]:
    evidence = {"endpoint": endpoint, "method": method, "params": params,
                "request_started_at": datetime.now(SHANGHAI).isoformat(),
                "fetched_at": None, "http_status": None, "response_sha256": None,
                "response_artifact": None, "endpoint_kind": kind}
    total = None
    try:
        kwargs = {"headers": HEADERS if method == "POST" else INFO_HEADERS, "timeout": (5, 12)}
        kwargs["data" if method == "POST" else "params"] = params
        response = send(endpoint, **kwargs)
        evidence["http_status"] = response.status_code
        body = response.content
        digest = hashlib.sha256(body).hexdigest()
        audit_dir.mkdir(parents=True, exist_ok=True)
        body_path = audit_dir / f"{digest}.response.json.gz"
        if not body_path.exists():
            body_path.write_bytes(gzip.compress(body, mtime=0))
        evidence.update(response_sha256=digest, response_artifact=str(body_path), hash_basis="http_response_body")
        if response.status_code != 200:
            raise ValueError(f"http_status_{response.status_code}")
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("response_not_object")
        if kind == "valuation":
            if payload.get("success") is not True or str(payload.get("errorCode")) != "0":
                raise ValueError("business_response_failed")
            rows = payload.get("data")
        else:
            if (payload.get("Success") is not True or type(payload.get("ErrCode")) is not int
                    or payload["ErrCode"] != 0
                    or ("ErrorCode" in payload and payload["ErrorCode"] != "0")):
                raise ValueError("business_response_failed")
            rows = payload.get("Datas")
            total = payload.get("TotalCount")
            if type(total) is not int or total < 0:
                raise ValueError("pagination_total_invalid")
        if rows is None:
            rows = []
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ValueError("invalid_data_shape")
        evidence.update(response_state="rows" if rows else "empty", returned_count=len(rows))
    except (requests.RequestException, ValueError, TypeError, OSError) as exc:
        evidence.update(response_state="failed", error_type=type(exc).__name__, error=str(exc))
        rows = None
    evidence["fetched_at"] = datetime.now(SHANGHAI).isoformat()
    audit_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = audit_dir / f"{datetime.now(SHANGHAI).strftime('%H%M%S_%f')}_{kind}.json"
    evidence["metadata_artifact"] = str(metadata_path)
    metadata_path.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return rows, evidence, total


def fetch_cross_validation(
    settings: Any, codes: list[str], *, post: Callable[..., Any] | None = None,
    get: Callable[..., Any] | None = None, now: datetime | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fetch new observations with finite batches/timeouts and explicit pagination.

    Returns incomplete metadata on transport/business failure. No AutoStock
    calls, source cache reuse or synthesized rows occur. Invalid observations
    retain their raw evidence and cannot pass ``compare_endpoint_candidate``.
    """
    requested = list(dict.fromkeys(codes))
    if any(_code(code) is None for code in requested):
        raise ValueError("invalid_fund_code")
    clock = now or datetime.now(SHANGHAI)
    clock = clock.replace(tzinfo=SHANGHAI) if clock.tzinfo is None else clock.astimezone(SHANGHAI)
    audit_dir = Path(settings.reports_dir) / "audit" / "fund_cross_validation" / clock.strftime("%Y%m%d")
    attempts: list[dict[str, Any]] = []
    valuations: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    info: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    endpoints = list(dict.fromkeys((settings.tiantian_valuation_primary_url, settings.tiantian_valuation_fallback_url)))
    for offset in range(0, len(requested), 50):
        batch = requested[offset:offset + 50]
        pending = batch
        for endpoint in endpoints:
            if not pending:
                break
            rows, evidence, _ = _request(post or requests.post, endpoint,
                {"FCODES": ",".join(pending), "FIELDS": FIELDS}, method="POST", clock=clock, audit_dir=audit_dir, kind="valuation")
            evidence["requested_codes"] = list(pending)
            observed = [row.get("FCODE") for row in rows or []]
            if rows is not None and (any(_code(code) is None or code not in pending for code in observed)
                                     or len(observed) != len(set(observed))):
                evidence.update(response_state="failed", error="duplicate_or_unexpected_identity")
                rows = None
            attempts.append(evidence)
            for row in rows or []:
                valuations[row["FCODE"]] = (row, evidence)
            pending = [code for code in pending if code not in valuations]
    info_url = settings.eastmoney_fund_info_url
    for offset in range(0, len(requested), 20):
        batch = requested[offset:offset + 20]
        collected: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        declared_total = None
        # A page can contain fewer rows than requested. At most one request per
        # requested row plus a terminal page, so malformed pagination is bounded.
        for page in range(1, len(batch) + 2):
            params = {"Fcodes": ",".join(batch), "plat": "Android", "deviceid": "fund-ana",
                      "product": "EFund", "Version": "6.6.8", "appType": "ttjj",
                      "pageIndex": page, "pageSize": len(batch)}
            rows, evidence, total = _request(get or requests.get, info_url, params,
                method="GET", clock=clock, audit_dir=audit_dir, kind="info")
            evidence["requested_codes"] = list(batch)
            observed = [row.get("FCODE") for row in rows or []]
            invalid = (rows is not None and (total > len(batch) or (declared_total is not None and total != declared_total)
                or any(_code(code) is None or code not in batch or code in collected for code in observed)
                or len(observed) != len(set(observed)) or len(observed) + len(collected) > total))
            if invalid:
                evidence.update(response_state="failed", error="pagination_or_identity_invalid")
                rows = None
            attempts.append(evidence)
            if rows is None:
                collected.clear()
                break
            declared_total = total
            for row in rows:
                collected[row["FCODE"]] = (row, evidence)
            if not rows or len(collected) >= total:
                break
        info.update(collected)
    # Identity/pagination checks happen after parsing the body. Persist their
    # final outcomes alongside the original entity hash, including failed pages.
    for attempt in attempts:
        Path(attempt["metadata_artifact"]).write_text(
            json.dumps(attempt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    result: dict[str, dict[str, Any]] = {}
    invalid_codes: list[str] = []
    max_age = int(getattr(settings, "publication_max_snapshot_age_days", 10))
    for code in requested:
        if code not in valuations:
            continue
        row, valuation_evidence = valuations[code]
        info_row, info_evidence = info.get(code, ({}, {}))
        errors = []
        nav, nav_state = _field(row, "NAV")
        nav_date = _date(row.get("PDATE"))
        name, type_code = _text(row.get("SHORTNAME")), _text(row.get("FUNDTYPE"))
        if not name:
            errors.append("external_name_missing")
        if type_code not in TYPE_LABELS:
            errors.append("external_type_missing_or_unknown")
        if type_code == "005":
            errors.append("monetary_nav_semantics_unsupported")
        if nav is None or nav <= 0:
            errors.append("external_nav_invalid")
        if nav_date is None:
            errors.append("external_nav_date_invalid")
        else:
            age = (clock.date() - date.fromisoformat(nav_date)).days
            if age < 0 or age > max_age:
                errors.append("external_nav_date_future" if age < 0 else "external_nav_date_stale")
        if not info_row:
            errors.append("fund_info_missing")
        else:
            info_nav = _number(info_row.get("NAV"))
            if info_nav is None or info_nav <= 0:
                errors.append("fund_info_nav_invalid")
            elif nav is not None and not math.isclose(nav, info_nav, rel_tol=0, abs_tol=1e-8):
                errors.append("endpoint_nav_mismatch")
            if _date(info_row.get("PDATE")) is None or _date(info_row.get("PDATE")) != nav_date:
                errors.append("endpoint_nav_date_mismatch")
            if not _text(info_row.get("SHORTNAME")):
                errors.append("fund_info_name_missing")
        snapshot: dict[str, Any] = {"code": code, "name": name, "type": TYPE_LABELS.get(type_code),
            "fund_type_code": type_code, "netWorth": nav, "netWorthDate": nav_date,
            "period_return_date": nav_date, "provider": PROVIDER, "contract": CONTRACT,
            "independent_source": False, "_field_states": {"netWorth": nav_state},
            "_raw_fields": {"valuation": dict(row), "info": dict(info_row)},
            "_source_evidence": {"provider": PROVIDER, "contract": CONTRACT, "independent_source": False,
                                 "valuation": valuation_evidence, "info": info_evidence}}
        for field, raw_field in RETURN_MAP.items():
            snapshot[field], snapshot["_field_states"][field] = _field(row, raw_field)
            if snapshot[field] is None:
                errors.append(f"{field}_missing" if snapshot["_field_states"][field] != "parse_failed" else f"{field}_parse_failed")
        snapshot["_contract_errors"] = errors
        result[code] = snapshot
        if errors:
            invalid_codes.append(code)
    missing = [code for code in requested if code not in result]
    info_missing = [code for code in requested if code not in info]
    endpoint_missing = [code for code in requested if code in set(missing) | set(info_missing)]
    unresolved_errors = [attempt.get("error") for attempt in attempts if attempt.get("error")
                         and set(attempt["requested_codes"]) & set(endpoint_missing)]
    complete = not missing and not invalid_codes
    available = bool(valuations) and bool(info) if requested else True
    missing_ratio = len(endpoint_missing) / len(requested) if requested else 0.0
    residual_demoted = (bool(endpoint_missing) and len(endpoint_missing) <= DEFAULT_CONFIG["residual_missing_max_count"]
                        and missing_ratio <= DEFAULT_CONFIG["residual_missing_max_ratio"] and not unresolved_errors)
    source_evidence = {"provider": PROVIDER, "source": PROVIDER, "contract": CONTRACT, "supplier_group": "eastmoney_tiantian",
        "independent_source": False, "verification_scope": "same_supplier_endpoint_consistency",
        "request_window": {"started_at": attempts[0]["request_started_at"] if attempts else None,
                           "finished_at": attempts[-1]["fetched_at"] if attempts else None},
        "endpoints": list(dict.fromkeys(attempt["endpoint"] for attempt in attempts)),
        "base_url": info_url, "request_count": len(attempts), "source_complete": complete,
        "requested_count": len(requested), "returned_count": len(result), "valid_count": len(result) - len(invalid_codes),
        "missing_codes": missing, "invalid_codes": invalid_codes, "info_missing_codes": info_missing,
        "endpoint_missing_codes": endpoint_missing, "missing_count": len(endpoint_missing),
        "last_error": unresolved_errors[-1] if unresolved_errors else None,
        "residual_missing_count": len(endpoint_missing), "residual_missing_ratio": round(missing_ratio, 8),
        "residual_missing_max_count": DEFAULT_CONFIG["residual_missing_max_count"],
        "residual_missing_max_ratio": DEFAULT_CONFIG["residual_missing_max_ratio"],
        "residual_missing_demoted_to_hard_block": residual_demoted,
        "missing_gate_passed": not endpoint_missing or residual_demoted,
        "attempts": attempts, "cache_reused_count": 0}
    meta = {**source_evidence, "status": "empty" if not requested else "ok" if complete else "partial" if available else "unavailable",
            "source_available": available, "source_evidence": source_evidence, "effective_batch_size": 50,
            "info_batch_size": 20, "batch_count": len(attempts), "cache_hit": False}
    return result, meta


def _return_compare(local: Any, external: Any, config: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    local, external = _number(local), _number(external)
    detail = {"local": local, "external": external, "mode": "missing", "abs_diff": None,
              "absolute_diff_pct_point": None, "relative_diff_pct": None,
              "absolute_tolerance_pct_point": config["return_absolute_tolerance"],
              "relative_tolerance_pct": config["return_relative_tolerance_pct"]}
    if local is None or external is None:
        return False, detail
    absolute = abs(local - external)
    relative = absolute / max(abs(external), 0.01) * 100
    absolute_match = absolute <= config["return_absolute_tolerance"]
    relative_match = relative <= config["return_relative_tolerance_pct"]
    small = abs(external) < config["return_small_base_threshold"]
    detail.update(mode="absolute_pct_point" if small else "relative_pct" if relative_match else "absolute_pct_point_fallback",
                  abs_diff=round(absolute, 4), absolute_diff_pct_point=round(absolute, 4),
                  relative_diff_pct=round(relative, 4), absolute_match=absolute_match, relative_match=relative_match)
    return absolute_match if small else absolute_match or relative_match, detail


def compare_endpoint_candidate(local: Mapping[str, Any], upstream: Mapping[str, Any], config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Apply the existing numerical tolerances and one-week-only soft warning."""
    policy = {**DEFAULT_CONFIG, **(config or {})}
    hard = list(upstream.get("_contract_errors") or [])
    warning: list[str] = []
    code = _code(local.get("基金代码", local.get("fcode")))
    external_code = _code(upstream.get("code"))
    details: dict[str, Any] = {"基金代码": code, "基金名称": local.get("基金名称"),
        "本地基金类型": local.get("基金类型"), "外部基金类型": upstream.get("type"),
        "校验来源": PROVIDER, "校验契约版本": CONTRACT, "独立供应商校验": False,
        "原始证据": upstream.get("_source_evidence"), "原始字段": upstream.get("_raw_fields"),
        "本地最新净值": _number(local.get("最新单位净值")), "外部最新净值": _number(upstream.get("netWorth")),
        "本地净值日期": _date(local.get("最新净值日期")), "外部净值日期": _date(upstream.get("netWorthDate")),
        "本地区间收益基准日": _date(local.get("区间收益基准日")),
        "外部区间收益基准日": _date(upstream.get("period_return_date"))}
    if upstream.get("provider") != PROVIDER or upstream.get("contract") != CONTRACT:
        hard.append("source_contract_invalid")
    evidence = upstream.get("_source_evidence") or {}
    if not all(isinstance(evidence.get(kind), Mapping) and evidence[kind].get("response_sha256")
               and evidence[kind].get("endpoint") for kind in ("valuation", "info")):
        hard.append("source_evidence_missing")
    if code is None or code != external_code:
        hard.append("code_mismatch")
    if not _text(upstream.get("name")):
        hard.append("external_name_missing")
    else:
        local_name, name = _text(local.get("基金名称")), _text(upstream.get("name"))
        if local_name and local_name != name and local_name not in name and name not in local_name:
            warning.append("name_mismatch")
    local_type, external_type = _text(local.get("基金类型")), _text(upstream.get("type"))
    type_match = bool(local_type and external_type and (local_type in external_type or external_type in local_type
        or (any(label in local_type for label in ("债券", "固收")) and any(label in external_type for label in ("债券", "固收")))))
    details["类型一致"] = type_match
    if not external_type:
        hard.append("external_type_missing_or_unknown")
    elif not type_match:
        warning.append("type_mismatch")
    local_date, external_date = details["本地净值日期"], details["外部净值日期"]
    date_mismatch = False
    if local_date is None or external_date is None:
        hard.append("latest_nav_date_missing")
    elif local_date != external_date:
        date_mismatch = True
    nav, external_nav = details["本地最新净值"], details["外部最新净值"]
    nav_match = False
    if nav is None or external_nav is None or nav <= 0 or external_nav <= 0:
        hard.append("latest_nav_missing")
    else:
        absolute = abs(nav - external_nav)
        relative = absolute / max(abs(external_nav), 1e-8) * 100
        details.update({"净值绝对差": round(absolute, 6), "净值相对差(%)": round(relative, 4)})
        nav_match = not (relative > policy["nav_relative_tolerance_pct"] and absolute > policy["nav_absolute_tolerance"])
        if not nav_match:
            hard.append("latest_nav_mismatch")
    if date_mismatch:
        gap = abs((date.fromisoformat(local_date) - date.fromisoformat(external_date)).days)
        details["净值日期差(天)"] = gap
        if nav_match and gap <= int(policy["nav_date_soft_lag_days"]):
            warning.append("latest_nav_date_soft_lag")
        else:
            hard.append("latest_nav_date_mismatch")
    if details["本地区间收益基准日"] is None:
        hard.append("period_return_date_missing")
    elif details["外部区间收益基准日"] is None or details["本地区间收益基准日"] != details["外部区间收益基准日"]:
        hard.append("period_return_date_mismatch")
    issues = []
    for field, local_field in LOCAL_RETURN_MAP.items():
        matched, comparison = _return_compare(local.get(local_field), upstream.get(field), policy)
        for suffix, value in comparison.items():
            details[f"{field}_{suffix}"] = value
        if not matched:
            issues.append((field, comparison["mode"]))
    if len(issues) == 1 and issues[0][0] == "return_1w" and issues[0][1] != "missing":
        warning.append("return_1w_soft_mismatch")
    else:
        hard.extend(f"{field}_{'missing' if mode == 'missing' else 'mismatch'}" for field, mode in issues)
    hard, warning = list(dict.fromkeys(hard)), list(dict.fromkeys(warning))
    details.update({"失败原因": "；".join(hard), "告警原因": "；".join(warning),
                    "校验完成": True, "校验通过": not hard})
    return {"passed": not hard, "hard_reasons": hard, "warning_reasons": warning, "details": details}


def validate_candidate_frame(settings: Any, df_candidates: pd.DataFrame, df_local: pd.DataFrame, config: Mapping[str, Any] | None = None) -> tuple[pd.DataFrame, set[str], dict[str, Any], pd.DataFrame]:
    """Validate every candidate using newly requested, provenance-bound rows."""
    empty_failed = pd.DataFrame(columns=["基金代码", "基金名称", "失败原因", "告警原因"])
    if df_candidates.empty:
        return df_candidates.copy(), set(), {"enabled": True, "reason": "empty_candidates", "checked_count": 0,
            "passed_count": 0, "filtered_count": 0, "_detail_records": [], "provider": PROVIDER,
            "source": PROVIDER, "contract": CONTRACT, "independent_source": False}, empty_failed
    if "fcode" not in df_candidates.columns or "基金代码" not in df_local.columns:
        raise RuntimeError("endpoint_validation_local_identity_missing")
    codes = list(dict.fromkeys(df_candidates["fcode"].tolist()))
    lookup, meta = fetch_cross_validation(settings, codes)
    if not meta["source_available"]:
        raise RuntimeError("endpoint_validation_source_unavailable: 两个净值端点无法返回有效记录，已阻断正式发布")
    policy = {**DEFAULT_CONFIG, **(config or {})}
    missing_count = int(meta.get("missing_count", 0))
    missing_ratio = missing_count / len(codes)
    missing_demoted = (missing_count > 0 and missing_count <= int(policy["residual_missing_max_count"])
                       and missing_ratio <= float(policy["residual_missing_max_ratio"])
                       and not meta.get("last_error"))
    meta.update(residual_missing_count=missing_count, residual_missing_ratio=round(missing_ratio, 8),
                residual_missing_max_count=int(policy["residual_missing_max_count"]),
                residual_missing_max_ratio=float(policy["residual_missing_max_ratio"]),
                residual_missing_demoted_to_hard_block=missing_demoted)
    if missing_count and not missing_demoted:
        raise RuntimeError(f"endpoint_validation_incomplete: missing_count={missing_count}, "
                           f"missing_ratio={missing_ratio}, last_error={meta.get('last_error')}")
    local_lookup = {row["基金代码"]: row for _, row in df_local.iterrows()}
    details = []
    failed: set[str] = set()
    reason_counts: Counter = Counter()
    warning_counts: Counter = Counter()
    for code in codes:
        row = local_lookup.get(code)
        if row is None or code not in lookup:
            reason = "local_lookup_missing" if row is None else "missing_in_endpoint_source"
            detail = {"基金代码": code, "基金名称": row.get("基金名称") if row is not None else None,
                      "失败原因": reason, "告警原因": "", "校验来源": PROVIDER, "校验契约版本": CONTRACT,
                      "独立供应商校验": False, "原始证据": None, "校验完成": True, "校验通过": False}
            hard, warnings = [reason], []
        else:
            comparison = compare_endpoint_candidate(row, lookup[code], config)
            detail, hard, warnings = comparison["details"], comparison["hard_reasons"], comparison["warning_reasons"]
        details.append(detail)
        reason_counts.update(hard)
        warning_counts.update(warnings)
        if hard:
            failed.add(code)
    summary = {"enabled": True, "reason": None, "checked_count": len(codes), "passed_count": len(codes) - len(failed),
        "filtered_count": len(failed), "sample_fcodes": sorted(failed)[:10],
        "reason_counts": [{"reason": reason, "count": count} for reason, count in reason_counts.most_common()],
        "warning_counts": [{"reason": reason, "count": count} for reason, count in warning_counts.most_common()],
        "provider": PROVIDER, "source": PROVIDER, "contract": CONTRACT, "independent_source": False,
        "batch_meta": meta, "source_evidence": meta["source_evidence"], "cache_hit": False,
        "cache_reused_count": 0, "_detail_records": details}
    failed_df = pd.DataFrame([detail for detail in details if detail["基金代码"] in failed]) if failed else empty_failed
    return df_candidates.loc[~df_candidates["fcode"].isin(failed)].copy(), failed, summary, failed_df


def validation_status_map(detail_records: list[dict[str, Any]] | pd.DataFrame | None, *, failed_codes: set[str] | None = None) -> dict[str, dict[str, Any]]:
    rows = detail_records.to_dict(orient="records") if isinstance(detail_records, pd.DataFrame) else detail_records or []
    failed = failed_codes or set()
    result = {}
    labels = {"return_1w_soft_mismatch": "近1周收益口径差异", "type_mismatch": "基金类型口径差异",
              "name_mismatch": "基金名称口径差异", "latest_nav_date_soft_lag": "双端点净值日期存在可容忍滞后"}
    for row in rows:
        code = _code(row.get("基金代码"))
        if code is None:
            continue
        evidence = row.get("原始证据") or {}
        has_evidence = (isinstance(evidence, Mapping)
                        and all(isinstance(evidence.get(kind), Mapping)
                                and evidence[kind].get("response_sha256") and evidence[kind].get("endpoint")
                                for kind in ("valuation", "info")))
        checked = (isinstance(row.get("失败原因"), str) and isinstance(row.get("告警原因"), str)
                   and row.get("校验来源") == PROVIDER and row.get("校验契约版本") == CONTRACT
                   and row.get("校验完成") is True and type(row.get("校验通过")) is bool
                   and (row.get("校验通过") is False or has_evidence))
        hard = str(row.get("失败原因") or "").split("；") if _text(row.get("失败原因")) else []
        warning = str(row.get("告警原因") or "").split("；") if _text(row.get("告警原因")) else []
        if checked and row["校验通过"] is False and not hard:
            hard.append("validation_failure_reason_missing")
        result[code] = {"validation_status": "hard_block" if hard or code in failed else "not_checked" if not checked else "soft_pass_with_warnings" if warning else "strict_match",
                       "validation_hard_reasons": hard, "validation_warning_reasons": warning,
                       "validation_warning_display": "；".join(labels.get(value, value) for value in warning),
                       "validation_source": row.get("校验来源"),
                       "validation_contract": row.get("校验契约版本"),
                       "independent_source": False, "source_evidence": row.get("原始证据")}
    for code in failed:
        if _code(code) and code not in result:
            result[code] = {"validation_status": "hard_block", "validation_hard_reasons": [],
                           "validation_warning_reasons": [], "validation_warning_display": "",
                           "validation_source": PROVIDER, "validation_contract": CONTRACT,
                           "independent_source": False, "source_evidence": None}
    return result
