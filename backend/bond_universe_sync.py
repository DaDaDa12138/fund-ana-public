from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from api.autostock_client import AutoStockClient  # noqa: E402
from api.client import APIClient  # noqa: E402
from api.endpoints import FundAPIEndpoints  # noqa: E402
from analyzers.risk_analyzer import RiskAnalyzer  # noqa: E402
from collectors.detail_collector import FundDetailCollector  # noqa: E402
from collectors.fund_collector import FundCollector  # noqa: E402
from collectors.nav_collector import NavCollector  # noqa: E402
from config.api_config import FundType, SortColumn  # noqa: E402
from database.connection import get_db_manager  # noqa: E402
from utils.fund_exclusion_rules import is_runtime_excluded_fund  # noqa: E402
from utils.project_runtime_config import load_runtime_config  # noqa: E402
from utils.tiantian_data_quality import inspect_tiantian_rank_payload  # noqa: E402


TIANTIAN_RANK_PAGE_SIZE_CANDIDATES = (400, 200, 100, 50, 30)
AUTOSTOCK_SAMPLE_LIMIT = 50
MISSING_SAMPLE_LIMIT = 30


@dataclass(slots=True)
class UniverseSyncContext:
    settings: Settings
    core_config: dict[str, Any]
    db_manager: Any
    tiantian_client: APIClient
    tiantian_endpoints: FundAPIEndpoints
    autostock_client: AutoStockClient
    fund_collector: FundCollector
    detail_collector: FundDetailCollector
    nav_collector: NavCollector
    risk_analyzer: RiskAnalyzer


def _load_core_config(project_root: Path) -> dict[str, Any]:
    return load_runtime_config(project_root / "config" / "project.yaml")


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _build_context(settings: Settings) -> UniverseSyncContext:
    core_config = _load_core_config(settings.project_root)
    core_config.setdefault("api", {})
    core_config["api"].setdefault("tiantian", {})
    core_config["api"]["tiantian"]["base_url"] = settings.tiantian_base_url
    core_config["api"].setdefault("autostock", {})
    core_config["api"]["autostock"]["base_url"] = settings.autostock_base_url

    db_manager = get_db_manager(db_path=str(settings.database_path))
    tiantian_client = APIClient(core_config)
    tiantian_endpoints = FundAPIEndpoints(tiantian_client)

    autostock_cfg = (core_config.get("api") or {}).get("autostock", {}) or {}
    retry_cfg = autostock_cfg.get("retry", {}) or {}
    # P0-04.2 / B15: 速率与共享状态统一读 settings.autostock_* 字段；
    # retry.timeout 保留读 core config（为了和 tiantian client 对齐）。
    autostock_client = AutoStockClient(
        enabled=getattr(settings, "autostock_enabled", True),
        base_url=settings.autostock_base_url,
        min_interval=float(settings.autostock_rate_limit_min_interval_seconds),
        timeout=int(retry_cfg.get("timeout", 60)),
        max_calls_per_hour=int(settings.autostock_rate_limit_max_calls_per_hour),
        shared_state_path=str(settings.autostock_rate_limit_shared_state_path),
        directory_cache_path=str(settings.autostock_directory_cache_path),
        directory_cache_ttl_seconds=settings.autostock_directory_cache_ttl_seconds,
    )

    return UniverseSyncContext(
        settings=settings,
        core_config=core_config,
        db_manager=db_manager,
        tiantian_client=tiantian_client,
        tiantian_endpoints=tiantian_endpoints,
        autostock_client=autostock_client,
        fund_collector=FundCollector(config=core_config, db_manager=db_manager, api_client=tiantian_client),
        detail_collector=FundDetailCollector(config=core_config, db_manager=db_manager, api_client=tiantian_client),
        nav_collector=NavCollector(config=core_config, db_manager=db_manager, api_client=tiantian_client),
        risk_analyzer=RiskAnalyzer(config=core_config, db_manager=db_manager),
    )


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _read_local_bond_funds(db_path: Path) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT
                f.fcode,
                COALESCE(f.shortname, ''),
                COALESCE(f.ftype, ''),
                COALESCE(f.type_code, ''),
                COALESCE(f.type_category, ''),
                r.latest_nav_date
            FROM funds f
            LEFT JOIN fund_returns r ON r.fcode = f.fcode
            WHERE f.type_code IN ('31', '32')
            """
        ).fetchall()
    finally:
        conn.close()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        result[code] = {
            "fcode": code,
            "shortname": row[1],
            "ftype": row[2],
            "type_code": row[3],
            "type_category": row[4],
            "latest_nav_date": row[5],
            "runtime_excluded": is_runtime_excluded_fund(code),
        }
    return result


def _fetch_tiantian_curated_codes(
    endpoints: FundAPIEndpoints,
    *,
    cltype: str,
    log,
    max_pages: int = 50,
    page_size: int = 30,
) -> tuple[set[str], dict[str, Any]]:
    """P1-09.2 / B14: 按 ``CLTYPE`` 拉取天天基金预置的基金筛选方案（例如 038=优质债券）。

    文档说明：``/fundMNRank`` 的 ``CLTYPE=038`` 对应"优质债券"筛选方案。我们取
    ``FundType=0`` 让上游自行按筛选方案给全量候选，分页最多 ``max_pages``（默认 50，
    按 30 per page 等效 1500 只上限）以免极端情况下无限翻页。
    """
    codes: set[str] = set()
    pages_fetched = 0
    last_total = 0
    errors: list[str] = []
    for page_index in range(1, max_pages + 1):
        try:
            payload = endpoints.get_bond_fund_rank(
                fund_type="0",
                sort_column=SortColumn.RETURN_1Y,
                page_index=page_index,
                page_size=page_size,
                only_buyable=False,
                CLTYPE=cltype,
            )
        except Exception as exc:
            errors.append(str(exc))
            break
        records, quality = inspect_tiantian_rank_payload(
            payload,
            endpoint_label=f"/fundMNRank CLTYPE={cltype} page={page_index} size={page_size}",
        )
        pages_fetched = page_index
        last_total = int((payload or {}).get("TotalCount") or last_total)
        if not quality.accepted:
            errors.append(quality.message or "rank_payload_not_accepted")
            break
        if not quality.has_data or not records:
            break
        added = 0
        for item in records:
            if not isinstance(item, dict):
                continue
            code = _normalize_code(item.get("FCODE") or item.get("fcode"))
            if code and code not in codes:
                codes.add(code)
                added += 1
        if added == 0:
            break
        if last_total and len(codes) >= last_total:
            break

    meta = {
        "cltype": str(cltype),
        "pages_fetched": int(pages_fetched),
        "page_size": int(page_size),
        "unique_code_count": int(len(codes)),
        "upstream_total_count": int(last_total),
        "errors": list(errors),
    }
    if log is not None:
        log(f"Tiantian CLTYPE={cltype} 候选池: codes={len(codes)} pages={pages_fetched} upstream_total={last_total}")
    return codes, meta


def _apply_external_curation_flags(
    database_path: Path,
    *,
    curation_codes: set[str],
    flag: str,
    log=None,
) -> dict[str, Any]:
    """把 ``curation_codes`` 对应基金的 ``funds.external_curation_flag`` 置为 ``flag``。

    与现有 flag 语义合并：若当前 flag 已包含 ``flag``（按 ``+`` 分隔），不重复写；
    若包含其它 flag，追加 ``+flag``；若为 NULL/空，直接置 ``flag``。
    同时把**不在** curation_codes 集合里、但现有 flag **仅等于** ``flag`` 的基金
    清空 flag（反映上游下架）。
    """
    flag_norm = str(flag or "").strip()
    if not flag_norm:
        return {"updated_total": 0, "cleared_total": 0}

    import sqlite3 as _sqlite3

    conn = _sqlite3.connect(str(database_path))
    conn.row_factory = _sqlite3.Row
    updated_total = 0
    cleared_total = 0
    try:
        rows = conn.execute(
            "SELECT fcode, external_curation_flag FROM funds"
        ).fetchall()
        now_iso = datetime.now().isoformat(timespec="seconds")

        batch_updates: list[tuple[str, str, str]] = []
        for row in rows:
            fcode = str(row["fcode"] or "").strip()
            if not fcode:
                continue
            current = str(row["external_curation_flag"] or "").strip()
            current_flags = [item for item in current.split("+") if item]
            in_curation = fcode in curation_codes

            if in_curation:
                if flag_norm in current_flags:
                    continue
                new_flags = current_flags + [flag_norm]
                new_value = "+".join(new_flags)
                batch_updates.append((new_value, now_iso, fcode))
                updated_total += 1
            else:
                if flag_norm not in current_flags:
                    continue
                remaining = [item for item in current_flags if item != flag_norm]
                new_value = "+".join(remaining) if remaining else None
                batch_updates.append((new_value, now_iso, fcode))
                cleared_total += 1

        if batch_updates:
            conn.executemany(
                "UPDATE funds SET external_curation_flag = ?, last_updated_at = ? WHERE fcode = ?",
                batch_updates,
            )
            conn.commit()
    finally:
        conn.close()

    if log is not None and (updated_total or cleared_total):
        log(
            f"external_curation_flag={flag_norm!r} 更新: 新增={updated_total} 清除={cleared_total}"
        )
    return {"updated_total": int(updated_total), "cleared_total": int(cleared_total)}


def _safe_rank_page_fetch(
    endpoints: FundAPIEndpoints,
    *,
    fund_type: str,
    log,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    last_error = None
    for page_size in TIANTIAN_RANK_PAGE_SIZE_CANDIDATES:
        try:
            payload = endpoints.get_bond_fund_rank(
                fund_type=fund_type,
                sort_column=SortColumn.RETURN_1Y,
                page_index=1,
                page_size=page_size,
                only_buyable=False,
            )
            records, quality = inspect_tiantian_rank_payload(
                payload,
                endpoint_label=f"/fundMNRank FundType={fund_type} page=1 size={page_size}",
            )
            if not quality.accepted:
                last_error = quality.message
                continue
            actual_record_count = len(records)
            effective_page_size = min(
                page_size,
                actual_record_count if actual_record_count > 0 else page_size,
            )
            if actual_record_count > 0 and effective_page_size != page_size:
                log(
                    "Tiantian fundMNRank page_size 被上游收敛: "
                    f"FundType={fund_type}, requested={page_size}, actual={actual_record_count}"
                )
            return records, {
                "requested_page_size": page_size,
                "effective_page_size": effective_page_size,
                "total_count": int((payload or {}).get("TotalCount") or 0),
                "record_count": actual_record_count,
            }
        except Exception as exc:
            last_error = str(exc)
            continue

    raise RuntimeError(f"天天基金 fundMNRank 首屏拉取失败: FundType={fund_type}, error={last_error}")


def _fetch_tiantian_type_codes(
    endpoints: FundAPIEndpoints,
    *,
    fund_type: str,
    log,
) -> tuple[set[str], dict[str, Any]]:
    first_page_records, first_meta = _safe_rank_page_fetch(endpoints, fund_type=fund_type, log=log)
    total_count = int(first_meta.get("total_count") or 0)
    page_size = int(first_meta.get("effective_page_size") or first_meta.get("requested_page_size") or 30)
    total_pages = max(1, (total_count + page_size - 1) // page_size) if total_count else 1
    seen_codes = {
        _normalize_code(item.get("FCODE") or item.get("fcode"))
        for item in first_page_records
        if isinstance(item, dict)
    }
    seen_codes.discard("")

    for page_index in range(2, total_pages + 1):
        payload = endpoints.get_bond_fund_rank(
            fund_type=fund_type,
            sort_column=SortColumn.RETURN_1Y,
            page_index=page_index,
            page_size=page_size,
            only_buyable=False,
        )
        records, quality = inspect_tiantian_rank_payload(
            payload,
            endpoint_label=f"/fundMNRank FundType={fund_type} page={page_index} size={page_size}",
        )
        if not quality.accepted:
            raise RuntimeError(f"天天基金分页拉取失败: FundType={fund_type} page={page_index} error={quality.message}")
        if not quality.has_data:
            break
        for item in records:
            if not isinstance(item, dict):
                continue
            code = _normalize_code(item.get("FCODE") or item.get("fcode"))
            if code:
                seen_codes.add(code)

    return seen_codes, {
        "fund_type": str(fund_type),
        "requested_page_size": int(first_meta.get("requested_page_size") or page_size),
        "effective_page_size": page_size,
        "first_page_record_count": int(first_meta.get("record_count") or 0),
        "declared_total_count": total_count,
        "fetched_unique_count": len(seen_codes),
        "fetched_pages": total_pages,
    }


def _fetch_autostock_bond_directory(
    client: AutoStockClient,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    rows = client.get_all_bond_funds_from_directory()
    result: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        code = _normalize_code(row.get("code"))
        if not code:
            continue
        result[code] = {
            "code": code,
            "name": str(row.get("name") or "").strip(),
            "type": str(row.get("type") or "").strip(),
            "pinyin": row.get("pinyin"),
            "runtime_excluded": is_runtime_excluded_fund(code),
        }
    meta = client.get_last_fund_all_meta()
    meta["fetched_unique_total"] = len(result)
    meta["fetched_active_total"] = sum(1 for row in result.values() if not bool(row.get("runtime_excluded")))
    return result, meta


def _merge_autostock_directory_parse_error_evidence(
    evidence: dict[str, Any],
    autostock_meta: dict[str, Any],
    *,
    parse_error_count: int,
) -> None:
    """Preserve AutoStock directory parse-error evidence in the sync artifact.

    The AutoStock client owns raw row parsing. Bond universe sync only copies
    its structured evidence into the weekly artifact so the truthfulness audit
    can prove that malformed upstream rows were observed, not synthesized into
    valid fund business values.
    """
    source_evidence = (
        autostock_meta.get("source_evidence")
        if isinstance(autostock_meta.get("source_evidence"), dict)
        else {}
    )

    def _first_mapping(*values: Any) -> dict[str, Any]:
        for value in values:
            if isinstance(value, dict) and value:
                return dict(value)
        return {}

    def _first_list(*values: Any) -> list[Any]:
        for value in values:
            if isinstance(value, list) and value:
                return list(value)
        return []

    parse_error_reason_counts = _first_mapping(
        evidence.get("parse_error_reason_counts"),
        source_evidence.get("parse_error_reason_counts"),
        autostock_meta.get("parse_error_reason_counts"),
    )
    parse_error_codes = _first_list(
        evidence.get("parse_error_codes"),
        source_evidence.get("parse_error_codes"),
        autostock_meta.get("parse_error_codes"),
    )
    parse_error_samples = _first_list(
        evidence.get("parse_error_samples"),
        source_evidence.get("parse_error_samples"),
        autostock_meta.get("parse_error_samples"),
    )
    if not parse_error_codes and parse_error_samples:
        seen_parse_error_codes: set[str] = set()
        for sample in parse_error_samples:
            if not isinstance(sample, dict):
                continue
            code = str(sample.get("code") or "").strip()
            if not code or code in seen_parse_error_codes:
                continue
            seen_parse_error_codes.add(code)
            parse_error_codes.append(code)

    if parse_error_reason_counts:
        evidence["parse_error_reason_counts"] = parse_error_reason_counts
    if parse_error_codes:
        evidence["parse_error_codes"] = parse_error_codes
        evidence["parse_error_code_count"] = len(parse_error_codes)
        evidence["parse_error_code_limit"] = int(
            source_evidence.get("parse_error_code_limit")
            or autostock_meta.get("parse_error_code_limit")
            or len(parse_error_codes)
        )
        evidence["parse_error_code_truncated"] = bool(
            source_evidence.get("parse_error_code_truncated")
            or autostock_meta.get("parse_error_code_truncated")
        )
    if parse_error_samples:
        evidence["parse_error_samples"] = parse_error_samples
        evidence["parse_error_sample_count"] = len(parse_error_samples)
        evidence["parse_error_sample_limit"] = int(
            source_evidence.get("parse_error_sample_limit")
            or autostock_meta.get("parse_error_sample_limit")
            or len(parse_error_samples)
        )
        evidence["parse_error_sample_truncated"] = bool(
            source_evidence.get("parse_error_sample_truncated")
            or autostock_meta.get("parse_error_sample_truncated")
        )

    if parse_error_count <= 0:
        return

    truth_policy = (
        evidence.get("parse_error_truth_policy")
        or source_evidence.get("parse_error_truth_policy")
        or autostock_meta.get("parse_error_truth_policy")
        or "preserve_missing_semantics"
    )
    evidence["parse_error_truth_policy"] = str(truth_policy)

    handling = (
        evidence.get("parse_error_business_value_handling")
        or source_evidence.get("parse_error_business_value_handling")
        or autostock_meta.get("parse_error_business_value_handling")
        or (
            "Rows missing required AutoStock directory fields are not used to synthesize "
            "fund type, bond membership, or valid directory records."
        )
    )
    evidence["parse_error_business_value_handling"] = str(handling)


def _build_missing_records_from_search(
    ctx: UniverseSyncContext,
    missing_codes: list[str],
    autostock_rows: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    existing_funds = []
    for code in missing_codes:
        row = autostock_rows.get(code) or {}
        existing_funds.append(
            {
                "fcode": code,
                "shortname": row.get("name"),
                "ftype": row.get("type"),
                "type_code": "32" if str(row.get("type") or "").startswith("定开债") else "31",
                "type_category": "fixed_income",
                "latest_nav_date": None,
            }
        )
    return ctx.fund_collector.build_search_fallback_records(existing_funds)


def _save_missing_records(
    ctx: UniverseSyncContext,
    fallback_records: list[dict[str, Any]],
    missing_codes: list[str],
    autostock_rows: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    records_to_save = list(fallback_records or [])
    recovered_by_search = {
        _normalize_code(item.get("fcode"))
        for item in records_to_save
        if isinstance(item, dict) and item.get("fcode")
    }
    skipped_source_missing_count = 0

    for code in missing_codes:
        if code in recovered_by_search:
            continue
        row = autostock_rows.get(code) or {}
        if not row:
            skipped_source_missing_count += 1
            continue
        records_to_save.append(
            {
                "fcode": code,
                "shortname": row.get("name") or code,
                "ftype": row.get("type"),
                "sub_type": None,
                "type_code": "32" if str(row.get("type") or "").startswith("定开债") else "31",
                "type_category": "fixed_income",
                "data_source": "autostock_directory_fallback",
                "first_collected_at": datetime.now(),
                "last_updated_at": datetime.now(),
            }
        )

    write_attempt_count = len(records_to_save)
    skip_reason_counts = {}
    if skipped_source_missing_count:
        skip_reason_counts["missing_search_and_autostock_directory_record"] = skipped_source_missing_count

    if not records_to_save:
        return {
            "saved_count": 0,
            "fund_inserts": 0,
            "fund_updates": 0,
            "returns_inserts": 0,
            "returns_updates": 0,
            "failed_records": 0,
            "recovered_by_search": 0,
            "direct_autostock_stub_count": 0,
            "write_attempt_count": 0,
            "skipped_records": skipped_source_missing_count,
            "skip_reason_counts": skip_reason_counts,
        }

    save_stats = ctx.fund_collector._save_funds(records_to_save, return_stats=True)
    save_stats["recovered_by_search"] = len(recovered_by_search)
    save_stats["direct_autostock_stub_count"] = max(0, len(records_to_save) - len(recovered_by_search))
    save_stats["write_attempt_count"] = write_attempt_count
    save_stats["skipped_records"] = skipped_source_missing_count
    save_stats["skip_reason_counts"] = skip_reason_counts
    return save_stats


def _collect_missing_enrichment(ctx: UniverseSyncContext, codes: list[str], log) -> dict[str, Any]:
    detail_success = 0
    nav_success = 0
    nav_records = 0
    risk_metrics = 0
    detail_failed: list[str] = []
    nav_failed: list[str] = []
    risk_failed: list[str] = []

    for code in codes:
        try:
            if ctx.detail_collector.collect_fund_detail(code, force_update=True):
                detail_success += 1
        except Exception:
            detail_failed.append(code)

        try:
            records = int(ctx.nav_collector.collect_fund_history(code, force_update=False) or 0)
            nav_records += records
            nav_success += 1
        except Exception:
            nav_failed.append(code)

        try:
            risk_metrics += int(ctx.risk_analyzer.calculate_fund_risk(code, force_update=True) or 0)
        except Exception:
            risk_failed.append(code)

    return {
        "detail_success_count": detail_success,
        "detail_failed_count": len(detail_failed),
        "detail_failed_codes_sample": detail_failed[:AUTOSTOCK_SAMPLE_LIMIT],
        "nav_success_count": nav_success,
        "nav_failed_count": len(nav_failed),
        "nav_failed_codes_sample": nav_failed[:AUTOSTOCK_SAMPLE_LIMIT],
        "nav_records_inserted": nav_records,
        "risk_metric_periods_generated": risk_metrics,
        "risk_failed_count": len(risk_failed),
        "risk_failed_codes_sample": risk_failed[:AUTOSTOCK_SAMPLE_LIMIT],
    }


def run_bond_universe_sync(
    settings: Settings,
    *,
    log=lambda _message: None,
) -> dict[str, Any]:
    ctx = _build_context(settings)

    generated_at = datetime.now().isoformat(timespec="seconds")
    autostock_enabled = getattr(settings, "autostock_enabled", True)
    log("开始执行债券基金全集对账与自动补齐" + ("（AutoStock停用，独立目录覆盖率未知）" if not autostock_enabled else ""))

    local_bonds = _read_local_bond_funds(settings.database_path)
    tiantian_31_codes, tiantian_31_meta = _fetch_tiantian_type_codes(
        ctx.tiantian_endpoints,
        fund_type=FundType.BOND,
        log=log,
    )
    tiantian_32_codes, tiantian_32_meta = _fetch_tiantian_type_codes(
        ctx.tiantian_endpoints,
        fund_type=FundType.BOND_FIXED,
        log=log,
    )
    tiantian_codes = tiantian_31_codes | tiantian_32_codes
    tiantian_active_codes = {
        code for code in tiantian_codes
        if not is_runtime_excluded_fund(code)
    }

    # P1-09.2 / B14: 拉天天基金"优质债券"筛选方案（CLTYPE=038），落 external_curation_flag。
    # 纯展示维度，不参与评分；失败不阻塞主 sync。
    curated_038_codes: set[str] = set()
    curated_038_meta: dict[str, Any] = {"error": None, "applied": None}
    try:
        curated_038_codes, curated_038_raw_meta = _fetch_tiantian_curated_codes(
            ctx.tiantian_endpoints,
            cltype="038",
            log=log,
        )
        curated_038_meta.update(curated_038_raw_meta)
        curated_038_meta["applied"] = _apply_external_curation_flags(
            settings.database_path,
            curation_codes=curated_038_codes,
            flag="tiantian_038",
            log=log,
        )
    except Exception as exc:
        curated_038_meta["error"] = str(exc)
        if log is not None:
            log(f"Tiantian CLTYPE=038 候选池拉取失败: {exc}")

    if autostock_enabled:
        autostock_rows, autostock_meta = _fetch_autostock_bond_directory(ctx.autostock_client)
    else:
        autostock_rows = {}
        autostock_meta = {"status": "disabled", "reason": "deployment_transport_unavailable",
                          "source_evidence": {"source": "autostock", "source_type": "unavailable",
                                              "source_complete": False, "request_count": 0}}
    autostock_codes = set(autostock_rows)
    autostock_active_codes = {
        code for code, row in autostock_rows.items()
        if not bool((row or {}).get("runtime_excluded"))
    }
    local_codes = set(local_bonds)
    local_active_codes = {
        code for code, row in local_bonds.items()
        if not bool((row or {}).get("runtime_excluded"))
    }

    missing_in_local_vs_autostock = sorted(autostock_codes - local_codes)
    missing_in_local_vs_tiantian = sorted(tiantian_codes - local_codes)
    missing_in_local_active_vs_autostock = sorted(autostock_active_codes - local_active_codes)
    missing_in_local_active_vs_tiantian = sorted(tiantian_active_codes - local_active_codes)
    missing_in_autostock = sorted(local_codes - autostock_codes)
    missing_in_tiantian = sorted(local_codes - tiantian_codes)
    union_external_codes = sorted((autostock_codes | tiantian_codes) - local_codes)

    log(
        "债基全集对账: local=%s, tiantian_31_32=%s, autostock_directory=%s, external_missing=%s"
        % (len(local_codes), len(tiantian_codes), len(autostock_codes), len(union_external_codes))
    )

    fallback_records, fallback_stats = _build_missing_records_from_search(
        ctx,
        union_external_codes,
        autostock_rows,
    )
    save_stats = _save_missing_records(
        ctx,
        fallback_records,
        union_external_codes,
        autostock_rows,
    )

    post_local_bonds = _read_local_bond_funds(settings.database_path)
    post_local_codes = set(post_local_bonds)
    newly_present_codes = sorted(post_local_codes - local_codes)
    enrichment_stats = _collect_missing_enrichment(ctx, newly_present_codes, log) if newly_present_codes else {
        "detail_success_count": 0,
        "detail_failed_count": 0,
        "detail_failed_codes_sample": [],
        "nav_success_count": 0,
        "nav_failed_count": 0,
        "nav_failed_codes_sample": [],
        "nav_records_inserted": 0,
        "risk_metric_periods_generated": 0,
        "risk_failed_count": 0,
        "risk_failed_codes_sample": [],
    }

    final_local_bonds = _read_local_bond_funds(settings.database_path)
    final_local_codes = set(final_local_bonds)
    final_local_active_codes = {
        code for code, row in final_local_bonds.items()
        if not bool((row or {}).get("runtime_excluded"))
    }
    final_missing_vs_autostock = sorted(autostock_codes - final_local_codes)
    final_missing_vs_tiantian = sorted(tiantian_codes - final_local_codes)
    final_missing_active_vs_autostock = sorted(autostock_active_codes - final_local_active_codes)
    final_missing_active_vs_tiantian = sorted(tiantian_active_codes - final_local_active_codes)

    runtime_excluded_before = sorted(local_codes - local_active_codes)
    runtime_excluded_after = sorted(final_local_codes - final_local_active_codes)
    autostock_directory_source_evidence = dict(autostock_meta.get("source_evidence") or {})
    if autostock_directory_source_evidence:
        parse_error_count = int(
            autostock_directory_source_evidence.get("parse_errors")
            or autostock_meta.get("parse_errors")
            or 0
        )
        database_failed_record_count = int(save_stats.get("failed_records") or 0)
        failure_reason_counts = {}
        if parse_error_count:
            failure_reason_counts["directory_row_parse_error"] = parse_error_count
        if database_failed_record_count:
            failure_reason_counts["database_save_failed"] = database_failed_record_count
        skip_reason_counts = (
            dict(save_stats.get("skip_reason_counts") or {})
            if isinstance(save_stats.get("skip_reason_counts"), dict)
            else {}
        )
        _merge_autostock_directory_parse_error_evidence(
            autostock_directory_source_evidence,
            autostock_meta,
            parse_error_count=parse_error_count,
        )
        autostock_directory_source_evidence.update(
            {
                "task": "bond_universe_sync.autostock_directory",
                "database_path": str(settings.database_path),
                "business_date": None,
                "business_date_semantics": "not_provided_by_autostock_directory",
                "parse_error_count": parse_error_count,
                "matched_local_before_count": len(local_codes & autostock_codes),
                "matched_local_after_count": len(final_local_codes & autostock_codes),
                "missing_in_local_before_count": len(missing_in_local_vs_autostock),
                "missing_in_local_after_count": len(final_missing_vs_autostock),
                "fallback_search_record_count": len(fallback_records or []),
                "database_write_attempt_count": int(save_stats.get("write_attempt_count") or 0),
                "database_saved_count": int(save_stats.get("saved_count") or 0),
                "database_fund_insert_count": int(save_stats.get("fund_inserts") or 0),
                "database_fund_update_count": int(save_stats.get("fund_updates") or 0),
                "database_failed_record_count": database_failed_record_count,
                "database_skipped_count": int(save_stats.get("skipped_records") or 0),
                "skip_reason_counts": skip_reason_counts,
                "failure_reason_counts": failure_reason_counts,
                "direct_autostock_stub_count": int(save_stats.get("direct_autostock_stub_count") or 0),
                "newly_synced_codes_total": len(newly_present_codes),
            }
        )
        autostock_directory_source_evidence["source_complete"] = bool(
            autostock_directory_source_evidence.get("source_complete", True)
            and parse_error_count == 0
            and database_failed_record_count == 0
        )

    summary = {
        "generated_at": generated_at,
        "database_path": str(settings.database_path),
        "counts_before_sync": {
            "local_bond_total": len(local_codes),
            "local_bond_stored_total": len(local_codes),
            "local_bond_active_total": len(local_active_codes),
            "runtime_excluded_total": len(runtime_excluded_before),
            "tiantian_31_total": len(tiantian_31_codes),
            "tiantian_32_total": len(tiantian_32_codes),
            "tiantian_31_32_total": len(tiantian_codes),
            "tiantian_active_total": len(tiantian_active_codes),
            "autostock_directory_bond_total": len(autostock_codes),
            "autostock_directory_active_total": len(autostock_active_codes),
        },
        "counts_after_sync": {
            "local_bond_total": len(final_local_codes),
            "local_bond_stored_total": len(final_local_codes),
            "local_bond_active_total": len(final_local_active_codes),
            "runtime_excluded_total": len(runtime_excluded_after),
            "missing_vs_tiantian": len(final_missing_vs_tiantian),
            "missing_vs_autostock": len(final_missing_vs_autostock),
            "missing_active_vs_tiantian": len(final_missing_active_vs_tiantian),
            "missing_active_vs_autostock": len(final_missing_active_vs_autostock),
        },
        "coverage_before_sync": {
            "stored_vs_tiantian_pct": round((len(local_codes & tiantian_codes) / len(tiantian_codes) * 100), 4) if tiantian_codes else None,
            "stored_vs_autostock_pct": round((len(local_codes & autostock_codes) / len(autostock_codes) * 100), 4) if autostock_codes else None,
            "active_vs_tiantian_pct": round((len(local_active_codes & tiantian_active_codes) / len(tiantian_active_codes) * 100), 4) if tiantian_active_codes else None,
            "active_vs_autostock_pct": round((len(local_active_codes & autostock_active_codes) / len(autostock_active_codes) * 100), 4) if autostock_active_codes else None,
            "vs_tiantian_pct": round((len(local_codes & tiantian_codes) / len(tiantian_codes) * 100), 4) if tiantian_codes else None,
            "vs_autostock_pct": round((len(local_codes & autostock_codes) / len(autostock_codes) * 100), 4) if autostock_codes else None,
        },
        "coverage_after_sync": {
            "stored_vs_tiantian_pct": round((len(final_local_codes & tiantian_codes) / len(tiantian_codes) * 100), 4) if tiantian_codes else None,
            "stored_vs_autostock_pct": round((len(final_local_codes & autostock_codes) / len(autostock_codes) * 100), 4) if autostock_codes else None,
            "active_vs_tiantian_pct": round((len(final_local_active_codes & tiantian_active_codes) / len(tiantian_active_codes) * 100), 4) if tiantian_active_codes else None,
            "active_vs_autostock_pct": round((len(final_local_active_codes & autostock_active_codes) / len(autostock_active_codes) * 100), 4) if autostock_active_codes else None,
            "vs_tiantian_pct": round((len(final_local_codes & tiantian_codes) / len(tiantian_codes) * 100), 4) if tiantian_codes else None,
            "vs_autostock_pct": round((len(final_local_codes & autostock_codes) / len(autostock_codes) * 100), 4) if autostock_codes else None,
        },
        "tiantian_rank_meta": {
            "type_31": tiantian_31_meta,
            "type_32": tiantian_32_meta,
        },
        "autostock_directory_meta": autostock_meta,
        "autostock_directory_source_evidence": autostock_directory_source_evidence,
        "fallback_search_stats": fallback_stats,
        "save_stats": save_stats,
        "enrichment_stats": enrichment_stats,
        "diffs_before_sync": {
            "missing_in_local_vs_autostock_total": len(missing_in_local_vs_autostock),
            "missing_in_local_vs_tiantian_total": len(missing_in_local_vs_tiantian),
            "missing_in_local_active_vs_autostock_total": len(missing_in_local_active_vs_autostock),
            "missing_in_local_active_vs_tiantian_total": len(missing_in_local_active_vs_tiantian),
            "missing_in_autostock_total": len(missing_in_autostock),
            "missing_in_tiantian_total": len(missing_in_tiantian),
            "runtime_excluded_total": len(runtime_excluded_before),
            "union_external_missing_total": len(union_external_codes),
            "missing_in_local_vs_autostock_samples": [autostock_rows[code] for code in missing_in_local_vs_autostock[:MISSING_SAMPLE_LIMIT]],
            "missing_in_local_vs_tiantian_samples": missing_in_local_vs_tiantian[:MISSING_SAMPLE_LIMIT],
            "missing_in_local_active_vs_autostock_samples": [autostock_rows[code] for code in missing_in_local_active_vs_autostock[:MISSING_SAMPLE_LIMIT]],
            "missing_in_local_active_vs_tiantian_samples": missing_in_local_active_vs_tiantian[:MISSING_SAMPLE_LIMIT],
            "missing_in_autostock_samples": [local_bonds[code] for code in missing_in_autostock[:MISSING_SAMPLE_LIMIT]],
            "missing_in_tiantian_samples": [local_bonds[code] for code in missing_in_tiantian[:MISSING_SAMPLE_LIMIT]],
            "runtime_excluded_samples": [local_bonds[code] for code in runtime_excluded_before[:MISSING_SAMPLE_LIMIT]],
        },
        "diffs_after_sync": {
            "missing_in_local_vs_autostock_total": len(final_missing_vs_autostock),
            "missing_in_local_vs_tiantian_total": len(final_missing_vs_tiantian),
            "missing_in_local_active_vs_autostock_total": len(final_missing_active_vs_autostock),
            "missing_in_local_active_vs_tiantian_total": len(final_missing_active_vs_tiantian),
            "missing_in_local_vs_autostock_samples": [autostock_rows[code] for code in final_missing_vs_autostock[:MISSING_SAMPLE_LIMIT]],
            "missing_in_local_vs_tiantian_samples": final_missing_vs_tiantian[:MISSING_SAMPLE_LIMIT],
            "missing_in_local_active_vs_autostock_samples": [autostock_rows[code] for code in final_missing_active_vs_autostock[:MISSING_SAMPLE_LIMIT]],
            "missing_in_local_active_vs_tiantian_samples": final_missing_active_vs_tiantian[:MISSING_SAMPLE_LIMIT],
            "runtime_excluded_total": len(runtime_excluded_after),
            "runtime_excluded_samples": [final_local_bonds[code] for code in runtime_excluded_after[:MISSING_SAMPLE_LIMIT]],
            "newly_synced_codes_total": len(newly_present_codes),
            "newly_synced_codes_sample": newly_present_codes[:MISSING_SAMPLE_LIMIT],
        },
        "external_curation": {
            # P1-09.2 / B14: 天天基金优质债券筛选方案（CLTYPE=038）落地情况
            "tiantian_038": curated_038_meta,
        },
    }

    summary["collection_source"] = "tiantian_fundMNRank"
    summary["independent_directory_available"] = autostock_enabled
    if not autostock_enabled:
        # An unrequested directory is unknown, not an empty observed universe.
        for group in ("counts_before_sync", "counts_after_sync", "diffs_before_sync", "diffs_after_sync"):
            for key in summary.get(group, {}):
                if "autostock" in key:
                    summary[group][key] = None
        for key in ("missing_in_local_before_count", "missing_in_local_after_count",
                    "matched_local_before_count", "matched_local_after_count"):
            summary["autostock_directory_source_evidence"][key] = None

    artifact_dir = settings.reports_dir / "audit" / "bond_universe_sync"
    artifact_file = artifact_dir / f"bond_universe_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    _write_json(artifact_file, summary)
    summary["artifact_path"] = str(artifact_file)
    log(f"债券基金全集对账完成，审计产物: {artifact_file}")
    return summary
