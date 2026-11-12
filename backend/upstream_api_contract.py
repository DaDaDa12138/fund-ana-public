from __future__ import annotations

from copy import deepcopy
from typing import Any
from urllib.parse import urlparse

try:
    from api.autostock_endpoints import AutoStockEndpoint
    from config.api_config import APIEndpoints, FundType, SortColumn
except ModuleNotFoundError:  # direct backend imports may not have core/ on sys.path yet.
    from core.api.autostock_endpoints import AutoStockEndpoint
    from core.config.api_config import APIEndpoints, FundType, SortColumn


UPSTREAM_API_SOURCE_CONTRACT_SCHEMA_VERSION = 2


def _clean_base_url(value: Any) -> str | None:
    text = str(value or "").strip().rstrip("/")
    return text or None


def _relative_path(full_url: str, *, base_url: str | None) -> str:
    full = str(full_url or "").strip()
    base = str(base_url or "").strip().rstrip("/")
    if base and full.startswith(f"{base}/"):
        return full[len(base) :]
    try:
        parsed_full = urlparse(full)
        parsed_base = urlparse(base)
    except Exception:
        return full
    full_path = parsed_full.path or full
    base_path = parsed_base.path.rstrip("/")
    if base_path and full_path.startswith(f"{base_path}/"):
        return full_path[len(base_path) :]
    return full_path or full


def _endpoint_index(endpoints: list[dict[str, Any]]) -> list[str]:
    return [str(endpoint.get("path") or "").strip() for endpoint in endpoints if endpoint.get("path")]


def _tiantian_endpoints() -> list[dict[str, Any]]:
    return [
        {
            "path": APIEndpoints.FUND_RANK,
            "label": "fundMNRank",
            "authoritative_for": ["fund_universe", "fund_directory_snapshot", "rank_return_snapshot"],
            "used_by": ["collect-funds", "tiantian-business-preflight", "api-settings-probe"],
            "required_params": {
                "FundType": [FundType.BOND, FundType.BOND_FIXED],
                "SortColumn": SortColumn.RETURN_1Y,
                "Sort": "desc",
                "pageIndex": "1-based integer",
                "pageSize": "integer 1..30; documented maximum 30",
                "ISABNORMAL": "true",
            },
            "pagination": {
                "required": True,
                "page_index_param": "pageIndex",
                "page_size_param": "pageSize",
                "declared_total_field": "TotalCount",
                "data_field": "Datas",
                "stop_condition": "read until accepted pages cover declared TotalCount or last page is reached",
            },
            "source_evidence_required": True,
            "evidence_contract": {
                "schema": "fundMNRank api_source_evidence v2",
                "page_observation_schema_version": 2,
                "page_member_fields": ["FCODE", "SHORTNAME", SortColumn.RETURN_1Y],
                "page_member_semantics": "All observed positions, original field types and missing fields; not a complete raw metric payload",
                "response_binding": "HTTP entity SHA and request IDs bound to the unchanged parsed payload; legacy transport evidence stays unknown",
                "required_fields": [
                    "total_page_request_count",
                    "accepted_page_count",
                    "failed_page_count",
                    "declared_total_count",
                    "raw_record_count",
                    "unique_record_count",
                    "missing_code_count",
                    "invalid_record_count",
                ],
            },
            "quality_checks": [
                "HTTP success is not sufficient; payload must expose usable Datas records",
                "missing fund code/name is invalid evidence, not a defaultable business value",
                "pagination truncation, duplicate overflow, and declared-count mismatch are audit findings",
            ],
        },
        {
            "path": APIEndpoints.FUND_SEARCH,
            "label": "fundSearch",
            "authoritative_for": ["fund_directory_fallback", "missing_fund_metadata_backfill"],
            "used_by": ["collect-funds fallback"],
            "required_params": {
                "m": "1 for fund search; 3 company initial; 7 manager; 8 company",
                "key": "fund code or name keyword",
            },
            "pagination": {
                "required": False,
                "data_field": "Datas",
            },
            "source_evidence_required": True,
            "quality_checks": [
                "only fills upstream-covered missing directory fields",
                "does not synthesize fund membership or publication eligibility",
            ],
        },
        {
            "path": APIEndpoints.FUND_NAV_HISTORY,
            "label": "fundMNHisNetList",
            "authoritative_for": ["historical_nav", "return_calculation_inputs", "risk_metric_inputs"],
            "used_by": ["collect-nav", "long-term-history-maintenance", "truncated-history-tail-audit"],
            "required_params": {
                "FCODE": "6-digit fund code",
                "pageIndex": "1-based integer",
                "pagesize": "positive integer; lowercase spelling is required",
            },
            "forbidden_param_aliases": ["pageSize"],
            "pagination": {
                "required": True,
                "page_index_param": "pageIndex",
                "page_size_param": "pagesize",
                "declared_total_field": "TotalCount",
                "data_field": "Datas",
            },
            "source_evidence_required": True,
            "quality_checks": [
                "NAV rows must keep upstream dates and values; missing rows remain missing",
                "zero-increment days cannot be treated as successful freshness without coverage evidence",
                "risk metrics must derive from real NAV history or remain unavailable",
            ],
        },
        {
            "path": APIEndpoints.FUND_DETAIL,
            "label": "fundMNDetailInformation",
            "authoritative_for": ["fund_detail", "holding_period_inputs", "manager_ids"],
            "used_by": ["collect-details", "holding-period-gate"],
            "required_params": {"FCODE": "6-digit fund code"},
            "pagination": {"required": False, "data_field": "Datas"},
            "source_evidence_required": True,
            "quality_checks": [
                "Datas=None is preserved as upstream missing, not converted to fake detail data",
                "holding_days > 90 excludes publication when structured evidence is available",
            ],
        },
        {
            "path": APIEndpoints.FUND_MANAGER_LIST,
            "label": "fundMNMangerList",
            "authoritative_for": ["fund_manager_membership"],
            "used_by": ["collect-managers"],
            "required_params": {"FCODE": "6-digit fund code"},
            "pagination": {"required": False, "data_field": "Datas"},
            "source_evidence_required": True,
        },
        {
            "path": APIEndpoints.MANAGER_INFO,
            "label": "fundMSNMangerInfo",
            "authoritative_for": ["manager_profile", "manager_career_experience_inputs"],
            "used_by": ["collect-managers"],
            "required_params": {"FCODE": "manager identifier from upstream manager list (not a fund code)"},
            "field_semantics": {
                "TOTALDAYS": "Career days; not continuous tenure on the current fund",
                "MGOLD": "Upstream composite score; averages disclose scored / bound manager counts",
                "as_of": "Per-field observation date is not established; profile update time is acquisition metadata",
            },
            "pagination": {"required": False, "data_field": "Datas"},
            "source_evidence_required": True,
        },
        {
            "path": APIEndpoints.FUND_PERIOD_INCREASE,
            "label": "fundMNPeriodIncrease",
            "authoritative_for": ["period_return_snapshot", "annual_return_inputs"],
            "used_by": ["collect-period-increase", "annual-return-gate-shadow"],
            "required_params": {"FCODE": "6-digit fund code"},
            "optional_params": {"RANGE": "omit for period returns; n for calendar-year returns"},
            "pagination": {"required": False, "data_field": "Datas"},
            "source_evidence_required": True,
        },
        {
            "path": APIEndpoints.FUND_RANK_DIAGRAM,
            "label": "fundRankDiagram",
            "authoritative_for": ["rank_history_snapshot"],
            "used_by": ["rank-consistency-observation"],
            "required_params": {"FCODE": "6-digit fund code"},
            "pagination": {"required": False, "data_field": "Datas"},
            "source_evidence_required": True,
        },
        {
            "path": APIEndpoints.FUND_VALUATION,
            "label": "fundVarietieValuationDetail",
            "authoritative_for": [],
            "used_by": [],
            "availability": "endpoint wrapper only; current UI uses configured FundValuationLast providers",
            "required_params": {"FCODE": "6-digit fund code"},
            "pagination": {"required": False, "data_field": "Datas"},
            "source_evidence_required": True,
        },
        {
            "path": APIEndpoints.FUND_STOP_WATCH,
            "label": "fundMNStopWatch",
            "used_by": ["fund detail metadata fallback"],
            "required_params": {"FCODE": "6-digit fund code"},
            "quality_checks": ["does not provide fee components or sufficient holding-period evidence"],
        },
        {
            "path": APIEndpoints.FUND_GRADE,
            "label": "fundGradeDetail",
            "used_by": ["fund grade supplement"],
            "required_params": {"FCODE": "6-digit fund code"},
            "optional_params": {"pageIndex": "1-based integer", "pageSize": "positive integer"},
            "pagination": {"required": True, "data_field": "Datas"},
        },
        {
            "path": APIEndpoints.FUND_NAV_DIAGRAM,
            "label": "fundVPageDiagram",
            "used_by": [],
            "availability": "endpoint wrapper only; not used by production history collectors",
            "required_params": {"FCODE": "6-digit fund code"},
            "optional_params": {"RANGE": "1n/3n/5n/ln", "POINTCOUNT": "requested chart points"},
            "quality_checks": ["chart points may be sampled; validate daily coverage before risk calculation"],
        },
        {
            "path": APIEndpoints.FUND_SEARCH_BY_NAME,
            "label": "fundSearchInfoByName",
            "used_by": ["fund-code-alias-audit"],
            "required_params": {"key": "fund name keyword"},
            "optional_params": {"orderType": "2 default; 1 popularity", "pageindex": "1-based integer", "pagesize": "positive integer"},
            "pagination": {"required": True, "data_field": "data", "declared_total_field": "totalCount"},
        },
        {
            "path": APIEndpoints.COMPANY_API,
            "label": "companyApi2",
            "used_by": ["collect-companies", "weekly_maintenance"],
            "required_params": {"cc": "valid company identifier from fund_managers", "action": "companyarchives"},
            "pagination": {"required": False, "data_field": "data"},
            "source_evidence_required": True,
            "quality_checks": [
                "company API requires code=1, a data object, and a nonempty FDMC",
                "GLGM needs an explicit currency unit; missing values remain NULL",
                "last_updated_at is retrieval time; Datas2 does not date the current GLGM",
                "refresh missing/stale profiles with a per-run cap and failure cooldown",
            ],
        },
    ]


def _autostock_endpoints(*, base_url: str | None) -> list[dict[str, Any]]:
    # Paths are relative to the provider's canonical base even with a custom proxy prefix.
    base_url = AutoStockEndpoint.BASE_URL
    return [
        {
            "path": _relative_path(AutoStockEndpoint.FUND_ALL, base_url=base_url),
            "label": "AutoStock fund/all",
            "authoritative_for": ["autostock_fund_directory_crosscheck"],
            "used_by": ["autostock-directory-evidence"],
            "required_params": {},
            "optional_params": {"keyWord": "optional keyword; local fallback if upstream ignores it"},
            "pagination": {"required": False},
            "source_evidence_required": True,
        },
        {
            "path": _relative_path(AutoStockEndpoint.FUND, base_url=base_url),
            "label": "AutoStock fund",
            "authoritative_for": ["autostock_fund_snapshot"],
            "used_by": ["autostock-nav-tail-fill", "final screening independent cross validation", "legacy favorites basic snapshot"],
            "required_params": {"code": "single code or comma-separated fund codes"},
            "method": "GET",
            "batch_limit": {"safe_max": 50, "basis": "2026-09-07 live probe: 75 requested, only 50 returned"},
            "quality_checks": ["validate requestFail/resolveFail for each row; reconcile requested membership"],
            "pagination": {"required": False},
            "source_evidence_required": True,
        },
        {
            "path": _relative_path(AutoStockEndpoint.FUND_DETAIL, base_url=base_url),
            "label": "AutoStock fund/detail",
            "authoritative_for": ["autostock_detail_crosscheck"],
            "used_by": ["documented endpoint; legacy single-code method delegates to detail/list"],
            "required_params": {"code": "6-digit fund code"},
            "optional_params": {"startDate": "yyyy-MM-dd", "endDate": "yyyy-MM-dd"},
            "pagination": {"required": False},
            "source_evidence_required": True,
        },
        {
            "path": _relative_path(AutoStockEndpoint.FUND_DETAIL_LIST, base_url=base_url),
            "label": "AutoStock fund/detail/list",
            "authoritative_for": ["autostock_batch_detail_crosscheck"],
            "used_by": ["autostock-nav-tail-fill detail retry", "autostock-missing-response-observation"],
            "required_params": {"code": "comma-separated fund codes"},
            "optional_params": {"startDate": "yyyy-MM-dd", "endDate": "yyyy-MM-dd"},
            "method": "GET",
            "quality_checks": ["validate per-row failure flags and membership; bound history when only current NAV is needed"],
            "pagination": {"required": False},
            "source_evidence_required": True,
        },
        {
            "path": "/fund/rank", "label": "AutoStock fund/rank", "method": "POST",
            "used_by": ["auxiliary client; not the authoritative fund universe"],
            "required_params": {},
            "optional_body_params": {"fundType": "array, e.g. [zq]", "sort": "r/z/1y/3y/6y/jn/1n/2n/3n", "asc": "0 descending, 1 ascending", "pageIndex": "1-based in live probes", "pageSize": "positive integer", "cp": "company array", "creatTimeLimit": "years", "fundScale": "10/100/1001"},
            "pagination": {"required": True, "data_field": "data.rank", "declared_total_field": "totalRecord"},
            "quality_checks": ["success can contain requestFail/resolveFail; declared total is not proof of complete coverage"],
        },
        {
            "path": "/fund/position", "label": "AutoStock fund/position", "method": "GET",
            "used_by": ["auxiliary single-fund client"],
            "required_params": {"code": "one 6-digit fund code"},
            "quality_checks": ["requestFail=1 was observed with HTTP/business code 200; preserve unavailable state"],
            "pagination": {"required": False},
        },
        {
            "path": "/fund/hot", "label": "AutoStock fund/hot", "method": "GET",
            "used_by": [], "required_params": {}, "pagination": {"required": False},
            "quality_checks": ["documented but unused; popularity is not a quality selection signal"],
        },
    ]


def upstream_api_source_contract(*, settings: Any) -> dict[str, Any]:
    """Return the machine-readable contract for real upstream business data sources."""
    tiantian_base_url = _clean_base_url(getattr(settings, "tiantian_base_url", None))
    autostock_base_url = _clean_base_url(getattr(settings, "autostock_base_url", None))
    tiantian_endpoints = _tiantian_endpoints()
    autostock_endpoints = _autostock_endpoints(base_url=autostock_base_url)
    source_groups = {
        "tiantian": {
            "display_name": "Tiantian fund API proxy",
            "current_configured_base_url": tiantian_base_url,
            "runtime_base_url": tiantian_base_url,
            "config_key": "apis.tiantian_base_url",
            "editable_via": ["/api-settings", "/settings", "/api/settings/apis"],
            "read_only_probe": {
                "endpoint": "/api/settings/apis/probe",
                "method": "POST",
                "writes_database": False,
                "writes_business_artifacts": False,
                "probe_path": APIEndpoints.FUND_RANK,
                "probe_params": {
                    "FundType": FundType.BOND,
                    "pageIndex": 1,
                    "pageSize": 1,
                },
            },
            "endpoints": tiantian_endpoints,
        },
        "autostock": {
            "enabled": getattr(settings, "autostock_enabled", True),
            "availability": "enabled" if getattr(settings, "autostock_enabled", True) else "disabled_after_transport_failures",
            "disabled_semantics": "independent verification remains unavailable; no same-supplier pass substitution",
            "display_name": "AutoStock API",
            "current_configured_base_url": autostock_base_url,
            "runtime_base_url": autostock_base_url,
            "config_key": "apis.autostock_base_url",
            "quota_policy": {
                "unauthenticated_calls_per_hour": 100,
                "source": "https://www.autostock.cn/#/guide",
                "verified_at": "2026-09-07",
                "verification": "provider guide; not a 101-call exhaustion experiment",
                "count_basis": "every HTTP attempt including retries; shared by web/scheduler/scripts",
            },
            "editable_via": ["/api-settings", "/settings", "/api/settings/apis"],
            "endpoints": autostock_endpoints,
        },
    }
    source_groups["fund_snapshot"] = {
        "current_configured_endpoints": [getattr(settings, "tiantian_valuation_primary_url", None),
                                         getattr(settings, "tiantian_valuation_fallback_url", None)],
        "supplier_group": "eastmoney_tiantian", "independent_of_tiantian": False,
        "used_by": ["favorites latest basic snapshot", "tracker intraday valuation"],
        "covers": ["latest NAV and date", "optional period returns", "optional intraday valuation"],
        "does_not_cover": ["complete directory", "daily NAV history", "manager", "scale", "holding period", "independent verification"],
        "history_and_directory_replacement": "existing Tiantian proxy collectors and maintenance",
        "evidence": ["request time", "endpoint", "raw response SHA256", "raw response artifact", "field states"],
    }
    source_groups["publication_validation"] = {
        "provider": getattr(settings, "publication_validation_provider", "autostock"),
        "current_configured_endpoints": [getattr(settings, "tiantian_valuation_primary_url", None),
                                         getattr(settings, "tiantian_valuation_fallback_url", None),
                                         getattr(settings, "eastmoney_fund_info_url", None)],
        "supplier_group": "eastmoney_tiantian", "independent_of_tiantian": False,
        "verification_scope": "same_supplier_endpoint_and_database_consistency",
        "covers": ["identity", "latest NAV/date confirmed by both endpoints", "five period returns from FundValuationLast"],
        "does_not_cover": ["independent supplier verification", "daily NAV history", "universal intraday valuation"],
        "quality_policy": "Fresh responses; row dates; original numerical and missing-data gates; no legacy status reuse",
        "evidence": ["raw body SHA256", "request time", "endpoint", "field states", "contract version", "report run/revision"],
    }
    return {
        "schema_version": UPSTREAM_API_SOURCE_CONTRACT_SCHEMA_VERSION,
        "status": "ok",
        "contract_scope": "real_upstream_business_data_sources",
        "source_groups": deepcopy(source_groups),
        "endpoint_index": {
            "tiantian": _endpoint_index(tiantian_endpoints),
            "autostock": _endpoint_index(autostock_endpoints),
        },
        "quality_policy": {
            "business_data_truth": [
                "real upstream APIs",
                "local sqlite database populated from real upstream APIs",
                "real runtime artifacts produced by project scripts",
                "reproducible calculations over real NAV/return inputs",
            ],
            "missing_value_policy": "preserve_missing_semantics",
            "fake_data_policy": "forbidden",
            "frontend_truth_boundary": "frontend displays are not authoritative publication truth",
            "publication_truth_boundary": "publication truth comes from reports/publication_registry.json and canonical run artifacts",
            "threshold_policy": "do not loosen production safety gates to increase final counts",
        },
    }
