from __future__ import annotations

from typing import Any, Callable

from flask import Flask, Response, jsonify, request


def register_tracker_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    load_current_published_report: Callable[[], dict[str, Any] | None],
    load_latest_report: Callable[[], dict[str, Any] | None],
    load_latest_report_snapshot: Callable[[], dict[str, Any] | None],
    load_manager_latest_report: Callable[[], dict[str, Any] | None],
    request_arg_truthy: Callable[..., bool],
    build_tracker_current_response: Callable[..., dict[str, Any]],
    build_tracker_current_compact_response: Callable[[dict[str, Any]], dict[str, Any]],
    normalize_tracker_valuation_codes: Callable[[list[str]], list[str]],
    get_tracker_valuation_max_request_codes: Callable[[], int],
    fetch_tracker_valuations: Callable[..., dict[str, Any]],
    tracker_valuation_fetch_error: type[Exception],
    valuation_cache_ttl_seconds: int,
    valuation_missing_cache_ttl_seconds: int,
) -> None:
    @app.get("/api/tracker/current", endpoint="tracker_current")
    def tracker_current() -> Response:
        ensure_scheduler()
        payload = load_current_published_report()
        if not payload:
            return jsonify(
                {
                    "error": "当前没有可用的正式发布",
                    "status": "publication_unavailable",
                    "publication_truth": False,
                }
            ), 503
        if request_arg_truthy("full", "detail") or str(request.args.get("scope") or "").strip().lower() == "detail":
            response_payload = build_tracker_current_response(
                payload,
                audit_read_only=request_arg_truthy("audit_read_only", "read_only"),
            )
            if request.args.get("row_aliases") == "0":
                # Preserve the legacy response and any cached builder object.
                response_payload = dict(response_payload)
                response_payload.pop("funds", None)
                scope = dict(response_payload.get("payload_scope") or {})
                scope["row_aliases"] = "rows_only"
                scope["omitted_fields"] = list(dict.fromkeys([*(scope.get("omitted_fields") or []), "funds"]))
                response_payload["payload_scope"] = scope
            return jsonify(response_payload)
        return jsonify(build_tracker_current_compact_response(payload))

    @app.get("/api/tracker/valuations", endpoint="tracker_valuations")
    def tracker_valuations() -> Response:
        ensure_scheduler()
        raw_codes = (request.args.get("codes") or "").strip()
        codes = [item.strip() for item in raw_codes.split(",") if item.strip()]
        max_request_codes = get_tracker_valuation_max_request_codes()
        if not codes:
            return jsonify(
                {
                    "error": "缺少基金代码",
                    "status": "bad_request",
                    "source": "tiantian_fund_valuation_last",
                    "requested_count": 0,
                    "returned_count": 0,
                    "max_request_codes": max_request_codes,
                    "cache_ttl_seconds": valuation_cache_ttl_seconds,
                    "missing_cache_ttl_seconds": valuation_missing_cache_ttl_seconds,
                    "valuations": {},
                }
            ), 400
        normalized_codes = normalize_tracker_valuation_codes(codes)
        if len(normalized_codes) > max_request_codes:
            return jsonify(
                {
                    "error": f"单次实时估值最多请求 {max_request_codes} 只基金",
                    "status": "too_many_codes",
                    "source": "tiantian_fund_valuation_last",
                    "requested_count": len(normalized_codes),
                    "returned_count": 0,
                    "max_request_codes": max_request_codes,
                    "cache_ttl_seconds": valuation_cache_ttl_seconds,
                    "missing_cache_ttl_seconds": valuation_missing_cache_ttl_seconds,
                    "valuations": {},
                }
            ), 400
        if str(request.args.get("audit_read_only") or request.args.get("read_only") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }:
            return jsonify(fetch_tracker_valuations(normalized_codes, audit_read_only=True))
        try:
            payload = fetch_tracker_valuations(normalized_codes)
        except tracker_valuation_fetch_error as exc:
            return jsonify(
                {
                    "error": f"天天基金实时估值请求失败：{exc}",
                    "status": "upstream_failed",
                    "source": "tiantian_fund_valuation_last",
                    "requested_count": len(normalized_codes),
                    "returned_count": 0,
                    "cached_count": 0,
                    "uncached_count": len(normalized_codes),
                    "missing_count": 0,
                    "rate_limit_wait_seconds": 0.0,
                    "quota_exhausted": False,
                    "quota_unavailable": False,
                    "cache_ttl_seconds": valuation_cache_ttl_seconds,
                    "missing_cache_ttl_seconds": valuation_missing_cache_ttl_seconds,
                    "max_request_codes": max_request_codes,
                    "valuations": {},
                }
            ), 503
        return jsonify(payload)
