from __future__ import annotations

import json
from typing import Any, Callable

from flask import Flask, Response, jsonify, request, send_file

from backend.favorites_export import MAX_REQUEST_BYTES, build_favorites_export_workbook


def _normalize_request_codes(
    raw_codes: object,
    *,
    normalize_code_text: Callable[[object], str],
) -> list[str] | None:
    if not isinstance(raw_codes, list):
        return None
    return [
        code
        for code in (normalize_code_text(item) for item in raw_codes)
        if code
    ]


def register_favorites_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    load_manager_latest_report: Callable[[], dict[str, Any] | None],
    favorite_query_history_snapshot: Callable[[], dict[str, Any]],
    save_favorite_query_history: Callable[..., dict[str, Any]],
    normalize_favorite_history_entry_type: Callable[..., str],
    normalize_code_text: Callable[[object], str],
    request_arg_truthy: Callable[..., bool],
    is_truthy_text: Callable[[object], bool],
    build_favorite_fund_scores_response: Callable[..., dict[str, Any]],
    tracker_valuation_fetch_error: type[Exception],
    favorite_fund_query_max_codes: int,
    favorite_metric_detail_max_codes: int,
    favorite_metric_detail_fields: list[str],
    load_cached_favorite_metric_detail_payload: Callable[[list[str]], dict[str, dict[str, Any]]],
    build_favorite_metric_detail_source_basis: Callable[..., dict[str, Any]],
    build_favorite_metric_detail_source_contract: Callable[[dict[str, Any]], dict[str, Any]],
) -> None:
    @app.post("/api/favorites/export", endpoint="favorite_fund_export")
    def favorite_fund_export() -> Response:
        # Export the already displayed query; no new API call, database lookup,
        # history write, or publication selection is involved.
        if request.content_length and request.content_length > MAX_REQUEST_BYTES:
            return jsonify({"error": "导出内容过大"}), 413
        raw = request.stream.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            return jsonify({"error": "导出内容过大"}), 413
        try:
            buffer, filename, row_count = build_favorites_export_workbook(json.loads(raw))
        except (ValueError, TypeError, UnicodeError) as exc:
            return jsonify({"error": str(exc) if isinstance(exc, ValueError) and not isinstance(exc, json.JSONDecodeError) else "导出数据格式错误"}), 400
        response = send_file(buffer, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             as_attachment=True, download_name=filename, max_age=0)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Export-Row-Count"] = str(row_count)
        return response

    @app.get("/api/favorites/history", endpoint="favorite_fund_history")
    def favorite_fund_history() -> Response:
        ensure_scheduler()
        return jsonify(favorite_query_history_snapshot())

    @app.post("/api/favorites/history", endpoint="favorite_fund_history_upsert")
    def favorite_fund_history_upsert() -> Response:
        ensure_scheduler()
        request_payload = request.get_json(silent=True) or {}
        normalized_codes = _normalize_request_codes(
            request_payload.get("codes"),
            normalize_code_text=normalize_code_text,
        )
        if normalized_codes is None:
            return jsonify({"error": "基金代码列表格式错误"}), 400
        if not normalized_codes:
            return jsonify({"error": "缺少基金代码"}), 400
        if len(normalized_codes) > favorite_fund_query_max_codes:
            return jsonify({"error": f"单次最多保存 {favorite_fund_query_max_codes} 只基金"}), 400
        label_text = str(request_payload.get("label") or "").strip()
        history = save_favorite_query_history(
            normalized_codes,
            label=label_text or None,
            entry_type=normalize_favorite_history_entry_type(
                request_payload.get("entry_type"),
                label=label_text,
            ),
        )
        return jsonify(history)

    @app.post("/api/favorites/score", endpoint="favorite_fund_score")
    def favorite_fund_score() -> Response:
        ensure_scheduler()
        payload = load_manager_latest_report()
        if not payload:
            return jsonify({"error": "暂无报告"}), 404

        request_payload = request.get_json(silent=True) or {}
        normalized_codes = _normalize_request_codes(
            request_payload.get("codes"),
            normalize_code_text=normalize_code_text,
        )
        if normalized_codes is None:
            return jsonify({"error": "基金代码列表格式错误"}), 400
        if not normalized_codes:
            return jsonify({"error": "缺少基金代码"}), 400
        if len(normalized_codes) > favorite_fund_query_max_codes:
            return jsonify({"error": f"单次最多查询 {favorite_fund_query_max_codes} 只基金"}), 400

        response_mode = str(request_payload.get("response_mode") or "").strip().lower()
        include_details = (
            request_arg_truthy("detail", "full", "include_details")
            or is_truthy_text(request_payload.get("detail"))
            or is_truthy_text(request_payload.get("full"))
            or is_truthy_text(request_payload.get("include_details"))
            or response_mode in {"detail", "details", "full"}
        )
        include_recommendation_pool = not (
            request_payload.get("include_recommendation_pool") is False
            or str(request_payload.get("include_recommendation_pool") or "").strip().lower()
            in {"0", "false", "no", "off"}
        )
        include_autostock = not (
            request_payload.get("include_autostock") is False
            or str(request_payload.get("include_autostock") or "").strip().lower()
            in {"0", "false", "no", "off"}
        )

        try:
            response = build_favorite_fund_scores_response(
                payload,
                normalized_codes,
                label=str(request_payload.get("label") or "").strip() or None,
                save_history=bool(request_payload.get("save_history", True)),
                include_details=include_details,
                include_recommendation_pool=include_recommendation_pool,
                include_autostock=include_autostock,
            )
        except tracker_valuation_fetch_error as exc:
            quota_exceeded = (
                "配额已耗尽" in str(exc)
                or "AutoStockQuotaExceeded" in type(exc.__cause__).__name__
                if exc.__cause__
                else False
            )
            return jsonify(
                {
                    "error": f"AutoStock 实时基金查询失败：{exc}",
                    "status": "upstream_failed",
                    "source": "autostock_batch",
                    "autostock_validation_unavailable": bool(quota_exceeded),
                    "quota_exceeded": bool(quota_exceeded),
                    "requested_count": len(normalized_codes),
                    "returned_count": 0,
                    "rows": [],
                }
            ), 503
        return jsonify(response)

    @app.post("/api/favorites/metric-details", endpoint="favorite_fund_metric_details")
    def favorite_fund_metric_details() -> Response:
        ensure_scheduler()
        request_payload = request.get_json(silent=True) or {}
        normalized_codes = _normalize_request_codes(
            request_payload.get("codes"),
            normalize_code_text=normalize_code_text,
        )
        if normalized_codes is None:
            return jsonify({"error": "基金代码列表格式错误"}), 400
        if not normalized_codes:
            return jsonify({"error": "缺少基金代码"}), 400
        if len(normalized_codes) > favorite_metric_detail_max_codes:
            return jsonify({"error": f"单次最多查询 {favorite_metric_detail_max_codes} 只基金"}), 400

        details = load_cached_favorite_metric_detail_payload(normalized_codes)
        source_basis = build_favorite_metric_detail_source_basis(
            normalized_codes,
            details=details,
        )
        return jsonify(
            {
                "requested_count": len(normalized_codes),
                "returned_count": len(details),
                "max_request_codes": favorite_metric_detail_max_codes,
                "detail_fields": list(favorite_metric_detail_fields),
                "source_basis": source_basis,
                "cache_basis": source_basis,
                "source_contract": build_favorite_metric_detail_source_contract(source_basis),
                "details": details,
            }
        )
