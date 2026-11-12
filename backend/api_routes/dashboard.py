from __future__ import annotations

from typing import Any, Callable

from flask import Flask, Response, jsonify, request


def register_dashboard_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    is_test_context: Callable[[], bool],
    build_overview_response: Callable[[dict[str, Any], dict[str, Any], dict[str, Any]], dict[str, Any]],
    load_overview_payload_for_test: Callable[[], dict[str, Any]],
    load_overview_publication_for_test: Callable[[], dict[str, Any]],
    load_scheduler_state_snapshot: Callable[[], dict[str, Any]],
    build_overview_cached: Callable[[], dict[str, Any]],
    build_latest_dashboard_detail_response: Callable[..., Response],
) -> None:
    def _request_full_samples() -> bool:
        return (request.args.get("full") or "").strip().lower() in {"1", "true", "yes", "on"}

    @app.get("/api/dashboard/overview", endpoint="dashboard_overview")
    def dashboard_overview() -> Response:
        ensure_scheduler()
        if is_test_context():
            payload = build_overview_response(
                load_overview_payload_for_test(),
                load_overview_publication_for_test(),
                load_scheduler_state_snapshot(),
            )
        else:
            payload = build_overview_cached()
        if payload.get("error") == "暂无报告":
            return jsonify(payload), 404
        return jsonify(payload)

    @app.get("/api/dashboard/latest", endpoint="latest_dashboard")
    def latest_dashboard() -> Response:
        compact_raw = (request.args.get("compact") or "").strip().lower()
        detail_scope = (request.args.get("detail_scope") or "").strip() or "diagnostic_summary"
        compact = compact_raw not in {"0", "false", "no", "off"}
        if request.args.get("detail_scope") is not None and compact_raw == "":
            compact = False
        return build_latest_dashboard_detail_response(
            compact=compact,
            detail_scope=detail_scope,
            dashboard_sample_limit=request.args.get("sample_limit"),
            dashboard_full_samples=_request_full_samples(),
        )

    @app.get("/api/dashboard/latest/detail", endpoint="latest_dashboard_detail")
    def latest_dashboard_detail() -> Response:
        return build_latest_dashboard_detail_response(
            compact=False,
            detail_scope="diagnostic_summary",
            dashboard_sample_limit=request.args.get("sample_limit"),
            dashboard_full_samples=_request_full_samples(),
        )

    @app.get("/api/dashboard/latest/detail/full-audit", endpoint="latest_dashboard_detail_full_audit")
    def latest_dashboard_detail_full_audit() -> Response:
        return build_latest_dashboard_detail_response(
            compact=False,
            detail_scope="audit_full",
            dashboard_sample_limit=request.args.get("sample_limit"),
            dashboard_full_samples=_request_full_samples(),
        )

    @app.get("/api/dashboard/latest/reports", endpoint="latest_dashboard_reports_detail")
    def latest_dashboard_reports_detail() -> Response:
        return build_latest_dashboard_detail_response(
            compact=False,
            detail_scope="reports",
            dashboard_sample_limit=request.args.get("sample_limit"),
            dashboard_full_samples=_request_full_samples(),
        )

    @app.get("/api/dashboard/latest/task-summary", endpoint="latest_dashboard_task_summary_detail")
    def latest_dashboard_task_summary_detail() -> Response:
        return build_latest_dashboard_detail_response(
            compact=False,
            detail_scope="task_summary",
            dashboard_sample_limit=request.args.get("sample_limit"),
            dashboard_full_samples=_request_full_samples(),
        )
