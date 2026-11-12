from __future__ import annotations

from typing import Any, Callable

from flask import Flask, Response, jsonify, request, send_file


def register_recommendations_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    is_test_context: Callable[[], bool],
    load_recommendations_payload_for_test: Callable[[], dict[str, Any]],
    load_current_published_report: Callable[[], dict[str, Any] | None],
    load_latest_report: Callable[[], dict[str, Any] | None],
    load_latest_report_snapshot: Callable[[], dict[str, Any] | None],
    load_manager_latest_report: Callable[[], dict[str, Any] | None],
    build_recommendations_summary_response: Callable[[dict[str, Any]], dict[str, Any]],
    build_recommendations_detail_response: Callable[..., dict[str, Any]],
    build_recommendations_export_workbook: Callable[[dict[str, Any]], tuple[Any, str]],
) -> None:
    def _load_recommendations_payload() -> dict[str, Any] | None:
        if is_test_context():
            return load_recommendations_payload_for_test()
        return load_current_published_report()

    def _publication_unavailable_response() -> tuple[Response, int]:
        return jsonify(
            {
                "error": "当前没有可用的正式发布",
                "status": "publication_unavailable",
                "publication_truth": False,
            }
        ), 503

    @app.get("/api/recommendations/latest", endpoint="recommendations_latest")
    def recommendations_latest() -> Response:
        ensure_scheduler()
        payload = _load_recommendations_payload()
        if not payload:
            return _publication_unavailable_response()
        if (
            str(request.args.get("scope") or "").strip().lower() == "detail"
            or request.args.get("full") in {"1", "true"}
        ):
            return jsonify(
                build_recommendations_detail_response(
                    payload,
                    request.args.get("group") or "",
                    audit_read_only=bool(request.args.get("audit_read_only") in {"1", "true", "yes"})
                    or bool(request.args.get("read_only") in {"1", "true", "yes"}),
                )
            )
        return jsonify(build_recommendations_summary_response(payload))

    @app.get("/api/recommendations/latest/detail", endpoint="recommendations_latest_detail")
    def recommendations_latest_detail() -> Response:
        ensure_scheduler()
        payload = _load_recommendations_payload()
        if not payload:
            return _publication_unavailable_response()
        return jsonify(
            build_recommendations_detail_response(
                payload,
                request.args.get("group") or "",
                audit_read_only=bool(request.args.get("audit_read_only") in {"1", "true", "yes"})
                or bool(request.args.get("read_only") in {"1", "true", "yes"}),
            )
        )

    @app.get("/api/recommendations/export", endpoint="recommendations_export")
    def recommendations_export() -> Response:
        ensure_scheduler()
        payload = _load_recommendations_payload()
        if not payload:
            return _publication_unavailable_response()
        workbook_buffer, filename = build_recommendations_export_workbook(payload)
        return send_file(
            workbook_buffer,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename,
            max_age=0,
        )
