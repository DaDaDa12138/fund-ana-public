from __future__ import annotations

import time
from typing import Any, Callable

from flask import Flask, Response, jsonify, request


def register_scheduler_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    build_status_summary_cached: Callable[[], dict[str, Any]],
    default_detail_sample_limit: Callable[[], Any],
    start_detail_build_profile: Callable[..., tuple[float, dict[str, Any]]],
    record_detail_build_step: Callable[..., None],
    with_detail_build_profile: Callable[[dict[str, Any], float, dict[str, Any]], dict[str, Any]],
    build_detail_section_cached: Callable[..., dict[str, Any]],
    build_detail_index_cached: Callable[..., dict[str, Any]],
    build_detail_variant_cached: Callable[..., dict[str, Any]],
    manual_operations_enabled: Callable[[], bool],
    dispatch_run_request: Callable[[str], tuple[dict[str, Any], int]],
    dispatch_internal_run_request: Callable[[str], tuple[dict[str, Any], int]],
    is_loopback_request: Callable[[], bool],
    load_collection_count_correction: Callable[[int], dict[str, Any]] | None = None,
    load_refresh_collection_correction: Callable[[int], dict[str, Any]] | None = None,
) -> None:
    @app.get("/api/scheduler/tasks/<int:task_id>/collection-counts", endpoint="scheduler_collection_counts")
    def scheduler_collection_counts(task_id: int) -> Response:
        result = (load_collection_count_correction(task_id) if load_collection_count_correction
                  else {"status": "absent", "verified": False, "task_id": task_id})
        return jsonify(result), 200 if result.get("verified") else 404 if result.get("status") == "absent" else 409

    @app.get("/api/scheduler/tasks/<int:task_id>/collection-outcome", endpoint="scheduler_collection_outcome")
    def scheduler_collection_outcome(task_id: int) -> Response:
        result = (load_refresh_collection_correction(task_id) if load_refresh_collection_correction
                  else {"status": "absent", "verified": False, "task_id": task_id})
        response = jsonify(result)
        response.headers["Cache-Control"] = "no-store"
        return response, 200 if result.get("verified") else 404 if result.get("status") == "absent" else 409

    @app.get("/api/scheduler/status", endpoint="scheduler_status")
    def scheduler_status() -> Response:
        ensure_scheduler()
        return jsonify(build_status_summary_cached())

    @app.get("/api/scheduler/status/detail", endpoint="scheduler_status_detail")
    def scheduler_status_detail() -> Response:
        ensure_scheduler()
        full_raw = (request.args.get("full") or "").strip().lower()
        detail_raw = (request.args.get("detail") or "").strip().lower()
        section_raw = (request.args.get("section") or "").strip()
        include_full_samples = full_raw in {"1", "true", "yes", "on"}
        include_detail = include_full_samples or detail_raw in {"1", "true", "yes", "on"}
        sample_limit = request.args.get("sample_limit", default_detail_sample_limit())
        normalized_section = str(section_raw or "").strip().lower().replace("-", "_")
        detail_scope = (
            "section"
            if section_raw and not include_full_samples
            else "detail_index"
            if include_detail and not include_full_samples
            else "full"
            if include_full_samples
            else "compact"
        )
        profile_started_at, detail_profile = start_detail_build_profile(
            detail_scope,
            section=normalized_section if normalized_section else None,
            sample_limit=sample_limit,
            include_full_samples=include_full_samples,
        )
        if section_raw and not include_full_samples:
            step_started_at = time.perf_counter()
            section_payload = build_detail_section_cached(
                section_raw,
                sample_limit=sample_limit,
            )
            record_detail_build_step(
                detail_profile,
                "section_payload_load",
                step_started_at,
                section=normalized_section,
                error=section_payload.get("error"),
            )
            response_payload = with_detail_build_profile(
                section_payload,
                profile_started_at,
                detail_profile,
            )
            if section_payload.get("error") == "unknown_scheduler_detail_section":
                return jsonify(response_payload), 400
            return jsonify(response_payload)

        if include_detail and not include_full_samples:
            step_started_at = time.perf_counter()
            payload = build_detail_index_cached(sample_limit=sample_limit)
            record_detail_build_step(
                detail_profile,
                "detail_index_payload_load",
                step_started_at,
            )
            return jsonify(with_detail_build_profile(payload, profile_started_at, detail_profile))

        step_started_at = time.perf_counter()
        payload = build_detail_variant_cached(
            include_detail=include_detail,
            include_full_samples=include_full_samples,
            sample_limit=sample_limit,
        )
        record_detail_build_step(
            detail_profile,
            "variant_payload_load",
            step_started_at,
            include_detail=include_detail,
        )
        return jsonify(with_detail_build_profile(payload, profile_started_at, detail_profile))

    @app.post("/api/scheduler/manual-run", endpoint="scheduler_manual_run")
    def scheduler_manual_run() -> Response:
        if not manual_operations_enabled():
            return jsonify(
                {
                    "error": "当前环境已切换为自动运行模式，普通页面不开放手动补跑",
                    "status": "manual_operations_disabled",
                }
            ), 403

        payload = request.get_json(silent=True) or {}
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"error": "缺少 job_id", "status": "invalid_request"}), 400

        result, http_status = dispatch_run_request(job_id)
        return jsonify(result), http_status

    @app.post("/api/scheduler/internal-run-once", endpoint="scheduler_internal_run_once")
    def scheduler_internal_run_once() -> Response:
        ensure_scheduler()
        if not is_loopback_request() or request.headers.get("X-Fund-ANA-Internal-Run") != "scheduler-cli":
            return jsonify({"error": "forbidden", "status": "forbidden"}), 403

        payload = request.get_json(silent=True) or {}
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            return jsonify({"error": "缺少 job_id", "status": "invalid_request"}), 400

        result, http_status = dispatch_internal_run_request(job_id)
        return jsonify(result), http_status
