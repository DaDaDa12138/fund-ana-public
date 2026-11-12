from __future__ import annotations

from typing import Any, Callable

from flask import Flask, Response, jsonify, request


def _no_store_jsonify(payload: dict[str, Any], status: int = 200) -> tuple[Response, int] | Response:
    response = jsonify(payload)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    if status == 200:
        return response
    return response, status


def register_system_routes(
    app: Flask,
    *,
    ensure_scheduler: Callable[[], None],
    build_api_info_payload: Callable[[], dict[str, Any]],
    build_health_payload: Callable[[], dict[str, Any]],
    build_health_summary_payload: Callable[[], dict[str, Any]],
    build_bootstrap_summary_payload: Callable[[], dict[str, Any]],
    build_autostock_missing_response_observation_payload: Callable[[], dict[str, Any]],
    build_api_settings_payload: Callable[[], dict[str, Any]],
    update_api_settings: Callable[[dict[str, Any]], tuple[dict[str, Any], int]],
    probe_api_settings: Callable[[dict[str, Any]], tuple[dict[str, Any], int]],
    build_storage_usage_payload: Callable[[], dict[str, Any]],
) -> None:
    @app.get("/api/info", endpoint="api_info")
    def api_info() -> Response:
        return jsonify(build_api_info_payload())

    @app.get("/api/health", endpoint="health")
    def health() -> Response:
        ensure_scheduler()
        return jsonify(build_health_payload())

    @app.get("/api/healthz", endpoint="healthz")
    def healthz() -> Response:
        return jsonify({"status": "ok"})

    @app.get("/api/storage/usage", endpoint="storage_usage")
    def storage_usage() -> Response:
        return _no_store_jsonify(build_storage_usage_payload())

    @app.get("/api/health/summary", endpoint="health_summary")
    def health_summary() -> Response:
        return jsonify(build_health_summary_payload())

    @app.get("/api/bootstrap/summary", endpoint="bootstrap_summary")
    def bootstrap_summary() -> Response:
        ensure_scheduler()
        return jsonify(build_bootstrap_summary_payload())

    @app.get("/api/settings/apis", endpoint="api_settings")
    def api_settings() -> Response:
        return _no_store_jsonify(build_api_settings_payload())

    @app.post("/api/settings/apis", endpoint="api_settings_update")
    def api_settings_update() -> Response:
        payload = request.get_json(silent=True) or {}
        result, http_status = update_api_settings(payload if isinstance(payload, dict) else {})
        return _no_store_jsonify(result, http_status)

    @app.post("/api/settings/apis/probe", endpoint="api_settings_probe")
    def api_settings_probe() -> Response:
        payload = request.get_json(silent=True) or {}
        result, http_status = probe_api_settings(payload if isinstance(payload, dict) else {})
        return _no_store_jsonify(result, http_status)

    @app.get(
        "/api/autostock/missing-response-observation",
        endpoint="autostock_missing_response_observation",
    )
    def autostock_missing_response_observation() -> Response:
        ensure_scheduler()
        return jsonify(build_autostock_missing_response_observation_payload())
