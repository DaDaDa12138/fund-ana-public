"""HTTP cache and projection contracts; fixtures are isolated from business data."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys
from unittest.mock import Mock, patch

import pytest
from flask import Flask, request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.api_routes.tracker import register_tracker_routes


@pytest.fixture
def frontend_client(monkeypatch):
    with patch("backend.schema_bootstrap.ensure_database_schema", return_value={}):
        import backend.app as backend_app
    monkeypatch.setattr(backend_app, "ensure_embedded_scheduler", lambda: None)
    return backend_app.app.test_client(), backend_app.__version__


@pytest.mark.parametrize("asset", ["shared.js", "styles.css", "favicon.svg"])
def test_current_version_asset_keeps_validator_and_conditional_response(frontend_client, asset):
    client, version = frontend_client
    url = f"/{asset}?v={version}"
    response = client.get(url)
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=31536000, immutable"
    etag = response.headers["ETag"]
    assert "Pragma" not in response.headers
    assert "Expires" not in response.headers
    conditional = client.get(url, headers={"If-None-Match": etag})
    assert conditional.status_code == 304
    assert conditional.data == b""
    assert conditional.headers["ETag"] == etag
    assert conditional.headers["Cache-Control"] == response.headers["Cache-Control"]
    head = client.head(url)
    assert head.status_code == 200
    assert head.data == b""
    assert head.headers["ETag"] == etag
    assert head.headers["Cache-Control"] == response.headers["Cache-Control"]


@pytest.mark.parametrize("path", ["/", "/tracker", "/favorites", "/shared.js", "/shared.js?v=old"])
def test_html_and_unversioned_or_mismatched_assets_require_fresh_response(frontend_client, path):
    client, _ = frontend_client
    for method in (client.get, client.head):
        response = method(path)
        assert response.status_code == 200
        assert "no-store" in response.headers["Cache-Control"]
        assert response.headers["Pragma"] == "no-cache"
        assert response.headers["Expires"] == "0"
        assert "ETag" not in response.headers


def test_missing_asset_is_never_cached_as_immutable(frontend_client):
    client, version = frontend_client
    response = client.get(f"/nonexistent-asset.js?v={version}")
    assert response.status_code == 404
    assert "no-store" in response.headers["Cache-Control"]


@pytest.fixture
def tracker_client():
    app = Flask(__name__)
    rows = [{"marker": "isolated-io-fixture", "missing": None, "zero": 0}]
    full = {"rows": rows, "funds": rows, "row_count": 1, "counts": {"fixture_count": 1},
            "source_basis": {"digest": "fixture-source"},
            "pagination": {"page": 1, "total_rows": 1},
            "payload_scope": {"mode": "detail", "omitted_fields": ["internal_fixture"]}}
    compact = {"rows": rows, "row_count": 1, "payload_scope": {"mode": "compact"}}
    build_full = Mock(return_value=full)
    published = {"run_id": "isolated-io-fixture"}
    register_tracker_routes(
        app, ensure_scheduler=lambda: None, load_current_published_report=lambda: published,
        load_latest_report=Mock(side_effect=AssertionError("no fallback")),
        load_latest_report_snapshot=Mock(side_effect=AssertionError("no fallback")),
        load_manager_latest_report=Mock(side_effect=AssertionError("no fallback")),
        request_arg_truthy=lambda *names: any(request.args.get(name) == "1" for name in names),
        build_tracker_current_response=build_full,
        build_tracker_current_compact_response=lambda payload: compact,
        normalize_tracker_valuation_codes=lambda values: values,
        get_tracker_valuation_max_request_codes=lambda: 50,
        fetch_tracker_valuations=Mock(side_effect=AssertionError("no upstream request")),
        tracker_valuation_fetch_error=RuntimeError,
        valuation_cache_ttl_seconds=300, valuation_missing_cache_ttl_seconds=30,
    )
    return app.test_client(), full, compact, build_full, published


@pytest.mark.parametrize("detail_args", ["full=1", "detail=1", "scope=detail"])
def test_rows_only_preserves_members_values_source_and_builder_cache(tracker_client, detail_args):
    client, full, _, builder, published = tracker_client
    original = deepcopy(full)
    response = client.get(f"/api/tracker/current?{detail_args}&row_aliases=0&audit_read_only=1")
    assert response.status_code == 200
    actual = response.get_json()
    expected = deepcopy(original)
    expected.pop("funds")
    expected["payload_scope"].update(row_aliases="rows_only", omitted_fields=["internal_fixture", "funds"])
    assert actual == expected
    assert actual["rows"][0]["missing"] is None
    assert actual["rows"][0]["zero"] == 0
    assert full == original
    assert full["funds"] is full["rows"]
    builder.assert_called_once_with(published, audit_read_only=True)
    legacy = client.get(f"/api/tracker/current?{detail_args}").get_json()
    assert legacy == original


def test_projection_is_opt_in_and_does_not_change_compact_contract(tracker_client):
    client, full, compact, builder, _ = tracker_client
    assert client.get("/api/tracker/current?full=1&row_aliases=1").get_json() == full
    builder.reset_mock()
    assert client.get("/api/tracker/current?row_aliases=0").get_json() == compact
    builder.assert_not_called()
