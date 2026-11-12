"""Exercise the production compact builder, bypassing the route's pytest branch."""
from types import SimpleNamespace


def test_compact_cold_build_preserves_snapshot_without_live_history_scan(tmp_path, monkeypatch):
    import backend.app as app

    payload = {
        "run_id": "committed-run",
        "generated_at": "2026-10-27T13:55:00",
        "counts": {"final_count": 280, "enhanced_bond_final_count": 13},
        "database_freshness": {"long_term_history_snapshot": {"candidate_fund_count": 12}},
        "project_publish_summary": {},
    }
    overview = {"maintenance_summary": {
        "long_term_status": "success",
        "long_term_upstream_observation_summary": {"upstream_limited_watchlist_total": 3},
        # An incomplete snapshot must not trigger the live audit fallback.
        "long_term_priority_blocked_summary": {"total": 2, "other_metadata_blocked_total": 2},
    }}
    monkeypatch.setattr(app, "settings", SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path))
    monkeypatch.setattr(app, "_cached_payload", lambda _key, _ttl, build: build())
    monkeypatch.setattr(app, "_file_content_cache_token", lambda _path: "snapshot")
    monkeypatch.setattr(app, "_scheduler_state_semantic_cache_token", lambda _path: "scheduler")
    monkeypatch.setattr(app, "_final_selection_contraction_shadow_cache_token", lambda: "shadow")
    monkeypatch.setattr(app, "_low_vol_overlap_audit_cache_token", lambda: "overlap")
    app.settings.latest_report_file = tmp_path / "latest.json"
    app.settings.publication_registry_file = tmp_path / "registry.json"
    monkeypatch.setattr(app, "_runtime_scheduler_settings", lambda: SimpleNamespace(scheduler_state_file=tmp_path / "scheduler.json"))
    monkeypatch.setattr(app, "_load_current_published_report_cached", lambda: payload)
    monkeypatch.setattr(app, "_load_publication_registry_snapshot_cached", lambda: {"current_published_run_id": "committed-run"})
    monkeypatch.setattr(app, "_load_scheduler_state_snapshot_cached", lambda: {})
    monkeypatch.setattr(app, "_build_dashboard_scheduler_lightweight_overview", lambda *_a, **_kw: overview)
    monkeypatch.setattr(app, "_load_exclusion_reason_summary", lambda *_a: {})
    monkeypatch.setattr(app, "_load_bond_universe_snapshot", lambda *_a: {})
    monkeypatch.setattr(app, "_build_coverage_layers_from_snapshots", lambda *_a, **_kw: {})
    monkeypatch.setattr(app, "_build_publish_funnel_summary", lambda *_a, **_kw: {})

    def forbidden(*_args, **_kwargs):
        raise AssertionError("compact cold build must not read or reconstruct live history audits")

    for name in ("_merge_live_long_term_priority_blocked_summary",
                 "_load_latest_long_term_maintenance_payload_cached",
                 "_load_long_term_gap_audit_cached", "_load_long_term_no_stale_audit_cached",
                 "_load_long_term_gap_audit_bundle_inputs"):
        monkeypatch.setattr(app, name, forbidden)

    response = app._build_latest_dashboard_compact_cached()

    assert response["run_id"] == "committed-run"
    assert response["counts"]["final_count"] == 280
    assert response["counts"]["enhanced_bond_final_count"] == 13
    assert response["scope_contract"]["live_diagnostics_merged"] is False
    assert "不重新验证其当前时效" in response["scope_contract"]["truth_boundary"]
    summary = response["long_term_governance_summary"]
    assert summary["candidate_fund_count"] == 12
    assert summary["auxiliary_scope"] == "same_run_report_aggregate"
