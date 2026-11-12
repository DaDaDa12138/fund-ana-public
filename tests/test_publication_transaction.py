"""Failure injection for the publication protocol (synthetic I/O fixtures)."""
from __future__ import annotations

import copy
from dataclasses import replace
import json
import multiprocessing
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.config import load_settings
from backend.publication import build_publication_registry, load_publication_registry
from backend import publication_transaction as tx


@pytest.fixture
def settings(tmp_path):
    return replace(load_settings(), project_root=tmp_path, reports_dir=tmp_path / "reports",
                   runs_dir=tmp_path / "reports/runs", archived_runs_dir=tmp_path / "reports/archive/superseded_runs",
                   latest_report_file=tmp_path / "reports/latest.json",
                   publication_registry_file=tmp_path / "reports/publication_registry.json",
                   database_path=tmp_path / "unused.db", state_file=tmp_path / "state.json",
                   scheduler_state_file=tmp_path / "scheduler.json")


def make_candidate(settings, run_id="20260907_100000_sync", generated_at="2026-09-07T10:00:00"):
    directory = tx.new_report_revision(settings, run_id)
    paths = {"report_json": directory / f"bond_analysis_report_{run_id}.json",
             "project_tracker_excel": directory / tx.TRACKER_NAME,
             "project_tracker_excel_meta": directory / tx.TRACKER_META_NAME}
    for key in ("project_tracker_excel", "project_tracker_excel_meta"):
        paths[key].write_text(f"synthetic io fixture: {directory.name}: {key}")
    checks = [{"name": name, "aligned": True} for name in (
        "formal_csv_tracker_membership", "formal_tracker_metric_values", "dynamic_csv_count", "publication_member_hard_gates")]
    report = {"run_id": run_id, "generated_at": generated_at,
              "counts": {"final_count": 1},
              "database_freshness": {"effective_nav_snapshot": {"candidate_date": "2026-09-04", "ready": True}},
              "publication_control": {"commit_protocol": 1, "mode": "formal", "artifact_checks_required": True},
              "internal_audit": {"artifact_consistency": {"aligned": True, "blocker_count": 0,
                                                          "checks": checks, "check_count": len(checks)}},
              "artifacts": {k: str(v.relative_to(settings.project_root)) for k, v in paths.items()}}
    paths["report_json"].write_text(json.dumps(report), encoding="utf-8")
    return report


def published_files(settings):
    return tuple(p.read_bytes() for p in (settings.latest_report_file,
                 settings.reports_dir / tx.TRACKER_NAME, settings.reports_dir / tx.TRACKER_META_NAME))


def test_new_duration_contract_requires_source_check_before_promotion(settings):
    from backend.publication import _resolve_publish_blockers

    candidate = make_candidate(settings)
    candidate['publication_control']['drawdown_source_checks_required'] = True
    assert any(item['code'] == 'publication_artifact_integrity_failed' for item in _resolve_publish_blockers(candidate))
    audit = candidate['internal_audit']['artifact_consistency']
    candidate['publication_control']['full_value_checks_required'] = True
    audit['checks'].extend({'name': name, 'aligned': True} for name in (
        'publication_input_signature', 'formal_all_values_from_frozen_inputs',
        'formal_fee_and_source_provenance', 'formal_workbook_complete_membership',
    ))
    audit['checks'].append({'name': 'formal_drawdown_matches_current_risk_source', 'aligned': True})
    audit['check_count'] = len(audit['checks'])
    assert not any(item['code'] == 'publication_artifact_integrity_failed' for item in _resolve_publish_blockers(candidate))


def test_candidate_and_failed_same_run_rebuild_never_change_published_artifacts(settings):
    first = make_candidate(settings)
    assert build_publication_registry(settings)["current_published_run_id"] == ""
    tx.commit_report_revision(settings, first)
    before = published_files(settings)
    second = make_candidate(settings, generated_at="2026-09-07T10:01:00")
    # Even an accidentally exposed root candidate must be ignored by readers.
    tx.expose_report_alias(settings, second)
    registry = build_publication_registry(settings)
    assert registry["current_published_report_path"].endswith(first["artifacts"]["report_json"])
    assert published_files(settings) == before
    second["internal_audit"]["artifact_consistency"]["aligned"] = False
    (settings.project_root / second["artifacts"]["report_json"]).write_text(json.dumps(second))
    with pytest.raises(tx.PublicationIntegrityError, match="business_gates"):
        tx.commit_report_revision(settings, second)
    assert published_files(settings) == before


@pytest.mark.parametrize("fault", ["missing_check", "failed_check", "missing_audit"])
def test_required_csv_provenance_gate_blocks_commit_and_preserves_publication(settings, fault):
    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    before = published_files(settings)
    candidate = make_candidate(settings, generated_at="2026-09-07T10:01:00")
    candidate["publication_control"]["csv_return_provenance_checks_required"] = True
    audit = candidate["internal_audit"]["artifact_consistency"]
    if fault == "failed_check":
        audit["checks"].append({"name": "csv_return_provenance_matches_frozen_inputs", "aligned": False})
        audit["check_count"] = len(audit["checks"])
    elif fault == "missing_audit":
        candidate["publication_control"]["artifact_checks_required"] = False
        candidate.pop("internal_audit")
    (settings.project_root / candidate["artifacts"]["report_json"]).write_text(json.dumps(candidate))
    with pytest.raises(tx.PublicationIntegrityError, match="business_gates"):
        tx.commit_report_revision(settings, candidate)
    assert published_files(settings) == before
    assert tx.load_publication_head(settings)["generation"] == 1


@pytest.mark.parametrize("stage", ["manifest", "descriptor", "commit_link", "run_alias"])
def test_crash_at_each_commit_phase_preserves_one_complete_generation(settings, monkeypatch, stage):
    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    before = published_files(settings)
    second = make_candidate(settings, "20260907_110000_sync", "2026-09-07T11:00:00")
    write, link = tx._write_new_json, tx._replace_link
    def fail_write(path, payload):
        if (stage == "manifest" and path.name == tx.MANIFEST_NAME) or (stage == "descriptor" and path.name == tx.STATE_NAME):
            raise OSError(f"injected_{stage}")
        return write(path, payload)
    def fail_link(path, target):
        if stage == "commit_link" and path.name == tx.CURRENT_LINK:
            raise OSError("injected_commit_link")
        if stage == "run_alias" and path.name == f"bond_analysis_report_{second['run_id']}.json":
            raise OSError("injected_run_alias")
        return link(path, target)
    with monkeypatch.context() as patch:
        patch.setattr(tx, "_write_new_json", fail_write)
        patch.setattr(tx, "_replace_link", fail_link)
        with pytest.raises(OSError, match="injected"):
            tx.commit_report_revision(settings, second)
    committed = stage == "run_alias"
    head = tx.load_publication_head(settings)
    assert head["current_published_run_id"] == (second if committed else first)["run_id"]
    if committed:
        # The global aliases follow one pointer even before index recovery.
        assert published_files(settings) != before
        assert json.loads(settings.latest_report_file.read_text())["run_id"] == second["run_id"]
        assert (settings.reports_dir / tx.TRACKER_NAME).read_bytes() == (settings.project_root / second["artifacts"]["project_tracker_excel"]).read_bytes()
    else:
        assert published_files(settings) == before
    tx.recover_publication_aliases(settings)
    assert tx.load_publication_head(settings)["generation"] == (2 if committed else 1)
    tx.recover_publication_aliases(settings)
    assert tx.load_publication_head(settings)["generation"] == (2 if committed else 1)


def test_same_run_republication_keeps_previous_files_immutable(settings):
    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    original_paths = [settings.project_root / value for value in first["artifacts"].values()]
    original_bytes = [path.read_bytes() for path in original_paths]
    second = make_candidate(settings, generated_at="2026-09-07T10:01:00")
    tx.commit_report_revision(settings, second)
    assert [path.read_bytes() for path in original_paths] == original_bytes
    assert tx.load_publication_head(settings)["generation"] == 2
    tx.commit_report_revision(settings, second)  # Recovery is idempotent.
    assert tx.load_publication_head(settings)["generation"] == 2
    assert json.loads(settings.latest_report_file.read_text())["generated_at"] == second["generated_at"]


@pytest.mark.parametrize("absolute_alias", [False, True])
def test_restart_preserves_configured_alias_and_follows_next_commit(settings, monkeypatch, absolute_alias):
    from backend import config as config_module

    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    first_path = settings.project_root / first["artifacts"]["report_json"]
    before = first_path.read_bytes()
    config_path = settings.project_root / "restart.yaml"
    configured_alias = str(settings.latest_report_file) if absolute_alias else "reports/latest.json"
    config_path.write_text(f"paths:\n  latest_report_file: {configured_alias}\n")
    monkeypatch.setattr(config_module, "PROJECT_ROOT", settings.project_root)
    restarted = config_module.load_settings(config_path)
    assert restarted.latest_report_file == settings.latest_report_file
    assert restarted.latest_report_file != first_path
    tx.recover_publication_aliases(restarted)
    assert not first_path.is_symlink()
    assert first_path.read_bytes() == before
    second = make_candidate(restarted, "20260907_110000_sync", "2026-09-07T11:00:00")
    tx.commit_report_revision(restarted, second)
    # A settings instance loaded before publication must follow the new head.
    assert json.loads(restarted.latest_report_file.read_text())["run_id"] == second["run_id"]
    assert first_path.read_bytes() == before
    tx.recover_publication_aliases(config_module.load_settings(config_path))
    assert json.loads(settings.latest_report_file.read_text())["run_id"] == second["run_id"]


@pytest.mark.parametrize("path_kind", ["resolved_report", "through_head", "archived_run"])
def test_alias_recovery_rejects_history_destination_before_any_write(settings, path_kind):
    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    report_path = settings.project_root / first["artifacts"]["report_json"]
    before = report_path.read_bytes()
    if path_kind == "resolved_report":
        bad_alias = settings.latest_report_file.resolve()
    elif path_kind == "through_head":
        bad_alias = settings.reports_dir / tx.CURRENT_LINK / "report.json"
    else:
        bad_alias = settings.archived_runs_dir / "old-run/latest.json"
    invalid = replace(settings, latest_report_file=bad_alias)
    with pytest.raises(tx.PublicationIntegrityError, match="latest_alias_inside_run_history"):
        tx.recover_publication_aliases(invalid)
    second = make_candidate(settings, "20260907_110000_sync", "2026-09-07T11:00:00")
    with pytest.raises(tx.PublicationIntegrityError, match="latest_alias_inside_run_history"):
        tx.commit_report_revision(invalid, second)
    assert tx.load_publication_head(settings)["current_published_run_id"] == first["run_id"]
    assert not report_path.is_symlink()
    assert report_path.read_bytes() == before


@pytest.mark.parametrize("key", ["report_json", "project_tracker_excel", "project_tracker_excel_meta"])
def test_committed_artifact_corruption_fails_explicitly(settings, key):
    report = make_candidate(settings)
    tx.commit_report_revision(settings, report)
    path = settings.project_root / report["artifacts"][key]
    path.write_bytes(path.read_bytes() + b"corruption")
    with pytest.raises(tx.PublicationIntegrityError, match="hash_mismatch"):
        build_publication_registry(settings)


def test_legacy_bootstrap_uses_published_run_local_tracker(settings):
    legacy = make_candidate(settings)
    source = settings.project_root / legacy["artifacts"]["report_json"]
    legacy["publication_control"].pop("commit_protocol")
    source.write_text(json.dumps(legacy))
    alias = settings.runs_dir / legacy["run_id"] / source.name
    alias.write_text(json.dumps(legacy))
    legacy["artifacts"]["report_json"] = str(alias.relative_to(settings.project_root))
    alias.write_text(json.dumps(legacy))
    next_report = make_candidate(settings, "20260907_110000_sync", "2026-09-07T11:00:00")
    tx.commit_report_revision(settings, next_report, legacy_report=legacy)
    head = tx.load_publication_head(settings)
    assert set(head["committed_runs"]) == {legacy["run_id"], next_report["run_id"]}
    assert source.exists() and alias.exists()
    assert json.loads(settings.latest_report_file.read_text())["run_id"] == next_report["run_id"]


def test_corrupt_commit_descriptor_is_not_reconstructed_from_directory_scan(settings):
    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    descriptor = (settings.reports_dir / tx.CURRENT_LINK).resolve() / tx.STATE_NAME
    descriptor.write_text("")
    with pytest.raises(tx.PublicationIntegrityError, match="control_unreadable"):
        build_publication_registry(settings)


def test_same_run_legacy_bootstrap_survives_alias_switch_and_rollback(settings):
    legacy = make_candidate(settings)
    original_path = settings.runs_dir / legacy["run_id"] / f"bond_analysis_report_{legacy['run_id']}.json"
    legacy["publication_control"].pop("commit_protocol")
    legacy["artifacts"]["report_json"] = str(original_path.relative_to(settings.project_root))
    original_path.write_text(json.dumps(legacy))
    original_tracker = (settings.project_root / legacy["artifacts"]["project_tracker_excel"]).read_bytes()
    new = make_candidate(settings, generated_at="2026-09-07T10:05:00")
    tx.commit_report_revision(settings, new, legacy_report=legacy)
    head = tx.load_publication_head(settings)
    previous = settings.project_root / head["previous_generation_dir"]
    tx._replace_link(settings.reports_dir / tx.CURRENT_LINK, previous)
    tx.recover_publication_aliases(settings)
    recovered = tx.load_publication_head(settings)
    assert recovered["generation"] == 0
    assert json.loads(settings.latest_report_file.read_text())["generated_at"] == legacy["generated_at"]
    assert (settings.reports_dir / tx.TRACKER_NAME).read_bytes() == original_tracker


def test_request_pins_one_generation_and_next_request_recovers_stale_registry(settings, monkeypatch):
    from backend import app as api

    monkeypatch.setattr(api, "settings", settings)
    first = make_candidate(settings)
    tx.commit_report_revision(settings, first)
    with api.app.test_request_context("/api/tracker/current"):
        before = api._load_current_published_report_cached()
        second = make_candidate(settings, "20260907_110000_sync", "2026-09-07T11:00:00")
        tx.commit_report_revision(settings, second)
        assert api._load_current_published_report_cached()["run_id"] == before["run_id"]
        assert api._load_publication_registry_snapshot_cached()["current_published_run_id"] == before["run_id"]
    with api.app.test_request_context("/api/tracker/current"):
        assert api._load_current_published_report_cached()["run_id"] == second["run_id"]
        assert api._load_publication_registry_snapshot_cached()["commit_generation"] == 2


def test_recommendations_basis_is_frozen_and_api_cannot_rewrite_it(settings, monkeypatch):
    from backend.recommendations_sidecar import build_recommendations_detail_cache_basis, recommendations_detail_sidecar_path
    from backend import app as api

    report = make_candidate(settings)
    directory = (settings.project_root / report["artifacts"]["report_json"]).parent
    report["artifacts"]["recommendations_detail_json"] = str((directory / "recommendations_detail.json").relative_to(settings.project_root))
    before = build_recommendations_detail_cache_basis(settings, report)
    settings.database_path.write_bytes(b"new database state is irrelevant to a frozen report")
    settings.latest_report_file.write_text('{"run_id":"unrelated"}')
    assert build_recommendations_detail_cache_basis(settings, report) == before
    assert recommendations_detail_sidecar_path(settings, report) == directory / "recommendations_detail.json"
    monkeypatch.setattr(api, "settings", settings)
    assert api._write_recommendations_detail_sidecar(report, before, {"rows": []}) is None
    assert not (directory / "recommendations_detail.json").exists()


def _lock_counter_worker(settings, path):
    for _ in range(5):
        with tx.publication_io_lock(settings):
            value = int(path.read_text())
            with tx.publication_io_lock(settings):
                path.write_text(str(value + 1))


def test_publication_lock_is_reentrant_and_shared_by_four_processes(settings):
    path = settings.project_root / "io_counter.txt"
    path.write_text("0")
    context = multiprocessing.get_context("fork")
    workers = [context.Process(target=_lock_counter_worker, args=(settings, path)) for _ in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=20)
        assert not worker.is_alive()
        assert worker.exitcode == 0
    assert path.read_text() == "20"


def test_committed_tracker_derived_values_use_frozen_inputs_and_cannot_be_overwritten(settings, monkeypatch):
    from backend import app as api

    report = make_candidate(settings)
    directory = (settings.project_root / report["artifacts"]["report_json"]).parent
    path = directory / "project_tracker_current_detail.json"
    report["artifacts"]["project_tracker_current_detail_json"] = str(path.relative_to(settings.project_root))
    row = {"基金代码": "000001", "债基风格标签": "中短债", "正式榜分组": "稳健组"}
    fixture = {"artifact_type": "project_tracker_current_detail_sidecar", "run_id": report["run_id"],
               "generated_at": report["generated_at"], "rows": [row], "derived_fields": {
                   "schema_version": 2,
                   "data_source_by_code": {"000001": "fixture_source"},
                   "annualized_return_by_code": {"000001": "0"},
                   "annual_return_breakdown_by_code": {"000001": [{"year": 2025, "annual_return": "0"}]}}}
    path.write_text(json.dumps(fixture))
    before = path.read_bytes()
    monkeypatch.setattr(api, "settings", settings)
    def reject_later_database(*args, **kwargs):
        raise AssertionError("Frozen business values must not be reconstructed from a later database")
    for name in ("_load_fund_data_source_map", "_compute_since_inception_annualized_return_map",
                 "_load_since_inception_annual_return_breakdown_map"):
        monkeypatch.setattr(api, name, reject_later_database)
    fields = api._build_tracker_current_detail_derived_fields(report, [row], style_threshold_payload={})
    assert fields["annualized_return_by_code"] == {"000001": "0"}
    basis = api._build_tracker_current_derived_fields_cache_basis(report, [row], {})
    assert basis["basis_kind"] == "immutable_publication_revision"
    assert "database_file" not in basis
    scope = {"sidecar_file": report["artifacts"]["project_tracker_current_detail_json"]}
    assert api._write_tracker_current_detail_derived_fields(scope, basis, {"annualized_return_by_code": {}}) is None
    assert path.read_bytes() == before


def test_frozen_tracker_missing_inputs_fail_without_live_fallback(settings, monkeypatch):
    from backend import app as api

    monkeypatch.setattr(api, "settings", settings)
    report = make_candidate(settings)
    with pytest.raises(RuntimeError, match="derived_inputs_missing"):
        api._build_tracker_current_detail_derived_fields(report, [{"基金代码": "000001"}], style_threshold_payload={})


def enable_history(settings):
    from core.utils import report_history_store
    settings.project_root.joinpath('config').mkdir()
    settings.project_root.joinpath('config/project.yaml').write_text('report_history:\n  read_mode: ledger\n')
    return replace(settings, report_history_database_path=settings.project_root/'backend/data/report_history.sqlite')


def ledger_candidate(settings,run_id='20260907_100000_sync'):
    import pandas as pd
    report=make_candidate(settings,run_id)
    path=settings.project_root/report['artifacts']['project_tracker_excel']
    pd.DataFrame({'基金代码':['000001'],'数值':[0]}).to_excel(path,index=False)
    report['publication_control']['revision']=path.parent.name
    (settings.project_root/report['artifacts']['report_json']).write_text(json.dumps(report))
    return report


def test_ledger_binding_is_part_of_only_committed_manifest(settings,monkeypatch):
    from backend import report_history
    settings=enable_history(settings)
    first=ledger_candidate(settings)
    tx.commit_report_revision(settings,first)
    head=tx.load_publication_head(settings)
    manifest=settings.project_root/head['committed_runs'][first['run_id']]['manifest_path']
    assert 'history_binding.json' in json.loads(manifest.read_text())['files']
    assert report_history.get_report(settings,first['run_id'])['semantic']=='committed'
    before=published_files(settings)
    second=ledger_candidate(settings,'20260907_110000_sync')
    def fail(*a,**k):raise OSError('artificial ledger write failure')
    monkeypatch.setattr(report_history,'import_report',fail)
    with pytest.raises(OSError,match='ledger write failure'):tx.commit_report_revision(settings,second)
    assert published_files(settings)==before
    assert tx.load_publication_head(settings)['generation']==1
