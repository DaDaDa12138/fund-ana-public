"""Cache provenance checks use synthetic I/O fixtures, never business values."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import audit_data_truthfulness as audit


@pytest.fixture(params=[2, 3])
def frozen_publication(tmp_path, request):
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports",
                               runs_dir=tmp_path / "reports/runs")
    run_id, revision = "fixture_run", "a" * 32
    directory = settings.runs_dir / run_id / "revisions" / revision
    directory.mkdir(parents=True)
    basis = {"schema_version": request.param, "basis_kind": "immutable_publication_revision",
             "run_id": run_id, "generated_at": "2026-09-07T20:45:00", "revision": revision}
    report = {"run_id": run_id, "generated_at": basis["generated_at"],
              "publication_control": {"commit_protocol": 1, "revision": revision}, "artifacts": {}}
    files = {}
    for key, filename in (
        ("project_tracker_excel", "tracker.xlsx"),
        ("project_tracker_current_detail_json", "tracker.json"),
        ("publication_basis_json", "basis.json"),
    ):
        contents = f"synthetic provenance fixture: {key}".encode()
        path = directory / filename
        path.write_bytes(contents)
        report["artifacts"][key] = str(path.relative_to(tmp_path))
        basis[key] = f"{len(contents)}:{hashlib.sha1(contents).hexdigest()}"
        files[filename] = hashlib.sha256(contents).hexdigest()
    report_path = directory / "report.json"
    report["artifacts"]["report_json"] = str(report_path.relative_to(tmp_path))
    report_path.write_text(json.dumps(report))
    files[report_path.name] = hashlib.sha256(report_path.read_bytes()).hexdigest()
    manifest_path = directory / "validated.json"
    manifest_path.write_text(json.dumps({"schema_version": 1, "run_id": run_id,
                                         "revision": revision, "files": files}))
    entry = {"report_path": report["artifacts"]["report_json"],
             "report_sha256": files[report_path.name], "revision": revision,
             "manifest_path": str(manifest_path.relative_to(tmp_path)),
             "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}
    head = {"schema_version": 1, "generation": 1, "current_published_run_id": run_id,
            "committed_runs": {run_id: entry}}
    (directory / "publication_state.json").write_text(json.dumps(head))
    (settings.reports_dir / ".publication_current").symlink_to(directory, target_is_directory=True)
    return settings, report, basis


def summarize(fixture, basis=None):
    settings, report, original = fixture
    return audit._summarize_recommendations_cache_basis_contract(
        original if basis is None else basis, settings=settings, report=report, artifact_key="detail",
    )


def test_frozen_provenance_requires_real_committed_hashes_not_mutable_database(frozen_publication):
    settings, report, _ = frozen_publication
    before = {path: path.read_bytes() for path in settings.reports_dir.rglob("*") if path.is_file()}
    summary, blockers = summarize(frozen_publication)
    assert blockers == []
    assert "database_file" not in summary["required_keys"]
    assert "latest_report_file" not in summary["required_keys"]
    assert summary["publication_evidence"]["revision"] == report["publication_control"]["revision"]
    assert all(item["cache_token_matches"] and item["committed_sha256_matches"]
               for item in summary["frozen_artifacts"].values())
    assert before == {path: path.read_bytes() for path in before}


@pytest.mark.parametrize("key,value,reason", [
    ("revision", "b" * 32, "revision_mismatch"),
    ("run_id", "other_run", "run_id_mismatch"),
    ("generated_at", "2026-09-07T21:45:00", "generated_at_mismatch"),
    ("basis_kind", None, "basis_kind_mismatch"),
    ("schema_version", 99, "schema_version_mismatch"),
    ("project_tracker_excel", "missing", "project_tracker_excel_fingerprint_mismatch"),
    ("project_tracker_current_detail_json", "123:incorrect", "project_tracker_current_detail_json_fingerprint_mismatch"),
    ("publication_basis_json", None, "publication_basis_json_fingerprint_mismatch"),
])
def test_frozen_basis_rejects_incomplete_or_wrong_provenance(frozen_publication, key, value, reason):
    basis = deepcopy(frozen_publication[2])
    basis[key] = value
    _, blockers = summarize(frozen_publication, basis)
    assert "detail_cache_basis_" + reason in blockers


@pytest.mark.parametrize("change", ["alter_file", "remove_file", "remove_head", "alter_manifest", "wrong_report"])
def test_self_consistent_tokens_cannot_bypass_committed_publication(frozen_publication, change):
    settings, report, basis = frozen_publication
    path = settings.project_root / report["artifacts"]["publication_basis_json"]
    if change == "alter_file":
        path.write_text("changed after publication")
        contents = path.read_bytes()
        basis["publication_basis_json"] = f"{len(contents)}:{hashlib.sha1(contents).hexdigest()}"
    elif change == "remove_file":
        path.unlink()
    elif change == "remove_head":
        (settings.reports_dir / ".publication_current").unlink()
    elif change == "alter_manifest":
        (path.parent / "validated.json").write_text("{}")
    else:
        report["generated_at"] = basis["generated_at"] = "2026-09-07T22:00:00"
    _, blockers = summarize(frozen_publication)
    assert "detail_cache_basis_publication_evidence_invalid" in blockers


def test_legacy_contract_stays_required_and_cannot_self_declare_frozen(tmp_path):
    report = {"run_id": "legacy"}
    settings = SimpleNamespace(project_root=tmp_path)
    basis = {"schema_version": 2, "run_id": "legacy", "generated_at": "2026-06-05",
             "latest_report_file": "legacy-report-token", "database_file": "legacy-db-token",
             "project_tracker_excel": "legacy-workbook-stat-token"}
    _, blockers = audit._summarize_recommendations_cache_basis_contract(
        basis, settings=settings, report=report, artifact_key="legacy",
    )
    assert blockers == []
    basis.pop("database_file")
    basis["basis_kind"] = "immutable_publication_revision"
    _, blockers = audit._summarize_recommendations_cache_basis_contract(
        basis, settings=settings, report=report, artifact_key="legacy",
    )
    assert "legacy_cache_basis_required_keys_missing:database_file" in blockers
    assert "legacy_cache_basis_unexpected_basis_kind" in blockers
