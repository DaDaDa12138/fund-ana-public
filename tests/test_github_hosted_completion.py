"""Synthetic publication, rejection proof and date-boundary tests; isolated only."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import hashlib
import sqlite3
from types import SimpleNamespace
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_github_reports import export
from backend.github_reports import ReportExportError, sha256, write_json
from backend.github_report_view import report_view
from backend.github_task_validation import hosted_task_outcome
from backend.shanghai_time import now_shanghai, display_shanghai
from scripts.check_github_daily_completion import completed_daily
from scripts import check_github_daily_completion as completion
from scripts.sync_github_reports import merge_export
from core.utils.period_return_snapshot import RETURN_FIELDS, RETURN_PROVENANCE_FIELDS, RETURN_REJECTION_FIELDS


def warning_fixture(tmp_path):
    output, bundle, check, source = export(tmp_path)
    from core.utils.period_return_snapshot import build_period_snapshot
    from core.database.models import FundReturns
    from sqlalchemy import create_engine
    db = SimpleNamespace(db_path=str(tmp_path / "synthetic.sqlite"))
    engine = create_engine("sqlite:///" + db.db_path)
    FundReturns.__table__.create(engine)
    row = build_period_snapshot({"return_1y": 0}, as_of_date="2026-01-01",
                               observed_at=datetime(2026, 1, 2), source="fundMNRank",
                               source_payload={"synthetic": True, "missing": None, "zero": 0})
    response = {"Success": True, "ErrCode": 0, "Datas": [
        {"title": title, "syl": "0" if title == "LN" else ""}
        for title in ("Z", "Y", "3Y", "6Y", "1N", "2N", "3N", "5N", "JN", "LN")],
        "Expansion": {"TIME": "2020-01-01", "ESTABDATE": "2020-01-01", "ISUPDATING": False}}
    proof = {"contract": "period-initialization-rejection-v1", "fcode": "000002",
             "observed_at": "2026-01-02T00:00:00", "nav_date": "2026-01-01", "nav_value": 1,
             "estab_date": "2020-01-01", "accepted_source_sha256": row["period_return_source_sha256"],
             "response": response}
    raw = json.dumps(proof); digest = hashlib.sha256(raw.encode()).hexdigest()
    row.update(fcode="000002", latest_nav_date="2026-01-01", latest_nav=1,
               period_return_rejection_json=raw, period_return_rejection_sha256=digest)
    row = {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in row.items()}
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("INSERT INTO fund_returns (" + ",".join(row) + ") VALUES (" + ",".join("?" for _ in row) + ")", list(row.values()))
    summary = {"schema_version": 2, "target_count": 1, "saved_fund_count": 0, "failed_count": 1,
               "cooldown_count": 0, "unselected_count": 0, "pending_count": 1,
               "blocking_count": 0, "screening_blocked": False, "cooldown_outcomes": [],
               "nonblocking_missing_fcodes": [], "initialization_placeholder_fcodes": ["000002"],
               "outcomes": [{"fcode": "000002", "status": "initialization_placeholder_quarantined",
                             "initialization_placeholder_quarantined": True, "rejection_sha256": digest}]}
    after = {k: bundle["publication"][k] for k in ("run_id", "revision", "generation")}
    before = {"run_id": "before", "revision": "old", "generation": 2}
    result = {"status": "degraded", "task_terminal": True, "background_continues": False,
              "warning_reason": "period_return_initialization_placeholder_quarantined",
              "degraded_stage": "collect-period-returns", "screening_triggered": True,
              "screening_run_id": after["run_id"], "period_return_collection": summary,
              "publication_result": {"status": "published", "run_id": after["run_id"],
                  "current_published_run_id": after["run_id"], "publishable": True,
                  "publish_hold": False, "legacy_publish_hold": False,
                  "publish_governance_summary": {"publishable": True, "risk_publish_ready": True,
                      "publish_blocked": False, "blocking_total": 0}, "publish_blockers": []}}
    return output, bundle, check, db, result, before, after


def test_warning_completion_requires_full_proof_and_preserves_raw_degraded(tmp_path):
    output, bundle, check, db, result, before, after = warning_fixture(tmp_path)
    unchanged = deepcopy(result)
    outcome, proof = hosted_task_outcome(result, 0, before, after, output, Path(db.db_path))
    assert outcome == "published_with_warnings" and proof["excluded_fcodes"] == ["000002"]
    assert proof["formal_overlap_count"] == 0 and result == unchanged
    repository = tmp_path / "repo"; repository.mkdir()
    check.update(status=outcome, task_status="degraded", nonblocking_validation=proof,
                 task_warning_reason=result["warning_reason"], publication_sync_status="verified_current")
    assert merge_export(repository, check, output)["status"] == outcome
    assert "已隔离的数据警告" in (repository / "README.md").read_text()


def missing_returns_fixture(tmp_path, *, cooldown=False):
    values = warning_fixture(tmp_path)
    output, bundle, check, db, result, before, after = values
    fields = (*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, *RETURN_REJECTION_FIELDS)
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("UPDATE fund_returns SET " + ",".join(field + "=NULL" for field in fields)
                     + ",period_return_last_status='missing' WHERE fcode='000002'")
    outcome = {"fcode": "000002", "status": "missing", "unavailable_without_returns": True}
    summary = result["period_return_collection"]
    summary.update(initialization_placeholder_fcodes=[], nonblocking_missing_fcodes=["000002"],
                   outcomes=[] if cooldown else [outcome], cooldown_outcomes=[outcome] if cooldown else [],
                   target_count=0 if cooldown else 1, failed_count=0 if cooldown else 1,
                   cooldown_count=1 if cooldown else 0)
    result["warning_reason"] = "period_return_unavailable_without_returns"
    return values


@pytest.mark.parametrize("cooldown", [False, True])
def test_empty_returns_exclusion_requires_database_proof_and_preserves_degradation(tmp_path, cooldown):
    output, bundle, check, db, result, before, after = missing_returns_fixture(tmp_path, cooldown=cooldown)
    original = deepcopy(result)
    status, proof = hosted_task_outcome(result, 0, before, after, output, Path(db.db_path))
    assert status == "published_with_warnings" and result == original
    assert proof["excluded_fcodes"] == ["000002"] and proof["rejection_proofs"] == []
    assert proof["missing_returns_proofs"] == [{"fcode": "000002", "status": "missing",
                                               "returns_and_provenance": "all_null"}]
    check.update(status=status, task_status="degraded", nonblocking_validation=proof,
                 task_warning_reason=result["warning_reason"], publication_sync_status="verified_current")
    repository = tmp_path / "repository"; repository.mkdir()
    merge_export(repository, check, output)
    assert "已隔离的数据警告" in (repository / "README.md").read_text()


@pytest.mark.parametrize("field", [*RETURN_FIELDS, *RETURN_PROVENANCE_FIELDS, *RETURN_REJECTION_FIELDS])
def test_partial_or_real_zero_returns_never_qualify_as_empty(tmp_path, field):
    output, _, _, db, result, before, after = missing_returns_fixture(tmp_path)
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("UPDATE fund_returns SET " + field + "=? WHERE fcode='000002'",
                     (0 if field in RETURN_FIELDS else "unverified",))
    with pytest.raises(ReportExportError, match="missing_returns_exclusion_not_verified"):
        hosted_task_outcome(result, 0, before, after, output, Path(db.db_path))


@pytest.mark.parametrize("fault", ["transport", "database_status", "missing_row", "reason",
                                    "overlap", "counts", "blocked", "error"])
def test_missing_returns_does_not_mask_failures_or_inclusion(tmp_path, fault):
    output, bundle, _, db, result, before, after = missing_returns_fixture(tmp_path)
    if fault == "transport":
        result["period_return_collection"]["outcomes"][0]["status"] = "request_failed"
    elif fault == "database_status":
        with sqlite3.connect(db.db_path) as conn:
            conn.execute("UPDATE fund_returns SET period_return_last_status='request_failed'")
    elif fault == "missing_row":
        with sqlite3.connect(db.db_path) as conn: conn.execute("DELETE FROM fund_returns")
    elif fault == "reason": result["warning_reason"] = "period_return_initialization_placeholder_quarantined"
    elif fault == "overlap":
        csv_path = output / "formal.csv"
        csv_path.write_text(csv_path.read_text(encoding="utf-8-sig").replace("000001", "000002"), encoding="utf-8-sig")
        entry = next(x for x in bundle["files"] if x["path"] == "formal.csv")
        entry.update(sha256=sha256(csv_path), bytes=csv_path.stat().st_size)
        write_json(output / "bundle.json", bundle)
    elif fault == "counts": result["period_return_collection"]["saved_fund_count"] = 1
    elif fault == "blocked": result["publication_result"]["publish_hold"] = True
    elif fault == "error": result["report_refresh_error"] = "failure"
    with pytest.raises(ReportExportError):
        hosted_task_outcome(result, 0, before, after, output, Path(db.db_path))


def test_empty_returns_and_initializer_can_be_verified_together(tmp_path):
    output, _, _, db, result, before, after = warning_fixture(tmp_path)
    with sqlite3.connect(db.db_path) as conn:
        conn.execute("INSERT INTO fund_returns (fcode,period_return_last_status) VALUES ('000003','missing')")
    summary = result["period_return_collection"]
    summary.update(target_count=2, failed_count=2, pending_count=2, nonblocking_missing_fcodes=["000003"])
    summary["outcomes"].append({"fcode": "000003", "status": "missing", "unavailable_without_returns": True})
    status, proof = hosted_task_outcome(result, 0, before, after, output, Path(db.db_path))
    assert status == "published_with_warnings" and proof["excluded_fcodes"] == ["000002", "000003"]


@pytest.mark.parametrize("fault", ["absent", "status", "null_state", "code", "duplicate", "reason"])
def test_report_sync_rejects_unbound_missing_return_proofs(tmp_path, fault):
    output, _, check, db, result, before, after = missing_returns_fixture(tmp_path)
    status, proof = hosted_task_outcome(result, 0, before, after, output, Path(db.db_path))
    check.update(status=status, task_status="degraded", nonblocking_validation=proof,
                 task_warning_reason=result["warning_reason"], publication_sync_status="verified_current")
    if fault == "absent": proof["missing_returns_proofs"] = []
    elif fault == "status": proof["missing_returns_proofs"][0]["status"] = "request_failed"
    elif fault == "null_state": proof["missing_returns_proofs"][0]["returns_and_provenance"] = "unknown"
    elif fault == "code": proof["missing_returns_proofs"][0]["fcode"] = "000003"
    elif fault == "duplicate": proof["missing_returns_proofs"] *= 2
    elif fault == "reason": check["task_warning_reason"] = "api_failed"
    repository = tmp_path / "repository"; repository.mkdir()
    with pytest.raises(ReportExportError, match="warning_check_proof_missing"):
        merge_export(repository, check, output)


def test_empty_returns_unchanged_publication_requires_matching_ready_snapshot(tmp_path):
    output, _, _, db, result, before, after = missing_returns_fixture(tmp_path, cooldown=True)
    snapshot = {"candidate_date": "2026-01-01", "signature": "same", "ready": True}
    result.update(screening_triggered=False, report_refresh_triggered=False, collection_skipped=False,
                  skip_reason="effective_nav_snapshot_already_screened", publication_result={},
                  after_effective_nav_snapshot=deepcopy(snapshot), latest_screened_snapshot=deepcopy(snapshot),
                  maintenance_data_change_summary={"changed": False}, lineage_rescreen_requirement={"required": False})
    status, proof = hosted_task_outcome(result, 0, after, after, output, Path(db.db_path))
    assert status == "unchanged_with_warnings" and proof["formal_overlap_count"] == 0
    result["maintenance_data_change_summary"]["changed"] = True
    with pytest.raises(ReportExportError):
        hosted_task_outcome(result, 0, after, after, output, Path(db.db_path))


@pytest.mark.parametrize("fault", ["return_code", "terminal", "background", "reason", "stage", "no_screen",
    "run_binding", "unchanged", "maintenance_error", "nav_failure", "blocked", "risk", "missing_proof",
    "blocking", "unbalanced", "unknown_exemption", "corrupt_database_proof", "overlap", "corrupt_csv"])
def test_unverified_or_additional_failures_remain_failures(tmp_path, fault):
    output, bundle, check, db, result, before, after = warning_fixture(tmp_path)
    code = 0
    if fault == "return_code": code = 1
    elif fault == "terminal": result["task_terminal"] = False
    elif fault == "background": result["background_continues"] = True
    elif fault == "reason": result["warning_reason"] = "api_failed"
    elif fault == "stage": result["degraded_stage"] = "collect-nav"
    elif fault == "no_screen": result["screening_triggered"] = False
    elif fault == "run_binding": result["screening_run_id"] = "other"
    elif fault == "unchanged": before = after
    elif fault == "maintenance_error": result["report_refresh_error"] = "failed"
    elif fault == "nav_failure": result["records_failed"] = 1
    elif fault == "blocked": result["publication_result"]["publish_blockers"] = [{"blocking": True}]
    elif fault == "risk": result["publication_result"]["publish_governance_summary"]["risk_publish_ready"] = False
    elif fault == "missing_proof": result["period_return_collection"]["outcomes"][0].pop("rejection_sha256")
    elif fault == "blocking": result["period_return_collection"]["blocking_count"] = 1
    elif fault == "unbalanced": result["period_return_collection"]["saved_fund_count"] += 1
    elif fault == "unknown_exemption": result["period_return_collection"]["initialization_placeholder_fcodes"] = []
    elif fault == "corrupt_database_proof":
        with sqlite3.connect(db.db_path) as conn:
            conn.execute("UPDATE fund_returns SET period_return_rejection_sha256=? WHERE fcode='000002'", ("0" * 64,))
    elif fault == "overlap":
        csv_path = output / "formal.csv"
        csv_path.write_text(csv_path.read_text(encoding="utf-8-sig").replace("000001", "000002"), encoding="utf-8-sig")
        entry = next(x for x in bundle["files"] if x["path"] == "formal.csv")
        entry.update(sha256=sha256(csv_path), bytes=csv_path.stat().st_size)
        write_json(output / "bundle.json", bundle)
    elif fault == "corrupt_csv": (output / "formal.csv").write_text("corrupt")
    with pytest.raises(ReportExportError):
        hosted_task_outcome(result, code, before, after, output, Path(db.db_path))


def test_shanghai_offset_and_legacy_time_display():
    assert now_shanghai().utcoffset().total_seconds() == 28800
    assert display_shanghai("2026-11-08T23:47:00Z") == "2026-11-09 07:47:00 +08:00"
    assert display_shanghai("2026-11-09T07:47:00") == "2026-11-09 07:47:00 +08:00"


def test_reading_view_preserves_null_zero_code_and_escapes_markdown(tmp_path):
    output, bundle, check, _ = export(tmp_path)
    text = (output / "formal.csv").read_text(encoding="utf-8-sig").replace("synthetic fixture", "fixture|<script>bad</script>")
    view = report_view(bundle, text, "reports/2026-01-02/synthetic_run/revision_one")
    assert "000001" in view and "| 0 | — | 0 |" in view
    assert "fixture&#124;&lt;script&gt;bad&lt;/script&gt;" in view
    assert "../../reports/2026-01-02/" in view
    assert "+08:00" in view
    with pytest.raises(ReportExportError, match="count_mismatch"):
        report_view(bundle, text + text.splitlines()[-1] + "\n", "reports/x")


def test_catch_up_uses_completed_daily_after_shanghai_boundary_only():
    valid = {"schema": "fund-ana-github-check-v1", "job": "daily_refresh", "executor": "github_hosted",
             "collection_requested": True, "status": "published", "task_status": "success",
             "publication_sync_status": "verified_current", "cloud_state_commit": "a" * 40,
             "checked_at": "2026-11-08T23:47:00Z", "finished_at": "2026-11-09T01:10:00Z", "github_run_id": "123"}
    now = datetime(2026, 11, 9, 2, 47, tzinfo=timezone.utc)
    assert completed_daily([valid], now) == "123"
    for changes in ({"job": "publish_only"}, {"collection_requested": False}, {"status": "failed"},
                    {"task_status": "degraded"}, {"checked_at": "2026-11-08T20:00:00Z"},
                    {"finished_at": "2026-11-10T01:10:00Z"}, {"cloud_state_commit": None}):
        assert completed_daily([{**valid, **changes}], now) is None


@pytest.mark.parametrize("event,schedule,should_query", [
    ("schedule", "47 23 * * *", True),
    ("schedule", "47 2 * * *", True),
    ("schedule", "17 19 * * 6", False),
    ("workflow_dispatch", "47 2 * * *", False),
    ("schedule", "unknown", False),
])
def test_utc_schedule_events_use_shanghai_day_for_completion_guard(
        tmp_path, monkeypatch, event, schedule, should_query):
    # 02:47 UTC is 10:47 Shanghai. The receipt's UTC start belongs to the
    # previous UTC day but to the same Shanghai business day as the catch-up.
    now = datetime(2026, 11, 10, 2, 47, tzinfo=timezone.utc)
    receipt = {"schema": "fund-ana-github-check-v1", "job": "daily_refresh", "executor": "github_hosted",
               "collection_requested": True, "status": "published", "task_status": "success",
               "publication_sync_status": "verified_current", "cloud_state_commit": "a" * 40,
               "checked_at": "2026-11-09T23:48:00Z", "finished_at": "2026-11-10T01:20:00Z",
               "github_run_id": "123"}
    calls = []
    def fake_run(command, **kwargs):
        calls.append(command)
        path = command[-1]
        if "checks/2026-11-10?" in path:
            data = [{"type": "file", "name": "123-1.json", "path": "checks/2026-11-10/123-1.json"}]
        else:
            assert "checks/2026-11-10/123-1.json?" in path
            data = receipt
        return SimpleNamespace(returncode=0, stdout=json.dumps(data), stderr="")
    monkeypatch.setattr(completion.subprocess, "run", fake_run)
    monkeypatch.setattr(completion, "now_shanghai", lambda: now)
    output = tmp_path / "github-output"
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    monkeypatch.setenv("GITHUB_EVENT_SCHEDULE", schedule)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    completion.main()
    assert bool(calls) is should_query
    assert output.read_text() == ("run=false\n" if should_query else "run=true\n")


@pytest.mark.parametrize("return_code,stderr", [(0, ""), (1, "HTTP 503")])
def test_utc_catch_up_keeps_real_collection_when_receipts_absent_or_unavailable(
        tmp_path, monkeypatch, return_code, stderr):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("GITHUB_EVENT_SCHEDULE", "47 2 * * *")
    output = tmp_path / "github-output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr(completion.subprocess, "run", lambda *args, **kwargs:
                        SimpleNamespace(returncode=return_code, stdout="[]", stderr=stderr))
    completion.main()
    assert output.read_text() == "run=true\n"


@pytest.mark.parametrize("fault", [None, "signature", "not_ready", "maintenance", "lineage", "collection_skipped", "wrong_date"])
def test_unchanged_warning_requires_ready_matching_snapshot_and_complete_maintenance(tmp_path, fault):
    output, bundle, check, db, result, before, after = warning_fixture(tmp_path)
    snapshot = {"candidate_date": "2026-01-01", "signature": "synthetic_exact_signature", "ready": True}
    result.update(screening_triggered=False, report_refresh_triggered=False, collection_skipped=False,
                  skip_reason="effective_nav_snapshot_already_screened", publication_result={},
                  after_effective_nav_snapshot=deepcopy(snapshot), latest_screened_snapshot=deepcopy(snapshot),
                  maintenance_data_change_summary={"changed": False}, lineage_rescreen_requirement={"required": False})
    if fault == "signature": result["after_effective_nav_snapshot"]["signature"] = "changed"
    elif fault == "not_ready": result["after_effective_nav_snapshot"]["ready"] = False
    elif fault == "maintenance": result["maintenance_data_change_summary"]["changed"] = True
    elif fault == "lineage": result["lineage_rescreen_requirement"]["required"] = True
    elif fault == "collection_skipped": result["collection_skipped"] = True
    elif fault == "wrong_date":
        for key in ("after_effective_nav_snapshot", "latest_screened_snapshot"):
            result[key]["candidate_date"] = "2026-01-02"
    if fault:
        with pytest.raises(ReportExportError):
            hosted_task_outcome(result, 0, after, after, output, Path(db.db_path))
    else:
        status, proof = hosted_task_outcome(result, 0, after, after, output, Path(db.db_path))
        assert status == "unchanged_with_warnings" and proof["formal_overlap_count"] == 0
