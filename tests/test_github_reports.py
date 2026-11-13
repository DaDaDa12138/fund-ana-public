"""Synthetic publication/export I/O only; no real fund data or network."""
from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from openpyxl import Workbook

from backend.github_reports import (
    ReportExportError, export_current, parse_scheduler_output, sha256, task_outcome,
    verify_bundle, write_json,
)
from scripts.sync_github_reports import merge_export
from scripts.run_github_daily_job import cleanup_generated_outputs


def publication(tmp_path: Path):
    root = tmp_path / "source"
    directory = root / "reports/runs/synthetic_run/revisions/revision_one"
    directory.mkdir(parents=True)
    artifacts = {}
    for role, name in (("filtered_excel", "formal.xlsx"), ("filtered_csv", "formal.csv"),
                       ("report_markdown", "report.md"), ("project_tracker_excel", "tracker.xlsx")):
        (directory / name).write_bytes(b"synthetic export fixture; NULL != 0; 000001\n")
        artifacts[role] = (directory / name).relative_to(root).as_posix()
    (directory / "formal.csv").write_text(
        "基金代码,基金名称,正式榜分组,正式投资评分,近1年(%),1年-最大回撤(%),持有期状态\n"
        "000001,synthetic fixture,纯债稳健榜,0,,0,synthetic status\n", encoding="utf-8-sig")
    workbook = Workbook()
    current = workbook.active
    current.title = "当前在榜"
    current.append(["当前在榜数", 1, "最新净值日期", "2026-01-01"])
    current.append(["synthetic section labels"])
    current.append(["基金代码", "基金名称", "正式榜分组", "正式投资评分", "出现次数", "出现频率(%)",
                    "首次出现日期", "最后出现日期", "当前是否在榜", "近1月(%)", "近3月(%)", "近1年(%)", "持有期状态"])
    current.append(["000001", "synthetic fixture", "纯债稳健榜", "0", "1", "100.00", "2026-01-01",
                    "2026-01-01", "是", "0", None, None, "synthetic status"])
    historical = workbook.create_sheet("完整数据")
    historical.append(["synthetic exited fixture, excluded from current Markdown"])
    workbook.save(directory / "tracker.xlsx")
    workbook.close()
    (directory / "not-for-export.txt").write_text("synthetic private diagnostic")
    report = {"run_id": "synthetic_run", "generated_at": "2026-01-02T09:00:00",
              "counts": {"final_count": 1, "stable_bond_final_count": 1, "enhanced_bond_final_count": 0},
              "publication_control": {"mode": "formal"},
              "database_freshness": {"effective_nav_snapshot": {"candidate_date": "2026-01-01"}},
              "artifacts": artifacts}
    report_file = directory / "report.json"
    write_json(report_file, report)
    manifest = directory / "validated.json"
    write_json(manifest, {"schema_version": 1, "run_id": "synthetic_run", "revision": directory.name,
                          "files": {p.name: sha256(p) for p in directory.iterdir() if p.is_file()}})
    entry = {"run_id": "synthetic_run", "revision": directory.name,
             "report_path": report_file.relative_to(root).as_posix(), "report_sha256": sha256(report_file),
             "manifest_path": manifest.relative_to(root).as_posix(), "manifest_sha256": sha256(manifest)}
    write_json(directory / "publication_state.json", {"schema_version": 1, "generation": 3,
               "current_published_run_id": "synthetic_run", "committed_at": "2026-01-02T09:01:00",
               "committed_runs": {"synthetic_run": entry}})
    (root / "reports/.publication_current").symlink_to(directory, target_is_directory=True)
    settings = SimpleNamespace(project_root=root, reports_dir=root / "reports")
    return settings, directory


def export(tmp_path):
    settings, source = publication(tmp_path)
    output = tmp_path / "bundle"
    value = export_current(settings, output)
    check = {"schema": "fund-ana-github-check-v1", "github_run_id": "12345", "attempt": "1",
             "job": "publish_only", "status": "verified_existing_publication", "check_date": "2026-01-02",
             "publication": value["publication"], "checked_at": "2026-01-02T10:00:00Z"}
    return output, value, check, source


def test_export_uses_committed_head_and_only_selected_bytes(tmp_path):
    output, bundle, _, source = export(tmp_path)
    assert verify_bundle(output) == bundle
    assert (output / "formal.csv").read_bytes() == (source / "formal.csv").read_bytes()
    assert not (output / "not-for-export.txt").exists()
    assert not (output / "report.json").exists()
    assert bundle["publication"]["generation"] == 3
    assert "forward_validation_excel" in bundle["omitted_optional_roles"]
    assert bundle["scope"].endswith("not_a_complete_runtime_backup")


def test_corrupt_committed_input_fails_closed_and_removes_partial_export(tmp_path):
    from backend.publication_transaction import PublicationIntegrityError
    settings, source = publication(tmp_path)
    (source / "formal.csv").write_text("changed")
    with pytest.raises(PublicationIntegrityError):
        export_current(settings, tmp_path / "bundle")
    assert not (tmp_path / "bundle").exists()


def test_uncommitted_candidate_cannot_be_exported(tmp_path):
    settings, source = publication(tmp_path)
    (settings.reports_dir / ".publication_current").unlink()
    with pytest.raises(ReportExportError, match="no_committed"):
        export_current(settings, tmp_path / "bundle")


@pytest.mark.parametrize("fault", ["tamper", "extra", "symlink", "path_escape"])
def test_bundle_rejects_corruption_and_unlisted_content(tmp_path, fault):
    output, value, _, _ = export(tmp_path)
    if fault == "tamper":
        (output / "formal.csv").write_text("bad")
    elif fault == "extra":
        (output / "database.db").write_text("must not upload")
    elif fault == "symlink":
        (output / "formal.csv").unlink()
        (output / "formal.csv").symlink_to(output / "report.md")
    else:
        value["files"][0]["path"] = "../outside"
        write_json(output / "bundle.json", value)
    with pytest.raises(ReportExportError):
        verify_bundle(output)


def test_report_branch_is_idempotent_and_failed_check_preserves_latest(tmp_path):
    output, bundle, check, _ = export(tmp_path)
    repository = tmp_path / "repository"
    repository.mkdir()
    merge_export(repository, check, output)
    before = {p.relative_to(repository): p.read_bytes() for p in repository.rglob("*") if p.is_file()}
    merge_export(repository, check, output)
    assert before == {p.relative_to(repository): p.read_bytes() for p in repository.rglob("*") if p.is_file()}
    failure = {**check, "github_run_id": "12346", "status": "failed"}
    failure.pop("publication")
    merge_export(repository, failure, None)
    assert (repository / "latest.json").read_bytes() == before[Path("latest.json")]
    assert len(list(repository.glob("reports/*/*/*/bundle.json"))) == 1
    assert (repository / "checks/2026-01-02/12346-1.json").exists()


def test_report_branch_rejects_failed_success_and_immutable_conflicts(tmp_path):
    output, bundle, check, _ = export(tmp_path)
    repository = tmp_path / "repository"
    repository.mkdir()
    merge_export(repository, check, output)
    with pytest.raises(ReportExportError, match="failed_check"):
        merge_export(repository, {**check, "status": "failed"}, output)
    other = deepcopy(bundle)
    other["publication"]["counts"]["final_count"] = 2
    write_json(output / "bundle.json", other)
    with pytest.raises(ReportExportError, match="immutable_report"):
        merge_export(repository, {**check, "publication": other["publication"]}, output)


def test_source_branch_layout_is_not_a_report_branch(tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / "backend").mkdir()
    check = {"schema": "fund-ana-github-check-v1", "status": "failed", "check_date": "2026-01-02",
             "github_run_id": "1", "attempt": "1"}
    with pytest.raises(ReportExportError, match="unexpected_content"):
        merge_export(repository, check, None)


def test_degraded_task_can_mirror_verified_current_but_stays_failed(tmp_path):
    output, bundle, check, _ = export(tmp_path)
    repository = tmp_path / "repository"
    repository.mkdir()
    failed = {**check, "status": "failed", "publication_sync_status": "verified_current",
              "task_status": "degraded", "error_code": "scheduler_not_successful",
              "task_warning_reason": "period_return_initialization_placeholder_quarantined"}
    outcome = merge_export(repository, failed, output)
    assert outcome["status"] == "failed"
    assert json.loads((repository / "latest.json").read_text())["publication"] == bundle["publication"]
    receipt = json.loads((repository / "checks/2026-01-02/12345-1.json").read_text())
    assert receipt["status"] == "failed" and receipt["task_status"] == "degraded"
    assert receipt["publication"]["committed_at"] == "2026-01-02T09:01:00"
    readme = (repository / "README.md").read_text()
    assert "| 任务原始状态 | degraded |" in readme
    assert "period_return_initialization_placeholder_quarantined" in readme
    (output / "formal.csv").write_text("corrupt")
    with pytest.raises(ReportExportError):
        merge_export(repository, {**failed, "github_run_id": "12346"}, output)


def test_scheduler_parser_only_accepts_terminal_json():
    assert parse_scheduler_output('log\n{"status":"running"}\n{"status":"success"}\n')["status"] == "success"
    with pytest.raises(ReportExportError):
        parse_scheduler_output('{"status":"success"}\nprocess failed afterwards')


@pytest.mark.parametrize("state", ["failed", "degraded", "warning", "accepted", "running", "timeout"])
def test_bad_scheduler_states_do_not_publish_old_report_as_success(state):
    with pytest.raises(ReportExportError):
        task_outcome({"status": state}, 0, {"run_id": "old"}, {"run_id": "old"})


def test_scheduler_busy_blocked_and_unbound_workflow_are_not_success():
    before = {"run_id": "old", "revision": "first"}
    after = {"run_id": "new", "revision": "second"}
    with pytest.raises(ReportExportError):
        task_outcome({"status": "skipped", "skip_reason": "workflow_busy_locked"}, 0, before, before)
    with pytest.raises(ReportExportError):
        task_outcome({"status": "success", "result": {"screening_triggered": True, "run_id": "other"}},
                     0, before, after)
    with pytest.raises(ReportExportError):
        task_outcome({"status": "success", "publication_result": {"status": "blocked"}}, 0, before, before)
    assert task_outcome({"status": "success", "result": {"screening_triggered": True, "run_id": "new"}},
                        0, before, after) == "published"
    assert task_outcome({"status": "success", "skip_reason": "current_snapshot_already_processed"},
                        0, before, before) == "unchanged"
    with pytest.raises(ReportExportError, match="skip_reason_unverified"):
        task_outcome({"status": "skipped", "skip_reason": "unknown"}, 0, before, before)


def test_automation_retention_only_removes_named_old_generated_files(tmp_path):
    base, logs = tmp_path / "exports", tmp_path / "logs"
    base.mkdir(); logs.mkdir()
    old = base / "123-1"; old.mkdir(); (old / "test.txt").write_text("synthetic")
    protected = base / "unrelated"; protected.mkdir()
    recent = base / "124-1"; recent.mkdir()
    linked = base / "125-1"; linked.symlink_to(protected, target_is_directory=True)
    log = logs / "123-1.log"; log.write_text("synthetic")
    other = logs / "unrelated.log"; other.write_text("synthetic")
    now = 30 * 86400
    for path in (old, protected, log, other):os.utime(path, (0, 0))
    os.utime(recent, (now, now))
    cleanup_generated_outputs(base, logs, now)
    assert not old.exists() and not log.exists()
    assert protected.exists() and recent.exists() and linked.is_symlink() and other.exists()
