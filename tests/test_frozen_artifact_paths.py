"""Synthetic file fixtures exercise portable evidence binding, never fund values."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.artifact_paths import project_relative_artifact_path, resolve_project_artifact_path
from backend.annual_return_gate_shadow import load_incomplete_annual_shadow_terminal
from backend.frozen_strategy_audit import FROZEN_STRATEGY_CONTRACT, load_incomplete_score_shadow_terminal


@pytest.fixture(params=["annual", "score"])
def terminal_case(request, tmp_path):
    annual = request.param == "annual"
    namespace = "annual_return_gate_shadow" if annual else "score_dedup_shadow"
    path = tmp_path / "reports/audit" / namespace / f"{namespace}_fixture_now.json"
    path.parent.mkdir(parents=True)
    payload = {"schema_version": 2, "run_id": "fixture", "status": "scenario_inputs_incomplete",
               "passed": False, "scenario_status": "missing_tracker_history"}
    if not annual:
        payload.update(input_contract=FROZEN_STRATEGY_CONTRACT, baseline_verification={"baseline_verified": True},
                       membership_impact=None, shadow_replay={"status": "missing_tracker_history", "final_count": None,
                       "missing_tracker_history_codes": ["synthetic-missing-member"]})
    path.write_text(json.dumps(payload))
    key = "artifact" if annual else "artifact_path"
    terminal = {key: str(path), "artifact_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "run_id": "fixture", "status": payload["status"],
                "scenario_status": "missing_tracker_history", "passed": False}
    reader = load_incomplete_annual_shadow_terminal if annual else load_incomplete_score_shadow_terminal
    settings = SimpleNamespace(project_root=tmp_path, reports_dir=tmp_path / "reports")

    def read(*, run_id="fixture", return_code=2):
        return reader(settings, run_id=run_id, stdout_tail=[json.dumps(terminal)], return_code=return_code)

    return SimpleNamespace(root=tmp_path, path=path, payload=payload, terminal=terminal, key=key,
                           read=read, reader=reader, settings=settings)


@pytest.mark.parametrize("style", ["native", "relative", "docker"])
def test_terminal_accepts_only_bound_original_bytes_in_each_environment(terminal_case, style):
    case = terminal_case
    relative = case.path.relative_to(case.root).as_posix()
    case.terminal[case.key] = {"native": str(case.path), "relative": relative, "docker": "/app/" + relative}[style]
    original = case.path.read_bytes()
    result = case.read()
    assert result["status"] == "scenario_inputs_incomplete"
    assert result["scenario_status"] == "missing_tracker_history"
    assert Path(result["artifact"]) == case.path
    assert case.path.read_bytes() == original and case.terminal["passed"] is False


@pytest.mark.parametrize("damage", ["wrong_sha", "wrong_run", "wrong_payload_run", "wrong_directory",
    "traversal", "outside_file", "outside_symlink", "other_directory_symlink", "missing", "non_object"])
def test_valid_checksums_cannot_excuse_wrong_scope_or_identity(terminal_case, damage):
    case = terminal_case
    if damage == "wrong_sha":
        case.terminal["artifact_sha256"] = "0" * 64
    elif damage == "wrong_run":
        assert case.read(run_id="other") is None
        return
    elif damage in {"wrong_payload_run", "non_object"}:
        payload = {**case.payload, "run_id": "other"} if damage == "wrong_payload_run" else []
        case.path.write_text(json.dumps(payload))
        case.terminal["artifact_sha256"] = hashlib.sha256(case.path.read_bytes()).hexdigest()
    elif damage == "traversal":
        relative = case.path.relative_to(case.root).as_posix()
        case.terminal[case.key] = "/app/reports/../" + relative
    elif damage == "missing":
        case.path.unlink()
    else:
        outside = damage in {"outside_file", "outside_symlink"}
        directory = case.root.parent / (case.root.name + "_outside") if outside else case.root / "other"
        directory.mkdir()
        moved = directory / case.path.name
        case.path.rename(moved)
        if damage.endswith("symlink"):
            case.path.symlink_to(moved)
        else:
            case.terminal[case.key] = str(moved)
    assert case.read() is None


@pytest.mark.parametrize("value", [None, [], {}, 7, "", " ", ".", "../report.json",
                                    "reports/../report.json", "/app/../report.json", "file\x00.json"])
def test_invalid_paths_fail_before_any_file_read(tmp_path, value):
    with pytest.raises(ValueError):
        resolve_project_artifact_path(value, project_root=tmp_path)


def test_unknown_absolute_roots_are_not_searched_by_basename(tmp_path):
    expected = tmp_path / "reports/source.json"
    expected.parent.mkdir()
    expected.write_text("synthetic original")
    unknown = tmp_path.parent / (tmp_path.name + "_unknown") / "reports/source.json"
    unknown.parent.mkdir(parents=True)
    unknown.write_bytes(expected.read_bytes())
    with pytest.raises(ValueError, match="unknown_absolute_root"):
        resolve_project_artifact_path(unknown, project_root=tmp_path)
    assert not resolve_project_artifact_path("reports/other/source.json", project_root=tmp_path).exists()


def test_internal_report_alias_and_configured_root_alias_remain_readable(tmp_path):
    root = tmp_path / "project"
    target = root / "reports/run/revision/report.json"
    target.parent.mkdir(parents=True)
    target.write_text("synthetic original")
    (root / "reports/latest.json").symlink_to(target)
    alias = tmp_path / "project_alias"
    alias.symlink_to(root, target_is_directory=True)
    for value in ("reports/latest.json", alias / "reports/latest.json", "/app/reports/latest.json"):
        assert resolve_project_artifact_path(value, project_root=alias) == target
        assert project_relative_artifact_path(value, project_root=alias) == "reports/run/revision/report.json"


def test_internal_symlink_loop_is_a_rejected_path(tmp_path):
    (tmp_path / "a").symlink_to("b")
    (tmp_path / "b").symlink_to("a")
    with pytest.raises(ValueError, match="symlink_loop"):
        resolve_project_artifact_path("a", project_root=tmp_path)


def test_non_object_terminal_cannot_raise_or_explain_a_child_failure(terminal_case):
    case = terminal_case
    for value in ([], None, "synthetic non-object"):
        assert case.reader(case.settings, run_id="fixture", stdout_tail=[json.dumps(value)], return_code=2) is None
    assert case.read(return_code=1) is None
    assert case.read(return_code=0) is None
