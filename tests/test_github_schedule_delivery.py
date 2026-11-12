"""Schedule conversion, duplicate protection and truthful timing evidence; no network."""
from datetime import datetime
import json
from types import SimpleNamespace

import pytest

from scripts import check_github_daily_completion as completion


@pytest.fixture(autouse=True)
def isolated_github_environment(monkeypatch):
    for name in ("GITHUB_RUN_ID", "GITHUB_STEP_SUMMARY", "SCHEDULE_CHECK_ONLY"):
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("cron,created,expected", [
    ("47 23 * * *", "2026-11-10T23:48:00+00:00", "2026-11-11T07:47:00+08:00"),
    ("47 2 * * *", "2026-11-11T02:48:00+00:00", "2026-11-11T10:47:00+08:00"),
    ("17 0,1,4 * * *", "2026-11-11T00:18:00+00:00", "2026-11-11T08:17:00+08:00"),
    ("17 0,1,4 * * *", "2026-11-11T01:18:00+00:00", "2026-11-11T09:17:00+08:00"),
    ("17 0,1,4 * * *", "2026-11-11T04:18:00+00:00", "2026-11-11T12:17:00+08:00"),
    ("17 19 * * 6", "2026-11-14T19:18:00+00:00", "2026-11-15T03:17:00+08:00"),
    ("47 23 * * *", "2026-12-31T23:48:00+00:00", "2027-01-01T07:47:00+08:00"),
])
def test_utc_schedule_slots_keep_shanghai_time_and_cross_day(cron, created, expected):
    instant = datetime.fromisoformat(created)
    result = completion.schedule_observation(cron, instant, instant)
    assert result["expected_at"] == expected
    assert result["delivery_delay_seconds"] == 60
    assert result["pre_guard_seconds"] == 0


def test_observed_incident_delay_precedes_runner_execution():
    result = completion.schedule_observation("47 2 * * *",
        datetime.fromisoformat("2026-11-10T18:27:40+08:00"),
        datetime.fromisoformat("2026-11-10T10:27:34+00:00"))
    assert result["expected_at"] == "2026-11-10T10:47:00+08:00"
    assert result["delivery_delay_seconds"] == 27634
    assert result["pre_guard_seconds"] == 6
    assert result["expected_at_basis"] == "latest_matching_slot_not_original_event_timestamp"


def test_missing_creation_time_stays_unknown_not_zero():
    now = datetime.fromisoformat("2026-11-11T08:17:00+08:00")
    result = completion.schedule_observation("17 0,1,4 * * *", now, None)
    assert result["delivery_delay_seconds"] is result["pre_guard_seconds"] is None
    assert result["expected_at"] == now.isoformat()
    assert completion.schedule_observation("", now, now)["expected_at"] is None
    with pytest.raises(ValueError, match="requires_aware_time"):
        completion.schedule_observation("47 2 * * *", now.replace(tzinfo=None), now)


def receipt():
    return {"schema": "fund-ana-github-check-v1", "job": "daily_refresh", "executor": "github_hosted",
            "collection_requested": True, "status": "published", "task_status": "success",
            "publication_sync_status": "verified_current", "cloud_state_commit": "a" * 40,
            "checked_at": "2026-11-10T07:48:00+08:00", "finished_at": "2026-11-10T09:00:00+08:00",
            "github_run_id": "123"}


@pytest.mark.parametrize("has_receipt", [False, True])
def test_bounded_catch_up_skips_only_verified_completed_day(tmp_path, monkeypatch, capsys, has_receipt):
    now = datetime.fromisoformat("2026-11-10T12:17:00+08:00")
    def read(command, **kwargs):
        path = command[-1]
        if "checks/2026-11-10?" in path:
            data = [{"type": "file", "name": "123-1.json", "path": "checks/2026-11-10/123-1.json"}] if has_receipt else []
        else:
            assert "checks/2026-11-10/123-1.json?" in path
            data = receipt()
        return SimpleNamespace(returncode=0, stdout=json.dumps(data), stderr="")
    monkeypatch.setattr(completion.subprocess, "run", read)
    monkeypatch.setattr(completion, "now_shanghai", lambda: now)
    monkeypatch.setenv("GITHUB_EVENT_NAME", "schedule")
    monkeypatch.setenv("GITHUB_EVENT_SCHEDULE", "17 0,1,4 * * *")
    # A manual-only diagnostic flag cannot suppress real natural collection.
    monkeypatch.setenv("SCHEDULE_CHECK_ONLY", "true")
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    completion.main()
    assert output.read_text() == ("run=false\n" if has_receipt else "run=true\n")
    value = json.loads(capsys.readouterr().out)
    assert value["diagnostic_only"] is False
    assert value["verified_prior_daily_run"] == ("123" if has_receipt else None)


def test_check_only_cannot_collect_or_claim_publication(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_EVENT_SCHEDULE", "")
    monkeypatch.setenv("SCHEDULE_CHECK_ONLY", "true")
    monkeypatch.setattr(completion.subprocess, "run", lambda *a, **kw:
                        SimpleNamespace(returncode=0, stdout="[]", stderr=""))
    output, summary = tmp_path / "output", tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    completion.main()
    assert output.read_text() == "run=false\n"
    value = json.loads(capsys.readouterr().out)
    assert value["diagnostic_only"] and value["collection_required"]
    assert "未采集、未发布" in summary.read_text()
    assert "未知/不适用" in summary.read_text()


def test_manual_publish_only_has_no_invented_daily_collection_requirement(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_EVENT_SCHEDULE", "")
    def unexpected_read(*args, **kwargs):
        raise AssertionError("manual tasks must not be suppressed by a daily receipt")
    monkeypatch.setattr(completion.subprocess, "run", unexpected_read)
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    completion.main()
    assert output.read_text() == "run=true\n"
    assert json.loads(capsys.readouterr().out)["collection_required"] is None


@pytest.mark.parametrize("returncode,stdout", [(1, ""), (0, "bad"), (0, "2026-11-11T00:00:00")])
def test_timing_lookup_failure_preserves_unknown(monkeypatch, returncode, stdout):
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr(completion.subprocess, "run", lambda *a, **kw:
                        SimpleNamespace(returncode=returncode, stdout=stdout, stderr="unavailable"))
    assert completion.workflow_created_at() is None
