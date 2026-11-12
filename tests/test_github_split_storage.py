"""Public execution never grants public access to operational state or logs."""
import base64
from datetime import datetime
import json
from pathlib import Path
import tarfile
from types import SimpleNamespace

import pytest

from backend.github_reports import ReportExportError
from backend.github_storage import source_repository, state_repository
from backend.github_report_view import report_index
from scripts import check_github_daily_completion as completion
from scripts import validate_github_split_storage as preflight
from scripts import sync_github_reports as reports
from scripts.store_github_private_diagnostics import archive_logs


@pytest.fixture(autouse=True)
def split_environment(monkeypatch):
    monkeypatch.setenv("GITHUB_REPOSITORY", "synthetic/public-source")
    monkeypatch.setenv("FUND_ANA_STATE_REPOSITORY", "synthetic/private-state")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_REF", "refs/heads/main")
    monkeypatch.setenv("GH_TOKEN", "synthetic-scoped-token")


def test_distinct_identity_and_legacy_default(monkeypatch):
    assert source_repository() == "synthetic/public-source"
    assert state_repository() == "synthetic/private-state"
    monkeypatch.delenv("FUND_ANA_STATE_REPOSITORY")
    assert state_repository() == "DaDaDa12138/fund-ana"
    monkeypatch.setenv("FUND_ANA_STATE_REPOSITORY", "../invalid")
    with pytest.raises(ValueError):
        state_repository()


@pytest.mark.parametrize("change,value,error", [
    ("FUND_ANA_STATE_REPOSITORY", "synthetic/public-source", "must_be_separate"),
    ("GH_TOKEN", "", "token_missing"),
    ("GITHUB_EVENT_NAME", "pull_request_target", "event_not_trusted"),
    ("GITHUB_REF", "refs/heads/untrusted", "ref_not_trusted"),
])
def test_untrusted_context_rejected_before_private_read(monkeypatch, change, value, error):
    monkeypatch.setenv(change, value)
    monkeypatch.setattr(preflight.subprocess, "run", lambda *a, **k:
                        (_ for _ in ()).throw(AssertionError("private read")))
    with pytest.raises(RuntimeError, match=error):
        preflight.validate()


@pytest.mark.parametrize("private,archived,accepted", [(True, False, True), (False, False, False), (True, True, False)])
def test_preflight_requires_private_available_identity(monkeypatch, private, archived, accepted):
    def read(command, **kwargs):
        assert command[-1] == "repos/synthetic/private-state"
        return SimpleNamespace(returncode=0, stdout=json.dumps(
            {"private": private, "archived": archived, "full_name": "synthetic/private-state"}))
    monkeypatch.setattr(preflight.subprocess, "run", read)
    if accepted:
        preflight.validate()
    else:
        with pytest.raises(RuntimeError):
            preflight.validate()


def test_auth_failure_cannot_leak_private_response(monkeypatch, capsys):
    monkeypatch.setattr(preflight.subprocess, "run", lambda *a, **kw:
                        SimpleNamespace(returncode=1, stdout="private response", stderr="synthetic-scoped-token"))
    assert preflight.main() == 1
    output = capsys.readouterr().out
    assert "authentication_failed" in output
    assert "private response" not in output and "synthetic-scoped-token" not in output


def test_run_timing_uses_source_token_and_receipts_use_storage_token(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("GITHUB_TOKEN", "synthetic-public-token")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("SCHEDULE_CHECK_ONLY", "true")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "output"))
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    calls = []
    def read(command, **kwargs):
        calls.append(command)
        if "/actions/runs/" in command[2]:
            assert command[2] == "repos/synthetic/public-source/actions/runs/123"
            assert kwargs["env"]["GH_TOKEN"] == "synthetic-public-token"
            return SimpleNamespace(returncode=0, stdout="2026-11-12T01:00:00Z", stderr="")
        assert command[-1].startswith("repos/synthetic/private-state/contents/checks/")
        assert "env" not in kwargs  # Inherits only this step's scoped GH_TOKEN.
        return SimpleNamespace(returncode=0, stdout="[]", stderr="")
    monkeypatch.setattr(completion.subprocess, "run", read)
    monkeypatch.setattr(completion, "now_shanghai", lambda: datetime.fromisoformat("2026-11-12T10:00:00+08:00"))
    completion.main()
    assert len(calls) == 2
    assert json.loads(capsys.readouterr().out)["collection_required"] is True
    assert (tmp_path / "output").read_text() == "run=false\n"


def test_reports_cannot_be_pushed_to_public_repository(monkeypatch):
    monkeypatch.setattr(reports.subprocess, "run", lambda *a, **kw:
                        SimpleNamespace(returncode=0, stdout='{"private":false}'))
    monkeypatch.setattr(reports, "git", lambda *a, **kw:
                        (_ for _ in ()).throw(AssertionError("public push")))
    with pytest.raises(ReportExportError, match="require_private"):
        reports.sync_remote("synthetic/public-source", {}, None, "synthetic-token")


def test_report_action_link_keeps_source_identity_and_legacy_history():
    check = {"status": "failed", "github_run_id": "123", "attempt": "1", "check_date": "2026-11-12",
             "github_repository": "synthetic/public-source"}
    assert "https://github.com/synthetic/public-source/actions/runs/123" in report_index(None, check)
    del check["github_repository"]
    assert "https://github.com/DaDaDa12138/fund-ana/actions/runs/123" in report_index(None, check)


def test_private_diagnostics_redact_credentials_and_exclude_unlisted_files(tmp_path):
    secret = "synthetic-private-secret-token"
    basic = base64.b64encode(("x-access-token:" + secret).encode()).decode()
    logs = tmp_path / ".cloud-private-logs"
    logs.mkdir()
    (logs / "production.log").write_text("private diagnostic " + secret + " " + basic)
    (logs / "not-allowlisted.log").write_text("do not archive")
    (tmp_path / "secret.key").write_text("do not archive")
    (logs / "state.log").symlink_to(tmp_path / "secret.key")
    output = tmp_path / "logs.tar.gz"
    assert archive_logs(tmp_path, output, [secret]) == 1
    with tarfile.open(output) as archive:
        assert archive.getnames() == [".cloud-private-logs/production.log"]
        value = archive.extractfile(archive.getmembers()[0]).read().decode()
        assert secret not in value and basic not in value
        assert "private diagnostic" in value and "[REDACTED]" in value
