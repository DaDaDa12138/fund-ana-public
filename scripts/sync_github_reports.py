#!/usr/bin/env python3
"""Commit an allowlisted report bundle to a separate, append-only report branch."""
from __future__ import annotations

import argparse
import base64
from datetime import date, datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.github_reports import ReportExportError, verify_bundle, write_json
from backend.github_report_view import report_view, report_index
from backend.shanghai_time import now_shanghai, display_shanghai
from core.utils.period_return_snapshot import verified_superseded_date_proof


BRANCH = "daily-reports"
CHECK_STATUSES = {"published", "unchanged", "verified_existing_publication", "published_with_warnings", "unchanged_with_warnings", "failed"}


def validate_check(value: dict) -> None:
    if value.get("schema") != "fund-ana-github-check-v1" or value.get("status") not in CHECK_STATUSES:
        raise ReportExportError("check_schema_invalid")
    date.fromisoformat(value["check_date"])
    if value["status"] in {"published_with_warnings", "unchanged_with_warnings"}:
        proof = value.get("nonblocking_validation") or {}
        rejections = proof.get("rejection_proofs", [])
        missing = proof.get("missing_returns_proofs", [])
        superseded = proof.get("superseded_refresh_proofs", [])
        if (value.get("task_status") != "degraded" or proof.get("status") != "verified"
                or proof.get("contract") != ("github-safe-publication-completion-v2" if superseded else "github-safe-publication-completion-v1")
                or proof.get("blocking_count") != 0 or proof.get("formal_overlap_count") != 0
                or not proof.get("excluded_fcodes") or not isinstance(rejections, list)
                or not isinstance(missing, list) or not isinstance(superseded, list) or not (rejections or missing or superseded)
                or not all(isinstance(item, dict) for item in [*rejections, *missing, *superseded])):
            raise ReportExportError("warning_check_proof_missing")
        codes = [item.get("fcode") for item in [*rejections, *missing, *superseded]]
        expected_reason = ("period_return_refresh_superseded" if superseded else
                           "period_return_initialization_placeholder_quarantined" if rejections
                           else "period_return_unavailable_without_returns")
        if (any(not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code) for code in codes)
                or len(set(codes)) != len(codes) or sorted(codes) != proof["excluded_fcodes"]
                or value.get("task_warning_reason") != expected_reason
                or any(item.get("status") != "missing" or item.get("returns_and_provenance") != "all_null"
                       for item in missing)
                or any(not re.fullmatch(r"[0-9a-f]{64}", str(item.get("rejection_sha256") or ""))
                       for item in rejections)
                or any(item.get("status") != "superseded_refresh_rejected"
                       or item.get("returns_and_nav") != "reprojected_current_rank"
                       or not re.fullmatch(r"[0-9a-f]{64}", str(item.get("accepted_source_sha256") or ""))
                       or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", str(item.get("nav_date") or ""))
                       for item in superseded)):
            raise ReportExportError("warning_check_proof_missing")
    for key in ("github_run_id", "attempt"):
        if not re.fullmatch(r"[0-9]{1,20}", str(value.get(key) or "")):
            raise ReportExportError("check_identity_invalid")


def merge_export(repository: Path, check: dict, bundle_dir: Path | None) -> dict:
    """Pure filesystem operation, also used by isolated tests; no git/network."""
    validate_check(check)
    for path in repository.iterdir():
        if path.name == ".git":
            continue
        if path.name not in {"README.md", "latest.json", ".fund-ana-reports.json", "reports", "checks", "views"}:
            raise ReportExportError("reports_branch_contains_unexpected_content")
        if path.is_symlink() or (path.is_dir() and any(p.is_symlink() for p in path.rglob("*"))):
            raise ReportExportError("reports_branch_symlink")
    marker = repository / ".fund-ana-reports.json"
    if marker.exists() and json.loads(marker.read_text()).get("schema") != "fund-ana-reports-branch-v1":
        raise ReportExportError("reports_branch_schema_invalid")
    write_json(marker, {"schema": "fund-ana-reports-branch-v1", "source_branch": "main"})
    latest_path = repository / "latest.json"
    latest = json.loads(latest_path.read_text()) if latest_path.exists() else None
    if check["status"] == "failed":
        if bundle_dir is not None and check.get("publication_sync_status") != "verified_current":
            raise ReportExportError("failed_check_cannot_publish_bundle")
    elif bundle_dir is None:
        raise ReportExportError("successful_check_requires_verified_bundle")
    if bundle_dir is not None:
        bundle = verify_bundle(bundle_dir)
        publication = bundle["publication"]
        if check.get("publication") != publication:
            raise ReportExportError("check_publication_binding_mismatch")
        if check["status"] in {"published_with_warnings", "unchanged_with_warnings"}:
            proof = check["nonblocking_validation"]
            csv_hash = next(item["sha256"] for item in bundle["files"] if item["path"] == "formal.csv")
            if (proof.get("formal_csv_sha256") != csv_hash or proof.get("publication_run_id") != publication["run_id"]
                    or any(not verified_superseded_date_proof(item, publication.get("nav_date")) for item in proof.get("superseded_refresh_proofs", []))):
                raise ReportExportError("warning_bundle_proof_mismatch")
        day = datetime.fromisoformat(publication["committed_at"]).date().isoformat()
        relative = Path("reports") / day / publication["run_id"] / publication["revision"]
        destination = repository / relative
        if destination.exists():
            previous = verify_bundle(destination)
            if previous["publication"] != publication or previous["files"] != bundle["files"]:
                raise ReportExportError("immutable_report_revision_conflict")
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(bundle_dir, destination)
            verify_bundle(destination)
        # A delayed export must not move the repository's latest pointer backwards.
        if latest and int(latest["publication"]["generation"]) > int(publication["generation"]):
            raise ReportExportError("publication_generation_regression")
        latest = {"schema": "fund-ana-github-latest-v1", "publication": publication,
                  "directory": relative.as_posix(), "files": [item["path"] for item in bundle["files"]]}
        view_path = Path("views") / publication["run_id"] / (publication["revision"] + ".md")
        rendered = report_view(bundle, (destination / "formal.csv").read_text(encoding="utf-8-sig"), relative.as_posix())
        (repository / view_path).parent.mkdir(parents=True, exist_ok=True)
        (repository / view_path).write_text(rendered, encoding="utf-8")
        latest["reading_view"] = view_path.as_posix()
        latest["timezone"] = "Asia/Shanghai"
        write_json(latest_path, latest)
    check_dir = repository / "checks" / check["check_date"]
    check_dir.mkdir(parents=True, exist_ok=True)
    check_path = check_dir / f'{check["github_run_id"]}-{check["attempt"]}.json'
    if check_path.exists() and json.loads(check_path.read_text()) != check:
        raise ReportExportError("immutable_check_conflict")
    write_json(check_path, check)
    (repository / "README.md").write_text(report_index(latest, check), encoding="utf-8")
    return {"status": check["status"], "has_publication": latest is not None}


def git(args: list[str], directory: Path, environment: dict, *, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["git", *args], cwd=directory, env=environment, text=True, capture_output=True)
    if check and result.returncode:
        # Do not echo credentials, git command arguments, or remote response bodies.
        raise ReportExportError("git_" + args[0].replace("-", "_") + "_failed")
    return result


def sync_remote(repository: str, check: dict, bundle: Path | None, token: str) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ReportExportError("github_repository_invalid")
    if not token:
        raise ReportExportError("github_token_missing")
    # Reports contain operational data too: never push them to the public
    # execution repository, even if its workflow token has write permission.
    metadata = subprocess.run(["gh", "api", "repos/" + repository],
                              env=dict(os.environ, GH_TOKEN=token),
                              capture_output=True, text=True, timeout=30)
    if metadata.returncode:
        raise ReportExportError("report_repository_unavailable")
    if json.loads(metadata.stdout).get("private") is not True:
        raise ReportExportError("reports_require_private_repository")
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("GIT_") and key not in {"GH_TOKEN", "GITHUB_TOKEN"}}
    auth = base64.b64encode(("x-access-token:" + token).encode()).decode()
    environment.update({"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
                        "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
                        "GIT_CONFIG_VALUE_0": "AUTHORIZATION: basic " + auth, "GIT_TERMINAL_PROMPT": "0",
                        "TZ": "Asia/Shanghai", "GIT_AUTHOR_DATE": now_shanghai().isoformat(),
                        "GIT_COMMITTER_DATE": now_shanghai().isoformat()})
    with tempfile.TemporaryDirectory(prefix="fund-ana-report-push-") as temporary:
        directory = Path(temporary)
        git(["init", "--initial-branch=" + BRANCH], directory, environment)
        git(["remote", "add", "origin", "https://github.com/" + repository + ".git"], directory, environment)
        git(["config", "user.name", "github-actions[bot]"], directory, environment)
        git(["config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], directory, environment)
        git(["config", "core.hooksPath", os.devnull], directory, environment)
        for attempt in range(3):
            remote = git(["ls-remote", "--exit-code", "--heads", "origin", "refs/heads/" + BRANCH],
                         directory, environment, check=False)
            if remote.returncode == 0:
                git(["fetch", "--depth=1", "origin", "refs/heads/" + BRANCH], directory, environment)
                git(["reset", "--hard", "FETCH_HEAD"], directory, environment)
                git(["clean", "-fd"], directory, environment)
            elif remote.returncode != 2:
                raise ReportExportError("git_remote_unavailable")
            outcome = merge_export(directory, check, bundle)
            git(["add", "--", "README.md", ".fund-ana-reports.json", "checks"], directory, environment)
            if (directory / "latest.json").exists():
                git(["add", "--", "latest.json", "reports", "views"], directory, environment)
            changes = git(["diff", "--cached", "--quiet"], directory, environment, check=False)
            if changes.returncode == 0:
                return {**outcome, "changed": False, "branch": BRANCH}
            if changes.returncode != 1:
                raise ReportExportError("git_staged_diff_failed")
            git(["commit", "-m", "reports: " + check["check_date"] + " " + check["status"]], directory, environment)
            pushed = git(["push", "origin", "HEAD:refs/heads/" + BRANCH], directory, environment, check=False)
            if pushed.returncode == 0:
                commit = git(["rev-parse", "HEAD"], directory, environment).stdout.strip()
                return {**outcome, "changed": True, "branch": BRANCH, "commit": commit}
        raise ReportExportError("report_push_failed_after_retries")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--directory", required=True, type=Path, help="Trusted host export directory with check.json")
    args = parser.parse_args()
    check = json.loads((args.directory / "check.json").read_text())
    bundle = (args.directory / "bundle" if check.get("status") != "failed"
              or check.get("publication_sync_status") == "verified_current" else None)
    result = sync_remote(args.repository, check, bundle, os.environ.get("GH_TOKEN", ""))
    print(json.dumps(result))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as stream:
            stream.write(f'## Fund-ANA 云端执行\n\n上海时间：{display_shanghai(check.get("finished_at") or check.get("checked_at"))}\n\n自动化结果：`{result["status"]}`。报告分支：`{BRANCH}`。\n')
            for key in ("task_status", "task_warning_reason", "publication_sync_status", "error_code"):
                value = str(check.get(key) or "")
                if re.fullmatch(r"[a-z_]{1,100}", value):
                    stream.write(f'\n{key}: `{value}`.\n')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
