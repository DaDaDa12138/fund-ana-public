#!/usr/bin/env python3
"""Trusted host entry point; scheduler output stays local, only reports leave."""
from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.shanghai_time import now_shanghai
from backend.config import load_settings
from backend.github_reports import ReportExportError, export_current, parse_scheduler_output, task_outcome, write_json
from backend.publication_transaction import load_publication_head


def cleanup_generated_outputs(base: Path, logs: Path, now: float) -> None:
    """Only our own completed export/log names; called while holding job.lock."""
    for path in base.iterdir():
        if (not path.is_symlink() and path.is_dir() and re.fullmatch(r"[0-9]{1,20}-[0-9]{1,5}", path.name)
                and now - path.stat().st_mtime > 2 * 86400):
            shutil.rmtree(path)
    for path in logs.iterdir():
        if (not path.is_symlink() and path.is_file()
                and re.fullmatch(r"[0-9]{1,20}-[0-9]{1,5}(\.error)?\.log", path.name)
                and now - path.stat().st_mtime > 14 * 86400):
            path.unlink()


def head_identity(settings) -> dict:
    head = load_publication_head(settings)
    if not head:
        return {}
    run_id = head["current_published_run_id"]
    return {"run_id": run_id, "revision": head["committed_runs"][run_id]["revision"], "generation": head["generation"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--github-run-id", required=True)
    parser.add_argument("--attempt", required=True)
    parser.add_argument("--job", required=True, choices=("daily_refresh", "intraday_refresh", "publish_only"))
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9]{1,20}", args.github_run_id) or not re.fullmatch(r"[0-9]{1,5}", args.attempt):
        parser.error("invalid GitHub execution identity")
    settings = load_settings()
    base = Path("/var/lib/fund-ana-actions/exports")
    base.mkdir(parents=True, exist_ok=True)
    directory = base / f"{args.github_run_id}-{args.attempt}"
    directory.mkdir(mode=0o755, exist_ok=False)
    log_root = Path("/var/log/fund-ana-actions")
    log_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    check = {"schema": "fund-ana-github-check-v1", "github_run_id": args.github_run_id,
             "attempt": args.attempt, "job": args.job, "status": "failed",
             "checked_at": now_shanghai().isoformat(timespec="seconds"),
             "check_date": now_shanghai().date().isoformat(),
             "collection_requested": args.job != "publish_only"}
    code = 1
    # Independent outer lock: Actions concurrency does not cover manual host calls.
    with (base.parent / "job.lock").open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            cleanup_generated_outputs(base, log_root, now_shanghai().timestamp())
            before = head_identity(settings)
            if args.job != "publish_only":
                command = ["docker", "exec", "fund-ana-scheduler", "python", "/app/scripts/run_scheduler.py",
                           "--once", args.job, "--wait-timeout-seconds", "9000"]
                log = log_root / f"{args.github_run_id}-{args.attempt}.log"
                with log.open("xb") as stream:
                    result = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=9060)
                payload = parse_scheduler_output(log.read_text(encoding="utf-8", errors="replace"))
                # Retain full diagnostics locally; expose only selected state fields.
                nested = payload.get("result") if isinstance(payload.get("result"), dict) else {}
                for key in ("request_id", "status", "skip_reason", "warning_reason", "degraded_stage"):
                    check["task_" + key] = payload.get(key) or nested.get(key)
                after = head_identity(settings)
                try:
                    outcome = task_outcome(payload, result.returncode, before, after)
                except ReportExportError as exc:
                    # Collection health and the committed report are separate facts.
                    # Still mirror the independently verified current publication,
                    # but preserve a failing Actions result and the original dates.
                    outcome = None
                    check["error_type"] = type(exc).__name__
                    check["error_code"] = str(exc) if re.fullmatch(r"[a-z_]{3,100}", str(exc)) else "scheduler_outcome_rejected"
            else:
                outcome = "verified_existing_publication"
            # These reads do not launch a new workflow. Do not copy response bodies.
            for endpoint in ("health", "dashboard/latest", "tracker/current", "scheduler/status/detail"):
                with urlopen(f"http://127.0.0.1:{settings.web_port}/api/{endpoint}", timeout=90) as response:
                    body = json.loads(response.read())
                    if not isinstance(body, dict) or body.get("error"):
                        raise RuntimeError("runtime_endpoint_unavailable")
            bundle = export_current(settings, directory / "bundle")
            check.update(status=outcome or "failed", publication=bundle["publication"],
                         publication_sync_status="verified_current")
            code = 0 if outcome else 1
        except Exception as exc:
            # Raw exception messages can contain local paths/upstream responses.
            check["error_type"] = type(exc).__name__
            check["error_code"] = str(exc) if re.fullmatch(r"[a-z_]{3,100}", str(exc)) else "production_job_failed"
            with (log_root / f"{args.github_run_id}-{args.attempt}.error.log").open("a") as stream:
                import traceback
                traceback.print_exc(file=stream)
        finally:
            check["finished_at"] = now_shanghai().isoformat(timespec="seconds")
            write_json(directory / "check.json", check)
            print(json.dumps({"status": check["status"], "job": args.job, "exit_code": code}))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
