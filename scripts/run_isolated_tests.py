#!/usr/bin/env python3
"""Run matching-source pytest with no production data and no external requests."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time
import uuid

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.isolation_support import (
    CONTEXT_FILE, IsolationError, assert_isolated_context, check_default_runtime_paths,
    copy_source_workspace, manifest_digest, restrict_test_io, source_manifest,
    build_test_environment, validate_pytest_args,
)
from scripts.test_artifact_storage import enable_worker_child_tracking, _atomic_json
from scripts.test_source_storage import store_completed_workspace, share_test_sources


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _worker(args: list[str]) -> int:
    workspace = PROJECT_ROOT
    os.chdir(workspace)
    sys.path.insert(0, str(workspace / "core"))
    restrictions = restrict_test_io(workspace)
    assert_isolated_context(workspace)
    runtime_paths = check_default_runtime_paths(workspace)
    import pytest

    started = time.monotonic()
    code = int(pytest.main([*args, f"--basetemp={workspace / '.tmp' / 'pytest'}"]))
    _write_json(workspace / ".test_result.json", {
        "exit_code": code, "elapsed_seconds": round(time.monotonic() - started, 3),
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "restrictions": restrictions, "runtime_paths": runtime_paths,
    })
    return code


def _run_container_tests(output: Path, args: list[str], expected: str | None, timeout: int) -> int:
    if not Path("/.dockerenv").is_file():
        raise IsolationError("test_container_required: host entrypoint must use Docker")
    manifest = source_manifest(PROJECT_ROOT)
    digest = manifest_digest(manifest)
    if expected and expected != digest:
        raise IsolationError(f"test_image_source_mismatch: expected={expected} actual={digest}")
    output.mkdir(parents=True, exist_ok=False)
    _write_json(output / "source_manifest.json", {"sha256": digest, "files": manifest})
    workspace = output / "workspace"
    copy_source_workspace(PROJECT_ROOT, workspace, manifest)
    guard = output / "outside_workspace_guard.txt"
    guard.write_text("isolation probe; this file must not be readable by pytest\n")
    _write_json(workspace / CONTEXT_FILE, {
        "schema_version": 1, "guard_path": str(guard), "source_sha256": digest,
        "source_root": str(PROJECT_ROOT), "scope": "isolated_unit_regression",
    })
    command = [sys.executable, str(workspace / "scripts/run_isolated_tests.py"), "--worker", "--", *args]
    started = time.monotonic()
    timed_out = False
    child_tracking_available = enable_worker_child_tracking()
    with (output / "pytest.log").open("w") as log:
        process = subprocess.Popen(command, cwd=workspace, env=build_test_environment(workspace),
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            timed_out = True
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass  # The group can exit between the deadline and the signal.
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait()
            code = 124
    result_path = workspace / ".test_result.json"
    worker_result = json.loads(result_path.read_text()) if result_path.exists() else {}
    unchanged = source_manifest(PROJECT_ROOT) == manifest
    if not unchanged:
        code = 2
    summary = {"scope": "isolated_unit_regression", "source_sha256": digest,
               "source_unchanged": unchanged, "exit_code": code, "timed_out": timed_out,
               "elapsed_seconds": round(time.monotonic() - started, 3),
               "worker": worker_result, "pytest_args": args, "artifact_dir": str(output),
               "worker_child_tracking_available": child_tracking_available}
    _write_json(output / "summary.json", summary)
    # Storage completion never changes pytest's failure, timeout, or missing
    # worker result. The archiver checks the actual process group again before
    # removing files, and otherwise leaves the workspace in place.
    summary["workspace_storage"] = store_completed_workspace(output, process)
    if output.is_relative_to(PROJECT_ROOT / "reports"):
        _share_sources(output, summary)
    _write_json(output / "summary.json", summary)
    print(f"[isolated-tests] workspace_storage={json.dumps(summary['workspace_storage'])}", flush=True)
    print((output / "pytest.log").read_text()[-16000:], flush=True)
    print(f"[isolated-tests] summary={output / 'summary.json'}", flush=True)
    return code


def _share_sources(run_dir: Path, summary: dict) -> None:
    if summary.get("workspace_storage", {}).get("status") != "source_archived":
        return
    try:
        summary["source_pool_sharing"] = share_test_sources(PROJECT_ROOT, run_dir)
    except (OSError, ValueError, RuntimeError) as error:
        # Pool maintenance cannot turn a passed test into failure, or obscure
        # the original archive when sharing was not possible.
        summary["source_pool_sharing"] = {"status": "retained", "reason": str(error)}


def _run_docker_tests(output: Path, args: list[str], image: str | None, timeout: int) -> int:
    manifest = source_manifest(PROJECT_ROOT)
    digest = manifest_digest(manifest)
    # The only bind is an exclusively created directory; there is no option for
    # mounting project data, production config, a socket, or an existing folder.
    output.mkdir(parents=True, exist_ok=False)
    run_name = f"fund-ana-tests-{uuid.uuid4().hex[:12]}"
    build_tag = f"fund-ana-test:{uuid.uuid4().hex}" if image is None else None
    image_reference = image or build_tag
    code = 2
    try:
        if build_tag:
            built = subprocess.run(["docker", "build", "-t", build_tag, "."], cwd=PROJECT_ROOT)
            if built.returncode:
                code = built.returncode
                return code
        image_id = subprocess.check_output(
            ["docker", "image", "inspect", "--format", "{{.Id}}", str(image_reference)], text=True
        ).strip()
        command = ["docker", "run", "--rm", "--name", run_name, "--network", "none",
                   "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "256",
                   "--memory", "2g", "--cpus", "2", "--mount",
                   f"type=bind,src={output},dst=/test-artifacts", "--entrypoint", "python", image_id,
                   "scripts/run_isolated_tests.py", "--in-container", "--artifact-dir", "/test-artifacts/run",
                   "--expected-source-sha256", digest, "--timeout", str(timeout), "--", *args]
        _write_json(output / "docker_context.json", {"image_id": image_id, "source_sha256": digest,
                    "network": "none", "mounts": [{"source": str(output), "target": "/test-artifacts"}],
                    "command": command})
        try:
            code = subprocess.run(command, cwd=PROJECT_ROOT, timeout=timeout + 60).returncode
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            code = 124
        if source_manifest(PROJECT_ROOT) != manifest:
            raise IsolationError("test_source_changed_during_run")
        summary_path = output / "run/summary.json"
        if code == 0 and summary_path.is_file():
            summary = json.loads(summary_path.read_text())
            if output.is_relative_to(PROJECT_ROOT / 'reports'):
                _share_sources(summary_path.parent, summary)
            else:
                summary['source_pool_sharing'] = {'status': 'not_requested', 'reason': 'manual_artifacts_outside_project'}
            _atomic_json(summary_path, summary)
            print(f"[isolated-tests] source_pool={json.dumps(summary.get('source_pool_sharing'))}", flush=True)
        return code
    finally:
        subprocess.run(["docker", "rm", "-f", run_name], capture_output=True, text=True)
        if build_tag:
            subprocess.run(["docker", "image", "rm", build_tag], capture_output=True, text=True)
        _write_json(output / "entrypoint_result.json", {"exit_code": code, "source_sha256": digest})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", help="Use an existing image after comparing every source SHA; default builds a temporary image")
    parser.add_argument("--artifact-dir", type=Path, help="New output directory; an existing directory is rejected")
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--in-container", action="store_true", help="Used by scheduled smoke inside its Linux container")
    parser.add_argument("--expected-source-sha256", help=argparse.SUPPRESS)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    options = parser.parse_args()
    args = options.pytest_args
    if args[:1] == ["--"]:
        args = args[1:]
    validate_pytest_args(args)
    if options.timeout < 1:
        raise IsolationError("test_timeout_must_be_positive")
    if options.worker:
        return _worker(args)
    artifact_root = Path(os.environ.get('FUND_ANA_TEST_ARTIFACT_ROOT', '/mnt/Project-Archives/Fund-ANA-manual-tests'))
    output = options.artifact_dir or artifact_root / (
        "pytest_" + datetime.now().strftime("%Y%m%d_%H%M%S_%f"))
    output = output.absolute()
    if any(path.is_symlink() for path in (output, *output.parents)):
        raise IsolationError("test_artifact_symlink_not_allowed")
    if Path("/.dockerenv").is_file() or options.in_container:
        return _run_container_tests(output, args, options.expected_source_sha256, options.timeout)
    return _run_docker_tests(output, args, options.image, options.timeout)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (IsolationError, OSError, subprocess.SubprocessError) as exc:
        print(f"[isolated-tests] isolation_error: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(2)
