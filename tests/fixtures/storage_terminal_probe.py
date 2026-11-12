"""Explicit terminal probes for the isolated runner; excluded from default discovery.

These cases intentionally fail or wait and must only be selected by the storage
acceptance harness. All bytes are synthetic test evidence, never fund data.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def _write_evidence(tmp_path):
    (tmp_path / "synthetic_terminal.json").write_text(json.dumps({"missing": None, "zero": 0, "scope": "test_fixture"}))
    (tmp_path / "synthetic_payload.bin").write_bytes(bytes(range(256)) * 512)


def test_intentional_failure(tmp_path):
    _write_evidence(tmp_path)
    raise AssertionError("expected_storage_failure_probe")


def test_timeout(tmp_path):
    _write_evidence(tmp_path)
    while True:
        time.sleep(1)


def test_process_exit_without_worker_result(tmp_path):
    _write_evidence(tmp_path)
    os._exit(17)


def test_detached_writer_is_retained(tmp_path):
    _write_evidence(tmp_path)
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", str(tmp_path)],
                             start_new_session=True, cwd=tmp_path)
    (tmp_path / "detached_child.json").write_text(json.dumps({"pid": child.pid, "cwd": str(tmp_path)}))
    raise AssertionError("expected_detached_child_probe")
