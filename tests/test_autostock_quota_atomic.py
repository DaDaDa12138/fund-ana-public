"""Quota persistence drills only; no external HTTP is sent."""
from __future__ import annotations
import json
import multiprocessing
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))
from api.autostock_client import AutoStockRateLimiter, AutoStockQuotaStateError
from backend.autostock_budget import read_autostock_quota_snapshot
from utils import autostock_quota_state as ledger


def limiter(path):
    return AutoStockRateLimiter(min_interval=0, max_calls_per_hour=100, shared_state_path=str(path))


def test_lock_inode_survives_atomic_data_replacements_and_old_format_is_preserved(tmp_path):
    path = tmp_path / "quota.json"
    path.write_text(json.dumps({"last_call_time": 0, "recent_call_timestamps": [], "blocked_until": 0}))
    current = limiter(path)
    current.wait_if_needed()
    lock = path.with_name(path.name + ".lock")
    inode = lock.stat().st_ino
    current.wait_if_needed()
    assert lock.stat().st_ino == inode
    assert lock.stat().st_size > 0
    payload = json.loads(path.read_text())
    assert payload["schema_version"] == 2
    assert len(payload["recent_call_timestamps"]) == 2


def test_missing_initialized_ledger_is_unavailable_not_a_new_free_hour(tmp_path):
    path = tmp_path / "quota.json"
    current = limiter(path)
    current.wait_if_needed()
    path.unlink()
    with pytest.raises(AutoStockQuotaStateError, match="missing"):
        current.wait_if_needed()
    snapshot = read_autostock_quota_snapshot(shared_state_path=path, max_calls_per_hour=100, min_interval_seconds=0)
    assert snapshot["quota_remaining"] == 0 and snapshot["state_read_error"]
    assert not path.exists()


def test_first_read_does_not_consume_a_slot_and_first_write_initializes_it(tmp_path):
    path = tmp_path / "quota.json"
    snapshot = read_autostock_quota_snapshot(shared_state_path=path, max_calls_per_hour=100, min_interval_seconds=0)
    assert snapshot["quota_remaining"] == 100 and not path.exists()
    limiter(path).wait_if_needed()
    assert len(json.loads(path.read_text())["recent_call_timestamps"]) == 1


def _crash_during_replace(path_string, when):
    path = Path(path_string)
    original = ledger.os.replace
    def replace(source, target):
        if Path(target) == path:
            if when == "after":
                original(source, target)
            os._exit(83)
        return original(source, target)
    ledger.os.replace = replace
    limiter(path).wait_if_needed()


@pytest.mark.parametrize("when,expected_reserved", [("before", 1), ("after", 2)])
def test_killed_writer_leaves_complete_old_or_new_ledger(tmp_path, when, expected_reserved):
    path = tmp_path / "quota.json"
    limiter(path).wait_if_needed()
    process = multiprocessing.get_context("fork").Process(target=_crash_during_replace, args=(str(path), when))
    process.start()
    process.join(timeout=10)
    assert process.exitcode == 83
    payload = ledger.read_quota_state(path)
    assert len(payload["recent_call_timestamps"]) == expected_reserved
    limiter(path).wait_if_needed()
    assert len(ledger.read_quota_state(path)["recent_call_timestamps"]) == expected_reserved + 1


def test_failed_replace_does_not_erase_old_budget_or_claim_sent_http(tmp_path, monkeypatch):
    path = tmp_path / "quota.json"
    current = limiter(path)
    current.wait_if_needed()
    before = path.read_bytes()
    def reject(*args):
        raise OSError("injected disk failure")
    monkeypatch.setattr(ledger.os, "replace", reject)
    with pytest.raises(AutoStockQuotaStateError, match="persisted"):
        current.wait_if_needed()
    assert path.read_bytes() == before
    assert current.call_count == 1
