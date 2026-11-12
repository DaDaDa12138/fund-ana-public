from __future__ import annotations

import sys
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.storage import WorkflowStore
from backend.storage import WorkflowOwnershipError
from backend.workflow import WorkflowManager
from backend.scheduler import ProjectScheduler


@pytest.mark.parametrize("options", [
    {"run_collection": True, "run_screening": True},
    {"reuse_existing_outputs": True},
])
def test_missing_core_fails_workflow_and_preserves_persisted_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, options: dict,
) -> None:
    from unittest.mock import Mock
    from backend import workflow

    state_file = tmp_path / "workflow_state.json"
    settings = SimpleNamespace(
        state_file=state_file,
        core_root=tmp_path / "absent-core",
        tiantian_base_url="http://disabled.invalid/tiantian",
        autostock_base_url="http://disabled.invalid/autostock",
    )
    manager = WorkflowManager(settings)
    manager._schema_ready = True
    published = {"run_id": "previous-contract-run", "revision_id": "previous-revision"}
    manager.store.set_published_report(published)
    report_builders = []
    for name in ("build_report_payload", "build_lightweight_report_payload", "commit_report_revision"):
        step = Mock(side_effect=AssertionError(f"unexpected {name}"))
        monkeypatch.setattr(workflow, name, step)
        report_builders.append(step)
    publish = Mock(side_effect=AssertionError("publication attempted after missing core"))
    monkeypatch.setattr(manager, "_publish_formal_report", publish)

    try:
        with pytest.raises(FileNotFoundError, match="core_engine_unavailable"):
            manager.run_sync(options)
        state = manager.store.snapshot()
        assert state["status"] == "failed"
        assert state["phase"] == "failed"
        assert "core_engine_unavailable" in state["error"]
        assert state["pid"] is None
        assert state["published_report"] == published
        assert state["current_run_report"] is None
        assert not manager.store.has_active_lock()
        for step in report_builders:
            step.assert_not_called()
        publish.assert_not_called()
        reloaded = WorkflowStore(state_file)
        try:
            assert reloaded.snapshot()["published_report"] == published
        finally:
            reloaded.close()
    finally:
        manager.store.close()


def test_workflow_manager_recovers_stale_running_state(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "workflow_state.json")
    store.start("run_sync_1", {"run_screening": True})
    with store._lock:
        store.state["pid"] = 999999
        store.state["last_updated_at"] = "2026-04-01T00:00:00"
        store._persist()
    store.close()

    manager = WorkflowManager.__new__(WorkflowManager)
    manager.store = store
    manager._thread = None
    manager._lock = threading.Lock()
    manager.settings = None
    manager._pid_is_alive = lambda pid: False  # type: ignore[method-assign]

    manager._recover_if_stale()
    state = manager.store.snapshot()

    assert state["status"] == "failed"
    assert state["error"] == "workflow_process_missing"
    assert state["pid"] is None


def test_workflow_manager_recovers_dead_pid_even_when_heartbeat_is_fresh(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "workflow_state.json")
    store.start("run_sync_heartbeat", {"run_screening": True})
    with store._lock:
        store.state["pid"] = 999999
        store.state["last_updated_at"] = datetime.now().isoformat(timespec="seconds")
        store._persist()
    store.close()

    manager = WorkflowManager.__new__(WorkflowManager)
    manager.store = store
    manager._thread = None
    manager._lock = threading.Lock()
    manager.settings = None
    manager._pid_is_alive = lambda pid: False  # type: ignore[method-assign]
    manager.STALE_HEARTBEAT_SECONDS = 300

    manager._recover_if_stale()
    state = manager.store.snapshot()

    assert state["status"] == "failed"
    assert state["error"] == "workflow_process_missing"
    assert state["pid"] is None


def test_workflow_manager_does_not_recover_running_state_while_workflow_lock_is_still_held(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "workflow_state.json")
    store.start("run_sync_lock_held", {"run_screening": True})
    with store._lock:
        store.state["pid"] = None
        store.state["last_updated_at"] = "2026-04-01T00:00:00"
        store._persist()

    manager = WorkflowManager.__new__(WorkflowManager)
    manager.store = store
    manager._thread = None
    manager._lock = threading.Lock()
    manager.settings = None
    manager._pid_is_alive = lambda pid: False  # type: ignore[method-assign]
    manager.STALE_HEARTBEAT_SECONDS = 300

    manager._recover_if_stale()
    state = manager.store.snapshot()

    assert state["status"] == "running"
    assert state["error"] is None
    assert state["run_id"] == "run_sync_lock_held"

    store.close()


def test_workflow_manager_run_sync_marks_failure_on_keyboard_interrupt(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "workflow_state.json")

    manager = WorkflowManager.__new__(WorkflowManager)
    manager.store = store
    manager._thread = None
    manager._lock = threading.Lock()
    manager.settings = None
    manager._schema_ready = True
    manager.is_running = lambda: False  # type: ignore[method-assign]
    manager._ensure_schema_once = lambda: None  # type: ignore[method-assign]
    manager._execute_with_heartbeat = lambda run_id, options: (_ for _ in ()).throw(KeyboardInterrupt("manual_interrupt"))  # type: ignore[method-assign]

    with pytest.raises(KeyboardInterrupt, match="manual_interrupt"):
        manager.run_sync({"run_screening": True})

    state = manager.store.snapshot()
    assert state["status"] == "failed"
    assert state["phase"] == "failed"
    assert state["error"] == "manual_interrupt"
    assert state["pid"] is None


def test_workflow_store_tracks_current_and_published_reports(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "workflow_state.json")
    current_report = {"run_id": "20260408_100000_sync", "counts": {"final_count": 10}}
    published_report = {"run_id": "20260408_093000_sync", "counts": {"final_count": 12}}

    store.start("20260408_100000_sync", {"run_screening": True})
    store.finish_success(current_report)
    store.set_published_report(published_report)

    state = store.snapshot()
    assert state["report"]["run_id"] == "20260408_100000_sync"
    assert state["current_run_report"]["run_id"] == "20260408_100000_sync"
    assert state["published_report"]["run_id"] == "20260408_093000_sync"


@pytest.mark.parametrize("latest_publication", [None, {"run_id": "newly-published-contract-run"}])
def test_workflow_store_start_preserves_disk_publication_without_reusing_current_report(
    tmp_path: Path, latest_publication: dict | None,
) -> None:
    state_file = tmp_path / "workflow_state.json"
    writer = WorkflowStore(state_file)
    writer.start("previous-current-contract-run", {})
    writer.finish_success({"run_id": "previous-current-contract-run"})
    writer.set_published_report({"run_id": "old-published-contract-run"})
    reader = WorkflowStore(state_file)
    writer.set_published_report(latest_publication)

    try:
        reader.start("next-contract-run", {})
        state = reader.snapshot()
        assert state["published_report"] == latest_publication
        assert state["report"] is None
        assert state["current_run_report"] is None
        assert state["run_id"] == "next-contract-run"
        reader.finish_failure("controlled failure")
        assert writer.snapshot()["published_report"] == latest_publication
    finally:
        reader.close()
        writer.close()


def test_workflow_store_finish_success_aligns_top_level_run_id_to_report(tmp_path: Path) -> None:
    store = WorkflowStore(tmp_path / "workflow_state.json")

    store.start("20260408_090000_sync", {"run_screening": True})
    store.finish_success({"run_id": "20260408_091500_sync", "counts": {"final_count": 8}})

    state = store.snapshot()
    assert state["run_id"] == "20260408_091500_sync"
    assert state["report"]["run_id"] == "20260408_091500_sync"


def test_workflow_store_snapshot_reloads_cross_process_state_file(tmp_path: Path) -> None:
    state_file = tmp_path / "workflow_state.json"
    writer = WorkflowStore(state_file)
    reader = WorkflowStore(state_file)

    writer.start("20260421_212530_sync", {"run_collection": True})
    initial_reader_state = reader.snapshot()
    assert initial_reader_state["run_id"] == "20260421_212530_sync"
    assert initial_reader_state["status"] == "running"

    writer.finish_success({"run_id": "20260421_212530_sync", "counts": {"final_count": 12}})

    refreshed_state = reader.snapshot()
    refreshed_summary = reader.snapshot_summary()

    assert refreshed_state["status"] == "succeeded"
    assert refreshed_state["report"]["counts"]["final_count"] == 12
    assert refreshed_summary["status"] == "succeeded"
    assert refreshed_summary["run_id"] == "20260421_212530_sync"


def test_workflow_store_has_active_lock_detects_cross_store_busy_state(tmp_path: Path) -> None:
    state_file = tmp_path / "workflow_state.json"
    writer = WorkflowStore(state_file)
    reader = WorkflowStore(state_file)

    writer.start("20260511_001500_sync", {"run_collection": True})
    assert writer.has_active_lock() is True
    assert reader.has_active_lock() is True

    writer.finish_success({"run_id": "20260511_001500_sync", "counts": {"final_count": 3}})
    assert writer.has_active_lock() is False
    assert reader.has_active_lock() is False


def test_workflow_store_rejects_state_write_after_run_ownership_lost(tmp_path: Path) -> None:
    state_file = tmp_path / "workflow_state.json"
    writer = WorkflowStore(state_file)

    writer.start("20260511_002000_sync", {"run_screening": True})
    with writer._lock:
        writer.state["run_id"] = "20260511_002001_sync"
        writer.state["status"] = "running"
        writer._persist()

    with pytest.raises(WorkflowOwnershipError, match="workflow_run_ownership_lost"):
        writer.append_log("should be rejected")

    writer.close()


def test_workflow_manager_run_sync_rejects_when_workflow_lock_is_busy(tmp_path: Path) -> None:
    state_file = tmp_path / "workflow_state.json"
    primary_store = WorkflowStore(state_file)
    primary_store.start("20260511_003000_sync", {"run_screening": True})

    secondary_store = WorkflowStore(state_file)
    manager = WorkflowManager.__new__(WorkflowManager)
    manager.store = secondary_store
    manager._thread = None
    manager._lock = threading.Lock()
    manager.settings = None
    manager._schema_ready = True
    manager._ensure_schema_once = lambda: None  # type: ignore[method-assign]
    manager._recover_if_stale = lambda: None  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="已有筛选任务正在运行"):
        manager.run_sync({"run_screening": True})

    primary_store.close()


def test_project_scheduler_busy_reason_prefers_workflow_lock_signal(tmp_path: Path) -> None:
    state_file = tmp_path / "workflow_state.json"
    primary_store = WorkflowStore(state_file)
    primary_store.start("20260511_004000_sync", {"run_screening": True})

    scheduler = ProjectScheduler.__new__(ProjectScheduler)
    scheduler.workflow_manager = SimpleNamespace(store=WorkflowStore(state_file))

    assert scheduler._workflow_busy_skip_reason(after_collection=False) == "workflow_busy_locked"
    assert scheduler._workflow_busy_skip_reason(after_collection=True) == "workflow_busy_after_collection_locked"

    primary_store.close()
