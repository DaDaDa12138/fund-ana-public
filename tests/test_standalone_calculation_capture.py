"""Synthetic execution contracts; no fixture represents production fund data."""
from __future__ import annotations

from datetime import datetime, timedelta
import io
import json
import logging
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

from backend import calculation_input_capture as binding
from backend import standalone_calculation_capture as standalone
from backend.storage import WorkflowLockBusyError, WorkflowStore
from core.utils import calculation_capture as capture


@pytest.fixture
def cli(tmp_path, monkeypatch):
    root = tmp_path / "synthetic_project"
    source = root / "core/main.py"
    source.parent.mkdir(parents=True)
    source.write_text("# synthetic provenance fixture, not a fund calculation\n")
    reports = root / "reports"
    reports.mkdir()
    state_file = reports / "workflow_state.json"
    state_file.write_text('{"status":"idle","run_id":"existing_formal_run"}\n')
    settings = SimpleNamespace(project_root=root, reports_dir=reports, state_file=state_file)
    monkeypatch.setattr(standalone, "load_settings", lambda: settings)
    monkeypatch.setattr(capture, "PROJECT_ROOT", root)
    monkeypatch.chdir(root)
    monkeypatch.setattr(sys, "argv", ["core/main.py", "calculate-risk"])
    monkeypatch.setattr(sys, "stdin", io.StringIO("n\n"))
    for key in standalone._ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATABASE_PATH", str(root / "explicit_database.db"))
    state = SimpleNamespace(settings=settings, calls=0, source=source, initial_state=state_file.read_bytes())

    def command(*, code=0, error=None, partial=False, prompt=False, empty=False):
        state.calls += 1
        assert os.environ["DATABASE_PATH"] == str(root / "explicit_database.db")
        assert os.environ[capture.ENV_CAPTURE_SCOPE] == "standalone"
        assert "FUND_ANA_WORKFLOW_RUN_ID" not in os.environ
        writer = capture.CaptureWriter(Path(os.environ[capture.ENV_DIRECTORY]), "database_risk", {},
                                       code_paths=[source])
        if prompt:
            assert input("Synthetic continue? ") == "n"
            print("Synthetic cancelled")
        if not empty:
            writer.event("synthetic_null_and_zero", pd.DataFrame({"synthetic": [None, 0.0]}))
        if partial:
            writer._close()
        else:
            writer.finish("raised" if error else "returned")
        if error is not None:
            raise error
        return code

    state.command = command
    state.run = lambda **kwargs: standalone.run_standalone_risk(lambda: command(**kwargs))
    state.roots = lambda: sorted(reports.glob(".audit/standalone_calculations/*/cli_*"))
    state.index = lambda: next(state.roots()[-1].glob("calculation_inputs/calculation_input_index_*.json"))
    state.terminal = lambda: json.loads((state.roots()[-1] / "terminal.json").read_text())
    return state


def assert_lease_and_state_unchanged(cli):
    assert cli.settings.state_file.read_bytes() == cli.initial_state
    store = WorkflowStore(cli.settings.state_file)
    with store._lock:
        store._acquire_run_lock_locked("synthetic_next_command")
        store._release_run_lock_locked()
    assert not any(os.environ.get(key) for key in standalone._ENV_KEYS)


@pytest.mark.parametrize("empty", [False, True])
def test_direct_capture_keeps_actual_process_and_null_zero_or_no_objects(cli, empty):
    assert cli.run(empty=empty) == 0 and cli.calls == 1
    payload = binding.validate_calculation_input_index(cli.index(), full_journals=True)
    header = payload["sessions"][0]["header"]
    assert header["pid"] == os.getpid() and header["argv"] == sys.argv
    assert header["capture_scope"] == "standalone" and header["workflow_run_id"] is None
    assert payload["sessions"][0]["schema_version"] == 3
    bound = json.loads((cli.index().parent / "bound_standalone_execution.json").read_text())
    assert bound["execution"]["task"] is None
    reference = binding.capture_index_reference(cli.settings, str(cli.index()))
    assert reference["source_workflow_run_id"] is None and reference["capture_complete"]
    assert reference["scientific_reproduction_status"] == "not_verified"
    rows = list(capture.iter_journal(cli.index().parent / payload["sessions"][0]["file"]))
    objects = [row for row in rows if row["record"] == "object"]
    assert bool(objects) is not empty
    if not empty:
        frame = capture.decode(objects[0]["value"])
        assert pd.isna(frame.iloc[0, 0]) and frame.iloc[1, 0] == 0.0
    assert_lease_and_state_unchanged(cli)


def test_interactive_cancel_keeps_original_prompt_stdin_and_zero_exit(cli):
    assert cli.run(prompt=True, empty=True) == 0
    assert "--yes" not in sys.argv
    assert (cli.roots()[0] / "logs/stdout.log").read_text() == "Synthetic continue? Synthetic cancelled\n"
    assert cli.terminal()["completion"]["return_code"] == 0
    binding.validate_calculation_input_index(cli.index())


@pytest.mark.parametrize("code,error,partial", [
    (2, None, False), (0, RuntimeError("synthetic command error"), False),
    (0, KeyboardInterrupt("synthetic interrupt"), True), (0, SystemExit(7), False),
])
def test_failed_exit_exception_and_partial_keep_original_outcome(cli, code, error, partial):
    if error is not None:
        with pytest.raises(type(error)) as caught:
            cli.run(code=code, error=error, partial=partial)
        assert caught.value is error
    else:
        assert cli.run(code=code) == code
    terminal = cli.terminal()
    assert not terminal["capture_complete"] and terminal["capture_error"]
    assert terminal["index_path"] is None and terminal["index_sha256"] is None
    assert not list(cli.roots()[0].glob("calculation_inputs/calculation_input_index_*.json"))
    if partial:
        assert list(cli.roots()[0].glob("calculation_inputs/*.partial"))
    assert_lease_and_state_unchanged(cli)


def test_busy_workflow_blocks_command_without_changing_its_state(cli):
    store = WorkflowStore(cli.settings.state_file)
    with store._lock:
        store._acquire_run_lock_locked("synthetic_active_workflow")
    try:
        with pytest.raises(WorkflowLockBusyError):
            cli.run()
        assert cli.calls == 0 and not cli.roots()
        assert cli.settings.state_file.read_bytes() == cli.initial_state
    finally:
        store.close()


@pytest.mark.parametrize("key", standalone._ENV_KEYS)
def test_incomplete_parent_environment_never_runs_an_unbound_command(cli, monkeypatch, key):
    monkeypatch.setenv(key, "synthetic_parent_value")
    with pytest.raises(binding.CalculationCaptureError, match="incomplete_parent"):
        cli.run()
    assert cli.calls == 0


@pytest.mark.parametrize("field,value", [
    ("pid", 999999), ("argv", ["core/main.py", "calculate-risk", "--force"]),
    ("cwd", "/unrelated"), ("project_root", "/unrelated"), ("entrypoint_source", "other.py"),
    ("task", {"task_history_id": 123}), ("workflow_run_id", "invented"),
    ("capture_scope", "workflow"), ("pid", True),
])
def test_execution_identity_tampering_is_rejected(cli, field, value):
    cli.run()
    path = cli.index()
    payload = json.loads(path.read_text())
    bound_path = path.parent / "bound_standalone_execution.json"
    bound = json.loads(bound_path.read_text())
    bound["execution"][field] = value
    bound_path.write_text(json.dumps(bound))
    payload["standalone_execution_sha256"] = binding._sha(bound_path)
    path.write_text(json.dumps(payload))
    with pytest.raises(binding.CalculationCaptureError):
        binding.validate_calculation_input_index(path)


def test_execution_time_window_cannot_exclude_original_journal(cli):
    cli.run()
    path = cli.index()
    payload = json.loads(path.read_text())
    bound_path = path.parent / "bound_standalone_execution.json"
    bound = json.loads(bound_path.read_text())
    bound["completion"]["finished_at"] = (datetime.fromisoformat(bound["execution"]["started_at"])
                                           - timedelta(seconds=1)).isoformat()
    bound_path.write_text(json.dumps(bound))
    payload["standalone_execution_sha256"] = binding._sha(bound_path)
    path.write_text(json.dumps(payload))
    with pytest.raises(binding.CalculationCaptureError, match="time_mismatch"):
        binding.validate_calculation_input_index(path)


@pytest.mark.parametrize("target", ["manifest", "extra_member", "terminal", "execution", "console"])
def test_changed_missing_or_extra_delivery_files_never_validate(cli, target):
    cli.run()
    path = cli.index()
    if target == "manifest":
        member = next(path.parent.glob("*.manifest.json"))
        member.write_text(member.read_text() + " ")
    elif target == "extra_member":
        (path.parent / "unexpected.partial").write_text("synthetic interrupted capture")
    elif target == "terminal":
        (path.parent.parent / "terminal.json").unlink()
    elif target == "execution":
        (path.parent.parent / "execution.json").write_text("{}")
    else:
        (path.parent.parent / "logs/stdout.log").write_text("synthetic changed console")
    with pytest.raises(binding.CalculationCaptureError):
        binding.validate_calculation_input_index(path)


def test_repeated_invocation_restores_logger_stream_and_uses_distinct_paths(cli):
    logger = logging.getLogger("synthetic_standalone_logger")
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    original = sys.stderr
    try:
        def command():
            handler.setStream(sys.stderr)
            logger.info("synthetic during invocation")
            return cli.command()
        assert standalone.run_standalone_risk(command) == 0
        first = cli.index()
        before = first.read_bytes()
        assert handler.stream is original
        logger.info("synthetic after invocation")
        assert standalone.run_standalone_risk(command) == 0
        assert len(cli.roots()) == 2 and first.read_bytes() == before
        assert handler.stream is original
        assert_lease_and_state_unchanged(cli)
    finally:
        logger.removeHandler(handler)
        handler.close()


@pytest.mark.parametrize("command_error", [False, True])
def test_console_failure_never_looks_complete_or_hides_command_error(cli, monkeypatch, command_error):
    monkeypatch.setattr(standalone, "MAX_CONSOLE_BYTES", 1)
    error = RuntimeError("synthetic original failure") if command_error else None
    def command():
        print("synthetic output exceeds capture budget")
        return cli.command(error=error)
    with pytest.raises(RuntimeError) as caught:
        standalone.run_standalone_risk(command)
    if command_error:
        assert caught.value is error
    else:
        assert "console_budget_exhausted" in str(caught.value)
    assert not cli.terminal()["capture_complete"]
    assert_lease_and_state_unchanged(cli)


@pytest.mark.parametrize("code,error", [(0, None), (2, None), (0, RuntimeError("synthetic original failure"))])
def test_terminal_write_failure_keeps_exit_semantics_and_index_unusable(cli, monkeypatch, code, error):
    original = standalone._write_immutable
    def write(path, raw, **kwargs):
        if path.name == "terminal.json":
            raise OSError("synthetic terminal disk failure")
        return original(path, raw, **kwargs)
    monkeypatch.setattr(standalone, "_write_immutable", write)
    if error is not None or code == 0:
        with pytest.raises(type(error) if error else OSError) as caught:
            cli.run(code=code, error=error)
        if error is not None:
            assert caught.value is error
    else:
        assert cli.run(code=code) == 2
    if code == 0 and error is None:
        with pytest.raises(binding.CalculationCaptureError, match="terminal_required"):
            binding.validate_calculation_input_index(cli.index())
    assert_lease_and_state_unchanged(cli)


def test_core_main_delegates_only_bare_risk_command(cli, monkeypatch):
    from core import main
    calls = []
    monkeypatch.setattr(main, "_run_main_command", lambda args, parser: calls.append(args.command) or 3)
    monkeypatch.setattr(standalone, "run_standalone_risk", lambda command: calls.append("capture") or command())
    assert main.main() == 3 and calls == ["capture", "calculate-risk"]
    calls.clear()
    monkeypatch.setenv(capture.ENV_DIRECTORY, str(cli.settings.reports_dir / "parent_inputs"))
    assert main.main() == 3 and calls == ["calculate-risk"]
