"""Capture a directly invoked risk CLI without inventing a workflow or child process.

The original command runs in this process, so stdin, prompts, return codes and
exceptions keep their meaning. Successful indexes bind the actual PID, argv,
time interval and complete typed journal. Failed or interrupted attempts retain
their evidence and never advertise a complete index.
"""
from __future__ import annotations

from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime
import json
import logging
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import sys
import threading
from typing import Any, Callable
from uuid import uuid4

from backend.calculation_input_capture import (
    CalculationCaptureError, MAX_RUN_BYTES, _capture_members, _pack_members,
    _safe, _sha, _write_immutable,
)
from backend.config import load_settings
from backend.storage import WorkflowStore
from core.utils.calculation_capture import validate_journal

MAX_CONSOLE_BYTES = 64 * 1024**2
_RUN_ID = re.compile(r"cli_[0-9]{8}_[0-9]{6}_[0-9a-f]{32}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_ENV_KEYS = (
    "FUND_ANA_WORKFLOW_RUN_ID", "FUND_ANA_CALCULATION_INPUT_DIR", "FUND_ANA_CALCULATION_RUN_ID",
    "FUND_ANA_CALCULATION_SCOPE", "FUND_ANA_CALCULATION_SHARED_DIR",
)


def _json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"


def _time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise CalculationCaptureError("standalone_execution_time_required")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise CalculationCaptureError("standalone_execution_timezone_required")
    return parsed


class _Tee:
    """Preserve Python text writes, including prompts that have no newline."""

    def __init__(self, original, target):
        self.original, self.target = original, target
        self.written = 0
        self.failure: Exception | None = None
        self.lock = threading.RLock()

    def write(self, text):
        raw = text.encode("utf-8", errors="backslashreplace")
        with self.lock:
            if self.failure is None:
                try:
                    if self.written + len(raw) > MAX_CONSOLE_BYTES:
                        raise CalculationCaptureError("standalone_console_budget_exhausted")
                    if self.target.write(raw) != len(raw):
                        raise CalculationCaptureError("standalone_console_short_write")
                    self.target.flush()
                    self.written += len(raw)
                except Exception as error:
                    # Logging may swallow write exceptions. Keep this failure
                    # explicit so a truncated console can never look complete.
                    self.failure = error
            return self.original.write(text)

    def flush(self):
        with self.lock:
            if self.failure is None:
                try:
                    self.target.flush()
                except Exception as error:
                    self.failure = error
            return self.original.flush()

    def __getattr__(self, name):
        return getattr(self.original, name)


@contextmanager
def _environment(values: dict[str, str]):
    previous = {key: os.environ.get(key) for key in _ENV_KEYS}
    try:
        for key in _ENV_KEYS:
            if key in values:
                os.environ[key] = values[key]
            else:
                os.environ.pop(key, None)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _validate_invocation(execution: dict, run_id: str) -> datetime:
    if (not isinstance(execution, dict) or not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id)
            or execution.get("capture_run_id") != run_id
            or execution.get("capture_scope") != "standalone" or execution.get("schema_version") != 1
            or execution.get("task") is not None or execution.get("workflow_run_id") is not None
            or execution.get("process_role") != "direct_cli" or type(execution.get("pid")) is not int
            or execution["pid"] <= 0):
        raise CalculationCaptureError("standalone_execution_identity_mismatch")
    argv = execution.get("argv")
    if (not isinstance(argv, list) or len(argv) < 2 or any(not isinstance(part, str) for part in argv)
            or argv[1] != "calculate-risk" or execution.get("entrypoint_source") != "core/main.py"
            or not isinstance(execution.get("cwd"), str) or not PurePosixPath(execution["cwd"]).is_absolute()
            or not isinstance(execution.get("project_root"), str)
            or not PurePosixPath(execution["project_root"]).is_absolute()):
        raise CalculationCaptureError("standalone_execution_command_mismatch")
    entrypoint = PurePosixPath(argv[0])
    if not entrypoint.is_absolute():
        entrypoint = PurePosixPath(execution["cwd"]) / entrypoint
    # No resolving against today's filesystem: the capture can be restored elsewhere.
    if PurePosixPath(posixpath.normpath(str(entrypoint))) != PurePosixPath(execution["project_root"]) / "core/main.py":
        raise CalculationCaptureError("standalone_execution_entrypoint_mismatch")
    started = _time(execution.get("started_at"))
    if started.strftime("%Y%m%d_%H%M%S") != run_id[4:19]:
        raise CalculationCaptureError("standalone_execution_time_mismatch")
    return started


def _validate_execution(bound: dict, run_id: str) -> tuple[dict, dict]:
    if not isinstance(bound, dict) or bound.get("schema_version") != 1:
        raise CalculationCaptureError("standalone_execution_schema_mismatch")
    execution, completion = bound.get("execution"), bound.get("completion")
    if not isinstance(execution, dict) or not isinstance(completion, dict):
        raise CalculationCaptureError("standalone_execution_required")
    started = _validate_invocation(execution, run_id)
    if _time(completion.get("finished_at")) < started:
        raise CalculationCaptureError("standalone_execution_time_mismatch")
    if (completion.get("returned") is not True or type(completion.get("return_code")) is not int
            or completion["return_code"] != 0 or completion.get("exception") is not None):
        raise CalculationCaptureError("standalone_command_not_successfully_returned")
    console = completion.get("console")
    if (not isinstance(console, list) or any(not isinstance(item, dict) for item in console)
            or [item.get("file") for item in console] != ["stdout.log", "stderr.log"]
            or any(type(item.get("bytes")) is not int or not 0 <= item["bytes"] <= MAX_CONSOLE_BYTES
                   or not isinstance(item.get("sha256"), str) or not _SHA.fullmatch(item["sha256"])
                   for item in console)):
        raise CalculationCaptureError("standalone_console_identity_mismatch")
    return execution, completion


def _bind_header(header: dict, terminal: dict, bound: dict, run_id: str) -> None:
    execution, completion = _validate_execution(bound, run_id)
    for name in ("pid", "argv", "cwd", "project_root", "entrypoint_source"):
        if header.get(name) != execution.get(name):
            raise CalculationCaptureError("standalone_journal_execution_mismatch:" + name)
    if (header.get("capture_scope") != "standalone" or header.get("workflow_run_id") is not None
            or header.get("kind") != "database_risk" or header.get("run_id") != run_id
            or "core/main.py" not in (header.get("code_sha256") or {})
            or not _time(execution["started_at"]) <= _time(header.get("started_at"))
            <= _time(terminal.get("finished_at")) <= _time(completion["finished_at"])):
        raise CalculationCaptureError("standalone_journal_scope_or_time_mismatch")


def build_standalone_index(directory: Path, bound: dict) -> Path:
    execution = bound["execution"]
    run_id = execution["capture_run_id"]
    _validate_execution(bound, run_id)
    files = _capture_members(_safe(directory))
    manifests = [path for path in files if path.name.endswith(".manifest.json")]
    if len(manifests) != 1:
        raise CalculationCaptureError("standalone_requires_one_risk_session")
    checked = validate_journal(manifests[0], expected_run_id=run_id)
    manifest, header, terminal = (checked[key] for key in ("manifest", "header", "terminal"))
    _bind_header(header, terminal, bound, run_id)
    if manifest["operation_outcome"] != "returned":
        raise CalculationCaptureError("standalone_risk_operation_did_not_return")
    expected = {manifests[0].name, manifest["file"], *_pack_members(manifest)}
    if ({path.name for path in files} != expected or any(path.is_symlink() or not path.is_file() for path in files)
            or sum(path.stat().st_size for path in files) > MAX_RUN_BYTES):
        raise CalculationCaptureError("standalone_capture_members_or_budget_mismatch")
    bound_path = directory / "bound_standalone_execution.json"
    _write_immutable(bound_path, _json(bound), bounded_capture=True)
    target = directory / f"calculation_input_index_{run_id}.json"
    payload = {
        "schema_version": 2, "capture_scope": "standalone", "run_id": run_id,
        "capture_complete": True, "scientific_reproduction_status": "not_verified",
        "created_at": datetime.now().astimezone().isoformat(),
        "expected_scopes": {"database_risk": 1}, "period_days": [], "code_sha256": header["code_sha256"],
        "standalone_execution": bound_path.name, "standalone_execution_sha256": _sha(bound_path),
        "total_compressed_bytes": sum(path.stat().st_size for path in files),
        "sessions": [{"manifest": manifests[0].name, "manifest_sha256": checked["manifest_sha256"],
                      **manifest, "context": header["context"], "header": header, "terminal": terminal,
                      "typed_objects_roundtripped": checked["typed_objects_roundtripped"]}],
        "scope": "Actual calls of this direct risk CLI, including zero calls, NULL and skipped results. No scheduler task, child process or formal publication is inferred; completed capture does not prove economic policy correctness.",
    }
    _write_immutable(target, _json(payload), bounded_capture=True)
    return target


def validate_standalone_index(path: Path, payload: dict, *, full_journals: bool = False) -> dict:
    run_id = payload.get("run_id", "")
    if (payload.get("schema_version") != 2 or payload.get("capture_scope") != "standalone"
            or not isinstance(run_id, str) or not _RUN_ID.fullmatch(run_id)
            or payload.get("capture_complete") is not True
            or path.name != f"calculation_input_index_{run_id}.json"
            or payload.get("standalone_execution") != "bound_standalone_execution.json"
            or payload.get("expected_scopes") != {"database_risk": 1} or payload.get("period_days") != []
            or payload.get("scientific_reproduction_status") != "not_verified"
            or "subprocess_index" in payload or "collection_execution" in payload):
        raise CalculationCaptureError("invalid_standalone_calculation_index")
    bound_path = _safe(path.parent / payload["standalone_execution"])
    if bound_path.stat().st_size > 256 * 1024 or _sha(bound_path) != payload.get("standalone_execution_sha256"):
        raise CalculationCaptureError("bound_standalone_execution_changed")
    bound = json.loads(bound_path.read_bytes())
    _validate_execution(bound, run_id)
    sessions = payload.get("sessions")
    if not isinstance(sessions, list) or len(sessions) != 1 or not isinstance(sessions[0], dict):
        raise CalculationCaptureError("standalone_requires_one_risk_session")
    session = sessions[0]
    identifier = session.get("session_id")
    if (not isinstance(identifier, str) or not re.fullmatch(r"[0-9a-f]{32}", identifier)
            or session.get("manifest") != identifier + ".manifest.json" or session.get("file") != identifier + ".jsonl.gz"
            or session.get("capture_scope") != "standalone" or session.get("run_id") != run_id
            or session.get("workflow_run_id") is not None or session.get("kind") != "database_risk"
            or session.get("operation_outcome") != "returned"):
        raise CalculationCaptureError("standalone_session_identity_mismatch")
    # PID/time/terminal binding requires the actual journal, even for an index
    # caller that did not ask for an additional full replay. No metadata-only
    # success is accepted for the new standalone format.
    checked = validate_journal(path.parent / session["manifest"], expected_run_id=run_id)
    manifest, header, terminal = (checked[key] for key in ("manifest", "header", "terminal"))
    if (session.get("manifest_sha256") != checked["manifest_sha256"]
            or any(session.get(key) != value for key, value in manifest.items())):
        raise CalculationCaptureError("standalone_manifest_index_disagree")
    members = {path.name, bound_path.name, session["manifest"], session["file"], *_pack_members(manifest)}
    data_files = members - {path.name, bound_path.name}
    total = sum((path.parent / name).stat().st_size for name in data_files)
    if (header != session.get("header") or terminal != session.get("terminal")
            or header.get("code_sha256") != payload.get("code_sha256")
            or header.get("context") != session.get("context")):
        raise CalculationCaptureError("standalone_bound_header_changed")
    _bind_header(header, terminal, bound, run_id)
    if (total != payload.get("total_compressed_bytes") or total > MAX_RUN_BYTES
            or {member.name for member in _capture_members(path.parent)} != members):
        raise CalculationCaptureError("incomplete_standalone_capture_members")
    _validate_delivery(path, bound, run_id)
    return payload


def _validate_delivery(path: Path, bound: dict, run_id: str) -> None:
    """An index is usable only after the original execution terminal is durable."""
    root = path.parent.parent
    if path.parent.name != "calculation_inputs" or root.name != run_id:
        raise CalculationCaptureError("standalone_delivery_directory_mismatch")
    terminal_path, execution_path = (_safe(root / name) for name in ("terminal.json", "execution.json"))
    if (not terminal_path.is_file() or not execution_path.is_file()
            or terminal_path.stat().st_size > 256 * 1024 or execution_path.stat().st_size > 256 * 1024):
        raise CalculationCaptureError("standalone_delivery_terminal_required")
    terminal = json.loads(terminal_path.read_bytes())
    if (not isinstance(terminal, dict) or terminal.get("schema_version") != 1
            or terminal.get("capture_scope") != "standalone" or terminal.get("capture_run_id") != run_id
            or terminal.get("capture_complete") is not True or terminal.get("capture_error") is not None
            or terminal.get("scientific_reproduction_status") != "not_verified"
            or terminal.get("index_sha256") != _sha(path)
            or terminal.get("execution_sha256") != _sha(execution_path)
            or terminal.get("completion") != bound["completion"]
            or json.loads(execution_path.read_bytes()) != bound["execution"]):
        raise CalculationCaptureError("standalone_delivery_identity_mismatch")
    declared = terminal.get("index_path")
    if (not isinstance(declared, str) or ".." in PurePosixPath(declared).parts
            or PurePosixPath(declared).parts[-3:] != (run_id, "calculation_inputs", path.name)):
        raise CalculationCaptureError("standalone_delivery_index_path_mismatch")
    logs = _safe(root / "logs")
    if {member.name for member in logs.iterdir()} != {"stdout.log", "stderr.log"}:
        raise CalculationCaptureError("standalone_console_members_mismatch")
    for item in bound["completion"]["console"]:
        log = _safe(logs / item["file"])
        if not log.is_file() or log.stat().st_size != item["bytes"] or _sha(log) != item["sha256"]:
            raise CalculationCaptureError("standalone_console_bytes_mismatch")


def _restore_log_streams(*streams: _Tee) -> None:
    loggers = [logging.getLogger(), *(value for value in logging.Logger.manager.loggerDict.values()
                                     if isinstance(value, logging.Logger))]
    for logger in loggers:
        for handler in logger.handlers:
            for stream in streams:
                if isinstance(handler, logging.StreamHandler) and handler.stream is stream:
                    # Tee writes flush immediately; setStream would flush the
                    # possibly failed capture again and hide a command error.
                    handler.acquire()
                    try:
                        handler.stream = stream.original
                    finally:
                        handler.release()


def _finish_console(streams: tuple[_Tee, _Tee]) -> Exception | None:
    failure = next((stream.failure for stream in streams if stream.failure is not None), None)
    try:
        _restore_log_streams(*streams)
    except Exception as error:
        failure = failure or error
    for stream in streams:
        try:
            stream.target.flush()
            os.fsync(stream.target.fileno())
        except Exception as error:
            failure = failure or error
        finally:
            try:
                stream.target.close()
            except Exception as error:
                failure = failure or error
    return failure


def run_standalone_risk(command: Callable[[], int]) -> int:
    """Run the original CLI once, under the shared write lease, with input capture."""
    if any(os.environ.get(key, "").strip() for key in _ENV_KEYS):
        raise CalculationCaptureError("incomplete_parent_capture_environment")
    settings = load_settings()
    started = datetime.now().astimezone()
    run_id = "cli_" + started.strftime("%Y%m%d_%H%M%S_") + uuid4().hex
    execution = {
        "schema_version": 1, "capture_scope": "standalone", "capture_run_id": run_id,
        "workflow_run_id": None, "task": None, "process_role": "direct_cli",
        "pid": os.getpid(), "argv": list(sys.argv), "cwd": str(Path.cwd()),
        "project_root": str(settings.project_root), "entrypoint_source": "core/main.py",
        "python_executable": sys.executable, "python": sys.version, "started_at": started.isoformat(),
    }
    _validate_invocation(execution, run_id)
    root = _safe(settings.reports_dir / ".audit/standalone_calculations" / started.strftime("%Y%m%d") / run_id)
    store = WorkflowStore(settings.state_file)
    with store._lock:
        store._acquire_run_lock_locked(run_id)
    try:
        root.mkdir(parents=True, exist_ok=False)
        directory, logs = root / "calculation_inputs", root / "logs"
        directory.mkdir()
        logs.mkdir()
        _write_immutable(root / "execution.json", _json(execution))
        print(f"[risk-capture] execution={root / 'execution.json'}", flush=True)
        result, failure, index, capture_error = None, None, None, None
        values = {
            "FUND_ANA_CALCULATION_INPUT_DIR": str(directory), "FUND_ANA_CALCULATION_RUN_ID": run_id,
            "FUND_ANA_CALCULATION_SCOPE": "standalone",
            "FUND_ANA_CALCULATION_SHARED_DIR": str(settings.reports_dir / ".calculation_input_pool"),
        }
        stdout = (logs / "stdout.log").open("xb")
        try:
            stderr = (logs / "stderr.log").open("xb")
        except BaseException:
            stdout.close()
            raise
        stdout_tee, stderr_tee = _Tee(sys.stdout, stdout), _Tee(sys.stderr, stderr)
        try:
            with _environment(values), redirect_stdout(stdout_tee), redirect_stderr(stderr_tee):
                result = command()
                if type(result) is not int:
                    raise CalculationCaptureError("standalone_cli_integer_exit_code_required")
        except BaseException as error:
            failure = error
        finally:
            capture_error = _finish_console((stdout_tee, stderr_tee))
        completion = {
            "finished_at": datetime.now().astimezone().isoformat(), "returned": failure is None,
            "return_code": result, "exception": f"{type(failure).__name__}:{failure}" if failure else None,
            "console_capture": "Python text stdout/stderr, UTF-8, including interactive prompts; native fd writes are not asserted",
            "console": None,
        }
        try:
            completion["console"] = [{"file": name, "bytes": (logs / name).stat().st_size, "sha256": _sha(logs / name)}
                                     for name in ("stdout.log", "stderr.log")]
            if capture_error is not None:
                raise capture_error
            index = build_standalone_index(directory, {"schema_version": 1, "execution": execution, "completion": completion})
        except Exception as error:
            capture_error = capture_error or error
        try:
            terminal = {
                "schema_version": 1, "capture_scope": "standalone", "capture_run_id": run_id,
                "execution_sha256": _sha(root / "execution.json"), "completion": completion,
                "capture_complete": index is not None, "capture_error": f"{type(capture_error).__name__}:{capture_error}" if capture_error else None,
                "index_path": str(index.relative_to(settings.project_root)) if index else None,
                "index_sha256": _sha(index) if index else None,
                "scientific_reproduction_status": "not_verified",
            }
            _write_immutable(root / "terminal.json", _json(terminal))
            print(f"[risk-capture] terminal={root / 'terminal.json'} capture_complete={index is not None}", flush=True)
            if index is not None:
                print(f"[risk-capture] index={index}", flush=True)
        except Exception as error:
            # A missing terminal is rejected by the reader. Preserve the actual
            # command exception or nonzero exit if evidence storage also fails.
            capture_error = capture_error or error
        if failure is not None:
            if capture_error is not None:
                failure.add_note(f"standalone capture also failed: {capture_error}")
            raise failure
        if result != 0:
            return result
        if capture_error is not None:
            raise capture_error
        return result
    finally:
        original_error = sys.exception()
        try:
            with store._lock:
                store._release_run_lock_locked()
        except Exception as error:
            if original_error is None:
                raise
            original_error.add_note(f"standalone lease cleanup also failed: {error}")
