#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Structured subprocess logging helpers for screening tasks."""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional


_LOG_LOCK = threading.Lock()
_ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
_STDERR_BLOCKER_SIGNAL_PATTERN = re.compile(
    r"\bTraceback\b|\bCRITICAL\b|\[CRITICAL\]|\bERROR\b|\[ERROR\]|\bException\b|异常|错误",
    re.IGNORECASE,
)
_STDERR_WARNING_SIGNAL_PATTERN = re.compile(r"\bWARNING\b|\[WARNING\]|\bWARN\b|\[WARN\]|警告", re.IGNORECASE)
_STDERR_FAILURE_SIGNAL_PATTERN = re.compile(r"\bfailed\b|失败", re.IGNORECASE)
_STDERR_ZERO_FAILURE_PATTERN = re.compile(
    r"(?:failed|failed_count|records_failed|total_failed|失败)\s*(?:[:：=]|为)\s*0\b",
    re.IGNORECASE,
)
_STDERR_RETRY_MARKER_PATTERN = re.compile(
    r"准备重试|重试\s*\(?\s*\d+\s*/\s*\d+\s*\)?|\bretr(?:y|ying)\b",
    re.IGNORECASE,
)
_STDERR_TERMINAL_MARKER_PATTERN = re.compile(
    r"Traceback|CRITICAL|\[CRITICAL\]|\bFATAL\b|最终失败|永久失败|任务失败|退出码",
    re.IGNORECASE,
)


@dataclass(slots=True)
class StructuredSubprocessResult:
    task_name: str
    command: list[str]
    cwd: str
    started_at: str
    finished_at: str
    duration_seconds: float
    return_code: int
    timed_out: bool
    timeout_reason: str | None
    stdout_path: str
    stderr_path: str
    meta_path: str
    stdout_tail: list[str]
    stderr_tail: list[str]
    stderr_signal_summary: dict

    @property
    def success(self) -> bool:
        return self.return_code == 0 and not self.timed_out

    @property
    def stdout_text(self) -> str:
        try:
            return Path(self.stdout_path).read_text(encoding="utf-8")
        except OSError:
            return "\n".join(self.stdout_tail)

    def to_dict(self) -> dict:
        return {
            "task_name": self.task_name,
            "command": self.command,
            "cwd": self.cwd,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "return_code": self.return_code,
            "timed_out": self.timed_out,
            "timeout_reason": self.timeout_reason,
            "success": self.success,
            "stdout_path": self.stdout_path,
            "stderr_path": self.stderr_path,
            "meta_path": self.meta_path,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
            "stderr_signal_summary": self.stderr_signal_summary,
        }


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _safe_name(value: str) -> str:
    text = str(value or "").strip() or "subprocess"
    return re.sub(r"[^0-9A-Za-z_.\-\u4e00-\u9fff]+", "_", text).strip("._") or "subprocess"


def _tail_lines(path: Path, limit: int = 80) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    return lines[-limit:]


def _read_text_tail(path: Path, *, max_bytes: int = 5_000_000) -> str:
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            if size > max_bytes:
                fh.seek(-max_bytes, 2)
            raw = fh.read()
    except OSError:
        return ""
    return raw.decode("utf-8", errors="replace")


def _strip_ansi(value: str) -> str:
    return _ANSI_ESCAPE_PATTERN.sub("", str(value or ""))


def classify_stderr_signals(
    *,
    task_name: str,
    stderr_path: str | Path | None = None,
    stderr_text: str | None = None,
    success: bool | None = None,
    return_code: int | str | None = None,
    timed_out: bool | None = None,
    timeout_reason: str | None = None,
) -> dict:
    """Classify subprocess stderr without changing task success semantics."""
    if stderr_text is None and stderr_path:
        stderr_text = _read_text_tail(Path(stderr_path))
    text = _strip_ansi(stderr_text or "")
    lines = text.splitlines()
    non_empty_lines = [line.strip() for line in lines if line.strip()]
    blocker_samples: list[str] = []
    warning_samples: list[str] = []
    retry_samples: list[str] = []
    informational_line_count = 0

    for line in non_empty_lines:
        # 上游连接/超时等“准备重试”是可观测的瞬时状态，不代表任务最终失败。
        # 只有带有明确终态标记时才继续按 blocker 处理；返回码、超时仍由
        # execution_blocker 单独决定，避免把真实失败降级。
        is_retry_line = bool(_STDERR_RETRY_MARKER_PATTERN.search(line))
        has_terminal_marker = bool(_STDERR_TERMINAL_MARKER_PATTERN.search(line))
        if is_retry_line and not has_terminal_marker:
            retry_samples.append(line)
            warning_samples.append(line)
            continue
        if _STDERR_BLOCKER_SIGNAL_PATTERN.search(line):
            blocker_samples.append(line)
            continue
        if _STDERR_FAILURE_SIGNAL_PATTERN.search(line) and not _STDERR_ZERO_FAILURE_PATTERN.search(line):
            blocker_samples.append(line)
            continue
        if _STDERR_WARNING_SIGNAL_PATTERN.search(line):
            warning_samples.append(line)
            continue
        informational_line_count += 1

    execution_blocker = bool(timed_out) or (
        return_code not in (None, 0, "0") and success is not True
    )
    classification = "empty"
    if blocker_samples or execution_blocker:
        classification = "blocker_signals"
    elif warning_samples:
        classification = "warning_signals"
    elif non_empty_lines:
        classification = "informational_only"

    return {
        "schema_version": 1,
        "source": "stderr",
        "task_name": str(task_name or "").strip(),
        "available": bool(text),
        "line_count": len(lines),
        "non_empty_line_count": len(non_empty_lines),
        "informational_line_count": informational_line_count,
        "warning_line_count": len(warning_samples),
        "blocker_line_count": len(blocker_samples) + (1 if execution_blocker else 0),
        "warning_samples": warning_samples[:5],
        "retry_line_count": len(retry_samples),
        "retry_samples": retry_samples[:5],
        "blocker_samples": blocker_samples[:5],
        "classification": classification,
        "execution_status": {
            "success": bool(success) if success is not None else None,
            "return_code": return_code,
            "timed_out": bool(timed_out),
            "timeout_reason": timeout_reason,
            "execution_blocker": execution_blocker,
        },
        "truth_policy": "classify_log_signals_only_no_business_value_synthesis",
    }


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def resolve_structured_log_dir(
    *,
    root: Optional[Path] = None,
    run_id: Optional[str] = None,
) -> Path:
    """Resolve a stable log dir for workflow/core subprocesses."""
    configured = os.environ.get("FUND_ANA_SUBPROCESS_LOG_DIR")
    if configured:
        return Path(configured)

    normalized_run_id = str(run_id or os.environ.get("FUND_ANA_WORKFLOW_RUN_ID") or "").strip()
    project_root = root or _project_root()
    if normalized_run_id:
        return project_root / "reports" / "runs" / normalized_run_id / "logs" / "subprocess"
    return project_root / "logs" / "subprocess" / datetime.now().strftime("%Y%m%d")


def run_structured_subprocess(
    *,
    task_name: str,
    cmd: list[str],
    cwd: Path,
    env: Optional[dict[str, str]] = None,
    log: Optional[Callable[[str], None]] = None,
    timeout: Optional[float] = None,
    idle_timeout: Optional[float] = None,
    log_dir: Optional[Path] = None,
    stream_output: bool = True,
    tail_limit: int = 80,
) -> StructuredSubprocessResult:
    """Run a subprocess while preserving full stdout/stderr and JSON metadata."""
    started = datetime.now()
    started_at = started.isoformat(timespec="seconds")
    safe_name = _safe_name(task_name)
    base_dir = log_dir or resolve_structured_log_dir()
    base_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"{started.strftime('%Y%m%d_%H%M%S')}_{safe_name}_{os.getpid()}_{threading.get_ident()}"
    stdout_path = base_dir / f"{prefix}.stdout.log"
    stderr_path = base_dir / f"{prefix}.stderr.log"
    meta_path = base_dir / f"{prefix}.meta.json"
    command = [str(part) for part in cmd]
    run_env = env if env is not None else os.environ.copy()
    if log is not None:
        log(f"[{task_name}] $ {' '.join(command)}")
        log(f"[{task_name}] structured_logs stdout={stdout_path} stderr={stderr_path} meta={meta_path}")

    return_code = -1
    timed_out = False
    timeout_reason: str | None = None
    process: subprocess.Popen[str] | None = None
    last_output_at = time.monotonic()
    output_lock = threading.Lock()

    def _mark_output_progress() -> None:
        nonlocal last_output_at
        with output_lock:
            last_output_at = time.monotonic()

    def _seconds_since_output() -> float:
        with output_lock:
            return time.monotonic() - last_output_at

    def _terminate_process_tree() -> None:
        if process is None or process.poll() is not None:
            return
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except Exception:
            try:
                process.terminate()
            except Exception:
                pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass

    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open("w", encoding="utf-8") as stderr_handle:
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=run_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )

        def _pump(source, target_handle, *, is_stderr: bool = False) -> None:
            if source is None:
                return
            for line in source:
                text = str(line).rstrip("\n")
                _mark_output_progress()
                with _LOG_LOCK:
                    target_handle.write(text + "\n")
                    target_handle.flush()
                if stream_output and log is not None and text:
                    if is_stderr:
                        log(f"[{task_name}][stderr] {text}")
                    else:
                        log(text)

        stdout_thread = threading.Thread(target=_pump, args=(process.stdout, stdout_handle), daemon=True)
        stderr_thread = threading.Thread(target=_pump, args=(process.stderr, stderr_handle), kwargs={"is_stderr": True}, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        total_deadline = time.monotonic() + timeout if timeout and timeout > 0 else None
        try:
            while True:
                polled = process.poll()
                if polled is not None:
                    return_code = polled
                    break
                now = time.monotonic()
                if total_deadline is not None and now >= total_deadline:
                    timed_out = True
                    timeout_reason = "total_timeout"
                    _terminate_process_tree()
                    return_code = -2
                    break
                if idle_timeout and idle_timeout > 0 and _seconds_since_output() >= idle_timeout:
                    timed_out = True
                    timeout_reason = "idle_timeout"
                    _terminate_process_tree()
                    return_code = -2
                    break
                time.sleep(0.2)
        finally:
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()

    finished = datetime.now()
    result = StructuredSubprocessResult(
        task_name=task_name,
        command=command,
        cwd=str(cwd),
        started_at=started_at,
        finished_at=finished.isoformat(timespec="seconds"),
        duration_seconds=round((finished - started).total_seconds(), 3),
        return_code=int(return_code),
        timed_out=timed_out,
        timeout_reason=timeout_reason,
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
        meta_path=str(meta_path),
        stdout_tail=_tail_lines(stdout_path, tail_limit),
        stderr_tail=_tail_lines(stderr_path, tail_limit),
        stderr_signal_summary=classify_stderr_signals(
            task_name=task_name,
            stderr_path=stderr_path,
            success=(int(return_code) == 0 and not timed_out),
            return_code=int(return_code),
            timed_out=timed_out,
            timeout_reason=timeout_reason,
        ),
    )
    _write_json_atomic(meta_path, result.to_dict())
    if log is not None:
        status = "success" if result.success else "failed"
        log(
            f"[{task_name}] finished status={status} return_code={result.return_code} "
            f"timed_out={result.timed_out} timeout_reason={result.timeout_reason or ''} "
            f"duration={result.duration_seconds:.1f}s meta={meta_path}"
        )
    return result


def raise_for_failed_subprocess(result: StructuredSubprocessResult) -> None:
    if result.success:
        return
    if result.timed_out:
        raise subprocess.TimeoutExpired(result.command, timeout=None)
    raise subprocess.CalledProcessError(
        result.return_code,
        result.command,
        output="\n".join(result.stdout_tail),
        stderr="\n".join(result.stderr_tail),
    )


def print_result_tail(result: StructuredSubprocessResult, *, stdout_limit: int = 2000, stderr_limit: int = 2000) -> None:
    if result.stdout_tail:
        stdout_text = "\n".join(result.stdout_tail)
        if stdout_text:
            print(stdout_text[-stdout_limit:])
    if result.stderr_tail:
        stderr_text = "\n".join(result.stderr_tail)
        if stderr_text:
            print(f"   错误输出:\n{stderr_text[-stderr_limit:]}")
    print(f"   stdout日志: {result.stdout_path}")
    print(f"   stderr日志: {result.stderr_path}")
    print(f"   meta: {result.meta_path}")


def _relative_to_project(path_text: str, project_root: Path) -> str | None:
    text = str(path_text or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute() and len(path.parts) > 1 and path.parts[1] == "app":
        path = project_root.joinpath(*path.parts[2:])
    try:
        return str(path.resolve().relative_to(project_root.resolve()))
    except (OSError, ValueError):
        return str(path)


def _resolve_logged_path(path_text: str, project_root: Path) -> Path | None:
    text = str(path_text or "").strip()
    if not text:
        return None
    path = Path(text)
    if path.is_absolute() and len(path.parts) > 1 and path.parts[1] == "app":
        return project_root.joinpath(*path.parts[2:])
    if path.is_absolute():
        return path
    return project_root / path


def build_subprocess_log_index(
    *,
    log_dir: Path,
    project_root: Path,
    run_id: Optional[str] = None,
    output_path: Optional[Path] = None,
) -> dict:
    """Build a JSON index for all structured subprocess metadata in a log dir."""
    log_dir = Path(log_dir)
    project_root = Path(project_root)
    items: list[dict] = []
    if log_dir.exists():
        for meta_path in sorted(log_dir.glob("*.meta.json")):
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if not isinstance(meta, dict):
                continue
            item = {
                "task_name": meta.get("task_name"),
                "success": bool(meta.get("success")),
                "return_code": meta.get("return_code"),
                "timed_out": bool(meta.get("timed_out")),
                "timeout_reason": meta.get("timeout_reason"),
                "duration_seconds": meta.get("duration_seconds"),
                "started_at": meta.get("started_at"),
                "finished_at": meta.get("finished_at"),
                "command": meta.get("command") or [],
                "meta_path": str(meta_path),
                "meta_path_relative": _relative_to_project(str(meta_path), project_root),
                "stdout_path": meta.get("stdout_path"),
                "stdout_path_relative": _relative_to_project(str(meta.get("stdout_path") or ""), project_root),
                "stderr_path": meta.get("stderr_path"),
                "stderr_path_relative": _relative_to_project(str(meta.get("stderr_path") or ""), project_root),
            }
            stdout_tail = meta.get("stdout_tail")
            stderr_tail = meta.get("stderr_tail")
            if isinstance(stdout_tail, list):
                item["stdout_tail"] = stdout_tail[-20:]
            if isinstance(stderr_tail, list):
                item["stderr_tail"] = stderr_tail[-20:]
            stderr_summary = meta.get("stderr_signal_summary")
            if not isinstance(stderr_summary, dict):
                stderr_path = _resolve_logged_path(str(meta.get("stderr_path") or ""), project_root)
                fallback_stderr_text = None
                if (
                    (stderr_path is None or not stderr_path.exists())
                    and isinstance(stderr_tail, list)
                ):
                    fallback_stderr_text = "\n".join(stderr_tail)
                stderr_summary = classify_stderr_signals(
                    task_name=str(meta.get("task_name") or ""),
                    stderr_path=stderr_path,
                    stderr_text=fallback_stderr_text,
                    success=bool(meta.get("success")),
                    return_code=meta.get("return_code"),
                    timed_out=bool(meta.get("timed_out")),
                    timeout_reason=meta.get("timeout_reason"),
                )
            item["stderr_signal_summary"] = stderr_summary
            items.append(item)

    failed_items = [item for item in items if not item.get("success")]
    index_path = output_path or (log_dir / "subprocess_index.json")
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "run_id": str(run_id or os.environ.get("FUND_ANA_WORKFLOW_RUN_ID") or "").strip() or None,
        "log_dir": str(log_dir),
        "log_dir_relative": _relative_to_project(str(log_dir), project_root),
        "index_path": str(index_path),
        "index_path_relative": _relative_to_project(str(index_path), project_root),
        "subprocess_count": len(items),
        "success_count": len(items) - len(failed_items),
        "failed_count": len(failed_items),
        "failed_task_names": [str(item.get("task_name") or "") for item in failed_items],
        "items": items,
    }
    _write_json_atomic(index_path, payload)
    return payload
