#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.task_audit_storage import (  # noqa: E402
    artifact_exists, is_task_log_path, read_original_artifact_bytes, task_log_metadata,
)
from scripts.audit_runtime_utils import build_audit_artifact_path  # noqa: E402

_MAX_PREVIEW_CHARS = 420
_TIMESTAMP_PATTERNS = (
    re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})"),
    re.compile(r"\[(?P<ts>\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2}\s+[+-]\d{4})\]"),
)

_SIGNATURE_RULES: tuple[dict[str, Any], ...] = (
    {
        "category": "python_traceback",
        "severity": "error",
        "pattern": re.compile(r"Traceback \(most recent call last\):"),
        "summary": "Python traceback detected in runtime logs.",
    },
    {
        "category": "database_locked",
        "severity": "error",
        "pattern": re.compile(r"database (?:is )?locked", re.IGNORECASE),
        "summary": "SQLite/database lock signal detected in runtime logs.",
    },
    {
        "category": "critical_runtime_error",
        "severity": "error",
        "pattern": re.compile(r"\b(?:CRITICAL|critical|panic|segmentation fault|fatal error)\b"),
        "summary": "Critical runtime error signal detected in runtime logs.",
    },
    {
        "category": "uncaught_exception",
        "severity": "error",
        "pattern": re.compile(r"(?:uncaught|unhandled)\s+exception|Exception in thread", re.IGNORECASE),
        "summary": "Uncaught exception signal detected in runtime logs.",
    },
    {
        "category": "task_failed",
        "severity": "error",
        "pattern": re.compile(r"\[task-finish\].*\bstatus=failed\b|\bstatus_detail=.*failed", re.IGNORECASE),
        "summary": "Scheduler task failure signal detected in task logs.",
    },
    {
        "category": "http_5xx",
        "severity": "error",
        "pattern": re.compile(r'"\s+5\d\d\s+'),
        "summary": "HTTP 5xx response detected in web access logs.",
    },
    {
        "category": "python_warning",
        "severity": "warning",
        "pattern": re.compile(r"\b(?:FutureWarning|DeprecationWarning|RuntimeWarning|UserWarning):"),
        "summary": "Python warning detected in runtime logs.",
    },
    {
        "category": "generic_warning",
        "severity": "warning",
        "pattern": re.compile(r"\bWARNING\b|警告", re.IGNORECASE),
        "summary": "Generic warning detected in runtime logs.",
    },
)

_HTTP_ACCESS_PATTERN = re.compile(
    r'"(?P<method>[A-Z]+)\s+(?P<path>[^"\s?]+)(?:\?[^"\s]*)?\s+HTTP/[0-9.]+"\s+(?P<status>5\d\d)\b'
)
_TASK_NAME_PATTERN = re.compile(r"\btask(?:_name)?=(?P<task>[A-Za-z0-9_.:-]+)")
_STATUS_DETAIL_PATTERN = re.compile(r"\bstatus_detail=(?P<status>[A-Za-z0-9_.:-]+)")
_TASK_START_PATTERN = re.compile(
    r"\[task-start\].*?task_id=(?P<task_id>\d+).*?task_name=(?P<task_name>[A-Za-z0-9_.:-]+).*?started_at=(?P<started_at>\S+)"
)
_TASK_FINISH_PATTERN = re.compile(
    r"\[task-finish\].*?status=(?P<status>[A-Za-z0-9_.:-]+).*?finished_at=(?P<finished_at>\S+)"
)
_WHITESPACE_PATTERN = re.compile(r"\s+")
_ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-9;]*m")
_COMPOSE_PREFIX_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+\s+\|\s+")
_LEADING_TIMESTAMP_PATTERN = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:,\d+)?|\d{2}/[A-Z][a-z]{2}/\d{4}:\d{2}:\d{2}:\d{2}\s+[+-]\d{4})\s*[-:]?\s*"
)
_LOG_PREFIX_PATTERN = re.compile(
    r"^(?:(?:[A-Za-z0-9_.-]+)\s+-\s+)?(?:DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL)\s+-\s+",
    re.IGNORECASE,
)
_CORROBORATION_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
_CORROBORATION_STOP_TOKENS = {
    "task",
    "scheduled",
    "status",
    "failed",
    "failure",
    "error",
    "refresh",
    "log",
}


def _strip_runtime_log_prefixes(line: str) -> str:
    text = _ANSI_ESCAPE_PATTERN.sub("", str(line or "")).strip()
    text = _COMPOSE_PREFIX_PATTERN.sub("", text).strip()
    text = _LEADING_TIMESTAMP_PATTERN.sub("", text).strip()
    text = _LOG_PREFIX_PATTERN.sub("", text).strip()
    return _WHITESPACE_PATTERN.sub(" ", text)


def _normalize_error_signature(category: str, line: str) -> str:
    text = _strip_runtime_log_prefixes(line)
    if category == "http_5xx":
        match = _HTTP_ACCESS_PATTERN.search(text)
        if match:
            return f"http_5xx:{match.group('method')}:{match.group('path')}:{match.group('status')}"
    if category == "task_failed":
        task_match = _TASK_NAME_PATTERN.search(text)
        status_match = _STATUS_DETAIL_PATTERN.search(text)
        task = task_match.group("task") if task_match else "unknown_task"
        status = status_match.group("status") if status_match else "status_failed"
        return f"task_failed:{task}:{status}"
    if category == "database_locked":
        return "database_locked"
    if category == "python_traceback":
        return "python_traceback"
    normalized = _WHITESPACE_PATTERN.sub(" ", text.lower())
    return f"{category}:{normalized[:160]}"


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _project_relative(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except (OSError, ValueError):
        return str(path)


def _extract_timestamp(line: str) -> str | None:
    for pattern in _TIMESTAMP_PATTERNS:
        match = pattern.search(line)
        if match:
            return match.group("ts")
    return None


def _preview_line(line: str) -> str:
    text = str(line or "").strip()
    if len(text) <= _MAX_PREVIEW_CHARS:
        return text
    return f"{text[:_MAX_PREVIEW_CHARS - 3]}..."


def tail_file(path: Path, *, max_lines: int) -> list[str]:
    if max_lines <= 0:
        return []
    lines: deque[str] = deque(maxlen=max_lines)
    try:
        if not path.exists() and is_task_log_path(path):
            with io.TextIOWrapper(io.BytesIO(read_original_artifact_bytes(path)),
                                  encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    lines.append(line.rstrip("\n"))
        else:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    lines.append(line.rstrip("\n"))
    except (OSError, ValueError):
        return []
    return list(lines)


def classify_log_lines(
    lines: list[str],
    *,
    source_name: str,
    source_kind: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(lines, start=1):
        for rule in _SIGNATURE_RULES:
            if not rule["pattern"].search(line):
                continue
            events.append(
                {
                    "category": rule["category"],
                    "severity": rule["severity"],
                    "signature": _normalize_error_signature(str(rule["category"]), line),
                    "summary": rule["summary"],
                    "source_name": source_name,
                    "source_kind": source_kind,
                    "line_number": line_number,
                    "timestamp": _extract_timestamp(line),
                    "preview": _preview_line(line),
                }
            )
            break
    return events


def build_file_source(
    path: Path,
    *,
    project_root: Path,
    kind: str,
    max_lines: int,
) -> dict[str, Any]:
    exists = artifact_exists(path)
    lines = tail_file(path, max_lines=max_lines) if exists else []
    source = {
        "name": _project_relative(path, project_root),
        "kind": kind,
        "available": bool(exists),
        "line_count": len(lines),
        "max_lines": max_lines,
        "path": str(path),
        "project_path": _project_relative(path, project_root),
    }
    if exists:
        try:
            metadata = task_log_metadata(path)
            source.update(
                {
                    "size": metadata["size"],
                    "mtime": datetime.fromtimestamp(metadata["mtime_ns"] / 1_000_000_000).isoformat(timespec="seconds"),
                }
            )
        except (OSError, ValueError):
            pass
    if kind == "recent_task_logs" and lines:
        task_runtime: dict[str, Any] = {}
        for line in lines:
            start_match = _TASK_START_PATTERN.search(_strip_runtime_log_prefixes(line))
            if start_match:
                task_runtime.update(start_match.groupdict())
            finish_match = _TASK_FINISH_PATTERN.search(_strip_runtime_log_prefixes(line))
            if finish_match:
                task_runtime.update(finish_match.groupdict())
        if task_runtime:
            source["task_runtime"] = task_runtime
    source["events"] = classify_log_lines(lines, source_name=source["name"], source_kind=kind)
    return source


def build_text_source(
    name: str,
    *,
    kind: str,
    text: str,
    max_lines: int,
    available: bool = True,
    error: str = "",
) -> dict[str, Any]:
    raw_lines = str(text or "").splitlines()
    lines = raw_lines[-max_lines:] if max_lines > 0 else []
    source = {
        "name": name,
        "kind": kind,
        "available": bool(available),
        "line_count": len(lines),
        "max_lines": max_lines,
    }
    if error:
        source["error"] = error
    source["events"] = classify_log_lines(lines, source_name=name, source_kind=kind)
    return source


def collect_recent_task_log_sources(
    logs_dir: Path,
    *,
    project_root: Path,
    max_files: int,
    max_lines: int,
) -> list[dict[str, Any]]:
    task_dir = logs_dir / "tasks"
    if not task_dir.exists():
        return [
            {
                "name": "logs/tasks",
                "kind": "recent_task_logs",
                "available": False,
                "line_count": 0,
                "events": [],
                "error": "task_log_directory_missing",
            }
        ]
    files = {path for path in task_dir.glob("*.log") if path.is_file()}
    files.update(path.with_suffix("").with_suffix("") for path in task_dir.glob("*.log.gz.receipt")
                 if is_task_log_path(path.with_suffix("").with_suffix("")))

    def sort_key(path: Path) -> tuple[int, str]:
        try:
            return task_log_metadata(path)["mtime_ns"], path.name
        except (OSError, ValueError):
            return 0, path.name

    ordered = sorted(files, key=sort_key, reverse=True)
    return [
        build_file_source(path, project_root=project_root, kind="recent_task_logs", max_lines=max_lines)
        for path in ordered[:max(0, max_files)]
    ]


def collect_persistent_log_sources(
    logs_dir: Path,
    *,
    project_root: Path,
    max_lines: int,
) -> list[dict[str, Any]]:
    candidates = [
        logs_dir / "web_8090.log",
        logs_dir / "web_8090.out",
    ]
    return [
        build_file_source(path, project_root=project_root, kind="persistent_log_tail", max_lines=max_lines)
        for path in candidates
    ]


def collect_docker_compose_source(
    project_root: Path,
    *,
    since: str,
    max_lines: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    cmd = ["docker", "compose", "logs", "--since", since, "web", "scheduler"]
    try:
        result = subprocess.run(
            cmd,
            cwd=project_root,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return build_text_source(
            f"docker_compose_logs_since_{since}",
            kind="current_container_logs",
            text="",
            max_lines=max_lines,
            available=False,
            error=str(exc),
        )
    text = "\n".join(part for part in (result.stdout, result.stderr) if part)
    source = build_text_source(
        f"docker_compose_logs_since_{since}",
        kind="current_container_logs",
        text=text,
        max_lines=max_lines,
        available=result.returncode == 0,
        error="" if result.returncode == 0 else f"docker_compose_logs_exit_{result.returncode}",
    )
    source["command"] = cmd
    source["returncode"] = result.returncode
    return source


def fetch_live_api_summary(base_url: str, *, timeout_seconds: int) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/scheduler/status/detail?sample_limit=0"
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, URLError, json.JSONDecodeError, TypeError, ValueError) as exc:
        return {
            "available": False,
            "url": url,
            "error": str(exc),
            "automation_attention": None,
            "truth_policy": "live_api_unavailable_no_bug_claim",
        }
    automation = payload.get("automation_autonomy") if isinstance(payload, dict) else {}
    automation = automation if isinstance(automation, dict) else {}
    blocking_codes = automation.get("blocking_issue_codes")
    warning_codes = automation.get("warning_issue_codes")
    publication_issue_details = automation.get("publication_issue_details")
    blocking_codes = blocking_codes if isinstance(blocking_codes, list) else []
    warning_codes = warning_codes if isinstance(warning_codes, list) else []
    publication_issue_details = publication_issue_details if isinstance(publication_issue_details, dict) else {}
    automation_status = _clean_text(payload.get("automation_status"))
    manual_required = bool(automation.get("manual_intervention_required"))
    automation_attention = bool(
        blocking_codes
        or manual_required
        or publication_issue_details
        or automation_status in {"attention_required", "blocked", "error"}
    )
    return {
        "available": True,
        "url": url,
        "automation_status": automation_status,
        "automation_attention": automation_attention,
        "manual_intervention_required": manual_required,
        "blocking_issue_codes": blocking_codes,
        "warning_issue_codes": warning_codes,
        "publication_issue_detail_codes": sorted(publication_issue_details.keys()),
        "latest_successful_is_currently_published": automation.get("latest_successful_is_currently_published"),
        "latest_successful_publishable_not_current": automation.get("latest_successful_publishable_not_current"),
        "truth_policy": "live_scheduler_status_detail_only",
    }


def _sample_events(events: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    return [
        {
            "source_name": item.get("source_name"),
            "source_kind": item.get("source_kind"),
            "line_number": item.get("line_number"),
            "timestamp": item.get("timestamp"),
            "signature": item.get("signature"),
            "preview": item.get("preview"),
        }
        for item in events[:limit]
    ]


def _live_attention_correlates(
    category_events: list[dict[str, Any]],
    live_api: dict[str, Any],
) -> bool:
    """Require issue-specific evidence instead of treating any live alert as corroboration."""
    blocking_codes = [str(item or "").strip().lower() for item in live_api.get("blocking_issue_codes") or []]
    detail_codes = [
        str(item or "").strip().lower()
        for item in live_api.get("publication_issue_detail_codes") or []
    ]
    issue_codes = [item for item in (*blocking_codes, *detail_codes) if item]
    if not issue_codes:
        return False

    event_text = " ".join(
        f"{item.get('source_name') or ''} {item.get('signature') or ''} {item.get('preview') or ''}".lower()
        for item in category_events
    )
    event_tokens = set(_CORROBORATION_TOKEN_PATTERN.findall(event_text)) - _CORROBORATION_STOP_TOKENS
    for issue_code in issue_codes:
        issue_tokens = set(_CORROBORATION_TOKEN_PATTERN.findall(issue_code)) - _CORROBORATION_STOP_TOKENS
        if issue_tokens and issue_tokens <= event_tokens:
            return True
    return False


def build_runtime_log_audit_payload(
    sources: list[dict[str, Any]],
    *,
    live_api_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    live_api = dict(live_api_summary or {})
    events: list[dict[str, Any]] = []
    for source in sources:
        for event in source.get("events") or []:
            if isinstance(event, dict):
                events.append(event)

    events_by_signature: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        category = str(event.get("category") or "unknown")
        signature = str(event.get("signature") or category)
        events_by_signature[(category, signature)].append(event)

    confirmed_issues: list[dict[str, Any]] = []
    review_items: list[dict[str, Any]] = []
    observations: list[dict[str, Any]] = []
    live_attention = bool(live_api.get("automation_attention"))
    task_runtime_by_source = {
        str(source.get("name") or ""): dict(source.get("task_runtime") or {})
        for source in sources
        if isinstance(source.get("task_runtime"), dict)
    }
    successful_task_runs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source_name, runtime in task_runtime_by_source.items():
        if _clean_text(runtime.get("status")).lower() != "success":
            continue
        task_name = _clean_text(runtime.get("task_name"))
        if task_name:
            successful_task_runs[task_name].append({"source_name": source_name, **runtime})

    def later_successful_task_recovery(category_events: list[dict[str, Any]]) -> dict[str, Any] | None:
        failed_runs: list[dict[str, Any]] = []
        for event in category_events:
            runtime = task_runtime_by_source.get(_clean_text(event.get("source_name"))) or {}
            if _clean_text(runtime.get("status")).lower() == "failed" and _clean_text(runtime.get("task_name")):
                failed_runs.append(runtime)
        if not failed_runs:
            return None
        failed_run = max(failed_runs, key=lambda item: _clean_text(item.get("finished_at")))
        failed_finished_at = _clean_text(failed_run.get("finished_at"))
        later_runs = [
            item
            for item in successful_task_runs.get(_clean_text(failed_run.get("task_name")), [])
            if _clean_text(item.get("finished_at")) > failed_finished_at
        ]
        if not later_runs:
            return None
        recovered = min(later_runs, key=lambda item: _clean_text(item.get("finished_at")))
        return {
            "task_name": failed_run.get("task_name"),
            "failed_task_id": failed_run.get("task_id"),
            "failed_at": failed_finished_at or None,
            "recovery_task_id": recovered.get("task_id"),
            "recovered_at": recovered.get("finished_at"),
            "recovery_source_name": recovered.get("source_name"),
        }

    for (category, signature), category_events in sorted(events_by_signature.items()):
        severities = {str(item.get("severity") or "") for item in category_events}
        severity = "error" if "error" in severities else "warning"
        source_kinds = sorted({str(item.get("source_kind") or "") for item in category_events if item.get("source_kind")})
        source_names = sorted({str(item.get("source_name") or "") for item in category_events if item.get("source_name")})
        source_kind_count = len(source_kinds)
        item = {
            "category": category,
            "signature": signature,
            "severity": severity,
            "event_count": len(category_events),
            "source_kind_count": source_kind_count,
            "source_kinds": source_kinds,
            "source_names": source_names[:8],
            "sample_events": _sample_events(category_events),
        }
        recovery = later_successful_task_recovery(category_events) if severity == "error" else None
        if severity == "error" and recovery:
            observations.append(
                {
                    **item,
                    "resolved": True,
                    "resolution_basis": "later_successful_run_of_same_scheduler_task",
                    "recovery_evidence": recovery,
                    "observation_reason": "historical_task_failure_recovered_by_later_equivalent_automatic_run",
                    "truth_policy": "runtime_task_log_timestamps_and_same_task_identity_only",
                }
            )
        elif severity == "error" and source_kind_count >= 2:
            confirmed_issues.append(
                {
                    **item,
                    "confirmation_basis": "same_normalized_error_signature_seen_in_multiple_log_windows",
                    "truth_policy": "multi_window_runtime_log_evidence_only",
                }
            )
        elif severity == "error" and live_attention and _live_attention_correlates(category_events, live_api):
            confirmed_issues.append(
                {
                    **item,
                    "confirmation_basis": "runtime_log_error_corroborated_by_live_scheduler_attention",
                    "live_api_summary": live_api,
                    "truth_policy": "runtime_log_and_live_api_evidence_only",
                }
            )
        elif severity == "error":
            review_items.append(
                {
                    **item,
                    "review_reason": "single_window_error_signal_not_enough_to_claim_project_bug",
                    "truth_policy": "single_window_runtime_log_evidence_requires_recheck",
                }
            )
        else:
            observations.append(
                {
                    **item,
                    "observation_reason": "warning_or_deprecation_signal_not_classified_as_bug_without_repeated_runtime_failure",
                    "truth_policy": "runtime_log_warning_observation_only",
                }
            )

    unavailable_sources = [source for source in sources if not bool(source.get("available"))]
    source_summaries = []
    for source in sources:
        source_events = source.get("events") or []
        source_summaries.append(
            {
                "name": source.get("name"),
                "kind": source.get("kind"),
                "available": bool(source.get("available")),
                "line_count": int(source.get("line_count") or 0),
                "event_count": len(source_events) if isinstance(source_events, list) else 0,
                "error": source.get("error"),
                "project_path": source.get("project_path"),
            }
        )

    event_counter = Counter(str(item.get("category") or "unknown") for item in events)
    status = "ok"
    if confirmed_issues:
        status = "needs_review"
    elif review_items or observations or unavailable_sources:
        status = "watch"

    return {
        "schema_version": 1,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "passed": not confirmed_issues,
        "status": status,
        "confirmed_issue_count": len(confirmed_issues),
        "review_item_count": len(review_items),
        "observation_count": len(observations),
        "unavailable_source_count": len(unavailable_sources),
        "event_count": len(events),
        "event_category_counts": dict(sorted(event_counter.items())),
        "confirmation_policy": {
            "confirmed_issue_requires": [
                "same normalized error signature appears in at least two independent log windows",
                "or error signature is specifically correlated with a live scheduler blocking/detail code",
            ],
            "single_window_error_policy": "review_only_not_bug_claim",
            "recovered_task_error_policy": "observation_only_when_a_later_successful_run_of_the_same_scheduler_task_is_present",
            "warning_policy": "observation_only_until_runtime_failure_or_repeated_error",
        },
        "live_api_summary": live_api,
        "source_summaries": source_summaries,
        "confirmed_issues": confirmed_issues,
        "review_items": review_items,
        "observations": observations,
        "truth_policy": "real_runtime_logs_and_optional_live_api_only_no_frontend_synthesis",
    }


def collect_runtime_log_sources(
    *,
    project_root: Path,
    logs_dir: Path,
    include_docker: bool,
    docker_since: str,
    docker_timeout_seconds: int,
    max_lines_per_source: int,
    recent_task_log_count: int,
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    if include_docker:
        sources.append(
            collect_docker_compose_source(
                project_root,
                since=docker_since,
                max_lines=max_lines_per_source,
                timeout_seconds=docker_timeout_seconds,
            )
        )
    sources.extend(
        collect_recent_task_log_sources(
            logs_dir,
            project_root=project_root,
            max_files=recent_task_log_count,
            max_lines=max_lines_per_source,
        )
    )
    sources.extend(
        collect_persistent_log_sources(
            logs_dir,
            project_root=project_root,
            max_lines=max_lines_per_source,
        )
    )
    return sources


def write_runtime_log_audit(settings: Any, payload: dict[str, Any]) -> Path:
    audit_dir = Path(settings.reports_dir) / "audit" / "runtime_log_multi_window"
    artifact_path = build_audit_artifact_path(audit_dir, "runtime_log_multi_window")
    artifact_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path = audit_dir / "latest.json"
    latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return artifact_path


def main() -> int:
    parser = argparse.ArgumentParser(description="多时间窗口运行日志审计，避免把单次日志误判为项目 bug。")
    parser.add_argument("--docker-since", default="30m", help="docker compose logs --since 窗口，默认 30m。")
    parser.add_argument("--max-lines-per-source", type=int, default=500, help="每个日志源最多读取尾部行数。")
    parser.add_argument("--recent-task-log-count", type=int, default=12, help="读取最近 N 个任务日志。")
    parser.add_argument("--skip-docker", action="store_true", help="不读取 docker compose 当前窗口日志。")
    parser.add_argument("--skip-live-api", action="store_true", help="不请求 live scheduler API 佐证。")
    parser.add_argument("--base-url", default="http://127.0.0.1:8090", help="live API base URL。")
    parser.add_argument("--timeout-seconds", type=int, default=10, help="docker/live API 超时秒数。")
    args = parser.parse_args()

    settings = load_settings()
    project_root = Path(getattr(settings, "project_root", PROJECT_ROOT))
    logs_dir = Path(getattr(settings, "logs_dir", project_root / "logs"))
    sources = collect_runtime_log_sources(
        project_root=project_root,
        logs_dir=logs_dir,
        include_docker=not bool(args.skip_docker),
        docker_since=str(args.docker_since or "30m"),
        docker_timeout_seconds=max(1, int(args.timeout_seconds or 10)),
        max_lines_per_source=max(1, int(args.max_lines_per_source or 500)),
        recent_task_log_count=max(0, int(args.recent_task_log_count or 0)),
    )
    live_api_summary = (
        {}
        if args.skip_live_api
        else fetch_live_api_summary(str(args.base_url or "http://127.0.0.1:8090"), timeout_seconds=max(1, int(args.timeout_seconds or 10)))
    )
    payload = build_runtime_log_audit_payload(sources, live_api_summary=live_api_summary)
    artifact_path = write_runtime_log_audit(settings, payload)
    response = {
        "passed": payload.get("passed"),
        "status": payload.get("status"),
        "confirmed_issue_count": payload.get("confirmed_issue_count"),
        "review_item_count": payload.get("review_item_count"),
        "observation_count": payload.get("observation_count"),
        "event_category_counts": payload.get("event_category_counts"),
        "artifact_path": str(artifact_path),
        "artifact_project_path": str(artifact_path.relative_to(project_root)),
    }
    print(json.dumps(response, ensure_ascii=False, indent=2))
    return 0 if payload.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
