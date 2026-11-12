"""Bounded filesystem observations; never read fund rows or remove runtime data."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import resource
import stat
import subprocess
import sys
import time
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
MAX_ENTRIES = 1_000_000
MAX_INDEX_BYTES = 8 * 1024 * 1024
BUCKETS = {
    "online_database": "在线主数据库",
    "online_journals": "在线主库 WAL / SHM / journal",
    "backups_compressed": "压缩数据库备份",
    "backups_other": "其他数据库备份及清单",
    "history_parquet": "历史 Parquet 数据",
    "history_recovery": "历史 ZSTD 恢复包",
    "history_sqlite": "活动及其他 SQLite 历史库",
    "history_other": "历史清单及其他归档材料",
    "data_other": "其他运行状态及缓存",
    "test_workspaces": "审计中的隔离测试工作目录",
    "audit_database_copies": "审计中的数据库及恢复副本",
    "audit_implementation": "历史实施审计与恢复材料",
    "audit_research": "策略影子研究材料",
    "audit_other": "其他审计与诊断材料",
    "archived_runs": "归档报告",
    "runs": "运行报告",
    "run_calculation_inputs": "运行冻结计算输入",
    "run_revisions": "运行报告修订与导出",
    "calculation_pool": "共享计算输入与索引",
    "test_source_pool": "测试源码归档池",
    "report_artifact_pool": "共享报告产物池",
    "core_output": "筛选中间产物",
    "reports_other": "其他报告与发布状态",
    "logs": "项目日志",
    "core_logs": "核心脚本日志",
}


def _absolute(path: str | Path) -> Path:
    # Keep symlinks visible so scanning cannot silently leave the declared scope.
    return Path(os.path.abspath(path))


def _display(root: Path, path: Path) -> str:
    return str(path.relative_to(root)) if path.is_relative_to(root) else str(path)


def _layout(settings: Any) -> dict[str, Path]:
    root = _absolute(settings.project_root)
    return {
        "database": _absolute(settings.database_path),
        "data": _absolute(settings.database_path).parent,
        "reports": _absolute(settings.reports_dir),
        "runs": _absolute(settings.runs_dir),
        "archived_runs": _absolute(settings.archived_runs_dir),
        "core_output": _absolute(settings.core_output_dir),
        "logs": root / "logs",
        "core_logs": _absolute(settings.core_root) / "logs",
    }


def _policy(settings: Any) -> dict:
    policy = {
        "enabled": bool(getattr(settings, "storage_observation_enabled", False)),
        "interval_hours": int(getattr(settings, "storage_observation_interval_hours", 6)),
        "timeout_seconds": int(getattr(settings, "storage_observation_timeout_seconds", 45)),
        "retain_observations": int(getattr(settings, "storage_observation_retain_observations", 120)),
    }
    if not (1 <= policy["interval_hours"] <= 24 and 5 <= policy["timeout_seconds"] <= 60
            and 28 <= policy["retain_observations"] <= 240):
        raise ValueError("storage_observation_policy_out_of_bounds")
    return policy


def measurement_signature(settings: Any) -> str:
    root = _absolute(settings.project_root)
    basis = {"schema_version": SCHEMA_VERSION, "timezone": settings.timezone,
             "layout": {key: _display(root, path) for key, path in _layout(settings).items()},
             "collector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    return hashlib.sha256(json.dumps(basis, sort_keys=True).encode()).hexdigest()


def _bucket(path: Path, layout: dict[str, Path]) -> str:
    if path == layout["database"]:
        return "online_database"
    if str(path) in {str(layout["database"]) + suffix for suffix in ("-wal", "-shm", "-journal")}:
        return "online_journals"
    if path.is_relative_to(layout["data"] / "backups"):
        return "backups_compressed" if path.suffix == ".zst" else "backups_other"
    if path.is_relative_to(layout["data"] / "archives"):
        if path.suffix == ".parquet":
            return "history_parquet"
        if path.suffix == ".zst":
            return "history_recovery"
        if path.name.endswith((".db", ".db-wal", ".db-shm", ".db-journal")):
            return "history_sqlite"
        return "history_other"
    if path.is_relative_to(layout["data"]):
        return "data_other"
    if any(path.is_relative_to(layout["reports"] / name) for name in ("audit", ".audit")):
        relative = path.relative_to(layout["reports"])
        if "workspace" in relative.parts:
            return "test_workspaces"
        if path.name.endswith((".db", ".sqlite", ".sqlite3", ".db.zst", ".db-wal", ".db-shm", ".db-journal")):
            return "audit_database_copies"
        if len(relative.parts) > 1 and relative.parts[0] == "audit":
            family = relative.parts[1]
            if family.startswith("implementation_"):
                return "audit_implementation"
            if family in {"score_dedup_shadow", "annual_return_gate_shadow",
                          "holding_no_signal_zero_shadow", "dynamic_threshold_robustness",
                          "low_vol_risk_metric_distortion", "low_vol_annual_positive_ratio_overlap"}:
                return "audit_research"
        return "audit_other"
    for directory, key in ((".calculation_input_pool", "calculation_pool"),
                           (".test_source_pool", "test_source_pool"),
                           (".report_artifact_pool", "report_artifact_pool")):
        if path.is_relative_to(layout["reports"] / directory):
            return key
    if path.is_relative_to(layout["runs"]):
        parts = path.relative_to(layout["runs"]).parts
        if len(parts) > 1 and parts[1] == "calculation_inputs":
            return "run_calculation_inputs"
        if len(parts) > 1 and parts[1] == "revisions":
            return "run_revisions"
    for key in ("archived_runs", "runs", "core_output", "logs", "core_logs"):
        if path.is_relative_to(layout[key]):
            return key
    return "reports_other"


def _safe_ancestors(path: Path) -> None:
    for parent in (*reversed(path.parents), path):
        try:
            if stat.S_ISLNK(parent.lstat().st_mode):
                raise ValueError("storage_scope_has_symlink:" + str(parent))
        except FileNotFoundError:
            return


def collect_storage_snapshot(
    settings: Any, *, max_seconds: float = 40, clock: Callable[[], float] = time.time,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict:
    """Count unique inodes from metadata. Concurrent writes are not a DB snapshot."""
    started, started_epoch = monotonic(), clock()
    root, layout = _absolute(settings.project_root), _layout(settings)
    candidates = set(layout.values()) - {layout["database"]}
    roots = sorted(path for path in candidates if not any(path != other and path.is_relative_to(other) for other in candidates))
    counters = {key: Counter() for key in BUCKETS}
    root_counters = {str(path): Counter() for path in roots}
    seen: set[tuple[int, int]] = set()
    totals: Counter = Counter()
    errors: list[dict] = []
    error_count = 0
    roots_state: list[dict] = []
    filesystems: dict[int, dict] = {}
    legacy = {key: 0 for key in ("logs_bytes", "archive_bytes", "runs_dir_bytes", "archive_run_count")}
    legacy_roots = {"logs_bytes": layout["logs"], "archive_bytes": layout["archived_runs"],
                    "runs_dir_bytes": layout["runs"]}

    def fail(path: Path, reason: str) -> None:
        nonlocal error_count
        error_count += 1
        if len(errors) < 20:
            errors.append({"path": _display(root, path), "reason": reason})

    def bounded() -> bool:
        return totals["entries_visited"] < MAX_ENTRIES and monotonic() - started < max_seconds

    for scan_root in roots:
        if not bounded():
            fail(scan_root, "scan_resource_budget_exhausted")
            break
        state = {"path": _display(root, scan_root)}
        roots_state.append(state)
        try:
            _safe_ancestors(scan_root)
            initial = scan_root.lstat()
            if not stat.S_ISDIR(initial.st_mode):
                raise ValueError("storage_scope_is_not_a_directory")
            fs = os.statvfs(scan_root)
            filesystems.setdefault(initial.st_dev, {
                "device": initial.st_dev, "observed_at_utc": datetime.fromtimestamp(clock(), timezone.utc).isoformat(),
                "path": _display(root, scan_root), "capacity_bytes": fs.f_blocks * fs.f_frsize,
                "used_bytes": (fs.f_blocks - fs.f_bfree) * fs.f_frsize,
                "available_bytes": fs.f_bavail * fs.f_frsize,
                "reserved_bytes": (fs.f_bfree - fs.f_bavail) * fs.f_frsize,
            })
            state.update(status="present", device=initial.st_dev)
        except FileNotFoundError:
            state["status"] = "absent"
            if scan_root in (layout["data"], layout["reports"]):
                fail(scan_root, "required_storage_root_missing")
            continue
        except (OSError, ValueError) as exc:
            state.update(status="unavailable", error=str(exc))
            fail(scan_root, str(exc))
            continue
        stack = [(scan_root, initial)]
        while stack and bounded():
            path, info = stack.pop()
            totals["entries_visited"] += 1
            identity = (info.st_dev, info.st_ino)
            if identity in seen:
                totals["duplicate_inode_entries"] += 1
                continue
            seen.add(identity)
            kind = ("symlinks" if stat.S_ISLNK(info.st_mode) else "directories"
                    if stat.S_ISDIR(info.st_mode) else "regular_files"
                    if stat.S_ISREG(info.st_mode) else "special_files")
            values = {"allocated_bytes": info.st_blocks * 512, "logical_bytes": info.st_size, kind: 1}
            totals.update(values)
            counters[_bucket(path, layout)].update(values)
            root_counters[str(scan_root)].update(values)
            for key, parent in legacy_roots.items():
                if path.is_relative_to(parent) and kind == "regular_files":
                    legacy[key] += info.st_size
            if path.parent == layout["archived_runs"] and kind == "directories":
                legacy["archive_run_count"] += 1
            if kind != "directories":
                continue
            # Do not traverse a symlink swapped in between lstat and scandir.
            try:
                fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    opened = os.fstat(fd)
                    if (opened.st_dev, opened.st_ino) != identity:
                        raise ValueError("directory_changed_during_scan")
                    children = []
                    with os.scandir(fd) as iterator:
                        for entry in iterator:
                            if not bounded() or len(children) + totals["entries_visited"] >= MAX_ENTRIES:
                                raise ValueError("scan_resource_budget_exhausted")
                            try:
                                children.append((path / entry.name, entry.stat(follow_symlinks=False)))
                            except OSError as exc:
                                fail(path / entry.name, str(exc))
                    # Stable ownership for hardlinks in different storage categories.
                    stack.extend(sorted(children, key=lambda item: str(item[0]), reverse=True))
                finally:
                    os.close(fd)
            except (OSError, ValueError) as exc:
                fail(path, str(exc))
        if stack:
            fail(scan_root, "scan_resource_budget_exhausted")

    named_files = {
        "database_bytes": layout["database"],
        "latest_report_bytes": _absolute(settings.latest_report_file),
        "publication_registry_bytes": _absolute(settings.publication_registry_file),
        "autostock_rate_limit_state_bytes": _absolute(settings.autostock_rate_limit_shared_state_path),
    }
    for key, path in named_files.items():
        try:
            target = path.resolve(strict=True)
            if not any(target.is_relative_to(scan_root) for scan_root in roots):
                raise ValueError("named_file_target_outside_storage_scope")
            value = target.stat()
            if not stat.S_ISREG(value.st_mode):
                raise ValueError("named_file_is_not_regular")
            legacy[key] = value.st_size
        except (OSError, ValueError) as exc:
            legacy[key] = None
            if key == "database_bytes":
                fail(path, str(exc))

    complete = error_count == 0

    def measured(values: Counter) -> dict:
        return {**values, "allocated_bytes": values["allocated_bytes"] if complete else None,
                "logical_bytes": values["logical_bytes"] if complete else None,
                "observed_allocated_bytes": values["allocated_bytes"],
                "observed_logical_bytes": values["logical_bytes"]}

    if not complete:
        for key in (*legacy_roots, "archive_run_count"):
            legacy[key] = None
    finished_epoch = clock()
    external = collect_external_archive(settings, max_seconds=max(0, min(5, max_seconds - (monotonic() - started))))
    return {
        "schema_version": SCHEMA_VERSION, "status": "complete" if complete else "partial",
        "measurement_signature": measurement_signature(settings),
        "started_at_utc": datetime.fromtimestamp(started_epoch, timezone.utc).isoformat(),
        "observed_at_utc": datetime.fromtimestamp(finished_epoch, timezone.utc).isoformat(),
        "observed_at_local": datetime.fromtimestamp(finished_epoch, ZoneInfo(settings.timezone)).isoformat(),
        "duration_seconds": round(monotonic() - started, 3),
        "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "totals": measured(totals),
        "external_archive": external,
        "buckets": {key: {"label": label, **measured(counters[key])} for key, label in BUCKETS.items()},
        "roots": [{**row, **measured(root_counters[str(root / row["path"])])}
                  if not Path(row["path"]).is_absolute() else
                  {**row, **measured(root_counters[row["path"]])} for row in roots_state],
        "filesystems": list(filesystems.values()), "legacy_logical_metrics": legacy,
        "error_count": error_count, "errors": errors,
        "semantics": {
            "scope": "configured runtime data, reports, core output and both log directories; excludes other projects, Docker layers and source outside this scope",
            "allocated_bytes": "st_blocks * 512, directories and symlinks included, each device/inode counted once",
            "logical_bytes": "st_size of those same inodes; not physical disk usage",
            "hardlink_ownership": "first path in sorted depth-first traversal; category attribution can change if links move",
            "consistency": "metadata sampled during the stated interval; concurrent writes and reflink/CoW sharing are not resolved",
            "partial_values": "observed_* are partial counts only when status is partial; complete totals remain null",
            "legacy_logical_metrics": "overlapping compatibility diagnostics; never add to totals",
            "business_rows": "not queried; no NAV, decision or task row growth is inferred",
        },
    }


def collect_external_archive(settings: Any, *, max_seconds: float = 5) -> dict:
    """Separate scope: moving here does not imply that disk blocks were freed."""
    path = Path(getattr(settings, 'storage_lifecycle_archive_root', '/mnt/Project-Archives/Fund-ANA'))
    result = {'path': str(path), 'status': 'not_mounted', 'allocated_bytes': None, 'logical_bytes': None,
              'semantics': 'separate inode-deduplicated scope; shared links with production may overlap; same-disk migration is not disk reclamation'}
    if not path.exists():
        return result
    started, seen, allocated, logical, count = time.monotonic(), set(), 0, 0, 0
    try:
        _safe_ancestors(path)
        stack = [path]
        while stack:
            if time.monotonic() - started >= max_seconds:
                result.update(status='partial', observed_allocated_bytes=allocated, observed_logical_bytes=logical)
                return result
            target = stack.pop()
            info = target.lstat()
            key = (info.st_dev, info.st_ino)
            if key in seen:
                continue
            seen.add(key)
            count += 1
            allocated += info.st_blocks * 512
            logical += info.st_size
            if stat.S_ISDIR(info.st_mode):
                stack.extend(target.iterdir())
        result.update(status='complete', allocated_bytes=allocated, logical_bytes=logical, unique_entries=count)
    except OSError as exc:
        result.update(status='unavailable', error=str(exc))
    return result


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()


def _index_path(settings: Any) -> Path:
    return Path(settings.reports_dir) / ".audit/storage_usage/observations.json"


def _read_index(path: Path) -> dict:
    try:
        with path.open("rb") as stream:
            body = stream.read(MAX_INDEX_BYTES + 1)
    except FileNotFoundError:
        return {"schema_version": SCHEMA_VERSION, "observations": []}
    if len(body) > MAX_INDEX_BYTES:
        raise ValueError("storage_observation_index_too_large")
    state = json.loads(body)
    if not isinstance(state, dict):
        raise ValueError("storage_observation_index_invalid")
    entries = state.get("observations")
    if state.get("schema_version") != SCHEMA_VERSION or not isinstance(entries, list) or len(entries) > 240:
        raise ValueError("storage_observation_index_invalid")
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("observation_id"):
            raise ValueError("storage_observation_entry_invalid")
        snapshot = entry.get("snapshot")
        if snapshot is not None and hashlib.sha256(_canonical(snapshot)).hexdigest() != entry.get("snapshot_sha256"):
            raise ValueError("storage_observation_snapshot_hash_mismatch")
    return state


def _write_index(path: Path, value: dict) -> None:
    body = _canonical(value)
    if len(body) > MAX_INDEX_BYTES:
        raise ValueError("storage_observation_index_too_large")
    temporary = path.with_name("." + path.name + "." + uuid4().hex + ".tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def _growth(entries: list[dict], signature: str) -> dict:
    points = [entry for entry in entries if entry.get("measurement_signature") == signature
              and entry.get("status") == "complete" and entry.get("trigger_source") in {"cron", "interval"}]
    daily = {}
    for entry in sorted(points, key=lambda row: row["snapshot"]["observed_at_utc"]):
        snapshot = entry["snapshot"]
        daily[snapshot["observed_at_local"][:10]] = snapshot
    ordered = list(daily.values())
    result = {"status": "insufficient_natural_history", "complete_natural_window_count": len(points),
              "distinct_local_dates": len(ordered), "required_local_dates": 7,
              "elapsed_days": None, "net_allocated_change_bytes": None,
              "average_net_allocated_bytes_per_day": None, "by_bucket": None,
              "note": "last complete observation per local date; net change includes creation and cleanup, not a forecast or gross business data growth"}
    if len(ordered) < 2:
        return result
    first, last = ordered[0], ordered[-1]
    days = (datetime.fromisoformat(last["observed_at_utc"]) - datetime.fromisoformat(first["observed_at_utc"])).total_seconds() / 86400
    result["elapsed_days"] = round(days, 6)
    if len(ordered) < 7 or days < 6:
        return result
    delta = last["totals"]["allocated_bytes"] - first["totals"]["allocated_bytes"]
    result.update(status="observed", first_observed_at=first["observed_at_utc"],
                  last_observed_at=last["observed_at_utc"], net_allocated_change_bytes=delta,
                  average_net_allocated_bytes_per_day=round(delta / days, 3),
                  by_bucket={key: {
                      "net_allocated_change_bytes": last["buckets"][key]["allocated_bytes"] - first["buckets"][key]["allocated_bytes"],
                      "average_net_allocated_bytes_per_day": round(
                          (last["buckets"][key]["allocated_bytes"] - first["buckets"][key]["allocated_bytes"]) / days, 3),
                  } for key in BUCKETS})
    return result


def observe_due_storage_usage(
    settings: Any, *, task_id: int | None, task_name: str, trigger_source: str,
    log: Callable[[str], None], clock: Callable[[], float] = time.time,
) -> dict:
    """Run under scheduler ownership. Observer failure does not change business status."""
    if trigger_source not in {"cron", "interval"}:
        return {"status": "skipped", "reason": "not_an_automatic_trigger"}
    policy = _policy(settings)
    if not policy["enabled"]:
        return {"status": "skipped", "reason": "storage_observation_disabled"}
    signature = measurement_signature(settings)
    path = _index_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    window = int(clock() // (policy["interval_hours"] * 3600))
    with path.with_suffix(".lock").open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "skipped", "reason": "storage_observer_busy"}
        state = _read_index(path)
        entries = state["observations"]
        interrupted = False
        for previous in entries:
            if previous.get("status") == "running":
                previous.update(status="interrupted", error="observer_lock_released_before_completion")
                interrupted = True
        if interrupted:
            _write_index(path, state)
        existing = [entry for entry in entries if entry.get("measurement_signature") == signature
                    and entry.get("window_id") == window and entry.get("status") != "interrupted"]
        if existing:
            return {"status": "skipped", "reason": "window_already_attempted", "window_id": window,
                    "last_attempt_status": existing[-1]["status"], "observation_id": existing[-1]["observation_id"]}
        entry = {
            "observation_id": uuid4().hex, "measurement_signature": signature, "window_id": window,
            "task_id": task_id, "task_name": task_name, "trigger_source": trigger_source,
            "started_at_utc": datetime.fromtimestamp(clock(), timezone.utc).isoformat(), "status": "running",
        }
        entries.append(entry)
        state["observations"] = entries[-policy["retain_observations"]:]
        state["policy"] = policy
        _write_index(path, state)
        started = time.monotonic()
        command = [sys.executable, "scripts/observe_storage_usage.py", "--config", str(settings.config_path),
                   "--max-seconds", str(policy["timeout_seconds"] - 2)]
        log("[storage-observation] 开始只读容量采样；timeout=" + str(policy["timeout_seconds"]) + "s")
        try:
            completed = subprocess.run(command, cwd=settings.project_root, capture_output=True, text=True,
                                       timeout=policy["timeout_seconds"], check=False)
            entry.update(return_code=completed.returncode, stderr_tail=completed.stderr[-2000:], timed_out=False)
            if completed.returncode not in (0, 2):
                raise ValueError("storage_observation_subprocess_failed")
            snapshot = json.loads(completed.stdout)
            if (snapshot.get("schema_version") != SCHEMA_VERSION
                    or snapshot.get("measurement_signature") != signature
                    or snapshot.get("status") not in {"complete", "partial"}
                    or (snapshot["status"] == "complete") != (completed.returncode == 0)):
                raise ValueError("storage_observation_subprocess_contract_mismatch")
            entry.update(status=snapshot["status"], snapshot=snapshot,
                         snapshot_sha256=hashlib.sha256(_canonical(snapshot)).hexdigest())
        except subprocess.TimeoutExpired:
            entry.update(status="failed", error="storage_observation_timeout", timed_out=True, return_code=None)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            entry.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        entry.update(finished_at_utc=datetime.fromtimestamp(clock(), timezone.utc).isoformat(),
                     duration_seconds=round(time.monotonic() - started, 3))
        _write_index(path, state)
        snapshot = entry.get("snapshot") or {}
        result = {key: value for key, value in entry.items() if key != "snapshot"}
        result.update(allocated_bytes=(snapshot.get("totals") or {}).get("allocated_bytes"),
                      logical_bytes=(snapshot.get("totals") or {}).get("logical_bytes"),
                      peak_process_rss_kib=snapshot.get("peak_process_rss_kib"),
                      error_count=snapshot.get("error_count"),
                      index_path=_display(_absolute(settings.project_root), path))
        log(f"[storage-observation] status={entry['status']} exit={entry.get('return_code')} "
            f"allocated_bytes={result['allocated_bytes']} seconds={entry['duration_seconds']} "
            f"peak_rss_kib={result['peak_process_rss_kib']} errors={result['error_count']}")
        return result


def load_storage_usage(settings: Any, *, clock: Callable[[], float] = time.time) -> dict:
    """API reads bounded observations only; it never scans directories or queries SQL."""
    legacy = {key: None for key in (
        "logs_bytes", "archive_bytes", "runs_dir_bytes", "archive_run_count", "latest_report_bytes",
        "publication_registry_bytes", "database_bytes", "autostock_rate_limit_state_bytes")}
    result = {**legacy, "schema_version": SCHEMA_VERSION, "status": "not_observed",
              "source": "automatic_storage_observations", "snapshot": None, "last_attempt": None,
              "age_seconds": None, "stale": None, "growth": None}
    try:
        policy = _policy(settings)
        result["enabled"] = policy["enabled"]
        state = _read_index(_index_path(settings))
        signature = measurement_signature(settings)
        entries = [entry for entry in state["observations"] if entry.get("measurement_signature") == signature]
        result["retained_observation_count"] = len(entries)
        result["growth"] = _growth(entries, signature)
        if not entries:
            return result
        last = entries[-1]
        result["last_attempt"] = {key: value for key, value in last.items() if key != "snapshot"}
        complete = [entry for entry in entries if entry.get("status") == "complete"]
        if not complete:
            result.update(status=last["status"], snapshot=last.get("snapshot"))
            return result
        snapshot = complete[-1]["snapshot"]
        age = clock() - datetime.fromisoformat(snapshot["observed_at_utc"]).timestamp()
        stale = age < 0 or age > policy["interval_hours"] * 3600 + 1800
        result.update(snapshot["legacy_logical_metrics"])
        result.update(status="stale" if stale else "ok" if last["status"] == "complete" else "degraded",
                      snapshot=snapshot, snapshot_observation_id=complete[-1]["observation_id"],
                      age_seconds=round(age, 3), stale=stale)
        return result
    except (OSError, ValueError, KeyError, TypeError) as exc:
        result.update(status="unavailable", error=f"{type(exc).__name__}: {exc}")
        return result
