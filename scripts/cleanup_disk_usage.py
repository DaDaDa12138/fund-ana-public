"""项目磁盘占用 TTL 清理（P0-02）。

治理以下会持续累积的产物：
1. ``reports/archive/superseded_runs/`` —— 每个被降级的 run 都会移进来，长期占用磁盘
   （2026-04-18 实测 4.1G / 152 个 run）。
2. ``logs/*.log`` 和配置的 ``core_root/logs/*.log`` —— ``enhanced_logger`` 按日期命名，
   ``safe_rotating_handler`` 只在单文件超 50MB 时轮转，不会清理"过期的日期文件"。
3. ``backend/data/maintenance/*_vacuum/*_vacuum_candidate_*.db`` —— NAV/rank
   VACUUM INTO 生成的候选库是可再生成的临时副本，单个文件接近主库大小，若替换窗口
   未消费会快速堆积。
4. ``logs/tasks/task_<id>_<name>.log`` —— 超过日志保留期的大任务日志在原终态核验后
   压缩，完整恢复成功才移除未压缩副本；原路径通过兼容reader与下载接口读取。
5. 配置的核心输出顶层历史 CSV —— 至少保留30天普通文件；已关闭的输出持
   workflow锁、逐份完整恢复后压缩保存，每批最多256份／512 MiB，原路径仍可读。
6. 已退役成功测试工作区的旧源码包 —— 保留至少24小时，每批最多8份／256 MiB
   原源码／120秒；恢复核验后迁至独立对象包，旧清单由恢复入口继续解析。
   版本恢复所直接依赖、没有原工作区退役链的源码包不自动迁移。

本脚本由 ``scheduler.weekly_maintenance`` 末段自动调用；也可以独立 CLI 调用：

    python3 scripts/cleanup_disk_usage.py                # 仅 dry-run，列出候选
    python3 scripts/cleanup_disk_usage.py --apply        # 真实删除
    python3 scripts/cleanup_disk_usage.py --logs-only --apply  # 仅治理两处日期日志
    python3 scripts/cleanup_disk_usage.py --logs-keep-days 14 --archive-keep-count 60 --archive-keep-days 30

保留策略：
- archive：保留最近 ``keep-count`` 个细粒度 run，并在最近 ``keep-days`` 天内每个自然日
  额外保留一个最新检查点（默认 60 / 30 天）。这样既保留近期排障粒度和跨日证据，
  又不会因为同一天反复补跑数十次而让 superseded 目录无上限增长。
- logs：保留最近 ``keep-days`` 天（默认 14 天）。仅清理日期和最后写入时间均过期的
  ``*_YYYY-MM-DD.log*`` 和 ``analyzer_<类型>_<周期>_YYYYMMDD.log`` 普通文件；
  任务子目录、未知名称、软链接和近期写入的文件保留。
- maintenance candidates：默认不保留旧候选库，但保护最近 ``min-age-hours`` 小时内
  仍可能处于生成/复核窗口的文件；也可以用 ``keep-count`` 为每个 candidate 目录保留
  最近 N 个候选库。
- task logs：保留至少1天、默认14天未压缩日志；每轮最多32份／128 MiB，单文件
  256 KiB至64 MiB。原任务状态、内容和SHA保持，压缩历史不按此规则删除。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import stat
import sys
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import Settings, load_settings  # noqa: E402


DEFAULT_ARCHIVE_KEEP_COUNT = 60
DEFAULT_ARCHIVE_KEEP_DAYS = 30
DEFAULT_LOGS_KEEP_DAYS = 14
DEFAULT_MAINTENANCE_CANDIDATE_KEEP_COUNT = 0
DEFAULT_MAINTENANCE_CANDIDATE_MIN_AGE_HOURS = 6
DEFAULT_MAINTENANCE_CANDIDATE_DIRS = (
    "backend/data/maintenance/nav_history_vacuum",
    "backend/data/maintenance/rank_history_vacuum",
)

LOG_DATE_SUFFIX_RE = re.compile(
    # `collector_2026-04-18.log` 或 rotate 后的 `collector_2026-04-18.log.N`
    r"^(?P<prefix>[a-zA-Z_.]+)_(?P<date>\d{4}-\d{2}-\d{2})\.log(?P<rotate_suffix>(\.\d+)?)$"
)
ANALYZER_LOG_DATE_RE = re.compile(
    # BaseAnalyzer._setup_logger writes this exact family with FileHandler.
    # Compact dates in unrelated files and task evidence remain out of scope.
    r"^analyzer_(?P<fund_type>[^/\\\r\n]+)_[1-9]\d*(?:day|year)_"
    r"(?P<date>\d{8})\.log$"
)
_RUN_ID_DATE_RE = re.compile(r"^(?P<date>\d{8})_(?P<time>\d{6})")
MAINTENANCE_CANDIDATE_RE = re.compile(
    r"^.+_vacuum_candidate_(?P<timestamp>\d{8}_\d{6})\.db$"
)


@dataclass(slots=True)
class CleanupPlanItem:
    path: Path
    size_bytes: int
    reason: str
    file_identity: tuple[int, int, int, int, int] | None = None


@dataclass(slots=True)
class CleanupReport:
    lifecycle_policy_active: bool = False
    archive_kept: list[Path] = field(default_factory=list)
    archive_removed: list[CleanupPlanItem] = field(default_factory=list)
    logs_kept: list[Path] = field(default_factory=list)
    logs_removed: list[CleanupPlanItem] = field(default_factory=list)
    log_roots: list[Path] = field(default_factory=list)
    logs_only: bool = False
    maintenance_candidates_kept: list[Path] = field(default_factory=list)
    maintenance_candidates_removed: list[CleanupPlanItem] = field(default_factory=list)
    archive_keep_count: int = 0
    archive_keep_days: int = 0
    logs_keep_days: int = 0
    maintenance_candidate_keep_count: int = 0
    maintenance_candidate_min_age_hours: int = 0
    dry_run: bool = True
    generated_at: str = ""
    publication_registry_refreshed: bool = False
    publication_registry_refresh_error: str | None = None
    forward_cache_source_preservation: list[dict[str, Any]] = field(default_factory=list)
    planned: dict[str, list[CleanupPlanItem]] = field(default_factory=dict)
    failures: dict[str, list[dict[str, str]]] = field(default_factory=dict)
    removed_allocated_bytes_estimate: dict[str, int | None] = field(default_factory=dict)
    partial_removal_possible: bool = False
    forward_cache_accounting_complete: bool = True
    calculation_input_pool_gc: dict[str, Any] | None = None
    report_artifact_pool_gc: dict[str, Any] | None = None
    task_log_compression: dict[str, Any] | None = None
    core_csv_compression: dict[str, Any] | None = None
    test_source_pool_gc: dict[str, Any] | None = None
    test_source_migration: dict[str, Any] | None = None
    shared_audit_csv_compression: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        def section(name: str, kept: list[Path], items: list[CleanupPlanItem]) -> dict[str, Any]:
            planned = self.planned.get(name, items if self.dry_run else [])
            removed = [] if self.dry_run else items

            def describe(item: CleanupPlanItem) -> dict[str, Any]:
                return {"path": str(item.path), "size_bytes": item.size_bytes, "reason": item.reason}

            return {
                "kept_total": len(kept),
                "planned_total": len(planned),
                "planned_bytes_total": sum(item.size_bytes for item in planned),
                "planned": [describe(item) for item in planned],
                "removed_total": len(removed),
                "removed_bytes_total": sum(item.size_bytes for item in removed),
                "removed": [describe(item) for item in removed],
                "failed_total": len(self.failures.get(name, [])),
                "failed": list(self.failures.get(name, [])),
                "removed_allocated_bytes_estimate": self.removed_allocated_bytes_estimate.get(name, 0),
            }

        sections = {
            "archive": section("archive", self.archive_kept, self.archive_removed),
            "logs": section("logs", self.logs_kept, self.logs_removed),
            "maintenance_candidates": section("maintenance_candidates", self.maintenance_candidates_kept,
                                              self.maintenance_candidates_removed),
        }
        sections["logs"]["roots"] = [str(path) for path in self.log_roots]
        removed_logical = sum(value["removed_bytes_total"] for value in sections.values())
        added_logical = (sum(item["new_cache_bytes"] for entry in self.forward_cache_source_preservation
                             for item in entry["preserved"])
                         if self.forward_cache_accounting_complete else None)
        allocated = [value["removed_allocated_bytes_estimate"] for value in sections.values()]
        return {
            "schema_version": 2,
            "generated_at": self.generated_at,
            "dry_run": self.dry_run,
            "scope": "dated_logs" if self.logs_only else "full",
            "archive_keep_count": self.archive_keep_count,
            "archive_keep_days": self.archive_keep_days,
            "archive_retention_mode": ("offline_lifecycle_no_historical_deletion" if self.lifecycle_policy_active
                                       else "latest_count_plus_daily_checkpoints"),
            "logs_keep_days": self.logs_keep_days,
            "maintenance_candidate_keep_count": self.maintenance_candidate_keep_count,
            "maintenance_candidate_min_age_hours": self.maintenance_candidate_min_age_hours,
            "publication_registry_refreshed": self.publication_registry_refreshed,
            "publication_registry_refresh_error": self.publication_registry_refresh_error,
            "forward_cache_source_preservation": list(self.forward_cache_source_preservation),
            "forward_cache_added_bytes": added_logical,
            "calculation_input_pool_gc": self.calculation_input_pool_gc,
            "report_artifact_pool_gc": self.report_artifact_pool_gc,
            "task_log_compression": self.task_log_compression,
            "core_csv_compression": self.core_csv_compression,
            "test_source_pool_gc": self.test_source_pool_gc,
            "test_source_migration": self.test_source_migration,
            "shared_audit_csv_compression": self.shared_audit_csv_compression,
            "accounting": {
                "removed_logical_bytes": removed_logical,
                "forward_cache_added_logical_bytes": added_logical,
                "net_logical_bytes": (removed_logical - added_logical
                                      if added_logical is not None and not self.partial_removal_possible else None),
                "partial_removal_possible": self.partial_removal_possible,
                "removed_allocated_bytes_estimate": sum(allocated) if all(v is not None for v in allocated) else None,
                "net_reclaimed_disk_bytes": None,
                "scope": "Completed removals only; partial directory failures are incomplete. Allocation is a gross estimate from st_blocks for removed last links and directories, excluding new cache and audit files. Open descriptors and concurrent writers can delay or change physical reclamation; no net disk measurement is claimed.",
            },
            **sections,
            "errors": list(self.errors),
        }


def _dir_size_bytes(path: Path) -> int:
    total = 0
    try:
        for entry in path.rglob("*"):
            if entry.is_file() and not entry.is_symlink():
                try:
                    total += entry.stat().st_size
                except OSError:
                    continue
    except OSError:
        pass
    return total


def _archive_run_date(run_dir: Path) -> datetime | None:
    match = _RUN_ID_DATE_RE.match(run_dir.name)
    if not match:
        return None
    try:
        return datetime.strptime(
            f"{match.group('date')}_{match.group('time')}",
            "%Y%m%d_%H%M%S",
        )
    except ValueError:
        return None


def plan_archive_cleanup(
    archive_root: Path,
    *,
    keep_count: int,
    keep_days: int,
    now: datetime | None = None,
) -> tuple[list[Path], list[CleanupPlanItem]]:
    if not archive_root.exists() or _has_symlink_parent(archive_root):
        return [], []

    entries: list[tuple[Path, datetime]] = []
    undated: list[Path] = []
    for entry in sorted(archive_root.iterdir()):
        if entry.is_symlink():
            undated.append(entry)
            continue
        if not entry.is_dir():
            continue
        dt = _archive_run_date(entry)
        if dt is None:
            undated.append(entry)
            continue
        entries.append((entry, dt))

    entries.sort(key=lambda item: item[1], reverse=True)
    now = now or datetime.now()
    # 日期比较用 .date() 规避 hh:mm:ss 误差：
    # 归档 run_id 只含日期部分，cutoff 用 now 的秒会导致同日归档被误判过期。
    cutoff_date = (now - timedelta(days=max(0, int(keep_days)))).date()

    kept: list[Path] = list(undated)
    removed: list[CleanupPlanItem] = []
    daily_checkpoint_dates: set[Any] = set()
    for index, (path, dt) in enumerate(entries):
        keep_by_count = index < max(0, int(keep_count))
        keep_by_daily_checkpoint = (
            dt.date() >= cutoff_date
            and dt.date() not in daily_checkpoint_dates
        )
        if keep_by_count or keep_by_daily_checkpoint:
            kept.append(path)
            daily_checkpoint_dates.add(dt.date())
            continue
        removed.append(
            CleanupPlanItem(
                path=path,
                size_bytes=_dir_size_bytes(path),
                reason=(
                    f"beyond latest_count={keep_count}+daily_checkpoint_days={keep_days} "
                    f"(run_at={dt:%Y-%m-%dT%H:%M:%S})"
                ),
                file_identity=_log_file_identity(path.lstat()),
            )
        )
    return kept, removed


def plan_logs_cleanup(
    logs_root: Path,
    *,
    keep_days: int,
    now: datetime | None = None,
) -> tuple[list[Path], list[CleanupPlanItem]]:
    if not logs_root.exists() or _has_symlink_parent(logs_root):
        return [], []

    now = now or datetime.now()
    cutoff_date = (now - timedelta(days=max(0, int(keep_days)))).date()

    kept: list[Path] = []
    removed: list[CleanupPlanItem] = []
    for entry in sorted(logs_root.iterdir()):
        if entry.is_symlink():
            kept.append(entry)
            continue
        if not entry.is_file():
            continue
        match = LOG_DATE_SUFFIX_RE.fullmatch(entry.name)
        date_format = "%Y-%m-%d"
        if not match:
            match = ANALYZER_LOG_DATE_RE.fullmatch(entry.name)
            date_format = "%Y%m%d"
        if not match:
            kept.append(entry)
            continue
        try:
            file_date = datetime.strptime(match.group("date"), date_format).date()
        except ValueError:
            kept.append(entry)
            continue

        if file_date >= cutoff_date:
            kept.append(entry)
            continue
        try:
            info = entry.lstat()
        except OSError:
            kept.append(entry)
            continue
        # Long-lived handlers can keep writing to a file named on an earlier day.
        modified_date = datetime.fromtimestamp(info.st_mtime, tz=now.tzinfo).date()
        if not stat.S_ISREG(info.st_mode) or modified_date >= cutoff_date:
            kept.append(entry)
            continue
        removed.append(
            CleanupPlanItem(
                path=entry,
                size_bytes=info.st_size,
                reason=f"log older than {keep_days} days (file_date={file_date.isoformat()})",
                file_identity=_log_file_identity(info),
            )
        )
    return kept, removed


def _has_symlink_parent(path: Path) -> bool:
    return any(parent.is_symlink() for parent in (path, *path.parents))


def _log_file_identity(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def plan_maintenance_candidate_cleanup(
    project_root: Path,
    *,
    keep_count: int,
    min_age_hours: int,
    candidate_dirs: list[Path] | tuple[Path, ...] | None = None,
    now: datetime | None = None,
) -> tuple[list[Path], list[CleanupPlanItem]]:
    now = now or datetime.now()
    normalized_keep_count = max(0, int(keep_count))
    normalized_min_age_hours = max(0, int(min_age_hours))
    roots = list(candidate_dirs or [project_root / item for item in DEFAULT_MAINTENANCE_CANDIDATE_DIRS])

    kept: list[Path] = []
    removed: list[CleanupPlanItem] = []
    for root in roots:
        if not root.exists() or _has_symlink_parent(root):
            continue
        entries: list[tuple[Path, datetime]] = []
        try:
            candidates = sorted(root.iterdir())
        except OSError:
            continue
        for entry in candidates:
            if entry.is_symlink() or not entry.is_file():
                continue
            if MAINTENANCE_CANDIDATE_RE.match(entry.name) is None:
                continue
            try:
                modified_at = datetime.fromtimestamp(entry.stat().st_mtime)
            except OSError:
                continue
            entries.append((entry, modified_at))

        entries.sort(key=lambda item: (item[1], item[0].name), reverse=True)
        for index, (path, modified_at) in enumerate(entries):
            keep_by_count = index < normalized_keep_count
            age_hours = (now - modified_at).total_seconds() / 3600
            keep_by_age = age_hours < normalized_min_age_hours
            if keep_by_count or keep_by_age:
                kept.append(path)
                continue
            try:
                info = path.lstat()
            except OSError:
                kept.append(path)
                continue
            removed.append(
                CleanupPlanItem(
                    path=path,
                    size_bytes=info.st_size,
                    reason=(
                        "maintenance VACUUM candidate beyond "
                        f"keep_count={normalized_keep_count}+min_age_hours="
                        f"{normalized_min_age_hours} "
                        f"(modified_at={modified_at.isoformat(timespec='seconds')})"
                    ),
                    file_identity=_log_file_identity(info),
                )
            )
    return kept, removed


def _path_footprint(path: Path) -> tuple[int, int]:
    """Measure deleted names, counting blocks only if their last links go away.

    Do not follow symlinks or count a shared file's blocks once per pathname.
    This is a gross estimate, not a filesystem-wide before/after measurement.
    """
    pending = [path]
    logical = directory_blocks = 0
    files: dict[tuple[int, int], tuple[os.stat_result, int]] = {}
    while pending:
        current = pending.pop()
        info = current.lstat()
        if stat.S_ISDIR(info.st_mode):
            directory_blocks += info.st_blocks * 512
            with os.scandir(current) as entries:
                pending.extend(Path(entry.path) for entry in entries)
        else:
            logical += info.st_size
            key = info.st_dev, info.st_ino
            files[key] = info, files.get(key, (info, 0))[1] + 1
    allocated = directory_blocks + sum(info.st_blocks * 512 for info, links in files.values()
                                       if links == info.st_nlink)
    return logical, allocated


def _remove_path(path: Path, errors: list[str], *,
                 expected_identity: tuple[int, int, int, int, int] | None = None) -> tuple[int, int] | None:
    try:
        info = path.lstat()
        if (_has_symlink_parent(path)
                or expected_identity is None or _log_file_identity(info) != expected_identity):
            raise OSError("path changed after cleanup planning; deletion not performed")
        footprint = _path_footprint(path)
        if stat.S_ISDIR(info.st_mode):
            shutil.rmtree(path)
        else:
            path.unlink()
        return footprint
    except OSError as exc:  # pragma: no cover - defensive io
        errors.append(f"{path}: {exc}")
        return None


def _resolve_int_setting(settings: Settings, field_name: str, raw_value: int | None, default: int) -> int:
    if raw_value is not None:
        return int(raw_value)
    value = getattr(settings, field_name, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _settings_support_publication_registry_refresh(settings: Settings) -> bool:
    return all(
        hasattr(settings, name)
        for name in (
            "runs_dir",
            "archived_runs_dir",
            "reports_dir",
            "publication_registry_file",
            "effective_nav_coverage_ratio",
            "effective_nav_weekday_only",
            "database_path",
        )
    )


def run_cleanup(
    settings: Settings | None = None,
    *,
    archive_keep_count: int | None = None,
    archive_keep_days: int | None = None,
    logs_keep_days: int | None = None,
    maintenance_candidate_keep_count: int | None = None,
    maintenance_candidate_min_age_hours: int | None = None,
    logs_only: bool = False,
    apply: bool = False,
    now: datetime | None = None,
) -> CleanupReport:
    settings = settings or load_settings()
    now = now or datetime.now()

    archive_keep_count = _resolve_int_setting(
        settings,
        "scheduler_weekly_disk_cleanup_archive_keep_count",
        archive_keep_count,
        DEFAULT_ARCHIVE_KEEP_COUNT,
    )
    archive_keep_days = _resolve_int_setting(
        settings,
        "scheduler_weekly_disk_cleanup_archive_keep_days",
        archive_keep_days,
        DEFAULT_ARCHIVE_KEEP_DAYS,
    )
    logs_keep_days = _resolve_int_setting(
        settings,
        "scheduler_weekly_disk_cleanup_logs_keep_days",
        logs_keep_days,
        DEFAULT_LOGS_KEEP_DAYS,
    )
    maintenance_candidate_keep_count = _resolve_int_setting(
        settings,
        "scheduler_weekly_disk_cleanup_maintenance_candidate_keep_count",
        maintenance_candidate_keep_count,
        DEFAULT_MAINTENANCE_CANDIDATE_KEEP_COUNT,
    )
    maintenance_candidate_min_age_hours = _resolve_int_setting(
        settings,
        "scheduler_weekly_disk_cleanup_maintenance_candidate_min_age_hours",
        maintenance_candidate_min_age_hours,
        DEFAULT_MAINTENANCE_CANDIDATE_MIN_AGE_HOURS,
    )

    archive_root = Path(settings.archived_runs_dir)
    project_root = Path(settings.project_root)
    core_root = Path(getattr(settings, "core_root", project_root / "core"))
    log_roots = list(dict.fromkeys((project_root / "logs", core_root / "logs")))

    report = CleanupReport(
        lifecycle_policy_active=bool(getattr(settings, 'storage_lifecycle_enabled', False)),
        archive_keep_count=int(archive_keep_count),
        archive_keep_days=int(archive_keep_days),
        logs_keep_days=int(logs_keep_days),
        maintenance_candidate_keep_count=int(maintenance_candidate_keep_count),
        maintenance_candidate_min_age_hours=int(maintenance_candidate_min_age_hours),
        dry_run=not bool(apply),
        generated_at=now.isoformat(timespec="seconds"),
        log_roots=log_roots,
        logs_only=logs_only,
    )

    if not logs_only:
        report.archive_kept, report.archive_removed = plan_archive_cleanup(
            archive_root,
            keep_count=archive_keep_count,
            keep_days=archive_keep_days,
            now=now,
        )
    for logs_root in log_roots:
        kept, removed = plan_logs_cleanup(logs_root, keep_days=logs_keep_days, now=now)
        report.logs_kept.extend(kept)
        report.logs_removed.extend(removed)
    if not logs_only:
        report.maintenance_candidates_kept, report.maintenance_candidates_removed = (
            plan_maintenance_candidate_cleanup(
                Path(settings.project_root),
                keep_count=maintenance_candidate_keep_count,
                min_age_hours=maintenance_candidate_min_age_hours,
                now=now,
            )
        )

    if report.lifecycle_policy_active:
        # Offline lifecycle supersedes the former permanent TTL of unique run
        # histories and diagnostic/recovery directories. Unknown units stay put.
        report.archive_kept.extend(item.path for item in report.archive_removed)
        report.archive_removed = []
        report.maintenance_candidates_kept.extend(item.path for item in report.maintenance_candidates_removed)
        report.maintenance_candidates_removed = []
        from backend.storage_lifecycle import open_inodes
        if os.environ.get('FUND_ANA_STORAGE_LIFECYCLE_PROJECT_ROOT'):
            opened = None  # other container/host PID namespaces cannot be proven idle
        else:
            try:
                opened = open_inodes()
            except (OSError, ValueError):
                opened = None
        for item in list(report.logs_removed):
            identity = item.file_identity
            if opened is None or identity is None or (identity[0], identity[1]) in opened:
                report.logs_removed.remove(item)
                report.logs_kept.append(item.path)

    report.planned = {name: [replace(item) for item in items] for name, items in (
        ("archive", report.archive_removed), ("logs", report.logs_removed),
        ("maintenance_candidates", report.maintenance_candidates_removed),
    )}

    if apply:
        from backend.forward_validation_cache import preserve_cache_sources_before_removal

        def failed(scope: str, item: CleanupPlanItem, removed: list[CleanupPlanItem],
                   kept: list[Path], error: str) -> None:
            removed.remove(item)
            if item.path.exists() or item.path.is_symlink():
                kept.append(item.path)
            report.failures.setdefault(scope, []).append({"path": str(item.path), "error": error})

        def remove(scope: str, item: CleanupPlanItem, removed: list[CleanupPlanItem], kept: list[Path]) -> None:
            footprint = _remove_path(item.path, report.errors, expected_identity=item.file_identity)
            if footprint is None:
                failed(scope, item, removed, kept, report.errors[-1])
                if scope == "archive":
                    # rmtree can unlink some children before raising. Do not
                    # claim either a complete removal or intact preservation.
                    report.partial_removal_possible = True
                    report.removed_allocated_bytes_estimate[scope] = None
                return
            item.size_bytes, allocated = footprint
            previous = report.removed_allocated_bytes_estimate.get(scope, 0)
            if previous is not None:
                report.removed_allocated_bytes_estimate[scope] = previous + allocated

        for item in list(report.archive_removed):
            try:
                preserved = preserve_cache_sources_before_removal(settings, item.path)
                if preserved["preserved"]:
                    report.forward_cache_source_preservation.append(preserved)
            except (OSError, ValueError) as exc:
                report.errors.append(f"forward validation cache source preservation failed: {item.path}: {exc}")
                report.forward_cache_accounting_complete = False
                failed("archive", item, report.archive_removed, report.archive_kept, report.errors[-1])
                continue
            remove("archive", item, report.archive_removed, report.archive_kept)
        for item in list(report.logs_removed):
            remove("logs", item, report.logs_removed, report.logs_kept)
        for item in list(report.maintenance_candidates_removed):
            remove("maintenance_candidates", item, report.maintenance_candidates_removed,
                   report.maintenance_candidates_kept)
        if not logs_only and _settings_support_publication_registry_refresh(settings):
            try:
                from backend.publication import load_publication_registry

                load_publication_registry(settings)
                report.publication_registry_refreshed = True
            except Exception as exc:  # pragma: no cover - defensive refresh path
                report.publication_registry_refresh_error = str(exc)
                report.errors.append(f"publication registry refresh failed: {exc}")

    if not logs_only:
        from backend.task_log_compression import run_task_log_compression
        from scripts.test_source_storage import collect_unused_test_sources
        from scripts.test_source_migration import migrate_legacy_sources

        try:
            report.test_source_migration = migrate_legacy_sources(project_root, apply=apply, now=now.timestamp())
            report.errors.extend(f"legacy test source migration: {item['error']}"
                                 for item in report.test_source_migration["errors"])
            if not report.test_source_migration["scan_complete"]:
                report.errors.append("legacy test source migration: scan budget exhausted")
        except (OSError, ValueError, RuntimeError) as exc:
            report.errors.append(f"legacy test source migration failed: {exc}")

        try:
            report.test_source_pool_gc = collect_unused_test_sources(project_root, apply=apply, now=now.timestamp())
            report.errors.extend(f"test source pool cleanup: {item['error']}" for item in report.test_source_pool_gc["errors"])
            if not report.test_source_pool_gc["scan_complete"]:
                report.errors.append("test source pool cleanup: scan budget exhausted")
        except (OSError, ValueError, RuntimeError) as exc:
            report.errors.append(f"test source pool cleanup failed: {exc}")

        from backend.shared_audit_csv_compression import run_shared_audit_csv_compression

        try:
            report.shared_audit_csv_compression = run_shared_audit_csv_compression(settings, apply=apply)
            report.errors.extend(f"shared audit CSV compression: {item['error']}"
                                 for item in report.shared_audit_csv_compression["errors"])
            if not report.shared_audit_csv_compression["scan_complete"]:
                report.errors.append("shared audit CSV compression: scan budget exhausted")
        except (OSError, ValueError, RuntimeError) as exc:
            report.errors.append(f"shared audit CSV compression failed: {exc}")

        if getattr(settings, "scheduler_weekly_disk_cleanup_core_csv_enabled", True):
            from backend.core_csv_compression import run_core_csv_compression

            try:
                report.core_csv_compression = run_core_csv_compression(
                    settings, apply=apply,
                    keep_days=getattr(settings, "scheduler_weekly_disk_cleanup_core_csv_keep_days", 30),
                    now=now.timestamp(),
                )
                report.errors.extend(f"core CSV compression: {item['path']}: {item['error']}"
                                     for item in report.core_csv_compression["errors"])
                if not report.core_csv_compression["scan_complete"]:
                    report.errors.append(f"core CSV compression: {report.core_csv_compression.get('reason')}")
            except (OSError, ValueError, RuntimeError) as exc:
                report.errors.append(f"core CSV compression failed: {exc}")

        try:
            report.task_log_compression = run_task_log_compression(project_root, keep_days=max(1, logs_keep_days), apply=apply)
            report.errors.extend(f"task log compression: {item['path']}: {item['error']}"
                                 for item in report.task_log_compression["errors"])
            if not report.task_log_compression["scan_complete"]:
                report.errors.append(f"task log compression: {report.task_log_compression.get('reason')}")
        except (OSError, ValueError, RuntimeError) as exc:
            report.errors.append(f"task log compression failed: {exc}")

        pool = Path(getattr(settings, "reports_dir", project_root / "reports")) / ".calculation_input_pool"
        if pool.exists() or pool.is_symlink():
            from core.utils.calculation_pack_storage import collect_unused_packs

            try:
                report.calculation_input_pool_gc = collect_unused_packs(pool, apply=apply, now=now.timestamp())
                report.errors.extend(f"calculation input pool cleanup: {item['error']}"
                                     for item in report.calculation_input_pool_gc["errors"])
            except (OSError, RuntimeError, sqlite3.Error) as exc:
                report.errors.append(f"calculation input pool cleanup failed: {exc}")
        pool = Path(getattr(settings, "reports_dir", project_root / "reports")) / ".report_artifact_pool"
        if pool.exists() or pool.is_symlink():
            from backend.report_artifact_storage import collect_unused_report_artifacts

            try:
                report.report_artifact_pool_gc = collect_unused_report_artifacts(settings, apply=apply, now=now.timestamp())
                report.errors.extend(f"report artifact pool cleanup: {item['error']}"
                                     for item in report.report_artifact_pool_gc["errors"])
            except (OSError, RuntimeError) as exc:
                report.errors.append(f"report artifact pool cleanup failed: {exc}")
    return report


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="P0-02: reports/archive + logs/ + core_root/logs/ TTL 清理")
    parser.add_argument("--apply", action="store_true", help="真实删除（默认 dry-run）")
    parser.add_argument("--logs-only", action="store_true", help="仅处理日期日志，不处理归档或刷新发布状态")
    parser.add_argument("--archive-keep-count", type=int, default=None, help="保留最近 N 个归档 run；默认读取配置")
    parser.add_argument("--archive-keep-days", type=int, default=None, help="保留最近 N 天内的归档 run；默认读取配置")
    parser.add_argument("--logs-keep-days", type=int, default=None, help="保留最近 N 天的日志文件；默认读取配置")
    parser.add_argument(
        "--maintenance-candidate-keep-count",
        type=int,
        default=None,
        help="每个 maintenance vacuum candidate 目录保留最近 N 个候选库；默认读取配置",
    )
    parser.add_argument(
        "--maintenance-candidate-min-age-hours",
        type=int,
        default=None,
        help="保护最近 N 小时内的 maintenance vacuum candidate；默认读取配置",
    )
    args = parser.parse_args(argv)

    report = run_cleanup(
        archive_keep_count=args.archive_keep_count,
        archive_keep_days=args.archive_keep_days,
        logs_keep_days=args.logs_keep_days,
        maintenance_candidate_keep_count=args.maintenance_candidate_keep_count,
        maintenance_candidate_min_age_hours=args.maintenance_candidate_min_age_hours,
        logs_only=args.logs_only,
        apply=args.apply,
    )

    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if not report.errors else 1


if __name__ == "__main__":
    raise SystemExit(_main())
