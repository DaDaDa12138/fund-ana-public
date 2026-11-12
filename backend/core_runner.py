from __future__ import annotations

import os
import json
import re
import sqlite3
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
import importlib.util

from backend.config import Settings
from backend.effective_nav import load_effective_nav_snapshot
from core.utils.core_csv_storage import artifact_is_file, glob_artifacts, original_artifact_stat
try:
    from bond_fund_screening.tasks.subprocess_logging import (
        build_subprocess_log_index,
        classify_stderr_signals,
        run_structured_subprocess,
    )
except ImportError:  # pragma: no cover - fallback before core path bootstrap
    _SCREENING_ROOT = Path(__file__).resolve().parent.parent / "core" / "bond_fund_screening"
    if str(_SCREENING_ROOT) not in sys.path:
        sys.path.insert(0, str(_SCREENING_ROOT))
    from bond_fund_screening.tasks.subprocess_logging import (
        build_subprocess_log_index,
        classify_stderr_signals,
        run_structured_subprocess,
    )


@dataclass(slots=True)
class StepResult:
    name: str
    command: list[str]


class CoreStepFailedError(RuntimeError):
    """A failed child process with its actual stage and exit status."""

    def __init__(self, *, stage: str, return_code: int, meta_path: str,
                 timed_out: bool = False, timeout_reason: str | None = None) -> None:
        self.stage = stage
        self.return_code = return_code
        self.meta_path = meta_path
        self.timed_out = timed_out
        self.timeout_reason = timeout_reason
        self.deferred_failures: list[dict] = []
        suffix = f", timed_out=True, timeout_reason={timeout_reason or 'timeout'}" if timed_out else ""
        super().__init__(f"步骤失败: {stage} (exit={return_code}{suffix}, meta={meta_path})")

    def as_dict(self) -> dict:
        return {
            "stage": self.stage, "return_code": self.return_code, "meta_path": self.meta_path,
            "timed_out": self.timed_out, "timeout_reason": self.timeout_reason,
        }


TIANTIAN_BUSINESS_PREFLIGHT_STEPS = {"collect-funds", "collect-details", "collect-managers"}
TIANTIAN_BUSINESS_PREFLIGHT_ATTEMPTS = 3
TIANTIAN_BUSINESS_PREFLIGHT_TIMEOUT_SECONDS = 5.0
TIANTIAN_BUSINESS_PREFLIGHT_RETRY_INTERVAL_SECONDS = 1.0


class TiantianBusinessUnavailableError(RuntimeError):
    """External dependency failure raised before starting Tiantian collection subprocesses."""

    def __init__(
        self,
        *,
        stage: str,
        probe: dict,
        diagnostic_path: Path | None = None,
    ) -> None:
        self.stage = str(stage or "").strip() or "tiantian-business-preflight"
        self.probe = dict(probe or {})
        self.diagnostic_path = diagnostic_path
        status = str(self.probe.get("status") or "unknown").strip() or "unknown"
        message = str(self.probe.get("message") or "").strip()
        detail = f" status={status}"
        if message:
            detail += f" message={message}"
        if diagnostic_path:
            detail += f" diagnostic={diagnostic_path}"
        super().__init__(f"天天基金业务接口不可用: stage={self.stage}{detail}")


CORE_ARTIFACT_PATTERNS = {
    "long_csv": ["长期优质基金_*.csv"],
    "long_excel": ["长期优质基金_详细报告_*.xlsx"],
    "short_csv": ["近期优质基金_*.csv"],
    "short_excel": ["近期优质基金_详细报告_*.xlsx"],
    "comparison_excel": ["近期与长期优质基金对比报告_*.xlsx"],
    "final_excel": ["当日最终优质基金_*.xlsx"],
    "accumulation_tracker_excel": ["优质基金累积跟踪.xlsx"],
    "short_watchlist": ["仅近期优质观察池_*只_*.csv", "仅近期优质观察池_Top*_*.csv"],
    "long_watchlist": ["仅长期优质观察池_*只_*.csv", "仅长期优质观察池_Top*_*.csv"],
    "only_short_simple_csv": ["仅近期优质_简版_*.csv"],
    "only_long_simple_csv": ["仅长期优质_简版_*.csv"],
}


def _sqlite_count(db_path: Path, table_name: str) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row = cursor.fetchone()
        return int(row[0] or 0) if row else 0
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def _sqlite_has_rows(db_path: Path, table_name: str) -> bool:
    if not db_path.exists():
        return False
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return cursor.fetchone() is not None
    except sqlite3.Error:
        return False
    finally:
        conn.close()


def _sqlite_scalar(db_path: Path, query: str) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(query)
        row = cursor.fetchone()
        return int(row[0] or 0) if row else 0
    except (sqlite3.Error, TypeError, ValueError):
        return 0
    finally:
        conn.close()


def _sqlite_text(db_path: Path, query: str) -> str:
    if not db_path.exists():
        return ""
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(query)
        row = cursor.fetchone()
        return str(row[0] or "").strip() if row else ""
    except sqlite3.Error:
        return ""
    finally:
        conn.close()


def _load_risk_refresh_target_codes(path_value: str | Path | None) -> list[str]:
    normalized_path = str(path_value or "").strip()
    if not normalized_path:
        return []
    path = Path(normalized_path)
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        raw_codes = payload.get("target_fcodes") or payload.get("updated_fcodes") or []
    elif isinstance(payload, list):
        raw_codes = payload
    else:
        raw_codes = []
    result: list[str] = []
    seen_codes: set[str] = set()
    for raw_code in raw_codes:
        code = str(raw_code or "").strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        result.append(code)
    return result


def _write_risk_refresh_target_codes(path: Path, codes: list[str]) -> None:
    normalized_codes: list[str] = []
    seen_codes: set[str] = set()
    for raw_code in codes:
        code = str(raw_code or "").strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        normalized_codes.append(code)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"target_fcodes": normalized_codes}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _python_has_module(python_bin: Path, module_name: str) -> bool:
    try:
        result = subprocess.run(
            [
                str(python_bin),
                "-c",
                (
                    "import importlib.util,sys; "
                    f"sys.exit(0 if importlib.util.find_spec('{module_name}') else 1)"
                ),
            ],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return False
    return result.returncode == 0


def resolve_core_python(settings: Settings) -> str:
    project_venv_python = settings.project_root / ".venv" / "bin" / "python"
    if project_venv_python.exists() and _python_has_module(project_venv_python, "sqlalchemy"):
        return str(project_venv_python)

    current_python = Path(sys.executable)
    if current_python.exists() and importlib.util.find_spec("sqlalchemy") is not None:
        return str(current_python)

    return sys.executable


def should_use_incremental_nav(settings: Settings) -> bool:
    """
    仅当数据库中已经存在净值历史时才允许增量更新。

    新库首次运行时直接走增量模式会得到 0 条记录，
    导致后续长短周期筛选全部 Top0，最终阶段没有输入文件。
    """
    return _sqlite_has_rows(settings.database_path, "fund_nav_history")


def should_collect_managers(settings: Settings) -> bool:
    """
    在只跑筛选或历史库补跑场景下，若基金经理元数据为空则自动补齐。

    核心筛选第3层会将 `manager_names/manager_ids` 作为数据完整性硬门槛；
    如果前置采集遗漏基金经理信息，会导致候选池在第3层被整体清空。
    """
    fund_count = _sqlite_count(settings.database_path, "funds")
    if fund_count <= 0:
        return False

    manager_count = _sqlite_scalar(
        settings.database_path,
        """
        SELECT COUNT(*)
        FROM funds
        WHERE COALESCE(TRIM(manager_names), '') <> ''
           OR COALESCE(TRIM(manager_ids), '') <> ''
        """,
    )
    return manager_count == 0


def should_calculate_risk(settings: Settings) -> bool:
    """
    若 1 年风险指标覆盖不足，则在正式筛选前自动补齐。

    核心筛选最终阶段依赖 fund_risk_metrics(period='1y') 提供
    `1年-年化收益(%) / 1年-夏普比率 / 1年-最大回撤(%)`。
    若该表为空或覆盖不足，会把最终候选池整体判为关键指标缺失。
    """
    nav_fund_count = _sqlite_scalar(
        settings.database_path,
        "SELECT COUNT(DISTINCT fcode) FROM fund_nav_history",
    )
    if nav_fund_count <= 0:
        return False

    effective_snapshot = load_effective_nav_snapshot(
        settings.database_path,
        coverage_ratio=settings.effective_nav_coverage_ratio,
        weekday_only=settings.effective_nav_weekday_only,
    )
    risk_1y_fund_count = int(effective_snapshot.risk_1y_effective_fund_count or 0)
    if risk_1y_fund_count < nav_fund_count:
        return True

    risk_1y_max_end_date = str(
        effective_snapshot.risk_1y_effective_end_date
        or effective_snapshot.risk_1y_latest_end_date
        or ""
    ).strip()
    if effective_snapshot.ready and effective_snapshot.candidate_date:
        return risk_1y_max_end_date != effective_snapshot.candidate_date
    return False


def should_refresh_changed_nav_risk(settings: Settings) -> bool:
    """
    仅当确有净值已更新但风险指标未对齐的基金时，才执行 changed-nav risk refresh。

    盘中链经常出现 `collect-nav` 实际 0 条新增，但仍然启动一次完整
    `calculate-risk --changed-nav-only` 子进程；这里把等价判断前移，减少空跑。
    """
    if not settings.database_path.exists():
        return True

    try:
        from core.cli.aux_commands import _resolve_changed_nav_risk_candidates
    except Exception:
        return True

    try:
        candidates = _resolve_changed_nav_risk_candidates(
            settings.database_path,
            config={
                "risk_metrics": {
                    "skip_risk_metrics_for_holding_days_gt": settings.exclude_holding_days_gt,
                }
            },
        )
    except Exception:
        return True
    return bool(candidates)


def should_refresh_holding_period(settings: Settings) -> bool:
    """
    仅当持有期补强确有候选时，才启动 supplement_bond_holding_period 脚本。

    在 `no_signal_reviewed` 和结构化可升级过滤落地后，盘中链可能已经没有任何
    待处理基金；此时继续起一个子进程只会产生额外启动开销和无意义日志。
    """
    if not settings.database_path.exists():
        return True

    try:
        from core.collectors.supplement_holding_period import HoldingPeriodCollector
    except Exception:
        return True

    try:
        collector = HoldingPeriodCollector(config={"database": {"path": str(settings.database_path)}})
        try:
            funds = collector._get_funds_to_update(force_update=False)
        finally:
            collector.db_manager.close()
    except Exception:
        return True
    return bool(funds)


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def _probe_tiantian_business(settings: Settings, *, timeout_seconds: float) -> dict:
    core_root = Path(getattr(settings, "core_root", Path(__file__).resolve().parent.parent / "core"))
    core_root_text = str(core_root)
    if core_root_text not in sys.path:
        sys.path.insert(0, core_root_text)
    try:
        from utils.service_checks import probe_tiantian_api_business_status
    except Exception as exc:
        return {
            "ok": False,
            "status": "probe_import_error",
            "message": f"Tiantian 业务探针加载失败: {exc}",
            "base_url": getattr(settings, "tiantian_base_url", None),
            "errors": [str(exc)],
        }

    try:
        return dict(
            probe_tiantian_api_business_status(
                base_url=getattr(settings, "tiantian_base_url", None),
                timeout_seconds=timeout_seconds,
            )
            or {}
        )
    except Exception as exc:
        return {
            "ok": False,
            "status": "probe_error",
            "message": f"Tiantian 业务探针执行失败: {exc}",
            "base_url": getattr(settings, "tiantian_base_url", None),
            "errors": [str(exc)],
        }


def _write_tiantian_preflight_failure_meta(
    *,
    settings: Settings,
    stage: str,
    log_dir: Path,
    started_at: datetime,
    attempts: list[dict],
    latest_probe: dict,
) -> Path:
    finished_at = datetime.now()
    safe_stage = re.sub(r"[^0-9A-Za-z_.-]+", "_", stage).strip("._") or "collection"
    meta_path = log_dir / (
        f"{started_at.strftime('%Y%m%d_%H%M%S')}_"
        f"tiantian-business-preflight_{safe_stage}_{os.getpid()}.meta.json"
    )
    payload = {
        "task_name": "tiantian-business-preflight",
        "stage": stage,
        "failure_kind": "tiantian_business_unavailable",
        "command": [
            "preflight",
            "GET",
            f"{str(getattr(settings, 'tiantian_base_url', '')).rstrip('/')}/fundMNRank",
            "FundType=31",
            "pageIndex=1",
            "pageSize=1",
        ],
        "cwd": str(getattr(settings, "core_root", settings.project_root)),
        "started_at": started_at.isoformat(timespec="seconds"),
        "finished_at": finished_at.isoformat(timespec="seconds"),
        "duration_seconds": round((finished_at - started_at).total_seconds(), 3),
        "return_code": -3,
        "timed_out": False,
        "timeout_reason": None,
        "success": False,
        "stdout_path": "",
        "stderr_path": "",
        "meta_path": str(meta_path),
        "stdout_tail": [],
        "stderr_tail": [str(latest_probe.get("message") or "")],
        "stderr_signal_summary": classify_stderr_signals(
            task_name="tiantian-business-preflight",
            stderr_text=str(latest_probe.get("message") or ""),
            success=False,
            return_code=-3,
            timed_out=False,
            timeout_reason=None,
        ),
        "attempt_count": len(attempts),
        "attempts": attempts,
        "probe": latest_probe,
    }
    _write_json_atomic(meta_path, payload)
    return meta_path


def _ensure_tiantian_business_ready_for_stage(
    *,
    settings: Settings,
    stage: str,
    log: Callable[[str], None],
    log_dir: Path,
) -> None:
    started_at = datetime.now()
    attempts: list[dict] = []
    latest_probe: dict = {}
    for attempt in range(1, TIANTIAN_BUSINESS_PREFLIGHT_ATTEMPTS + 1):
        latest_probe = _probe_tiantian_business(
            settings,
            timeout_seconds=TIANTIAN_BUSINESS_PREFLIGHT_TIMEOUT_SECONDS,
        )
        attempt_record = {
            "attempt": attempt,
            "checked_at": datetime.now().isoformat(timespec="seconds"),
            "ok": bool(latest_probe.get("ok")),
            "status": latest_probe.get("status"),
            "message": latest_probe.get("message"),
            "base_url": latest_probe.get("base_url"),
            "url": latest_probe.get("url"),
            "http_status": latest_probe.get("http_status"),
            "datas_count": latest_probe.get("datas_count"),
            "total_count": latest_probe.get("total_count"),
            "err_code": latest_probe.get("err_code"),
            "err_msg": latest_probe.get("err_msg"),
            "errors": latest_probe.get("errors") or [],
        }
        attempts.append(attempt_record)
        log(
            "[tiantian-preflight] "
            f"stage={stage} attempt={attempt}/{TIANTIAN_BUSINESS_PREFLIGHT_ATTEMPTS} "
            f"ok={attempt_record['ok']} status={attempt_record['status'] or 'unknown'} "
            f"http_status={attempt_record['http_status'] or '-'}"
        )
        if bool(latest_probe.get("ok")):
            return
        if attempt < TIANTIAN_BUSINESS_PREFLIGHT_ATTEMPTS:
            time.sleep(TIANTIAN_BUSINESS_PREFLIGHT_RETRY_INTERVAL_SECONDS)

    diagnostic_path = _write_tiantian_preflight_failure_meta(
        settings=settings,
        stage=stage,
        log_dir=log_dir,
        started_at=started_at,
        attempts=attempts,
        latest_probe=latest_probe,
    )
    log(
        "[tiantian-preflight] "
        f"failed stage={stage} diagnostic={diagnostic_path} "
        f"message={str(latest_probe.get('message') or '').strip()}"
    )
    raise TiantianBusinessUnavailableError(
        stage=stage,
        probe=latest_probe,
        diagnostic_path=diagnostic_path,
    )


def _build_and_log_subprocess_index(
    *,
    settings: Settings,
    log_dir: Path,
    workflow_run_id: str,
    log: Callable[[str], None],
    planned_task_names: list[str] | None = None,
) -> dict:
    subprocess_log_index = build_subprocess_log_index(
        log_dir=log_dir,
        project_root=settings.project_root,
        run_id=workflow_run_id,
    )
    if planned_task_names is not None:
        subprocess_log_index["period_return_collection_planned"] = "collect-period-returns" in planned_task_names
        subprocess_log_index["planned_task_names"] = list(planned_task_names)
        index_path = Path(subprocess_log_index["index_path"])
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        payload["period_return_collection_planned"] = subprocess_log_index["period_return_collection_planned"]
        payload["planned_task_names"] = subprocess_log_index["planned_task_names"]
        index_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    log(
        "[core-pipeline] subprocess_index="
        f"{subprocess_log_index.get('index_path')} "
        f"count={subprocess_log_index.get('subprocess_count')} "
        f"failed={subprocess_log_index.get('failed_count')}"
    )
    return subprocess_log_index



def ensure_core_output_link(settings: Settings) -> Path:
    target = settings.core_output_dir
    target.mkdir(parents=True, exist_ok=True)
    output_path = settings.core_root / "bond_fund_screening" / "output"

    if output_path.is_symlink() or output_path.exists():
        if output_path.is_symlink():
            resolved = output_path.resolve()
            if resolved == target.resolve():
                return output_path
            output_path.unlink()
        elif output_path.is_dir() and not any(output_path.iterdir()):
            output_path.rmdir()
        elif output_path.is_dir() and output_path.resolve() == target.resolve():
            return output_path
        else:
            backup_path = output_path.with_name("output_core_backup")
            if backup_path.exists():
                if backup_path.is_dir():
                    shutil.rmtree(backup_path)
                else:
                    backup_path.unlink()
            output_path.rename(backup_path)

    os.symlink(target, output_path)
    return output_path



def _run_streaming_subprocess(
    *,
    name: str,
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    log: Callable[[str], None],
    log_dir: Path | None = None,
    timeout: float | None = None,
    idle_timeout: float | None = None,
) -> list[str]:
    result = run_structured_subprocess(
        task_name=name,
        cmd=cmd,
        cwd=cwd,
        env=env,
        log=log,
        log_dir=log_dir,
        timeout=timeout,
        idle_timeout=idle_timeout,
    )
    if not result.success:
        raise CoreStepFailedError(
            stage=name, return_code=result.return_code, meta_path=str(result.meta_path),
            timed_out=result.timed_out, timeout_reason=result.timeout_reason,
        )
    return result.stdout_text.splitlines()


def _extract_collect_nav_inserted_count(output_lines: list[str]) -> int | None:
    inserted_count: int | None = None
    for line in output_lines:
        matches = re.findall(r"(\d[\d,]*)条净值记录", line)
        if not matches:
            continue
        candidate = matches[-1].replace(",", "")
        try:
            inserted_count = int(candidate)
        except ValueError:
            continue
    return inserted_count


def _workflow_task_timeout(settings: Settings, task_name: str) -> float | None:
    mapping = getattr(settings, "workflow_task_timeout_seconds", {}) or {}
    try:
        value = mapping.get(task_name)
    except AttributeError:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def _workflow_task_idle_timeout(settings: Settings, task_name: str) -> float | None:
    mapping = getattr(settings, "workflow_task_idle_timeout_seconds", {}) or {}
    try:
        value = mapping.get(task_name)
    except AttributeError:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None



def build_core_commands(settings: Settings, options: dict) -> list[StepResult]:
    commands: list[StepResult] = []
    run_collection = bool(options.get("run_collection", settings.run_collection_by_default))
    run_screening = bool(options.get("run_screening", settings.run_screening_by_default))
    reuse_existing_outputs = bool(options.get("reuse_existing_outputs", False))
    collection_profile = str(options.get("collection_profile", "full") or "full").strip().lower()
    if collection_profile not in {"full", "daily", "weekly"}:
        collection_profile = "full"
    force_collect_managers = bool(options.get("force_collect_managers", False))
    force_calculate_risk = bool(options.get("force_calculate_risk", False))
    requested_changed_nav_only = bool(options.get("risk_refresh_changed_nav_only", False))
    # 普通工作流必须检查同日收益快照修正和缺失周期，不能退回到
    # RiskAnalyzer 的“已有任意风险行即整只跳过”非 force 语义。
    # 只有明确请求全量 force 时才允许关闭 changed-nav 定向刷新。
    risk_refresh_changed_nav_only = requested_changed_nav_only or not force_calculate_risk
    skip_risk_calculation = bool(options.get("skip_risk_calculation", False))
    skip_collect_funds = bool(options.get("skip_collect_funds", False))
    nav_workers_override = options.get("nav_workers_override")
    risk_refreshed_during_collection = False

    python_bin = resolve_core_python(settings)

    def _build_risk_command() -> list[str]:
        command = [
            python_bin,
            "main.py",
            "calculate-risk",
            "--yes",
            "--workers",
            str(settings.risk_workers),
        ]
        if risk_refresh_changed_nav_only:
            command.extend(["--changed-nav-only", "--force"])
        elif force_calculate_risk:
            command.append("--force")
        return command

    if reuse_existing_outputs:
        return commands

    if run_collection:
        if not settings.database_path.exists():
            commands.append(
                StepResult("init-db", [python_bin, "main.py", "init-db"])
            )
        if not skip_collect_funds:
            commands.append(
                StepResult("collect-funds", [python_bin, "main.py", "collect-funds", "-t", "bond"])
            )
        if settings.collect_details and collection_profile in {"full", "weekly"}:
            detail_cmd = [
                python_bin,
                "main.py",
                "collect-details",
                "--workers",
                str(settings.detail_workers),
            ]
            commands.append(StepResult("collect-details", detail_cmd))
        elif settings.collect_details and collection_profile == "daily":
            # 日更只补采详情已成功但规模仍为空的有限队列；完整详情刷新保留在周维护。
            detail_cmd = [
                python_bin,
                "main.py",
                "collect-details",
                "--scale-missing-only",
                "--workers",
                str(settings.detail_workers),
            ]
            commands.append(StepResult("collect-details", detail_cmd))
        if settings.collect_managers and (
            collection_profile in {"full", "weekly"}
            or force_collect_managers
        ):
            manager_cmd = [
                python_bin,
                "main.py",
                "collect-managers",
                "--workers",
                str(settings.manager_workers),
            ]
            if force_collect_managers:
                manager_cmd.append("--force")
            commands.append(StepResult("collect-managers", manager_cmd))
        if collection_profile in {"full", "daily"}:
            nav_workers = settings.nav_workers
            if nav_workers_override is not None:
                try:
                    nav_workers = max(1, int(nav_workers_override))
                except (TypeError, ValueError):
                    nav_workers = settings.nav_workers
            nav_cmd = [python_bin, "main.py", "collect-nav", "--yes"]
            if settings.collect_nav_incremental and should_use_incremental_nav(settings):
                nav_cmd.append("--incremental")
            nav_cmd.extend(
                [
                    "--workers",
                    str(nav_workers),
                    "--batch-size",
                    str(settings.nav_batch_size),
                    "--batch-interval",
                    str(settings.nav_batch_interval),
                ]
            )
            commands.append(StepResult("collect-nav", nav_cmd))
            if (
                settings.collect_risk
                and risk_refresh_changed_nav_only
                and not skip_risk_calculation
            ):
                commands.append(StepResult("calculate-risk", _build_risk_command()))
                risk_refreshed_during_collection = True
        if collection_profile in {"full", "daily", "weekly"} and should_refresh_holding_period(settings):
            commands.append(
                StepResult(
                    "supplement-holding-period",
                    [python_bin, str(settings.project_root / "scripts" / "supplement_bond_holding_period.py")],
                )
            )
        if (
            settings.collect_risk
            and collection_profile in {"full", "daily", "weekly"}
            and not risk_refreshed_during_collection
            and not skip_risk_calculation
        ):
            commands.append(StepResult("calculate-risk", _build_risk_command()))
            risk_refreshed_during_collection = True

        if getattr(settings, "collect_companies", False) and collection_profile in {"full", "weekly"}:
            # Company profiles do not feed risk calculations. Keep their bounded
            # refresh after the risk/holding updates so a profile failure cannot
            # prevent these maintenance stages from executing.
            commands.append(StepResult("collect-companies", [
                python_bin, "main.py", "collect-companies", "--workers",
                str(getattr(settings, "company_workers", 2)),
            ]))

    if run_screening:
        if (settings.collect_managers
                and not any(step.name == "collect-managers" for step in commands)
                and (force_collect_managers or should_collect_managers(settings))):
            manager_cmd = [python_bin, "main.py", "collect-managers", "--workers", str(settings.manager_workers)]
            if force_collect_managers:
                manager_cmd.append("--force")
            commands.append(
                StepResult("collect-managers", manager_cmd)
            )
        if (
            settings.collect_risk
            and not skip_risk_calculation
            and (
                risk_refresh_changed_nav_only
                or force_calculate_risk
                or should_calculate_risk(settings)
            )
            and not risk_refreshed_during_collection
        ):
            commands.append(StepResult("calculate-risk", _build_risk_command()))
        commands.append(
            StepResult(
                "daily-screening-master",
                [python_bin, "-m", "bond_fund_screening.tasks.daily_screening_master"],
            )
        )

    if run_collection or run_screening:
        # The actual daily, intraday, weekly and screening-only entry points
        # must repair dated returns before risk calculations consume them.
        insert_at = next((index for index, step in enumerate(commands)
                          if step.name in {"calculate-risk", "daily-screening-master"}), len(commands))
        commands.insert(insert_at, StepResult(
            "collect-period-returns", [python_bin, "main.py", "collect-period-returns"],
        ))
    return commands



def run_core_pipeline(
    settings: Settings,
    options: dict,
    log: Callable[[str], None],
) -> dict:
    ensure_core_output_link(settings)
    env = os.environ.copy()
    workflow_run_id = str(options.get("workflow_run_id") or os.environ.get("FUND_ANA_WORKFLOW_RUN_ID") or "").strip()
    collection_capture = None
    if not workflow_run_id:
        from backend.collection_calculation_capture import prepare_collection_capture

        collection_capture = prepare_collection_capture(settings, options)
    capture_run_id = workflow_run_id or collection_capture["execution"]["capture_run_id"]
    reports_dir = Path(getattr(settings, "reports_dir", settings.project_root / "reports"))
    runs_dir = Path(getattr(settings, "runs_dir", reports_dir / "runs"))
    subprocess_log_dir = (
        runs_dir / workflow_run_id / "logs" / "subprocess"
        if workflow_run_id
        else collection_capture["root"] / "logs" / "subprocess"
    )
    subprocess_log_dir.mkdir(parents=True, exist_ok=True)
    env.update(
        {
            "TIANTIAN_API_URL": settings.tiantian_base_url,
            "DATABASE_PATH": str(settings.database_path),
            "PYTHONUNBUFFERED": "1",
            "FUND_ANA_SUBPROCESS_LOG_DIR": str(subprocess_log_dir),
        }
    )
    if workflow_run_id:
        env["FUND_ANA_WORKFLOW_RUN_ID"] = workflow_run_id
        # Keep all dated screening artifacts on the workflow batch date. A
        # long run can cross midnight; independent datetime.now() calls in
        # child tasks would otherwise make final input pairing fail.
        batch_date = workflow_run_id[:8]
        if re.fullmatch(r"20\d{6}", batch_date):
            env["FUND_ANA_REPORT_DATE"] = batch_date
        else:
            env.pop("FUND_ANA_REPORT_DATE", None)
        from backend.calculation_input_capture import prepare_capture_directory

        calculation_input_dir = prepare_capture_directory(settings, workflow_run_id)
    else:
        calculation_input_dir = collection_capture["directory"]
        env.pop("FUND_ANA_WORKFLOW_RUN_ID", None)
        env.pop("FUND_ANA_REPORT_DATE", None)
        log(f"[core-pipeline] collection_calculation_execution={collection_capture['record']['execution_path']}")
    env["FUND_ANA_CALCULATION_INPUT_DIR"] = str(calculation_input_dir)
    env["FUND_ANA_CALCULATION_SHARED_DIR"] = str(reports_dir / ".calculation_input_pool")
    env["FUND_ANA_CALCULATION_RUN_ID"] = capture_run_id
    env["FUND_ANA_CALCULATION_SCOPE"] = "workflow" if workflow_run_id else "collection"
    log(f"[core-pipeline] subprocess_log_dir={subprocess_log_dir}")

    risk_refresh_changed_nav_only = bool(options.get("risk_refresh_changed_nav_only", False))
    collect_nav_inserted_count: int | None = None
    changed_nav_target_file: Path | None = None
    external_risk_target_file = str(options.get("risk_refresh_target_fcodes_file") or "").strip()
    external_risk_target_codes = _load_risk_refresh_target_codes(external_risk_target_file)
    deferred_collection_errors: list[CoreStepFailedError] = []
    period_returns_checked = False
    planned_task_names: list[str] = []

    try:
        planned_steps = build_core_commands(settings, options)
        planned_task_names = [step.name for step in planned_steps]
        for step in planned_steps:
            if step.name == "daily-screening-master" and deferred_collection_errors:
                log("[core-pipeline] 采集未完整完成，独立维护已执行；本轮不启动筛选")
                last_error = deferred_collection_errors[-1]
                last_error.deferred_failures.extend(item.as_dict() for item in deferred_collection_errors[:-1])
                raise last_error
            if step.name == "daily-screening-master" and not options.get("skip_risk_calculation"):
                from backend.publication_dependencies import ensure_publication_dependency_configured
                ensure_publication_dependency_configured(settings)
            command = list(step.command)
            if step.name in TIANTIAN_BUSINESS_PREFLIGHT_STEPS:
                _ensure_tiantian_business_ready_for_stage(
                    settings=settings,
                    stage=step.name,
                    log=log,
                    log_dir=subprocess_log_dir,
                )
            if risk_refresh_changed_nav_only and step.name == "collect-nav":
                changed_nav_target_file = settings.project_root / "tmp" / "changed_nav_targets_runtime.json"
                changed_nav_target_file.parent.mkdir(parents=True, exist_ok=True)
                if changed_nav_target_file.exists():
                    changed_nav_target_file.unlink()
                command.extend(["--updated-fcodes-output", str(changed_nav_target_file)])
            if (
                step.name == "calculate-risk"
                and risk_refresh_changed_nav_only
                and "--changed-nav-only" in step.command
            ):
                if changed_nav_target_file is None:
                    changed_nav_target_file = settings.project_root / "tmp" / "changed_nav_targets_runtime.json"
                if external_risk_target_codes:
                    existing_codes = _load_risk_refresh_target_codes(changed_nav_target_file)
                    _write_risk_refresh_target_codes(
                        changed_nav_target_file,
                        [*existing_codes, *external_risk_target_codes],
                    )
                if changed_nav_target_file.exists() and not period_returns_checked:
                    command.extend(["--target-fcodes-file", str(changed_nav_target_file)])
                elif period_returns_checked:
                    # A period correction can affect a fund outside the NAV
                    # collector's changed-code file. Scan all input signatures
                    # and still calculate only funds whose inputs changed.
                    log("[calculate-risk] 区间收益已独立刷新，扫描全体风险输入签名以包含同日收益修订")
                if collect_nav_inserted_count == 0:
                    log("[calculate-risk] collect-nav 本轮新增 0 条净值记录，继续执行 changed-nav 风险自愈扫描")
                if external_risk_target_codes:
                    log(f"[calculate-risk] 维护链路额外提示风险重算基金数: {len(external_risk_target_codes)}")

            try:
                output_lines = _run_streaming_subprocess(
                    name=step.name,
                    cmd=command,
                    cwd=settings.core_root,
                    env=env,
                    log=log,
                    log_dir=subprocess_log_dir,
                    timeout=_workflow_task_timeout(settings, step.name),
                    idle_timeout=_workflow_task_idle_timeout(settings, step.name),
                )
            except CoreStepFailedError as exc:
                if step.name in {"collect-details", "collect-managers", "collect-period-returns"} and exc.return_code == 2 and not exc.timed_out:
                    period_returns_checked = period_returns_checked or step.name == "collect-period-returns"
                    deferred_collection_errors.append(exc)
                    log(f"[core-pipeline] {step.name} 采集降级，保留失败并继续独立维护阶段")
                    continue
                exc.deferred_failures.extend(item.as_dict() for item in deferred_collection_errors)
                raise
            if step.name == "collect-period-returns":
                period_returns_checked = True
            if step.name == "collect-nav":
                collect_nav_inserted_count = _extract_collect_nav_inserted_count(output_lines)
                if risk_refresh_changed_nav_only and collect_nav_inserted_count is not None:
                    log(f"[collect-nav] 本轮净值新增记录: {collect_nav_inserted_count}")
        if deferred_collection_errors:
            last_error = deferred_collection_errors[-1]
            last_error.deferred_failures.extend(item.as_dict() for item in deferred_collection_errors[:-1])
            raise last_error
    except Exception as pipeline_error:
        if changed_nav_target_file and changed_nav_target_file.exists():
            changed_nav_target_file.unlink()
        failed_subprocess_index = {}
        try:
            failed_subprocess_index = _build_and_log_subprocess_index(
                settings=settings,
                log_dir=subprocess_log_dir,
                workflow_run_id=capture_run_id,
                log=log,
                planned_task_names=planned_task_names,
            )
        except Exception as index_exc:  # pragma: no cover - diagnostic best effort
            log(f"[core-pipeline] subprocess_index_failed={index_exc}")
        if collection_capture is not None:
            try:
                from backend.collection_calculation_capture import finish_collection_capture

                finish_collection_capture(
                    settings, collection_capture, planned_tasks=planned_task_names,
                    subprocess_index=failed_subprocess_index, pipeline_error=pipeline_error,
                )
                log(f"[core-pipeline] collection_calculation_terminal={collection_capture['record']['terminal_path']}")
            except Exception as capture_exc:  # Preserve the original business failure.
                collection_capture["record"].update(
                    pipeline_outcome="failed", capture_complete=False,
                    capture_error=f"{type(capture_exc).__name__}:{capture_exc}",
                )
                log(f"[core-pipeline] collection_calculation_terminal_failed={capture_exc}")
        raise

    if changed_nav_target_file and changed_nav_target_file.exists():
        changed_nav_target_file.unlink()

    subprocess_log_index = _build_and_log_subprocess_index(
        settings=settings,
        log_dir=subprocess_log_dir,
        workflow_run_id=capture_run_id,
        log=log,
        planned_task_names=planned_task_names,
    )

    artifacts = discover_core_artifacts(settings.core_output_dir)
    artifacts["subprocess_log_dir"] = str(subprocess_log_dir)
    artifacts["subprocess_log_index_file"] = str(subprocess_log_index.get("index_path") or "")
    if collection_capture is not None:
        from backend.collection_calculation_capture import finish_collection_capture

        capture_index = finish_collection_capture(
            settings, collection_capture, planned_tasks=planned_task_names,
            subprocess_index=subprocess_log_index,
        )
        artifacts["collection_calculation_capture"] = dict(collection_capture["record"])
        log(f"[core-pipeline] collection_calculation_terminal={collection_capture['record']['terminal_path']}")
    else:
        from backend.calculation_input_capture import build_calculation_input_index

        capture_index = build_calculation_input_index(
            settings, calculation_input_dir, run_id=workflow_run_id,
            planned_tasks=planned_task_names, subprocess_index=subprocess_log_index,
        )
    artifacts["calculation_input_index_file"] = str(capture_index)
    log(f"[core-pipeline] calculation_input_index={capture_index}")
    return artifacts



def _latest_matching_file(directory: Path, patterns: list[str]) -> Path | None:
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(path for path in glob_artifacts(directory, pattern) if artifact_is_file(path))
    if not candidates:
        return None
    return sorted(candidates, key=lambda path: (original_artifact_stat(path).st_mtime, path.name), reverse=True)[0]



def discover_core_artifacts(output_dir: Path) -> dict[str, str | None]:
    artifacts: dict[str, str | None] = {}
    for key, patterns in CORE_ARTIFACT_PATTERNS.items():
        latest = _latest_matching_file(output_dir, patterns)
        artifacts[key] = str(latest) if latest else None

    final_excel = artifacts.get("final_excel")
    if final_excel:
        final_path = Path(final_excel)
        artifacts["final_excel_meta"] = str(final_path.with_name(f"{final_path.stem}_report_meta.json"))
    comparison_excel = artifacts.get("comparison_excel")
    if comparison_excel:
        comparison_path = Path(comparison_excel)
        artifacts["comparison_excel_meta"] = str(
            comparison_path.with_name(f"{comparison_path.stem}_report_meta.json")
        )
    accumulation_tracker_excel = artifacts.get("accumulation_tracker_excel")
    if accumulation_tracker_excel:
        accumulation_tracker_path = Path(accumulation_tracker_excel)
        artifacts["accumulation_tracker_excel_meta"] = str(
            accumulation_tracker_path.with_name(f"{accumulation_tracker_path.stem}_report_meta.json")
        )

    final_excel_meta = artifacts.get("final_excel_meta")
    if final_excel_meta:
        final_meta_path = Path(final_excel_meta)
        if final_meta_path.exists():
            try:
                import json

                meta = json.loads(final_meta_path.read_text(encoding="utf-8"))
                if isinstance(meta, dict):
                    sidecar_fields = [
                        "only_short_simple_csv",
                        "only_long_simple_csv",
                        "only_short_watchlist_csv",
                        "only_long_watchlist_csv",
                        "comparison_report_file",
                        "autostock_validation_audit_file",
                        "bond_mainlist_quality_audit_json_file",
                        "bond_mainlist_quality_audit_md_file",
                        "final_screening_decision_ledger_file",
                    ]
                    for field in sidecar_fields:
                        value = meta.get(field)
                        if not value:
                            continue
                        field_path = output_dir / str(value)
                        artifacts[field] = str(field_path)
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                pass

    artifacts["core_output_dir"] = str(output_dir)
    return artifacts
