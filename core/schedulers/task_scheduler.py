"""
定时任务调度器
自动化执行数据采集和计算任务
增强版：包含详细的任务生命周期日志、性能监控、调度器状态追踪
SQLite锁库修复版：任务互斥、依赖检查、coalesce防堆积
"""

import logging
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
import traceback
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

from collectors import NavCollector, FundDetailCollector, ManagerCollector, CompanyCollector
from collectors.supplement_holding_period import HoldingPeriodCollector
from analyzers import RiskAnalyzer
from database.task_history import TaskHistoryManager
from validators import AutoStockValidator
from utils.logger import get_logger, setup_logger
from utils.project_runtime_config import load_runtime_config
from utils.manager_collection_summary import validate_manager_collection_summary
from utils.detail_collection_summary import detail_collection_source_errors, validate_detail_collection_summary
from utils.scheduler_day_of_week import (
    format_day_of_week_window as _format_project_day_of_week_window,
    normalize_daily_day_of_week_to_cron,
)
from utils.task_lock import db_write_task_lock, check_task_dependencies, TASK_DEPENDENCIES, TaskStatusManager

logger = get_logger(__name__)

# 尝试导入增强日志
try:
    from utils.enhanced_logger import get_task_logger, get_performance_logger
    HAS_ENHANCED_LOGGER = True
except ImportError:
    HAS_ENHANCED_LOGGER = False

task_logger = None
perf_logger = None


SUBPROCESS_OUTPUT_PREVIEW_LIMIT = 1000
DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT = 6
WEEKDAY_LABELS = {
    0: '日',
    1: '一',
    2: '二',
    3: '三',
    4: '四',
    5: '五',
    6: '六',
    '0': '日',
    '1': '一',
    '2': '二',
    '3': '三',
    '4': '四',
    '5': '五',
    '6': '六',
    'sun': '日',
    'mon': '一',
    'tue': '二',
    'wed': '三',
    'thu': '四',
    'fri': '五',
    'sat': '六',
}

CRON_DAY_OF_WEEK_LABELS = {
    0: '一',
    1: '二',
    2: '三',
    3: '四',
    4: '五',
    5: '六',
    6: '日',
    '0': '一',
    '1': '二',
    '2': '三',
    '3': '四',
    '4': '五',
    '5': '六',
    '6': '日',
    'mon': '一',
    'tue': '二',
    'wed': '三',
    'thu': '四',
    'fri': '五',
    'sat': '六',
    'sun': '日',
}


def _truncate_subprocess_output(value: str, limit: int = SUBPROCESS_OUTPUT_PREVIEW_LIMIT) -> str:
    """截断子进程输出，避免 task_history extra_data 过大。"""
    if not value:
        return ""
    text = value.strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}... [truncated {len(text) - limit} chars]"


def _build_subprocess_failure_metadata(result: subprocess.CompletedProcess, script_path: Path) -> dict:
    """构建子进程失败快照，便于后续排障。"""
    return {
        'status': 'failed',
        'returncode': result.returncode,
        'script_path': str(script_path),
        'stdout_tail': _truncate_subprocess_output(result.stdout),
        'stderr_tail': _truncate_subprocess_output(result.stderr),
    }


def _build_retry_failure_snapshot(
    *,
    attempt: int,
    max_attempts: int,
    execution_id: str,
    error_type: str,
    error: Exception,
    attempt_elapsed: float,
) -> dict:
    """构造单次失败尝试快照，便于进程中断后仍可追溯最近一次真实错误。"""
    snapshot = {
        'attempt': int(attempt),
        'max_attempts': int(max_attempts),
        'execution_id': str(execution_id),
        'error_type': str(error_type or '').strip() or type(error).__name__,
        'error_message': str(error)[:500],
        'attempt_elapsed_seconds': round(float(attempt_elapsed or 0.0), 2),
        'captured_at': datetime.now().isoformat(),
    }
    failure_metadata = getattr(error, 'failure_metadata', None)
    if isinstance(failure_metadata, dict):
        snapshot.update(failure_metadata)
    return snapshot


def _coerce_schedule_part(value):
    """将调度配置片段标准化为 int 或小写字符串。"""
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lstrip('-').isdigit():
            return int(stripped)
        return stripped.lower()
    return value


def _format_weekly_day_label(value) -> str:
    """将 CronTrigger 的 day_of_week 配置格式化为中文周几。"""
    return WEEKDAY_LABELS.get(value, str(value))


def _format_day_of_week_window(day_of_week) -> str:
    """将 day_of_week 配置格式化为中文周几窗口。"""
    return _format_project_day_of_week_window(day_of_week)


def _normalize_weekly_day_of_week_to_cron(day_of_week):
    """将项目内 weekly 配置周几语义转换为 APScheduler Cron 语义。"""
    if isinstance(day_of_week, int):
        config_to_cron = {
            0: 'sun',
            1: 'mon',
            2: 'tue',
            3: 'wed',
            4: 'thu',
            5: 'fri',
            6: 'sat',
        }
        return config_to_cron.get(day_of_week)

    if isinstance(day_of_week, str):
        normalized = day_of_week.strip().lower()
        if normalized in {'sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'}:
            return normalized
        if normalized in {'0', '1', '2', '3', '4', '5', '6'}:
            return _normalize_weekly_day_of_week_to_cron(int(normalized))

    return None


def _first_positive_number(mapping, keys, default):
    """按顺序读取首个正数配置值，新 key 优先，旧 key 兼容回退。"""
    if not isinstance(mapping, dict):
        return default
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return value
    return default


def apply_screening_runtime_defaults(*args, **kwargs):
    """惰性导入运行时环境注入，降低调度器模块导入期耦合。"""
    from bond_fund_screening.tasks.runtime_env import apply_screening_runtime_defaults as _impl

    return _impl(*args, **kwargs)


def is_runtime_excluded_fund(*args, **kwargs):
    """惰性导入排除规则，避免测试场景在 import 阶段拉起额外依赖。"""
    from utils.fund_exclusion_rules import is_runtime_excluded_fund as _impl

    return _impl(*args, **kwargs)


def summarize_task_artifacts(*args, **kwargs):
    """惰性导入任务副产物审计，减少顶层导入链。"""
    from utils.task_artifact_audit import summarize_task_artifacts as _impl

    return _impl(*args, **kwargs)


def get_database_backup_cleanup_policy(*args, **kwargs):
    """惰性导入数据库备份清理策略读取，降低导入期耦合。"""
    from utils.database_backup_cleanup_preview import get_database_backup_cleanup_policy as _impl

    return _impl(*args, **kwargs)


def cleanup_screening_history(*args, **kwargs):
    """惰性导入筛选历史清理逻辑，减少调度器顶层依赖。"""
    from database.screening_history_maintenance import cleanup_screening_history as _impl

    return _impl(*args, **kwargs)


def load_latest_screening_normalized_nav_gray_report_payload(*args, **kwargs):
    """惰性导入规范化净值日期灰度报告读取，降低调度器导入期耦合。"""
    from utils.screening_normalized_nav_gray_report import (
        load_latest_screening_normalized_nav_gray_report_payload as _impl,
    )

    return _impl(*args, **kwargs)


def _build_screening_shadow_gate_summary_fields(shadow_gate_meta: dict | None) -> dict:
    """生成稳定的扁平摘要字段，便于 task_history / 审计脚本直接消费。"""
    meta = shadow_gate_meta if isinstance(shadow_gate_meta, dict) else {}
    status = str(meta.get('status') or 'unknown')
    top_rule_decision = str(meta.get('top_rule_decision') or '')
    recommendation = top_rule_decision or status

    return {
        'normalized_nav_shadow_gate_available': bool(meta.get('available', False)),
        'normalized_nav_shadow_gate_status': status,
        'normalized_nav_shadow_gate_requested_fund_type': str(meta.get('requested_fund_type') or ''),
        'normalized_nav_shadow_gate_covers_current_fund_type': bool(meta.get('covers_current_fund_type', False)),
        'normalized_nav_shadow_gate_top_rule_key': str(meta.get('top_rule_key') or ''),
        'normalized_nav_shadow_gate_top_rule_decision': top_rule_decision,
        'normalized_nav_shadow_gate_top_rule_reason': str(meta.get('top_rule_reason') or ''),
        'normalized_nav_shadow_gate_recommendation': recommendation,
    }


def _sanitize_policy_monitor_summary(summary: dict | None) -> dict:
    """裁剪策略监控摘要中的大字段，避免 task_history extra_data 过大。"""
    if not isinstance(summary, dict):
        return {}
    sanitized = summary.copy()
    sanitized.pop('daily_kpis', None)
    sanitized.pop('daily_verdicts', None)
    return sanitized


class TaskScheduler:
    """任务调度器"""

    def _build_screening_shadow_gate_metadata(self, fund_type: str | None = None) -> dict:
        """读取最近一次 normalized nav 灰度报告，作为只观测不拦截的调度附注。"""
        normalized_fund_type = str(fund_type or "").strip().lower()
        requested_fund_type = normalized_fund_type or "all"

        try:
            artifact_path, payload = load_latest_screening_normalized_nav_gray_report_payload()
        except FileNotFoundError:
            return {
                'available': False,
                'status': 'missing_artifact',
                'requested_fund_type': requested_fund_type,
            }
        except Exception as exc:
            return {
                'available': False,
                'status': 'load_failed',
                'requested_fund_type': requested_fund_type,
                'error': f'{type(exc).__name__}: {exc}',
            }

        payload = payload if isinstance(payload, dict) else {}
        meta = payload.get('meta') if isinstance(payload.get('meta'), dict) else {}
        summary = payload.get('summary') if isinstance(payload.get('summary'), dict) else {}
        rule_decisions = payload.get('rule_decisions') if isinstance(payload.get('rule_decisions'), list) else []
        artifact_fund_types = [
            str(item).strip().lower()
            for item in (meta.get('fund_types') or [])
            if str(item).strip()
        ]
        covers_current_fund_type = (
            not artifact_fund_types
            or requested_fund_type == 'all'
            or normalized_fund_type in artifact_fund_types
        )
        top_rule = rule_decisions[0] if rule_decisions else {}

        return {
            'available': True,
            'status': 'available',
            'artifact_path': str(artifact_path),
            'generated_at': payload.get('generated_at'),
            'requested_fund_type': requested_fund_type,
            'artifact_fund_types': artifact_fund_types,
            'covers_current_fund_type': covers_current_fund_type,
            'window_count_total': int(summary.get('window_count_total') or 0),
            'rule_count_total': int(summary.get('rule_count_total') or 0),
            'not_ready_rule_count': int(summary.get('not_ready_rule_count') or 0),
            'observe_only_rule_count': int(summary.get('observe_only_rule_count') or 0),
            'can_enter_shadow_gray_rule_count': int(summary.get('can_enter_shadow_gray_rule_count') or 0),
            'top_rule_key': str(top_rule.get('rule_key') or ''),
            'top_rule_decision': str(top_rule.get('decision') or ''),
            'top_rule_reason': str(top_rule.get('decision_reason') or ''),
        }

    def _is_testing_mode(self) -> bool:
        """判断当前是否处于测试静噪模式。"""
        if not isinstance(self.config, dict):
            return False
        if self.config.get('testing') is True:
            return True
        scheduler_cfg = self.config.get('scheduler', {})
        return isinstance(scheduler_cfg, dict) and scheduler_cfg.get('testing') is True

    def _log_routine_info(self, message: str, *args):
        """测试环境将高频例行日志降为 debug，生产环境保持 info。"""
        if self._is_testing_mode():
            logger.debug(message, *args)
            return
        logger.info(message, *args)

    def _log_routine_warning(self, message: str, *args):
        """测试环境将例行 warning 降为 debug，减少回归日志噪音。"""
        if self._is_testing_mode():
            logger.debug(message, *args)
            return
        logger.warning(message, *args)

    @staticmethod
    def _build_daily_fund_snapshot_plan(type_payloads, existing_codes, merge_func):
        """构建每日基金列表快照落库计划，避免跨类型重复写入。"""
        known_codes = set(existing_codes or set())
        snapshot_funds = {}
        type_stats = {}
        total_new = 0

        for type_name, funds in type_payloads:
            valid_funds = [fund for fund in (funds or []) if fund.get('fcode')]
            api_codes = {fund['fcode'] for fund in valid_funds}
            new_codes = api_codes - known_codes
            known_codes.update(new_codes)
            total_new += len(new_codes)

            type_stats[type_name] = {
                'total': len(api_codes),
                'new': len(new_codes),
            }

            for fund in valid_funds:
                fcode = fund['fcode']
                existing_fund = snapshot_funds.get(fcode)
                if existing_fund is None:
                    snapshot_funds[fcode] = fund
                else:
                    snapshot_funds[fcode] = merge_func(existing_fund, fund)

        return {
            'snapshot_funds': list(snapshot_funds.values()),
            'snapshot_total': len(snapshot_funds),
            'total_new': total_new,
            'type_stats': type_stats,
        }

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化调度器"""
        global task_logger, perf_logger
        setup_logger(__name__)
        if HAS_ENHANCED_LOGGER:
            if task_logger is None:
                task_logger = get_task_logger()
            if perf_logger is None:
                perf_logger = get_performance_logger()
        # 防御性加载：config 为空时自动从 config.yaml 加载
        if not config:
            config = self._load_default_config()
        self.config = config

        # Step 5F: db_manager 为 None 时 fallback
        if db_manager is None:
            from database.connection import get_db_manager
            self.db_manager = get_db_manager()
        else:
            self.db_manager = db_manager

        # api_client 为 None 时使用 self.config 自动创建
        if api_client is None:
            from api.client import APIClient
            self.api_client = APIClient(self.config)
            logger.info(f"TaskScheduler: 自动创建 APIClient (base_url: {self.api_client.base_url})")
        else:
            self.api_client = api_client

        # 调度器配置
        scheduler_config = self.config.get('scheduler', {})
        self.timezone = scheduler_config.get('timezone', 'Asia/Shanghai')
        self.timeouts = scheduler_config.get('timeouts', {})
        self.schedules = scheduler_config.get('schedules', {})
        self.task_switches = scheduler_config.get('tasks', {})
        self.retry_config = scheduler_config.get('retry', {
            'max_attempts': 3,
            'initial_delay': 60,  # 初始重试延迟60秒
            'max_delay': 240  # 最大重试延迟240秒
        })

        # 创建调度器
        self.scheduler = BackgroundScheduler(timezone=self.timezone)
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)

        # 任务执行上下文（用于跟踪正在运行的任务）
        self._running_tasks = {}
        self._running_tasks_lock = threading.Lock()

        # Step 5F: 防重入锁（防止 APScheduler 并发触发同一任务）
        self._task_execution_locks = {}
        self._task_execution_locks_meta = threading.Lock()

        # 任务执行统计（使用 Lock 保护，防止 _job_listener 中的竞态条件）
        self.stats = {
            'total_executed': 0,
            'total_success': 0,
            'total_failed': 0,
            'last_execution': {}
        }
        self._stats_lock = threading.Lock()

        # 注册信号处理（仅在主线程中）
        # 修复: signal.signal() 只能在主线程中调用
        # 当从Web API的后台线程创建TaskScheduler时，跳过signal注册
        if threading.current_thread() is threading.main_thread():
            try:
                signal.signal(signal.SIGINT, self._signal_handler)
                signal.signal(signal.SIGTERM, self._signal_handler)
                logger.debug("已注册信号处理器（主线程）")
            except ValueError as e:
                logger.warning(f"无法注册信号处理器: {e}")
        else:
            logger.debug("跳过信号处理器注册（非主线程）")

    @staticmethod
    def _load_default_config():
        """从项目级配置加载默认运行配置。"""
        config_path = Path(__file__).resolve().parents[2] / 'config' / 'project.yaml'
        if config_path.exists():
            config = load_runtime_config(config_path)
            logger.info(f"TaskScheduler: 自动加载配置文件 {config_path}")
            return config or {}
        logger.warning(f"TaskScheduler: 配置文件不存在 {config_path}，使用空配置")
        return {}

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，正在停止调度器...")
        self.stop()
        sys.exit(0)

    def _job_listener(self, event):
        """任务执行监听器（增强版：详细的事件日志）"""
        job_id = event.job_id
        event_time = datetime.now()
        thread_id = threading.current_thread().ident

        # 判断事件类型
        if hasattr(event, 'exception') and event.exception:
            # 任务执行失败
            with self._stats_lock:
                self.stats['total_executed'] += 1
                self.stats['total_failed'] += 1

            error_type = type(event.exception).__name__
            error_msg = str(event.exception)[:200]

            if HAS_ENHANCED_LOGGER:
                task_logger.log_error(job_id, error_msg,
                                     error_type=error_type,
                                     thread_id=thread_id,
                                     scheduled_time=event.scheduled_run_time.isoformat() if hasattr(event, 'scheduled_run_time') and event.scheduled_run_time else None)
            else:
                logger.error(f"[SCHEDULER] 任务失败 job={job_id} error={error_type}: {error_msg}")

        elif hasattr(event, 'scheduled_run_time') and event.scheduled_run_time:
            # 任务执行成功
            with self._stats_lock:
                self.stats['total_executed'] += 1
                self.stats['total_success'] += 1
                self.stats['last_execution'][job_id] = event_time

            # 计算调度延迟（实际执行时间 - 计划执行时间）
            scheduled_time = event.scheduled_run_time
            if scheduled_time.tzinfo:
                # 将event_time转换为与scheduled_time相同的时区
                # 注意：zoneinfo.ZoneInfo 没有 localize 方法，使用 replace 代替
                event_time_aware = event_time.replace(tzinfo=scheduled_time.tzinfo)
                schedule_delay = (event_time_aware - scheduled_time).total_seconds()
            else:
                schedule_delay = (event_time - scheduled_time).total_seconds()

            if HAS_ENHANCED_LOGGER:
                task_logger.log_complete(job_id, 0, success=True,
                                        schedule_delay_sec=round(schedule_delay, 2),
                                        thread_id=thread_id)
            else:
                logger.info(f"[SCHEDULER] 任务完成 job={job_id} delay={schedule_delay:.2f}s")

        else:
            # 任务错过执行（EVENT_JOB_MISSED）
            if HAS_ENHANCED_LOGGER:
                task_logger.log_error(job_id, "任务错过计划执行时间",
                                     error_type="MISSED",
                                     thread_id=thread_id)
            else:
                logger.warning(f"[SCHEDULER] 任务错过执行 job={job_id}")

    def _get_dependency_status_snapshot(self, dep_keys: list) -> dict:
        """
        Step 6-2: 查询 task_status 表，构建依赖状态快照（用于 skip 可诊断化）

        当依赖不满足导致跳过时，记录每个依赖在 task_status 表中的实际状态，
        便于排障判断是"没记录"还是"有记录但没完成"。

        Args:
            dep_keys: 依赖任务的 task_key 列表

        Returns:
            dict: {checked_on, dependencies: {key: {exists, status, started_at, completed_at}}}
        """
        snapshot = {
            'checked_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'dependencies': {}
        }
        try:
            from database.models import TaskStatus
            with self.db_manager.get_session(readonly=True) as session:
                for dep_key in dep_keys:
                    task = session.query(TaskStatus).filter_by(task_name=dep_key).first()
                    if task:
                        snapshot['dependencies'][dep_key] = {
                            'exists': True,
                            'status': task.status,
                            'started_at': task.started_at.isoformat() if task.started_at else None,
                            'completed_at': task.completed_at.isoformat() if task.completed_at else None,
                        }
                    else:
                        snapshot['dependencies'][dep_key] = {'exists': False}
        except Exception as e:
            snapshot['error'] = str(e)
        return snapshot

    def _skip_if_dependency_unsatisfied(self, task_key: str, display_name: str) -> bool:
        """
        Step 5E-2: 检查任务依赖，不满足时写 task_history(status='skipped') 并返回 True

        Args:
            task_key: 任务英文标识（用于 TASK_DEPENDENCIES 查询）
            display_name: 任务中文名称（用于 task_history 记录）

        Returns:
            True = 依赖不满足，已跳过（调用方应直接 return）
            False = 依赖满足，继续执行
        """
        dep = check_task_dependencies(task_key, db_manager=self.db_manager)

        if dep['satisfied']:
            return False

        # 提取未满足的依赖列表
        unsatisfied = [k for k, v in dep['dependencies'].items() if not v['satisfied']]
        logger.warning(f"[SKIP] task={task_key} 依赖不满足 unsatisfied={unsatisfied}")

        # Step 6-2: 构建依赖状态快照，用于排障
        dep_snapshot = self._get_dependency_status_snapshot(
            [k for k in dep['dependencies']]
        )

        # 写 task_history: status='skipped'
        try:
            with self.db_manager.get_session() as session:
                history_mgr = TaskHistoryManager(session)
                task_info = history_mgr.start_task(display_name)
                history_mgr.complete_task(
                    task_info,
                    status_override='skipped',
                    extra_data={
                        'skipped': True,
                        'reason': 'dependency_not_satisfied',
                        'unsatisfied_dependencies': unsatisfied,
                        'dependency_result': dep,
                        'dependency_status_snapshot': dep_snapshot,
                    }
                )
        except Exception as e:
            logger.error(f"[SKIP] 写入 skipped 历史失败 task={task_key}: {e}")

        return True

    def _execute_with_history(self, task_name: str, task_func, *args, task_key: str = None, **kwargs):
        """
        执行任务并记录历史（带重试机制和增强日志）

        Args:
            task_name: 任务名称（中文，用于 task_history 展示）
            task_func: 任务函数
            *args: 任务函数位置参数
            task_key: 任务英文标识（用于 task_status 表同步，如 'daily_nav_update'）
            **kwargs: 任务函数关键字参数

        Returns:
            任务执行结果
        """
        # Step 5F: 防重入 — 同一 task 只允许一个实例并发执行
        lock_key = task_key or task_name
        with self._task_execution_locks_meta:
            if lock_key not in self._task_execution_locks:
                self._task_execution_locks[lock_key] = threading.Lock()
            task_lock = self._task_execution_locks[lock_key]

        if not task_lock.acquire(blocking=False):
            logger.warning(f"[TASK] 防重入：{lock_key} 已在执行中，跳过本次触发")
            return None

        try:
            return self._execute_with_history_inner(task_name, task_func, *args, task_key=task_key, **kwargs)
        finally:
            task_lock.release()

    def _execute_with_history_inner(self, task_name: str, task_func, *args, task_key: str = None, **kwargs):
        """_execute_with_history 的内部实现（防重入锁已在外层获取）

        关键设计：start_task 和 complete_task 使用独立的短 session，
        task_func() 执行期间不持有任何数据库 session，避免长时间占用全局写锁。
        """
        # 生成唯一的执行ID
        execution_id = str(uuid.uuid4())[:8]
        thread_id = threading.current_thread().ident
        overall_start_time = time.time()

        # 记录任务开始到运行中列表
        with self._running_tasks_lock:
            self._running_tasks[execution_id] = {
                'task_name': task_name,
                'start_time': datetime.now(),
                'thread_id': thread_id
            }

        if HAS_ENHANCED_LOGGER:
            task_logger.log_start(task_name, execution_id=execution_id, thread_id=thread_id)
        else:
            logger.info(f"[TASK:{execution_id}] 任务开始 name={task_name} thread={thread_id}")

        # --- 短 session 1: 记录任务开始 ---
        task_info = None
        with self.db_manager.get_session() as session:
            history_manager = TaskHistoryManager(session)
            task_info = history_manager.start_task(task_name)

        # Step 5E: 同步写 task_status 表（供依赖检查使用）
        _task_status_mgr = None
        if task_key:
            try:
                _task_status_mgr = TaskStatusManager(db_manager=self.db_manager)
                started = _task_status_mgr.start_task(task_key)
            except Exception as e:
                logger.warning(f"[TASK_STATUS] 同步 start_task 失败 task_key={task_key}: {e}")
            else:
                if started is False:
                    logger.warning(f"[TASK_STATUS] task_key={task_key} 已在其他进程运行，跳过本次执行")
                    with self.db_manager.get_session() as session:
                        history_manager = TaskHistoryManager(session)
                        history_manager.complete_task(
                            task_info,
                            success=True,
                            records_processed=0,
                            records_inserted=0,
                            records_updated=0,
                            records_failed=0,
                            extra_data={
                                'execution_id': execution_id,
                                'status': 'skipped',
                                'skip_reason': 'task_already_running',
                                'task_key': task_key,
                            },
                            status_override='skipped'
                        )

                    with self._running_tasks_lock:
                        self._running_tasks.pop(execution_id, None)

                    if HAS_ENHANCED_LOGGER:
                        task_logger.log_complete(
                            task_name,
                            round((time.time() - overall_start_time) * 1000, 2),
                            success=True,
                            execution_id=execution_id,
                            attempts=0,
                            records_processed=0,
                        )
                    return {
                        'count': 0,
                        'processed': 0,
                        'inserted': 0,
                        'updated': 0,
                        'failed': 0,
                        'metadata': {
                            'status': 'skipped',
                            'skip_reason': 'task_already_running',
                            'task_key': task_key,
                            'execution_id': execution_id,
                        },
                    }

        # 重试配置
        max_attempts = self.retry_config['max_attempts']
        initial_delay = self.retry_config['initial_delay']
        max_delay = self.retry_config['max_delay']

        last_error = None
        last_failure_snapshot = None
        result = None

        for attempt in range(1, max_attempts + 1):
            attempt_start_time = time.time()

            try:
                if HAS_ENHANCED_LOGGER:
                    task_logger.log_progress(task_name, attempt, max_attempts,
                                            execution_id=execution_id,
                                            phase='ATTEMPT_START')
                else:
                    logger.info(f"[TASK:{execution_id}] 执行尝试 {attempt}/{max_attempts}")

                # 执行任务（不持有任何数据库 session）
                result = task_func(*args, **kwargs)

                # 计算执行时间
                attempt_elapsed = time.time() - attempt_start_time
                overall_elapsed = time.time() - overall_start_time

                # 提取统计数据
                stats = result if isinstance(result, dict) else {'count': result if isinstance(result, int) else 0}

                # --- 短 session 2: 记录成功 ---
                metadata = stats.get('metadata', {})
                history_status_override = metadata.get('status') if metadata.get('status') in {'skipped', 'degraded'} else None
                task_status_success = history_status_override not in {'skipped', 'degraded'}

                with self.db_manager.get_session() as session:
                    history_manager = TaskHistoryManager(session)
                    success_extra = {
                        **metadata,
                        'execution_id': execution_id,
                        'attempts': attempt,
                        'elapsed_seconds': round(overall_elapsed, 2)
                    }
                    if last_failure_snapshot is not None:
                        success_extra.update({
                            'last_failure_snapshot': last_failure_snapshot,
                            'recovered_after_retry': True,
                            'recovered_attempt_count': attempt,
                        })
                    history_manager.complete_task(
                        task_info,
                        success=task_status_success,
                        records_processed=stats.get('processed', stats.get('count', 0)),
                        records_inserted=stats.get('inserted', 0),
                        records_updated=stats.get('updated', stats.get('count', 0)),
                        records_failed=stats.get('failed', 0),
                        extra_data=success_extra,
                        status_override=history_status_override
                    )

                # Step 5E: 同步写 task_status 表（成功/跳过）
                if _task_status_mgr and task_key:
                    try:
                        status_kwargs = {'success': task_status_success}
                        if history_status_override == 'skipped':
                            status_kwargs['status_override'] = 'skipped'
                            status_kwargs['message'] = (
                                str(metadata.get('task_status_message') or '').strip()
                                or '业务跳过：全部基金无可用净值数据'
                            )
                        _task_status_mgr.complete_task(task_key, **status_kwargs)
                    except Exception as e:
                        logger.warning(f"[TASK_STATUS] 同步 complete_task(done) 失败 task_key={task_key}: {e}")

                # 从运行中列表移除
                with self._running_tasks_lock:
                    self._running_tasks.pop(execution_id, None)

                if HAS_ENHANCED_LOGGER:
                    task_logger.log_complete(task_name, round(overall_elapsed * 1000, 2),
                                            success=task_status_success,
                                            execution_id=execution_id,
                                            attempts=attempt,
                                            records_processed=stats.get('processed', stats.get('count', 0)))
                    # 记录性能指标
                    perf_logger.log_metric(f"task.{task_name}.duration_ms", round(overall_elapsed * 1000, 2))
                    perf_logger.log_metric(f"task.{task_name}.success", 1 if task_status_success else 0)
                else:
                    logger.info(f"[TASK:{execution_id}] 任务完成 name={task_name} "
                               f"elapsed={overall_elapsed:.2f}s attempts={attempt}")

                return result

            except Exception as e:
                last_error = e
                attempt_elapsed = time.time() - attempt_start_time
                error_type = type(e).__name__
                error_msg = str(e)[:500]
                failure_snapshot = _build_retry_failure_snapshot(
                    attempt=attempt,
                    max_attempts=max_attempts,
                    execution_id=execution_id,
                    error_type=error_type,
                    error=e,
                    attempt_elapsed=attempt_elapsed,
                )
                last_failure_snapshot = failure_snapshot

                if HAS_ENHANCED_LOGGER:
                    task_logger.log_error(task_name, error_msg,
                                         execution_id=execution_id,
                                         error_type=error_type,
                                         attempt=attempt,
                                         elapsed_ms=round(attempt_elapsed * 1000, 2),
                                         traceback=traceback.format_exc()[:1000])
                else:
                    logger.error(f"[TASK:{execution_id}] 执行失败 attempt={attempt}/{max_attempts} "
                                f"error={error_type}: {error_msg}")

                try:
                    with self.db_manager.get_session() as session:
                        history_manager = TaskHistoryManager(session)
                        history_manager.merge_extra_data(
                            task_info,
                            {
                                'last_failure_snapshot': failure_snapshot,
                            },
                        )
                except Exception as history_err:
                    logger.warning(f"[TASK:{execution_id}] 写入中间失败快照时出错: {history_err}")

                # 如果不是最后一次尝试，等待后重试
                if attempt < max_attempts:
                    # 指数退避: 60s, 120s, 240s
                    delay = min(initial_delay * (2 ** (attempt - 1)), max_delay)

                    if HAS_ENHANCED_LOGGER:
                        task_logger.log_progress(task_name, attempt, max_attempts,
                                                execution_id=execution_id,
                                                phase='RETRY_WAIT',
                                                wait_seconds=delay)
                    else:
                        logger.info(f"[TASK:{execution_id}] 等待重试 delay={delay}s")

                    time.sleep(delay)
                else:
                    # 最后一次失败
                    overall_elapsed = time.time() - overall_start_time

                    # --- 短 session 3: 记录失败（独立 session，确保失败记录不会因 rollback 丢失）---
                    try:
                        with self.db_manager.get_session() as session:
                            history_manager = TaskHistoryManager(session)
                            failure_extra = {
                                'execution_id': execution_id,
                                'attempts': max_attempts,
                                'elapsed_seconds': round(overall_elapsed, 2),
                                'error_type': error_type,
                                'last_failure_snapshot': failure_snapshot,
                            }
                            if hasattr(last_error, 'failure_metadata') and isinstance(last_error.failure_metadata, dict):
                                failure_extra.update(last_error.failure_metadata)
                            history_manager.complete_task(
                                task_info,
                                success=False,
                                error_message=str(last_error),
                                extra_data=failure_extra
                            )
                    except Exception as history_err:
                        logger.error(f"[TASK:{execution_id}] 记录失败历史时出错: {history_err}")

                    # Step 5E: 同步写 task_status 表（失败）
                    if _task_status_mgr and task_key:
                        try:
                            _task_status_mgr.complete_task(task_key, success=False,
                                                          message=str(last_error)[:200])
                        except Exception as e:
                            logger.warning(f"[TASK_STATUS] 同步 complete_task(failed) 失败 task_key={task_key}: {e}")

                    # 从运行中列表移除
                    with self._running_tasks_lock:
                        self._running_tasks.pop(execution_id, None)

                    if HAS_ENHANCED_LOGGER:
                        task_logger.log_complete(task_name, round(overall_elapsed * 1000, 2),
                                                success=False,
                                                execution_id=execution_id,
                                                attempts=max_attempts,
                                                error_type=error_type)
                        perf_logger.log_metric(f"task.{task_name}.duration_ms", round(overall_elapsed * 1000, 2))
                        perf_logger.log_metric(f"task.{task_name}.failure", 1)
                    else:
                        logger.error(f"[TASK:{execution_id}] 任务最终失败 name={task_name} "
                                    f"elapsed={overall_elapsed:.2f}s attempts={max_attempts}")

                    raise

    def _parse_schedule_time(self, key: str, default: list) -> tuple:
        """解析调度时间配置，返回 (hour, minute)

        防御空列表、非列表、长度不足等异常配置
        """
        schedule_time = self.schedules.get(key, default)
        if isinstance(schedule_time, str):
            schedule_time = [part.strip() for part in schedule_time.split(',') if part.strip()]
        elif isinstance(schedule_time, tuple):
            schedule_time = list(schedule_time)
        if not isinstance(schedule_time, list) or len(schedule_time) < 2:
            schedule_time = list(default)
        schedule_time = [_coerce_schedule_part(part) for part in schedule_time]
        hour, minute = schedule_time[0], schedule_time[1]
        # 验证 hour/minute 合法范围，不合法时回退到 default
        if not (isinstance(hour, int) and 0 <= hour <= 23
                and isinstance(minute, int) and 0 <= minute <= 59):
            logger.warning(f"调度时间配置非法 key={key} hour={hour} minute={minute}，回退到默认值 {default}")
            hour, minute = default[0], default[1]
        return hour, minute

    def _parse_daily_schedule(self, key: str, default: list) -> tuple:
        """解析每日任务调度配置，返回 (hour, minute, day_of_week)。"""
        schedule_time = self.schedules.get(key, default)
        if isinstance(schedule_time, str):
            schedule_time = [part.strip() for part in schedule_time.split(',') if part.strip()]
        elif isinstance(schedule_time, tuple):
            schedule_time = list(schedule_time)
        if not isinstance(schedule_time, list) or len(schedule_time) < 2:
            schedule_time = list(default)
        schedule_time = [_coerce_schedule_part(part) for part in schedule_time]

        hour, minute = self._parse_schedule_time(key, default[:2])
        if len(schedule_time) < 3:
            return hour, minute, None

        day_of_week = schedule_time[2]
        normalized_day_of_week = normalize_daily_day_of_week_to_cron(day_of_week)
        if normalized_day_of_week:
            return hour, minute, normalized_day_of_week

        logger.warning(f"每日调度配置非法 key={key} day_of_week={day_of_week}，忽略周几约束")
        return hour, minute, None

    @staticmethod
    def _serialize_scheduler_fingerprint_value(value):
        """序列化输入指纹中的日期时间字段。"""
        return TaskScheduler._normalize_scheduler_fingerprint_payload(value)

    @staticmethod
    def _normalize_scheduler_fingerprint_payload(value):
        """把输入指纹值归一化为稳定的 JSON 原生类型。"""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, dict):
            return {
                str(key): TaskScheduler._normalize_scheduler_fingerprint_payload(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [
                TaskScheduler._normalize_scheduler_fingerprint_payload(item)
                for item in value
            ]
        return str(value)

    @staticmethod
    def _build_scheduler_fingerprint_hash(payload: dict) -> str:
        """基于规范化 JSON 构建输入指纹哈希。"""
        serialized = json.dumps(
            TaskScheduler._normalize_scheduler_fingerprint_payload(payload or {}),
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        )
        return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

    def _build_screening_runtime_config_fingerprint(self) -> dict:
        """构建长短期筛选主链运行时配置指纹。"""
        try:
            from bond_fund_screening.core.screening_tracker import get_config_snapshot

            snapshot = get_config_snapshot()
            if not isinstance(snapshot, dict):
                raise TypeError('get_config_snapshot() 未返回字典')

            scoring_policy = snapshot.get('scoring_policy', {}) or {}
            policy_section = scoring_policy.get('policy', {}) or {}
            runtime_section = scoring_policy.get('runtime', {}) or {}
            recommended_section = scoring_policy.get('recommended', {}) or {}
            override_section = scoring_policy.get('override', {}) or {}

            return {
                'ready': True,
                'hash': self._build_scheduler_fingerprint_hash(snapshot),
                'default_score_key': (
                    str(policy_section.get('default_score_key') or '').strip() or None
                ),
                'runtime_enabled': bool(runtime_section.get('enabled')),
                'recommended_enabled': bool(recommended_section.get('enabled')),
                'override_enabled': bool(override_section.get('enabled')),
            }
        except Exception as exc:
            logger.warning("构建筛选运行时配置指纹失败，禁止跳过正式筛选: %s", exc)
            return {
                'ready': False,
                'hash': None,
                'error': str(exc),
            }

    def _build_final_screening_policy_fingerprint(self, fund_type: str) -> dict:
        """构建最终筛选策略指纹，避免仅改策略时被误跳过。"""
        normalized_fund_type = str(fund_type or '').strip().lower()

        try:
            from bond_fund_screening.config import (
                FINAL_SCREENING_POLICY,
                FINAL_SCREENING_POLICY_BY_FUND_TYPE,
            )
            from bond_fund_screening.tasks import (
                final_screening_artifact_helpers as artifact_helpers,
            )
            from bond_fund_screening.tasks import (
                final_screening_autostock_helpers as autostock_helpers,
            )
            from bond_fund_screening.tasks import (
                final_screening_runtime_config_helpers as runtime_config_helpers,
            )
            from utils.config_manager import get_config_manager

            raw_policy = dict(FINAL_SCREENING_POLICY)
            raw_policy.update(FINAL_SCREENING_POLICY_BY_FUND_TYPE.get(normalized_fund_type, {}))
            anchor_limit_raw = raw_policy.get('mainlist_anchor_limit', raw_policy.get('top_n'))
            if anchor_limit_raw in (None, '', 0):
                mainlist_anchor_limit = None
            else:
                mainlist_anchor_limit = max(int(anchor_limit_raw), 1)
            watchlist_limit = max(int(raw_policy.get('watchlist_limit', 20) or 20), 1)
            mainlist_selection_mode = (
                str(raw_policy.get('mainlist_selection_mode', 'all_eligible') or 'all_eligible').strip()
                or 'all_eligible'
            )
            normalized_policy = {
                'mainlist_anchor_limit': mainlist_anchor_limit,
                'watchlist_limit': watchlist_limit,
                'short_score_weight': float(raw_policy.get('short_score_weight', 0.35)),
                'long_score_weight': float(raw_policy.get('long_score_weight', 0.50)),
                'hard_intersection_bonus': float(raw_policy.get('hard_intersection_bonus', 15.0)),
                'soft_intersection_bonus': float(raw_policy.get('soft_intersection_bonus', 8.0)),
                'mainlist_selection_mode': mainlist_selection_mode,
                'mainlist_score_band_delta': max(
                    float(raw_policy.get('mainlist_score_band_delta', 0.0) or 0.0),
                    0.0,
                ),
                'bond_subtype_edge_governance_enabled': bool(
                    raw_policy.get('bond_subtype_edge_governance_enabled', False)
                ),
                'bond_subtype_edge_warn_share_pct': max(
                    float(raw_policy.get('bond_subtype_edge_warn_share_pct', 70.0) or 70.0),
                    0.0,
                ),
                'bond_subtype_edge_max_replacements': max(
                    int(raw_policy.get('bond_subtype_edge_max_replacements', 0) or 0),
                    0,
                ),
                'bond_subtype_edge_score_gap_tolerance': max(
                    float(raw_policy.get('bond_subtype_edge_score_gap_tolerance', 0.0) or 0.0),
                    0.0,
                ),
                'drawdown_duration_analysis_window_days': max(
                    int(raw_policy.get('drawdown_duration_analysis_window_days', 365)),
                    30,
                ),
                'drawdown_duration_nav_buffer_days': max(
                    int(raw_policy.get('drawdown_duration_nav_buffer_days', 30)),
                    0,
                ),
            }
            autostock_enabled = runtime_config_helpers.should_apply_autostock_cross_validation(
                normalized_fund_type
            )
            autostock_config = (
                autostock_helpers.load_autostock_cross_validation_config(
                    config_loader=lambda: get_config_manager().load_config(),
                    env=os.environ,
                    default_base_url='https://api.autostock.pro',
                    safe_batch_size=50,
                    print_fn=lambda *_args, **_kwargs: None,
                )
                if autostock_enabled else None
            )
            policy_snapshot = artifact_helpers.build_final_screening_policy_snapshot(
                fund_type=normalized_fund_type,
                final_policy=normalized_policy,
                single_side_watchlist_limit=watchlist_limit,
                short_direct_score_column=None,
                long_direct_score_column=None,
                autostock_config=autostock_config,
                min_cross_periods=(
                    runtime_config_helpers.FINAL_SCREENING_SOFT_INTERSECTION_MIN_CROSS_PERIODS
                ),
                schema_version=(
                    runtime_config_helpers.FINAL_SCREENING_POLICY_SNAPSHOT_SCHEMA_VERSION
                ),
                should_filter_holding_period_fn=(
                    runtime_config_helpers.should_filter_holding_period_funds
                ),
                should_apply_autostock_cross_validation_fn=(
                    runtime_config_helpers.should_apply_autostock_cross_validation
                ),
            )
            return {
                'ready': True,
                'policy_id': (
                    str(policy_snapshot.get('policy_id') or '').strip() or None
                ),
                'policy_version': (
                    str(policy_snapshot.get('policy_version') or '').strip() or None
                ),
                'hash': str(policy_snapshot.get('policy_hash') or '').strip() or None,
                'mainlist_anchor_limit': normalized_policy.get('mainlist_anchor_limit'),
                'watchlist_limit': int(normalized_policy.get('watchlist_limit', 0) or 0),
                'mainlist_selection_mode': normalized_policy.get('mainlist_selection_mode'),
                'autostock_enabled': bool(autostock_enabled),
            }
        except Exception as exc:
            logger.warning("构建最终筛选策略指纹失败，禁止跳过正式筛选: %s", exc)
            return {
                'ready': False,
                'hash': None,
                'error': str(exc),
            }

    @staticmethod
    def _parse_task_history_extra_data(extra_data):
        """解析 task_history.extra_data，兼容 dict / JSON 字符串。"""
        if isinstance(extra_data, dict):
            return extra_data
        if not extra_data:
            return {}
        try:
            payload = json.loads(extra_data)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _get_screening_workflow_type_codes(fund_type: str) -> tuple[str, ...]:
        """返回每日筛选工作流对应的基金类型代码。"""
        normalized = str(fund_type or '').strip().lower()
        type_code_map = {
            'bond': ('31', '32'),
            'stock': ('25',),
            'hybrid': ('27',),
        }
        return type_code_map.get(normalized, tuple())

    @staticmethod
    def _normalize_scheduler_aggregate_row(row, expected_size: int) -> tuple:
        """标准化聚合查询结果，兼容 SQLAlchemy Row / tuple / 可迭代测试替身。"""
        if expected_size <= 0:
            return tuple()

        if row is None:
            return tuple([None] * expected_size)

        values = None
        if isinstance(row, (list, tuple)):
            values = tuple(row)
        else:
            try:
                values = tuple(row)
            except TypeError:
                values = None

        if values is None or len(values) == 0:
            item_values = []
            for index in range(expected_size):
                try:
                    item_values.append(row[index])
                except Exception:
                    item_values = []
                    break
            if item_values:
                values = tuple(item_values)

        if values is None or len(values) == 0:
            values = (row,)

        if len(values) < expected_size:
            values = values + tuple([None] * (expected_size - len(values)))
        elif len(values) > expected_size:
            values = values[:expected_size]
        return values

    def _build_screening_workflow_input_fingerprint(self, fund_type: str) -> dict:
        """构建每日筛选工作流输入指纹。"""
        from sqlalchemy import func
        from database.models import Fund, FundReturns, FundRiskMetrics
        from version import __version__

        type_codes = self._get_screening_workflow_type_codes(fund_type)
        if not type_codes:
            raise ValueError(f'不支持的筛选工作流基金类型: {fund_type}')

        with self.db_manager.get_session(readonly=True) as session:
            fund_count, fund_updated_at = self._normalize_scheduler_aggregate_row(session.query(
                func.count(Fund.fcode),
                func.max(Fund.last_updated_at),
            ).filter(Fund.type_code.in_(type_codes)).one(), 2)

            returns_count, returns_updated_at, latest_nav_date = self._normalize_scheduler_aggregate_row(session.query(
                func.count(FundReturns.fcode),
                func.max(FundReturns.last_updated_at),
                func.max(FundReturns.latest_nav_date),
            ).join(Fund, Fund.fcode == FundReturns.fcode).filter(
                Fund.type_code.in_(type_codes)
            ).one(), 3)

            risk_count, risk_fund_count, risk_calculated_at, risk_end_date = self._normalize_scheduler_aggregate_row(session.query(
                func.count(FundRiskMetrics.id),
                func.count(func.distinct(FundRiskMetrics.fcode)),
                func.max(FundRiskMetrics.calculated_at),
                func.max(FundRiskMetrics.end_date),
            ).join(Fund, Fund.fcode == FundRiskMetrics.fcode).filter(
                Fund.type_code.in_(type_codes)
            ).one(), 4)

        screening_strategy = self._build_screening_runtime_config_fingerprint()
        if not isinstance(screening_strategy, dict):
            screening_strategy = {}
        final_screening_policy = self._build_final_screening_policy_fingerprint(fund_type)
        if not isinstance(final_screening_policy, dict):
            final_screening_policy = {}
        allow_skip = bool(
            screening_strategy.get('ready') and final_screening_policy.get('ready')
        )

        snapshot = {
            'fund_type': str(fund_type or '').strip().lower(),
            'type_codes': list(type_codes),
            'code_version': __version__,
            'funds': {
                'count': int(fund_count or 0),
                'last_updated_at': self._serialize_scheduler_fingerprint_value(fund_updated_at),
            },
            'returns': {
                'count': int(returns_count or 0),
                'last_updated_at': self._serialize_scheduler_fingerprint_value(returns_updated_at),
                'latest_nav_date': self._serialize_scheduler_fingerprint_value(latest_nav_date),
            },
            'risk_metrics': {
                'count': int(risk_count or 0),
                'fund_coverage': int(risk_fund_count or 0),
                'calculated_at': self._serialize_scheduler_fingerprint_value(risk_calculated_at),
                'end_date': self._serialize_scheduler_fingerprint_value(risk_end_date),
            },
            'screening_strategy': {
                'ready': bool(screening_strategy.get('ready')),
                'config_hash': screening_strategy.get('hash'),
                'default_score_key': screening_strategy.get('default_score_key'),
                'runtime_enabled': screening_strategy.get('runtime_enabled'),
                'recommended_enabled': screening_strategy.get('recommended_enabled'),
                'override_enabled': screening_strategy.get('override_enabled'),
                'error': screening_strategy.get('error'),
            },
            'final_screening_policy': {
                'ready': bool(final_screening_policy.get('ready')),
                'policy_id': final_screening_policy.get('policy_id'),
                'policy_version': final_screening_policy.get('policy_version'),
                'policy_hash': final_screening_policy.get('hash'),
                'mainlist_anchor_limit': final_screening_policy.get('mainlist_anchor_limit'),
                'watchlist_limit': final_screening_policy.get('watchlist_limit'),
                'mainlist_selection_mode': final_screening_policy.get('mainlist_selection_mode'),
                'autostock_enabled': final_screening_policy.get('autostock_enabled'),
                'error': final_screening_policy.get('error'),
            },
            'allow_skip': allow_skip,
        }
        snapshot_hash = self._build_scheduler_fingerprint_hash(snapshot)
        return {
            'snapshot': snapshot,
            'hash': snapshot_hash,
            'allow_skip': allow_skip,
        }

    def _load_latest_effective_screening_input_reference(self, task_name: str) -> dict | None:
        """读取最近一次有效的筛选输入指纹基线。"""
        from database.models import TaskHistory

        with self.db_manager.get_session(readonly=True) as session:
            rows = session.query(TaskHistory).filter(
                TaskHistory.task_name == task_name,
                TaskHistory.status.in_(['success', 'skipped']),
            ).order_by(TaskHistory.started_at.desc()).limit(20).all()

        for row in rows:
            payload = self._parse_task_history_extra_data(row.extra_data)
            effective_hash = str(
                payload.get('screening_effective_input_hash')
                or payload.get('screening_input_hash')
                or ''
            ).strip()
            if not effective_hash:
                continue
            return {
                'history_id': row.id,
                'status': row.status,
                'started_at': row.started_at.isoformat(sep=' ', timespec='seconds') if row.started_at else None,
                'completed_at': row.completed_at.isoformat(sep=' ', timespec='seconds') if row.completed_at else None,
                'input_hash': effective_hash,
            }

        return None

    def _evaluate_screening_workflow_input_gate(self, *, task_name: str, fund_type: str) -> dict:
        """评估每日筛选工作流是否需要因输入未变化而跳过。"""
        fingerprint = self._build_screening_workflow_input_fingerprint(fund_type)
        previous_reference = self._load_latest_effective_screening_input_reference(task_name)
        current_hash = str(fingerprint.get('hash') or '').strip()
        previous_hash = str((previous_reference or {}).get('input_hash') or '').strip()
        allow_skip = bool(fingerprint.get('allow_skip', True))
        should_skip = bool(
            allow_skip and current_hash and previous_hash and current_hash == previous_hash
        )

        return {
            'skip': should_skip,
            'allow_skip': allow_skip,
            'skip_blocked_reason': None if allow_skip else 'strategy_fingerprint_unavailable',
            'fingerprint_snapshot': fingerprint.get('snapshot'),
            'fingerprint_hash': current_hash,
            'previous_reference': previous_reference,
        }

    def _parse_weekly_schedule(self, key: str, default: list) -> tuple:
        """解析每周任务调度配置，返回 (hour, minute, day_of_week)。"""
        schedule_time = self.schedules.get(key, default)
        if isinstance(schedule_time, str):
            schedule_time = [part.strip() for part in schedule_time.split(',') if part.strip()]
        elif isinstance(schedule_time, tuple):
            schedule_time = list(schedule_time)
        if not isinstance(schedule_time, list) or len(schedule_time) < 3:
            schedule_time = list(default)
        schedule_time = [_coerce_schedule_part(part) for part in schedule_time]

        hour, minute = self._parse_schedule_time(key, default[:2])
        day_of_week = schedule_time[2]
        normalized_day_of_week = _normalize_weekly_day_of_week_to_cron(day_of_week)
        if normalized_day_of_week:
            return hour, minute, normalized_day_of_week

        logger.warning(f"每周调度配置非法 key={key} day_of_week={day_of_week}，回退到默认值 {default}")
        return default[0], default[1], _normalize_weekly_day_of_week_to_cron(default[2]) or 'sun'

    def _parse_monthly_schedule(self, key: str, default: list) -> tuple:
        """解析每月任务调度配置，返回 (hour, minute, day)。"""
        schedule_time = self.schedules.get(key, default)
        if isinstance(schedule_time, str):
            schedule_time = [part.strip() for part in schedule_time.split(',') if part.strip()]
        elif isinstance(schedule_time, tuple):
            schedule_time = list(schedule_time)
        if not isinstance(schedule_time, list) or len(schedule_time) < 3:
            schedule_time = list(default)
        schedule_time = [_coerce_schedule_part(part) for part in schedule_time]

        hour, minute = self._parse_schedule_time(key, default[:2])
        day = schedule_time[2]
        if isinstance(day, int) and 1 <= day <= 31:
            return hour, minute, day

        logger.warning(f"每月调度配置非法 key={key} day={day}，回退到默认值 {default}")
        return default[0], default[1], default[2]

    def _get_worker_count(self, *keys, default: int) -> int:
        """读取 scheduler.workers，优先新 key，兼容历史 key。"""
        workers_config = self.config.get('scheduler', {}).get('workers', {})
        value = _first_positive_number(workers_config, keys, default)
        return int(value)

    def _get_timeout_value(self, *keys, default: int) -> int:
        """读取 scheduler.timeouts，优先新 key，兼容历史 key。"""
        value = _first_positive_number(self.timeouts, keys, default)
        return int(value)

    def _add_scheduler_job(self, task_func, trigger, *, id: str, name: str, misfire_grace_time: int):
        """统一封装 APScheduler 任务注册参数，避免注册块长期复制粘贴。"""
        self.scheduler.add_job(
            task_func,
            trigger,
            id=id,
            name=name,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=misfire_grace_time,
        )

    def _register_daily_job(
        self,
        task_func,
        *,
        id: str,
        name: str,
        schedule_key: str,
        default_time: tuple,
        misfire_grace_time: int = 3600,
    ):
        """注册按天执行的调度任务。"""
        hour, minute, day_of_week = self._parse_daily_schedule(schedule_key, list(default_time))
        trigger_kwargs = {
            'hour': hour,
            'minute': minute,
            'timezone': self.timezone,
        }
        if day_of_week:
            trigger_kwargs['day_of_week'] = day_of_week
        self._add_scheduler_job(
            task_func,
            CronTrigger(**trigger_kwargs),
            id=id,
            name=name,
            misfire_grace_time=misfire_grace_time,
        )
        schedule_label = _format_day_of_week_window(day_of_week) if day_of_week else '每天'
        logger.info(f"✅ 添加任务: {name} ({schedule_label} {hour:02d}:{minute:02d})")

    def _register_weekly_job(
        self,
        task_func,
        *,
        id: str,
        name: str,
        schedule_key: str,
        default_time: tuple,
        misfire_grace_time: int = 7200,
    ):
        """注册按周执行的调度任务。"""
        hour, minute, day_of_week = self._parse_weekly_schedule(schedule_key, list(default_time))
        self._add_scheduler_job(
            task_func,
            CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute, timezone=self.timezone),
            id=id,
            name=name,
            misfire_grace_time=misfire_grace_time,
        )
        logger.info(
            f"✅ 添加任务: {name} (每周{_format_weekly_day_label(day_of_week)} {hour:02d}:{minute:02d})"
        )

    def _register_monthly_job(
        self,
        task_func,
        *,
        id: str,
        name: str,
        schedule_key: str,
        default_time: tuple,
        misfire_grace_time: int = 86400,
    ):
        """注册按月执行的调度任务。"""
        hour, minute, day = self._parse_monthly_schedule(schedule_key, list(default_time))
        self._add_scheduler_job(
            task_func,
            CronTrigger(day=day, hour=hour, minute=minute, timezone=self.timezone),
            id=id,
            name=name,
            misfire_grace_time=misfire_grace_time,
        )
        logger.info(f"✅ 添加任务: {name} (每月{day}号 {hour:02d}:{minute:02d})")

    def add_jobs(self):
        """添加所有调度任务（增强版：coalesce防堆积、任务互斥）"""
        logger.info("正在添加调度任务...")

        # 每日基金列表检查（每天早上 5:30）
        if self.task_switches.get('daily_fund_list_check', True):
            self._register_daily_job(
                self._daily_fund_list_check,
                id='daily_fund_list_check',
                name='每日基金列表检查',
                schedule_key='daily_fund_list_check',
                default_time=(5, 30),
            )

        # 每日增量净值更新（每天早上 6:00）- 核心任务，其他任务依赖它
        if self.task_switches.get('daily_nav_update', True):
            self._register_daily_job(
                self._daily_nav_update,
                id='daily_nav_update',
                name='每日增量净值更新',
                schedule_key='daily_nav_update',
                default_time=(6, 0),
            )

        # 每日AutoStock验证（每天早上 7:00，在净值更新后）
        if self.task_switches.get('daily_autostock_verify', True):
            self._register_daily_job(
                self._daily_autostock_verify,
                id='daily_autostock_verify',
                name='每日AutoStock数据验证',
                schedule_key='daily_autostock_verify',
                default_time=(7, 0),
            )

        # 每日补充缺失净值（每天早上 7:30，在AutoStock验证后）
        if self.task_switches.get('daily_supplement_missing_nav', True):
            self._register_daily_job(
                self._daily_supplement_missing_nav,
                id='daily_supplement_missing_nav',
                name='每日补充缺失净值',
                schedule_key='daily_supplement_missing_nav',
                default_time=(7, 30),
            )

        # 每日风险指标更新（默认 9:30）- 依赖 daily_nav_update
        # 注意：净值更新实测约185分钟（6:00~9:05），此任务需在其后调度
        if self.task_switches.get('daily_risk_update', True):
            self._register_daily_job(
                self._daily_risk_update,
                id='daily_risk_update',
                name='每日风险指标更新',
                schedule_key='daily_risk_update',
                default_time=(9, 30),
            )

        # 每日债券基金筛选工作流（默认 10:00）- 依赖 daily_nav_update
        # 注意：须在净值更新完成后运行，由 _skip_if_dependency_unsatisfied 守护
        if self.task_switches.get('daily_screening_workflow', True):
            self._register_daily_job(
                self._daily_screening_workflow,
                id='daily_screening_workflow',
                name='每日债券基金筛选工作流',
                schedule_key='daily_screening_workflow',
                default_time=(10, 0),
            )

        # 每日策略监控（默认 11:30）- 依赖 daily_nav_update
        # Step 7-11: 在筛选工作流之后运行，确保当日产物已落库
        if self.task_switches.get('daily_policy_monitor', True):
            self._register_daily_job(
                self._daily_policy_monitor,
                id='daily_policy_monitor',
                name='每日策略监控',
                schedule_key='daily_policy_monitor',
                default_time=(11, 30),
            )

        # 每周基金详情更新（每周日凌晨 2:00）
        if self.task_switches.get('weekly_detail_update', True):
            self._register_weekly_job(
                self._weekly_detail_update,
                id='weekly_detail_update',
                name='每周基金详情更新',
                schedule_key='weekly_detail_update',
                default_time=(2, 0, 0),
            )

        # 每周持有期信息补充（每周日凌晨 2:30，在详情更新后）
        if self.task_switches.get('weekly_holding_period_update', True):
            self._register_weekly_job(
                self._weekly_holding_period_update,
                id='weekly_holding_period_update',
                name='每周持有期信息补充',
                schedule_key='weekly_holding_period_update',
                default_time=(2, 30, 0),
            )

        # 每周基金经理更新（每周日凌晨 3:00）
        if self.task_switches.get('weekly_manager_update', True):
            self._register_weekly_job(
                self._weekly_manager_update,
                id='weekly_manager_update',
                name='每周基金经理更新',
                schedule_key='weekly_manager_update',
                default_time=(3, 0, 0),
            )

        # 每周基金公司更新（每周日凌晨 4:00，暂时禁用）
        if self.task_switches.get('weekly_company_update', False):
            self._register_weekly_job(
                self._weekly_company_update,
                id='weekly_company_update',
                name='每周基金公司更新',
                schedule_key='weekly_company_update',
                default_time=(4, 0, 0),
            )

        # 每周完整验证（每周日早上 6:00）
        if self.task_switches.get('weekly_full_verify', True):
            self._register_weekly_job(
                self._weekly_full_verify,
                id='weekly_full_verify',
                name='每周完整数据验证',
                schedule_key='weekly_full_verify',
                default_time=(6, 0, 0),
            )

        # 每周高速基金详情采集（每周日凌晨 5:00，与 weekly_detail_update(02:00) 错开）
        if self.task_switches.get('weekly_high_speed_detail', True):
            self._register_weekly_job(
                self._weekly_high_speed_detail_update,
                id='weekly_high_speed_detail',
                name='每周高速基金详情采集',
                schedule_key='weekly_high_speed_detail',
                default_time=(5, 0, 0),
            )

        # 每月数据备份（每月1号凌晨 1:00）
        if self.task_switches.get('monthly_backup', True):
            self._register_monthly_job(
                self._monthly_backup,
                id='monthly_backup',
                name='每月数据备份',
                schedule_key='monthly_backup',
                default_time=(1, 0, 1),
            )

        # 每月数据清理（每月1号凌晨 2:00）
        if self.task_switches.get('monthly_cleanup', True):
            self._register_monthly_job(
                self._monthly_cleanup,
                id='monthly_cleanup',
                name='每月数据清理',
                schedule_key='monthly_cleanup',
                default_time=(2, 0, 1),
            )

        # 每月补充成立日期（每月1号凌晨 3:00）
        if self.task_switches.get('monthly_supplement_estab', True):
            self._register_monthly_job(
                self._monthly_supplement_estab_date,
                id='monthly_supplement_estab',
                name='每月补充成立日期',
                schedule_key='monthly_supplement_estab',
                default_time=(3, 0, 1),
            )

        # 任务超时监控（每小时执行一次）
        if self.task_switches.get('hourly_timeout_monitor', True):
            self._add_scheduler_job(
                self._hourly_timeout_monitor,
                CronTrigger(minute='*/5', timezone=self.timezone),
                id='hourly_timeout_monitor',
                name='任务超时监控',
                misfire_grace_time=3600,
            )
            logger.info("✅ 添加任务: 任务超时监控 (每5分钟)")

        logger.info(f"调度任务添加完成，共 {len(self.scheduler.get_jobs())} 个任务")

    # ============ 任务执行方法 ============

    def _daily_nav_update(self):
        """每日增量净值更新"""
        logger.info("="*60)
        logger.info("开始执行: 每日增量净值更新")
        logger.info("="*60)

        def task_logic():
            collector = NavCollector(
                config=self.config,
                db_manager=self.db_manager,
                api_client=self.api_client
            )

            # 增量更新（仅更新活跃基金的最新净值）
            incremental_config = self.config.get('collection', {}).get('incremental', {})
            workers = incremental_config.get('workers', 30)

            count = collector.collect_all_incremental(workers=workers)

            logger.info(f"每日增量净值更新完成: 更新了 {count} 只基金的净值")

            return {
                'count': count,
                'updated': count,
                'metadata': {'workers': workers}
            }

        try:
            return self._execute_with_history('每日增量净值更新', task_logic, task_key='daily_nav_update')
        except Exception as e:
            logger.error(f"每日增量净值更新失败: {e}", exc_info=True)
            raise


    def _daily_risk_update(self):
        """每日风险指标更新"""
        # Step 5E-2: 依赖检查 — daily_nav_update 未完成则跳过
        if self._skip_if_dependency_unsatisfied('daily_risk_update', '每日风险指标更新'):
            return

        logger.info("="*60)
        logger.info("开始执行: 每日风险指标更新")
        logger.info("="*60)

        def task_logic():
            analyzer = RiskAnalyzer(
                config=self.config,
                db_manager=self.db_manager
            )

            workers = self._get_worker_count('daily_risk_update', 'daily_risk', default=10)

            # 全量更新风险指标（每日都需要重新计算，因为净值数据每天更新）
            # force_update=True 会先清空旧数据，然后重新计算所有基金的风险指标
            count = analyzer.calculate_all_funds(workers=workers, force_update=True)

            logger.info(f"每日风险指标更新完成: 更新了 {count} 只基金的指标")

            return {
                'count': count,
                'updated': count,
                'metadata': {'workers': workers, 'mode': 'full_update'}
            }

        try:
            return self._execute_with_history('每日风险指标更新', task_logic, task_key='daily_risk_update')
        except Exception as e:
            logger.error(f"每日风险指标更新失败: {e}", exc_info=True)
            raise

    def _daily_screening_workflow(self):
        """每日债券基金筛选工作流"""
        # Step 5E-2: 依赖检查 — daily_nav_update 未完成则跳过
        if self._skip_if_dependency_unsatisfied('daily_screening_workflow', '每日债券基金筛选工作流'):
            return

        logger.info("="*60)
        logger.info("开始执行: 每日债券基金筛选工作流")
        logger.info("="*60)

        def task_logic():
            input_gate = self._evaluate_screening_workflow_input_gate(
                task_name='每日债券基金筛选工作流',
                fund_type='bond',
            )
            if input_gate.get('skip'):
                previous_reference = input_gate.get('previous_reference') or {}
                logger.info(
                    "债券基金筛选输入未变化，跳过重复正式筛选: current_hash=%s previous_history_id=%s",
                    input_gate.get('fingerprint_hash'),
                    previous_reference.get('history_id'),
                )
                return {
                    'count': 0,
                    'processed': 0,
                    'metadata': {
                        'status': 'skipped',
                        'skipped': True,
                        'reason': 'no_new_inputs',
                        'skip_reason': 'no_new_inputs',
                        'task_status_message': '输入指纹未变化，跳过重复正式筛选',
                        'fund_type': 'bond',
                        'screening_input_snapshot': input_gate.get('fingerprint_snapshot'),
                        'screening_input_hash': input_gate.get('fingerprint_hash'),
                        'screening_effective_input_hash': input_gate.get('fingerprint_hash'),
                        'screening_input_reference_history_id': previous_reference.get('history_id'),
                        'screening_input_reference_started_at': previous_reference.get('started_at'),
                    },
                }

            # 脚本路径
            script_path = Path(__file__).parent.parent / 'bond_fund_screening' / 'tasks' / 'daily_screening_master.py'
            shadow_gate_meta = self._build_screening_shadow_gate_metadata(fund_type='bond')
            shadow_gate_summary = _build_screening_shadow_gate_summary_fields(shadow_gate_meta)

            if not script_path.exists():
                raise FileNotFoundError(f"脚本不存在: {script_path}")

            # 执行脚本
            logger.info(f"执行脚本: {script_path}")
            child_env = apply_screening_runtime_defaults(
                os.environ.copy(),
                trigger_source='scheduler_bond_master',
                task_id='daily_screening_workflow',
                task_name='每日债券基金筛选工作流',
                override=True,
            )
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=self._get_timeout_value('daily_screening_workflow', default=3600),
                cwd=str(script_path.parent.parent.parent),  # 设置工作目录为项目根目录
                env=child_env,
            )

            if result.returncode == 0:
                logger.info("✅ 筛选工作流执行成功")
                # 记录最后500字符的输出
                if result.stdout:
                    logger.info(f"脚本输出（最后500字符）:\n{result.stdout[-500:]}")

                # 返回成功统计
                return {
                    'count': 1,
                    'processed': 1,
                    'metadata': {
                        'returncode': 0,
                        'status': 'success',
                        'fund_type': 'bond',
                        'screening_input_snapshot': input_gate.get('fingerprint_snapshot'),
                        'screening_input_hash': input_gate.get('fingerprint_hash'),
                        'screening_effective_input_hash': input_gate.get('fingerprint_hash'),
                        'screening_input_reference_history_id': (
                            (input_gate.get('previous_reference') or {}).get('history_id')
                        ),
                        'normalized_nav_shadow_gate': shadow_gate_meta,
                        **shadow_gate_summary,
                    }
                }
            else:
                logger.error(f"❌ 筛选工作流执行失败 (返回码: {result.returncode})")
                if result.stderr:
                    logger.error(f"错误输出:\n{result.stderr}")
                error = subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
                error.failure_metadata = {
                    **_build_subprocess_failure_metadata(result, script_path),
                    'normalized_nav_shadow_gate': shadow_gate_meta,
                    **shadow_gate_summary,
                }
                raise error

        try:
            return self._execute_with_history('每日债券基金筛选工作流', task_logic, task_key='daily_screening_workflow')
        except subprocess.TimeoutExpired:
            logger.error("❌ 筛选工作流超时")
            raise
        except Exception as e:
            logger.error(f"❌ 筛选工作流执行异常: {e}", exc_info=True)
            raise

    def _daily_policy_monitor(self):
        """每日策略监控（Step 7-11 + Step 7-13 recommendation）"""
        # 依赖检查 — daily_nav_update 未完成则跳过
        if self._skip_if_dependency_unsatisfied('daily_policy_monitor', '每日策略监控'):
            return

        logger.info("="*60)
        logger.info("开始执行: 每日策略监控")
        logger.info("="*60)

        def task_logic():
            try:
                shadow_gate_meta = self._build_screening_shadow_gate_metadata()
                shadow_gate_summary = _build_screening_shadow_gate_summary_fields(shadow_gate_meta)
                summary = None

                from scripts.monitor_policy_rollout import run_policy_monitor
                summary = run_policy_monitor(
                    days=14,
                    fund_types=None,
                )
                logger.info(f"策略监控完成: verdict={summary.get('overall_verdict')}")

                # Step 7-13: 生成策略建议并追加到 summary
                try:
                    from utils.policy_recommendation import (
                        recommend_policy_action,
                        load_latest_publishable_patch,
                        compute_patch_digest,
                    )
                    from bond_fund_screening.config import OUTPUT_DIR

                    patch = load_latest_publishable_patch(OUTPUT_DIR)
                    rec = recommend_policy_action(summary, patch)

                    # 只存轻量摘要（不含完整 patch_json/snippet）
                    summary['recommendation'] = {
                        'action': rec.get('action'),
                        'reason': rec.get('reason'),
                        'patch_source': rec.get('patch_source'),
                        'patch_digest': compute_patch_digest(rec.get('patch_json')),
                    }
                    logger.info(f"策略建议: action={rec.get('action')}, reason={rec.get('reason')}")
                except Exception as e:
                    logger.warning(f"策略建议生成失败（不影响主流程）: {e}")
                    summary['recommendation'] = {
                        'action': 'collect_more_data',
                        'reason': f'recommendation_error: {e}',
                        'patch_source': None,
                        'patch_digest': None,
                    }

                # Step 7-16: 受控自动回滚尝试（默认关闭，容错隔离）
                try:
                    from bond_fund_screening.config import AUTO_ROLLBACK_POLICY
                    auto_cfg = AUTO_ROLLBACK_POLICY
                    if auto_cfg.get('enabled', False):
                        logger.info("自动回滚策略已启用，开始决策判定...")
                        auto_rollback_result = self._attempt_auto_rollback(summary, auto_cfg)
                        summary['auto_rollback'] = auto_rollback_result
                        logger.info(f"自动回滚决策: attempted={auto_rollback_result.get('attempted')}, "
                                    f"reason={auto_rollback_result.get('decision_reason')}")
                    else:
                        summary['auto_rollback'] = {
                            'attempted': False, 'decided': False,
                            'decision_reason': 'disabled',
                        }
                except Exception as e:
                    logger.warning(f"自动回滚尝试异常（不影响主流程）: {e}")
                    summary['auto_rollback'] = {
                        'attempted': False, 'decided': False,
                        'decision_reason': f'error: {e}',
                    }

                return {
                    'monitor_summary': summary,
                    'metadata': {
                        'monitor_summary': _sanitize_policy_monitor_summary(summary),
                        'normalized_nav_shadow_gate': shadow_gate_meta,
                        **shadow_gate_summary,
                    },
                }
            except Exception as exc:
                failure_metadata = {
                    'normalized_nav_shadow_gate': shadow_gate_meta,
                    **shadow_gate_summary,
                }
                sanitized_summary = _sanitize_policy_monitor_summary(summary)
                if sanitized_summary:
                    failure_metadata['monitor_summary'] = sanitized_summary
                existing_failure_metadata = getattr(exc, 'failure_metadata', None)
                if isinstance(existing_failure_metadata, dict):
                    existing_failure_metadata.update(failure_metadata)
                    exc.failure_metadata = existing_failure_metadata
                else:
                    exc.failure_metadata = failure_metadata
                raise

        try:
            return self._execute_with_history('每日策略监控', task_logic, task_key='daily_policy_monitor')
        except Exception as e:
            logger.error(f"❌ 策略监控执行异常: {e}", exc_info=True)
            raise

    def _attempt_auto_rollback(self, monitor_summary: dict, auto_cfg: dict) -> dict:
        """
        Step 7-16: 尝试自动回滚（容错隔离，异常不上抛）。

        Args:
            monitor_summary: 当前 monitor 输出摘要
            auto_cfg: AUTO_ROLLBACK_POLICY 配置

        Returns:
            auto_rollback 结果 dict
        """
        import importlib.util as _ilu
        from datetime import datetime

        # 动态加载 auto_rollback 模块（避免命名空间冲突）
        _ar_path = Path(__file__).parent.parent / 'utils' / 'auto_rollback.py'
        _ar_spec = _ilu.spec_from_file_location('_auto_rollback', str(_ar_path))
        _ar_mod = _ilu.module_from_spec(_ar_spec)
        _ar_spec.loader.exec_module(_ar_mod)

        now = datetime.now()

        # 构建审计历史（从 task_history 查询最近的回滚记录）
        audit_history = self._get_rollback_audit_history()

        # 决策判定
        should, reason, details = _ar_mod.should_auto_rollback(
            monitor_summary, auto_cfg, audit_history, now,
        )

        result = {
            'attempted': should,
            'decided': should,
            'decision_reason': reason,
            'plan_digest': None,
            'dry_run': {'ok': False, 'reason': 'not_executed'},
            'apply': {'ok': False, 'reason': 'not_executed'},
            'cooldown_blocked': details.get('cooldown_blocked', False),
            'daily_limit_blocked': details.get('daily_limit_blocked', False),
        }

        if not should:
            result['attempted'] = False
            return result

        # 构建执行计划
        plan = _ar_mod.build_auto_rollback_plan(monitor_summary, auto_cfg, reason)
        result['plan_digest'] = plan.get('patch_digest')

        # 执行自动回滚
        exec_result = _ar_mod.execute_auto_rollback(
            plan, db_manager=self.db_manager, auto_cfg=auto_cfg,
        )
        result['dry_run'] = exec_result.get('dry_run', result['dry_run'])
        result['apply'] = exec_result.get('apply', result['apply'])

        if exec_result.get('apply', {}).get('ok'):
            logger.info("✅ 自动回滚执行成功")
        else:
            logger.warning(f"自动回滚未成功: {exec_result.get('apply', {}).get('reason')}")

        return result

    def _get_rollback_audit_history(self) -> list:
        """从 task_history 查询最近的回滚审计记录。"""
        try:
            from database.task_history import TaskHistoryManager
            # 使用独立 session 包裹 TaskHistoryManager（它期望 Session 而非 db_manager）
            with self.db_manager.get_session() as session:
                thm = TaskHistoryManager(session)
                records = thm.get_recent_history('策略补丁执行', limit=20)
                # 在 session 内部处理 ORM 对象，避免 detached 状态问题
                history = []
                for r in records:
                    extra = r.extra_data if hasattr(r, 'extra_data') else None
                    if extra:
                        import json
                        try:
                            data = json.loads(extra) if isinstance(extra, str) else extra
                        except (json.JSONDecodeError, TypeError):
                            data = {}
                    else:
                        data = {}
                    history.append({
                        'action': data.get('action', ''),
                        'status': r.status if hasattr(r, 'status') else '',
                        'checked_on': r.started_at.isoformat() if hasattr(r, 'started_at') and r.started_at else None,
                    })
            return history
        except Exception as e:
            logger.warning(f"查询回滚审计历史失败: {e}")
            return []

    def _weekly_detail_update(self):
        """每周基金详情更新"""
        logger.info("="*60)
        logger.info("开始执行: 每周基金详情更新")
        logger.info("="*60)

        def task_logic():
            collector = FundDetailCollector(
                config=self.config,
                db_manager=self.db_manager,
                api_client=self.api_client
            )

            workers = self._get_worker_count('weekly_detail_update', 'weekly_details', default=10)

            # 强制更新所有基金详情（包含成立日期和规模）
            count = collector.collect_all_details(workers=workers, force_update=True)
            summary = validate_detail_collection_summary(collector.detail_collection_summary, returned_count=count)
            evidence = collector.get_api_source_evidence()
            errors = detail_collection_source_errors(summary, evidence)
            if errors:
                raise ValueError(";".join(errors))
            logger.info("每周基金详情处理结束: 写入=%s 不可用=%s 异常=%s", count,
                        summary['unavailable_count'], summary['failed_count'])

            return {
                'count': count,
                'processed': summary['target_count'],
                'updated': count,
                'failed': summary['failed_count'],
                'metadata': {
                    'workers': workers,
                    # This legacy runner has no screening-relevance gate for
                    # unavailable details. Preserve the observation as degraded.
                    'status': 'degraded' if summary['status'] == 'completed_with_unavailable' else summary['status'],
                    'reason': summary['reason'],
                    'task_status_message': summary['reason'],
                    'detail_collection_summary': summary,
                    'api_source_evidence': evidence,
                },
            }

        try:
            return self._execute_with_history('每周基金详情更新', task_logic, task_key='weekly_detail_update')
        except Exception as e:
            logger.error(f"每周基金详情更新失败: {e}", exc_info=True)
            raise

    def _weekly_holding_period_update(self):
        """每周持有期信息补充"""
        logger.info("="*60)
        logger.info("开始执行: 每周持有期信息补充")
        logger.info("="*60)

        def task_logic():
            collector = HoldingPeriodCollector(
                config=self.config,
                db_manager=self.db_manager,
                api_client=self.api_client
            )

            workers = self._get_worker_count('weekly_holding_period_update', 'weekly_holding_period', default=20)

            # 补充所有基金的持有期信息
            count = collector.supplement_all(workers=workers, force_update=True)

            logger.info(f"每周持有期信息补充完成: 更新了 {count} 只基金")

            return {
                'count': count,
                'updated': count,
                'metadata': {'workers': workers}
            }

        try:
            return self._execute_with_history('每周持有期信息补充', task_logic, task_key='weekly_holding_period_update')
        except subprocess.TimeoutExpired:
            timeout = self._get_timeout_value('weekly_holding_period_update', 'weekly_holding_period', default=3600)
            logger.error(f"每周持有期信息补充超时（{timeout}秒）")
            raise
        except Exception as e:
            logger.error(f"每周持有期信息补充失败: {e}", exc_info=True)
            raise

    def _weekly_manager_update(self):
        """每周基金经理更新"""
        self._log_routine_info("=" * 60)
        self._log_routine_info("开始执行: 每周基金经理更新")
        self._log_routine_info("=" * 60)

        def task_logic():
            collector = ManagerCollector(
                config=self.config,
                db_manager=self.db_manager,
                api_client=self.api_client
            )

            workers = self._get_worker_count('weekly_manager_update', 'weekly_managers', default=5)

            # 强制更新所有经理信息
            count = collector.collect_all_managers(
                workers=workers,
                force_update=True,
                report_task_history=False
            )
            stage_summary = validate_manager_collection_summary(
                collector.last_run_summary, returned_manager_count=count,
            )
            self._log_routine_info("每周基金经理更新结束: status=%s，更新了 %s 位经理",
                                   stage_summary['status'], count)

            return {
                'count': count,
                'processed': stage_summary['target_count'],
                'updated': stage_summary['success_count'],
                'failed': stage_summary['failed_count'],
                'metadata': {
                    'status': stage_summary['status'],
                    'reason': stage_summary['reason'],
                    'task_status_message': stage_summary['reason'],
                    'workers': workers,
                    'stage_summary': stage_summary,
                }
            }

        try:
            return self._execute_with_history('每周基金经理更新', task_logic, task_key='weekly_manager_update')
        except Exception as e:
            logger.error(f"每周基金经理更新失败: {e}", exc_info=True)
            raise

    def _weekly_company_update(self):
        """每周基金公司更新"""
        logger.info("="*60)
        logger.info("开始执行: 每周基金公司更新")
        logger.info("="*60)

        def task_logic():
            collector = CompanyCollector(
                config=self.config,
                db_manager=self.db_manager,
                api_client=self.api_client
            )

            workers = self._get_worker_count('weekly_company_update', 'weekly_companies', default=1)

            count = collector.collect_all_companies(workers=workers, force_update=False)
            summary = collector.last_run_summary

            logger.info("每周基金公司刷新结束: 成功=%s 失败=%s 冷却=%s", count,
                        summary['failed_count'], summary['cooldown_count'])

            return {
                'count': count,
                'processed': summary['target_count'],
                'inserted': summary['inserted_count'],
                'updated': summary['updated_count'],
                'failed': summary['failed_count'],
                'metadata': {
                    'workers': workers,
                    'status': summary['status'],
                    'task_status_message': '公司资料刷新：' + str(summary['skip_reason'] or summary['status']),
                    'stage_summary': summary,
                    'api_source_evidence': collector.get_api_source_evidence(),
                },
            }

        try:
            return self._execute_with_history('每周基金公司更新', task_logic, task_key='weekly_company_update')
        except Exception as e:
            logger.error(f"每周基金公司更新失败: {e}", exc_info=True)
            raise

    def _weekly_high_speed_detail_update(self):
        """每周高速基金详情采集"""
        logger.info("="*60)
        logger.info("开始执行: 每周高速基金详情采集")
        logger.info("="*60)

        def task_logic():
            # 调用高速采集脚本
            script_path = Path(__file__).parent.parent / 'scripts' / 'run_detail_collector_fast.py'

            timeout = self._get_timeout_value('weekly_high_speed_detail', default=600)

            result = subprocess.run(
                [sys.executable, str(script_path), '--execute'],
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                logger.info("高速详情采集成功")
                logger.info(f"脚本输出: {result.stdout[-500:]}")  # 最后500字符
                return {
                    'count': 1,
                    'processed': 1,
                    'metadata': {'returncode': 0, 'status': 'success'}
                }
            else:
                logger.error(f"高速详情采集失败: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)

        try:
            return self._execute_with_history('每周高速基金详情采集', task_logic, task_key='weekly_high_speed_detail')
        except subprocess.TimeoutExpired:
            logger.error("高速详情采集超时（10分钟）")
            raise
        except Exception as e:
            logger.error(f"高速详情采集失败: {e}", exc_info=True)
            raise

    def _daily_supplement_missing_nav(self):
        """每日补充缺失净值"""
        logger.info("="*60)
        logger.info("开始执行: 每日补充缺失净值")
        logger.info("="*60)

        def task_logic():
            
            script_path = Path(__file__).parent.parent / 'scripts' / 'supplement_missing_nav.py'
            result = subprocess.run(
                [sys.executable, str(script_path), '--execute', '--workers', '8'],
                capture_output=True,
                text=True,
                timeout=self._get_timeout_value('daily_supplement_missing_nav', default=900)
            )

            if result.returncode == 0:
                logger.info("缺失净值补充成功")
                logger.info(f"脚本输出: {result.stdout[-500:]}")

                # 返回成功统计
                return {
                    'count': 1,
                    'processed': 1,
                    'metadata': {'returncode': 0, 'status': 'success'}
                }

            if result.returncode == 2:
                logger.warning("缺失净值补充无可用数据，按业务跳过处理")
                logger.info(f"脚本输出: {result.stdout[-500:]}")
                return {
                    'count': 0,
                    'processed': 0,
                    'metadata': {
                        'returncode': 2,
                        'status': 'skipped',
                        'skipped': True,
                        'reason': 'all_no_data'
                    }
                }

            logger.error(f"缺失净值补充失败: {result.stderr}")
            raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)

        try:
            return self._execute_with_history('每日补充缺失净值', task_logic, task_key='daily_supplement_missing_nav')
        except subprocess.TimeoutExpired:
            logger.error("缺失净值补充超时")
            raise
        except Exception as e:
            logger.error(f"缺失净值补充失败: {e}", exc_info=True)
            raise

    def _monthly_supplement_estab_date(self):
        """每月补充成立日期"""
        logger.info("="*60)
        logger.info("开始执行: 每月补充成立日期")
        logger.info("="*60)

        def task_logic():
            script_path = Path(__file__).parent.parent / 'scripts' / 'supplement_estab_date.py'

            timeout = self._get_timeout_value('monthly_supplement_estab', default=300)

            result = subprocess.run(
                [sys.executable, str(script_path), '--execute'],
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if result.returncode == 0:
                logger.info("成立日期补充成功")
                logger.info(f"脚本输出: {result.stdout[-500:]}")
                return {
                    'count': 1,
                    'processed': 1,
                    'metadata': {'returncode': 0, 'status': 'success'}
                }
            else:
                logger.error(f"成立日期补充失败: {result.stderr}")
                raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)

        try:
            return self._execute_with_history('每月补充成立日期', task_logic, task_key='monthly_supplement_estab')
        except subprocess.TimeoutExpired:
            timeout = self._get_timeout_value('monthly_supplement_estab', default=300)
            logger.error(f"成立日期补充超时（{timeout}秒）")
            raise
        except Exception as e:
            logger.error(f"成立日期补充失败: {e}", exc_info=True)
            raise

    def _hourly_timeout_monitor(self):
        """任务超时监控
        每小时检查并标记超时的running任务为失败状态
        """
        logger.info("="*60)
        logger.info("开始执行: 任务超时监控")
        logger.info("="*60)

        try:
            from schedulers.task_timeout_monitor import monitor_and_cleanup_timeout_tasks

            # 获取数据库路径
            db_path = self.config.get('database', {}).get('path', 'database/bond_funds.db')

            # 执行监控和清理
            timeout_count = monitor_and_cleanup_timeout_tasks(db_path)

            if timeout_count > 0:
                logger.warning(f"发现并处理了 {timeout_count} 个超时任务")
            else:
                logger.info("所有任务运行正常，无超时任务")

        except Exception as e:
            logger.error(f"超时监控执行失败: {e}", exc_info=True)
            # 监控失败不应影响主系统运行，所以不抛出异常

    def _monthly_backup(self):
        """每月数据备份"""
        logger.info("="*60)
        logger.info("开始执行: 每月数据备份")
        logger.info("="*60)

        def task_logic():
            from utils.sqlite_backup_snapshot import create_verified_sqlite_backup

            # 备份数据库
            db_path = Path(
                getattr(self.db_manager, 'db_path', None)
                or self.config.get('database', {}).get('path', 'database/bond_funds.db')
            )
            if not db_path.exists():
                logger.warning("数据库文件不存在，跳过备份")
                return {'count': 0, 'metadata': {'skipped': True, 'reason': 'db_not_found'}}

            backup_dir = db_path.parent / 'backups'
            backup_dir.mkdir(exist_ok=True)

            backup_name = f"bond_funds_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
            backup_path = backup_dir / backup_name

            # SQLite backup includes committed WAL pages. Verification precedes retention.
            backup = create_verified_sqlite_backup(
                db_path, backup_path, purpose='scheduled_monthly_backup',
                backup_label=backup_path.stem, compress=True,
            )
            backup_path = backup['artifact_path']

            logger.info(f"数据库备份完成: {backup_path}")

            backup_cleanup_policy = get_database_backup_cleanup_policy(self.config)
            scheduled_keep_count = int(
                backup_cleanup_policy.get('scheduled_keep_count', DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT)
            )

            # 清理旧备份，保留配置中指定的最近月备份数量。
            self._cleanup_old_backups(backup_dir, keep_count=scheduled_keep_count)

            return {
                'count': 1,
                'processed': 1,
                'metadata': {
                    'backup_file': str(backup_path),
                    'backup_sha256': backup['artifact_sha256'],
                    'verification_manifest_path': backup['verification_manifest_path'],
                    'verification_manifest_sha256': backup['verification_manifest_sha256'],
                    'snapshot_sha256': backup['snapshot_sha256'],
                    'recovery_verification': backup['recovery_verification'],
                }
            }

        try:
            return self._execute_with_history('每月数据备份', task_logic, task_key='monthly_backup')
        except Exception as e:
            logger.error(f"每月数据备份失败: {e}", exc_info=True)
            raise

    def _cleanup_old_backups(
        self,
        backup_dir: Path,
        keep_count: int = DEFAULT_SCHEDULED_BACKUP_KEEP_COUNT,
    ):
        """清理旧备份文件"""
        try:
            from utils.database_backup_cleanup_preview import select_scheduled_backup_cleanup_candidates

            backups = select_scheduled_backup_cleanup_candidates(
                backup_dir,
                keep_count=keep_count,
            )
            for backup in backups:
                backup.unlink()
                # Keep adjacent verification manifests as evidence of retired points.
                logger.info(f"删除旧备份: {backup.name}")

        except Exception as e:
            logger.warning(f"清理旧备份失败: {e}")

    def _daily_fund_list_check(self):
        """每日基金列表检查 - 检查所有类型是否有新基金需要采集"""
        logger.info("="*60)
        logger.info("开始执行: 每日基金列表检查（全类型）")
        logger.info("="*60)

        def task_logic():
            from collectors.fund_collector import FundCollector
            from database.models import Fund, FundReturns
            from config.api_config import FundType

            collector = FundCollector(
                config=self.config,
                db_manager=self.db_manager,
                api_client=self.api_client
            )

            # 定义要检查的所有基金类型
            all_fund_types = [
                (FundType.STOCK, '股票型'),
                (FundType.INDEX, '指数型'),
                (FundType.HYBRID, '混合型'),
                (FundType.BOND, '债券型'),
                (FundType.BOND_FIXED, '定开债'),
                (FundType.MONEY, '货币型'),
                (FundType.QDII, 'QDII'),
                (FundType.FOF, 'FOF'),
                (FundType.ETF, 'ETF'),
                (FundType.LOF, 'LOF'),
            ]

            # 获取数据库中已有的所有基金代码
            fixed_income_existing = []
            with self.db_manager.get_session(readonly=True) as session:
                existing_codes = set(row[0] for row in session.query(Fund.fcode).all())
                fixed_income_existing = [
                    {
                        'fcode': fund.fcode,
                        'shortname': fund.shortname,
                        'ftype': fund.ftype,
                        'type_code': fund.type_code,
                        'type_category': fund.type_category,
                        'latest_nav_date': returns.latest_nav_date if returns else None,
                    }
                    for fund, returns in session.query(Fund, FundReturns)
                    .outerjoin(FundReturns, FundReturns.fcode == Fund.fcode)
                    .filter(Fund.type_category == 'fixed_income')
                    .all()
                    if not is_runtime_excluded_fund(fund.fcode)
                ]
            logger.info(f"数据库已有 {len(existing_codes)} 只基金")

            type_payloads = []
            type_failures = []

            # 逐个类型检查新基金
            for fund_type, type_name in all_fund_types:
                try:
                    funds = collector._collect_by_type(fund_type, only_buyable=False)
                    type_payloads.append((type_name, funds))
                except Exception as e:
                    logger.error(f"  {type_name}: 检查失败 - {e}")
                    type_failures.append({
                        'type_name': type_name,
                        'fund_type': str(fund_type),
                        'error': f"{type(e).__name__}: {e}",
                    })

            if type_failures:
                raise RuntimeError(f"基金类型快照采集存在失败类型: {type_failures}")

            snapshot_plan = self._build_daily_fund_snapshot_plan(
                type_payloads=type_payloads,
                existing_codes=existing_codes,
                merge_func=collector._merge_duplicate_fund_records,
            )
            type_stats = snapshot_plan['type_stats']
            total_new = snapshot_plan['total_new']

            for type_name, stats in type_stats.items():
                if stats.get('new', 0) > 0:
                    logger.info(f"  {type_name}: 共{stats['total']}只，发现 {stats['new']} 只新基金")
                else:
                    logger.info(f"  {type_name}: {stats['total']} 只，无新基金")

            snapshot_codes = {item['fcode'] for item in snapshot_plan['snapshot_funds']}
            uncovered_fixed_income = [
                fund for fund in fixed_income_existing
                if fund['fcode'] not in snapshot_codes
            ]
            fallback_records = []
            fallback_stats = {
                'targets': len(uncovered_fixed_income),
                'queried': 0,
                'matched': 0,
                'missing': 0,
                'failed': 0,
            }
            if uncovered_fixed_income:
                logger.info(
                    "固定收益基金中有 %s 只未被 fundMNRank 快照覆盖，开始使用 fundSearch 做保底补源",
                    len(uncovered_fixed_income),
                )
                fallback_records, fallback_stats = collector.build_search_fallback_records(uncovered_fixed_income)
                if fallback_records:
                    fallback_records, nav_history_override_stats = collector.merge_with_newer_nav_history_records(
                        fallback_records,
                        uncovered_fixed_income,
                    )
                else:
                    nav_history_override_stats = {
                        'targets': 0,
                        'overridden': 0,
                        'unchanged': 0,
                        'nav_history_candidates': 0,
                    }
                if fallback_records:
                    snapshot_plan['snapshot_funds'].extend(fallback_records)
            else:
                nav_history_override_stats = {
                    'targets': 0,
                    'overridden': 0,
                    'unchanged': 0,
                    'nav_history_candidates': 0,
                }

            fallback_search_codes = {item['fcode'] for item in fallback_records}
            nav_history_targets = [
                fund for fund in uncovered_fixed_income
                if fund['fcode'] not in fallback_search_codes
            ]
            nav_history_fallback_records = []
            nav_history_fallback_stats = {
                'targets': len(nav_history_targets),
                'matched': 0,
                'missing': len(nav_history_targets),
            }
            if nav_history_targets:
                logger.info(
                    "固定收益基金中仍有 %s 只未从 fundSearch 获得可用净值，开始使用本地 nav_history 做二级保底",
                    len(nav_history_targets),
                )
                nav_history_fallback_records, nav_history_fallback_stats = collector.build_nav_history_fallback_records(
                    nav_history_targets
                )
                if nav_history_fallback_records:
                    snapshot_plan['snapshot_funds'].extend(nav_history_fallback_records)

            primary_snapshot_total = snapshot_plan['snapshot_total']
            save_stats = collector._save_funds(
                snapshot_plan['snapshot_funds'],
                return_stats=True,
            )
            final_snapshot_total = len(snapshot_plan['snapshot_funds'])

            logger.info(
                "基金列表检查完成: 共发现 %s 只新基金，去重后快照 %s 只，"
                "新增入库 %s 只，更新基础信息 %s 只，更新收益快照 %s 只",
                total_new,
                final_snapshot_total,
                save_stats['fund_inserts'],
                save_stats['fund_updates'],
                save_stats['returns_updates'],
            )
            if fallback_stats['targets'] > 0:
                logger.info(
                    "fundSearch 保底补源: 目标 %s，只查询 %s，只命中 %s，只缺失 %s，只失败 %s",
                    fallback_stats['targets'],
                    fallback_stats['queried'],
                    fallback_stats['matched'],
                    fallback_stats['missing'],
                    fallback_stats['failed'],
                )
            if nav_history_override_stats['targets'] > 0:
                logger.info(
                    "nav_history 覆盖修正: 候选 %s，只覆盖 %s，只保持原值 %s",
                    nav_history_override_stats['targets'],
                    nav_history_override_stats['overridden'],
                    nav_history_override_stats['unchanged'],
                )
            if nav_history_fallback_stats['targets'] > 0:
                logger.info(
                    "nav_history 二级保底: 目标 %s，只命中 %s，只缺失 %s",
                    nav_history_fallback_stats['targets'],
                    nav_history_fallback_stats['matched'],
                    nav_history_fallback_stats['missing'],
                )

            return {
                'count': total_new,
                'processed': final_snapshot_total,
                'inserted': save_stats['fund_inserts'],
                'updated': save_stats['fund_updates'],
                'failed': save_stats['failed_records'],
                'metadata': {
                    'type_stats': type_stats,
                    'primary_snapshot_total': primary_snapshot_total,
                    'snapshot_total': final_snapshot_total,
                    'snapshot_saved': save_stats['saved_count'],
                    'returns_updated': save_stats['returns_updates'],
                    'returns_inserted': save_stats['returns_inserts'],
                    'fixed_income_uncovered': len(uncovered_fixed_income),
                    'fallback_search_stats': fallback_stats,
                    'fallback_nav_history_override_stats': nav_history_override_stats,
                    'fallback_nav_history_stats': nav_history_fallback_stats,
                }
            }

        try:
            return self._execute_with_history('每日基金列表检查', task_logic, task_key='daily_fund_list_check')
        except Exception as e:
            logger.error(f"每日基金列表检查失败: {e}", exc_info=True)
            raise

    def _daily_autostock_verify(self):
        """每日AutoStock数据验证 - 抽样验证100只基金"""
        logger.info("="*60)
        logger.info("开始执行: 每日AutoStock数据验证")
        logger.info("="*60)

        def task_logic():
            validator = AutoStockValidator(
                config=self.config,
                db_manager=self.db_manager
            )

            # 抽样验证100只基金（约3分钟）
            result = validator.verify_sample(sample_size=100)

            accuracy = result['accuracy']
            logger.info(f"抽样验证完成: 准确率 {accuracy:.2f}%")

            # 检查准确率阈值
            threshold = self.config.get('validation', {}).get('accuracy_threshold', 90.0)
            if accuracy < threshold:
                logger.warning(f"⚠️ 准确率 {accuracy:.2f}% 低于阈值 {threshold}%")

            return {
                'count': result['sample_size'],
                'processed': result['compared_count'],
                'metadata': {
                    'accuracy': accuracy,
                    'perfect_match': result['perfect_match'],
                    'threshold': threshold,
                    'passed': accuracy >= threshold
                }
            }

        try:
            return self._execute_with_history('每日AutoStock数据验证', task_logic, task_key='daily_autostock_verify')
        except Exception as e:
            logger.error(f"每日AutoStock数据验证失败: {e}", exc_info=True)
            raise

    def _weekly_full_verify(self):
        """每周完整数据验证 - 完整性+准确性验证"""
        self._log_routine_info("=" * 60)
        self._log_routine_info("开始执行: 每周完整数据验证")
        self._log_routine_info("=" * 60)

        def task_logic():
            validator = AutoStockValidator(
                config=self.config,
                db_manager=self.db_manager
            )

            # 执行完整验证（约40分钟）
            result = validator.verify_all()
            completeness_result = result['completeness']
            accuracy_result = result['accuracy']

            completeness = completeness_result.get('completeness_assessed')
            completeness_on_fetched = completeness_result.get('coverage_on_fetched')
            can_conclude_completeness = completeness_result.get('can_conclude_completeness', True)
            accuracy = accuracy_result['accuracy']
            overall_score = result['overall_score']

            self._log_routine_info("完整验证完成: 综合得分 %.2f%%", overall_score)
            if can_conclude_completeness and completeness is not None:
                self._log_routine_info("  - 完整性: %.2f%%", completeness)
            else:
                self._log_routine_warning(
                    "  - 完整性: 外部源疑似不完整，本轮不下结论"
                    f"（可见样本覆盖率: {completeness_on_fetched:.2f}%）"
                )
            self._log_routine_info("  - 准确性: %.2f%%", accuracy)

            # 检查是否有缺失基金需要补充
            missing_count = completeness_result['missing_in_local']
            if missing_count > 0:
                self._log_routine_warning("⚠️ 本地数据库缺失 %s 只基金", missing_count)

            completeness_processed = (
                completeness_result.get('autostock_fetched_total') or
                completeness_result.get('autostock_total', 0)
            )
            compared_count = accuracy_result.get('compared_count', 0)
            sample_size = accuracy_result.get('sample_size', compared_count)
            failed_count = max(sample_size - compared_count, 0)
            stage_summary = {
                'completeness': {
                    'data_source': completeness_result.get('data_source'),
                    'autostock_total': completeness_result.get('autostock_total', 0),
                    'autostock_fetched_total': completeness_processed,
                    'local_total': completeness_result.get('local_total', 0),
                    'missing_in_local': missing_count,
                    'missing_in_autostock': completeness_result.get('missing_in_autostock', 0),
                    'coverage_on_fetched': completeness_on_fetched,
                    'completeness_assessed': completeness,
                    'can_conclude_completeness': can_conclude_completeness,
                    'source_meta': completeness_result.get('source_meta', {}),
                },
                'accuracy': {
                    'sample_size': sample_size,
                    'compared_count': compared_count,
                    'perfect_match': accuracy_result.get('perfect_match', 0),
                    'accuracy': accuracy,
                    'batch_meta': accuracy_result.get('batch_meta', {}),
                }
            }

            return {
                'count': sample_size,
                'processed': completeness_processed + compared_count,
                'updated': 0,
                'failed': failed_count,
                'metadata': {
                    'completeness': completeness,
                    'completeness_on_fetched': completeness_on_fetched,
                    'can_conclude_completeness': can_conclude_completeness,
                    'accuracy': accuracy,
                    'overall_score': overall_score,
                    'overall_score_mode': result.get('overall_score_mode'),
                    'missing_count': missing_count,
                    'stage_summary': stage_summary,
                }
            }

        try:
            return self._execute_with_history('每周完整数据验证', task_logic, task_key='weekly_full_verify')
        except Exception as e:
            logger.error(f"每周完整数据验证失败: {e}", exc_info=True)
            raise

    def _monthly_cleanup(self):
        """每月数据清理 - 默认安全模式下预览筛选历史清理并清理旧任务历史"""
        logger.info("="*60)
        logger.info("开始执行: 每月数据清理")
        logger.info("="*60)

        def task_logic():
            cleanup_cfg = (
                self.config.get('scheduler', {})
                .get('cleanup', {})
                .get('screening_history', {})
            )
            project_root = Path(__file__).resolve().parent.parent
            screening_retention_days = int(cleanup_cfg.get('retention_days', 30))
            screening_delete_runs = bool(cleanup_cfg.get('delete_runs', False))
            screening_delete_batch_run_limit = int(cleanup_cfg.get('delete_batch_run_limit', 100))
            screening_apply = bool(cleanup_cfg.get('apply', False))
            keep_latest_success = bool(cleanup_cfg.get('keep_latest_success_per_period', True))

            screening_cleanup = cleanup_screening_history(
                db_manager=self.db_manager,
                retention_days=screening_retention_days,
                dry_run=not screening_apply,
                delete_runs=screening_delete_runs,
                delete_batch_run_limit=screening_delete_batch_run_limit,
                keep_latest_success_per_period=keep_latest_success,
            )

            with self.db_manager.get_session() as session:
                history_manager = TaskHistoryManager(session)

                # 清理90天前的历史记录
                deleted = history_manager.cleanup_old_history(days=90)
                task_artifacts = summarize_task_artifacts(project_root, runtime_config=self.config)

                logger.info(f"清理了 {deleted} 条历史记录（90天前）")
                if screening_cleanup.get('dry_run'):
                    logger.info(
                        "筛选历史清理预览: candidate_runs=%d decisions=%d picks=%d",
                        screening_cleanup.get('candidate_run_count', 0),
                        screening_cleanup.get('decisions_to_delete', 0),
                        screening_cleanup.get('picks_to_delete', 0),
                    )
                else:
                    logger.warning(
                        "筛选历史清理已执行: deleted_decisions=%d deleted_picks=%d deleted_runs=%d",
                        screening_cleanup.get('deleted_decisions', 0),
                        screening_cleanup.get('deleted_picks', 0),
                        screening_cleanup.get('deleted_runs', 0),
                    )
                logger.info(
                    "任务副产物审计预览: logs=%d files/%d bytes tasks=%d files/%d bytes disk=%s(%s%%)",
                    task_artifacts.get('directories', {}).get('logs', {}).get('file_count', 0),
                    task_artifacts.get('directories', {}).get('logs', {}).get('total_size_bytes', 0),
                    task_artifacts.get('directories', {}).get('tasks', {}).get('file_count', 0),
                    task_artifacts.get('directories', {}).get('tasks', {}).get('total_size_bytes', 0),
                    task_artifacts.get('disk_usage', {}).get('status', 'unknown'),
                    task_artifacts.get('disk_usage', {}).get('usage_percent', 0),
                )

                return {
                    'count': deleted + screening_cleanup.get('deleted_total', 0),
                    'metadata': {
                        'task_history_retention_days': 90,
                        'task_history_deleted': deleted,
                        'task_artifacts': task_artifacts,
                        'project_artifacts': {
                            'project_directories': task_artifacts.get('project_directories', {}),
                            'project_categories': task_artifacts.get('project_categories', {}),
                            'project_retention_preview': task_artifacts.get('project_retention_preview', {}),
                            'project_largest_files': task_artifacts.get('project_largest_files', []),
                        },
                        'screening_history': {
                            'retention_days': screening_cleanup.get('retention_days'),
                            'dry_run': screening_cleanup.get('dry_run', True),
                            'delete_runs': screening_cleanup.get('delete_runs', False),
                            'delete_batch_run_limit': screening_cleanup.get('delete_batch_run_limit', screening_delete_batch_run_limit),
                            'delete_batch_count': screening_cleanup.get('delete_batch_count', 0),
                            'candidate_run_count': screening_cleanup.get('candidate_run_count', 0),
                            'decisions_to_delete': screening_cleanup.get('decisions_to_delete', 0),
                            'picks_to_delete': screening_cleanup.get('picks_to_delete', 0),
                            'deleted_decisions': screening_cleanup.get('deleted_decisions', 0),
                            'deleted_picks': screening_cleanup.get('deleted_picks', 0),
                            'deleted_runs': screening_cleanup.get('deleted_runs', 0),
                            'protected_run_count': screening_cleanup.get('protected_run_count', 0),
                            'protected_reason_counts': screening_cleanup.get('protected_reason_counts', {}),
                            'candidate_status_counts': screening_cleanup.get('candidate_status_counts', {}),
                            'candidate_fund_type_counts': screening_cleanup.get('candidate_fund_type_counts', {}),
                        },
                    }
                }

        try:
            return self._execute_with_history('每月数据清理', task_logic, task_key='monthly_cleanup')
        except Exception as e:
            logger.error(f"每月数据清理失败: {e}", exc_info=True)
            raise

    # ============ 调度器控制方法 ============

    def start(self):
        """启动调度器（增强版日志）"""
        start_time = datetime.now()

        logger.info("="*60)
        logger.info("启动任务调度器")
        logger.info("="*60)

        if HAS_ENHANCED_LOGGER:
            task_logger.log_start('SCHEDULER_STARTUP',
                                 timezone=self.timezone,
                                 retry_config=self.retry_config,
                                 task_switches=self.task_switches)

        self.add_jobs()
        self.scheduler.start()

        job_count = len(self.scheduler.get_jobs())
        elapsed = (datetime.now() - start_time).total_seconds()

        if HAS_ENHANCED_LOGGER:
            task_logger.log_complete('SCHEDULER_STARTUP', round(elapsed * 1000, 2),
                                    success=True,
                                    job_count=job_count)
        else:
            logger.info(f"调度器已启动 jobs={job_count} elapsed={elapsed:.2f}s")

        logger.info("调度器已启动，按 Ctrl+C 停止")
        self.print_schedule()

    def stop(self):
        """停止调度器（增强版日志）"""
        stop_time = datetime.now()

        if HAS_ENHANCED_LOGGER:
            task_logger.log_start('SCHEDULER_SHUTDOWN',
                                 running_tasks=len(self._running_tasks))

        logger.info("正在停止调度器...")

        # 记录当前运行中的任务
        with self._running_tasks_lock:
            if self._running_tasks:
                logger.warning(f"有 {len(self._running_tasks)} 个任务正在运行:")
                for exec_id, task_info in self._running_tasks.items():
                    logger.warning(f"  - [{exec_id}] {task_info['task_name']} "
                                  f"(started: {task_info['start_time'].isoformat()})")

        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            elapsed = (datetime.now() - stop_time).total_seconds()

            if HAS_ENHANCED_LOGGER:
                task_logger.log_complete('SCHEDULER_SHUTDOWN', round(elapsed * 1000, 2),
                                        success=True,
                                        stats=self.stats)

            logger.info(f"调度器已停止 (耗时: {elapsed:.2f}s)")
            self.print_stats()

    def get_running_tasks(self) -> dict:
        """获取当前运行中的任务列表"""
        with self._running_tasks_lock:
            return {
                exec_id: {
                    'task_name': info['task_name'],
                    'start_time': info['start_time'].isoformat(),
                    'thread_id': info['thread_id'],
                    'running_seconds': (datetime.now() - info['start_time']).total_seconds()
                }
                for exec_id, info in self._running_tasks.items()
            }

    def get_scheduler_status(self) -> dict:
        """获取调度器完整状态"""
        jobs = self.scheduler.get_jobs()
        running_tasks = self.get_running_tasks()

        status = {
            'scheduler_running': self.scheduler.running,
            'timezone': str(self.timezone),
            'total_jobs': len(jobs),
            'running_tasks_count': len(running_tasks),
            'running_tasks': running_tasks,
            'stats': self.stats,
            'jobs': []
        }

        for job in jobs:
            status['jobs'].append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })

        if HAS_ENHANCED_LOGGER:
            task_logger.log_progress('SCHEDULER_STATUS', 0, 0,
                                    **{k: v for k, v in status.items() if k != 'jobs'})

        return status

    def print_schedule(self):
        """打印调度计划"""
        logger.info("\n" + "="*60)
        logger.info("调度任务列表")
        logger.info("="*60)

        jobs = self.scheduler.get_jobs()
        if not jobs:
            logger.info("暂无调度任务")
            return

        for job in jobs:
            next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else "未计划"
            logger.info(f"  • {job.name}")
            logger.info(f"    ID: {job.id}")
            logger.info(f"    下次执行: {next_run}")
            logger.info(f"    触发器: {job.trigger}")
            logger.info("")

    def print_stats(self):
        """打印执行统计"""
        logger.info("\n" + "="*60)
        logger.info("调度器执行统计")
        logger.info("="*60)
        logger.info(f"总执行次数: {self.stats['total_executed']}")
        logger.info(f"成功次数: {self.stats['total_success']}")
        logger.info(f"失败次数: {self.stats['total_failed']}")

        if self.stats['last_execution']:
            logger.info("\n最近执行时间:")
            for job_id, exec_time in self.stats['last_execution'].items():
                logger.info(f"  • {job_id}: {exec_time.strftime('%Y-%m-%d %H:%M:%S')}")

        logger.info("="*60)

    def run_task_once(self, task_name: str):
        """手动执行一次任务（用于测试）"""
        task_methods = {
            'daily_fund_list_check': self._daily_fund_list_check,
            'daily_nav_update': self._daily_nav_update,
            'daily_autostock_verify': self._daily_autostock_verify,
            'daily_risk_update': self._daily_risk_update,
            'daily_screening_workflow': self._daily_screening_workflow,
            'daily_supplement_missing_nav': self._daily_supplement_missing_nav,
            'daily_policy_monitor': self._daily_policy_monitor,
            'weekly_detail_update': self._weekly_detail_update,
            'weekly_holding_period_update': self._weekly_holding_period_update,
            'weekly_high_speed_detail': self._weekly_high_speed_detail_update,
            'weekly_manager_update': self._weekly_manager_update,
            'weekly_company_update': self._weekly_company_update,
            'weekly_full_verify': self._weekly_full_verify,
            'monthly_backup': self._monthly_backup,
            'monthly_cleanup': self._monthly_cleanup,
            'monthly_supplement_estab': self._monthly_supplement_estab_date,
            'hourly_timeout_monitor': self._hourly_timeout_monitor,
        }

        if task_name not in task_methods:
            logger.error(f"未知任务: {task_name}")
            print(f"\n可用任务列表:")
            for task in task_methods.keys():
                print(f"  • {task}")
            return False

        logger.info(f"手动执行任务: {task_name}")
        try:
            task_methods[task_name]()
            return True
        except Exception as e:
            logger.error(f"任务执行失败: {e}", exc_info=True)
            return False
