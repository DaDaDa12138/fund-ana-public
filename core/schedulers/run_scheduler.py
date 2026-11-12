#!/usr/bin/env python3
"""
任务调度器启动脚本
用于Docker容器中独立运行调度器
增强版: 包含详细的心跳日志和状态监控
"""

import argparse
import json
import sys
import signal
import os
import time
import threading
from pathlib import Path
from datetime import datetime

import psutil

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import yaml
from api.client import APIClient
from api.client import resolve_tiantian_base_url
from database import DatabaseManager
from database.connection import resolve_db_path
from schedulers.task_scheduler import TaskScheduler
from utils.logger import get_logger, setup_logger
from utils.project_runtime_config import load_runtime_config
from utils.service_checks import wait_tiantian_api_ready
from database.screening_run_maintenance import cleanup_stale_screening_runs

logger = get_logger(__name__)

# 心跳日志间隔（秒）- 每10分钟输出一次心跳日志
HEARTBEAT_INTERVAL = 600
# 详细状态输出间隔（秒）- 每小时输出一次详细状态
STATUS_INTERVAL = 3600
# PID 文件路径 — 防止同一环境运行多个 scheduler 实例
PID_FILE = '/tmp/scheduler.pid'
SCHEDULER_HEARTBEAT_FILE = os.environ.get('SCHEDULER_HEARTBEAT_FILE', '/tmp/scheduler.running')
TIANTIAN_READY_TIMEOUT_SECONDS = 60.0
TIANTIAN_READY_PROBE_INTERVAL_SECONDS = 2.0


def load_config():
    """加载配置文件"""
    config_path = project_root / 'config' / 'project.yaml'
    if not config_path.exists():
        logger.error(f"配置文件不存在: {config_path}")
        return {}
    return load_runtime_config(config_path)


def _get_configured_db_path(config):
    """统一解析运行期数据库路径。"""
    db_config = config.get('database', {}) if config else {}
    return resolve_db_path(db_config.get('path', 'database/bond_funds.db'))


def _check_and_write_pid():
    """检查是否已有 scheduler 实例运行，写入 PID 文件"""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                old_pid = int(f.read().strip())
            # 检查旧进程是否仍存活
            if psutil.pid_exists(old_pid):
                try:
                    proc = psutil.Process(old_pid)
                    if proc.is_running() and 'python' in proc.name().lower():
                        logger.error(f"检测到已有 scheduler 进程运行 (PID={old_pid})，退出")
                        sys.exit(1)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            logger.warning(f"清理过期 PID 文件 (旧 PID={old_pid} 已不存在)")
        except (ValueError, IOError):
            pass

    # 写入当前 PID
    with open(PID_FILE, 'w') as f:
        f.write(str(os.getpid()))
    logger.info(f"PID 文件已创建: {PID_FILE} (PID={os.getpid()})")


def _cleanup_pid_file():
    """清理 PID 文件"""
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
            logger.info(f"PID 文件已清理: {PID_FILE}")
    except Exception as e:
        logger.warning(f"清理 PID 文件失败: {e}")


def _ensure_tiantian_api_ready(config) -> bool:
    """调度器启动前等待 Tiantian 代理就绪，避免启动瞬时故障被熔断放大。"""
    base_url = resolve_tiantian_base_url(config=config)
    logger.info(
        "等待 Tiantian API 就绪: base_url=%s, timeout=%.1fs, interval=%.1fs",
        base_url,
        TIANTIAN_READY_TIMEOUT_SECONDS,
        TIANTIAN_READY_PROBE_INTERVAL_SECONDS,
    )
    ok, msg = wait_tiantian_api_ready(
        base_url=base_url,
        timeout_seconds=TIANTIAN_READY_TIMEOUT_SECONDS,
        probe_interval=TIANTIAN_READY_PROBE_INTERVAL_SECONDS,
    )
    if ok:
        logger.info("Tiantian API 就绪确认完成: %s", msg)
        return True

    logger.error("Tiantian API 就绪等待失败: %s", msg)
    return False


def heartbeat_logger(scheduler: TaskScheduler, stop_event: threading.Event):
    """
    心跳日志线程
    定期输出调度器状态，方便监控
    """
    last_status_time = time.time()
    heartbeat_count = 0

    while not stop_event.is_set():
        current_time = time.time()
        heartbeat_count += 1

        try:
            with open(SCHEDULER_HEARTBEAT_FILE, 'w', encoding='utf-8') as f:
                f.write(str(current_time))
        except Exception as e:
            logger.warning(f"[HEARTBEAT] 更新心跳文件失败: {e}")

        # 获取调度器状态
        try:
            status = scheduler.get_scheduler_status()
            running_tasks = status.get('running_tasks', {})
            stats = status.get('stats', {})

            # 查询 task_history 表中 running 状态的记录数（发现僵尸记录）
            db_running_count = 0
            try:
                with scheduler.db_manager.get_session(readonly=True) as session:
                    from database.models import TaskHistory
                    db_running_count = session.query(TaskHistory).filter(
                        TaskHistory.status == 'running'
                    ).count()
            except Exception:
                pass

            # 简单心跳日志（每10分钟）
            logger.info(
                f"[HEARTBEAT #{heartbeat_count}] "
                f"scheduler_running={status.get('scheduler_running', False)} | "
                f"total_jobs={status.get('total_jobs', 0)} | "
                f"running_tasks={len(running_tasks)} | "
                f"db_running_history={db_running_count} | "
                f"total_executed={stats.get('total_executed', 0)} | "
                f"success={stats.get('total_success', 0)} | "
                f"failed={stats.get('total_failed', 0)}"
            )

            # 如果有运行中的任务，输出详情
            if running_tasks:
                for exec_id, task_info in running_tasks.items():
                    running_sec = task_info.get('running_seconds', 0)
                    logger.info(
                        f"  └─ [RUNNING] {task_info.get('task_name', 'unknown')} | "
                        f"exec_id={exec_id} | "
                        f"running_time={running_sec:.1f}s | "
                        f"thread={task_info.get('thread_id', 'N/A')}"
                    )

            # 详细状态日志（每小时）
            if current_time - last_status_time >= STATUS_INTERVAL:
                last_status_time = current_time
                logger.info("=" * 60)
                logger.info("[HOURLY STATUS REPORT]")
                logger.info("=" * 60)

                # 输出所有任务的下次执行时间
                jobs = status.get('jobs', [])
                if jobs:
                    logger.info(f"调度任务列表 (共{len(jobs)}个):")
                    for job in sorted(jobs, key=lambda x: x.get('next_run') or ''):
                        next_run = job.get('next_run', 'N/A')
                        if next_run and next_run != 'N/A':
                            # 计算距离下次执行的时间
                            try:
                                next_dt = datetime.fromisoformat(next_run.replace('Z', '+00:00'))
                                now = datetime.now(next_dt.tzinfo) if next_dt.tzinfo else datetime.now()
                                delta = next_dt - now
                                hours, remainder = divmod(int(delta.total_seconds()), 3600)
                                minutes, _ = divmod(remainder, 60)
                                time_until = f"in {hours}h {minutes}m"
                            except Exception:
                                time_until = ""
                        else:
                            time_until = ""

                        logger.info(
                            f"  • {job.get('name', 'unknown')} | "
                            f"next_run={next_run} {time_until}"
                        )

                # 输出执行统计
                if stats.get('last_execution'):
                    logger.info("最近执行记录:")
                    for job_id, exec_time in stats['last_execution'].items():
                        exec_time_str = exec_time.isoformat() if hasattr(exec_time, 'isoformat') else str(exec_time)
                        logger.info(f"  • {job_id}: {exec_time_str}")

                logger.info("=" * 60)

        except Exception as e:
            logger.error(f"[HEARTBEAT] 获取状态失败: {e}")

        # 等待下一次心跳
        stop_event.wait(HEARTBEAT_INTERVAL)


def _mark_stale_task_history_failed(db_manager) -> int:
    """启动时清理残留 running 记录，同时保留原始失败上下文。"""
    from database.models import TaskHistory

    cleaned_count = 0
    with db_manager.get_session() as session:
        stale_running = session.query(TaskHistory).filter(
            TaskHistory.status == 'running'
        ).all()
        if not stale_running:
            return 0

        now = datetime.now()
        restart_message = '调度器重启时自动标记为失败（进程重启导致任务中断）'
        for task in stale_running:
            task.status = 'failed'
            task.completed_at = now
            if not str(task.error_message or '').strip():
                task.error_message = restart_message

            extra_data = {}
            if task.extra_data:
                try:
                    parsed_extra_data = json.loads(task.extra_data)
                except (TypeError, ValueError, json.JSONDecodeError):
                    parsed_extra_data = {}
                if isinstance(parsed_extra_data, dict):
                    extra_data = parsed_extra_data

            extra_data.update({
                'scheduler_restart_marked_failed': True,
                'scheduler_restart_marked_failed_at': now.isoformat(),
                'scheduler_restart_failure_message': restart_message,
            })
            task.extra_data = json.dumps(extra_data, ensure_ascii=False)
            cleaned_count += 1

        session.commit()

    return cleaned_count


def main():
    """主函数"""
    setup_logger(__name__)
    start_time = datetime.now()

    logger.info("=" * 60)
    logger.info("启动任务调度器（Docker模式）- 增强版")
    logger.info(f"启动时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"心跳间隔: {HEARTBEAT_INTERVAL}秒")
    logger.info(f"状态报告间隔: {STATUS_INTERVAL}秒")
    logger.info("=" * 60)

    # PID 文件防多实例检查（在配置加载前执行）
    _check_and_write_pid()

    # 加载配置
    config = load_config()
    if config:
        logger.info("配置文件加载成功")
        # 输出关键配置
        scheduler_config = config.get('scheduler', {})
        logger.info(f"  时区: {scheduler_config.get('timezone', 'Asia/Shanghai')}")
        logger.info(f"  任务开关: {scheduler_config.get('tasks', {})}")
    else:
        logger.warning("使用默认配置")

    # 初始化数据库管理器
    db_path = _get_configured_db_path(config)
    logger.info(f"初始化数据库: {db_path}")
    db_manager = DatabaseManager(
        db_path=db_path
    )

    if not _ensure_tiantian_api_ready(config):
        _cleanup_pid_file()
        sys.exit(1)

    # 初始化API客户端
    logger.info(f"初始化API客户端: {resolve_tiantian_base_url(config=config)}")
    api_client = APIClient(config=config)

    # 启动时清理 stale locks（容器重启后残留的 running 状态）
    from utils.task_lock import TaskStatusManager
    stale_cleaner = TaskStatusManager(db_manager=db_manager)
    cleaned = stale_cleaner.cleanup_stale_locks(timeout_seconds=7200)
    if cleaned:
        logger.info(f"启动时清理了 {cleaned} 个 stale task_status 锁")

    # 清理 task_history 表中残留的 running 记录（进程重启 = 旧任务已死）
    try:
        cleaned_stale_history_count = _mark_stale_task_history_failed(db_manager)
        if cleaned_stale_history_count > 0:
            logger.warning(f"启动时清理了 {cleaned_stale_history_count} 个 task_history 僵尸记录")
    except Exception as e:
        logger.warning(f"清理 task_history 僵尸记录失败（非致命）: {e}")

    # 清理 screening_runs 表中残留的 running 记录（筛选中断导致）
    try:
        result = cleanup_stale_screening_runs(
            db_manager=db_manager,
            stale_hours=2.0,
            fail_reason='调度器启动时自动清理：筛选任务中断后遗留 running 状态',
        )
        if result.get('cleaned', 0) > 0:
            logger.warning(
                "启动时清理了 %d 个 screening_runs 僵尸记录（running=%d）",
                result['cleaned'],
                result.get('inspected_running', 0),
            )
    except Exception as e:
        logger.warning(f"清理 screening_runs 僵尸记录失败（非致命）: {e}")

    # 创建并启动调度器
    logger.info("创建任务调度器...")
    scheduler = TaskScheduler(
        config=config,
        db_manager=db_manager,
        api_client=api_client
    )

    # 启动调度器
    scheduler.start()

    # 创建健康检查标志文件
    try:
        with open(SCHEDULER_HEARTBEAT_FILE, 'w', encoding='utf-8') as f:
            f.write(str(time.time()))
        logger.info("健康检查标志文件已创建")
    except Exception as e:
        logger.warning(f"无法创建健康检查标志文件: {e}")

    # 启动心跳日志线程
    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=heartbeat_logger,
        args=(scheduler, stop_event),
        daemon=True,
        name="HeartbeatLogger"
    )
    heartbeat_thread.start()
    logger.info("心跳日志线程已启动")

    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到终止信号，正在停止调度器...")

        # 停止心跳线程
        stop_event.set()
        heartbeat_thread.join(timeout=5)

        # 删除健康检查标志文件
        try:
            if os.path.exists(SCHEDULER_HEARTBEAT_FILE):
                os.remove(SCHEDULER_HEARTBEAT_FILE)
        except Exception:
            pass

        # 清理 PID 文件
        _cleanup_pid_file()

        # 停止调度器
        scheduler.stop()

        # 输出运行时间统计
        end_time = datetime.now()
        run_duration = end_time - start_time
        logger.info(f"调度器已停止，总运行时间: {run_duration}")
        sys.exit(0)


def run_check_mode():
    """单次检查调度器配置和任务注册，不进入常驻循环。"""
    config = load_config()
    if config:
        logger.info("调度器检查模式: 配置加载成功")
    else:
        logger.warning("调度器检查模式: 使用默认配置")

    db_path = _get_configured_db_path(config)
    logger.info(f"调度器检查模式: 初始化数据库 {db_path}")
    db_manager = DatabaseManager(db_path=db_path)

    logger.info(f"调度器检查模式: 初始化API客户端 {resolve_tiantian_base_url(config=config)}")
    api_client = APIClient(config=config)

    scheduler = TaskScheduler(
        config=config,
        db_manager=db_manager,
        api_client=api_client
    )
    scheduler.add_jobs()
    job_count = len(scheduler.scheduler.get_jobs())
    logger.info(f"调度器检查通过，已注册任务数: {job_count}")
    return 0


def cli_main(argv=None):
    parser = argparse.ArgumentParser(description='任务调度器启动脚本')
    parser.add_argument('--check', action='store_true', help='执行一次调度器配置检查后退出（审计模式）')
    args = parser.parse_args(argv)

    if args.check:
        return run_check_mode()

    main()
    return 0


if __name__ == '__main__':
    sys.exit(cli_main())
