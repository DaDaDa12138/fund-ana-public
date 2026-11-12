#!/usr/bin/env python3
"""
任务超时监控器
自动检测并标记超时的running任务为失败状态
"""

import sys
from pathlib import Path
from datetime import datetime

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from database.connection import get_db_manager
from database.connection import resolve_db_path
from database.models import TaskHistory, TaskStatus
from database.screening_run_maintenance import cleanup_stale_screening_runs
from utils.config_manager import get_config_manager
from utils.logger import get_logger, setup_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 3600  # 默认超时：1小时

# 任务中文名到 scheduler.timeouts 配置键的映射。
# 新 key 优先，旧 key 仅作为兼容回退，避免控制面与运行面脱节。
NAME_TO_TIMEOUT_KEYS = {
    '每日增量净值更新': ('daily_nav_update', 'daily_nav'),
    '每日风险指标更新': ('daily_risk_update', 'daily_risk'),
    '每日债券基金筛选工作流': ('daily_screening_workflow',),
    '每日策略监控': ('daily_policy_monitor',),
    '每日基金列表检查': ('daily_fund_list_check',),
    '每日补充缺失净值': ('daily_supplement_missing_nav',),
    '每日AutoStock数据验证': ('daily_autostock_verify',),
    '每日AutoStock验证': ('daily_autostock_verify',),  # 历史别名
    '每周基金详情更新': ('weekly_detail_update', 'weekly_details'),
    '每周持有期信息补充': ('weekly_holding_period_update', 'weekly_holding_period'),
    '每周基金经理更新': ('weekly_manager_update', 'weekly_managers'),
    '每周基金公司更新': ('weekly_company_update', 'weekly_companies'),
    '每周高速基金详情采集': ('weekly_high_speed_detail',),
    '每周完整数据验证': ('weekly_full_verify',),
    '每周完整验证': ('weekly_full_verify',),  # 历史别名
    '每月数据备份': ('monthly_backup',),
    '每月数据清理': ('monthly_cleanup',),
    '每月补充成立日期': ('monthly_supplement_estab',),
}

# 非 scheduler.timeouts 管控任务保留固定阈值
FIXED_TIMEOUTS = {
    '全量数据采集': 14400,
}

# 任务中文名到 task_status 英文 key 的映射
NAME_TO_KEY = {
    '每日增量净值更新': 'daily_nav_update',
    '每日风险指标更新': 'daily_risk_update',
    '每日债券基金筛选工作流': 'daily_screening_workflow',
    '每日策略监控': 'daily_policy_monitor',
    '每日基金列表检查': 'daily_fund_list_check',
    '每日补充缺失净值': 'daily_supplement_missing_nav',
    '每日AutoStock数据验证': 'daily_autostock_verify',
    '每日AutoStock验证': 'daily_autostock_verify',  # 历史别名
    '每周基金详情更新': 'weekly_detail_update',
    '每周持有期信息补充': 'weekly_holding_period_update',
    '每周基金经理更新': 'weekly_manager_update',
    '每周高速基金详情采集': 'weekly_high_speed_detail',
    '每周完整数据验证': 'weekly_full_verify',
    '每周完整验证': 'weekly_full_verify',  # 历史别名
    '每月数据备份': 'monthly_backup',
    '每月数据清理': 'monthly_cleanup',
    '每月补充成立日期': 'monthly_supplement_estab',
}


def load_timeout_thresholds():
    """从配置文件加载超时阈值，并补齐少量固定任务阈值。"""
    scheduler_timeouts = (
        get_config_manager()
        .load_config(force_reload=True)
        .get('scheduler', {})
        .get('timeouts', {})
    )

    timeout_map = dict(FIXED_TIMEOUTS)
    for task_name, config_keys in NAME_TO_TIMEOUT_KEYS.items():
        for config_key in config_keys:
            timeout_value = scheduler_timeouts.get(config_key)
            if isinstance(timeout_value, (int, float)) and timeout_value > 0:
                timeout_map[task_name] = int(timeout_value)
                break

    return timeout_map


def monitor_and_cleanup_timeout_tasks(db_path: str = None):
    """
    监控并清理超时任务

    Args:
        db_path: 数据库路径
    """
    setup_logger(__name__)
    if not db_path:
        db_path = resolve_db_path()

    # 使用全局单例，复用全局写锁和连接池
    db_manager = get_db_manager(db_path=db_path)
    task_timeouts = load_timeout_thresholds()

    # 先清理 screening_runs 僵尸记录，避免长期 running 污染运行状态
    try:
        run_cleanup = cleanup_stale_screening_runs(
            db_manager=db_manager,
            stale_hours=2.0,
            fail_reason='超时监控巡检自动清理：筛选运行超过阈值仍为 running',
        )
        if run_cleanup.get('cleaned', 0) > 0:
            logger.warning(
                "额外清理 screening_runs 僵尸记录: cleaned=%d running=%d",
                run_cleanup['cleaned'],
                run_cleanup.get('inspected_running', 0),
            )
    except Exception as e:
        logger.warning(f"清理 screening_runs 僵尸记录失败（非致命）: {e}")

    with db_manager.get_session() as session:
        # 查找所有running状态的任务
        running_tasks = session.query(TaskHistory).filter(
            TaskHistory.status == 'running'
        ).all()

        if not running_tasks:
            logger.info("✅ 没有发现running状态的任务")
            return 0

        logger.info(f"发现 {len(running_tasks)} 个running状态的任务，开始检查...")

        now = datetime.now()
        timeout_count = 0

        for task in running_tasks:
            # 获取任务的超时阈值
            timeout_threshold = task_timeouts.get(task.task_name, DEFAULT_TIMEOUT)

            # 计算运行时长
            duration = (now - task.started_at).total_seconds()

            # 检查是否超时
            if duration > timeout_threshold:
                logger.warning(
                    f"发现超时任务: ID={task.id}, 名称={task.task_name}, "
                    f"运行时长={int(duration/60)}分钟, 超时阈值={int(timeout_threshold/60)}分钟"
                )

                # 标记为失败
                task.status = 'failed'
                task.completed_at = now
                task.duration = int(duration)
                task.error_message = (
                    f"任务超时（超时阈值: {int(timeout_threshold/60)}分钟, "
                    f"实际运行: {int(duration/60)}分钟，已自动标记为失败）"
                )

                timeout_count += 1
                logger.info(f"  ✅ 已标记为失败: {task.task_name} (ID: {task.id})")

                # 同步清理 task_status 表
                _sync_task_status_on_timeout(session, task.task_name, now, timeout_threshold, duration)

        if timeout_count > 0:
            session.commit()
            logger.info(f"✅ 共标记 {timeout_count} 个超时任务为失败")
        else:
            logger.info("✅ 所有running任务都在正常运行中")

        return timeout_count


def _sync_task_status_on_timeout(session, task_name, now, timeout_threshold, duration):
    """超时时同步清理 task_status 表"""
    task_key = NAME_TO_KEY.get(task_name)
    if not task_key:
        logger.debug(f"  未找到 task_status 映射: {task_name}")
        return

    try:
        task_status = session.query(TaskStatus).filter_by(task_name=task_key).first()
        if task_status and task_status.status == 'running':
            task_status.status = 'failed'
            task_status.completed_at = now
            task_status.error_message = (
                f"任务超时（阈值: {int(timeout_threshold/60)}分钟, "
                f"实际: {int(duration/60)}分钟，由超时监控器清理）"
            )
            logger.info(f"  ✅ 已同步清理 task_status: {task_key}")
    except Exception as e:
        logger.warning(f"  同步清理 task_status 失败 ({task_key}): {e}")


def main():
    """主函数"""
    setup_logger(__name__)
    logger.info("=" * 60)
    logger.info("任务超时监控器")
    logger.info("=" * 60)

    try:
        timeout_count = monitor_and_cleanup_timeout_tasks()

        logger.info("\n" + "=" * 60)
        logger.info(f"监控完成，处理了 {timeout_count} 个超时任务")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.error(f"监控过程出错: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
