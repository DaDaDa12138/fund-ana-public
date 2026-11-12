#!/usr/bin/env python3
"""
配置文件管理器
提供读取、更新、保存配置文件的功能
"""

import copy
import os
import stat
import tempfile
import yaml
import threading
from pathlib import Path
from typing import Any, Dict, Optional
from utils.logger import get_logger, setup_logger
from utils.project_runtime_config import (
    get_project_config_path,
    load_project_config_payload,
    load_runtime_config,
    project_payload_from_core_config,
    resolve_runtime_config_path,
    uses_project_runtime_bridge,
)
from utils.scheduler_day_of_week import normalize_daily_day_of_week_to_cron

logger = get_logger(__name__)


def _is_valid_daily_day_of_week_token(value: str) -> bool:
    """校验 daily 任务第三段 day_of_week 是否可被规范化为有效 Cron 语义。"""
    return normalize_daily_day_of_week_to_cron(value) is not None


def _is_valid_weekly_day_of_week_token(value: str) -> bool:
    """校验 weekly 任务第三段周几是否符合项目配置语义。"""
    normalized = str(value or '').strip().lower()
    if not normalized:
        return False
    valid_values = {
        '0', '1', '2', '3', '4', '5', '6',
        'sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat',
    }
    return normalized in valid_values


class ConfigManager:
    """配置管理器 - 线程安全的配置文件读写"""

    def __init__(self, config_path: str | Path | None = None):
        """
        初始化配置管理器

        Args:
            config_path: 配置文件路径；默认使用项目级 config/project.yaml
        """
        setup_logger(__name__)
        requested_path = resolve_runtime_config_path(config_path)
        self.config_path = requested_path
        self._lock = threading.Lock()  # 线程锁，保证并发安全
        self._config_cache: Optional[Dict] = None

        if not self.config_path.exists():
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")

    def load_config(self, force_reload: bool = False) -> Dict[str, Any]:
        """
        加载配置文件

        Args:
            force_reload: 是否强制重新加载（忽略缓存）

        Returns:
            配置字典
        """
        with self._lock:
            if self._config_cache is not None and not force_reload:
                return copy.deepcopy(self._config_cache)

            try:
                loaded = load_runtime_config(self.config_path)
                self._config_cache = self._ensure_config_dict(loaded)
                logger.info(f"配置文件已加载: {self.config_path}")
                return copy.deepcopy(self._config_cache)
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}", exc_info=True)
                raise

    @staticmethod
    def _ensure_config_dict(config_data: Any) -> Dict[str, Any]:
        """确保配置顶层始终为 dict，异常结构回退为空配置。"""
        if isinstance(config_data, dict):
            return config_data
        if config_data is not None:
            logger.warning("检测到非对象顶层 YAML 配置，已回退为空配置")
        return {}

    def save_config(self, config: Dict[str, Any]) -> bool:
        """
        保存配置到文件

        Args:
            config: 要保存的配置字典

        Returns:
            是否成功
        """
        with self._lock:
            try:
                save_payload = copy.deepcopy(config)
                target_path = self.config_path
                if uses_project_runtime_bridge(self.config_path):
                    save_payload = project_payload_from_core_config(
                        save_payload,
                        existing_project_payload=load_project_config_payload(self.config_path),
                    )
                    target_path = resolve_runtime_config_path(self.config_path)

                # 创建备份（失败不阻止保存）
                backup_path = target_path.with_suffix('.yaml.bak')
                if target_path.exists():
                    import shutil
                    try:
                        shutil.copy2(target_path, backup_path)
                        logger.debug(f"已创建配置备份: {backup_path}")
                    except Exception as backup_err:
                        logger.warning(f"配置备份失败（继续保存）: {backup_err}")

                # 原子写回并继承原文件权限，避免容器/接口写回后破坏宿主机配置权限。
                config_dir = target_path.parent
                tmp_path = None
                original_mode = None
                original_uid = None
                original_gid = None

                if target_path.exists():
                    config_stat = target_path.stat()
                    original_mode = stat.S_IMODE(config_stat.st_mode)
                    original_uid = config_stat.st_uid
                    original_gid = config_stat.st_gid

                with tempfile.NamedTemporaryFile(
                    mode='w',
                    encoding='utf-8',
                    dir=str(config_dir),
                    delete=False,
                    suffix='.tmp'
                ) as tmp_f:
                    yaml.dump(
                        save_payload,
                        tmp_f,
                        allow_unicode=True,
                        default_flow_style=False,
                        sort_keys=False,
                        indent=2
                    )
                    tmp_path = tmp_f.name

                if original_mode is not None:
                    os.chmod(tmp_path, original_mode)
                if original_uid is not None and original_gid is not None:
                    try:
                        os.chown(tmp_path, original_uid, original_gid)
                    except PermissionError:
                        logger.warning("当前进程无权保留配置文件 owner，跳过 chown")

                os.replace(tmp_path, target_path)

                # 更新缓存
                self._config_cache = copy.deepcopy(config)
                logger.info(f"配置文件已保存: {target_path}")
                return True

            except Exception as e:
                if 'tmp_path' in locals() and tmp_path:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
                logger.error(f"保存配置文件失败: {e}", exc_info=True)
                return False

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        获取配置值（支持嵌套路径）

        Args:
            key_path: 配置路径，使用点号分隔，如 "scheduler.tasks.daily_nav_update"
            default: 默认值

        Returns:
            配置值

        Examples:
            >>> manager.get("scheduler.tasks.daily_nav_update")
            True
            >>> manager.get("api.tiantian.base_url")
            "http://tiantian-api:3000"
        """
        config = self.load_config()
        keys = key_path.split('.')

        value = config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def set(self, key_path: str, value: Any) -> bool:
        """
        设置配置值（支持嵌套路径）

        Args:
            key_path: 配置路径，使用点号分隔
            value: 要设置的值

        Returns:
            是否成功

        Examples:
            >>> manager.set("scheduler.tasks.daily_nav_update", False)
            True
        """
        config = self.load_config()
        keys = key_path.split('.')

        # 导航到最后一层
        current = config
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        # 设置值
        current[keys[-1]] = value

        # 保存
        return self.save_config(config)

    def update_task_config(
        self,
        task_id: str,
        enabled: Optional[bool] = None,
        schedule: Optional[str] = None,
        *,
        schedule_kind: Optional[str] = None,
    ) -> bool:
        """
        更新任务配置

        Args:
            task_id: 任务ID (如 "daily_nav_update")
            enabled: 是否启用（可选）
            schedule: 调度时间，格式为 "hour,minute" 或 "hour,minute,third_part"（可选）
                     例如:
                          daily: "6,0" 表示每天06:00，"10,0,1-5" 表示每周一到五10:00
                          weekly: "10,0" 表示沿用默认周几在10:00执行，"10,0,0" 表示每周日10:00
                          monthly: "2,0,1" 表示每月1号02:00
            schedule_kind: 调度类型，可选值 daily/weekly/monthly/interval/manual，用于校验第三段语义

        Returns:
            是否成功

        Note:
            修改调度时间后需要重启scheduler服务才能生效
        """
        if enabled is None and schedule is None:
            logger.warning("没有提供任何要更新的配置")
            return False

        try:
            # 加载当前配置
            config = self.load_config(force_reload=True)

            # 确保 scheduler.tasks 存在
            if 'scheduler' not in config:
                config['scheduler'] = {}
            if 'tasks' not in config['scheduler']:
                config['scheduler']['tasks'] = {}
            if 'schedules' not in config['scheduler']:
                config['scheduler']['schedules'] = {}

            tasks = config['scheduler']['tasks']
            schedules = config['scheduler']['schedules']

            # 如果任务不存在，创建它
            if task_id not in tasks:
                tasks[task_id] = True  # 默认启用

            # 更新 enabled 状态
            if enabled is not None:
                tasks[task_id] = enabled
                logger.info(f"任务 {task_id} 的启用状态已更新为: {enabled}")

            # 更新调度时间
            if schedule is not None:
                # 验证调度时间格式
                parts = schedule.split(',')
                if len(parts) < 2 or len(parts) > 3:
                    logger.error(f"调度时间格式无效: {schedule}，应为 'hour,minute' 或 'hour,minute,day_of_week'")
                    return False

                try:
                    hour = int(parts[0])
                    minute = int(parts[1])
                    if not (0 <= hour <= 23 and 0 <= minute <= 59):
                        raise ValueError("时间范围无效")
                except ValueError as e:
                    logger.error(f"调度时间格式无效: {schedule}，{e}")
                    return False

                if len(parts) == 3:
                    third_part = parts[2].strip()
                    if schedule_kind == 'daily':
                        if not _is_valid_daily_day_of_week_token(third_part):
                            logger.error(
                                f"调度时间格式无效: {schedule}，daily 第三段需为项目周几语义(0=周日, 6=周六)或 Cron 命名语义"
                            )
                            return False
                    elif schedule_kind == 'weekly':
                        if not _is_valid_weekly_day_of_week_token(third_part):
                            logger.error(
                                f"调度时间格式无效: {schedule}，weekly 第三段需为项目周几语义(0=周日, 6=周六)"
                            )
                            return False
                    elif schedule_kind == 'monthly':
                        try:
                            day = int(third_part)
                            if not 1 <= day <= 31:
                                raise ValueError("日期范围无效")
                        except ValueError as e:
                            logger.error(f"调度时间格式无效: {schedule}，monthly 第三段需为 1-31，{e}")
                            return False

                schedules[task_id] = schedule
                logger.info(f"任务 {task_id} 的调度时间已更新为: {schedule}")
                logger.warning(f"注意：调度时间修改需要重启scheduler服务才能生效")

            # 保存配置
            return self.save_config(config)

        except Exception as e:
            logger.error(f"更新任务配置失败: {e}", exc_info=True)
            return False

    def get_task_enabled(self, task_id: str) -> bool:
        """
        获取任务是否启用

        Args:
            task_id: 任务ID

        Returns:
            是否启用（默认True）
        """
        return self.get(f"scheduler.tasks.{task_id}", True)

    def reload(self) -> Dict[str, Any]:
        """
        强制重新加载配置文件

        Returns:
            配置字典
        """
        return self.load_config(force_reload=True)


# 全局单例
_config_manager_instance: Optional[ConfigManager] = None
_config_manager_lock = threading.Lock()


def get_config_manager(config_path: str | Path | None = None) -> ConfigManager:
    """
    获取全局配置管理器单例

    Args:
        config_path: 配置文件路径；默认使用项目级 config/project.yaml

    Returns:
        ConfigManager实例
    """
    global _config_manager_instance

    resolved_path = (
        resolve_runtime_config_path(config_path)
        if uses_project_runtime_bridge(config_path)
        else Path(config_path)
    )
    with _config_manager_lock:
        if (
            _config_manager_instance is None
            or _config_manager_instance.config_path != resolved_path
        ):
            _config_manager_instance = ConfigManager(config_path)

    return _config_manager_instance
