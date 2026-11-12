#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI 公共依赖函数。"""


def get_configured_db_path(
    config,
    *,
    resolve_db_path_fn,
    default_db_path='database/bond_funds.db',
):
    """统一解析运行期数据库路径。"""
    if config is None:
        config = {}

    if not isinstance(config, dict):
        raise ValueError("配置对象必须是映射")

    db_config = config.get('database', {})
    if db_config is None:
        db_config = {}

    if not isinstance(db_config, dict):
        raise ValueError("配置项 database 必须是映射")

    return resolve_db_path_fn(db_config.get('path', default_db_path))


def ensure_database_initialized(
    config,
    *,
    get_configured_db_path_fn,
    check_database_exists_fn,
    emit_command_error_fn,
    print_fn,
):
    """在构造采集器或读取 ORM 前，确认已有数据库完成当前迁移。"""
    try:
        db_path = get_configured_db_path_fn(config)
    except Exception as exc:
        emit_command_error_fn("解析数据库配置失败", exc)
        return None

    if check_database_exists_fn(db_path):
        try:
            from database.migrations import run_all_migrations

            run_all_migrations(db_path)
        except Exception as exc:
            emit_command_error_fn("数据库结构迁移失败，未开始采集或计算", exc)
            return None
        return db_path

    print_fn("❌ 数据库未初始化，请先运行: python main.py init-db")
    return None


def emit_command_error(message, exc, *, logger_obj, print_fn):
    """统一输出命令层异常。"""
    if logger_obj is not None:
        logger_obj.error("%s: %s", message, exc)
    print_fn(f"❌ {message}: {exc}")
    return 1


def update_collection_task_history(success_count, total_count, failed_count, inserted_count):
    """统一更新采集任务历史。"""
    from utils.task_reporter import update_task_history

    update_task_history(
        success_count=success_count,
        total_count=total_count,
        failed_count=failed_count,
        inserted_count=inserted_count,
    )
