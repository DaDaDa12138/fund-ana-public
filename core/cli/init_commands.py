#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""初始化相关 CLI 命令实现。"""


def cmd_init_db(
    args,
    config,
    *,
    logger,
    get_configured_db_path,
    emit_command_error,
    init_database_fn,
    print_database_stats_fn,
):
    """初始化数据库。"""
    logger.info("=" * 60)
    logger.info("初始化数据库")
    logger.info("=" * 60)

    try:
        db_path = get_configured_db_path(config)
    except Exception as exc:
        return emit_command_error("解析数据库配置失败", exc)

    try:
        success = init_database_fn(db_path=db_path, drop_existing=args.drop)
    except Exception as exc:
        return emit_command_error("初始化数据库失败", exc)

    if not success:
        print("\n❌ 数据库初始化失败！")
        return 1

    try:
        print_database_stats_fn(db_path)
    except Exception as exc:
        return emit_command_error("显示数据库统计失败", exc)

    print("\n✅ 数据库初始化成功！")
    return 0
