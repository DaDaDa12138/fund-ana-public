#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""运行态与查询类 CLI 命令实现。"""


def cmd_stats(
    args,
    config,
    *,
    get_configured_db_path,
    check_database_exists,
    print_database_stats,
    api_client_cls,
    emit_command_error,
):
    """显示统计信息。"""
    try:
        db_path = get_configured_db_path(config)

        if not check_database_exists(db_path):
            print("❌ 数据库未初始化")
            return 1

        print_database_stats(db_path)

        if getattr(args, 'api', False):
            client = api_client_cls(config)
            client.print_stats()

        return 0
    except Exception as exc:
        return emit_command_error("显示统计信息失败", exc)


def cmd_scheduler(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    database_manager_cls,
    api_client_cls,
    scheduler_cls,
):
    """启动调度器守护进程。"""
    logger.info("=" * 60)
    logger.info("启动定时调度器")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        db_config = config.get('database', {}) if isinstance(config, dict) else {}
        wal_mode = db_config.get('wal_mode', True) if isinstance(db_config, dict) else True
        db_manager = database_manager_cls(db_path=db_path, wal_mode=wal_mode)
        api_client = api_client_cls(config)

        scheduler = scheduler_cls(
            config=config,
            db_manager=db_manager,
            api_client=api_client,
        )

        try:
            scheduler.start()

            import time

            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n收到停止信号...")
            scheduler.stop()

        return 0
    except Exception as exc:
        return emit_command_error("启动定时调度器失败", exc)


def cmd_run_task(
    args,
    config,
    *,
    logger,
    ensure_database_initialized,
    emit_command_error,
    database_manager_cls,
    api_client_cls,
    scheduler_cls,
):
    """手动执行单个调度任务。"""
    logger.info("=" * 60)
    logger.info(f"手动执行任务: {args.task}")
    logger.info("=" * 60)

    db_path = ensure_database_initialized(config)
    if not db_path:
        return 1

    try:
        db_config = config.get('database', {}) if isinstance(config, dict) else {}
        wal_mode = db_config.get('wal_mode', True) if isinstance(db_config, dict) else True
        db_manager = database_manager_cls(db_path=db_path, wal_mode=wal_mode)
        api_client = api_client_cls(config)

        scheduler = scheduler_cls(
            config=config,
            db_manager=db_manager,
            api_client=api_client,
        )

        success = scheduler.run_task_once(args.task)

        if success:
            print(f"\n✅ 任务 {args.task} 执行成功")
            return 0

        print(f"\n❌ 任务 {args.task} 执行失败")
        return 1
    except Exception as exc:
        return emit_command_error(f"执行任务 {args.task} 失败", exc)


def cmd_query_decision(
    args,
    config,
    *,
    emit_command_error,
    query_fund_decisions_fn,
):
    """查询基金筛选决策路径。"""
    del config

    fcode = args.fcode
    run_id = getattr(args, 'run_id', None)

    print(f"\n{'=' * 60}")
    print(f"基金筛选决策路径查询: {fcode}")
    print(f"{'=' * 60}")

    try:
        decisions = query_fund_decisions_fn(fcode, run_id)

        if not decisions:
            print(f"\n⚠️  未找到基金 {fcode} 的筛选记录")
            print("   可能原因：1) 基金代码错误  2) 尚未运行筛选分析")
            return 1

        print(f"\n📋 决策路径 ({len(decisions)} 层):\n")

        for decision in decisions:
            status = "✅ 通过" if decision['passed'] else "❌ 淘汰"
            print(f"  第{decision['step_id']:2d}层 | {decision['step_name']:25s} | {status}")

            if decision['reason_detail']:
                print(f"         | 原因: {decision['reason_detail']}")

            if decision['metrics']:
                metrics_str = ', '.join(
                    f"{key}={value:.4f}" if isinstance(value, float) else f"{key}={value}"
                    for key, value in decision['metrics'].items()
                    if value is not None
                )
                if metrics_str:
                    print(f"         | 指标: {metrics_str}")

        final_decision = decisions[-1]
        print(f"\n{'=' * 60}")
        if final_decision['passed']:
            print(f"🎉 最终结果: 通过筛选 ({final_decision['reason_detail']})")
        else:
            print(f"📊 最终结果: 在第{final_decision['step_id']}层被淘汰")
            print(f"   淘汰原因: {final_decision['reason_code']} - {final_decision['reason_detail']}")

        return 0
    except Exception as exc:
        return emit_command_error(f"查询基金 {fcode} 的筛选决策失败", exc)
