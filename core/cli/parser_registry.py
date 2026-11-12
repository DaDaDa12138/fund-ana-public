#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI 参数注册与命令映射组装。"""

import argparse


def register_data_collection_commands(subparsers, *, cli_type_choices):
    """注册数据采集与分析相关命令。"""
    parser_init = subparsers.add_parser('init-db', help='初始化数据库')
    parser_init.add_argument('--drop', action='store_true', help='删除现有表')

    parser_funds = subparsers.add_parser('collect-funds', help='采集基金列表（默认采集全量）')
    parser_funds.add_argument(
        '-t', '--fund-type', dest='fund_type', type=str, default='bond',
        choices=cli_type_choices,
        help='基金类型 (默认: bond). 可选: stock/index/hybrid/bond/money/qdii/fof/etf/lof/all'
    )
    parser_funds.add_argument(
        '--only-buyable', dest='include_non_buyable', action='store_true',
        help='仅采集可购买的基金（默认采集全量）'
    )

    parser_period = subparsers.add_parser('collect-period-returns', help='有界补采缺失或过期的区间收益')
    parser_period.add_argument('--fcodes', help='限定六位基金代码，逗号分隔；仍遵守时效和冷却')
    parser_period.add_argument('--force-refresh', action='store_true', help='仅配合--fcodes显式重试，跳过冷却；日期和数据验证保持')

    parser_nav = subparsers.add_parser('collect-nav', help='采集历史净值')
    parser_nav.add_argument('--incremental', action='store_true', help='增量更新模式')
    parser_nav.add_argument('--fcode', type=str, help='指定基金代码')
    parser_nav.add_argument('--force', action='store_true', help='强制更新')
    parser_nav.add_argument('--workers', type=int, default=3, help='并发数 (default: 3)')
    parser_nav.add_argument('--batch-size', type=int, default=2000, help='每批基金数 (default: 2000)')
    parser_nav.add_argument('--batch-interval', type=int, default=1800, help='批次间隔秒数 (default: 1800)')
    parser_nav.add_argument('--updated-fcodes-output', type=str, help='将本轮新增净值基金代码写入文件')
    parser_nav.add_argument('--yes', action='store_true', help='自动确认')

    parser_risk = subparsers.add_parser('calculate-risk', help='计算风险指标')
    parser_risk.add_argument('--fcode', type=str, help='指定基金代码')
    parser_risk.add_argument('--force', action='store_true', help='强制重新计算')
    parser_risk.add_argument('--changed-nav-only', action='store_true', help='仅重算净值已更新的基金')
    parser_risk.add_argument('--target-fcodes-file', type=str, help='仅在指定基金代码文件范围内判断 changed-nav 候选')
    parser_risk.add_argument('--workers', type=int, default=3, help='并发数 (default: 3)')
    parser_risk.add_argument('--yes', action='store_true', help='自动确认')

    parser_details = subparsers.add_parser('collect-details', help='采集基金详情')
    parser_details.add_argument('--fcode', type=str, help='指定基金代码')
    parser_details.add_argument('--force', action='store_true', help='强制更新')
    parser_details.add_argument(
        '--bypass-empty-detail-cooldown',
        action='store_true',
        help='强制绕过空详情上游冷却，仅用于人工排障',
    )
    parser_details.add_argument(
        '--scale-missing-only',
        action='store_true',
        help='仅补采规模字段为空的基金；日更使用有界队列和短期冷却',
    )
    parser_details.add_argument('--workers', type=int, default=5, help='并发数 (default: 5)')

    parser_managers = subparsers.add_parser('collect-managers', help='采集基金经理')
    parser_managers.add_argument('--force', action='store_true', help='强制更新')
    parser_managers.add_argument('--workers', type=int, default=5, help='并发数 (default: 5)')

    parser_companies = subparsers.add_parser('collect-companies', help='采集基金公司')
    parser_companies.add_argument('--force', action='store_true', help='强制更新')
    parser_companies.add_argument('--workers', type=int, default=None, help='并发数（默认读取项目配置，最多3）')

    parser_all = subparsers.add_parser('collect-all', help='一键采集所有数据')
    parser_all.add_argument('--workers', type=int, default=50, help='并发数 (default: 50)')
    parser_all.add_argument('--batch-size', type=int, default=2000, help='每批基金数 (default: 2000)')
    parser_all.add_argument('--batch-interval', type=int, default=60, help='批次间隔秒数 (default: 60)')
    parser_all.add_argument('--limit', type=int, help='测试模式：限制采集数量')


def register_runtime_commands(subparsers):
    """注册运行态与查询相关命令。"""
    subparsers.add_parser('scheduler', help='启动定时调度器')

    parser_run_task = subparsers.add_parser('run-task', help='手动执行调度任务')
    parser_run_task.add_argument('task', type=str, help='任务名称 (如: daily_nav_update)')

    parser_stats = subparsers.add_parser('stats', help='显示统计信息')
    parser_stats.add_argument('--api', action='store_true', help='显示API统计')

    parser_decision = subparsers.add_parser('query-decision', help='查询基金筛选决策路径')
    parser_decision.add_argument('fcode', type=str, help='基金代码')
    parser_decision.add_argument('--run-id', type=str, help='指定运行ID（默认最近一次）')


def build_collection_command_handlers(
    *,
    cmd_init_db,
    cmd_collect_funds,
    cmd_collect_period_returns,
    cmd_collect_nav,
    cmd_calculate_risk,
    cmd_collect_details,
    cmd_collect_managers,
    cmd_collect_companies,
    cmd_collect_all,
):
    """构建数据采集与分析相关命令映射。"""
    return {
        'init-db': cmd_init_db,
        'collect-funds': cmd_collect_funds,
        'collect-period-returns': cmd_collect_period_returns,
        'collect-nav': cmd_collect_nav,
        'calculate-risk': cmd_calculate_risk,
        'collect-details': cmd_collect_details,
        'collect-managers': cmd_collect_managers,
        'collect-companies': cmd_collect_companies,
        'collect-all': cmd_collect_all,
    }


def build_runtime_command_handlers(
    *,
    cmd_scheduler,
    cmd_run_task,
    cmd_stats,
    cmd_query_decision,
):
    """构建运行态与查询相关命令映射。"""
    return {
        'scheduler': cmd_scheduler,
        'run-task': cmd_run_task,
        'stats': cmd_stats,
        'query-decision': cmd_query_decision,
    }


def build_main_parser(
    *,
    cli_type_help,
    has_registry,
    register_data_collection_commands_fn,
    register_runtime_commands_fn,
):
    """构建 CLI 参数解析器。"""
    parser = argparse.ArgumentParser(
        description='债券基金分析项目内部命令行工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=cli_type_help if has_registry else '',
    )

    subparsers = parser.add_subparsers(dest='command', help='命令')
    register_data_collection_commands_fn(subparsers)
    register_runtime_commands_fn(subparsers)
    return parser


def build_command_handlers(
    *,
    build_collection_command_handlers_fn,
    build_runtime_command_handlers_fn,
):
    """构建命令到处理函数的映射。"""
    handlers = {}
    handlers.update(build_collection_command_handlers_fn())
    handlers.update(build_runtime_command_handlers_fn())
    return handlers
