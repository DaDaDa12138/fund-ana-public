#!/usr/bin/env python3
"""
债券基金分析项目内部 CLI 入口。

当前项目的正式筛选、发布与前端展示均围绕债券型基金展开；
底层引擎仍保留多基金类型兼容能力，主要用于历史数据维护与调试。
"""

import sys
import argparse
import os
import yaml
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent))

from utils import setup_logger, get_logger
from utils.calculation_capture import capture_session
from database import init_database, check_database_exists, get_database_stats, print_database_stats, DatabaseManager
from database.connection import resolve_db_path
from collectors import FundCollector, BondFundCollector, NavCollector, FundDetailCollector, ManagerCollector, CompanyCollector
from analyzers import RiskAnalyzer
from api import APIClient
from schedulers import TaskScheduler
from cli.aux_commands import (
    cmd_calculate_risk as _aux_cmd_calculate_risk,
    cmd_collect_details as _aux_cmd_collect_details,
    cmd_collect_managers as _aux_cmd_collect_managers,
    cmd_collect_companies as _aux_cmd_collect_companies,
)
from cli.collection_commands import (
    cmd_collect_funds as _collection_cmd_collect_funds,
    cmd_collect_period_returns as _collection_cmd_collect_period_returns,
    cmd_collect_nav as _collection_cmd_collect_nav,
    collect_nav_with_limit as _collection_collect_nav_with_limit,
    cmd_collect_all as _collection_cmd_collect_all,
)
from cli.init_commands import (
    cmd_init_db as _init_cmd_init_db,
)
from cli.runtime_commands import (
    cmd_stats as _runtime_cmd_stats,
    cmd_scheduler as _runtime_cmd_scheduler,
    cmd_run_task as _runtime_cmd_run_task,
    cmd_query_decision as _runtime_cmd_query_decision,
)
from cli.parser_registry import (
    register_data_collection_commands as _parser_register_data_collection_commands,
    register_runtime_commands as _parser_register_runtime_commands,
    build_collection_command_handlers as _parser_build_collection_command_handlers,
    build_runtime_command_handlers as _parser_build_runtime_command_handlers,
    build_main_parser as _parser_build_main_parser,
    build_command_handlers as _parser_build_command_handlers,
)
from cli.dispatcher import (
    dispatch_command as _dispatcher_dispatch_command,
)
from cli.bootstrap import (
    load_config as _bootstrap_load_config,
    bootstrap_main_runtime as _bootstrap_main_runtime,
)
from cli.common import (
    get_configured_db_path as _common_get_configured_db_path,
    ensure_database_initialized as _common_ensure_database_initialized,
    emit_command_error as _common_emit_command_error,
    update_collection_task_history as _common_update_collection_task_history,
)
from cli.dependency_factory import (
    build_init_command_dependencies as _factory_build_init_command_dependencies,
    build_collection_command_dependencies as _factory_build_collection_command_dependencies,
    build_aux_command_dependencies as _factory_build_aux_command_dependencies,
    build_runtime_command_dependencies as _factory_build_runtime_command_dependencies,
    build_query_command_dependencies as _factory_build_query_command_dependencies,
)
from utils.project_runtime_config import (
    get_project_config_path as _runtime_project_config_path,
    load_runtime_config as _load_runtime_config,
)

# 导入基金类型注册表（用于CLI类型选项）
try:
    from config.fund_type_registry import CLI_TYPE_CHOICES, CLI_TYPE_HELP, cli_type_to_fund_types
    HAS_REGISTRY = True
except ImportError:
    CLI_TYPE_CHOICES = ['stock', 'index', 'hybrid', 'bond', 'money', 'qdii', 'fof', 'etf', 'lof', 'all']
    CLI_TYPE_HELP = ""
    cli_type_to_fund_types = None
    HAS_REGISTRY = False

logger = None


def load_config():
    """加载配置文件"""
    config_path = _get_default_config_path()
    _bootstrap_load_config(
        config_path,
        yaml_module=yaml,
        print_fn=print,
    )
    return _load_runtime_config(config_path)


def _get_default_config_path():
    """返回默认配置文件路径。"""
    return _runtime_project_config_path()


def _get_configured_db_path(config):
    """统一解析运行期数据库路径。"""
    return _common_get_configured_db_path(
        config,
        resolve_db_path_fn=resolve_db_path,
    )


def _ensure_database_initialized(config):
    """校验数据库已初始化。"""
    return _common_ensure_database_initialized(
        config,
        get_configured_db_path_fn=_get_configured_db_path,
        check_database_exists_fn=check_database_exists,
        emit_command_error_fn=_emit_command_error,
        print_fn=print,
    )


def _emit_command_error(message, exc):
    """统一输出命令层异常，避免直接抛 traceback 到用户界面。"""
    return _common_emit_command_error(
        message,
        exc,
        logger_obj=logger,
        print_fn=print,
    )


def _update_collection_task_history(success_count, total_count, failed_count, inserted_count):
    """统一更新采集任务历史。"""
    return _common_update_collection_task_history(
        success_count=success_count,
        total_count=total_count,
        failed_count=failed_count,
        inserted_count=inserted_count,
    )


def _build_collection_command_dependencies():
    """构建采集命令依赖。"""
    return _factory_build_collection_command_dependencies(
        logger=logger,
        ensure_database_initialized_fn=_ensure_database_initialized,
        emit_command_error_fn=_emit_command_error,
        update_collection_task_history_fn=_update_collection_task_history,
        get_configured_db_path_fn=_get_configured_db_path,
        check_database_exists_fn=check_database_exists,
        init_db_cmd=cmd_init_db,
        collect_funds_cmd=cmd_collect_funds,
        collect_nav_cmd=cmd_collect_nav,
        stats_cmd=cmd_stats,
        collect_nav_with_limit_fn=_collect_nav_with_limit,
        namespace_cls=argparse.Namespace,
        fund_collector_cls=FundCollector,
        nav_collector_cls=NavCollector,
    )


def _build_aux_command_dependencies():
    """构建辅助命令依赖。"""
    return _factory_build_aux_command_dependencies(
        logger=logger,
        ensure_database_initialized_fn=_ensure_database_initialized,
        emit_command_error_fn=_emit_command_error,
        analyzer_cls=RiskAnalyzer,
        database_manager_cls=DatabaseManager,
        detail_collector_cls=FundDetailCollector,
        manager_collector_cls=ManagerCollector,
        company_collector_cls=CompanyCollector,
    )


def _build_runtime_command_dependencies():
    """构建运行态命令依赖。"""
    return _factory_build_runtime_command_dependencies(
        logger=logger,
        ensure_database_initialized_fn=_ensure_database_initialized,
        emit_command_error_fn=_emit_command_error,
        get_configured_db_path_fn=_get_configured_db_path,
        check_database_exists_fn=check_database_exists,
        print_database_stats_fn=print_database_stats,
        api_client_cls=APIClient,
        database_manager_cls=DatabaseManager,
        scheduler_cls=TaskScheduler,
    )


def cmd_init_db(args, config):
    """初始化数据库"""
    dependencies = _factory_build_init_command_dependencies(
        logger=logger,
        get_configured_db_path_fn=_get_configured_db_path,
        emit_command_error_fn=_emit_command_error,
        init_database_fn=init_database,
        print_database_stats_fn=print_database_stats,
    )
    return _init_cmd_init_db(
        args,
        config,
        **dependencies,
    )


def cmd_collect_funds(args, config):
    """采集基金列表"""
    dependencies = _build_collection_command_dependencies()
    return _collection_cmd_collect_funds(
        args,
        config,
        **dependencies['collect_funds'],
    )


def cmd_collect_period_returns(args, config):
    from collectors.period_return_collector import PeriodReturnCollector
    dependencies = dict(_build_collection_command_dependencies()['collect_funds'])
    dependencies['fund_collector_cls'] = PeriodReturnCollector
    return _collection_cmd_collect_period_returns(args, config, **dependencies)


def cmd_collect_nav(args, config):
    """采集历史净值"""
    dependencies = _build_collection_command_dependencies()
    return _collection_cmd_collect_nav(
        args,
        config,
        **dependencies['collect_nav'],
    )


def _collect_nav_with_limit(config, limit):
    """限制模式下仅采集前 N 只基金的净值。"""
    dependencies = _build_collection_command_dependencies()
    return _collection_collect_nav_with_limit(
        config,
        limit,
        **dependencies['collect_nav_with_limit'],
    )


def cmd_stats(args, config):
    """显示统计信息"""
    dependencies = _build_runtime_command_dependencies()
    return _runtime_cmd_stats(
        args,
        config,
        **dependencies['stats'],
    )


@capture_session('database_risk')
def cmd_calculate_risk(args, config):
    """计算风险指标"""
    dependencies = _build_aux_command_dependencies()
    return _aux_cmd_calculate_risk(
        args,
        config,
        **dependencies['calculate_risk'],
    )


def cmd_collect_details(args, config):
    """采集基金详情"""
    dependencies = _build_aux_command_dependencies()
    return _aux_cmd_collect_details(
        args,
        config,
        **dependencies['collect_details'],
    )


def cmd_collect_managers(args, config):
    """采集基金经理"""
    dependencies = _build_aux_command_dependencies()
    return _aux_cmd_collect_managers(
        args,
        config,
        **dependencies['collect_managers'],
    )


def cmd_collect_companies(args, config):
    """采集基金公司"""
    dependencies = _build_aux_command_dependencies()
    return _aux_cmd_collect_companies(
        args,
        config,
        **dependencies['collect_companies'],
    )


def cmd_scheduler(args, config):
    """启动调度器守护进程"""
    dependencies = _build_runtime_command_dependencies()
    return _runtime_cmd_scheduler(
        args,
        config,
        **dependencies['scheduler'],
    )


def cmd_run_task(args, config):
    """手动执行单个调度任务"""
    dependencies = _build_runtime_command_dependencies()
    return _runtime_cmd_run_task(
        args,
        config,
        **dependencies['run_task'],
    )


def cmd_collect_all(args, config):
    """一键采集所有数据"""
    dependencies = _build_collection_command_dependencies()
    return _collection_cmd_collect_all(
        args,
        config,
        **dependencies['collect_all'],
    )


def cmd_query_decision(args, config):
    """查询基金筛选决策路径"""
    from bond_fund_screening.core.screening_tracker import query_fund_decisions

    dependencies = _factory_build_query_command_dependencies(
        emit_command_error_fn=_emit_command_error,
        query_fund_decisions_fn=query_fund_decisions,
    )
    return _runtime_cmd_query_decision(
        args,
        config,
        **dependencies,
    )


def _register_data_collection_commands(subparsers):
    """注册数据采集与分析相关命令。"""
    return _parser_register_data_collection_commands(
        subparsers,
        cli_type_choices=CLI_TYPE_CHOICES,
    )


def _register_runtime_commands(subparsers):
    """注册运行态与查询相关命令。"""
    return _parser_register_runtime_commands(subparsers)


def _build_collection_command_handlers():
    """构建数据采集与分析相关命令映射。"""
    return _parser_build_collection_command_handlers(
        cmd_init_db=cmd_init_db,
        cmd_collect_funds=cmd_collect_funds,
        cmd_collect_period_returns=cmd_collect_period_returns,
        cmd_collect_nav=cmd_collect_nav,
        cmd_calculate_risk=cmd_calculate_risk,
        cmd_collect_details=cmd_collect_details,
        cmd_collect_managers=cmd_collect_managers,
        cmd_collect_companies=cmd_collect_companies,
        cmd_collect_all=cmd_collect_all,
    )


def _build_runtime_command_handlers():
    """构建运行态与查询相关命令映射。"""
    return _parser_build_runtime_command_handlers(
        cmd_scheduler=cmd_scheduler,
        cmd_run_task=cmd_run_task,
        cmd_stats=cmd_stats,
        cmd_query_decision=cmd_query_decision,
    )


def _build_main_parser():
    """构建 CLI 参数解析器。"""
    return _parser_build_main_parser(
        cli_type_help=CLI_TYPE_HELP,
        has_registry=HAS_REGISTRY,
        register_data_collection_commands_fn=_register_data_collection_commands,
        register_runtime_commands_fn=_register_runtime_commands,
    )


def _build_command_handlers():
    """构建命令到处理函数的映射。"""
    return _parser_build_command_handlers(
        build_collection_command_handlers_fn=_build_collection_command_handlers,
        build_runtime_command_handlers_fn=_build_runtime_command_handlers,
    )


def _run_main_command(args, parser):
    """Initialize and execute a parsed command, preserving interactive input."""
    global logger

    # 加载配置并初始化日志
    config, logger, bootstrap_error = _bootstrap_main_runtime(
        load_config_fn=load_config,
        setup_logger_fn=setup_logger,
        print_fn=print,
    )
    if bootstrap_error != 0:
        return bootstrap_error

    # 如果没有命令，显示帮助
    if not args.command:
        parser.print_help()
        return 1

    return _dispatcher_dispatch_command(
        args,
        config,
        build_command_handlers_fn=_build_command_handlers,
        logger_obj=logger,
        emit_command_error_fn=_emit_command_error,
    )


def main():
    """主函数；直接风险CLI也自动保留当次输入和执行终态。"""
    parser = _build_main_parser()
    args = parser.parse_args()
    if args.command == "calculate-risk" and not os.environ.get("FUND_ANA_CALCULATION_INPUT_DIR", "").strip():
        project_root = Path(__file__).resolve().parent.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
        from backend.standalone_calculation_capture import run_standalone_risk

        return run_standalone_risk(lambda: _run_main_command(args, parser))
    return _run_main_command(args, parser)


if __name__ == '__main__':
    sys.exit(main())
