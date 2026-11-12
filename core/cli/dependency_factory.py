#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI 命令依赖装配工厂。"""


def build_init_command_dependencies(
    *,
    logger,
    get_configured_db_path_fn,
    emit_command_error_fn,
    init_database_fn,
    print_database_stats_fn,
):
    """构建 init-db 命令依赖。"""
    return {
        'logger': logger,
        'get_configured_db_path': get_configured_db_path_fn,
        'emit_command_error': emit_command_error_fn,
        'init_database_fn': init_database_fn,
        'print_database_stats_fn': print_database_stats_fn,
    }


def build_collection_command_dependencies(
    *,
    logger,
    ensure_database_initialized_fn,
    emit_command_error_fn,
    update_collection_task_history_fn,
    get_configured_db_path_fn,
    check_database_exists_fn,
    init_db_cmd,
    collect_funds_cmd,
    collect_nav_cmd,
    stats_cmd,
    collect_nav_with_limit_fn,
    namespace_cls,
    fund_collector_cls,
    nav_collector_cls,
):
    """构建采集类命令依赖。"""
    common = {
        'logger': logger,
        'ensure_database_initialized': ensure_database_initialized_fn,
        'emit_command_error': emit_command_error_fn,
    }
    return {
        'collect_funds': {
            **common,
            'fund_collector_cls': fund_collector_cls,
        },
        'collect_nav': {
            **common,
            'update_collection_task_history': update_collection_task_history_fn,
            'nav_collector_cls': nav_collector_cls,
        },
        'collect_nav_with_limit': {
            'logger': logger,
            'ensure_database_initialized': ensure_database_initialized_fn,
            'update_collection_task_history': update_collection_task_history_fn,
            'nav_collector_cls': nav_collector_cls,
            'emit_command_error': emit_command_error_fn,
        },
        'collect_all': {
            'logger': logger,
            'get_configured_db_path': get_configured_db_path_fn,
            'check_database_exists': check_database_exists_fn,
            'init_db_cmd': init_db_cmd,
            'collect_funds_cmd': collect_funds_cmd,
            'collect_nav_cmd': collect_nav_cmd,
            'stats_cmd': stats_cmd,
            'collect_nav_with_limit_fn': collect_nav_with_limit_fn,
            'emit_command_error': emit_command_error_fn,
            'namespace_cls': namespace_cls,
        },
    }


def build_aux_command_dependencies(
    *,
    logger,
    ensure_database_initialized_fn,
    emit_command_error_fn,
    analyzer_cls,
    database_manager_cls,
    detail_collector_cls,
    manager_collector_cls,
    company_collector_cls,
):
    """构建辅助命令依赖。"""
    common = {
        'logger': logger,
        'ensure_database_initialized': ensure_database_initialized_fn,
        'emit_command_error': emit_command_error_fn,
    }
    return {
        'calculate_risk': {
            **common,
            'analyzer_cls': analyzer_cls,
            'database_manager_cls': database_manager_cls,
        },
        'collect_details': {
            **common,
            'collector_cls': detail_collector_cls,
        },
        'collect_managers': {
            **common,
            'collector_cls': manager_collector_cls,
        },
        'collect_companies': {
            **common,
            'collector_cls': company_collector_cls,
        },
    }


def build_runtime_command_dependencies(
    *,
    logger,
    ensure_database_initialized_fn,
    emit_command_error_fn,
    get_configured_db_path_fn,
    check_database_exists_fn,
    print_database_stats_fn,
    api_client_cls,
    database_manager_cls,
    scheduler_cls,
):
    """构建运行态命令依赖。"""
    common = {
        'logger': logger,
        'ensure_database_initialized': ensure_database_initialized_fn,
        'emit_command_error': emit_command_error_fn,
    }
    return {
        'stats': {
            'get_configured_db_path': get_configured_db_path_fn,
            'check_database_exists': check_database_exists_fn,
            'print_database_stats': print_database_stats_fn,
            'api_client_cls': api_client_cls,
            'emit_command_error': emit_command_error_fn,
        },
        'scheduler': {
            **common,
            'database_manager_cls': database_manager_cls,
            'api_client_cls': api_client_cls,
            'scheduler_cls': scheduler_cls,
        },
        'run_task': {
            **common,
            'database_manager_cls': database_manager_cls,
            'api_client_cls': api_client_cls,
            'scheduler_cls': scheduler_cls,
        },
    }


def build_query_command_dependencies(
    *,
    emit_command_error_fn,
    query_fund_decisions_fn,
):
    """构建查询类命令依赖。"""
    return {
        'emit_command_error': emit_command_error_fn,
        'query_fund_decisions_fn': query_fund_decisions_fn,
    }
