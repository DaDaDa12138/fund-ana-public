#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI 启动引导与配置加载。"""


def load_config(config_path, *, yaml_module, print_fn):
    """从指定路径加载配置。"""
    if not config_path.exists():
        print_fn(f"⚠️  配置文件不存在: {config_path}")
        return {}

    try:
        with open(config_path, 'r', encoding='utf-8') as handle:
            payload = yaml_module.safe_load(handle)
    except yaml_module.YAMLError as exc:
        print_fn(f"❌ 配置文件解析失败: {config_path} ({exc})")
        return None
    except OSError as exc:
        print_fn(f"❌ 配置文件读取失败: {config_path} ({exc})")
        return None

    if payload is None:
        return {}

    if not isinstance(payload, dict):
        print_fn(f"❌ 配置文件格式错误: {config_path} 顶层必须是映射")
        return None

    return payload


def bootstrap_main_runtime(*, load_config_fn, setup_logger_fn, print_fn):
    """初始化主入口运行时。"""
    config = load_config_fn()
    if config is None:
        return None, None, 1

    try:
        logger = setup_logger_fn(config=config)
    except Exception as exc:
        print_fn(f"❌ 日志系统初始化失败: {exc}")
        return config, None, 1

    return config, logger, 0
