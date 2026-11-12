#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI 命令分发。"""


def dispatch_command(args, config, *, build_command_handlers_fn, logger_obj, emit_command_error_fn):
    """分发命令执行。"""
    try:
        commands = build_command_handlers_fn()
    except Exception as exc:
        return emit_command_error_fn("构建命令映射失败", exc)

    cmd_func = commands.get(args.command)
    if not cmd_func:
        if logger_obj is not None:
            logger_obj.error("未知命令: %s", args.command)
        return 1

    try:
        return cmd_func(args, config)
    except Exception as exc:
        return emit_command_error_fn(f"执行命令 {args.command} 失败", exc)
