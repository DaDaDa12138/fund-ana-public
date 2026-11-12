#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""筛选任务启动引导辅助。"""

from __future__ import annotations

import os
import sys
from pathlib import Path

_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"


def ensure_project_venv(project_root: Path) -> None:
    """
    若当前未运行在项目虚拟环境中，则切换到项目 `.venv/bin/python`。

    目的：
    1. 让 `python3 -m bond_fund_screening.tasks.*` 在无人值守场景下也能稳定启动；
    2. 避免系统解释器缺少 `sqlalchemy` 等核心依赖时直接失败；
    3. 保持脚本入口与 `scripts/*.py` 的自举行为一致。
    """
    venv_python = project_root / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (project_root / ".venv").resolve()
    if current_prefix == target_prefix:
        return

    # 不能对 venv 解释器做 resolve()，否则会退化成 /usr/bin/python3.x，
    # Python 将丢失 pyvenv.cfg 语义并可能反复自举重进程。
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return

    os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
    os.execv(str(venv_python), [str(venv_python), *sys.argv])
