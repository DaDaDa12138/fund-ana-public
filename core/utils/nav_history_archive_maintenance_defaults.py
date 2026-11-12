#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAV 历史归档维护默认值。
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml
from utils.project_runtime_config import get_project_config_path


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_CONFIG_PATH = get_project_config_path()
DEFAULT_ARCHIVE_TARGET_DIR = "backend/data/archives"
DEFAULT_ARCHIVE_TABLE_NAME = "fund_nav_history_archive"
DEFAULT_VACUUM_DECISION = "defer_to_followup_window"
DEFAULT_ARCHIVE_FILE_PREFIX = "fund_nav_history_before_"
DEFAULT_OPERATOR_ASSIGNMENTS = {
    "maintenance_owner": "bond-maintenance-owner",
    "db_operator": "bond-db-operator",
    "data_reviewer": "bond-data-reviewer",
    "rollback_owner": "bond-rollback-owner",
}


def _normalize_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def _normalize_text(value) -> str:
    return str(value or "").strip()


def _normalize_path_text(value) -> str:
    text = _normalize_text(value).replace("\\", "/")
    return text.rstrip("/")


def normalize_nav_history_archive_maintenance_defaults(value=None) -> dict:
    """标准化 NAV 历史归档维护默认值。"""
    payload = _normalize_dict(value)
    operator_assignments_payload = _normalize_dict(payload.get("operator_assignments"))

    operator_assignments = {}
    for role, default_assignee in DEFAULT_OPERATOR_ASSIGNMENTS.items():
        operator_assignments[role] = (
            _normalize_text(operator_assignments_payload.get(role)) or default_assignee
        )

    return {
        "archive_target_dir": _normalize_path_text(payload.get("archive_target_dir"))
        or DEFAULT_ARCHIVE_TARGET_DIR,
        "archive_table_name": _normalize_text(payload.get("archive_table_name"))
        or DEFAULT_ARCHIVE_TABLE_NAME,
        "vacuum_decision_default": _normalize_text(payload.get("vacuum_decision_default"))
        or DEFAULT_VACUUM_DECISION,
        "operator_assignments": operator_assignments,
    }


@lru_cache(maxsize=4)
def load_nav_history_archive_maintenance_defaults(config_path: Optional[str] = None) -> dict:
    """从配置文件读取 NAV 历史归档维护默认值。"""
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()

    if not path.exists():
        return normalize_nav_history_archive_maintenance_defaults()

    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return normalize_nav_history_archive_maintenance_defaults()

    config = _normalize_dict(loaded)
    database_config = _normalize_dict(config.get("database"))
    defaults_payload = _normalize_dict(database_config.get("nav_history_archive_maintenance"))
    return normalize_nav_history_archive_maintenance_defaults(defaults_payload)


def clear_nav_history_archive_maintenance_defaults_cache():
    """清空 NAV 历史归档维护默认值缓存。"""
    load_nav_history_archive_maintenance_defaults.cache_clear()


def build_nav_history_archive_target_path(
    archive_cutoff_date: Optional[str],
    maintenance_defaults=None,
) -> str:
    """基于 cutoff 与维护默认值构造归档目标路径。"""
    cutoff_date = _normalize_text(archive_cutoff_date)
    defaults = normalize_nav_history_archive_maintenance_defaults(maintenance_defaults)
    archive_target_dir = defaults.get("archive_target_dir") or DEFAULT_ARCHIVE_TARGET_DIR
    if not cutoff_date:
        return ""
    return f"{archive_target_dir}/{DEFAULT_ARCHIVE_FILE_PREFIX}{cutoff_date}.db"
