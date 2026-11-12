#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
项目级运行配置桥接。

目标：
1. 让 core 侧默认读取 config/project.yaml 作为项目运行真源；
2. 保留 core/config/config.yaml 作为兼容壳层，承载尚未迁移的旧配置项；
3. 输出给 core 代码的仍是旧版兼容结构，避免一次性改动过大。
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml


CORE_ROOT = Path(__file__).resolve().parent.parent
PROJECT_ROOT = CORE_ROOT.parent
DEFAULT_PROJECT_CONFIG_PATH = PROJECT_ROOT / "config" / "project.yaml"
DEFAULT_CORE_CONFIG_PATH = CORE_ROOT / "config" / "config.yaml"
_DEFAULT_ALIAS_TEXTS = {
    "",
    ".",
    "config/config.yaml",
    "core/config/config.yaml",
    "config/project.yaml",
}


def _normalize_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _deep_merge_mapping(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(_normalize_mapping(base))
    for key, value in _normalize_mapping(overlay).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_mapping(_normalize_mapping(merged.get(key)), _normalize_mapping(value))
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_load_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return _normalize_mapping(payload)


def _resolve_default_risk_free_rate() -> float:
    from bond_fund_screening.config import RISK_FREE_RATE as default_risk_free_rate

    return float(default_risk_free_rate)


def _resolve_runtime_risk_free_rate() -> tuple[float, str, str | None]:
    from bond_fund_screening.runtime_overrides import get_risk_free_rate

    default_risk_free_rate = _resolve_default_risk_free_rate()
    resolution = get_risk_free_rate(default_risk_free_rate)
    return (
        float(resolution.value),
        str(resolution.source),
        str(resolution.updated_at) if resolution.updated_at else None,
    )


def _resolve_project_path(raw_value: Any) -> str:
    text = _normalize_text(raw_value)
    if not text:
        return ""
    path = Path(text)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return str(path)


def _project_relative_path(raw_value: Any) -> str:
    text = _normalize_text(raw_value)
    if not text:
        return text

    path = Path(text)
    if not path.is_absolute():
        path = (CORE_ROOT / path).resolve()

    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _resolve_requested_config_path(config_path: str | Path | None) -> Path:
    text = _normalize_text(config_path).replace("\\", "/")
    if text in _DEFAULT_ALIAS_TEXTS:
        return DEFAULT_PROJECT_CONFIG_PATH

    path = Path(config_path) if config_path is not None else DEFAULT_PROJECT_CONFIG_PATH
    if path.is_absolute():
        return path.resolve()

    project_candidate = (PROJECT_ROOT / path).resolve()
    core_candidate = (CORE_ROOT / path).resolve()

    if project_candidate == DEFAULT_PROJECT_CONFIG_PATH.resolve():
        return project_candidate
    if core_candidate == DEFAULT_CORE_CONFIG_PATH.resolve():
        return DEFAULT_PROJECT_CONFIG_PATH
    if project_candidate.exists():
        return project_candidate
    return core_candidate


def uses_project_runtime_bridge(config_path: str | Path | None = None) -> bool:
    text = _normalize_text(config_path).replace("\\", "/")
    if text in _DEFAULT_ALIAS_TEXTS:
        return True

    if text:
        name = Path(text).name.lower()
        if name in {"project.yaml", "project.yml"}:
            return True

    resolved = _resolve_requested_config_path(config_path)
    return resolved in {
        DEFAULT_PROJECT_CONFIG_PATH.resolve(),
        DEFAULT_CORE_CONFIG_PATH.resolve(),
    }


def get_project_config_path() -> Path:
    return DEFAULT_PROJECT_CONFIG_PATH


def get_core_compat_config_path() -> Path:
    return DEFAULT_CORE_CONFIG_PATH


def resolve_runtime_config_path(config_path: str | Path | None = None) -> Path:
    return _resolve_requested_config_path(config_path)


def load_project_config_payload(config_path: str | Path | None = None) -> dict[str, Any]:
    resolved = _resolve_requested_config_path(config_path)
    if resolved == DEFAULT_CORE_CONFIG_PATH.resolve():
        resolved = DEFAULT_PROJECT_CONFIG_PATH
    return _safe_load_yaml_mapping(resolved)


def load_core_compat_defaults(config_path: str | Path | None = None) -> dict[str, Any]:
    resolved = Path(config_path).resolve() if config_path and Path(config_path).is_absolute() else DEFAULT_CORE_CONFIG_PATH
    if resolved == DEFAULT_PROJECT_CONFIG_PATH.resolve():
        resolved = DEFAULT_CORE_CONFIG_PATH
    return _safe_load_yaml_mapping(resolved)


def build_core_runtime_config(
    project_payload: dict[str, Any] | None = None,
    compat_defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    基于 project.yaml 生成 core 可直接消费的兼容配置结构。

    兼容壳层仍作为旧字段默认来源，但关键运行参数始终由 project.yaml 覆盖。
    """
    payload = copy.deepcopy(_normalize_mapping(project_payload) or load_project_config_payload())
    config = copy.deepcopy(_normalize_mapping(compat_defaults) or load_core_compat_defaults())

    apis = _normalize_mapping(payload.get("apis"))
    paths = _normalize_mapping(payload.get("paths"))
    scheduler = _normalize_mapping(payload.get("scheduler"))
    workflow = _normalize_mapping(payload.get("workflow"))
    project = _normalize_mapping(payload.get("project"))
    collection = _normalize_mapping(payload.get("collection"))

    api_config = _normalize_mapping(config.setdefault("api", {}))
    tiantian_config = _normalize_mapping(api_config.setdefault("tiantian", {}))
    autostock_config = _normalize_mapping(api_config.setdefault("autostock", {}))
    project_autostock = _normalize_mapping(apis.get("autostock"))
    if "enabled" in project_autostock:
        autostock_config["enabled"] = project_autostock["enabled"] is True
    database_config = _normalize_mapping(config.setdefault("database", {}))
    scheduler_config = _normalize_mapping(config.setdefault("scheduler", {}))

    tiantian_base_url = _normalize_text(apis.get("tiantian_base_url"))
    if tiantian_base_url:
        api_config["base_url"] = tiantian_base_url.rstrip("/")
        tiantian_config["base_url"] = tiantian_base_url.rstrip("/")

    autostock_base_url = _normalize_text(apis.get("autostock_base_url"))
    if autostock_base_url:
        autostock_config["base_url"] = autostock_base_url.rstrip("/")

    database_path = _resolve_project_path(paths.get("database_path"))
    if database_path:
        database_config["path"] = database_path

    timezone = _normalize_text(scheduler.get("timezone")) or _normalize_text(project.get("timezone"))
    if timezone:
        scheduler_config["timezone"] = timezone
    if scheduler.get("enabled") is not None:
        scheduler_config["enabled"] = bool(scheduler.get("enabled"))
    for nested_key in ("tasks", "schedules", "retry", "timeouts", "cleanup"):
        nested_value = _normalize_mapping(scheduler.get(nested_key))
        if nested_value:
            scheduler_config[nested_key] = copy.deepcopy(nested_value)

    services_config = _normalize_mapping(config.setdefault("services", {}))
    if tiantian_base_url:
        services_config["tiantian_api_url"] = tiantian_base_url.rstrip("/")

    if collection:
        config["collection"] = _deep_merge_mapping(
            _normalize_mapping(config.get("collection")),
            collection,
        )

    exclude_holding_days_gt = workflow.get("exclude_holding_days_gt")
    if exclude_holding_days_gt is not None:
        config["exclude_holding_days_gt"] = exclude_holding_days_gt
        risk_metrics_config = _normalize_mapping(config.setdefault("risk_metrics", {}))
        risk_metrics_config["skip_risk_metrics_for_holding_days_gt"] = exclude_holding_days_gt

    risk_metrics_config = _normalize_mapping(config.setdefault("risk_metrics", {}))
    risk_free_rate_value, risk_free_rate_source, risk_free_rate_updated_at = _resolve_runtime_risk_free_rate()
    risk_metrics_config["risk_free_rate"] = risk_free_rate_value
    risk_metrics_config["risk_free_rate_source"] = risk_free_rate_source
    risk_metrics_config["risk_free_rate_updated_at"] = risk_free_rate_updated_at
    risk_metrics_config["risk_free_rate_baseline"] = _resolve_default_risk_free_rate()

    return config


def load_runtime_config(config_path: str | Path | None = None) -> dict[str, Any]:
    resolved = _resolve_requested_config_path(config_path)
    if uses_project_runtime_bridge(resolved):
        return build_core_runtime_config(
            project_payload=load_project_config_payload(resolved),
            compat_defaults=load_core_compat_defaults(),
        )
    return _safe_load_yaml_mapping(resolved)


def project_payload_from_core_config(
    core_config: dict[str, Any],
    existing_project_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    从 core 兼容配置中提取仍需回写 project.yaml 的关键运行字段。
    """
    project_payload = copy.deepcopy(
        _normalize_mapping(existing_project_payload) or load_project_config_payload()
    )
    config = _normalize_mapping(core_config)
    api_config = _normalize_mapping(config.get("api"))
    tiantian_config = _normalize_mapping(api_config.get("tiantian"))
    autostock_config = _normalize_mapping(api_config.get("autostock"))
    database_config = _normalize_mapping(config.get("database"))
    scheduler_config = _normalize_mapping(config.get("scheduler"))

    apis = _normalize_mapping(project_payload.setdefault("apis", {}))
    paths = _normalize_mapping(project_payload.setdefault("paths", {}))
    scheduler = _normalize_mapping(project_payload.setdefault("scheduler", {}))

    tiantian_base_url = _normalize_text(
        tiantian_config.get("base_url") or api_config.get("base_url")
    )
    if tiantian_base_url:
        apis["tiantian_base_url"] = tiantian_base_url.rstrip("/")

    autostock_base_url = _normalize_text(autostock_config.get("base_url"))
    if autostock_base_url:
        apis["autostock_base_url"] = autostock_base_url.rstrip("/")

    database_path = _normalize_text(database_config.get("path"))
    if database_path:
        paths["database_path"] = _project_relative_path(database_path)

    if scheduler_config.get("enabled") is not None:
        scheduler["enabled"] = bool(scheduler_config.get("enabled"))
    timezone = _normalize_text(scheduler_config.get("timezone"))
    if timezone:
        scheduler["timezone"] = timezone
    for nested_key in ("tasks", "schedules", "retry", "timeouts", "cleanup"):
        nested_value = _normalize_mapping(scheduler_config.get(nested_key))
        if nested_value:
            scheduler[nested_key] = copy.deepcopy(nested_value)

    return project_payload
