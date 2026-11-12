#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金运行时排除规则。

当前主要用于隔离已高置信确认的“非基金命中”样本，避免继续污染筛选、
补源和自动补全链路，同时保留原始数据库记录便于后续人工复核。
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import not_, true

from utils.fund_code_alias_audit import load_alias_review_override_map


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = PROJECT_ROOT.parent
DEFAULT_RULES_PATH = PROJECT_ROOT / "config" / "fund_exclusion_rules.json"
DEFAULT_TERMINAL_UPSTREAM_LIMITED_RULES_PATH = (
    REPO_ROOT / "backend" / "data" / "long_term_terminal_upstream_exclusions.json"
)
RULE_GROUP_LABELS = {
    "non_fund_high_confidence": "高置信非基金命中",
    "data_anomaly_high_confidence": "高置信数据异常排除",
    "alias_polluted_source_high_confidence": "人工复核高置信污染源",
    "terminal_upstream_limited_high_confidence": "终态上游受限排除",
}
DEFAULT_RUNTIME_GROUPS = (
    "non_fund_high_confidence",
    "data_anomaly_high_confidence",
    "terminal_upstream_limited_high_confidence",
)


@lru_cache(maxsize=4)
def load_fund_exclusion_rules(config_path: Optional[str] = None) -> Dict[str, List[dict]]:
    """加载基金排除规则配置。"""
    path = Path(config_path) if config_path else DEFAULT_RULES_PATH
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()

    if not path.exists():
        return {}

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return payload


def clear_fund_exclusion_rules_cache():
    """清空规则缓存，供测试或手工刷新使用。"""
    load_fund_exclusion_rules.cache_clear()
    _load_alias_polluted_source_entries.cache_clear()
    _load_terminal_upstream_limited_entries.cache_clear()


def _normalize_entry_list(entries: Iterable[dict]) -> List[dict]:
    normalized = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        fcode = str(entry.get("fcode") or "").strip()
        if not fcode:
            continue
        normalized.append({
            **entry,
            "fcode": fcode,
            "enabled": bool(entry.get("enabled", True)),
        })
    return normalized


@lru_cache(maxsize=4)
def _load_alias_polluted_source_entries(
    override_path: Optional[str] = None,
) -> Tuple[dict, ...]:
    """读取人工复核确认的高置信污染源代码。"""
    override_map = load_alias_review_override_map(Path(override_path) if override_path else None)
    entries = []
    for fcode, item in sorted(override_map.items()):
        recommended_disposition = str(item.get("recommended_disposition") or "").strip()
        if recommended_disposition != "likely_false_alias_or_polluted_source":
            continue
        entries.append({
            "fcode": fcode,
            "shortname": str(item.get("shortname") or "").strip(),
            "canonical_fcode": str(item.get("canonical_fcode") or "").strip() or None,
            "classification": str(item.get("classification") or "").strip() or None,
            "recommended_disposition": recommended_disposition,
            "reason": str(item.get("reason") or "").strip() or None,
            "note": str(item.get("note") or "").strip() or None,
            "enabled": True,
            "source": str(item.get("evidence_artifact") or "").strip() or None,
        })
    return tuple(entries)


def get_alias_polluted_source_entries(
    override_path: Optional[str] = None,
) -> List[dict]:
    """获取人工复核确认的高置信污染源排除项。"""
    return [dict(entry) for entry in _load_alias_polluted_source_entries(override_path)]


@lru_cache(maxsize=4)
def _load_terminal_upstream_limited_entries(
    override_path: Optional[str] = None,
) -> Tuple[dict, ...]:
    """读取由长周期维护自动产出的终态上游受限排除项。"""
    path = Path(override_path) if override_path else DEFAULT_TERMINAL_UPSTREAM_LIMITED_RULES_PATH
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    if not path.exists():
        return tuple()

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return tuple()

    raw_entries = payload.get("entries") if isinstance(payload, dict) else payload
    if not isinstance(raw_entries, list):
        return tuple()
    return tuple(_normalize_entry_list(raw_entries))


def get_terminal_upstream_limited_entries(
    override_path: Optional[str] = None,
) -> List[dict]:
    """获取长周期维护自动产出的终态上游受限排除项。"""
    return [dict(entry) for entry in _load_terminal_upstream_limited_entries(override_path)]


def _merge_unique_exclusion_entries(*entry_groups: Iterable[dict]) -> List[dict]:
    """按 fcode 合并排除项，避免重复代码。"""
    merged: Dict[str, dict] = {}
    for group in entry_groups:
        for entry in group or []:
            normalized = _normalize_entry_list([entry])
            if not normalized:
                continue
            current = normalized[0]
            merged[current["fcode"]] = {
                **merged.get(current["fcode"], {}),
                **current,
            }
    return [merged[fcode] for fcode in sorted(merged.keys())]


def _should_include_alias_polluted_source(
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
) -> bool:
    """
    判断是否应自动并入 alias 污染源排除项。

    规则：
    - 正常运行环境（未显式传入 config_path）默认并入；
    - 使用临时/自定义 config_path 的测试与工具，只有显式给出 alias_override_path 才并入，
      避免把真实工作区覆写规则意外带入隔离场景。
    """
    return alias_override_path is not None or config_path is None


def _should_include_terminal_upstream_limited(
    config_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
) -> bool:
    """判断是否应自动并入终态上游受限排除项。"""
    return terminal_override_path is not None or config_path is None


def get_fund_exclusion_rule_groups(
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
) -> List[dict]:
    """获取全部排除规则分组，保留启用状态供只读展示使用。"""
    rules = load_fund_exclusion_rules(config_path)
    groups = []

    for group_name in sorted(rules.keys()):
        entries = _normalize_entry_list(rules.get(group_name, []))
        if not entries:
            continue

        enabled_count = sum(1 for entry in entries if entry.get("enabled", True))
        groups.append({
            "group": group_name,
            "label": RULE_GROUP_LABELS.get(group_name, group_name),
            "total_count": len(entries),
            "enabled_count": enabled_count,
            "disabled_count": len(entries) - enabled_count,
            "entries": entries,
        })

    alias_entries = []
    if _should_include_alias_polluted_source(config_path=config_path, alias_override_path=alias_override_path):
        alias_entries = get_alias_polluted_source_entries(alias_override_path)
    if alias_entries:
        groups.append({
            "group": "alias_polluted_source_high_confidence",
            "label": RULE_GROUP_LABELS["alias_polluted_source_high_confidence"],
            "total_count": len(alias_entries),
            "enabled_count": len(alias_entries),
            "disabled_count": 0,
            "entries": alias_entries,
        })

    terminal_entries = []
    if _should_include_terminal_upstream_limited(
        config_path=config_path,
        terminal_override_path=terminal_override_path,
    ):
        terminal_entries = get_terminal_upstream_limited_entries(terminal_override_path)
    if terminal_entries:
        enabled_count = sum(1 for entry in terminal_entries if entry.get("enabled", True))
        groups.append({
            "group": "terminal_upstream_limited_high_confidence",
            "label": RULE_GROUP_LABELS["terminal_upstream_limited_high_confidence"],
            "total_count": len(terminal_entries),
            "enabled_count": enabled_count,
            "disabled_count": len(terminal_entries) - enabled_count,
            "entries": terminal_entries,
        })

    return groups


def get_excluded_fund_entries(
    group: str = "non_fund_high_confidence",
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
) -> List[dict]:
    """获取启用中的排除项。"""
    if group == "alias_polluted_source_high_confidence":
        return get_alias_polluted_source_entries(alias_override_path)
    if group == "terminal_upstream_limited_high_confidence":
        return [
            entry
            for entry in get_terminal_upstream_limited_entries(terminal_override_path)
            if entry.get("enabled", True)
        ]

    rules = load_fund_exclusion_rules(config_path)
    entries = _normalize_entry_list(rules.get(group, []))
    return [entry for entry in entries if entry.get("enabled", True)]


def get_runtime_excluded_fund_codes(
    group: str = "non_fund_high_confidence",
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
) -> Tuple[str, ...]:
    """获取运行时排除的基金代码集合。"""
    if group == "non_fund_high_confidence":
        entries = _merge_unique_exclusion_entries(
            *(
                get_excluded_fund_entries(
                    group=runtime_group,
                    config_path=config_path,
                    alias_override_path=alias_override_path,
                    terminal_override_path=terminal_override_path,
                )
                for runtime_group in DEFAULT_RUNTIME_GROUPS
            )
        )
        if _should_include_alias_polluted_source(
            config_path=config_path,
            alias_override_path=alias_override_path,
        ):
            entries = _merge_unique_exclusion_entries(
                entries,
                get_alias_polluted_source_entries(alias_override_path),
            )
    else:
        entries = get_excluded_fund_entries(
            group=group,
            config_path=config_path,
            alias_override_path=alias_override_path,
            terminal_override_path=terminal_override_path,
        )
    codes = sorted({entry["fcode"] for entry in entries})
    return tuple(codes)


def is_runtime_excluded_fund(
    fcode: Optional[str],
    group: str = "non_fund_high_confidence",
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
) -> bool:
    """判断某个基金代码是否处于运行时排除名单。"""
    normalized = str(fcode or "").strip()
    if not normalized:
        return False
    return normalized in set(
        get_runtime_excluded_fund_codes(
            group=group,
            config_path=config_path,
            alias_override_path=alias_override_path,
            terminal_override_path=terminal_override_path,
        )
    )


def build_sqlalchemy_exclusion_filter(
    model,
    group: str = "non_fund_high_confidence",
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
):
    """构建 SQLAlchemy 排除条件。"""
    codes = get_runtime_excluded_fund_codes(
        group=group,
        config_path=config_path,
        alias_override_path=alias_override_path,
        terminal_override_path=terminal_override_path,
    )
    if not codes:
        return true()
    return not_(model.fcode.in_(codes))


def build_sql_exclusion_clause(
    column_name: str,
    group: str = "non_fund_high_confidence",
    config_path: Optional[str] = None,
    alias_override_path: Optional[str] = None,
    terminal_override_path: Optional[str] = None,
) -> Tuple[str, tuple]:
    """构建原始 SQL 排除条件片段及参数。"""
    codes = get_runtime_excluded_fund_codes(
        group=group,
        config_path=config_path,
        alias_override_path=alias_override_path,
        terminal_override_path=terminal_override_path,
    )
    if not codes:
        return "", ()
    placeholders = ",".join("?" for _ in codes)
    return f"{column_name} NOT IN ({placeholders})", tuple(codes)
