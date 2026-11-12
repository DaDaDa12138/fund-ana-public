#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AutoStock 完整性结论策略。
"""

from __future__ import annotations

from typing import Any, Dict


NON_AUTHORITATIVE_SOURCES = {"fund/all"}


def assess_autostock_completeness_source(source_meta: Dict[str, Any] | None) -> Dict[str, Any]:
    """评估 AutoStock 当前目录源是否可用于完整性硬结论。"""
    meta = dict(source_meta or {})
    source_name = str(meta.get("source_name") or "fund/all").strip() or "fund/all"
    source_complete = bool(meta.get("source_complete", True))
    incomplete_reason = str(meta.get("incomplete_reason") or "").strip()

    can_conclude = source_complete
    reason = incomplete_reason or ""

    if source_name in NON_AUTHORITATIVE_SOURCES:
        can_conclude = False
        if not reason:
            reason = f"non_authoritative_source({source_name})"

    return {
        "source_name": source_name,
        "raw_source_complete": source_complete,
        "can_conclude_completeness": can_conclude,
        "incomplete_reason": reason or None,
    }
