#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
策略建议生成器（Step 7-13）

根据 monitor_summary 的 overall_verdict 与最新 publishable patch，
生成 promote / hold / rollback / collect_more_data 动作建议。

所有函数为纯函数（无副作用），易于测试。
"""

import hashlib
import json
import logging
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# 回滚补丁生成
# ============================================================================

def generate_rollback_patch() -> Dict:
    """
    生成回滚补丁：回到最安全的 baseline 行为。

    Returns:
        结构化回滚补丁 dict
    """
    return {
        'SCORING_POLICY_RECOMMENDED': {
            'enabled': False,
        },
        'SCORING_POLICY': {
            'default_score_key': 'combo_score',
            'per_fund_type': {
                'bond': 'combo_score',
                'stock': 'combo_score',
                'hybrid': 'combo_score',
            },
        },
    }


def format_rollback_snippet(patch: Dict) -> str:
    """
    将回滚补丁格式化为 Python 代码片段（与 7-9 format_patch_snippet 风格一致）。

    Args:
        patch: generate_rollback_patch() 的返回值

    Returns:
        Python 代码片段字符串
    """
    lines = [
        '# === ROLLBACK PATCH ===',
        '# 回滚到 baseline 行为：禁用推荐策略，所有类型使用 combo_score',
        '#',
        '# 生成来源: policy_monitor verdict=rollback',
        '',
        'SCORING_POLICY_RECOMMENDED = {',
        "    'enabled': False,",
        '}',
        '',
        'SCORING_POLICY = {',
        "    'default_score_key': 'combo_score',",
        "    'per_fund_type': {",
    ]
    pft = patch.get('SCORING_POLICY', {}).get('per_fund_type', {})
    for ft, sk in sorted(pft.items()):
        lines.append(f"        '{ft}': '{sk}',")
    lines.extend([
        '    },',
        '}',
        '',
    ])
    return '\n'.join(lines)


# ============================================================================
# Publishable Patch 读取
# ============================================================================

def load_latest_publishable_patch(output_dir: Path) -> Optional[Dict]:
    """
    从 output_dir 读取最新的 publishable policy patch。

    查找策略：
    1. 优先读取 policy_patch_latest.json（如果存在）
    2. 否则扫描 *.policy_patch.json 取最新

    Args:
        output_dir: 输出目录路径

    Returns:
        解析后的 patch dict（publishable=True 时），或 None
    """
    if not output_dir or not output_dir.is_dir():
        return None

    resolved_dir = output_dir.resolve()

    # 策略1：latest 固定文件
    latest_path = resolved_dir / 'policy_patch_latest.json'
    if latest_path.exists():
        patch = _safe_read_patch(latest_path, resolved_dir)
        if patch and patch.get('recommendation', {}).get('publishable'):
            return patch

    # 策略2：扫描 *.policy_patch.json 取最新修改时间
    pattern = str(resolved_dir / '*.policy_patch.json')
    candidates = sorted(glob(pattern), key=lambda p: Path(p).stat().st_mtime, reverse=True)

    for candidate_path in candidates:
        patch = _safe_read_patch(Path(candidate_path), resolved_dir)
        if patch and patch.get('recommendation', {}).get('publishable'):
            return patch

    return None


def _safe_read_patch(file_path: Path, allowed_dir: Path) -> Optional[Dict]:
    """
    安全读取 patch 文件（路径白名单 + 防目录穿越）。

    Args:
        file_path: 文件路径
        allowed_dir: 允许的目录

    Returns:
        解析后的 dict 或 None
    """
    try:
        resolved = file_path.resolve()
        if not resolved.exists():
            return None
        # 使用 is_relative_to 替代 startswith，防止路径前缀欺骗
        if not resolved.is_relative_to(allowed_dir.resolve()):
            logger.warning(f"路径穿越检测: {file_path}")
            return None

        with open(resolved, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, ValueError) as e:
        logger.warning(f"读取 patch 文件失败: {file_path}: {e}")
        return None


# ============================================================================
# Patch Digest
# ============================================================================

def compute_patch_digest(patch_json: Optional[Dict]) -> Optional[str]:
    """
    对 patch_json 计算稳定的 SHA-256 摘要。

    Args:
        patch_json: 补丁字典

    Returns:
        十六进制摘要前 16 位，或 None
    """
    if not patch_json:
        return None
    canonical = json.dumps(patch_json, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:16]


# ============================================================================
# 核心推荐函数
# ============================================================================

def recommend_policy_action(
    monitor_summary: Optional[Dict],
    latest_publishable_patch: Optional[Dict] = None,
) -> Dict:
    """
    根据 monitor_summary 和最新 publishable patch 生成动作建议。

    规则（确定性、可测试）：
    1. verdict == "rollback_suggested" / "rollback"
       → action="rollback", patch=rollback_patch
    2. verdict in ("ok", "warn") + 有 publishable patch
       → action="promote", patch=该 patch
    3. verdict in ("ok", "warn") + 无 publishable patch
       → action="hold"
    4. verdict == "insufficient_data" / "unknown" / 其他
       → action="collect_more_data"

    Args:
        monitor_summary: task_history.extra_data.monitor_summary
        latest_publishable_patch: 可选的 publishable patch dict

    Returns:
        {action, reason, patch_json, patch_snippet, patch_source}
    """
    if not monitor_summary or not isinstance(monitor_summary, dict):
        return {
            'action': 'collect_more_data',
            'reason': 'no_monitor_summary',
            'patch_json': None,
            'patch_snippet': None,
            'patch_source': None,
        }

    verdict = monitor_summary.get('overall_verdict', 'unknown')

    # 规则 1：回滚
    if verdict in ('rollback_suggested', 'rollback'):
        rollback_patch = generate_rollback_patch()
        return {
            'action': 'rollback',
            'reason': 'monitor_verdict_rollback',
            'patch_json': rollback_patch,
            'patch_snippet': format_rollback_snippet(rollback_patch),
            'patch_source': 'generated_rollback',
        }

    # 规则 2/3：正常或警告
    if verdict in ('ok', 'warn'):
        if latest_publishable_patch and latest_publishable_patch.get('recommendation', {}).get('publishable'):
            # 从 patch 提取 snippet
            snippet = None
            try:
                from scripts.run_ab_matrix import format_patch_snippet
                snippet = format_patch_snippet(latest_publishable_patch)
            except (ImportError, Exception):
                snippet = json.dumps(
                    latest_publishable_patch.get('scoring_policy_patch', {}),
                    indent=2, ensure_ascii=False,
                )

            return {
                'action': 'promote',
                'reason': 'publishable_patch_available',
                'patch_json': latest_publishable_patch.get('scoring_policy_patch'),
                'patch_snippet': snippet,
                'patch_source': 'ab_matrix',
            }
        else:
            return {
                'action': 'hold',
                'reason': 'no_publishable_patch',
                'patch_json': None,
                'patch_snippet': None,
                'patch_source': None,
            }

    # 规则 4：数据不足或未知
    return {
        'action': 'collect_more_data',
        'reason': 'insufficient_data',
        'patch_json': None,
        'patch_snippet': None,
        'patch_source': None,
    }
