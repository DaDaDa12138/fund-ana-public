#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A/B 实验矩阵运行器

自动跑 fund_type × period_years × horizon × score_key 的笛卡尔积，
汇总 Evaluation 结论，输出 MatrixSummary + MatrixWinners sheets，
并自动生成推荐的 SCORING_POLICY 配置片段。

使用方式:
    python3 scripts/run_ab_matrix.py \
        --execute \
        --fund-types bond,stock,hybrid \
        --period-years 3,5 \
        --horizons 30,90 \
        --topk 20 --weeks 8 --step-days 7 \
        --output bond_fund_screening/output/ab_matrix.xlsx

Step 7-4: 评分策略选择器 + 实验矩阵
Step 7-7: 纳入 cstv2 + 智能推荐策略生成
Step 7-9: 可发布策略补丁 + 发布闸门（guardrail）
"""

import argparse
import sys
import json
import logging
from copy import deepcopy
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Optional, Any
from itertools import product

import pandas as pd

# 项目路径设置
_project_root = Path(__file__).parent.parent
_project_root_str = str(_project_root)
_screening_root_str = str(_project_root / 'bond_fund_screening')
# 强制调整顺序：项目根目录优先，避免 "utils" 解析到 bond_fund_screening/utils
while _project_root_str in sys.path:
    sys.path.remove(_project_root_str)
while _screening_root_str in sys.path:
    sys.path.remove(_screening_root_str)
sys.path.insert(0, _project_root_str)
sys.path.append(_screening_root_str)

from scripts.backtest_screening import BacktestRunner, evaluate_score_keys
from bond_fund_screening.config import OUTPUT_DIR
from bond_fund_screening.report_artifact_metadata import write_derived_report_meta

logger = logging.getLogger('ABMatrix')


# ============================================================================
# 纯函数：从 Evaluation 结果生成推荐配置
# ============================================================================

def select_best_candidate(
    eval_rows: List[Dict],
    epsilon: float = 0.001,
) -> Dict:
    """
    从一组 evaluation 行中选出推荐的 score_key。

    输入行应属于同一 (fund_type, period_years) 组合，包含多 horizon × score_key。

    推荐规则：
    1. 仅考虑 verdict='better' 的行
    2. 按 score_key 聚合 delta_avg_return（取均值）和 stability_win_rate（取均值）
    3. 选 delta_avg 最大者；若差 < epsilon，选 stability_win_rate 更高者
    4. 若无 better：返回 combo_score + reason='no_candidate_passed'

    Args:
        eval_rows: 同一 (fund_type, period_years) 的所有评估行
        epsilon: delta_avg 差值阈值，差距小于此值时按 stability 决胜

    Returns:
        {'score_key': str, 'reason': str, 'delta_avg': float|None, 'stability': float|None}
    """
    better_rows = [r for r in eval_rows if r.get('verdict') == 'better']

    if not better_rows:
        verdicts = sorted(set(r.get('verdict', 'unknown') for r in eval_rows))
        return {
            'score_key': 'combo_score',
            'reason': f'no_candidate_passed ({", ".join(verdicts)})',
            'delta_avg': None,
            'stability': None,
        }

    # 按 score_key 聚合
    by_sk: Dict[str, List[Dict]] = defaultdict(list)
    for r in better_rows:
        by_sk[r['score_key']].append(r)

    candidates = []
    for sk, rows in sorted(by_sk.items()):
        deltas = [r.get('delta_avg_return') or 0 for r in rows]
        avg_delta = sum(deltas) / len(deltas) if deltas else 0
        stabilities = [r.get('stability_win_rate') for r in rows
                       if r.get('stability_win_rate') is not None]
        avg_stab = sum(stabilities) / len(stabilities) if stabilities else None
        candidates.append({
            'score_key': sk,
            'delta_avg': avg_delta,
            'stability': avg_stab,
            'n_better_horizons': len(rows),
        })

    # 排序: primary=delta_avg desc, secondary=stability desc
    candidates.sort(
        key=lambda c: (c['delta_avg'], c['stability'] or 0),
        reverse=True,
    )

    best = candidates[0]
    reason = 'better+stable'

    if len(candidates) > 1:
        second = candidates[1]
        if abs(best['delta_avg'] - second['delta_avg']) < epsilon:
            # 差距极小，用 stability 决胜
            if (second['stability'] or 0) > (best['stability'] or 0):
                best = second
                reason = 'better+more_stable(tiebreak)'
            else:
                reason = 'better+highest_delta(tiebreak)'
        else:
            reason = 'better+highest_delta'

    return {
        'score_key': best['score_key'],
        'reason': reason,
        'delta_avg': best['delta_avg'],
        'stability': best['stability'],
    }


def build_recommended_policy(
    winners: List[Dict],
    all_eval_rows: Optional[List[Dict]] = None,
    epsilon: float = 0.001,
) -> Dict:
    """
    从评估结果生成推荐的 SCORING_POLICY 配置。

    增强模式（all_eval_rows 不为 None）：
        按 (fund_type, period_years) 分组，用 select_best_candidate 智能选择。
    兼容模式（all_eval_rows 为 None）：
        从 winners 列表取第一个 winner（Step 7-4 原逻辑）。

    Args:
        winners: verdict=better 的评估行（兼容旧调用）
        all_eval_rows: 全量评估行（新增，启用智能推荐）
        epsilon: tiebreak 阈值

    Returns:
        {'default_score_key': str, 'per_fund_type': dict, 'reasons': dict}
    """
    per_fund_type: Dict[str, Dict[str, str]] = {}
    reasons: Dict[str, Dict[str, str]] = {}

    if all_eval_rows:
        # 智能推荐：按 (fund_type, period_years) 分组
        groups: Dict[tuple, List[Dict]] = defaultdict(list)
        for row in all_eval_rows:
            key = (row.get('fund_type', 'bond'), str(row.get('period_years', 3)))
            groups[key].append(row)

        for (ft, py), rows in sorted(groups.items()):
            best = select_best_candidate(rows, epsilon)
            if ft not in reasons:
                reasons[ft] = {}
            reasons[ft][py] = best['reason']
            if best['score_key'] != 'combo_score':
                if ft not in per_fund_type:
                    per_fund_type[ft] = {}
                per_fund_type[ft][py] = best['score_key']
    else:
        # 向后兼容：老逻辑
        for row in winners:
            ft = row.get('fund_type', 'bond')
            py = str(row.get('period_years', 3))
            sk = row.get('score_key', 'combo_score')
            if ft not in per_fund_type:
                per_fund_type[ft] = {}
            if py not in per_fund_type[ft]:
                per_fund_type[ft][py] = sk

    return {
        'default_score_key': 'combo_score',
        'per_fund_type': per_fund_type,
        'reasons': reasons,
    }


def format_policy_snippet(policy: Dict) -> str:
    """将 SCORING_POLICY 字典格式化为可粘贴的 Python 代码片段。

    支持 reasons 字段（Step 7-7）：如果 policy 包含 reasons，
    则在片段末尾追加注释说明每个推荐的理由。
    """
    lines = ["SCORING_POLICY = {"]
    lines.append(f"    'default_score_key': '{policy['default_score_key']}',")
    lines.append("    'per_fund_type': {")
    for ft, periods in sorted(policy.get('per_fund_type', {}).items()):
        inner = ', '.join(f"'{k}': '{v}'" for k, v in sorted(periods.items()))
        lines.append(f"        '{ft}': {{{inner}}},")
    lines.append("    },")
    lines.append("}")

    # 追加推荐理由注释
    reasons = policy.get('reasons', {})
    if reasons:
        lines.append("")
        lines.append("# 推荐理由:")
        for ft in sorted(reasons.keys()):
            for py in sorted(reasons[ft].keys()):
                lines.append(f"#   {ft}/{py}y: {reasons[ft][py]}")

    return '\n'.join(lines)


def build_default_output_path(
    output_dir: Path,
    fund_types: List[str],
    period_years_list: List[int],
    report_date: Optional[date] = None,
) -> Path:
    """构建默认输出路径，避免不同 fund_type/period 结果互相覆盖。"""
    normalized_fund_types = []
    for fund_type in fund_types or ['bond']:
        label = str(fund_type or '').strip().lower()
        if label and label not in normalized_fund_types:
            normalized_fund_types.append(label)
    if not normalized_fund_types:
        normalized_fund_types = ['bond']

    normalized_periods = []
    for period_year in period_years_list or [3]:
        label = int(period_year)
        if label not in normalized_periods:
            normalized_periods.append(label)
    if not normalized_periods:
        normalized_periods = [3]

    target_date = report_date or date.today()
    fund_label = '-'.join(normalized_fund_types)
    period_label = '-'.join(f'{period}y' for period in normalized_periods)
    return Path(output_dir) / f'ab_matrix_{fund_label}_{period_label}_{target_date.isoformat()}.xlsx'


# ============================================================================
# Step 7-9: 策略补丁（Policy Patch）+ 发布闸门（Guardrail）
# ============================================================================

# 默认发布阈值
DEFAULT_PUBLISH_THRESHOLDS = {
    'min_delta_avg': 0.002,
    'min_win_rate': 0.55,
    'min_coverage': 0.5,
    'min_stability': 0.6,
}


def build_matrix_completeness(
    expected_combos: List[str],
    completed_combos: List[str],
    failed_combos: List[Dict],
) -> Dict:
    """构建矩阵完整性摘要，用于阻止残缺矩阵产出可发布补丁。"""
    expected = [str(c) for c in (expected_combos or [])]
    completed = [str(c) for c in (completed_combos or [])]
    failed_details = []
    failed_keys = []

    for item in failed_combos or []:
        if isinstance(item, dict):
            combo_key = str(item.get('combo_key', 'unknown'))
            reason = str(item.get('reason', 'unknown'))
        else:
            combo_key = str(item)
            reason = 'unknown'
        failed_keys.append(combo_key)
        failed_details.append({
            'combo_key': combo_key,
            'reason': reason,
        })

    expected_set = set(expected)
    completed_set = set(completed)
    missing = sorted(expected_set - completed_set)
    complete = bool(expected) and not failed_keys and not missing

    return {
        'complete': complete,
        'expected_count': len(expected),
        'completed_count': len(completed),
        'failed_count': len(failed_keys),
        'expected_combos': sorted(expected),
        'completed_combos': sorted(completed),
        'failed_combos': sorted(failed_keys),
        'missing_combos': missing,
        'failure_details': failed_details,
    }


def apply_matrix_completeness_guard(
    patch: Dict,
    completeness: Dict,
    current_policy: Optional[Dict] = None,
) -> Dict:
    """
    当矩阵组合不完整时，强制补丁降级为不可发布。
    """
    guarded = deepcopy(patch or {})
    completeness = deepcopy(completeness or {})
    guarded.setdefault('inputs', {})['matrix_completeness'] = completeness

    if completeness.get('complete', True):
        return guarded

    recommendation = guarded.setdefault('recommendation', {})
    original_reasons = list(recommendation.get('reasons', []) or [])
    recommendation['publishable'] = False

    guard_reasons = [
        f"matrix_incomplete:{completeness.get('completed_count', 0)}/{completeness.get('expected_count', 0)}"
    ]
    failed_combos = completeness.get('failed_combos', [])
    if failed_combos:
        guard_reasons.append(f"failed_combos={','.join(failed_combos)}")
    missing_combos = completeness.get('missing_combos', [])
    if missing_combos:
        guard_reasons.append(f"missing_combos={','.join(missing_combos)}")
    recommendation['reasons'] = guard_reasons + [
        reason for reason in original_reasons if reason not in guard_reasons
    ]

    effective_current_policy = current_policy or {}
    current_per_ft = deepcopy(effective_current_policy.get('per_fund_type', {}))
    current_default = effective_current_policy.get('default_score_key', 'combo_score')

    scoring_policy_patch = guarded.setdefault('scoring_policy_patch', {})
    scoring_policy_patch['SCORING_POLICY'] = {
        'default_score_key': current_default,
        'per_fund_type': current_per_ft,
    }
    scoring_policy_patch['SCORING_POLICY_RECOMMENDED'] = {
        'enabled': False,
        'per_fund_type': {},
        'source': 'run_ab_matrix',
    }

    guarded.setdefault('verdict_summary', {})['__matrix__'] = {
        'publishable': False,
        'complete': False,
        'expected_count': completeness.get('expected_count', 0),
        'completed_count': completeness.get('completed_count', 0),
        'failed_combos': failed_combos,
        'missing_combos': missing_combos,
    }

    return guarded


def _group_eval_rows_by_ft_period(all_eval_rows: List[Dict]) -> Dict[tuple, List[Dict]]:
    """按 (fund_type, period_years) 分组，避免不同周期互相覆盖。"""
    groups: Dict[tuple, List[Dict]] = defaultdict(list)
    for row in all_eval_rows:
        ft = row.get('fund_type', 'bond')
        py = str(row.get('period_years', 3))
        groups[(ft, py)].append(row)
    return groups


def _get_existing_policy_for_period(current_per_ft: Dict, fund_type: str, period_key: str) -> Optional[str]:
    """读取当前策略中指定 fund_type/period 的既有值，兼容 flat/nested 两种结构。"""
    existing = current_per_ft.get(fund_type)
    if isinstance(existing, str) and existing:
        return existing
    if isinstance(existing, dict):
        score_key = existing.get(period_key)
        if isinstance(score_key, str) and score_key:
            return score_key
    return None


def check_publishable(
    all_eval_rows: List[Dict],
    publish_thresholds: Optional[Dict] = None,
    epsilon: float = 0.001,
) -> Dict:
    """
    检查评估结果是否满足发布条件（guardrail）。

    对每个 fund_type，使用 select_best_candidate() 选出最佳候选，
    再逐条检查是否满足发布阈值。不满足则回退为 combo_score。

    Args:
        all_eval_rows: 全量评估行
        publish_thresholds: 发布阈值（min_delta_avg/min_win_rate/min_coverage/min_stability）
        epsilon: tiebreak 阈值

    Returns:
        {
            'publishable': bool,
            'per_fund_type': {ft: {'score_key': str, 'publishable': bool, 'reasons': [...]}},
            'reasons': [str],
        }
    """
    thresholds = {**DEFAULT_PUBLISH_THRESHOLDS, **(publish_thresholds or {})}

    # 按 fund_type 分组
    by_ft: Dict[str, List[Dict]] = defaultdict(list)
    for r in all_eval_rows:
        by_ft[r.get('fund_type', 'bond')].append(r)

    per_ft_result = {}
    global_reasons = []
    any_publishable = False

    for ft in sorted(by_ft.keys()):
        ft_rows = by_ft[ft]
        best = select_best_candidate(ft_rows, epsilon)
        sk = best['score_key']
        reasons = []

        if sk == 'combo_score':
            # 无候选通过
            per_ft_result[ft] = {
                'score_key': 'combo_score',
                'publishable': False,
                'reasons': [best.get('reason', 'no_candidate_passed')],
                'delta_avg': None,
                'stability': None,
                'coverage': None,
            }
            continue

        # 聚合该 fund_type + score_key 的 better 行指标
        better_rows = [r for r in ft_rows
                       if r.get('verdict') == 'better' and r.get('score_key') == sk]
        ft_publishable = True

        # delta_avg check
        delta_avg = best.get('delta_avg')
        if delta_avg is None or delta_avg < thresholds['min_delta_avg']:
            ft_publishable = False
            reasons.append(
                f"delta_avg={delta_avg:.4f} < min={thresholds['min_delta_avg']}"
                if delta_avg is not None else "delta_avg=None"
            )

        # stability check
        stability = best.get('stability')
        if stability is None or stability < thresholds['min_stability']:
            ft_publishable = False
            reasons.append(
                f"stability={stability:.2f} < min={thresholds['min_stability']}"
                if stability is not None else "stability=None"
            )

        # coverage check: 用该 fund_type 所有 better 行的平均 coverage
        coverages = [r.get('coverage', 0) for r in better_rows]
        avg_coverage = sum(coverages) / len(coverages) if coverages else 0
        if avg_coverage < thresholds['min_coverage']:
            ft_publishable = False
            reasons.append(
                f"coverage={avg_coverage:.2f} < min={thresholds['min_coverage']}"
            )

        # win_rate check: 用 better 行的平均 win_rate
        win_rates = [r.get('win_rate', 0) for r in better_rows if r.get('win_rate') is not None]
        avg_wr = sum(win_rates) / len(win_rates) if win_rates else 0
        if avg_wr < thresholds['min_win_rate']:
            ft_publishable = False
            reasons.append(
                f"win_rate={avg_wr:.2f} < min={thresholds['min_win_rate']}"
            )

        if ft_publishable:
            any_publishable = True
        else:
            global_reasons.append(f"{ft}: guardrail failed ({'; '.join(reasons)})")

        per_ft_result[ft] = {
            'score_key': sk if ft_publishable else 'combo_score',
            'publishable': ft_publishable,
            'reasons': reasons if reasons else ['all_thresholds_met'],
            'delta_avg': delta_avg,
            'stability': stability,
            'coverage': avg_coverage,
        }

    if not any_publishable:
        global_reasons.insert(0, 'no_fund_type_meets_publish_thresholds')

    return {
        'publishable': any_publishable,
        'per_fund_type': per_ft_result,
        'reasons': global_reasons if global_reasons else ['all_fund_types_publishable'],
    }


def build_policy_patch(
    all_eval_rows: List[Dict],
    current_policy: Optional[Dict] = None,
    config_meta: Optional[Dict] = None,
    publish_thresholds: Optional[Dict] = None,
    epsilon: float = 0.001,
) -> Dict:
    """
    从评估结果构建可落库/落 config 的策略补丁。

    包含 guardrail 检查：只有通过条件的 fund_type 才标记为"建议启用"。

    Args:
        all_eval_rows: 全量评估行
        current_policy: 当前 SCORING_POLICY 配置（用于保留未变更项）
        config_meta: 输入参数元数据（weeks/topk/horizons 等）
        publish_thresholds: 自定义发布阈值
        epsilon: tiebreak 阈值

    Returns:
        完整策略补丁字典
    """
    try:
        from version import __version__
    except ImportError:
        __version__ = 'unknown'

    grouped_rows = _group_eval_rows_by_ft_period(all_eval_rows)
    ft_period_counts: Dict[str, int] = defaultdict(int)
    for ft, _py in grouped_rows:
        ft_period_counts[ft] += 1

    # 构建 per_fund_type 映射（SCORING_POLICY + RECOMMENDED_RUNTIME）
    policy_per_ft = {}
    recommended_per_ft = {}
    current_per_ft = (current_policy or {}).get('per_fund_type', {})
    verdict_summary = {}
    recommendation_reasons = []
    any_publishable = False

    for (ft, py), rows in sorted(grouped_rows.items()):
        group_guard = check_publishable(rows, publish_thresholds, epsilon)
        ft_result = group_guard['per_fund_type'].get(ft, {
            'score_key': 'combo_score',
            'publishable': False,
            'reasons': ['no_candidate_passed'],
            'delta_avg': None,
            'stability': None,
            'coverage': None,
        })

        if ft_result['publishable'] and ft_result['score_key'] != 'combo_score':
            any_publishable = True
            if ft_period_counts.get(ft, 0) == 1:
                policy_per_ft[ft] = ft_result['score_key']
                recommended_per_ft[ft] = ft_result['score_key']
            else:
                policy_per_ft.setdefault(ft, {})[py] = ft_result['score_key']
                recommended_per_ft.setdefault(ft, {})[py] = ft_result['score_key']
        else:
            # 保留 current_policy 中指定周期的值（如有），否则不写入
            existing = _get_existing_policy_for_period(current_per_ft, ft, py)
            if existing:
                if ft_period_counts.get(ft, 0) == 1:
                    policy_per_ft[ft] = existing
                else:
                    policy_per_ft.setdefault(ft, {})[py] = existing

        summary_key = ft if ft_period_counts.get(ft, 0) == 1 else f'{ft}/{py}'
        verdict_summary[summary_key] = {
            'best_key': ft_result['score_key'],
            'publishable': ft_result['publishable'],
            'delta_avg': ft_result.get('delta_avg'),
            'stability': ft_result.get('stability'),
            'coverage': ft_result.get('coverage'),
            'reasons': ft_result.get('reasons', []),
        }

        if not ft_result['publishable']:
            reason_text = '; '.join(ft_result.get('reasons', [])) or 'guardrail_failed'
            recommendation_reasons.append(f'{summary_key}: {reason_text}')

    if not any_publishable:
        recommendation_reasons.insert(0, 'no_fund_type_meets_publish_thresholds')
    elif not recommendation_reasons:
        recommendation_reasons = ['all_fund_types_publishable']

    patch = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'version': __version__,
        'inputs': config_meta or {},
        'verdict_summary': verdict_summary,
        'recommendation': {
            'publishable': any_publishable,
            'reasons': recommendation_reasons,
        },
        'scoring_policy_patch': {
            'SCORING_POLICY': {
                'default_score_key': 'combo_score',
                'per_fund_type': policy_per_ft,
            },
            'SCORING_POLICY_RECOMMENDED': {
                'enabled': False,  # 默认 False，避免自动上线
                'per_fund_type': recommended_per_ft,
                'source': 'run_ab_matrix',
            },
        },
    }

    return patch


def format_patch_snippet(patch: Dict) -> str:
    """
    将策略补丁格式化为可粘贴到 config.py 的 Python 代码片段。

    Args:
        patch: build_policy_patch() 的返回值

    Returns:
        Python 代码片段字符串
    """
    sp = patch.get('scoring_policy_patch', {})
    sp_policy = sp.get('SCORING_POLICY', {})
    sp_recommended = sp.get('SCORING_POLICY_RECOMMENDED', {})
    publishable = patch.get('recommendation', {}).get('publishable', False)

    lines = []

    # SCORING_POLICY
    lines.append("SCORING_POLICY = {")
    lines.append(f"    'default_score_key': '{sp_policy.get('default_score_key', 'combo_score')}',")
    lines.append("    'per_fund_type': {")
    for ft, sk in sorted(sp_policy.get('per_fund_type', {}).items()):
        if isinstance(sk, dict):
            inner = ', '.join(f"'{k}': '{v}'" for k, v in sorted(sk.items()))
            lines.append(f"        '{ft}': {{{inner}}},")
        else:
            lines.append(f"        '{ft}': '{sk}',")
    lines.append("    },")
    lines.append("}")
    lines.append("")

    # SCORING_POLICY_RECOMMENDED
    enabled = sp_recommended.get('enabled', False)
    lines.append("SCORING_POLICY_RECOMMENDED = {")
    lines.append(f"    'enabled': {enabled},")
    lines.append("    'per_fund_type': {")
    for ft, sk in sorted(sp_recommended.get('per_fund_type', {}).items()):
        if isinstance(sk, dict):
            inner = ', '.join(f"'{k}': '{v}'" for k, v in sorted(sk.items()))
            lines.append(f"        '{ft}': {{{inner}}},")
        else:
            lines.append(f"        '{ft}': '{sk}',")
    lines.append("    },")
    lines.append(f"    'source': '{sp_recommended.get('source', 'run_ab_matrix')}',")
    lines.append("}")

    # 元信息注释
    lines.append("")
    lines.append(f"# generated_at: {patch.get('generated_at', 'unknown')}")
    lines.append(f"# version: {patch.get('version', 'unknown')}")
    lines.append(f"# publishable: {publishable}")
    reasons = patch.get('recommendation', {}).get('reasons', [])
    if reasons:
        lines.append(f"# reasons: {'; '.join(reasons)}")

    return '\n'.join(lines)


# ============================================================================
# 矩阵运行器
# ============================================================================

class ABMatrixRunner:
    """A/B 实验矩阵运行器"""

    def __init__(
        self,
        fund_types: List[str] = None,
        period_years_list: List[int] = None,
        horizons: List[int] = None,
        topk: int = 20,
        weeks: int = 8,
        step_days: int = 7,
        robustness: str = 'rolling',
        min_coverage: float = 0.5,
        min_win_rate: float = 0.55,
        min_delta_avg: float = 0.002,
        verbose: bool = True,
        emit_policy_patch: bool = True,
        publish_thresholds: Optional[Dict] = None,
    ):
        self.fund_types = fund_types or ['bond']
        self.period_years_list = period_years_list or [3]
        self.horizons = horizons or [30, 90]
        self.topk = topk
        self.weeks = weeks
        self.step_days = step_days
        self.robustness = robustness
        self.min_coverage = min_coverage
        self.min_win_rate = min_win_rate
        self.min_delta_avg = min_delta_avg
        self.verbose = verbose
        self.emit_policy_patch = emit_policy_patch
        self.publish_thresholds = publish_thresholds
        self.all_eval_rows: List[Dict] = []
        self.expected_combos: List[str] = []
        self.completed_combos: List[str] = []
        self.failed_combos: List[Dict] = []

    def run(self) -> List[Dict]:
        """
        运行完整矩阵。

        对每个 (fund_type, period_years) 组合运行一次 BacktestRunner，
        然后从 Evaluation 结果中收集所有行。
        """
        combos = list(product(self.fund_types, self.period_years_list))
        total = len(combos)
        self.all_eval_rows = []
        self.expected_combos = [f'{ft}_{py}y' for ft, py in combos]
        self.completed_combos = []
        self.failed_combos = []

        for idx, (ft, py) in enumerate(combos, 1):
            combo_key = f'{ft}_{py}y'
            if self.verbose:
                logger.info(f"[{idx}/{total}] 运行 {combo_key} ...")

            try:
                runner = BacktestRunner(
                    fund_type=ft,
                    period_years=py,
                    topk=self.topk,
                    horizons=self.horizons,
                    weeks=self.weeks,
                    step_days=self.step_days,
                    verbose=False,
                    robustness=self.robustness,
                    min_coverage=self.min_coverage,
                    min_win_rate=self.min_win_rate,
                    min_delta_avg=self.min_delta_avg,
                )
                summary = runner.run()

                if summary:
                    eval_rows = evaluate_score_keys(
                        summary=summary,
                        by_date_records=runner.by_date_records,
                        baseline_key=runner.score_key,
                        horizons=self.horizons,
                        fund_type=ft,
                        period_years=py,
                        robustness=self.robustness,
                        min_coverage=self.min_coverage,
                        min_win_rate=self.min_win_rate,
                        min_delta_avg=self.min_delta_avg,
                    )
                    self.all_eval_rows.extend(eval_rows)
                    self.completed_combos.append(combo_key)
                    if self.verbose:
                        logger.info(f"  {combo_key}: {len(eval_rows)} 个评估行")
                else:
                    self.failed_combos.append({
                        'combo_key': combo_key,
                        'reason': 'empty_summary',
                    })
                    logger.warning(f"  {combo_key} 无评估结果，标记为不完整矩阵")

            except Exception as e:
                self.failed_combos.append({
                    'combo_key': combo_key,
                    'reason': f'exception:{e}',
                })
                logger.warning(f"  {combo_key} 失败: {e}")
                continue

        return self.all_eval_rows

    def generate_report(self, output_path: str) -> str:
        """
        生成 Excel 报告（MatrixSummary + MatrixWinners + RecommendedPolicy sheets）。

        Step 7-9: 同时生成 policy_patch.json（与 xlsx 同目录同名不同后缀）。

        Args:
            output_path: 输出文件路径

        Returns:
            输出文件路径
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        df_all = pd.DataFrame(self.all_eval_rows)
        if df_all.empty:
            logger.warning("无评估数据，跳过报告生成")
            return output_path

        # MatrixSummary: 全部评估行
        summary_cols = [
            'fund_type', 'period_years', 'horizon_days',
            'baseline_key', 'score_key',
            'avg_return', 'delta_avg_return',
            'win_rate', 'delta_win_rate',
            'coverage', 'stability_win_rate',
            'verdict', 'recommendation',
        ]
        existing_cols = [c for c in summary_cols if c in df_all.columns]
        df_summary = df_all[existing_cols].copy()

        # MatrixWinners: 仅 verdict=better 的行
        df_winners = df_all[df_all['verdict'] == 'better'].copy()
        winners_list = df_winners.to_dict('records') if not df_winners.empty else []

        # Step 7-7: 使用智能推荐（传入全量评估行）
        policy = build_recommended_policy(
            winners_list,
            all_eval_rows=self.all_eval_rows,
        )
        snippet = format_policy_snippet(policy)

        # Step 7-9: 构建策略补丁
        patch = None
        patch_json_path = None
        latest_patch_json_path = None
        patch_snippet_path = None
        latest_patch_snippet_path = None
        if self.emit_policy_patch:
            config_meta = {
                'fund_types': self.fund_types,
                'period_years_list': self.period_years_list,
                'horizons': self.horizons,
                'topk': self.topk,
                'weeks': self.weeks,
                'step_days': self.step_days,
                'robustness': self.robustness,
                'min_coverage': self.min_coverage,
                'min_win_rate': self.min_win_rate,
                'min_delta_avg': self.min_delta_avg,
            }
            try:
                from bond_fund_screening.config import SCORING_POLICY
                current_policy = SCORING_POLICY
            except ImportError:
                current_policy = None

            patch = build_policy_patch(
                all_eval_rows=self.all_eval_rows,
                current_policy=current_policy,
                config_meta=config_meta,
                publish_thresholds=self.publish_thresholds,
            )
            matrix_completeness = build_matrix_completeness(
                expected_combos=self.expected_combos,
                completed_combos=self.completed_combos,
                failed_combos=self.failed_combos,
            )
            patch = apply_matrix_completeness_guard(
                patch=patch,
                completeness=matrix_completeness,
                current_policy=current_policy,
            )
            if self.verbose and not matrix_completeness['complete']:
                logger.warning(
                    "矩阵运行不完整，已强制禁用 publishable: %s",
                    matrix_completeness,
                )

            # 输出 JSON
            xlsx_path = Path(output_path)
            patch_json_content = json.dumps(patch, ensure_ascii=False, indent=2)
            patch_json_path = str(
                xlsx_path.parent / (xlsx_path.stem + '.policy_patch.json')
            )
            with open(patch_json_path, 'w', encoding='utf-8') as f:
                f.write(patch_json_content)
            latest_patch_json_path = xlsx_path.parent / 'policy_patch_latest.json'
            latest_patch_json_path.write_text(patch_json_content, encoding='utf-8')
            if self.verbose:
                logger.info(f"策略补丁已生成: {patch_json_path}")
                logger.info(f"策略补丁最新副本已生成: {latest_patch_json_path}")

            # 输出 snippet 文件
            patch_snippet_content = format_patch_snippet(patch)
            patch_snippet_path = str(
                xlsx_path.parent / (xlsx_path.stem + '.policy_patch_snippet.py')
            )
            with open(patch_snippet_path, 'w', encoding='utf-8') as f:
                f.write(patch_snippet_content)
            latest_patch_snippet_path = xlsx_path.parent / 'policy_patch_latest_snippet.py'
            latest_patch_snippet_path.write_text(patch_snippet_content, encoding='utf-8')
            if self.verbose:
                logger.info(f"策略片段已生成: {patch_snippet_path}")
                logger.info(f"策略片段最新副本已生成: {latest_patch_snippet_path}")

        # 写入 Excel
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_summary.to_excel(writer, sheet_name='MatrixSummary', index=False)
            if not df_winners.empty:
                df_winners[existing_cols].to_excel(
                    writer, sheet_name='MatrixWinners', index=False
                )

            # RecommendedPolicy sheet
            policy_rows = []
            # 说明区：CONSISTENCY_V2 上下文
            try:
                from bond_fund_screening.config import CONSISTENCY_V2_CONFIG
                cstv2_enabled = CONSISTENCY_V2_CONFIG.get('enabled', False)
                policy_rows.append({
                    'section': 'consistency_v2_context',
                    'key': 'enabled',
                    'value': str(cstv2_enabled),
                })
                policy_rows.append({
                    'section': 'consistency_v2_context',
                    'key': 'metric',
                    'value': 'positive_month_ratio (replaces consistency 0.25 weight)',
                })
            except ImportError:
                pass

            # 推荐详情
            reasons = policy.get('reasons', {})
            for ft in sorted(reasons.keys()):
                for py in sorted(reasons[ft].keys()):
                    sk = policy['per_fund_type'].get(ft, {}).get(py, 'combo_score')
                    policy_rows.append({
                        'section': 'recommendation',
                        'key': f'{ft}/{py}y',
                        'value': sk,
                        'reason': reasons[ft][py],
                    })

            # 配置片段
            policy_rows.append({
                'section': 'snippet',
                'key': 'SCORING_POLICY',
                'value': snippet,
            })

            # Step 7-9: 发布闸门信息
            if patch:
                publishable = patch['recommendation']['publishable']
                matrix_info = patch.get('inputs', {}).get('matrix_completeness', {})
                policy_rows.append({
                    'section': 'matrix_completeness',
                    'key': 'complete',
                    'value': str(matrix_info.get('complete')),
                })
                policy_rows.append({
                    'section': 'matrix_completeness',
                    'key': 'completed_count',
                    'value': str(matrix_info.get('completed_count')),
                })
                policy_rows.append({
                    'section': 'matrix_completeness',
                    'key': 'expected_count',
                    'value': str(matrix_info.get('expected_count')),
                })
                if matrix_info.get('failed_combos'):
                    policy_rows.append({
                        'section': 'matrix_completeness',
                        'key': 'failed_combos',
                        'value': ','.join(matrix_info['failed_combos']),
                    })
                if matrix_info.get('missing_combos'):
                    policy_rows.append({
                        'section': 'matrix_completeness',
                        'key': 'missing_combos',
                        'value': ','.join(matrix_info['missing_combos']),
                    })
                policy_rows.append({
                    'section': 'guardrail',
                    'key': 'publishable',
                    'value': str(publishable),
                })
                for reason in patch['recommendation']['reasons']:
                    policy_rows.append({
                        'section': 'guardrail',
                        'key': 'reason',
                        'value': reason,
                    })
                if patch_json_path:
                    policy_rows.append({
                        'section': 'guardrail',
                        'key': 'patch_file',
                        'value': Path(patch_json_path).name,
                    })

            df_policy = pd.DataFrame(policy_rows)
            df_policy.to_excel(
                writer, sheet_name='RecommendedPolicy', index=False
            )

        report_meta_path = write_derived_report_meta(
            output_path,
            artifact_type='ab_matrix_report',
            source_runs=[],
            extra_meta={
                'fund_types': list(self.fund_types),
                'period_years_list': list(self.period_years_list),
                'horizons': list(self.horizons),
                'topk': self.topk,
                'weeks': self.weeks,
                'step_days': self.step_days,
                'robustness': self.robustness,
                'min_coverage': self.min_coverage,
                'min_win_rate': self.min_win_rate,
                'min_delta_avg': self.min_delta_avg,
                'expected_combos': list(self.expected_combos),
                'completed_combos': list(self.completed_combos),
                'failed_combos': list(self.failed_combos),
                'expected_combo_count': len(self.expected_combos),
                'completed_combo_count': len(self.completed_combos),
                'failed_combo_count': len(self.failed_combos),
                'policy_patch_file': str(patch_json_path) if patch_json_path else None,
                'policy_patch_latest_file': str(latest_patch_json_path) if latest_patch_json_path else None,
                'policy_patch_snippet_file': str(patch_snippet_path) if patch_snippet_path else None,
                'policy_patch_latest_snippet_file': str(latest_patch_snippet_path) if latest_patch_snippet_path else None,
                'matrix_summary_row_count': len(df_summary),
                'matrix_winner_row_count': len(df_winners),
                'all_eval_row_count': len(self.all_eval_rows),
            },
        )

        if self.verbose:
            logger.info(f"报告已生成: {output_path}")
            logger.info(f"报告元数据已生成: {report_meta_path}")
            logger.info(f"MatrixSummary: {len(df_summary)} 行")
            logger.info(f"MatrixWinners: {len(df_winners)} 行")
            if snippet:
                logger.info("推荐配置片段:")
                print(snippet)
            if patch:
                pub_str = "✅ 建议启用" if patch['recommendation']['publishable'] else "⚠️ 仅供参考"
                logger.info(f"发布闸门: {pub_str}")

        return output_path


# ============================================================================
# CLI
# ============================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='A/B 实验矩阵运行器（Step 7-4）'
    )
    parser.add_argument('--execute', action='store_true',
                        help='显式执行重型 A/B 实验矩阵；默认仅输出执行计划')
    parser.add_argument('--fund-types', default='bond',
                        help='基金类型，逗号分隔（默认: bond）')
    parser.add_argument('--period-years', default='3',
                        help='分析周期（年），逗号分隔（默认: 3）')
    parser.add_argument('--horizons', default='30,90',
                        help='前瞻天数，逗号分隔（默认: 30,90）')
    parser.add_argument('--topk', type=int, default=20,
                        help='每次筛选取 TopK（默认: 20）')
    parser.add_argument('--weeks', type=int, default=8,
                        help='回测窗口周数（默认: 8）')
    parser.add_argument('--step-days', type=int, default=7,
                        help='cut_date 间隔天数（默认: 7）')
    parser.add_argument('--robustness', default='rolling',
                        choices=['rolling', 'none'],
                        help='稳健性检验方式（默认: rolling）')
    parser.add_argument('--min-coverage', type=float, default=0.5,
                        help='最低覆盖率（默认: 0.5）')
    parser.add_argument('--min-win-rate', type=float, default=0.55,
                        help='最低胜率（默认: 0.55）')
    parser.add_argument('--min-delta-avg', type=float, default=0.002,
                        help='最低 delta_avg（默认: 0.002）')
    parser.add_argument('--output', default=None,
                        help='输出 Excel 路径')
    parser.add_argument('--emit-policy-patch', action='store_true', default=True,
                        help='生成策略补丁 JSON + snippet（默认: True）')
    parser.add_argument('--no-policy-patch', action='store_true', default=False,
                        help='禁用策略补丁生成')
    parser.add_argument('--publish-min-stability', type=float, default=None,
                        help='发布闸门 stability 阈值（覆盖默认 0.6）')
    parser.add_argument('--quiet', action='store_true',
                        help='静默模式')
    return parser


def parse_args(argv: Optional[List[str]] = None):
    return build_parser().parse_args(argv)


def print_execution_plan(args) -> None:
    fund_types = [t.strip() for t in args.fund_types.split(',') if t.strip()]
    period_years_list = [int(p.strip()) for p in args.period_years.split(',') if p.strip()]
    output_path = (
        args.output
        if args.output is not None
        else str(
            build_default_output_path(
                OUTPUT_DIR,
                fund_types=fund_types,
                period_years_list=period_years_list,
            )
        )
    )
    emit_patch = args.emit_policy_patch and not args.no_policy_patch

    print("=" * 80)
    print("A/B 实验矩阵运行器（dry-run）")
    print("=" * 80)
    print("该脚本会对 fund_type × period_years × horizon 组合串行执行历史回测，并生成矩阵报告。")
    print("默认仅输出计划，避免无参数误触发重型实验矩阵。")
    print("如需真实执行，请显式传入 `--execute`。")
    print()
    print(f"- fund_types: {','.join(fund_types) or 'bond'}")
    print(f"- period_years: {','.join(str(item) for item in period_years_list) or '3'}")
    print(f"- horizons: {args.horizons}")
    print(f"- topk: {args.topk}")
    print(f"- weeks: {args.weeks}")
    print(f"- step_days: {args.step_days}")
    print(f"- robustness: {args.robustness}")
    print(f"- min_coverage: {args.min_coverage}")
    print(f"- min_win_rate: {args.min_win_rate}")
    print(f"- min_delta_avg: {args.min_delta_avg}")
    print(f"- emit_policy_patch: {emit_patch}")
    if args.publish_min_stability is not None:
        print(f"- publish_min_stability: {args.publish_min_stability}")
    print(f"- output: {output_path}")


def main(argv: Optional[List[str]] = None):
    """主入口"""
    args = parse_args(argv)
    if not args.execute:
        print_execution_plan(args)
        return 0

    fund_types = [t.strip() for t in args.fund_types.split(',')]
    period_years_list = [int(p.strip()) for p in args.period_years.split(',')]
    horizons = [int(h.strip()) for h in args.horizons.split(',')]

    if args.output is None:
        output_path = str(
            build_default_output_path(
                OUTPUT_DIR,
                fund_types=fund_types,
                period_years_list=period_years_list,
            )
        )
    else:
        output_path = args.output

    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    # Step 7-9: 发布阈值覆盖
    publish_thresholds = None
    if args.publish_min_stability is not None:
        publish_thresholds = {'min_stability': args.publish_min_stability}

    emit_patch = args.emit_policy_patch and not args.no_policy_patch

    runner = ABMatrixRunner(
        fund_types=fund_types,
        period_years_list=period_years_list,
        horizons=horizons,
        topk=args.topk,
        weeks=args.weeks,
        step_days=args.step_days,
        robustness=args.robustness,
        min_coverage=args.min_coverage,
        min_win_rate=args.min_win_rate,
        min_delta_avg=args.min_delta_avg,
        verbose=not args.quiet,
        emit_policy_patch=emit_patch,
        publish_thresholds=publish_thresholds,
    )

    runner.run()
    runner.generate_report(output_path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
