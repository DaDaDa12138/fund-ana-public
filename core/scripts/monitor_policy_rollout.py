#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
发布后在线监控 & 快速回滚信号

从 DB 读取最近 N 天的 screening_runs + config_snapshot，
聚合每日 KPI（覆盖率、fallback 率、分数漂移、freshness），
输出 Excel + JSON 报表，并给出回滚建议。

使用方式:
    python3 scripts/monitor_policy_rollout.py \
        --days 14 --fund-types bond,stock,hybrid \
        --output bond_fund_screening/output/policy_monitor.xlsx

Step 7-10: 发布后在线监控 & 快速回滚信号
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any

import pandas as pd

# 项目路径设置
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / 'bond_fund_screening'))

from bond_fund_screening.config import OUTPUT_DIR
from bond_fund_screening.core.scoring_engine import resolve_per_fund_type_score_key
from bond_fund_screening.report_artifact_metadata import write_derived_report_meta

logger = logging.getLogger('PolicyMonitor')


# ============================================================================
# 默认回滚阈值
# ============================================================================

DEFAULT_ROLLBACK_THRESHOLDS = {
    'max_fallback_rate': 0.30,
    'max_drift_consecutive_days': 2,
    'max_pass_rate_drop_pct': 0.20,
    'min_runs_per_day': 1,
    'drift_std_change_pct': 0.50,
    'drift_median_change_abs': 0.05,
}

FALLBACK_RATE_UNKNOWN = None


def _build_monitor_source_runs(runs: List[Dict]) -> List[Dict]:
    """把 screening_runs 行转换为 sidecar source_runs。"""
    normalized = []
    for row in sorted(
        runs or [],
        key=lambda item: (
            str(item.get('started_at') or ''),
            str(item.get('finished_at') or ''),
            str(item.get('run_id') or ''),
        ),
    ):
        run_id = str(row.get('run_id') or '').strip()
        if not run_id:
            continue

        normalized.append({
            'run_id': run_id,
            'fund_type': row.get('fund_type'),
            'fund_type_name': row.get('fund_type'),
            'period_years': row.get('period_years'),
            'source_kind': 'screening_run',
        })

    return normalized


def _should_ignore_legacy_baseline_fallback(
    selected_score_key: str,
    policy_source: str,
    fallback_count: Any,
    actual_pick_count: Any,
    selected_score_key_dist: Dict[str, int],
) -> bool:
    """
    兼容旧脏数据：历史版本会把 baseline combo_score 误记为 fallback。

    这种记录的共同特征是：
    1. 实际生效 score_key 已是 combo_score
    2. policy_source 属于 baseline 路径（fallback/policy）
    3. fallback_count == actual_pick_count
    4. output_meta 中只出现 combo_score
    """
    if selected_score_key != 'combo_score':
        return False
    if policy_source not in ('fallback', 'policy'):
        return False

    try:
        fallback_count = int(fallback_count)
        actual_pick_count = int(actual_pick_count)
    except (TypeError, ValueError):
        return False

    if fallback_count <= 0 or actual_pick_count <= 0:
        return False
    if fallback_count != actual_pick_count:
        return False
    if selected_score_key_dist:
        return set(str(k) for k in selected_score_key_dist.keys()) == {'combo_score'}
    return True

# ============================================================================
# 纯函数：从 screening_runs 提取每日 KPI
# ============================================================================

def parse_run_snapshot(run_row: Dict) -> Dict:
    """
    从单条 screening_run 解析 config_snapshot 中的策略追溯字段。

    Args:
        run_row: screening_runs 行（dict 形式）

    Returns:
        解析后的追溯字段字典
    """
    result = {
        'selected_score_key': 'unknown',
        'policy_source': 'unknown',
        'fallback': False,
        'fallback_known': False,
        'fallback_rate': None,
        'fallback_count': None,
        'actual_pick_count': None,
        'fallback_reason': None,
        'fresh_rate': None,
        'stale_rate': None,
        'no_nav_rate': None,
    }

    snapshot_json = run_row.get('config_snapshot_json')
    if not snapshot_json:
        return result

    try:
        snapshot = json.loads(snapshot_json)
    except (json.JSONDecodeError, TypeError):
        return result

    # 策略追溯
    sp = snapshot.get('scoring_policy', {})
    runtime = sp.get('runtime', {})
    recommended = sp.get('recommended', {})

    selected_score_key = sp.get('selected_score_key')
    policy_source = sp.get('policy_source')
    if isinstance(selected_score_key, str) and selected_score_key:
        result['selected_score_key'] = selected_score_key
    if isinstance(policy_source, str) and policy_source:
        result['policy_source'] = policy_source
    elif recommended.get('enabled') and recommended.get('per_fund_type'):
        ft = run_row.get('fund_type', 'bond')
        period_key = run_row.get('period_years')
        rec_key = resolve_per_fund_type_score_key(
            recommended.get('per_fund_type', {}),
            fund_type=ft,
            period_years=period_key,
        )
        if rec_key:
            result['selected_score_key'] = rec_key
            result['policy_source'] = 'recommended'
        else:
            result['policy_source'] = 'policy'
    elif runtime.get('enabled'):
        result['policy_source'] = 'runtime'
    else:
        result['policy_source'] = 'fallback'

    output_meta = snapshot.get('output_meta', {})
    fallback_count = output_meta.get('selected_score_fallback_count')
    actual_pick_count = output_meta.get('actual_pick_count')
    fallback_reason_dist = output_meta.get('selected_score_fallback_reason_dist') or {}
    selected_score_key_dist = output_meta.get('selected_score_key_dist') or {}

    if selected_score_key_dist:
        try:
            result['selected_score_key'] = max(selected_score_key_dist.items(), key=lambda item: item[1])[0]
        except Exception:
            pass

    if fallback_count is not None:
        try:
            fallback_count = int(fallback_count)
            actual_pick_count = int(actual_pick_count) if actual_pick_count is not None else 0
            result['fallback_known'] = True
            result['actual_pick_count'] = actual_pick_count

            if _should_ignore_legacy_baseline_fallback(
                selected_score_key=result['selected_score_key'],
                policy_source=result['policy_source'],
                fallback_count=fallback_count,
                actual_pick_count=actual_pick_count,
                selected_score_key_dist=selected_score_key_dist,
            ):
                result['fallback'] = False
                result['fallback_count'] = 0
                result['fallback_rate'] = 0.0
                result['fallback_reason'] = None
            else:
                result['fallback'] = fallback_count > 0
                result['fallback_count'] = fallback_count
                if actual_pick_count > 0:
                    result['fallback_rate'] = fallback_count / actual_pick_count
                else:
                    result['fallback_rate'] = 1.0 if fallback_count > 0 else 0.0
                if fallback_reason_dist:
                    top_reason = max(fallback_reason_dist.items(), key=lambda item: item[1])[0]
                    result['fallback_reason'] = top_reason
        except (TypeError, ValueError):
            pass

    # freshness
    fs = snapshot.get('freshness_summary', {})
    total = fs.get('total_funds', 0)
    if total > 0:
        result['fresh_rate'] = fs.get('fresh_count', 0) / total
        result['stale_rate'] = fs.get('stale_count', 0) / total
        result['no_nav_rate'] = fs.get('no_nav_count', 0) / total

    return result


def aggregate_daily_kpis(runs: List[Dict]) -> List[Dict]:
    """
    将 screening_runs 列表按日×fund_type 聚合为每日 KPI。

    Args:
        runs: screening_runs 行列表（每行含 started_at, fund_type,
              fund_count_in, fund_count_pass, config_snapshot_json 等）

    Returns:
        每日 KPI 列表，按 (date, fund_type) 排序
    """
    # 按 (date_str, fund_type) 分组
    groups: Dict[tuple, List[Dict]] = defaultdict(list)
    for r in runs:
        started = r.get('started_at')
        if isinstance(started, str):
            day_str = started[:10]
        elif isinstance(started, datetime):
            day_str = started.date().isoformat()
        elif isinstance(started, date):
            day_str = started.isoformat()
        else:
            continue
        ft = r.get('fund_type', 'unknown')
        groups[(day_str, ft)].append(r)

    daily_kpis = []
    for (day_str, ft), day_runs in sorted(groups.items()):
        runs_count = len(day_runs)
        total_funds = sum(r.get('fund_count_in') or 0 for r in day_runs)
        passed_funds = sum(r.get('fund_count_pass') or 0 for r in day_runs)
        pass_rate = passed_funds / total_funds if total_funds > 0 else 0

        # 解析每个 run 的 snapshot
        score_key_dist: Dict[str, int] = defaultdict(int)
        policy_source_dist: Dict[str, int] = defaultdict(int)
        fallback_count = 0
        fallback_pick_count = 0
        fallback_pick_total = 0
        fallback_reason_dist: Dict[str, int] = defaultdict(int)
        fresh_rates = []
        stale_rates = []

        for r in day_runs:
            parsed = parse_run_snapshot(r)
            score_key_dist[parsed['selected_score_key']] += 1
            policy_source_dist[parsed['policy_source']] += 1
            if parsed.get('fallback_known'):
                fallback_pick_count += int(parsed.get('fallback_count') or 0)
                fallback_pick_total += int(parsed.get('actual_pick_count') or 0)
            if parsed['fallback']:
                fallback_count += 1
                if parsed['fallback_reason']:
                    fallback_reason_dist[parsed['fallback_reason']] += 1
            if parsed['fresh_rate'] is not None:
                fresh_rates.append(parsed['fresh_rate'])
            if parsed.get('stale_rate') is not None:
                stale_rates.append(parsed['stale_rate'])

        fallback_rate = None
        if fallback_pick_total > 0:
            fallback_rate = fallback_pick_count / fallback_pick_total
        elif fallback_count > 0:
            fallback_rate = 1.0

        kpi = {
            'date': day_str,
            'fund_type': ft,
            'runs_count': runs_count,
            'total_funds': total_funds,
            'passed_funds': passed_funds,
            'pass_rate': round(pass_rate, 4),
            'fallback_rate': round(fallback_rate, 4) if fallback_rate is not None else FALLBACK_RATE_UNKNOWN,
            'fallback_rate_known': fallback_pick_total > 0 or fallback_count > 0,
            'fallback_runs_count': fallback_count,
            'score_key_dist': dict(score_key_dist),
            'policy_source_dist': dict(policy_source_dist),
            'fallback_reason_dist': dict(fallback_reason_dist),
            'avg_fresh_rate': round(sum(fresh_rates) / len(fresh_rates), 4) if fresh_rates else None,
            'avg_stale_rate': round(sum(stale_rates) / len(stale_rates), 4) if stale_rates else None,
        }
        daily_kpis.append(kpi)

    return daily_kpis


# ============================================================================
# 纯函数：分数漂移检测
# ============================================================================

def compute_drift_stats(
    daily_kpis: List[Dict],
    baseline_window: int = 7,
) -> List[Dict]:
    """
    对每日 KPI 计算分数漂移指标。

    使用 pass_rate 作为代理指标（因为 selected_combo_score 分布
    需要 per-fund 数据，而 run 粒度只有聚合值）。

    Args:
        daily_kpis: aggregate_daily_kpis() 的输出
        baseline_window: 基准窗口天数

    Returns:
        带 drift 字段的 daily_kpis（原地增强）
    """
    # 按 fund_type 分组
    by_ft: Dict[str, List[Dict]] = defaultdict(list)
    for kpi in daily_kpis:
        by_ft[kpi['fund_type']].append(kpi)

    result = []
    for ft, ft_kpis in sorted(by_ft.items()):
        ft_kpis_sorted = sorted(ft_kpis, key=lambda k: k['date'])
        for i, kpi in enumerate(ft_kpis_sorted):
            # 基准窗口：前 baseline_window 天
            start_idx = max(0, i - baseline_window)
            baseline = ft_kpis_sorted[start_idx:i]

            if len(baseline) < 2:
                kpi['drift_flag'] = False
                kpi['drift_reason'] = 'insufficient_baseline'
                result.append(kpi)
                continue

            baseline_pass_rates = [b['pass_rate'] for b in baseline]
            bl_mean = sum(baseline_pass_rates) / len(baseline_pass_rates)
            bl_std = (
                sum((x - bl_mean) ** 2 for x in baseline_pass_rates)
                / len(baseline_pass_rates)
            ) ** 0.5

            current_pr = kpi['pass_rate']
            drift_flag = False
            drift_reasons = []

            # pass_rate 偏离基准均值超过 2 倍 std（或绝对值 > 0.05）
            deviation = abs(current_pr - bl_mean)
            threshold = max(2 * bl_std, 0.05)
            if deviation > threshold:
                drift_flag = True
                drift_reasons.append(
                    f"pass_rate_deviation={deviation:.4f}(threshold={threshold:.4f})"
                )

            kpi['drift_flag'] = drift_flag
            kpi['drift_reason'] = '; '.join(drift_reasons) if drift_reasons else 'none'
            kpi['baseline_pass_rate_mean'] = round(bl_mean, 4)
            kpi['baseline_pass_rate_std'] = round(bl_std, 4)
            result.append(kpi)

    return result


# ============================================================================
# 纯函数：回滚建议
# ============================================================================

def evaluate_rollout_health(
    daily_kpis: List[Dict],
    thresholds: Optional[Dict] = None,
) -> Dict:
    """
    根据每日 KPI 给出回滚建议。

    Args:
        daily_kpis: 带 drift 字段的每日 KPI 列表
        thresholds: 自定义阈值（覆盖 DEFAULT_ROLLBACK_THRESHOLDS）

    Returns:
        {'verdict': str, 'reasons': [str], 'daily_verdicts': [{date, ft, verdict, reasons}]}
    """
    th = {**DEFAULT_ROLLBACK_THRESHOLDS, **(thresholds or {})}

    if not daily_kpis:
        return {
            'verdict': 'insufficient_data',
            'reasons': ['no_daily_kpis_available'],
            'daily_verdicts': [],
        }

    daily_verdicts = []
    global_reasons = []
    worst_verdict = 'ok'
    saw_insufficient_data = False

    # 按 fund_type 分组检查
    by_ft: Dict[str, List[Dict]] = defaultdict(list)
    for kpi in daily_kpis:
        by_ft[kpi['fund_type']].append(kpi)

    for ft, ft_kpis in sorted(by_ft.items()):
        ft_kpis_sorted = sorted(ft_kpis, key=lambda k: k['date'])
        consecutive_drift = 0

        for kpi in ft_kpis_sorted:
            day_verdict = 'ok'
            day_reasons = []

            # 检查 runs_count
            if kpi['runs_count'] < th['min_runs_per_day']:
                day_verdict = 'insufficient_data'
                day_reasons.append(
                    f"runs_count={kpi['runs_count']}<min={th['min_runs_per_day']}"
                )
                daily_verdicts.append({
                    'date': kpi['date'], 'fund_type': ft,
                    'verdict': day_verdict, 'reasons': day_reasons,
                })
                saw_insufficient_data = True
                consecutive_drift = 0
                continue

            # fallback_rate
            if kpi.get('fallback_rate') is None:
                if day_verdict == 'ok':
                    day_verdict = 'insufficient_data'
                day_reasons.append('fallback_rate=unknown')
            elif kpi['fallback_rate'] >= th['max_fallback_rate']:
                day_verdict = 'rollback_suggested'
                day_reasons.append(
                    f"fallback_rate={kpi['fallback_rate']:.2f}>=max={th['max_fallback_rate']}"
                )

            # pass_rate drop vs baseline
            bl_mean = kpi.get('baseline_pass_rate_mean')
            if bl_mean is not None and bl_mean > 0:
                drop_pct = (bl_mean - kpi['pass_rate']) / bl_mean
                if drop_pct >= th['max_pass_rate_drop_pct']:
                    if day_verdict != 'rollback_suggested':
                        day_verdict = 'warn'
                    day_reasons.append(
                        f"pass_rate_drop={drop_pct:.2%}>=max={th['max_pass_rate_drop_pct']:.0%}"
                    )

            # drift 连续天数
            if kpi.get('drift_flag'):
                consecutive_drift += 1
                if consecutive_drift >= th['max_drift_consecutive_days']:
                    day_verdict = 'rollback_suggested'
                    day_reasons.append(
                        f"consecutive_drift={consecutive_drift}>=max={th['max_drift_consecutive_days']}"
                    )
                elif day_verdict == 'ok':
                    day_verdict = 'warn'
                    day_reasons.append(f"drift_flag=True(consecutive={consecutive_drift})")
            else:
                consecutive_drift = 0

            if not day_reasons:
                day_reasons.append('all_checks_passed')

            daily_verdicts.append({
                'date': kpi['date'], 'fund_type': ft,
                'verdict': day_verdict, 'reasons': day_reasons,
            })

            # 更新全局 worst
            if day_verdict == 'rollback_suggested':
                worst_verdict = 'rollback_suggested'
                global_reasons.append(f"{ft}/{kpi['date']}: {'; '.join(day_reasons)}")
            elif day_verdict == 'warn' and worst_verdict == 'ok':
                worst_verdict = 'warn'
                global_reasons.append(f"{ft}/{kpi['date']}: {'; '.join(day_reasons)}")
            elif day_verdict == 'insufficient_data':
                saw_insufficient_data = True
                if worst_verdict == 'ok':
                    global_reasons.append(f"{ft}/{kpi['date']}: {'; '.join(day_reasons)}")

    if worst_verdict == 'ok' and saw_insufficient_data:
        worst_verdict = 'insufficient_data'

    if not global_reasons:
        global_reasons.append('all_checks_passed')

    return {
        'verdict': worst_verdict,
        'reasons': global_reasons,
        'daily_verdicts': daily_verdicts,
    }

# ============================================================================
# DB 查询
# ============================================================================

def fetch_recent_runs(days: int = 14, fund_types: Optional[List[str]] = None) -> List[Dict]:
    """
    从 DB 读取最近 N 天的 screening_runs。

    Args:
        days: 回溯天数
        fund_types: 过滤基金类型（None=全部）

    Returns:
        screening_runs 行列表
    """
    try:
        from database.connection import get_db_manager
        from database.models import ScreeningRun
    except ImportError:
        logger.warning("无法导入数据库模块，返回空列表")
        return []

    cutoff = datetime.now() - timedelta(days=days)
    db_manager = get_db_manager()

    with db_manager.get_session(task_name='monitor_fetch_runs', readonly=True) as session:
        query = session.query(ScreeningRun).filter(
            ScreeningRun.started_at >= cutoff,
            ScreeningRun.status == 'success',
        )
        if fund_types:
            query = query.filter(ScreeningRun.fund_type.in_(fund_types))

        query = query.order_by(ScreeningRun.started_at)
        rows = query.all()

        return [
            {
                'run_id': r.run_id,
                'started_at': r.started_at,
                'finished_at': r.finished_at,
                'status': r.status,
                'fund_type': r.fund_type,
                'period_years': r.period_years,
                'fund_count_in': r.fund_count_in,
                'fund_count_pass': r.fund_count_pass,
                'fund_count_fail': r.fund_count_fail,
                'config_snapshot_json': r.config_snapshot_json,
                'code_version': r.code_version,
            }
            for r in rows
        ]


# ============================================================================
# 报表生成
# ============================================================================

def generate_excel_report(
    daily_kpis: List[Dict],
    health: Dict,
    output_path: str,
    config_meta: Optional[Dict] = None,
) -> str:
    """
    生成 Excel 监控报表（4 个 sheet）。

    Args:
        daily_kpis: 带 drift 字段的每日 KPI
        health: evaluate_rollout_health() 的输出
        output_path: 输出路径
        config_meta: 脚本参数元数据

    Returns:
        输出文件路径
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Sheet 1: Summary
        summary_rows = []
        for kpi in daily_kpis:
            # 找对应的 daily verdict
            dv = next(
                (d for d in health.get('daily_verdicts', [])
                 if d['date'] == kpi['date'] and d['fund_type'] == kpi['fund_type']),
                {'verdict': 'unknown', 'reasons': []},
            )
            summary_rows.append({
                'date': kpi['date'],
                'fund_type': kpi['fund_type'],
                'runs_count': kpi['runs_count'],
                'total_funds': kpi['total_funds'],
                'passed_funds': kpi['passed_funds'],
                'pass_rate': kpi['pass_rate'],
                'fallback_rate': kpi['fallback_rate'],
                'avg_fresh_rate': kpi.get('avg_fresh_rate'),
                'drift_flag': kpi.get('drift_flag', False),
                'verdict': dv['verdict'],
                'reasons': '; '.join(dv['reasons']),
            })
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_excel(writer, sheet_name='Summary', index=False)

        # Sheet 2: ScoreKeyBreakdown
        breakdown_rows = []
        for kpi in daily_kpis:
            for sk, cnt in sorted(kpi.get('score_key_dist', {}).items()):
                breakdown_rows.append({
                    'date': kpi['date'],
                    'fund_type': kpi['fund_type'],
                    'field': 'score_key',
                    'value': sk,
                    'count': cnt,
                })
            for ps, cnt in sorted(kpi.get('policy_source_dist', {}).items()):
                breakdown_rows.append({
                    'date': kpi['date'],
                    'fund_type': kpi['fund_type'],
                    'field': 'policy_source',
                    'value': ps,
                    'count': cnt,
                })
            for fr, cnt in sorted(kpi.get('fallback_reason_dist', {}).items()):
                breakdown_rows.append({
                    'date': kpi['date'],
                    'fund_type': kpi['fund_type'],
                    'field': 'fallback_reason',
                    'value': fr,
                    'count': cnt,
                })
        df_breakdown = pd.DataFrame(breakdown_rows) if breakdown_rows else pd.DataFrame(
            columns=['date', 'fund_type', 'field', 'value', 'count']
        )
        df_breakdown.to_excel(writer, sheet_name='ScoreKeyBreakdown', index=False)

        # Sheet 3: Drift
        drift_rows = []
        for kpi in daily_kpis:
            drift_rows.append({
                'date': kpi['date'],
                'fund_type': kpi['fund_type'],
                'pass_rate': kpi['pass_rate'],
                'baseline_mean': kpi.get('baseline_pass_rate_mean'),
                'baseline_std': kpi.get('baseline_pass_rate_std'),
                'drift_flag': kpi.get('drift_flag', False),
                'drift_reason': kpi.get('drift_reason', ''),
            })
        df_drift = pd.DataFrame(drift_rows)
        df_drift.to_excel(writer, sheet_name='Drift', index=False)

        # Sheet 4: Metadata
        try:
            from version import __version__
        except ImportError:
            __version__ = 'unknown'

        meta_rows = [
            {'key': 'generated_at', 'value': datetime.now().isoformat(timespec='seconds')},
            {'key': 'version', 'value': __version__},
            {'key': 'overall_verdict', 'value': health['verdict']},
            {'key': 'overall_reasons', 'value': '; '.join(health['reasons'])},
        ]
        if config_meta:
            for k, v in sorted(config_meta.items()):
                meta_rows.append({'key': f'param_{k}', 'value': str(v)})

        df_meta = pd.DataFrame(meta_rows)
        df_meta.to_excel(writer, sheet_name='Metadata', index=False)

    return output_path


def generate_json_report(
    daily_kpis: List[Dict],
    health: Dict,
    output_path: str,
    config_meta: Optional[Dict] = None,
) -> str:
    """
    生成 JSON 监控报表。

    Args:
        daily_kpis: 带 drift 字段的每日 KPI
        health: evaluate_rollout_health() 的输出
        output_path: JSON 输出路径
        config_meta: 脚本参数元数据

    Returns:
        输出文件路径
    """
    try:
        from version import __version__
    except ImportError:
        __version__ = 'unknown'

    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'version': __version__,
        'inputs': config_meta or {},
        'overall_verdict': health['verdict'],
        'overall_reasons': health['reasons'],
        'daily_kpis': daily_kpis,
        'daily_verdicts': health.get('daily_verdicts', []),
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    return output_path


# ============================================================================
# 可调用入口（供 TaskScheduler 或外部脚本导入使用）
# ============================================================================

def build_report(
    days: int = 14,
    fund_types: Optional[List[str]] = None,
    output_path: Optional[str] = None,
    emit_json: bool = True,
    rollback_thresholds: Optional[Dict] = None,
) -> Dict:
    """
    核心报表生成入口（从 main() 提取，供 TaskScheduler 调用）。

    Args:
        days: 回溯天数
        fund_types: 过滤基金类型（None=全部）
        output_path: Excel 输出路径（None=自动生成）
        emit_json: 是否同时输出 JSON
        rollback_thresholds: 自定义回滚阈值

    Returns:
        {
            'daily_kpis': list,
            'health': dict,
            'xlsx_path': str,
            'json_path': str|None,
            'config_meta': dict,
        }
    """
    if output_path is None:
        output_path = str(
            OUTPUT_DIR / f'policy_monitor_{date.today().isoformat()}.xlsx'
        )

    config_meta = {
        'days': days,
        'fund_types': fund_types or 'all',
    }

    # 1. 读取数据
    runs = fetch_recent_runs(days=days, fund_types=fund_types)

    # 2. 聚合 KPI
    daily_kpis = aggregate_daily_kpis(runs)
    daily_kpis = compute_drift_stats(daily_kpis)

    # 3. 回滚建议
    health = evaluate_rollout_health(daily_kpis, thresholds=rollback_thresholds)

    # 4. 输出报表
    generate_excel_report(daily_kpis, health, output_path, config_meta)

    json_path = None
    if emit_json:
        json_path = str(Path(output_path).with_suffix('.json'))
        generate_json_report(daily_kpis, health, json_path, config_meta)

    report_meta_path = write_derived_report_meta(
        output_path,
        artifact_type='policy_monitor_report',
        source_runs=_build_monitor_source_runs(runs),
        extra_meta={
            'json_file': json_path,
            'run_count': len(runs),
            'days': days,
            'fund_types': fund_types or ['all'],
            'overall_verdict': health['verdict'],
            'overall_reasons': list(health['reasons']),
        },
    )

    return {
        'daily_kpis': daily_kpis,
        'health': health,
        'xlsx_path': output_path,
        'json_path': json_path,
        'report_meta_path': str(report_meta_path),
        'config_meta': config_meta,
    }


def run_policy_monitor(
    days: int = 14,
    fund_types: Optional[List[str]] = None,
    output_dir: Optional[str] = None,
    now: Optional[date] = None,
    rollback_thresholds: Optional[Dict] = None,
) -> Dict:
    """
    运行策略监控并返回轻量 summary（供 task_history.extra_data 写入）。

    不包含 daily_kpis 全量数据，仅 summary + 文件路径。

    Args:
        days: 回溯天数
        fund_types: 过滤基金类型
        output_dir: 输出目录（None=默认 OUTPUT_DIR）
        now: 当前日期（用于文件命名，None=today）
        rollback_thresholds: 自定义回滚阈值

    Returns:
        轻量 summary dict（写入 task_history.extra_data）
    """
    today = now or date.today()
    out_dir = Path(output_dir) if output_dir else OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx_path = str(out_dir / f'policy_monitor_{today.isoformat()}.xlsx')

    report = build_report(
        days=days,
        fund_types=fund_types,
        output_path=xlsx_path,
        emit_json=True,
        rollback_thresholds=rollback_thresholds,
    )

    health = report['health']
    daily_kpis = report['daily_kpis']

    # 提取轻量摘要
    fallback_rates = [k.get('fallback_rate') for k in daily_kpis if k.get('fallback_rate') is not None]
    pass_rates = [k.get('pass_rate', 0) for k in daily_kpis]

    # 计算最大连续 drift 天数
    max_consecutive_drift = 0
    current_streak = 0
    for k in sorted(daily_kpis, key=lambda x: (x.get('fund_type', ''), x.get('date', ''))):
        if k.get('drift_flag'):
            current_streak += 1
            max_consecutive_drift = max(max_consecutive_drift, current_streak)
        else:
            current_streak = 0

    # pass_rate drop: 最差的一天 vs baseline
    worst_drop = 0.0
    for k in daily_kpis:
        bl = k.get('baseline_pass_rate_mean')
        if bl and bl > 0:
            drop = (bl - k['pass_rate']) / bl
            worst_drop = max(worst_drop, drop)

    summary = {
        'overall_verdict': health['verdict'],
        'reasons': health['reasons'],
        'fallback_rate_max': round(max(fallback_rates), 4) if fallback_rates else None,
        'drift_days_consecutive_max': max_consecutive_drift,
        'pass_rate_drop_worst': round(worst_drop, 4),
        'kpi_days': len(daily_kpis),
        'file_paths': {
            'xlsx': report['xlsx_path'],
            'json': report['json_path'],
            'report_meta': report['report_meta_path'],
        },
        'checked_on': today.isoformat(),
        'params': {
            'days': days,
            'fund_types': fund_types or 'all',
        },
    }

    return summary


# ============================================================================
# CLI
# ============================================================================

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='发布后在线监控 & 快速回滚信号（Step 7-10）'
    )
    parser.add_argument('--execute', action='store_true',
                        help='显式执行真实策略监控报表生成；默认仅输出 dry-run 计划')
    parser.add_argument('--days', type=int, default=14,
                        help='回溯天数（默认: 14）')
    parser.add_argument('--fund-types', default=None,
                        help='基金类型过滤，逗号分隔（默认: 全部）')
    parser.add_argument('--output', default=None,
                        help='输出 Excel 路径')
    parser.add_argument('--emit-json', action='store_true', default=True,
                        help='同时输出 JSON（默认: True）')
    parser.add_argument('--no-json', action='store_true', default=False,
                        help='禁用 JSON 输出')
    parser.add_argument('--quiet', action='store_true',
                        help='静默模式')
    return parser.parse_args(argv)


def print_execution_plan(args, fund_types, emit_json):
    """默认仅打印执行计划，避免无参数误生成策略监控报表。"""
    print("=" * 80)
    print("策略发布后监控报表脚本（dry-run）")
    print("=" * 80)
    print("当前默认只输出执行计划，不会生成真实 Excel / JSON 监控报表。")
    print(f"- days: {args.days}")
    print(f"- fund_types: {fund_types or ['all']}")
    print(f"- output: {args.output or str(OUTPUT_DIR / f'policy_monitor_{date.today().isoformat()}.xlsx')}")
    print(f"- emit_json: {emit_json}")
    print("如需真实执行，请显式传入 `--execute`。")


def main(argv=None):
    """主入口"""
    args = parse_args(argv)

    fund_types = None
    if args.fund_types:
        fund_types = [t.strip() for t in args.fund_types.split(',')]

    log_level = logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    output_path = args.output
    emit_json = args.emit_json and not args.no_json
    if not args.execute:
        print_execution_plan(args, fund_types, emit_json)
        return 0

    logger.info(f"读取最近 {args.days} 天的 screening_runs ...")
    report = build_report(
        days=args.days,
        fund_types=fund_types,
        output_path=output_path,
        emit_json=emit_json,
    )

    health = report['health']
    logger.info(f"整体 verdict: {health['verdict']}")
    logger.info(f"Excel 报表: {report['xlsx_path']}")
    if report['json_path']:
        logger.info(f"JSON 报表: {report['json_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
