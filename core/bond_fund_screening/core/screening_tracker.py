#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
筛选追踪器 - 记录筛选运行和决策

功能：
1. 创建和管理screening_runs记录
2. 记录每只基金在每层的筛选决策
3. 生成配置快照和哈希
4. 提供决策查询接口
"""

import uuid
import json
import hashlib
import os
import re
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Iterable
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent))

from database.connection import get_db_manager
from database.models import ScreeningRun, ScreeningDecision, FundNavHistory

# 导入版本号
try:
    from version import __version__
except ImportError:
    __version__ = "unknown"


logger = logging.getLogger(__name__)

DEFAULT_RUN_DEDUP_WINDOW_HOURS = 18.0
DEDUP_DISABLED_ENV_VALUES = {'0', 'false', 'no', 'off'}
INTERACTIVE_TRIGGER_SOURCES = {'web_api_screening'}
NON_INTERACTIVE_TRIGGER_PREFIXES = ('scheduler_', 'direct_')
DEDUP_IGNORED_SNAPSHOT_KEYS = {
    'freshness_summary',
    'output_meta',
    'tiantian_runtime',
    'trigger_task_id',
    'trigger_task_name',
}


def _is_dedup_guard_enabled(
    trigger_source: Optional[str],
    decision_persist_mode: Optional[str] = None,
) -> bool:
    raw = str(os.getenv('SCREENING_RUN_DEDUP_ENABLED', '1')).strip().lower()
    if raw in DEDUP_DISABLED_ENV_VALUES:
        return False
    trigger_source = normalize_trigger_source(trigger_source)
    if not trigger_source:
        return False
    if trigger_source.startswith(NON_INTERACTIVE_TRIGGER_PREFIXES):
        return True
    if trigger_source in INTERACTIVE_TRIGGER_SOURCES:
        return normalize_decision_persist_mode(decision_persist_mode) == 'picked_only'
    return False


def _get_dedup_window_hours() -> float:
    raw = str(os.getenv('SCREENING_RUN_DEDUP_WINDOW_HOURS', DEFAULT_RUN_DEDUP_WINDOW_HOURS)).strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_RUN_DEDUP_WINDOW_HOURS
    return value if value > 0 else 0.0


def _build_dedup_snapshot(config_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(config_snapshot, dict):
        return {}
    return {
        key: value
        for key, value in config_snapshot.items()
        if key not in DEDUP_IGNORED_SNAPSHOT_KEYS
    }


def _compute_dedup_snapshot_hash(config_snapshot: Optional[Dict[str, Any]]) -> str:
    sanitized_snapshot = _build_dedup_snapshot(config_snapshot)
    payload = json.dumps(
        sanitized_snapshot,
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(payload.encode()).hexdigest()


class ScreeningTracker:
    """筛选追踪器"""

    def __init__(
        self,
        period_years: float,
        fund_type: str = 'bond',
        profile_name: str = None,
        decision_persist_mode: str = 'all',
    ):
        """
        初始化追踪器

        Args:
            period_years: 分析周期（年）
            fund_type: 基金类型
            profile_name: Profile名称
            decision_persist_mode: 决策持久化模式（all/picked_only）
        """
        self.run_id = str(uuid.uuid4())
        self.period_years = period_years
        self.fund_type = fund_type
        self.profile_name = profile_name
        self.decision_persist_mode = normalize_decision_persist_mode(decision_persist_mode)
        self.db_manager = get_db_manager()

        # 决策缓存（批量写入提升性能）
        self._decisions_buffer: List[Dict] = []
        self._picked_only_decisions: Dict[str, List[Dict]] = {}
        self._buffer_size = 500

        # 统计
        self.fund_count_in = 0
        self.fund_count_pass = 0
        self.is_finished = False
        self._persistence_enabled = True
        self._persistence_skip_context: Dict[str, Any] = {}

        # Step 5C: 新鲜度汇总（finish_run 时写入 config_snapshot）
        self._freshness_summary = {}

    def _set_persistence_skip(self, reason: str, **extra: Any) -> None:
        self._persistence_enabled = False
        self._persistence_skip_context = {
            'reason': reason,
            **extra,
        }

    def get_persisted_run_id(self) -> Optional[str]:
        if not self._persistence_enabled:
            return None
        run_id = str(self.run_id).strip() if self.run_id else None
        return run_id or None

    def get_persistence_skip_context(self) -> Dict[str, Any]:
        return dict(self._persistence_skip_context)

    def _find_recent_duplicate_success_run(
        self,
        *,
        config_snapshot: Dict[str, Any],
        fund_count_in: int,
        nav_latest_date: Optional[date],
        data_cutoff_date: date,
    ) -> Optional[Dict[str, Any]]:
        trigger_source = normalize_trigger_source(config_snapshot.get('trigger_source'))
        decision_persist_mode = config_snapshot.get('decision_persist_mode') or self.decision_persist_mode
        if not _is_dedup_guard_enabled(trigger_source, decision_persist_mode):
            return None

        window_hours = _get_dedup_window_hours()
        if window_hours <= 0:
            return None

        dedup_hash = _compute_dedup_snapshot_hash(config_snapshot)
        cutoff_dt = datetime.now() - timedelta(hours=window_hours)

        with self.db_manager.get_session(task_name='screening_tracker_dedup_probe', readonly=True) as session:
            query = session.query(ScreeningRun).filter(
                ScreeningRun.status == 'success',
                ScreeningRun.fund_type == self.fund_type,
                ScreeningRun.period_years == self.period_years,
                ScreeningRun.code_version == __version__,
                ScreeningRun.fund_count_in == fund_count_in,
                ScreeningRun.started_at >= cutoff_dt,
                ScreeningRun.data_cutoff_date == data_cutoff_date,
            )

            if self.profile_name is None:
                query = query.filter(ScreeningRun.profile_name.is_(None))
            else:
                query = query.filter(ScreeningRun.profile_name == self.profile_name)

            if nav_latest_date is None:
                query = query.filter(ScreeningRun.nav_latest_date.is_(None))
            else:
                query = query.filter(ScreeningRun.nav_latest_date == nav_latest_date)

            candidate_rows = query.order_by(ScreeningRun.started_at.desc()).all()

            for row in candidate_rows:
                try:
                    existing_snapshot = json.loads(row.config_snapshot_json) if row.config_snapshot_json else {}
                except (json.JSONDecodeError, TypeError):
                    existing_snapshot = {}

                if _compute_dedup_snapshot_hash(existing_snapshot) != dedup_hash:
                    continue

                return {
                    'trigger_source': trigger_source,
                    'window_hours': window_hours,
                    'matched_run_id': row.run_id,
                    'matched_started_at': row.started_at.isoformat() if row.started_at else None,
                    'matched_finished_at': row.finished_at.isoformat() if row.finished_at else None,
                }

        return None

    def start_run(self, fund_count_in: int, config_snapshot: Dict) -> Optional[str]:
        """
        开始筛选运行

        Args:
            fund_count_in: 输入基金数
            config_snapshot: 配置快照

        Returns:
            run_id
        """
        self.fund_count_in = fund_count_in
        self.is_finished = False
        self._persistence_enabled = True
        self._persistence_skip_context = {}
        self._decisions_buffer.clear()
        self._picked_only_decisions.clear()

        # 不依赖调度器路径，任意新 run 启动前都先尝试自愈历史僵尸记录。
        try:
            from database.screening_run_maintenance import cleanup_stale_screening_runs
            cleanup_stale_screening_runs(
                db_manager=self.db_manager,
                stale_hours=2.0,
                fail_reason='筛选启动前自动清理：发现历史遗留 running 状态',
            )
        except Exception:
            pass

        # 计算配置哈希
        config_json = json.dumps(config_snapshot, sort_keys=True, ensure_ascii=False)
        config_hash = hashlib.sha256(config_json.encode()).hexdigest()

        # 获取最新净值日期
        nav_latest_date = self._get_latest_nav_date()
        data_cutoff_date = date.today()

        duplicate_meta = self._find_recent_duplicate_success_run(
            config_snapshot=config_snapshot,
            fund_count_in=fund_count_in,
            nav_latest_date=nav_latest_date,
            data_cutoff_date=data_cutoff_date,
        )
        if duplicate_meta:
            self._set_persistence_skip('duplicate_recent_success', **duplicate_meta)
            logger.info(
                "检测到近窗重复成功 run，跳过 tracking 落库: fund_type=%s period_years=%s matched_run_id=%s trigger_source=%s window_hours=%.1f",
                self.fund_type,
                self.period_years,
                duplicate_meta.get('matched_run_id'),
                duplicate_meta.get('trigger_source'),
                duplicate_meta.get('window_hours', 0.0),
            )
            return None

        with self.db_manager.get_session(task_name='screening_tracker_start') as session:
            run = ScreeningRun(
                run_id=self.run_id,
                started_at=datetime.now(),
                status='running',
                period_years=self.period_years,
                fund_type=self.fund_type,
                profile_name=self.profile_name,
                data_cutoff_date=data_cutoff_date,
                nav_latest_date=nav_latest_date,
                fund_count_in=fund_count_in,
                config_hash=config_hash,
                config_snapshot_json=config_json,
                code_version=__version__,
            )
            session.add(run)

        return self.run_id

    def finish_run(self, fund_count_pass: int, success: bool = True, fail_reason: str = None):
        """
        结束筛选运行

        Args:
            fund_count_pass: 通过筛选的基金数
            success: 是否成功
            fail_reason: 失败原因
        """
        if self.is_finished:
            return

        if not self._persistence_enabled:
            self._decisions_buffer.clear()
            self._picked_only_decisions.clear()
            self.fund_count_pass = fund_count_pass
            self.is_finished = True
            return

        # all 模式立即落库；picked_only 模式在拿到最终入选列表后再定向持久化，
        # finish_run 只负责兜底清理未落库的内存缓存，避免把未入选基金写入数据库。
        if self.decision_persist_mode == 'all':
            self._flush_decisions()
        else:
            self._picked_only_decisions.clear()

        self.fund_count_pass = fund_count_pass

        with self.db_manager.get_session(task_name='screening_tracker_finish') as session:
            run = session.query(ScreeningRun).filter_by(run_id=self.run_id).first()
            if run:
                run.finished_at = datetime.now()
                run.status = 'success' if success else 'failed'
                run.fund_count_pass = fund_count_pass
                run.fund_count_fail = self.fund_count_in - fund_count_pass
                if fail_reason:
                    run.fail_reason = fail_reason

                # Step 5C: 合并 freshness_summary 到 config_snapshot
                if self._freshness_summary:
                    try:
                        snapshot = json.loads(run.config_snapshot_json) if run.config_snapshot_json else {}
                        snapshot['freshness_summary'] = self._freshness_summary
                        run.config_snapshot_json = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
                    except (json.JSONDecodeError, TypeError):
                        pass
        self.is_finished = True
        try:
            from database.screening_run_storage_stats import refresh_screening_run_storage_stats

            refresh_screening_run_storage_stats(
                db_manager=self.db_manager,
                run_ids=[self.run_id],
                stats_source='finish_run',
            )
        except Exception:
            logger.debug("筛选 run 存储统计刷新失败，不影响主流程", exc_info=True)
        try:
            from database.screening_decision_summaries import refresh_screening_decision_summaries

            refresh_screening_decision_summaries(
                db_manager=self.db_manager,
                run_ids=[self.run_id],
                stats_source='finish_run',
            )
        except Exception:
            logger.debug("筛选 decision 摘要刷新失败，不影响主流程", exc_info=True)

    def record_freshness_summary(self, df: 'pd.DataFrame'):
        """
        记录数据新鲜度汇总统计（Step 5C）

        不改 schema，将汇总存入 config_snapshot_json。
        在 Layer1 处理后由 filtering_engine 调用。

        Args:
            df: 包含 freshness 列的 DataFrame（Layer1 输入数据）
        """
        import pandas as pd

        if 'is_fresh' not in df.columns:
            return

        total = len(df)
        fresh_count = int(df['is_fresh'].sum()) if total > 0 else 0
        stale_count = int((~df['is_fresh'] & df['nav_age_days'].notna()).sum()) if total > 0 else 0
        no_nav_count = int(df['nav_age_days'].isna().sum()) if total > 0 else 0

        summary = {
            'total_funds': total,
            'fresh_count': fresh_count,
            'stale_count': stale_count,
            'no_nav_count': no_nav_count,
        }

        # 影子统计
        for col in df.columns:
            if col.startswith('shadow_age_fail_'):
                summary[col + '_count'] = int(df[col].sum()) if total > 0 else 0

        self._freshness_summary = summary

    def record_decision(
        self,
        fcode: str,
        step_id: int,
        step_name: str,
        passed: bool,
        reason_code: str = None,
        reason_detail: str = None,
        metrics: Dict = None
    ):
        """
        记录单个筛选决策

        Args:
            fcode: 基金代码
            step_id: 筛选层ID (1-10)
            step_name: 筛选层名称
            passed: 是否通过
            reason_code: 原因代码
            reason_detail: 原因详情
            metrics: 关键指标快照
        """
        if not self._persistence_enabled:
            return

        decision = {
            'run_id': self.run_id,
            'fcode': fcode,
            'step_id': step_id,
            'step_name': step_name,
            'passed': passed,
            'reason_code': reason_code or ('PASSED' if passed else 'UNKNOWN'),
            'reason_detail': reason_detail,
            'metrics_json': json.dumps(metrics, ensure_ascii=False) if metrics else None,
            'created_at': datetime.now(),
        }

        if self.decision_persist_mode == 'picked_only':
            self._picked_only_decisions.setdefault(str(fcode), []).append(decision)
            return

        self._decisions_buffer.append(decision)

        # 达到缓冲区大小时批量写入
        if len(self._decisions_buffer) >= self._buffer_size:
            self._flush_decisions()

    def record_batch_decisions(
        self,
        fcodes: List[str],
        step_id: int,
        step_name: str,
        passed: bool,
        reason_code: str = None,
        reason_detail: str = None
    ):
        """
        批量记录相同决策（用于通过的基金）

        Args:
            fcodes: 基金代码列表
            step_id: 筛选层ID
            step_name: 筛选层名称
            passed: 是否通过
            reason_code: 原因代码
            reason_detail: 原因详情
        """
        for fcode in fcodes:
            self.record_decision(
                fcode=fcode,
                step_id=step_id,
                step_name=step_name,
                passed=passed,
                reason_code=reason_code,
                reason_detail=reason_detail
            )

    def persist_selected_decisions(self, selected_fcodes: Iterable[str]) -> int:
        """
        picked_only 模式下，仅持久化最终入选基金的决策链。

        Args:
            selected_fcodes: 最终入选基金代码列表

        Returns:
            实际写入的 decision 条数
        """
        if self.decision_persist_mode != 'picked_only' or not self._persistence_enabled:
            return 0

        selected_codes = {
            str(fcode).strip()
            for fcode in (selected_fcodes or [])
            if str(fcode).strip()
        }
        decisions_to_persist: List[Dict] = []

        for fcode in selected_codes:
            decisions_to_persist.extend(self._picked_only_decisions.get(fcode, []))

        persisted_count = self._save_decisions(decisions_to_persist)
        self._picked_only_decisions.clear()
        return persisted_count

    def _flush_decisions(self):
        """将缓冲区的决策批量写入数据库"""
        if not self._decisions_buffer:
            return

        self._save_decisions(self._decisions_buffer)
        self._decisions_buffer.clear()

    def _save_decisions(self, decisions: List[Dict]) -> int:
        """批量写入决策记录。"""
        if not decisions or not self._persistence_enabled:
            return 0

        with self.db_manager.get_session(task_name='screening_tracker_flush') as session:
            from database.screening_decision_payloads import ensure_storage, intern_payloads, resolve_metric_texts, REFS

            connection = session.connection().connection.driver_connection
            ensure_storage(connection)
            original = [decision.get('metrics_json') for decision in decisions]
            payload_ids = intern_payloads(connection, original)
            # The SQLite write transaction is already reserved. Allocate IDs
            # explicitly so bulk INSERT ordering/RETURNING cannot misassociate
            # a fund's explanation with another row.
            first_id = connection.execute('SELECT COALESCE(MAX(id),0)+1 FROM screening_decisions').fetchone()[0]
            row_ids = list(range(first_id, first_id + len(decisions)))
            session.bulk_save_objects([
                ScreeningDecision(**{**decision, 'id': row_id, 'metrics_json': None})
                for row_id, decision in zip(row_ids, decisions)
            ])
            persisted_ids = [row[0] for row in connection.execute(
                'SELECT id FROM screening_decisions WHERE id>=? AND id<? ORDER BY id',
                (first_id, first_id + len(decisions)),
            )]
            if persisted_ids != row_ids:
                raise ValueError('persisted_decision_rows_incomplete')
            connection.executemany(f'INSERT INTO {REFS}(decision_id,payload_id) VALUES(?,?)',
                                   [(row_id, payload_id) for row_id, payload_id in zip(row_ids, payload_ids)
                                    if payload_id is not None])
            if resolve_metric_texts(connection, [(row_id, None) for row_id in row_ids]) != original:
                raise ValueError('persisted_decision_payload_values_changed')

        return len(decisions)

    def _get_latest_nav_date(self) -> Optional[date]:
        """获取数据库中最新的净值日期"""
        try:
            with self.db_manager.get_session(task_name='get_nav_date', readonly=True) as session:
                from sqlalchemy import func
                result = session.query(func.max(FundNavHistory.nav_date)).scalar()
                return result
        except Exception:
            return None


def _get_override_snapshot() -> Dict:
    """
    Step 7-15: 读取 policy_override.json 的轻量快照（用于 config_snapshot）。

    注意：
    这里描述的是“override 文件状态”，不是“本次运行实际生效策略”。
    实际生效策略应以 scoring_policy.selected_score_key / policy_source 为准。

    Returns:
        {enabled, digest, policy_source, effective_score_key, snapshot_scope}
    """
    import hashlib
    override_path = Path(__file__).parent.parent.parent / 'config' / 'policy_override.json'
    if not override_path.exists():
        return {
            'enabled': None,
            'digest': None,
            'policy_source': 'unknown',
            'effective_score_key': 'unknown',
            'snapshot_scope': 'file_state',
        }
    try:
        raw = override_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()[:16]
        data = json.loads(raw.decode('utf-8'))
        enabled = data.get('enabled', False) if isinstance(data, dict) else False
        # 提取 effective score_key（简化：取 default_score_key 或第一个 per_fund_type）
        patch = data.get('patch', {}) if isinstance(data, dict) else {}
        sp = patch.get('SCORING_POLICY', {}) if isinstance(patch, dict) else {}
        score_key = sp.get('default_score_key', 'unknown') if isinstance(sp, dict) else 'unknown'
        return {
            'enabled': enabled,
            'digest': digest,
            'policy_source': 'override_file' if enabled else 'unknown',
            'effective_score_key': score_key if enabled else 'unknown',
            'snapshot_scope': 'file_state',
        }
    except Exception:
        return {
            'enabled': None,
            'digest': None,
            'policy_source': 'unknown',
            'effective_score_key': 'unknown',
            'snapshot_scope': 'file_state',
        }


def normalize_trigger_source(source: Optional[str]) -> Optional[str]:
    """规范化触发来源标签，便于后续审计直接消费。"""
    normalized = str(source or '').strip().lower()
    if not normalized:
        return None

    normalized = normalized.replace('-', '_').replace(' ', '_')
    normalized = re.sub(r'[^a-z0-9_]+', '_', normalized)
    normalized = re.sub(r'_+', '_', normalized).strip('_')
    return normalized or None


def _normalize_runtime_context_value(value: Optional[str], max_length: int = 255) -> Optional[str]:
    """清洗运行时上下文字段，避免空字符串或异常长值进入快照。"""
    normalized = str(value or '').strip()
    if not normalized:
        return None
    return normalized[:max_length]


def get_runtime_context_snapshot() -> Dict[str, str]:
    """
    读取运行时触发上下文。

    不改 schema，统一写入 screening_runs.config_snapshot_json，供后续审计直接判定来源。
    """
    snapshot = {}

    trigger_source = normalize_trigger_source(os.getenv('SCREENING_TRIGGER_SOURCE'))
    trigger_task_id = _normalize_runtime_context_value(os.getenv('SCREENING_TRIGGER_TASK_ID'))
    trigger_task_name = _normalize_runtime_context_value(os.getenv('SCREENING_TRIGGER_TASK_NAME'))

    if trigger_source:
        snapshot['trigger_source'] = trigger_source
    if trigger_task_id:
        snapshot['trigger_task_id'] = trigger_task_id
    if trigger_task_name:
        snapshot['trigger_task_name'] = trigger_task_name

    return snapshot


def _sanitize_tiantian_runtime_snapshot(summary: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """清洗 Tiantian 运行态快照，避免异常结构直接写入 run 快照。"""
    payload = summary if isinstance(summary, dict) else {}
    focus_metrics = payload.get('focus_metrics')
    degraded_endpoints = payload.get('degraded_endpoints')
    empty_hotspots = payload.get('empty_hotspots')
    errors = payload.get('errors')

    return {
        'captured_at': datetime.now().isoformat(timespec='seconds'),
        'available': bool(payload.get('available', False)),
        'verdict': payload.get('verdict'),
        'status': payload.get('status'),
        'message': payload.get('message'),
        'base_url': payload.get('base_url'),
        'health_url': payload.get('health_url'),
        'http_status': payload.get('http_status'),
        'service': payload.get('service'),
        'version': payload.get('version'),
        'route_count': payload.get('route_count'),
        'metrics_available': bool(payload.get('metrics_available', False)),
        'has_any_degraded_endpoint': bool(payload.get('has_any_degraded_endpoint', False)),
        'degraded_endpoints': degraded_endpoints if isinstance(degraded_endpoints, list) else [],
        'empty_hotspots': empty_hotspots if isinstance(empty_hotspots, list) else [],
        'focus_metrics': focus_metrics if isinstance(focus_metrics, dict) else {},
        'errors': errors if isinstance(errors, list) else [],
    }


def get_runtime_dependency_snapshot() -> Dict[str, Any]:
    """
    采集运行期外部依赖快照。

    仅在存在明确 trigger_source 的真实运行上下文下执行，避免常规单测/离线调用
    因健康探针引入额外网络依赖。
    """
    runtime_context = get_runtime_context_snapshot()
    if not runtime_context.get('trigger_source'):
        return {}

    try:
        from utils.service_checks import summarize_tiantian_runtime_health

        summary = summarize_tiantian_runtime_health(timeout_seconds=0.2)
    except Exception as exc:
        summary = {
            'available': False,
            'verdict': 'down',
            'status': 'probe_error',
            'message': f'Tiantian 运行态快照采集失败: {exc}',
            'errors': [str(exc)],
        }

    return {
        'tiantian_runtime': _sanitize_tiantian_runtime_snapshot(summary),
    }


def normalize_decision_persist_mode(mode: Optional[str]) -> str:
    """规范化决策持久化模式，非法值回退到 all。"""
    normalized = str(mode or 'all').strip().lower()
    if normalized not in {'all', 'picked_only'}:
        return 'all'
    return normalized


def get_config_snapshot(freshness_config_override: Optional[Dict[str, Any]] = None) -> Dict:
    """
    获取当前筛选配置快照

    Returns:
        配置字典（包含所有关键阈值）
    """
    from bond_fund_screening.config import (
        MAX_HOLDING_DAYS,
        MIN_ESTABLISH_DAYS,
        MIN_SCALE_BASELINE,
        SCALE_THRESHOLDS,
        RISK_METRIC_PASS_COUNT,
        MAX_ANNUAL_RETURN,
        MIN_ANNUAL_RETURN,
        MAX_DRAWDOWN_THRESHOLD,
        CONSISTENCY_PASS_COUNT,
        MIN_ABSOLUTE_RETURN,
        RISK_FREE_RATE,
        SHORT_TERM_WEIGHTS,
        LONG_TERM_WEIGHTS,
        SCORE_COMBO_WEIGHTS,
        # Step 3: 分桶配置
        BUCKETING_ENABLED,
        BUCKET_KEY_FIELD,
        BUCKET_MIN_SAMPLES,
        BUCKET_FALLBACK_TO_GLOBAL,
        FINAL_RANKING_MODE,
        # Step 4: 回撤阈值与持续性窗口配置
        SHORT_TERM_MODE_THRESHOLD_DAYS,
        DRAWDOWN_THRESHOLDS,
        SHADOW_DRAWDOWN_THRESHOLDS,
        CONSISTENCY_RETURN_COLS,
        # Step 5: 数据新鲜度与缺口检测配置
        FRESHNESS_CONFIG,
        FRESHNESS_REASON_CODES,
        # Step 7-4: 评分策略选择配置
        SCORING_POLICY,
        # Step 7-5: 运行时开关 + 回退阈值
        SCORING_POLICY_RUNTIME,
        SCORING_POLICY_GUARDS,
        # Step 7-8: 推荐策略注入
        SCORING_POLICY_RECOMMENDED,
    )
    from bond_fund_screening.runtime_overrides import get_risk_free_rate

    freshness_config = dict(FRESHNESS_CONFIG)
    if isinstance(freshness_config_override, dict):
        freshness_config.update({
            key: value for key, value in freshness_config_override.items()
            if value is not None
        })

    risk_free_rate_resolution = get_risk_free_rate(float(RISK_FREE_RATE))

    return {
        'thresholds': {
            'max_holding_days': MAX_HOLDING_DAYS,
            'min_establish_days': MIN_ESTABLISH_DAYS,
            'min_scale_baseline': MIN_SCALE_BASELINE,
            'scale_thresholds': SCALE_THRESHOLDS,
            'risk_metric_pass_count': RISK_METRIC_PASS_COUNT,
            'max_annual_return': MAX_ANNUAL_RETURN,
            'min_annual_return': MIN_ANNUAL_RETURN,
            'max_drawdown_threshold': MAX_DRAWDOWN_THRESHOLD,
            'consistency_pass_count': CONSISTENCY_PASS_COUNT,
            'min_absolute_return': MIN_ABSOLUTE_RETURN,
        },
        'risk_params': {
            'risk_free_rate': risk_free_rate_resolution.value,
            'risk_free_rate_baseline': RISK_FREE_RATE,
            'risk_free_rate_source': risk_free_rate_resolution.source,
            'risk_free_rate_updated_at': risk_free_rate_resolution.updated_at,
        },
        'scoring_weights': {
            'short_term': SHORT_TERM_WEIGHTS,
            'long_term': LONG_TERM_WEIGHTS,
            'combo': SCORE_COMBO_WEIGHTS,
        },
        # Step 3: 分桶配置
        'bucketing': {
            'enabled': BUCKETING_ENABLED,
            'bucket_key_field': BUCKET_KEY_FIELD,
            'min_samples': BUCKET_MIN_SAMPLES,
            'fallback_to_global': BUCKET_FALLBACK_TO_GLOBAL,
            'final_ranking_mode': FINAL_RANKING_MODE,
        },
        # Step 4: 回撤阈值与持续性窗口配置（可追溯化）
        'step4_traceability': {
            'short_term_mode_threshold_days': SHORT_TERM_MODE_THRESHOLD_DAYS,
            'drawdown_thresholds': DRAWDOWN_THRESHOLDS,
            'shadow_drawdown_thresholds': SHADOW_DRAWDOWN_THRESHOLDS,
            'consistency_return_cols': CONSISTENCY_RETURN_COLS,
        },
        # Step 5: 数据新鲜度与缺口检测配置（可追溯化）
        'step5_freshness': {
            'config': freshness_config,
            'reason_codes': FRESHNESS_REASON_CODES,
        },
        # Step 7-4/7-5/7-8/7-15: 评分策略 + 运行时开关 + 回退阈值 + 推荐注入 + override（可追溯化）
        'scoring_policy': {
            'policy': SCORING_POLICY,
            'runtime': SCORING_POLICY_RUNTIME,
            'guards': SCORING_POLICY_GUARDS,
            'recommended': SCORING_POLICY_RECOMMENDED,
            'override': _get_override_snapshot(),
        },
    }


def query_fund_decisions(fcode: str, run_id: str = None) -> List[Dict]:
    """
    查询基金的筛选决策路径

    Args:
        fcode: 基金代码
        run_id: 运行ID（可选，不指定则查最近一次）

    Returns:
        决策列表
    """
    db_manager = get_db_manager()

    with db_manager.get_session(task_name='query_decisions', readonly=True) as session:
        from database.screening_decision_payloads import begin_read, resolve_metric_texts

        connection = session.connection().connection.driver_connection
        begin_read(connection)
        query = session.query(ScreeningDecision).filter_by(fcode=fcode)

        if run_id:
            query = query.filter_by(run_id=run_id)
        else:
            # 获取最近一次运行
            latest_run = session.query(ScreeningRun).order_by(
                ScreeningRun.started_at.desc()
            ).first()
            if latest_run:
                query = query.filter_by(run_id=latest_run.run_id)

        decisions = query.order_by(ScreeningDecision.step_id).all()
        payloads = resolve_metric_texts(connection, [(decision.id, decision.metrics_json) for decision in decisions])

        return [
            {
                'step_id': d.step_id,
                'step_name': d.step_name,
                'passed': d.passed,
                'reason_code': d.reason_code,
                'reason_detail': d.reason_detail,
                'metrics': json.loads(payload) if payload else None,
            }
            for d, payload in zip(decisions, payloads)
        ]


def query_run_stats(run_id: str = None) -> Optional[Dict]:
    """
    查询筛选运行统计

    Args:
        run_id: 运行ID（可选，不指定则查最近一次）

    Returns:
        运行统计字典
    """
    db_manager = get_db_manager()

    with db_manager.get_session(task_name='query_run_stats', readonly=True) as session:
        if run_id:
            run = session.query(ScreeningRun).filter_by(run_id=run_id).first()
        else:
            run = session.query(ScreeningRun).order_by(
                ScreeningRun.started_at.desc()
            ).first()

        if not run:
            return None

        # 统计各原因代码的数量
        from sqlalchemy import func
        reason_stats = session.query(
            ScreeningDecision.reason_code,
            func.count(ScreeningDecision.id)
        ).filter_by(run_id=run.run_id).filter(
            ScreeningDecision.passed == False
        ).group_by(ScreeningDecision.reason_code).all()

        return {
            'run_id': run.run_id,
            'started_at': run.started_at.isoformat() if run.started_at else None,
            'finished_at': run.finished_at.isoformat() if run.finished_at else None,
            'status': run.status,
            'period_years': run.period_years,
            'fund_type': run.fund_type,
            'fund_count_in': run.fund_count_in,
            'fund_count_pass': run.fund_count_pass,
            'fund_count_fail': run.fund_count_fail,
            'nav_latest_date': run.nav_latest_date.isoformat() if run.nav_latest_date else None,
            'code_version': run.code_version,
            'reason_stats': {code: count for code, count in reason_stats},
        }
