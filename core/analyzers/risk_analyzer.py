"""
风险指标分析器
计算基金的各项风险指标：波动率、最大回撤、夏普比率等
"""

import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

from database import get_db_manager, Fund, FundNavHistory, FundRiskMetrics, FundReturns
from database.nav_history_queries import list_nav_history_fcodes
from utils import ProgressMonitor, build_nav_snapshot_signature
from utils.period_return_snapshot import RETURN_PROVENANCE_FIELDS, has_dated_period_snapshot
from bond_fund_screening.config import RISK_FREE_RATE as DEFAULT_RISK_FREE_RATE
from bond_fund_screening.core.nav_observation_quality import assess_nav_observation_quality
from bond_fund_screening.core.standard_risk_metrics import (
    calculate_standard_risk_metrics, annualize_compound_return,
    estimate_periods_per_year, calculate_calmar_ratio,
    return_observation_diagnostics,
)
from bond_fund_screening.runtime_overrides import get_risk_free_rate
from utils.calculation_capture import capture_call, capture_enabled, record_event
from utils.holding_period_policy import HOLDING_PERIOD_LIMIT, combine_holding_period_names, holding_period_gate_reason

logger = logging.getLogger(__name__)

UNIT_ACCUM_SCALE_MISMATCH_RATIO = 20.0
UNIT_ACCUM_SCALE_STABILITY_TOLERANCE = 0.2
RESOLVED_GAP_REGRESSION_TOLERANCE = 0.05
RESOLVED_GAP_END_TOLERANCE = 0.001
RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE = 0.005

_RISK_METRIC_WRITE_COLUMNS = (
    'fcode', 'period', 'volatility', 'max_drawdown', 'max_drawdown_duration', 'annual_return',
    'sharpe_ratio', 'sortino_ratio', 'raw_sharpe_ratio',
    'raw_sortino_ratio', 'sharpe_ratio_cap_hit', 'sortino_ratio_cap_hit',
    'calmar_ratio', 'var_95', 'var_99', 'positive_ratio', 'data_points',
    'start_date', 'end_date', 'nav_snapshot_signature', 'undefined_metric_reasons_json', 'formula_contract_json', 'calculated_at',
)

_RISK_METRIC_PRECISION = {
    'volatility': 6, 'max_drawdown': 6, 'annual_return': 4,
    'sharpe_ratio': 4, 'sortino_ratio': 4, 'raw_sharpe_ratio': 4,
    'raw_sortino_ratio': 4, 'calmar_ratio': 4, 'var_95': 6,
    'var_99': 6, 'positive_ratio': 2,
}

_RISK_CAPTURE_FIELDS = (
    'risk_free_rate', 'risk_free_rate_source', 'risk_free_rate_updated_at',
    'minimum_acceptable_return', 'trading_days', 'min_data_points', 'periods',
    'period_coverage_grace_days', 'max_metric_nav_age_days', 'max_metric_nav_lag_days',
    'api_return_alignment_grace_days', 'accum_nav_gap_regression_tolerance',
    'holding_period_skip_days',
)


class RiskAnalyzer:
    """风险指标分析器"""

    def __init__(self, config=None, db_manager=None):
        """初始化分析器"""
        self.config = config or {}
        self.db_manager = db_manager or get_db_manager()

        # 风险指标配置
        risk_config = self.config.get('risk_metrics', {})
        resolved_risk_free_rate = risk_config.get('risk_free_rate')
        resolved_risk_free_rate_source = risk_config.get('risk_free_rate_source')
        resolved_risk_free_rate_updated_at = risk_config.get('risk_free_rate_updated_at')
        if resolved_risk_free_rate is None:
            resolution = get_risk_free_rate(float(DEFAULT_RISK_FREE_RATE))
            self.risk_free_rate = float(resolution.value)
            self.risk_free_rate_source = resolution.source
            self.risk_free_rate_updated_at = resolution.updated_at
        elif resolved_risk_free_rate_source in {'runtime_overrides', 'env', 'default'}:
            self.risk_free_rate = float(resolved_risk_free_rate)
            self.risk_free_rate_source = str(resolved_risk_free_rate_source)
            self.risk_free_rate_updated_at = (
                str(resolved_risk_free_rate_updated_at)
                if resolved_risk_free_rate_updated_at
                else None
            )
        else:
            self.risk_free_rate = float(resolved_risk_free_rate)
            self.risk_free_rate_source = 'config_explicit'
            self.risk_free_rate_updated_at = None
        self.minimum_acceptable_return = risk_config.get('mar', 0.0)  # Sortino 使用的最低可接受回报（MAR）
        self.trading_days = risk_config.get('trading_days_per_year', 252)  # 年交易日
        self.min_data_points = risk_config.get('min_data_points', 30)  # 最少数据点
        self.periods = risk_config.get('periods', ['1y', '3y', '5y'])  # 计算周期
        self.period_coverage_grace_days = risk_config.get('period_coverage_grace_days', 7)  # 周期覆盖容差（自然日）
        self.max_metric_nav_age_days = risk_config.get('max_metric_nav_age_days')
        self.max_metric_nav_lag_days = risk_config.get('max_metric_nav_lag_days', 30)
        self.api_return_alignment_grace_days = risk_config.get('api_return_alignment_grace_days', 7)
        self.accum_nav_gap_regression_tolerance = float(
            risk_config.get('accum_nav_gap_regression_tolerance', 0.01)
        )
        self.holding_period_skip_days = int(
            risk_config.get(
                'skip_risk_metrics_for_holding_days_gt',
                self.config.get('exclude_holding_days_gt', 90),
            ) or 90
        )

        # 统计信息
        self.stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0
        }

    @capture_call('database_risk.calculate_all', attributes=_RISK_CAPTURE_FIELDS)
    def calculate_all_funds(self, workers=3, force_update=False, target_fcodes: Optional[List[str]] = None):
        """
        计算所有基金的风险指标（内存优化版本）

        优化策略：
        1. 分批读取：每批处理500只基金，避免内存溢出
        2. 并行计算：纯CPU计算，无数据库访问
        3. 批量写入：每批完成后立即写入数据库

        Args:
            workers: 并发数
            force_update: 强制重新计算（每日更新应设为True）
            target_fcodes: 指定仅计算这批基金；为空时默认处理全部有净值基金

        Returns:
            计算成功的基金数
        """
        import time
        import gc
        start_time = time.time()
        self.stats = {
            'success': 0,
            'failed': 0,
            'skipped': 0
        }

        logger.info("="*60)
        logger.info("开始批量计算所有基金的风险指标...")
        logger.info("="*60)

        # ========== 阶段0: 获取基金列表 ==========
        with self.db_manager.get_session() as session:
            all_fcodes = list_nav_history_fcodes(session)

        total_funds_with_nav = len(all_fcodes)
        record_event('database_risk.available_nav_codes', all_fcodes)
        logger.info(f"共{total_funds_with_nav}只基金有净值数据")

        if not all_fcodes:
            logger.warning("没有找到任何基金")
            return 0

        if target_fcodes:
            normalized_targets = []
            seen_targets = set()
            for raw_code in target_fcodes:
                code = str(raw_code or "").strip()
                if not code or code in seen_targets:
                    continue
                seen_targets.add(code)
                normalized_targets.append(code)
            available_fcodes = set(all_fcodes)
            all_fcodes = [code for code in normalized_targets if code in available_fcodes]
            logger.info(
                "定向风险指标重算模式：请求%s只，具备净值数据%s只",
                len(normalized_targets),
                len(all_fcodes),
            )

        scope_fund_total = len(all_fcodes)

        full_refresh = bool(force_update and not target_fcodes)
        full_refresh_staging = []

        # 非强制更新模式下过滤已有数据；全量 force 在全部计算和校验成功前
        # 不触碰正式表，避免长任务期间暴露部分更新或空表。
        if not force_update:
            # 修复：非强制更新模式下，过滤掉已有风险指标的基金
            with self.db_manager.get_session() as session:
                existing_fcodes = set(
                    f[0] for f in session.query(FundRiskMetrics.fcode).distinct().all()
                )
                record_event('database_risk.existing_risk_codes', sorted(existing_fcodes))
                all_fcodes = [f for f in all_fcodes if f not in existing_fcodes]
                skipped_count = scope_fund_total - len(all_fcodes)
                if skipped_count > 0:
                    logger.info(f"跳过{skipped_count}只已有风险指标的基金")

        total_funds = len(all_fcodes)
        record_event('database_risk.calculation_targets', {
            'fcodes': all_fcodes, 'full_refresh': full_refresh,
            'scope_fund_total': scope_fund_total,
            'reuse_policy': 'Existing rows outside the actual calculations are not claimed to have newly frozen original inputs.',
        })
        if not all_fcodes:
            logger.info("所有基金已有风险指标，无需重新计算")
            return 0

        logger.info(f"需要计算{total_funds}只基金的风险指标")

        # ========== 分批处理 ==========
        batch_size = 500  # 每批处理500只基金
        total_metrics = 0
        read_time = 0
        calc_time = 0
        write_time = 0

        with ProgressMonitor(total_funds, desc="计算风险指标", unit="只基金") as progress:
            for batch_start in range(0, total_funds, batch_size):
                batch_end = min(batch_start + batch_size, total_funds)
                batch_fcodes = all_fcodes[batch_start:batch_end]
                batch_num = batch_start // batch_size + 1
                total_batches = (total_funds + batch_size - 1) // batch_size

                # ----- 读取本批次净值数据和API收益率 -----
                phase1_start = time.time()
                nav_data_by_fund = {}
                api_returns_by_fund = {}  # 新增：存储API收益率数据

                with self.db_manager.get_session() as session:
                    fund_gate_map = self._load_fund_risk_gate_map(session, batch_fcodes)
                    # 读取净值数据（包括单位净值、累计净值和日涨幅）
                    for fcode in batch_fcodes:
                        if self._should_skip_risk_metrics_for_fund(fund_gate_map.get(fcode)):
                            continue
                        records = session.query(
                            FundNavHistory.nav_date,
                            FundNavHistory.unit_nav,
                            FundNavHistory.accum_nav,
                            FundNavHistory.daily_growth
                        ).filter_by(
                            fcode=fcode
                        ).filter(
                            FundNavHistory.unit_nav.isnot(None)
                        ).order_by(
                            FundNavHistory.nav_date
                        ).all()

                        if records and (len(records) >= self.min_data_points or capture_enabled()):
                            df = pd.DataFrame(records, columns=['date', 'nav', 'accum_nav', 'daily_growth'])
                            df['date'] = pd.to_datetime(df['date'])
                            df = df.set_index('date').sort_index()
                            record_event('database_risk.nav_query', {'fcode': fcode, 'nav': df})
                            if len(records) >= self.min_data_points:
                                nav_data_by_fund[fcode] = df
                        elif capture_enabled():
                            record_event('database_risk.nav_query', {'fcode': fcode, 'nav': None, 'returned_rows': 0})

                    # 读取API收益率数据（批量查询）
                    api_returns_by_fund = self._load_api_returns_map(session, batch_fcodes)

                read_time += time.time() - phase1_start

                # ----- 并行计算本批次 -----
                phase2_start = time.time()
                batch_metrics = []
                completed_fcodes = set()

                @capture_call('database_risk.fund')
                def calculate_single_fund(fcode):
                    if self._should_skip_risk_metrics_for_fund(fund_gate_map.get(fcode)):
                        return []
                    nav_data = nav_data_by_fund.get(fcode)
                    if nav_data is None:
                        return []
                    # 获取API收益率数据
                    api_returns = api_returns_by_fund.get(fcode, {})
                    nav_snapshot_signature = build_nav_snapshot_signature(api_returns)
                    results = []
                    for period in self.periods:
                        metrics = self._calculate_period_metrics(
                            nav_data,
                            period,
                            api_return=api_returns.get(period),
                            api_return_date=api_returns.get('api_return_date'),
                            api_snapshot_date=api_returns.get('snapshot_date'),
                            nav_snapshot_signature=nav_snapshot_signature,
                        )
                        if metrics:
                            results.append((fcode, period, metrics))
                    return results

                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = {executor.submit(calculate_single_fund, fcode): fcode
                              for fcode in batch_fcodes}

                    for future in as_completed(futures):
                        fcode = futures[future]
                        try:
                            results = future.result()
                            completed_fcodes.add(fcode)
                            if results:
                                batch_metrics.extend(results)
                                self.stats['success'] += 1
                                progress.update(1, success=True)
                            else:
                                self.stats['skipped'] += 1
                                progress.update(1, skipped=True)
                        except Exception as e:
                            logger.error(f"计算基金{fcode}失败: {e}")
                            self.stats['failed'] += 1
                            progress.update(1, success=False)

                calc_time += time.time() - phase2_start

                # ----- 批量写入本批次结果 -----
                if batch_metrics or (force_update and target_fcodes and completed_fcodes):
                    phase3_start = time.time()
                    now = datetime.now()
                    metrics_objects = []

                    for fcode, period, metrics in batch_metrics:
                        metrics_objects.append(self._metric_write_record(fcode, period, metrics, now))

                    if full_refresh:
                        full_refresh_staging.extend(metrics_objects)
                    else:
                        with self.db_manager.get_session(task_name='replace_risk_metric_batch') as session:
                            if force_update and target_fcodes:
                                replace_fcodes = sorted(completed_fcodes)
                                session.query(FundRiskMetrics).filter(
                                    FundRiskMetrics.fcode.in_(replace_fcodes)
                                ).delete(synchronize_session=False)
                            else:
                                batch_fcodes_in_metrics = list(set(m['fcode'] for m in metrics_objects))
                                if batch_fcodes_in_metrics:
                                    session.query(FundRiskMetrics).filter(
                                        FundRiskMetrics.fcode.in_(batch_fcodes_in_metrics)
                                    ).delete(synchronize_session=False)

                            session.bulk_insert_mappings(FundRiskMetrics, metrics_objects)

                    total_metrics += len(metrics_objects)
                    write_time += time.time() - phase3_start

                # 释放内存
                del nav_data_by_fund
                del api_returns_by_fund
                del batch_metrics
                gc.collect()

        if full_refresh:
            if self.stats['failed']:
                self._write_full_refresh_audit(
                    status='failed_preserved_previous',
                    staged_count=len(full_refresh_staging),
                    expected_fund_count=total_funds,
                    error=f"risk calculation failed for {self.stats['failed']} funds",
                )
                raise RuntimeError(
                    f"全量风险重算有{self.stats['failed']}只基金计算失败，正式风险快照保持不变"
                )
            if total_funds > 0 and not full_refresh_staging:
                self._write_full_refresh_audit(
                    status='failed_preserved_previous',
                    staged_count=0,
                    expected_fund_count=total_funds,
                    error='staging result is empty',
                )
                raise RuntimeError("全量风险重算 staging 为空，正式风险快照保持不变")
            self._replace_all_metrics_atomically(full_refresh_staging)

        # ========== 汇总 ==========
        total_time = time.time() - start_time
        logger.info("="*60)
        logger.info("批量计算完成汇总")
        logger.info("="*60)
        logger.info(f"  总基金数: {total_funds}")
        logger.info(f"  计算成功: {self.stats['success']}")
        logger.info(f"  跳过: {self.stats['skipped']}")
        logger.info(f"  失败: {self.stats['failed']}")
        logger.info(f"  风险指标: {total_metrics}条")
        logger.info(f"  总耗时: {total_time:.1f}秒 ({total_time/60:.1f}分钟)")
        logger.info(f"    - 读取: {read_time:.1f}秒")
        logger.info(f"    - 计算: {calc_time:.1f}秒")
        logger.info(f"    - 写入: {write_time:.1f}秒")
        logger.info("="*60)

        record_event('database_risk.completed_statistics', {'stats': self.stats, 'metrics': total_metrics})
        return total_metrics

    @staticmethod
    def _serialize_staging_value(value):
        if isinstance(value, (datetime, pd.Timestamp)):
            return value.isoformat(sep=' ')
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        return value

    @staticmethod
    @capture_call('database_risk.write_record')
    def _metric_write_record(fcode: str, period: str, metrics: Dict, calculated_at: datetime) -> Dict:
        """Serialize all risk writers identically, retaining undefined ratios as NULL."""
        values = {
            field: None if metrics[field] is None else round(metrics[field], digits)
            for field, digits in _RISK_METRIC_PRECISION.items()
        }
        reasons = metrics.get('undefined_metric_reasons')
        return {
            'fcode': fcode,
            'period': period,
            **values,
            'max_drawdown_duration': metrics.get('max_drawdown_duration'),
            'sharpe_ratio_cap_hit': (
                None if metrics['raw_sharpe_ratio'] is None else bool(metrics['sharpe_ratio_cap_hit'])
            ),
            'sortino_ratio_cap_hit': (
                None if metrics['raw_sortino_ratio'] is None else bool(metrics['sortino_ratio_cap_hit'])
            ),
            'data_points': metrics['data_points'],
            'start_date': metrics['start_date'].date(),
            'end_date': metrics['end_date'].date(),
            'nav_snapshot_signature': str(metrics.get('nav_snapshot_signature') or '').strip() or None,
            'undefined_metric_reasons_json': (
                json.dumps(reasons, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
                if reasons is not None else None
            ),
            'formula_contract_json': (json.dumps(metrics['formula_contract'], ensure_ascii=False, sort_keys=True, allow_nan=False)
                                      if metrics.get('formula_contract') is not None else None),
            'calculated_at': calculated_at,
        }

    @capture_call('database_risk.atomic_replace')
    def _replace_all_metrics_atomically(self, metrics_objects: List[Dict]) -> None:
        """通过同连接 TEMP staging 表校验后，单事务替换正式风险快照。"""
        column_sql = ', '.join(_RISK_METRIC_WRITE_COLUMNS)
        value_sql = ', '.join(f':{column}' for column in _RISK_METRIC_WRITE_COLUMNS)
        staging_rows = [
            {
                column: self._serialize_staging_value(item.get(column))
                for column in _RISK_METRIC_WRITE_COLUMNS
            }
            for item in metrics_objects
        ]
        expected_count = len(staging_rows)
        with self.db_manager.get_session(task_name='atomic_replace_all_risk_metrics') as session:
            previous_count = int(session.query(FundRiskMetrics).count())
            session.execute(text('DROP TABLE IF EXISTS temp.fund_risk_metrics_staging'))
            session.execute(text(
                f'CREATE TEMP TABLE fund_risk_metrics_staging AS '
                f'SELECT {column_sql} FROM fund_risk_metrics WHERE 0'
            ))
            session.execute(
                text(f'INSERT INTO fund_risk_metrics_staging ({column_sql}) VALUES ({value_sql})'),
                staging_rows,
            )
            staged_count = int(session.execute(
                text('SELECT COUNT(*) FROM fund_risk_metrics_staging')
            ).scalar_one())
            duplicate_count = int(session.execute(text(
                'SELECT COUNT(*) FROM ('
                ' SELECT fcode, period FROM fund_risk_metrics_staging'
                ' GROUP BY fcode, period HAVING COUNT(*) > 1'
                ')'
            )).scalar_one())
            invalid_identity_count = int(session.execute(text(
                "SELECT COUNT(*) FROM fund_risk_metrics_staging "
                "WHERE TRIM(COALESCE(fcode, '')) = '' OR TRIM(COALESCE(period, '')) = ''"
            )).scalar_one())
            if staged_count != expected_count or duplicate_count or invalid_identity_count:
                raise RuntimeError(
                    '风险 staging 校验失败: '
                    f'expected={expected_count}, staged={staged_count}, '
                    f'duplicates={duplicate_count}, invalid_identity={invalid_identity_count}'
                )

            session.query(FundRiskMetrics).delete(synchronize_session=False)
            session.execute(text(
                f'INSERT INTO fund_risk_metrics ({column_sql}) '
                f'SELECT {column_sql} FROM fund_risk_metrics_staging'
            ))
            final_count = int(session.query(FundRiskMetrics).count())
            if final_count != staged_count:
                raise RuntimeError(
                    f'风险正式表替换后数量不一致: staged={staged_count}, final={final_count}'
                )
            session.execute(text('DROP TABLE fund_risk_metrics_staging'))

        self._write_full_refresh_audit(
            status='success_atomic_replaced',
            staged_count=expected_count,
            expected_fund_count=None,
            previous_count=previous_count,
            final_count=expected_count,
        )
        logger.info(
            '全量风险快照已原子替换: previous=%s, staged=%s, final=%s',
            previous_count,
            expected_count,
            expected_count,
        )

    def _write_full_refresh_audit(
        self,
        *,
        status: str,
        staged_count: int,
        expected_fund_count: Optional[int],
        previous_count: Optional[int] = None,
        final_count: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        generated_at = datetime.now()
        audit_dir = Path(__file__).resolve().parents[2] / 'reports' / 'audit' / 'risk_full_refresh'
        try:
            audit_dir.mkdir(parents=True, exist_ok=True)
            artifact_path = audit_dir / f"risk_full_refresh_{generated_at.strftime('%Y%m%d_%H%M%S_%f')}.json"
            artifact_path.write_text(json.dumps({
                'schema_version': 1,
                'artifact_type': 'risk_full_refresh_atomic_swap',
                'generated_at': generated_at.isoformat(timespec='seconds'),
                'status': status,
                'previous_count': previous_count,
                'staged_count': int(staged_count),
                'final_count': final_count,
                'expected_fund_count': expected_fund_count,
                'failed_fund_count': int(self.stats.get('failed') or 0),
                'error': error,
                'truth_policy': 'formal_table_is_unchanged_until_staging_validation_and_atomic_swap_succeed',
            }, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception as exc:
            logger.warning('写入全量风险原子替换审计 artifact 失败: %s', exc)

    @capture_call('database_risk.calculate_fund', attributes=_RISK_CAPTURE_FIELDS)
    def calculate_fund_risk(self, fcode: str, force_update=False) -> int:
        """
        计算单只基金的风险指标

        Args:
            fcode: 基金代码
            force_update: 强制重新计算

        Returns:
            计算的指标数量
        """
        with self.db_manager.get_session(readonly=True) as session:
            # 如果不强制更新，检查是否已存在
            if not force_update:
                existing = session.query(FundRiskMetrics).filter_by(fcode=fcode).first()
                if existing:
                    logger.debug(f"基金{fcode}风险指标已存在，跳过")
                    return 0
            fund_gate_map = self._load_fund_risk_gate_map(session, [fcode])

        if self._should_skip_risk_metrics_for_fund(fund_gate_map.get(fcode)):
            logger.debug(
                "基金%s命中长持有定开债风险跳过规则: holding_days>%s",
                fcode,
                self.holding_period_skip_days,
            )
            if force_update:
                self._replace_fund_metrics(fcode, [])
            return 0

        # 获取净值数据
        nav_data = self._get_nav_data(fcode)
        if nav_data is None or len(nav_data) < self.min_data_points:
            logger.debug(f"基金{fcode}净值数据不足（需要>{self.min_data_points}条）")
            if force_update:
                self._replace_fund_metrics(fcode, [])
            return 0

        with self.db_manager.get_session(readonly=True) as session:
            api_returns_map = self._load_api_returns_map(session, [fcode])
        api_returns = api_returns_map.get(fcode, {})
        nav_snapshot_signature = build_nav_snapshot_signature(api_returns)

        # 计算各周期的风险指标
        metrics_by_period = []
        for period in self.periods:
            metrics = self._calculate_period_metrics(
                nav_data,
                period,
                api_return=api_returns.get(period),
                api_return_date=api_returns.get('api_return_date'),
                api_snapshot_date=api_returns.get('snapshot_date'),
                nav_snapshot_signature=nav_snapshot_signature,
            )
            if metrics:
                metrics_by_period.append((period, metrics))

        if force_update:
            self._replace_fund_metrics(fcode, metrics_by_period)
        else:
            for period, metrics in metrics_by_period:
                self._save_metrics(fcode, period, metrics)

        return len(metrics_by_period)

    @capture_call('database_risk.nav_query_single')
    def _get_nav_data(self, fcode: str) -> Optional[pd.DataFrame]:
        """
        获取基金净值数据

        Args:
            fcode: 基金代码

        Returns:
            DataFrame包含日期、单位净值和累计净值
        """
        with self.db_manager.get_session() as session:
            records = session.query(
                FundNavHistory.nav_date,
                FundNavHistory.unit_nav,
                FundNavHistory.accum_nav,
                FundNavHistory.daily_growth
            ).filter_by(
                fcode=fcode
            ).filter(
                FundNavHistory.unit_nav.isnot(None)
            ).order_by(
                FundNavHistory.nav_date
            ).all()

            if not records:
                return None

            df = pd.DataFrame(records, columns=['date', 'nav', 'accum_nav', 'daily_growth'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            df = df.sort_index()

            return df

    @capture_call('database_risk.period', attributes=_RISK_CAPTURE_FIELDS)
    def _calculate_period_metrics(
        self,
        nav_data: pd.DataFrame,
        period: str,
        api_return: Optional[float] = None,
        api_return_date=None,
        api_snapshot_date=None,
        nav_snapshot_signature: str = "",
    ) -> Optional[Dict]:
        """
        计算指定周期的风险指标

        Args:
            nav_data: 净值数据（包含nav和accum_nav列）
            period: 计算周期（1y/3y/5y）
            api_return: API提供的累计收益率（优先使用）
            api_return_date: API周期收益独立业务基准日；未知时不使用该收益
            api_snapshot_date: 最新NAV业务日期；行更新时间不是业务日期

        Returns:
            风险指标字典
        """
        # 确定计算周期的天数
        days = self._get_period_days(period)
        if not days:
            return None

        # 截取指定周期的数据
        end_date = nav_data.index.max()
        if self._is_nav_data_too_stale(end_date):
            logger.debug(
                "基金风险指标跳过: nav_end_date=%s, max_metric_nav_age_days=%s",
                end_date,
                self.max_metric_nav_age_days,
            )
            return None
        if self._is_nav_stale_against_snapshot(end_date, api_snapshot_date):
            logger.debug(
                "基金风险指标跳过: nav_end_date=%s, api_snapshot_date=%s, max_metric_nav_lag_days=%s",
                end_date,
                api_snapshot_date,
                self.max_metric_nav_lag_days,
            )
            return None
        if self._is_1y_window_stale_against_snapshot(period, end_date, api_snapshot_date):
            logger.debug(
                "基金风险指标跳过: period=%s, nav_end_date=%s, api_snapshot_date=%s, lag_days>%s",
                period,
                end_date,
                api_snapshot_date,
                self.api_return_alignment_grace_days,
            )
            return None

        start_date = end_date - timedelta(days=days)
        period_window = nav_data[nav_data.index >= start_date].copy()

        observation_quality = assess_nav_observation_quality(
            period_window.index,
            period_days=days,
            coverage_grace_days=self.period_coverage_grace_days,
        )

        if observation_quality["duplicate_date_count"] > 0:
            logger.debug(
                "基金风险指标跳过: period=%s, duplicated_nav_dates=%s",
                period,
                [
                    str(pd.Timestamp(value).date())
                    for value in period_window.index[period_window.index.duplicated()].unique()
                ][:5],
            )
            return None

        if len(period_window) < self.min_data_points:
            return None

        coverage_days = int(observation_quality["coverage_days"])
        if not observation_quality["coverage_sufficient"]:
            logger.debug(
                "基金风险指标跳过: period=%s, coverage_days=%s, required_days=%s, grace=%s",
                period,
                coverage_days,
                observation_quality["required_coverage_days"],
                self.period_coverage_grace_days,
            )
            return None

        # 周期年数映射（用于年化收益率计算）
        years_map = {'1y': 1, '3y': 3, '5y': 5}
        period_years = years_map.get(period, 1)
        actual_years = max(coverage_days / 365.25, 1 / 365.25)

        # 优先使用 daily_growth 列计算日收益率
        # daily_growth 由API直接提供（已消除分红除息影响），精度高于 pct_change(accum_nav)
        use_daily_growth = False
        returns_series = None
        nav_series = None
        if self._can_use_daily_growth_series(period_window, actual_years):
            dg = pd.to_numeric(period_window['daily_growth'], errors='coerce')
            # 排除的是窗口首个净值点的区间外收益，不是首个非空收益。
            # 首点为缺失时，先 dropna 会把窗口内第一条真实收益一起丢掉。
            candidate_returns = dg.iloc[1:].dropna() / 100
            if len(candidate_returns) >= self.min_data_points:
                returns_series = candidate_returns
                use_daily_growth = True

        if not use_daily_growth:
            # 回退：选择净值列计算
            nav_column = 'accum_nav' if self._has_reliable_accum_nav_series(period_window) else 'nav'
            if nav_column not in period_window:
                return None
            nav_series = pd.to_numeric(period_window[nav_column], errors='coerce')
            if not np.isfinite(nav_series).all() or nav_series.le(0).any():
                return None
            returns_series = nav_series.pct_change().dropna()

        if returns_series is None or len(returns_series) < self.min_data_points:
            return None

        returns = returns_series.values
        periods_per_year = self._estimate_periods_per_year(len(returns), actual_years)

        # 基于 daily_growth 重建累计净值序列用于最大回撤计算
        if use_daily_growth:
            nav_values = np.concatenate([[1.0], np.cumprod(1 + returns)])
        else:
            nav_values = nav_series.values

        # 计算各项指标
        metrics = {}

        # 1. 年化收益率 - 优先使用API提供的累计收益率
        aligned_api_return = self._resolve_aligned_api_return(
            api_return=api_return,
            api_return_date=api_return_date,
            nav_end_date=end_date,
            period=period,
        )
        if aligned_api_return is not None and not np.isnan(aligned_api_return):
            # API返回的是累计收益率，��换为年化收益率
            # 公式: 年化收益率 = (1 + 累计收益率)^(1/年数) - 1
            cumulative_return = aligned_api_return / 100  # 转换为小数
            if period_years > 1 and cumulative_return > -1:
                metrics['annual_return'] = ((1 + cumulative_return) ** (1 / period_years) - 1) * 100
            else:
                metrics['annual_return'] = aligned_api_return  # 1年期直接使用
        else:
            # 回退到净值计算
            total_return = np.prod(1 + returns) - 1
            annual = annualize_compound_return(float(total_return), coverage_days)
            if annual is None:
                return None
            metrics['annual_return'] = annual * 100

        metrics.update(calculate_standard_risk_metrics(
            returns, nav_values,
            annual_return=metrics['annual_return'] / 100,
            periods_per_year=periods_per_year,
            risk_free_rate=self.risk_free_rate,
            mar=self.minimum_acceptable_return,
        ))
        # Preserve the persisted database contract: positive Sharpe is capped
        # at the display ceiling; negative values remain unbounded so a large
        # loss is not flattened to the same value as a milder decline.
        raw_sharpe = metrics['raw_sharpe_ratio']
        metrics['sharpe_ratio'] = min(raw_sharpe, 5.0) if raw_sharpe is not None else None
        metrics['sharpe_ratio_cap_hit'] = None if raw_sharpe is None else raw_sharpe > 5.0
        # 6. 卡玛比率（max_drawdown 已是小数形式）
        max_drawdown = metrics['max_drawdown']
        metrics['calmar_ratio'] = calculate_calmar_ratio(metrics['annual_return'] / 100, max_drawdown)
        metrics['standard_calmar_ratio'] = metrics['calmar_ratio']
        if metrics['calmar_ratio'] is None:
            metrics['undefined_metric_reasons']['calmar_ratio'] = (
                'zero_drawdown' if max_drawdown == 0 else 'invalid_observations'
            )

        # 7. VaR 95% 和 99%（小数形式）
        metrics['var_95'] = np.percentile(returns, 5)
        metrics['var_99'] = np.percentile(returns, 1)

        # 8. 正收益比率
        positive_days = np.sum(returns > 0)
        metrics['positive_ratio'] = (positive_days / len(returns)) * 100 if len(returns) > 0 else 0

        # 元数据
        metrics['data_points'] = len(returns)
        # 覆盖窗口应反映实际参与本周期判断的净值区间，而不是去掉首日收益后的收益率区间。
        metrics['start_date'] = period_window.index.min()
        metrics['end_date'] = period_window.index.max()
        metrics['nav_snapshot_signature'] = str(nav_snapshot_signature or '').strip()

        from bond_fund_screening.core.metric_formula_contract import strategy_formula_contract
        from bond_fund_screening.core.trading_calendar import continuity_evidence
        dg_input = pd.to_numeric(period_window.get('daily_growth', pd.Series(dtype=float)), errors='coerce').iloc[1:]
        continuity = continuity_evidence(period_window.index)
        verified = bool(use_daily_growth and len(dg_input) == len(returns)
                        and np.isfinite(dg_input).all() and dg_input.gt(-100).all()
                        and continuity['verified'])
        metrics['formula_contract'] = strategy_formula_contract(
            engine='database', periods_per_year=periods_per_year, risk_free_rate=self.risk_free_rate,
            mar=self.minimum_acceptable_return, return_source='daily_growth' if use_daily_growth else nav_column,
            verified=verified, annual_return_source='aligned_api_period_return' if aligned_api_return is not None else 'local_compound_return',
        )
        metrics['formula_contract']['date_continuity'] = continuity
        metrics['formula_contract']['observation_diagnostics'] = return_observation_diagnostics(returns)
        return metrics

    @staticmethod
    def _get_period_days(period: str) -> Optional[int]:
        """将周期标签映射为自然日天数。"""
        return {
            '1y': 365,
            '3y': 365 * 3,
            '5y': 365 * 5,
        }.get(period)

    @staticmethod
    def _calculate_window_span_days(period_window: pd.DataFrame) -> int:
        """计算窗口自然日跨度。"""
        if period_window is None or period_window.empty:
            return 0
        return max((period_window.index.max() - period_window.index.min()).days, 0)

    def _estimate_periods_per_year(self, observation_count: int, actual_years: float) -> float:
        """
        根据实际样本频率估算年化观测次数。

        说明：
        - 日频数据会接近 252；
        - 周频/低频数据会自动降到对应观测频率，避免把周收益错误按日收益年化；
        - 上限仍限制为配置交易日，避免异常密集数据放大波动率。
        """
        return estimate_periods_per_year(observation_count, actual_years, self.trading_days)

    def _can_use_daily_growth_series(
        self,
        period_window: pd.DataFrame,
        actual_years: float,
    ) -> bool:
        """
        判断当前窗口是否适合直接使用 daily_growth 作为收益序列。

        仅在样本密度接近日频时启用，避免周频/稀疏净值样本把单日涨跌幅误当成
        区间收益，进而把波动率压得过低并放大 Sharpe/Sortino。
        """
        if period_window is None or period_window.empty or actual_years <= 0:
            return False
        if 'daily_growth' not in period_window.columns:
            return False

        dg = pd.to_numeric(period_window['daily_growth'].iloc[1:], errors='coerce')
        if len(dg) < self.min_data_points or not np.isfinite(dg).all() or dg.le(-100).any():
            return False

        estimated_points_per_year = len(dg) / actual_years
        minimum_daily_like_points = min(float(self.trading_days), 180.0)
        return estimated_points_per_year >= minimum_daily_like_points

    def _has_reliable_accum_nav_series(self, period_window: pd.DataFrame) -> bool:
        """仅在累计净值与单位净值关系稳定时才回退使用累计净值。"""
        if period_window is None or period_window.empty or 'accum_nav' not in period_window.columns:
            return False

        accum = pd.to_numeric(period_window['accum_nav'], errors='coerce')
        if accum.isna().any() or (accum <= 0).any():
            return False

        unit_column = 'nav' if 'nav' in period_window.columns else None
        if unit_column is None and 'unit_nav' in period_window.columns:
            unit_column = 'unit_nav'
        if unit_column is None:
            return True

        unit = pd.to_numeric(period_window[unit_column], errors='coerce')
        if unit.isna().any() or (unit <= 0).any():
            return True

        if self._has_stable_unit_accum_scale_mismatch(unit, accum):
            return True

        dividend_gap = accum - unit
        if dividend_gap.isna().any() or (dividend_gap < -1e-6).any():
            return False

        max_regression = float((dividend_gap.cummax() - dividend_gap).max() or 0.0)
        if max_regression <= self.accum_nav_gap_regression_tolerance:
            return True

        return self._has_resolved_small_gap_regression(
            unit,
            accum,
            dividend_gap=dividend_gap,
            max_regression=max_regression,
            regression_tolerance=self.accum_nav_gap_regression_tolerance,
        )

    @staticmethod
    def _has_stable_unit_accum_scale_mismatch(
        unit: pd.Series,
        accum: pd.Series,
        *,
        mismatch_ratio: float = UNIT_ACCUM_SCALE_MISMATCH_RATIO,
        stability_tolerance: float = UNIT_ACCUM_SCALE_STABILITY_TOLERANCE,
    ) -> bool:
        scale_df = pd.DataFrame({"unit_nav": unit, "accum_nav": accum}).dropna()
        if scale_df.empty:
            return False

        scale_df = scale_df[(scale_df["unit_nav"] > 0) & (scale_df["accum_nav"] > 0)]
        if scale_df.empty:
            return False

        ratio_series = scale_df["unit_nav"] / scale_df["accum_nav"]
        ratio_median = float(ratio_series.median()) if not ratio_series.empty else None
        if ratio_median is None or ratio_median <= 0:
            return False

        if len(ratio_series) >= 3:
            ratio_p10 = float(ratio_series.quantile(0.10))
            ratio_p90 = float(ratio_series.quantile(0.90))
        else:
            ratio_p10 = ratio_median
            ratio_p90 = ratio_median

        ratio_band = (ratio_p90 - ratio_p10) / ratio_median if ratio_median > 0 else None
        return bool(
            ratio_median >= float(mismatch_ratio)
            and ratio_band is not None
            and ratio_band <= float(stability_tolerance)
        )

    @staticmethod
    def _has_resolved_small_gap_regression(
        unit: pd.Series,
        accum: pd.Series,
        *,
        dividend_gap: pd.Series,
        max_regression: float,
        regression_tolerance: float,
        resolved_regression_tolerance: float = RESOLVED_GAP_REGRESSION_TOLERANCE,
        resolved_gap_end_tolerance: float = RESOLVED_GAP_END_TOLERANCE,
        total_return_diff_tolerance: float = RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE,
    ) -> bool:
        if max_regression > max(float(resolved_regression_tolerance), float(regression_tolerance)):
            return False

        try:
            final_gap = abs(float(dividend_gap.iloc[-1]))
            unit_start = float(unit.iloc[0])
            unit_end = float(unit.iloc[-1])
            accum_start = float(accum.iloc[0])
            accum_end = float(accum.iloc[-1])
        except (TypeError, ValueError, IndexError):
            return False

        if min(unit_start, unit_end, accum_start, accum_end) <= 0:
            return False
        if final_gap > max(float(resolved_gap_end_tolerance), float(regression_tolerance)):
            return False

        unit_total_return = (unit_end / unit_start) - 1.0
        accum_total_return = (accum_end / accum_start) - 1.0
        return abs(unit_total_return - accum_total_return) <= float(total_return_diff_tolerance)

    def _is_nav_data_too_stale(self, nav_end_date) -> bool:
        """判断本地净值末日是否已过旧，不应继续生成“当前风险指标”。"""
        max_age_days = self.max_metric_nav_age_days
        if max_age_days is None:
            return False

        try:
            max_age_days = int(max_age_days)
        except (TypeError, ValueError):
            return False

        if max_age_days < 0:
            return False

        nav_end = pd.Timestamp(nav_end_date).date()
        evaluated_at = datetime.now()
        record_event('database_risk.freshness_clock', {
            'evaluated_at': evaluated_at, 'nav_end': nav_end, 'max_age_days': max_age_days,
        })
        age_days = (evaluated_at.date() - nav_end).days
        return age_days > max_age_days

    def _resolve_aligned_api_return(self, api_return, api_return_date, nav_end_date, period: str):
        """仅在 API 收益率与本地净值末日对齐时使用 API 收益率。"""
        if api_return is None:
            return None

        if api_return_date is None:
            logger.debug("基金风险指标忽略未带日期的API收益率: period=%s", period)
            return None

        api_date = pd.Timestamp(api_return_date).date()
        nav_end = pd.Timestamp(nav_end_date).date()
        date_gap = abs((api_date - nav_end).days)
        if date_gap > self.api_return_alignment_grace_days:
            logger.debug(
                "基金风险指标忽略日期未对齐的API收益率: period=%s, api_date=%s, nav_end_date=%s, gap_days=%s, grace=%s",
                period,
                api_date,
                nav_end,
                date_gap,
                self.api_return_alignment_grace_days,
            )
            return None

        return api_return

    def _is_nav_stale_against_snapshot(self, nav_end_date, api_snapshot_date) -> bool:
        """判断本地净值是否明显落后于当前 API 快照。"""
        if api_snapshot_date is None:
            return False

        try:
            snapshot_date = pd.Timestamp(api_snapshot_date).date()
            nav_end = pd.Timestamp(nav_end_date).date()
        except Exception:
            return False

        lag_days = (snapshot_date - nav_end).days
        return lag_days > self.max_metric_nav_lag_days

    def _is_1y_window_stale_against_snapshot(self, period: str, nav_end_date, api_snapshot_date) -> bool:
        """1年期口径必须与最新快照基本对齐，避免生成旧窗口收益指标。"""
        if period != '1y' or api_snapshot_date is None:
            return False

        try:
            snapshot_date = pd.Timestamp(api_snapshot_date).date()
            nav_end = pd.Timestamp(nav_end_date).date()
        except Exception:
            return False

        lag_days = (snapshot_date - nav_end).days
        return lag_days > self.api_return_alignment_grace_days

    def _calculate_max_drawdown(self, nav_values: np.ndarray) -> float:
        """
        计算最大回撤

        Args:
            nav_values: 净值序列

        Returns:
            最大回撤（小数形式，如 0.0837 表示 8.37% 的回撤）
        """
        cummax = np.maximum.accumulate(nav_values)
        drawdown = (nav_values - cummax) / cummax
        # 返回小数形式（与 risk_calculator.py 统一）
        return abs(np.min(drawdown))

    @staticmethod
    @capture_call('database_risk.api_returns', omit=('session',))
    def _load_api_returns_map(session, fcodes: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
        """批量读取 API 收益率，确保单基金/全量计算口径一致。"""
        normalized_fcodes = [str(fcode).strip() for fcode in (fcodes or []) if fcode]
        if not normalized_fcodes:
            return {}

        api_returns = session.query(FundReturns).filter(
            FundReturns.fcode.in_(normalized_fcodes)
        ).all()
        if capture_enabled():
            columns = [column.name for column in FundReturns.__table__.columns]
            record_event('database_risk.raw_period_return_rows', {
                'columns': columns,
                'rows': [[getattr(row, name) for name in columns] for row in api_returns],
            })
        return {
            ret.fcode: {
                'latest_nav': ret.latest_nav,
                'daily_growth': ret.daily_growth,
                'return_1w': ret.return_1w,
                'return_1m': ret.return_1m,
                'return_3m': ret.return_3m,
                'return_6m': ret.return_6m,
                'return_1y': ret.return_1y,
                'return_2y': ret.return_2y,
                'return_3y': ret.return_3y,
                'return_5y': ret.return_5y,
                'return_ytd': ret.return_ytd,
                'return_since': ret.return_since,
                '1y': ret.return_1y,
                '3y': ret.return_3y,
                '5y': ret.return_5y,
                'latest_nav_date': ret.latest_nav_date,
                'period_return_date': ret.period_return_date,
                'api_return_date': ret.period_return_date if has_dated_period_snapshot(
                    {field: getattr(ret, field) for field in RETURN_PROVENANCE_FIELDS}, verify_payload=True,
                ) else None,
                'snapshot_date': ret.latest_nav_date,
            }
            for ret in api_returns
        }

    @capture_call('database_risk.fund_gates', omit=('session',))
    def _load_fund_risk_gate_map(self, session, fcodes: List[str]) -> Dict[str, Dict[str, Any]]:
        normalized_fcodes = [str(fcode).strip() for fcode in (fcodes or []) if str(fcode).strip()]
        if not normalized_fcodes:
            return {}

        rows = session.query(
            Fund.fcode,
            Fund.has_holding_period,
            Fund.holding_days,
            Fund.shortname,
            Fund.fullname,
            Fund.holding_period_source,
            Fund.holding_keywords,
        ).filter(
            Fund.fcode.in_(normalized_fcodes)
        ).all()
        if capture_enabled():
            record_event('database_risk.raw_fund_gate_rows', [dict(row._mapping) for row in rows])
        return {
            str(row.fcode).strip(): {
                'has_holding_period': row.has_holding_period,
                'holding_days': row.holding_days,
                'shortname': row.shortname,
                'fullname': row.fullname,
                'holding_period_source': row.holding_period_source,
                'holding_keywords': row.holding_keywords,
            }
            for row in rows
            if str(row.fcode or '').strip()
        }

    def _should_skip_risk_metrics_for_fund(self, fund_gate_info: Optional[Dict[str, Any]]) -> bool:
        if not fund_gate_info:
            return False
        return holding_period_gate_reason(
            has_holding_period=fund_gate_info.get('has_holding_period'),
            holding_days=fund_gate_info.get('holding_days'),
            fund_name=combine_holding_period_names(fund_gate_info.get('shortname'), fund_gate_info.get('fullname')),
            holding_period_source=fund_gate_info.get('holding_period_source'),
            holding_keywords=fund_gate_info.get('holding_keywords'),
            threshold_days=self.holding_period_skip_days,
        ) == HOLDING_PERIOD_LIMIT

    def _save_metrics(self, fcode: str, period: str, metrics: Dict):
        """
        保存风险指标到数据库

        Args:
            fcode: 基金代码
            period: 计算周期
            metrics: 风险指标字典
        """
        with self.db_manager.get_session() as session:
            # 删除旧数据（如果存在）
            session.query(FundRiskMetrics).filter_by(
                fcode=fcode,
                period=period
            ).delete()

            # 插入新数据（volatility/max_drawdown/var 为小数形式，保留6位精度）
            risk_metrics = FundRiskMetrics(**self._metric_write_record(fcode, period, metrics, datetime.now()))

            session.add(risk_metrics)
            session.commit()

    def _replace_fund_metrics(self, fcode: str, metrics_by_period: List):
        """
        原子替换单基金全部风险指标。

        说明：
        - force_update 定向重算必须删除旧 period（包括现在不应存在的 3y/5y）；
        - 删除与插入放在同一事务中，避免“先删后算失败”导致该基金指标被清空。
        """
        with self.db_manager.get_session(task_name='replace_fund_risk_metrics') as session:
            session.query(FundRiskMetrics).filter_by(fcode=fcode).delete()

            now = datetime.now()
            for period, metrics in metrics_by_period:
                session.add(FundRiskMetrics(**self._metric_write_record(fcode, period, metrics, now)))

    def print_stats(self):
        """打印统计信息"""
        logger.info("="*60)
        logger.info("风险指标计算统计")
        logger.info("="*60)
        logger.info(f"成功:        {self.stats['success']:,}")
        logger.info(f"失败:        {self.stats['failed']:,}")
        logger.info(f"跳过:        {self.stats['skipped']:,}")
        logger.info(f"总计:        {sum(self.stats.values()):,}")
        success_rate = (self.stats['success'] / sum(self.stats.values()) * 100) if sum(self.stats.values()) > 0 else 0
        logger.info(f"成功率:      {success_rate:.2f}%")
        logger.info("="*60)
