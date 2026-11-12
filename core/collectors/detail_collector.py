"""
基金详情采集器
采集基金的详细信息：费率、基准、投资目标、投资策略等
"""

import json
import logging
import math
import os
import sqlite3
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from sqlalchemy import and_, case, func, not_, or_
from collectors.base_collector import BaseCollector
from collectors.source_evidence import ApiSourceEvidence
from database.models import Fund, FundReturns
from utils import ProgressMonitor, chunks, parse_date, safe_float, safe_int
from utils.fund_exclusion_rules import get_runtime_excluded_fund_codes, is_runtime_excluded_fund
from utils.fund_type_filters import (
    normalize_fund_type_fields,
    preserve_existing_qdii_semantic_ftype,
    should_preserve_existing_qdii_bucket,
)
from utils.holding_period_parser import build_holding_update_fields, parse_holding_match
from utils.negotiable_cd_policy import build_high_confidence_negotiable_cd_sqlalchemy_filter
from utils.tiantian_data_quality import inspect_tiantian_detail_payload
from utils.detail_collection_summary import build_detail_collection_summary

logger = logging.getLogger(__name__)

# 字段映射配置：(API字段, Fund属性, 转换函数)
# 转换函数: None=直接赋值, 'str'=转字符串, 'int'=safe_int, 'float'=safe_float,
#          'date'=parse_date, 'percent'=去%后转float
FIELD_MAPPING = [
    ('FTYPE', 'ftype', None),
    ('RISKLEVEL', 'risk_level', 'int'),
    ('ESTABDATE', 'estab_date', 'date'),
    ('BENCH', 'benchmark', None),
    ('INVTGT', 'investment_target', None),
    ('INVSTRA', 'investment_strategy', None),
    ('JJGSID', 'company_id', 'str'),
    ('JJGS', 'company_name', None),
    ('TGBANK', 'custodian_bank', None),
    ('FEATURE', 'feature_tags', None),
    ('MGREXP', 'mgr_fee_rate', 'percent'),
    ('TRUSTEXP', 'trust_fee_rate', 'percent'),
    ('SALESEXP', 'sales_fee_rate', 'percent'),
    # P1-01 / B13: /fundMNDetailInformation 权威结构化字段落库。
    # 这些字段保留原始字符串形态（上游可能是 "--"/"0"/"6M"/"180" 等），
    # 语义解析留给 supplement_holding_period 的 L1 detail_struct 分支。
    ('CYCLE', 'cycle', 'str'),
    ('PRSVPERIOD', 'preserve_period', 'str'),
    ('PRSVDATE', 'preserve_date', 'str'),
    ('PRSVTYPE', 'preserve_type', 'str'),
    ('BUYTIME', 'buy_time', 'str'),
    ('FEATURE', 'feature_code', 'str'),
    ('BFUNDTYPE', 'base_fund_type_code', 'str'),
    ('RLEVEL_SZ', 'rating_shanghai', 'str'),
]
PLACEHOLDER_TEXT_VALUES = {'--', '---', '暂无', 'N/A', 'n/a', 'null', 'NULL'}
RISK_READY_COLUMNS = (
    'annual_return',
    'sharpe_ratio',
    'max_drawdown',
    'volatility',
)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    try:
        return {
            str(row[1] or '').strip()
            for row in conn.execute(f'PRAGMA table_info({table_name})').fetchall()
            if row and str(row[1] or '').strip()
        }
    except sqlite3.Error:
        return set()


def _risk_metric_ready_predicate(conn: sqlite3.Connection, alias: str) -> str:
    columns = _table_columns(conn, 'fund_risk_metrics')
    if not set(RISK_READY_COLUMNS).issubset(columns):
        return '1 = 1'
    return ' AND '.join(f'{alias}.{column} IS NOT NULL' for column in RISK_READY_COLUMNS)


def _latest_risk_rowid_predicate(alias: str = 'risk', latest_alias: str = 'latest') -> str:
    return f"""
        {alias}.rowid = (
            SELECT {latest_alias}.rowid
            FROM fund_risk_metrics {latest_alias}
            WHERE {latest_alias}.fcode = {alias}.fcode
              AND {latest_alias}.period = {alias}.period
            ORDER BY COALESCE(TRIM({latest_alias}.end_date), '') DESC, {latest_alias}.rowid DESC
            LIMIT 1
        )
    """


class FundDetailCollector(BaseCollector):
    """基金详情采集器"""

    DETAIL_PLACEHOLDER_BENCHMARKS = ('', '--', '---')
    DETAIL_PLACEHOLDER_TEXTS = DETAIL_PLACEHOLDER_BENCHMARKS + tuple(PLACEHOLDER_TEXT_VALUES)

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化详情采集器"""
        super().__init__(config, db_manager, api_client)

        # 采集配置
        collection_config = self.config.get('collection', {})
        self.detail_config = collection_config.get('detail', {})
        self.default_workers = self.detail_config.get('workers', 5)
        self.default_batch_size = self.detail_config.get('batch_size', 500)
        self.refresh_stale_days = max(
            1,
            int(self.detail_config.get('refresh_stale_days', 56) or 56),
        )
        self.max_targets_per_run = max(
            0,
            int(self.detail_config.get('max_targets_per_run', 1000) or 0),
        )
        self.empty_detail_cooldown_days = max(
            0,
            int(self.detail_config.get('empty_detail_cooldown_days', 7) or 0),
        )
        self.empty_detail_stale_nav_days = max(
            1,
            int(self.detail_config.get('empty_detail_stale_nav_days', 10) or 10),
        )
        self.empty_detail_holding_days_threshold = max(
            1,
            int(self.detail_config.get('empty_detail_holding_days_threshold', 90) or 90),
        )
        self.empty_detail_force_bypass_cooldown = bool(
            self.detail_config.get('empty_detail_force_bypass_cooldown', False)
        )
        self._empty_detail_lock = threading.Lock()
        self._empty_detail_records: List[Dict] = []
        self.empty_detail_report_path: Optional[str] = None
        self.empty_detail_report_dir = Path(
            os.environ.get(
                'DETAIL_EMPTY_REPORT_DIR',
                str(Path(__file__).resolve().parent.parent / 'logs' / 'tasks')
            )
        )
        self.empty_detail_report_dir.mkdir(parents=True, exist_ok=True)
        self._detail_api_source_evidence = ApiSourceEvidence(
            task="collect-details",
            source="fundMNDetailInformation",
        )
        self.detail_collection_summary = None

    def _ensure_detail_api_source_evidence(self) -> ApiSourceEvidence:
        if not hasattr(self, "_detail_api_source_evidence") or self._detail_api_source_evidence is None:
            self._detail_api_source_evidence = ApiSourceEvidence(
                task="collect-details",
                source="fundMNDetailInformation",
            )
        return self._detail_api_source_evidence

    def reset_api_source_evidence(self, **context) -> None:
        self._ensure_detail_api_source_evidence().reset(**context)

    def update_api_source_evidence_context(self, **context) -> None:
        self._ensure_detail_api_source_evidence().update_context(**context)

    def get_api_source_evidence(self, **overrides) -> Dict:
        return self._ensure_detail_api_source_evidence().snapshot(**overrides)

    def get_api_source_evidence_json(self, **overrides) -> str:
        return self._ensure_detail_api_source_evidence().snapshot_json(**overrides)

    def _resolve_submit_batch_size(self, workers: int, total_items: int) -> int:
        """限制单轮提交的详情采集 future 数量，降低大任务调度开销。"""
        try:
            normalized_workers = int(workers)
        except (TypeError, ValueError):
            normalized_workers = 1
        try:
            normalized_total = int(total_items)
        except (TypeError, ValueError):
            normalized_total = 0

        normalized_workers = max(1, normalized_workers)
        submit_batch_base = min(max(1, int(self.default_batch_size)), 200)
        submit_batch_size = max(normalized_workers * 4, submit_batch_base)
        if normalized_total > 0:
            return min(normalized_total, submit_batch_size)
        return submit_batch_size

    @classmethod
    def _build_high_confidence_negotiable_cd_refetch_filter(cls):
        """
        将高置信误分类的同业存单指数基金重新纳入 detail 补采。

        背景：
        - 一批基金已经具备 benchmark/费率等详情字段，但类型仍停留在
          `ftype='002' + hybrid/equity` 的低信息量状态；
        - 这类样本不会再被“缺字段驱动”的规则重新拉回 detail 队列，
          导致 `指数型-固收` 的纠偏链路无法再次触发；
        - 这里只覆盖已验证的高置信家族：名称明确含 `同业存单`，且名称或
          benchmark 明示 `中证同业存单AAA指数`，同时排除显式 `混合` 名称。
        """
        stale_non_bond_bucket = or_(
            Fund.type_category.is_(None),
            Fund.type_category.in_(('hybrid', 'equity')),
            Fund.type_code.in_(('26', '27')),
        )
        return and_(
            Fund.ftype == '002',
            stale_non_bond_bucket,
            build_high_confidence_negotiable_cd_sqlalchemy_filter(Fund),
        )

    def collect_all_details(
        self,
        workers=None,
        force_update=False,
        *,
        bypass_empty_detail_cooldown: bool = False,
        target_fcodes: Optional[List[str]] = None,
        scale_missing_only: bool = False,
    ):
        """
        采集所有基金的详细信息

        Args:
            workers: 并发数
            force_update: 强制更新（忽略已有详情）

        Returns:
            采集成功的基金数
        """
        workers = workers or self.default_workers
        if scale_missing_only:
            from collectors.scale_refresh import collect_missing_scales
            return collect_missing_scales(self, workers=workers, target_fcodes=target_fcodes)
        logger.info(f"开始采集所有基金的详细信息（并发数={workers}）...")

        # 获取所有基金代码
        if target_fcodes is None:
            fcodes = self._load_target_fcodes(
                force_update=force_update,
                bypass_empty_detail_cooldown=(
                    bool(bypass_empty_detail_cooldown)
                    or bool(self.empty_detail_force_bypass_cooldown)
                ),
            )
        else:
            fcodes = list(dict.fromkeys(str(code).strip() for code in target_fcodes))
            if any(not code for code in fcodes):
                raise ValueError("empty_explicit_detail_fcode")
        self.reset_api_source_evidence(
            collection_mode="single" if target_fcodes is not None and len(fcodes) == 1 else "bulk",
            force_update=bool(force_update),
            target_count=len(fcodes),
            refresh_stale_days=self.refresh_stale_days,
            max_targets_per_run=(self.max_targets_per_run or None),
            bypass_empty_detail_cooldown=(
                bool(bypass_empty_detail_cooldown)
                or bool(self.empty_detail_force_bypass_cooldown)
            ),
        )
        with self._stats_lock:
            self.stats = {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0}
        with self._empty_detail_lock:
            self._empty_detail_records = []
        self.empty_detail_report_path = None
        self.detail_collection_summary = None

        if not fcodes:
            logger.info("本轮详情目标队列为空；不推定全库详情完整")
            self.detail_collection_summary = build_detail_collection_summary({
                "target_count": 0, "fetch_ready_count": 0, "fetch_failed_count": 0,
                "unavailable_count": 0, "write_success_count": 0, "write_failed_count": 0,
            })
            self.update_api_source_evidence_context(success_count=0, failed_count=0, skipped_count=0)
            return 0

        logger.info(f"共{len(fcodes)}只基金需要采集详情")
        pending_updates: List[Dict] = []
        fetch_failed_items: List[Tuple[str, str]] = []
        write_failed_items: List[Tuple[str, str]] = []
        success_count = 0
        fetch_ready_count = 0
        unavailable_count = 0

        submit_batch_size = self._resolve_submit_batch_size(workers, len(fcodes))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            with ProgressMonitor(len(fcodes), desc="获取基金详情", unit="只基金", show_completion_summary=False) as fetch_progress, \
                    ProgressMonitor(len(fcodes), desc="基金详情持久化结果", unit="只基金") as progress:
                def flush() -> None:
                    nonlocal success_count
                    written, failed = self._flush_pending_detail_updates(pending_updates)
                    success_count += written
                    write_failed_items.extend(failed)
                    if written:
                        progress.update(written, success=True)
                    if failed:
                        progress.update(len(failed), success=False)

                for fcode_batch in chunks(fcodes, submit_batch_size):
                    futures = {
                        executor.submit(self._collect_detail_update_payload, fcode): fcode
                        for fcode in fcode_batch
                    }

                    for future in as_completed(futures):
                        fcode = futures[future]
                        try:
                            update_payload = future.result()
                        except Exception as e:
                            error_msg = str(e)
                            logger.error(f"采集基金{fcode}详情异常: {error_msg}")
                            fetch_failed_items.append((fcode, error_msg))
                            fetch_progress.update(1, success=False)
                            progress.update(1, success=False)
                            continue
                        if update_payload:
                            fetch_ready_count += 1
                            pending_updates.append(update_payload)
                            fetch_progress.update(1, success=True)
                            # Persistence failures must not be attributed to the
                            # one fetch future that happened to fill this batch.
                            if len(pending_updates) >= self.default_batch_size:
                                flush()
                        else:
                            unavailable_count += 1
                            fetch_progress.update(1, skipped=True)
                            progress.update(1, skipped=True)
                        progress.set_postfix(current=fcode[:10], persisted=f"{success_count}",
                                             pending=f"{len(pending_updates)}")
                # The only completion summary is emitted after the final commit.
                flush()

        failed_count = len(fetch_failed_items) + len(write_failed_items)
        skipped_count = unavailable_count
        self.detail_collection_summary = build_detail_collection_summary({
            "target_count": len(fcodes), "fetch_ready_count": fetch_ready_count,
            "fetch_failed_count": len(fetch_failed_items), "unavailable_count": unavailable_count,
            "write_success_count": success_count, "write_failed_count": len(write_failed_items),
        })

        with self._stats_lock:
            self.stats = {
                'total': len(fcodes),
                'success': success_count,
                'failed': failed_count,
                'skipped': skipped_count,
            }

        for failed_fcode, error_msg in fetch_failed_items + write_failed_items:
            self.log_collection(
                task_type='采集基金详情',
                fcode=failed_fcode,
                status='failed',
                message=error_msg[:500]
            )

        empty_detail_report = self._write_empty_detail_report()
        if empty_detail_report:
            logger.info("空详情基金报告: %s", empty_detail_report)
        self.update_api_source_evidence_context(
            success_count=success_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
            empty_detail_report_path=empty_detail_report,
            empty_detail_count=len(self._empty_detail_records),
            empty_detail_reason_counts=dict(
                Counter(str(item.get('reason') or 'unknown') for item in self._empty_detail_records)
            ),
            stop_watch_fallback_scope="not_counted_as_detail_success",
        )

        self.print_stats()
        return success_count

    def _load_target_fcodes(
        self,
        force_update: bool = False,
        *,
        bypass_empty_detail_cooldown: bool = False,
    ) -> List[str]:
        """
        读取需要补详情的基金代码。

        非 force 模式下的补采策略：
        - `benchmark` 为 NULL：直接重试；
        - 已有 `benchmark` 但筛选关键字段仍缺失：继续补采；
        - `benchmark` 仅为占位值时，只有其它关键字段也缺失才重试；
        - 详情成功采集时间缺失或超过刷新周期：进入有界轮转队列；
        - 已验证为高置信误分类的同业存单指数家族：重新补采 detail 纠正类型。

        注意：
        - `sales_fee_rate` 在当前 schema 里是可空字段，且并非所有基金都会稳定返回；
          这里不把它作为统一必补条件，避免把合法缺失样本反复拉回详情队列。
        """
        with self.db_manager.get_session(readonly=True, task_name='detail_collect_list') as session:
            query = session.query(Fund.fcode)
            if force_update:
                excluded_codes = set(get_runtime_excluded_fund_codes())
                candidate_codes = [f.fcode for f in query.all() if f.fcode not in excluded_codes]
                if bypass_empty_detail_cooldown:
                    return candidate_codes
                cooled_codes = self._load_recent_non_blocking_empty_detail_codes(candidate_codes)
                if cooled_codes:
                    logger.info(
                        "详情强制补采仍跳过 %s 只近期非阻断空详情样本（冷却%s天）",
                        len(cooled_codes),
                        self.empty_detail_cooldown_days,
                    )
                return [fcode for fcode in candidate_codes if fcode not in cooled_codes]

            core_detail_missing_filter = or_(
                Fund.ftype.is_(None),
                Fund.ftype.in_(self.DETAIL_PLACEHOLDER_TEXTS),
                Fund.type_category.is_(None),
                Fund.type_category.in_(self.DETAIL_PLACEHOLDER_TEXTS),
                Fund.estab_date.is_(None),
                Fund.risk_level.is_(None),
                Fund.company_id.is_(None),
                Fund.company_id.in_(self.DETAIL_PLACEHOLDER_TEXTS),
                Fund.company_name.is_(None),
                Fund.company_name.in_(self.DETAIL_PLACEHOLDER_TEXTS),
                Fund.mgr_fee_rate.is_(None),
                Fund.trust_fee_rate.is_(None),
            )

            stale_cutoff = datetime.now() - timedelta(days=self.refresh_stale_days)
            stale_detail_filter = or_(
                Fund.detail_updated_at.is_(None),
                Fund.detail_updated_at < stale_cutoff,
            )
            pending_detail_filter = or_(
                Fund.benchmark.is_(None),
                core_detail_missing_filter,
                stale_detail_filter,
                self._build_high_confidence_negotiable_cd_refetch_filter(),
            )
            excluded_codes = set(get_runtime_excluded_fund_codes())
            candidate_rows = (
                query.filter(pending_detail_filter)
                .order_by(
                    case((core_detail_missing_filter, 0), else_=1),
                    Fund.detail_updated_at.asc(),
                    Fund.fcode.asc(),
                )
                .all()
            )
            candidate_codes = [
                row.fcode for row in candidate_rows if row.fcode not in excluded_codes
            ]
        cooled_codes = self._load_recent_non_blocking_empty_detail_codes(candidate_codes)
        if cooled_codes:
            logger.info(
                "详情补采跳过 %s 只近期非阻断空详情样本（冷却%s天）",
                len(cooled_codes),
                self.empty_detail_cooldown_days,
            )
        target_codes = [fcode for fcode in candidate_codes if fcode not in cooled_codes]
        if self.max_targets_per_run > 0:
            target_codes = target_codes[:self.max_targets_per_run]
        return target_codes

    @staticmethod
    def _parse_iso_datetime(value) -> Optional[datetime]:
        normalized = str(value or '').strip()
        if not normalized:
            return None
        normalized = normalized.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def _parse_iso_date(value):
        normalized = str(value or '').strip()
        if not normalized:
            return None
        return parse_date(normalized[:10])

    def _load_latest_empty_detail_report_payload(self) -> tuple[Optional[Path], Optional[Dict]]:
        if not self.empty_detail_report_dir.exists():
            return None, None

        candidates = sorted(
            self.empty_detail_report_dir.glob('detail_empty_funds_*.json'),
            key=lambda path: (path.stat().st_mtime, path.name),
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding='utf-8'))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if isinstance(payload, dict):
                return path, payload
        return None, None

    @staticmethod
    def _extract_non_blocking_empty_detail_codes(report_payload: Dict) -> set[str]:
        funds = report_payload.get('funds') or []
        if not isinstance(funds, list):
            return set()

        impact_counts = report_payload.get('impact_counts') or {}
        samples_by_impact = report_payload.get('samples_by_impact') or {}
        blocking_reason = str(report_payload.get('blocking_reason') or '').strip()

        if blocking_reason == 'non_blocking_empty_detail_only':
            return {
                str((item or {}).get('fcode') or '').strip()
                for item in funds
                if isinstance(item, dict) and str((item or {}).get('fcode') or '').strip()
            }

        non_blocking_codes: set[str] = set()
        for key in ('holding_period_blocked', 'stale_nav_or_missing', 'missing_effective_1y_risk'):
            if int(impact_counts.get(key) or 0) <= 0:
                continue
            for item in samples_by_impact.get(key) or []:
                if not isinstance(item, dict):
                    continue
                code = str(item.get('fcode') or '').strip()
                if code:
                    non_blocking_codes.add(code)
        return non_blocking_codes

    @staticmethod
    def _is_weekend(date_text: str) -> bool:
        try:
            return datetime.strptime(date_text, '%Y-%m-%d').weekday() >= 5
        except ValueError:
            return False

    def _resolve_local_effective_detail_snapshot(self) -> tuple[Optional[datetime.date], str]:
        db_path = str(getattr(self.db_manager, 'db_path', '') or '').strip()
        if not db_path or db_path == ':memory:':
            return None, ''

        conn = sqlite3.connect(db_path)
        try:
            nav_rows = conn.execute(
                """
                SELECT latest_nav_date, COUNT(*) AS fund_count
                FROM fund_returns
                WHERE latest_nav_date IS NOT NULL AND TRIM(latest_nav_date) <> ''
                GROUP BY latest_nav_date
                ORDER BY latest_nav_date DESC
                """
            ).fetchall()
            nav_distribution = [
                (str(row[0] or '').strip(), int(row[1] or 0))
                for row in nav_rows
                if str(row[0] or '').strip()
            ]

            candidate_date_text = ''
            threshold_count = 0
            eligible_nav_rows = [
                (date_text, fund_count)
                for date_text, fund_count in nav_distribution
                if not self._is_weekend(date_text)
            ]
            for index, (date_text, fund_count) in enumerate(eligible_nav_rows):
                previous_count = eligible_nav_rows[index + 1][1] if index + 1 < len(eligible_nav_rows) else 0
                fresh_window_fund_count = fund_count + previous_count
                current_threshold = (
                    int(math.ceil(float(fresh_window_fund_count) * 0.9))
                    if fresh_window_fund_count > 0
                    else 0
                )
                if fund_count >= current_threshold and current_threshold > 0:
                    candidate_date_text = date_text
                    threshold_count = current_threshold
                    break

            risk_effective_end_date = ''
            if threshold_count > 0:
                risk_rows = conn.execute(
                    """
                    SELECT end_date, COUNT(DISTINCT fcode) AS fund_count
                    FROM fund_risk_metrics
                    WHERE period = '1y' AND end_date IS NOT NULL AND TRIM(end_date) <> ''
                    GROUP BY end_date
                    ORDER BY end_date DESC
                    """
                ).fetchall()
                for end_date, fund_count in risk_rows:
                    normalized = str(end_date or '').strip()
                    if normalized and int(fund_count or 0) >= threshold_count:
                        risk_effective_end_date = normalized
                        break

            return self._parse_iso_date(candidate_date_text), risk_effective_end_date
        except sqlite3.Error:
            return None, ''
        finally:
            conn.close()

    def _classify_non_blocking_empty_detail_codes_from_context(self, report_payload: Dict) -> set[str]:
        funds = report_payload.get('funds') or []
        if not isinstance(funds, list):
            return set()

        report_codes = sorted(
            {
                str((item or {}).get('fcode') or '').strip()
                for item in funds
                if isinstance(item, dict) and str((item or {}).get('fcode') or '').strip()
            }
        )
        if not report_codes:
            return set()

        candidate_date, risk_effective_end_date = self._resolve_local_effective_detail_snapshot()

        nav_fresh_cutoff = (
            candidate_date - timedelta(days=self.empty_detail_stale_nav_days)
            if candidate_date is not None
            else None
        )

        context_map: Dict[str, Dict] = {}
        with self.db_manager.get_session(readonly=True, task_name='detail_empty_cooldown_context') as session:
            rows = (
                session.query(
                    Fund.fcode,
                    Fund.has_holding_period,
                    Fund.holding_days,
                    FundReturns.latest_nav_date,
                    func.count(FundReturns.fcode).label('fund_return_row_count'),
                )
                .outerjoin(FundReturns, FundReturns.fcode == Fund.fcode)
                .filter(Fund.fcode.in_(report_codes))
                .group_by(Fund.fcode, Fund.has_holding_period, Fund.holding_days, FundReturns.latest_nav_date)
                .all()
            )
            for row in rows:
                context_map[str(row.fcode)] = {
                    'has_holding_period': int(row.has_holding_period or 0),
                    'holding_days': row.holding_days,
                    'latest_nav_date': row.latest_nav_date,
                }

        risk_ready_codes: set[str] = set()
        if risk_effective_end_date:
            db_path = str(getattr(self.db_manager, 'db_path', '') or '').strip()
            if db_path and db_path != ':memory:':
                conn = sqlite3.connect(db_path)
                try:
                    risk_ready_predicate = _risk_metric_ready_predicate(conn, 'risk')
                    latest_rowid_predicate = _latest_risk_rowid_predicate('risk', 'latest')
                    risk_rows = conn.execute(
                        f"""
                        SELECT DISTINCT fcode
                        FROM fund_risk_metrics risk
                        WHERE risk.period = '1y'
                          AND COALESCE(risk.end_date, '') = ?
                          AND {risk_ready_predicate}
                          AND {latest_rowid_predicate}
                        """,
                        (risk_effective_end_date,),
                    ).fetchall()
                    risk_ready_codes = {
                        str(row[0] or '').strip()
                        for row in risk_rows
                        if row and str(row[0] or '').strip()
                    }
                except sqlite3.Error:
                    risk_ready_codes = set()
                finally:
                    conn.close()

        non_blocking_codes: set[str] = set()
        for fcode in report_codes:
            context = context_map.get(fcode) or {}
            if not context:
                continue
            if (
                int(context.get('has_holding_period') or 0) == 1
                and int(context.get('holding_days') or 0) > self.empty_detail_holding_days_threshold
            ):
                non_blocking_codes.add(fcode)
                continue

            latest_nav_date = context.get('latest_nav_date')
            if isinstance(latest_nav_date, str):
                latest_nav_date = self._parse_iso_date(latest_nav_date)
            if latest_nav_date is None:
                non_blocking_codes.add(fcode)
                continue
            if nav_fresh_cutoff is not None and latest_nav_date < nav_fresh_cutoff:
                non_blocking_codes.add(fcode)
                continue

            if risk_effective_end_date and fcode not in risk_ready_codes:
                non_blocking_codes.add(fcode)

        return non_blocking_codes

    def _load_recent_non_blocking_empty_detail_codes(self, candidate_codes: List[str]) -> set[str]:
        if self.empty_detail_cooldown_days <= 0:
            return set()

        normalized_codes = sorted({str(code or '').strip() for code in candidate_codes if str(code or '').strip()})
        if not normalized_codes:
            return set()

        _, report_payload = self._load_latest_empty_detail_report_payload()
        if not isinstance(report_payload, dict):
            return set()

        generated_at = self._parse_iso_datetime(report_payload.get('generated_at'))
        if generated_at is None:
            return set()

        if generated_at < datetime.now() - timedelta(days=self.empty_detail_cooldown_days):
            return set()

        non_blocking_codes = self._extract_non_blocking_empty_detail_codes(report_payload)
        if not non_blocking_codes:
            non_blocking_codes = self._classify_non_blocking_empty_detail_codes_from_context(
                report_payload
            )
        if not non_blocking_codes:
            return set()

        report_codes = [code for code in non_blocking_codes if code in normalized_codes]
        report_codes = [code for code in report_codes if code and code in normalized_codes]
        if not report_codes:
            return set()

        context_map: Dict[str, Dict] = {}
        with self.db_manager.get_session(readonly=True, task_name='detail_empty_cooldown_context') as session:
            rows = (
                session.query(
                    Fund.fcode,
                    Fund.has_holding_period,
                    Fund.holding_days,
                    FundReturns.latest_nav_date,
                )
                .outerjoin(FundReturns, FundReturns.fcode == Fund.fcode)
                .filter(Fund.fcode.in_(report_codes))
                .all()
            )
            for row in rows:
                context_map[str(row.fcode)] = {
                    'has_holding_period': int(row.has_holding_period or 0),
                    'holding_days': row.holding_days,
                    'latest_nav_date': row.latest_nav_date,
                }

        cooled_codes: set[str] = set()
        nav_fresh_cutoff = datetime.now().date() - timedelta(days=self.empty_detail_stale_nav_days)
        for fcode in report_codes:
            context = context_map.get(fcode) or {}
            if (
                int(context.get('has_holding_period') or 0) == 1
                and int(context.get('holding_days') or 0) > self.empty_detail_holding_days_threshold
            ):
                cooled_codes.add(fcode)
                continue

            latest_nav_date = context.get('latest_nav_date')
            if isinstance(latest_nav_date, str):
                latest_nav_date = self._parse_iso_date(latest_nav_date)
            if latest_nav_date is not None and latest_nav_date < nav_fresh_cutoff:
                cooled_codes.add(fcode)
                continue

            if fcode in non_blocking_codes:
                cooled_codes.add(fcode)

        return cooled_codes

    def _flush_pending_detail_updates(self, pending_updates: List[Dict]) -> Tuple[int, List[Tuple[str, str]]]:
        """按批次刷写待更新详情，降低全量任务内存峰值。"""
        if not pending_updates:
            return 0, []

        updates_to_flush = list(pending_updates)
        result = self._bulk_update_fund_details(updates_to_flush)
        pending_updates.clear()
        return result

    @staticmethod
    def _normalize_existing_detail_state_rows(rows) -> Dict[str, Dict[str, Optional[str]]]:
        """将数据库行整理为按 fcode 索引的详情重协调状态。"""
        state_map: Dict[str, Dict[str, Optional[str]]] = {}
        for row in rows or []:
            fcode = getattr(row, 'fcode', None)
            ftype = getattr(row, 'ftype', None)
            type_code = getattr(row, 'type_code', None)
            type_category = getattr(row, 'type_category', None)
            shortname = getattr(row, 'shortname', None)
            fullname = getattr(row, 'fullname', None)
            investment_target = getattr(row, 'investment_target', None)
            investment_strategy = getattr(row, 'investment_strategy', None)
            has_holding_period = getattr(row, 'has_holding_period', None)
            holding_days = getattr(row, 'holding_days', None)
            holding_keywords = getattr(row, 'holding_keywords', None)

            if fcode is None and isinstance(row, (tuple, list)) and row:
                fcode = row[0]
                ftype = row[1] if len(row) > 1 else None
                type_code = row[2] if len(row) > 2 else None
                type_category = row[3] if len(row) > 3 else None
                shortname = row[4] if len(row) > 4 else None
                fullname = row[5] if len(row) > 5 else None
                investment_target = row[6] if len(row) > 6 else None
                investment_strategy = row[7] if len(row) > 7 else None
                has_holding_period = row[8] if len(row) > 8 else None
                holding_days = row[9] if len(row) > 9 else None
                holding_keywords = row[10] if len(row) > 10 else None

            normalized_fcode = str(fcode or '').strip()
            if not normalized_fcode:
                continue

            state_map[normalized_fcode] = {
                'ftype': str(ftype).strip() if ftype not in (None, '') else None,
                'type_code': str(type_code).strip() if type_code not in (None, '') else None,
                'type_category': str(type_category).strip() if type_category not in (None, '') else None,
                'shortname': str(shortname).strip() if shortname not in (None, '') else None,
                'fullname': str(fullname).strip() if fullname not in (None, '') else None,
                'investment_target': str(investment_target).strip() if investment_target not in (None, '') else None,
                'investment_strategy': str(investment_strategy).strip() if investment_strategy not in (None, '') else None,
                'has_holding_period': has_holding_period,
                'holding_days': holding_days,
                'holding_keywords': str(holding_keywords).strip() if holding_keywords not in (None, '') else None,
            }
        return state_map

    def _load_existing_detail_states(self, fcodes: List[str]) -> Dict[str, Dict[str, Optional[str]]]:
        """批量读取当前基金详情重协调状态。"""
        normalized_fcodes = sorted({str(code or '').strip() for code in fcodes if str(code or '').strip()})
        if not normalized_fcodes:
            return {}

        with self.db_manager.get_session(readonly=True, task_name='detail_existing_type_state') as session:
            rows = (
                session.query(
                    Fund.fcode,
                    Fund.ftype,
                    Fund.type_code,
                    Fund.type_category,
                    Fund.shortname,
                    Fund.fullname,
                    Fund.investment_target,
                    Fund.investment_strategy,
                    Fund.has_holding_period,
                    Fund.holding_days,
                    Fund.holding_keywords,
                )
                .filter(Fund.fcode.in_(normalized_fcodes))
                .all()
            )
        return self._normalize_existing_detail_state_rows(rows)

    @classmethod
    def _reconcile_existing_type_state(
        cls,
        update_payload: Dict,
        existing_state: Optional[Dict[str, Optional[str]]],
    ) -> Dict:
        """在真正写库前，对歧义类型文本做保守收敛。"""
        if not update_payload or not existing_state:
            return dict(update_payload or {})

        merged = dict(update_payload)
        existing_type_code = existing_state.get('type_code')
        existing_type_category = existing_state.get('type_category')

        if not should_preserve_existing_qdii_bucket(
            merged.get('ftype'),
            existing_type_code,
            existing_type_category,
        ):
            return merged

        if existing_type_code:
            merged['type_code'] = existing_type_code
        if existing_type_category:
            merged['type_category'] = existing_type_category

        preserved_ftype = preserve_existing_qdii_semantic_ftype(
            merged.get('ftype'),
            existing_state.get('ftype'),
            existing_type_code,
            existing_type_category,
        )
        if preserved_ftype:
            merged['ftype'] = preserved_ftype

        return merged

    @staticmethod
    def _apply_detail_holding_period_update(
        update_payload: Dict,
        existing_state: Optional[Dict[str, Optional[str]]],
    ) -> Dict:
        if not update_payload or not existing_state:
            return dict(update_payload or {})

        merged = dict(update_payload)
        parsed = parse_holding_match(
            existing_state.get('shortname'),
            existing_state.get('fullname'),
            merged.get('investment_target') or existing_state.get('investment_target'),
            merged.get('investment_strategy') or existing_state.get('investment_strategy'),
        )
        holding_update = build_holding_update_fields(
            current_has_holding_period=existing_state.get('has_holding_period'),
            current_days=existing_state.get('holding_days'),
            current_keyword=existing_state.get('holding_keywords'),
            parsed=parsed,
        )
        if holding_update:
            merged.update(holding_update)
        return merged

    def collect_fund_detail(self, fcode: str, force_update=False) -> bool:
        """
        采集单只基金的详细信息

        Args:
            fcode: 基金代码
            force_update: 强制更新

        Returns:
            是否成功
        """
        del force_update
        normalized_fcode = str(fcode or "").strip()
        detail_payload = self._fetch_detail_payload(fcode)
        if not detail_payload:
            # P1-09.1 / B14 / V1: /fundMNDetailInformation 不可用时降级到
            # /fundMNStopWatch 补齐基金类型元数据（FTYPE/FEATURE/BFUNDTYPE/ESTABDATE）。
            # 持有期字段 (CYCLE/PRSVPERIOD/BUYTIME) 只有 Detail 返回，StopWatch 无法兜底。
            # 关键防护：空 fcode / runtime 排除的基金不走 StopWatch——这不是为了
            # 节流（天天基金代理无严格 QPS 限制），而是为了保持与 runtime exclusion
            # 策略一致：既然这些 fcode 已经被业务层排除，就不应再重新写入它们的字段。
            if not normalized_fcode:
                return False
            if is_runtime_excluded_fund(normalized_fcode):
                return False
            stop_watch_payload = self._fetch_stop_watch_payload(normalized_fcode)
            if stop_watch_payload:
                self._save_fund_stop_watch(normalized_fcode, stop_watch_payload)
                # Partial metadata is retained, but does not complete the
                # authoritative detail source (fees, benchmark, holding terms).
            return False
        return self._save_fund_detail(fcode, detail_payload)

    def _fetch_stop_watch_payload(self, fcode: str) -> Optional[Dict]:
        """P1-09.1 / B14: 调 /fundMNStopWatch 并校验返回。"""
        get_stop_watch = getattr(self.api, "get_fund_stop_watch", None)
        if not callable(get_stop_watch):
            return None
        try:
            payload = get_stop_watch(fcode)
        except Exception as exc:  # pragma: no cover - 防御性
            logger.warning("基金%s stop_watch 兜底失败: %s", fcode, exc)
            return None
        if not isinstance(payload, dict):
            return None
        data_block = payload.get("Datas")
        if isinstance(data_block, dict):
            return data_block
        if isinstance(payload.get("FCODE"), str):
            return payload
        return None

    def _save_fund_stop_watch(self, fcode: str, data: Dict) -> bool:
        """P1-09.1 / B14: 把 StopWatch 能兜底的结构性字段写回 funds 表。

        只写**Detail 可能缺失但 StopWatch 有**的字段：FTYPE / FEATURE /
        BFUNDTYPE / ESTABDATE。不动 BENCH/INVTGT/INVSTRA（StopWatch 无）；
        也不清空已有值。

        V2 保护：仅写入 funds 当前值为 NULL / 空字符串 / 占位符的字段——
        StopWatch 是 Detail 不可用时的兜底来源，质量次于 Detail；
        不覆盖历史已有的正确值（例如 Detail 曾采过的 estab_date）。
        """
        normalized = str(fcode or "").strip()
        if not normalized or not isinstance(data, dict):
            return False

        stop_watch_mapping = [
            ("FTYPE", "ftype", None),
            ("FEATURE", "feature_tags", None),
            ("FEATURE", "feature_code", "str"),
            ("BFUNDTYPE", "base_fund_type_code", "str"),
            ("ESTABDATE", "estab_date", "date"),
        ]

        candidate_updates: Dict[str, Any] = {}
        for api_field, fund_attr, converter in stop_watch_mapping:
            value = data.get(api_field)
            if value is None or value == "":
                continue
            if self._is_placeholder_value(value):
                continue
            try:
                converted = self._convert_field_value(converter, value)
            except (ValueError, TypeError, AttributeError) as exc:
                logger.debug("StopWatch 字段转换失败 %s=%r: %s", fund_attr, value, exc)
                continue
            if converted is not None:
                candidate_updates[fund_attr] = converted

        if not candidate_updates:
            return False

        # V2 保守写：读当前值，仅补 NULL/占位字段。
        try:
            fund_attrs = list(candidate_updates.keys())
            with self.db_manager.get_session(
                readonly=True,
                task_name="detail_stop_watch_exists_check",
            ) as session:
                row = session.query(
                    Fund.fcode,
                    *[getattr(Fund, attr) for attr in fund_attrs],
                ).filter_by(fcode=normalized).first()
            if row is None:
                logger.warning("基金%s 不存在于 funds 表，跳过 StopWatch 兜底", normalized)
                return False

            filtered_updates: Dict[str, Any] = {}
            for idx, attr in enumerate(fund_attrs):
                current = row[idx + 1]  # row[0] 是 fcode
                if current is None or (
                    isinstance(current, str) and (
                        current.strip() == "" or self._is_placeholder_value(current)
                    )
                ):
                    filtered_updates[attr] = candidate_updates[attr]

            if not filtered_updates:
                logger.debug(
                    "基金%s StopWatch 兜底无需写入：现有字段均已非空", normalized
                )
                return False

            update_payload: Dict[str, Any] = {
                "fcode": normalized,
                "last_updated_at": datetime.now(),
                **filtered_updates,
            }
            try:
                updated_count, failed = self._bulk_update_fund_details(
                    [update_payload],
                    batch_size=1,
                    record_api_source_evidence=False,
                )
            except TypeError as exc:
                if "record_api_source_evidence" not in str(exc):
                    raise
                updated_count, failed = self._bulk_update_fund_details(
                    [update_payload],
                    batch_size=1,
                )
            if failed:
                logger.warning("基金%s StopWatch 兜底写入失败: %s", normalized, failed[0][1])
                return False
            if updated_count == 1:
                logger.info(
                    "基金%s 使用 StopWatch 兜底补齐了 %d 个字段（保守：仅 NULL/占位字段）",
                    normalized,
                    len(filtered_updates),
                )
                return True
        except Exception as exc:  # pragma: no cover - 防御性
            logger.error("基金%s StopWatch 兜底写入异常: %s", normalized, exc)
        return False

    def _fetch_detail_payload(self, fcode: str) -> Optional[Dict]:
        """获取并校验单只基金详情 payload。"""
        normalized_fcode = str(fcode or '').strip()
        if not normalized_fcode:
            logger.warning("基金详情采集收到空基金代码，跳过请求")
            return None

        if is_runtime_excluded_fund(normalized_fcode):
            logger.info("基金%s已在运行时排除名单，跳过详情采集", normalized_fcode)
            return None

        evidence = self._ensure_detail_api_source_evidence()
        evidence.record_request()
        try:
            detail_data = self.api.get_fund_detail(normalized_fcode)
        except Exception as exc:
            evidence.record_transport_exception(fcode=normalized_fcode, exc=exc)
            raise
        detail_payload, quality = inspect_tiantian_detail_payload(
            detail_data,
            endpoint_label=f"/fundMNDetailInformation FCODE={normalized_fcode}",
        )
        evidence.record_response_quality(
            fcode=normalized_fcode,
            quality=quality,
            records=[detail_payload] if isinstance(detail_payload, dict) else [],
        )
        if not quality.accepted:
            logger.warning("基金%s详情不可用: %s", normalized_fcode, quality.message)
            self._record_empty_detail(
                normalized_fcode,
                quality.status,
                quality.payload_type or type(detail_data).__name__,
                message=quality.message,
                failure_kind=quality.failure_kind,
                upstream_business_code=quality.upstream_business_code,
                upstream_business_message=quality.upstream_business_message,
            )
            return None
        return detail_payload

    def _record_empty_detail(
        self,
        fcode: str,
        reason: str,
        payload_type: str,
        *,
        message: Optional[str] = None,
        failure_kind: Optional[str] = None,
        upstream_business_code: Optional[str] = None,
        upstream_business_message: Optional[str] = None,
    ):
        """记录空详情基金，供任务结束后生成结构化报告。"""
        with self._empty_detail_lock:
            payload = {
                'fcode': fcode,
                'reason': reason,
                'payload_type': payload_type,
                'occurred_at': datetime.now().isoformat(timespec='seconds'),
            }
            if message:
                payload['message'] = message
            if failure_kind:
                payload['failure_kind'] = failure_kind
            if upstream_business_code not in (None, ''):
                payload['upstream_business_code'] = upstream_business_code
            if upstream_business_message:
                payload['upstream_business_message'] = upstream_business_message
            self._empty_detail_records.append(payload)

    def _write_empty_detail_report(self) -> Optional[str]:
        """将空详情基金写入结构化报告，便于后续按公司/类型分类排查。"""
        with self._empty_detail_lock:
            if not self._empty_detail_records:
                self.empty_detail_report_path = None
                return None
            records = list(self._empty_detail_records)

        deduped_records = {}
        for record in records:
            deduped_records.setdefault(record['fcode'], record)
        records = list(deduped_records.values())

        fcodes = [record['fcode'] for record in records]
        funds_by_code = {}
        with self.db_manager.get_session(readonly=True, task_name='detail_empty_report') as session:
            fund_rows = (
                session.query(
                    Fund.fcode,
                    Fund.shortname,
                    Fund.ftype,
                    Fund.type_category,
                    Fund.company_name,
                    Fund.estab_date,
                )
                .filter(Fund.fcode.in_(fcodes))
                .all()
            )
            for row in fund_rows:
                funds_by_code[row.fcode] = {
                    'shortname': row.shortname,
                    'ftype': row.ftype,
                    'type_category': row.type_category,
                    'company_name': row.company_name,
                    'estab_date': row.estab_date.isoformat() if row.estab_date else None,
                }

        enriched_records = []
        company_counter = Counter()
        ftype_counter = Counter()
        type_category_counter = Counter()
        reason_counter = Counter()
        prefix3_counter = Counter()
        for record in records:
            fund_info = funds_by_code.get(record['fcode'], {})
            enriched = {
                **record,
                **fund_info,
            }
            enriched_records.append(enriched)
            company_counter[fund_info.get('company_name') or 'UNKNOWN'] += 1
            ftype_counter[fund_info.get('ftype') or 'UNKNOWN'] += 1
            type_category_counter[fund_info.get('type_category') or 'UNKNOWN'] += 1
            reason_counter[record['reason']] += 1
            prefix3_counter[record['fcode'][:3]] += 1

        report = {
            'report_type': 'empty_detail_funds',
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'count': len(enriched_records),
            'reason_counts': dict(reason_counter),
            'company_counts': dict(company_counter),
            'ftype_counts': dict(ftype_counter),
            'type_category_counts': dict(type_category_counter),
            'prefix3_counts': dict(prefix3_counter),
            'funds': sorted(enriched_records, key=lambda item: item['fcode']),
        }

        report_path = self.empty_detail_report_dir / (
            f"detail_empty_funds_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        self.empty_detail_report_path = str(report_path)
        return self.empty_detail_report_path

    def _collect_detail_update_payload(self, fcode: str) -> Optional[Dict]:
        """获取单只基金详情并转换为批量更新载荷。"""
        detail_payload = self._fetch_detail_payload(fcode)
        if not detail_payload:
            # V7: 批量路径与单只 collect_fund_detail 对齐，Detail 失败时走一次 StopWatch 兜底。
            # StopWatch 兜底走 _save_fund_stop_watch 的保守覆盖（V2：仅写当前 NULL 字段），
            # 不向上级 bulk_update 队列返回 payload，避免 StopWatch 的浅层字段覆盖已有 Detail 采过的值。
            normalized_fcode = str(fcode or "").strip()
            if normalized_fcode and not is_runtime_excluded_fund(normalized_fcode):
                try:
                    stop_watch_payload = self._fetch_stop_watch_payload(normalized_fcode)
                    if stop_watch_payload:
                        self._save_fund_stop_watch(normalized_fcode, stop_watch_payload)
                except Exception as exc:  # pragma: no cover - 兜底失败不拖垮批量主流程
                    logger.debug("基金%s 批量路径 StopWatch 兜底失败: %s", normalized_fcode, exc)
            return None
        return self._build_detail_update_payload(fcode, detail_payload)

    def _save_fund_detail(self, fcode: str, data: Dict) -> bool:
        """
        保存基金详情到数据库

        Args:
            fcode: 基金代码
            data: 详情数据

        Returns:
            是否成功
        """
        try:
            if not isinstance(data, dict):
                logger.warning(f"基金{fcode}详情格式异常，跳过保存: {type(data).__name__}")
                self._ensure_detail_api_source_evidence().record_save_failure(
                    fcode=str(fcode or '').strip(),
                    message=f"invalid_detail_payload_type:{type(data).__name__}",
                )
                return False

            update_payload = self._build_detail_update_payload(fcode, data)
            if not update_payload:
                self._ensure_detail_api_source_evidence().record_save_failure(
                    fcode=str(fcode or '').strip(),
                    message="empty_detail_update_payload",
                )
                return False

            with self.db_manager.get_session(readonly=True, task_name='detail_exists_check') as session:
                fund_exists = session.query(Fund.fcode).filter_by(fcode=fcode).first()

            if not fund_exists:
                logger.warning(f"基金{fcode}不存在于数据库")
                self._ensure_detail_api_source_evidence().record_save_failure(
                    fcode=str(fcode or '').strip(),
                    message="fund_not_found",
                )
                return False

            updated_count, failed_items = self._bulk_update_fund_details([update_payload], batch_size=1)
            if failed_items:
                logger.error(f"基金{fcode}详情保存失败: {failed_items[0][1]}")
                return False

            if updated_count == 1:
                logger.debug(f"基金{fcode}详情保存成功")
                return True
            return False

        except Exception as e:
            logger.error(f"保存基金{fcode}详情失败: {e}")
            self._ensure_detail_api_source_evidence().record_save_failure(
                fcode=str(fcode or '').strip(),
                message=str(e),
            )
            return False

    def _build_detail_update_payload(self, fcode: str, data: Dict) -> Optional[Dict]:
        """构建基金详情批量更新载荷。"""
        if not isinstance(data, dict):
            logger.warning(f"基金{fcode}详情格式异常，跳过构建: {type(data).__name__}")
            return None

        collected_at = datetime.now()
        update_payload = {
            'fcode': fcode,
            'last_updated_at': collected_at,
            'detail_updated_at': collected_at,
        }

        for api_field, fund_attr, converter in FIELD_MAPPING:
            value = data.get(api_field)
            if value is None or value == '':
                continue
            if self._is_placeholder_value(value):
                continue
            try:
                converted_value = self._convert_field_value(converter, value)
                if converted_value is not None:
                    update_payload[fund_attr] = converted_value
            except (ValueError, TypeError, AttributeError) as e:
                logger.debug(f"字段映射转换失败 {fund_attr}={value!r}: {e}")

        normalization_ftype = update_payload.get('ftype')
        raw_fundtype = data.get('FUNDTYPE')
        if (
            not normalization_ftype and
            raw_fundtype not in (None, '') and
            not self._is_placeholder_value(raw_fundtype)
        ):
            normalization_ftype = str(raw_fundtype).strip()

        if normalization_ftype:
            type_code, type_category = normalize_fund_type_fields(
                normalization_ftype,
                update_payload.get('type_code'),
                update_payload.get('type_category'),
                allow_bond_text_fallback=False,
            )
            if type_code is not None:
                update_payload['type_code'] = type_code
            if type_category is not None:
                update_payload['type_category'] = type_category

        self._apply_special_field_updates(update_payload, data)
        return update_payload

    @staticmethod
    def _is_placeholder_value(value) -> bool:
        """过滤详情接口中的占位文本，避免把 `--` 等伪值写入主表。"""
        normalized = str(value or '').strip()
        return normalized in PLACEHOLDER_TEXT_VALUES

    @classmethod
    def _apply_trade_status_value(
        cls,
        update_payload: Dict,
        field_name: str,
        raw_value,
    ) -> None:
        """
        仅在状态文本真实可用时写入交易状态字段。

        `BUYTIME` 表示起购信息，不等于申购状态；真实申购/赎回状态应来自
        `fundVarietieValuationDetail` 的 `SGZT/SHZT`。
        """
        if raw_value is None or cls._is_placeholder_value(raw_value):
            return

        normalized = str(raw_value).strip()
        if normalized:
            update_payload[field_name] = normalized

    def _convert_field_value(self, converter: Optional[str], value):
        """转换字段值。"""
        if converter == 'str':
            return str(value)
        if converter == 'int':
            return safe_int(value)
        if converter == 'float':
            return safe_float(value)
        if converter == 'date':
            return parse_date(value)
        if converter == 'percent':
            return safe_float(str(value).replace('%', ''))
        return value

    def _apply_special_field_updates(self, update_payload: Dict, data: Dict):
        """处理需要特殊逻辑的字段。"""
        # Value and report date form one indivisible observation. Unpaired or
        # invalid scale data must not overwrite a known scale/date snapshot.
        from collectors.scale_refresh import _parse_scale
        dated_scale, _scale_reason = _parse_scale(data)
        if dated_scale:
            update_payload.update(dated_scale)

        # 起购金额
        buytime = data.get('BUYTIME')
        if buytime and buytime != '--':
            try:
                amount_str = buytime.replace('万', '').replace(',', '')
                if amount_str:
                    min_purchase = safe_float(amount_str)
                    if min_purchase is not None:
                        update_payload['min_purchase'] = min_purchase * 10000
            except (ValueError, TypeError, AttributeError):
                pass

        # 交易状态只接受显式状态字段，避免把 BUYTIME 误写成“暂停申购”。
        self._apply_trade_status_value(update_payload, 'purchase_status', data.get('SGZT'))
        self._apply_trade_status_value(update_payload, 'redemption_status', data.get('SHZT'))

        # 备选benchmark
        if not update_payload.get('benchmark') and data.get('PERFCMP'):
            update_payload['benchmark'] = data['PERFCMP']

        # 备选特色标签
        if not update_payload.get('feature_tags') and data.get('QDII'):
            update_payload['feature_tags'] = 'QDII'

        # 净资产（单位：元，需转换为亿元）
        if data.get('ZJZC'):
            try:
                net_asset_yuan = safe_float(data['ZJZC'])
                if net_asset_yuan and net_asset_yuan > 0:
                    update_payload['net_asset'] = net_asset_yuan / 100000000
            except (ValueError, TypeError):
                pass

    def _bulk_update_fund_details(
        self,
        updates: List[Dict],
        batch_size=None,
        *,
        record_api_source_evidence: bool = True,
    ) -> Tuple[int, List[Tuple[str, str]]]:
        """
        批量更新基金详情，批次失败时回退到单条更新，避免整批数据丢失。

        Args:
            updates: 更新载荷
            batch_size: 批次大小

        Returns:
            (成功更新数, 失败项目列表)
        """
        if not updates:
            return 0, []

        batch_size = batch_size or self.default_batch_size
        updated_count = 0
        failed_items: List[Tuple[str, str]] = []
        normalized_updates = [dict(item) for item in updates]
        try:
            existing_state_map = self._load_existing_detail_states(
                [item.get('fcode') for item in normalized_updates]
            )
        except Exception as exc:
            # No update has been attempted. Every prepared fund in this batch
            # has an explicit persistence failure, rather than becoming skipped.
            for item in normalized_updates:
                code = str(item.get('fcode') or '').strip()
                message = f"detail_persistence_preparation_failed:{exc}"
                failed_items.append((code, message))
                if record_api_source_evidence:
                    self._ensure_detail_api_source_evidence().record_save_failure(fcode=code, message=message)
            logger.error("基金详情持久化准备失败: batch=%d error=%s", len(normalized_updates), exc)
            return 0, failed_items

        for batch in chunks(normalized_updates, max(1, batch_size)):
            reconciled_batch = [
                self._apply_detail_holding_period_update(
                    self._reconcile_existing_type_state(
                        item,
                        existing_state_map.get(str(item.get('fcode') or '').strip()),
                    ),
                    existing_state_map.get(str(item.get('fcode') or '').strip()),
                )
                for item in batch
            ]
            try:
                with self.db_manager.get_session(task_name='detail_bulk_update') as session:
                    session.bulk_update_mappings(Fund, reconciled_batch)
                updated_count += len(reconciled_batch)
                if record_api_source_evidence:
                    for item in reconciled_batch:
                        self._ensure_detail_api_source_evidence().record_saved_records(
                            fcode=str(item.get('fcode') or '').strip(),
                            record_count=1,
                        )
            except Exception as e:
                logger.warning(f"基金详情批量更新失败，回退单条更新: batch={len(batch)} error={e}")
                for item in reconciled_batch:
                    try:
                        with self.db_manager.get_session(task_name='detail_single_update_fallback') as session:
                            session.bulk_update_mappings(Fund, [item])
                        updated_count += 1
                        if record_api_source_evidence:
                            self._ensure_detail_api_source_evidence().record_saved_records(
                                fcode=str(item.get('fcode') or '').strip(),
                                record_count=1,
                            )
                    except Exception as item_error:
                        error_msg = str(item_error)
                        failed_items.append((item.get('fcode', ''), error_msg))
                        if record_api_source_evidence:
                            self._ensure_detail_api_source_evidence().record_save_failure(
                                fcode=str(item.get('fcode') or '').strip(),
                                message=error_msg,
                            )
                        logger.error(f"基金{item.get('fcode')}详情更新失败: {error_msg}")

        return updated_count, failed_items
