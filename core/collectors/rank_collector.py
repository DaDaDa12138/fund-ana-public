"""
同类排名追踪采集器 - P0-R2新增
采集基金的同类排名历史数据，用于分析排名持续性
"""

import logging
import sqlite3
import threading
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional, Dict, List
from urllib.parse import quote
from collectors.base_collector import BaseCollector
from collectors.source_evidence import ApiSourceEvidence
from database.models import Fund, FundRankHistory
from database.rank_history_queries import (
    load_latest_rank_history_archive_binding,
    normalize_rank_archive_table_name,
    rank_history_archive_binding_matches_database,
)
from utils import ProgressMonitor, chunks, parse_date, safe_int, safe_float
from utils.tiantian_data_quality import inspect_tiantian_rank_payload

logger = logging.getLogger(__name__)


class RankCollector(BaseCollector):
    """同类排名追踪采集器"""

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化排名采集器"""
        super().__init__(config, db_manager, api_client)

        # 采集配置
        collection_config = self.config.get('collection', {})
        self.rank_config = collection_config.get('rank', {})
        self.default_workers = self.rank_config.get('workers', 5)
        self._rank_api_source_evidence = ApiSourceEvidence(
            task="collect-rank-history",
            source="fundRankDiagram",
        )
        self._rank_archive_binding_lock = threading.Lock()
        self._rank_archive_binding_cache: Optional[dict[str, str]] = None

    def _ensure_rank_api_source_evidence(self) -> ApiSourceEvidence:
        if not hasattr(self, "_rank_api_source_evidence") or self._rank_api_source_evidence is None:
            self._rank_api_source_evidence = ApiSourceEvidence(
                task="collect-rank-history",
                source="fundRankDiagram",
            )
        return self._rank_api_source_evidence

    def reset_api_source_evidence(self, **context: Any) -> None:
        self._ensure_rank_api_source_evidence().reset(**context)

    def update_api_source_evidence_context(self, **context: Any) -> None:
        self._ensure_rank_api_source_evidence().update_context(**context)

    def get_api_source_evidence(self, **overrides: Any) -> dict[str, Any]:
        return self._ensure_rank_api_source_evidence().snapshot(**overrides)

    def get_api_source_evidence_json(self, **overrides: Any) -> str:
        return self._ensure_rank_api_source_evidence().snapshot_json(**overrides)

    def collect_all_rank_history(self, workers=None, force_update=False):
        """
        采集所有基金的同类排名历史

        Args:
            workers: 并发数
            force_update: 强制更新（重新采集所有数据）

        Returns:
            采集成功的基金数
        """
        workers = workers or self.default_workers
        logger.info(f"开始采集所有基金的同类排名历史（并发数={workers}）...")

        # 获取所有基金代码
        with self.db_manager.get_session(readonly=True) as session:
            if force_update:
                # 强制更新：采集所有基金
                fcodes = [f.fcode for f in session.query(Fund.fcode).all()]
            else:
                # 仅采集没有排名数据的基金
                existing_fcodes = session.query(FundRankHistory.fcode).distinct().all()
                existing_set = {f[0] for f in existing_fcodes}
                fcodes = [
                    f.fcode for f in session.query(Fund.fcode).all()
                    if f.fcode not in existing_set
                ]

        if not fcodes:
            logger.info("所有基金排名数据已采集完成")
            self.reset_api_source_evidence(
                collection_mode="bulk",
                force_update=bool(force_update),
                target_count=0,
            )
            return 0

        logger.info(f"共{len(fcodes)}只基金需要采集同类排名历史")
        self.reset_api_source_evidence(
            collection_mode="bulk",
            force_update=bool(force_update),
            target_count=len(fcodes),
        )

        # 并发采集
        success_count = 0

        with ProgressMonitor(len(fcodes), desc="采集同类排名", unit="只基金") as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(fcodes))

                for fcode_batch in chunks(fcodes, submit_batch_size):
                    futures = {
                        executor.submit(self.collect_fund_rank_history, fcode): fcode
                        for fcode in fcode_batch
                    }

                    for future in as_completed(futures):
                        fcode = futures[future]
                        try:
                            result = future.result()
                            if result:
                                success_count += 1
                                self.update_stats(success=True)
                            else:
                                self.update_stats(success=False, skipped=True)

                            progress.update(1, success=result)
                            progress.set_postfix(
                                fcode=fcode,
                                total=f"{success_count}"
                            )

                        except Exception as e:
                            logger.error(f"采集基金{fcode}排名历史失败: {e}")
                            self.update_stats(success=False)
                            progress.update(1, success=False)

        self.print_stats()
        return success_count

    def collect_fund_rank_history(self, fcode: str) -> bool:
        """
        采集单只基金的同类排名历史

        Args:
            fcode: 基金代码

        Returns:
            是否成功
        """
        # 获取排名走势数据
        evidence = self._ensure_rank_api_source_evidence()
        evidence.record_request()
        try:
            rank_data = self.api.get_rank_diagram(fcode)
        except Exception as exc:
            evidence.record_transport_exception(fcode=fcode, exc=exc)
            raise

        records, quality = inspect_tiantian_rank_payload(
            rank_data,
            endpoint_label=f"/fundRankDiagram FCODE={fcode}",
        )
        evidence.record_response_quality(
            fcode=fcode,
            quality=quality,
            records=records,
        )
        if not quality.accepted:
            self.record_recent_outcome(
                fcode,
                success=False,
                status=str(quality.status or 'request_failed'),
                message=quality.message,
            )
            logger.warning(f"获取基金{fcode}排名历史失败: {quality.message}")
            return False
        if not quality.has_data:
            self.record_recent_outcome(
                fcode,
                success=False,
                status=str(quality.status or 'empty_rank_records'),
                message=quality.message,
            )
            logger.warning(f"获取基金{fcode}排名历史失败")
            return False

        # 解析并保存
        result = self._save_rank_history(fcode, records)
        if result:
            self.record_recent_outcome(
                fcode,
                success=True,
                status="ok",
                message=f"records={len(records)}",
            )
        else:
            self.record_recent_outcome(
                fcode,
                success=False,
                status="no_effective_rank_records",
                message=f"records={len(records)}",
            )
        return result

    def _rank_archive_binding(self) -> dict[str, str]:
        with self._rank_archive_binding_lock:
            if self._rank_archive_binding_cache is None:
                binding = load_latest_rank_history_archive_binding()
                db_path = str(getattr(self.db_manager, "db_path", "") or "").strip()
                if (
                    not binding
                    or not db_path
                    or not rank_history_archive_binding_matches_database(binding, db_path)
                ):
                    binding = {}
                self._rank_archive_binding_cache = dict(binding)
            return dict(self._rank_archive_binding_cache)

    def _load_archived_rank_records(
        self,
        fcode: str,
        normalized_records: dict[tuple[object, str], dict[str, object]],
    ) -> tuple[dict[tuple[date, str], dict[str, object]], bool]:
        """Load bound cold rows for one fund so exact history is not reinserted hot."""
        binding = self._rank_archive_binding()
        archive_path_text = str(binding.get("archive_path") or "").strip()
        cutoff_text = str(binding.get("archive_cutoff_date") or "").strip()
        if not archive_path_text or not cutoff_text:
            return {}, False
        try:
            cutoff_date = date.fromisoformat(cutoff_text[:10])
        except ValueError:
            return {}, True
        cold_keys = {
            (rank_date, period)
            for rank_date, period in normalized_records
            if isinstance(rank_date, date) and rank_date < cutoff_date
        }
        if not cold_keys:
            return {}, False
        archive_path = Path(archive_path_text)
        if not archive_path.exists() or not archive_path.is_file():
            return {}, True
        table_name = normalize_rank_archive_table_name(binding.get("archive_table_name"))
        archive_uri = f"file:{quote(str(archive_path.resolve()))}?mode=ro"
        try:
            with sqlite3.connect(archive_uri, uri=True, timeout=120) as conn:
                conn.execute("PRAGMA busy_timeout=120000")
                rows = conn.execute(
                    f"""
                    SELECT rank_date, period, rank_position, category_total, percentile
                    FROM {table_name}
                    WHERE fcode = ? AND rank_date < ?
                    """,
                    (fcode, cutoff_date.isoformat()),
                ).fetchall()
        except sqlite3.Error as exc:
            logger.warning("读取基金%s排名冷归档失败，保留热库写入: %s", fcode, exc)
            return {}, True

        archived: dict[tuple[date, str], dict[str, object]] = {}
        for raw_date, raw_period, rank_position, category_total, percentile in rows:
            rank_date = parse_date(str(raw_date or ""))
            if not rank_date:
                continue
            period = str(raw_period or "overall").strip() or "overall"
            key = (rank_date, period)
            if key not in cold_keys:
                continue
            archived[key] = {
                "rank_position": safe_int(rank_position),
                "category_total": safe_int(category_total),
                "percentile": safe_float(percentile),
            }
        return archived, False

    @staticmethod
    def _rank_payload_matches_archive(
        payload: dict[str, object],
        archived: dict[str, object],
    ) -> bool:
        rank_position = safe_int(payload.get("rank_position"))
        category_total = safe_int(payload.get("category_total"))
        archived_rank = safe_int(archived.get("rank_position"))
        archived_total = safe_int(archived.get("category_total"))
        if rank_position != archived_rank or category_total != archived_total:
            return False
        percentile = safe_float(payload.get("percentile"))
        archived_percentile = safe_float(archived.get("percentile"))
        if percentile is None or archived_percentile is None:
            return percentile is None and archived_percentile is None
        return abs(float(percentile) - float(archived_percentile)) <= 1e-12

    def _save_rank_history(self, fcode: str, data_list: List[Dict]) -> bool:
        """
        保存排名历史数据到数据库

        Args:
            fcode: 基金代码
            data_list: 排名历史数据列表

        Returns:
            是否成功
        """
        if not data_list:
            logger.debug(f"基金{fcode}没有排名历史数据")
            return False

        try:
            normalized_records: dict[tuple[object, str], dict[str, object]] = {}

            for item in data_list:
                # 兼容 Tiantian 当前字段(PDATE/QRANK/QSC)与历史字段(x/y/sc)
                date_str = str(item.get('PDATE') or item.get('x') or '').strip()
                if not date_str:
                    continue

                rank_date = parse_date(date_str)
                if not rank_date:
                    if len(date_str) == 7:  # "2024-01" 格式
                        rank_date = parse_date(date_str + "-01")
                    if not rank_date:
                        continue

                rank_position = safe_int(item.get('QRANK'))
                if rank_position is None:
                    rank_position = safe_int(item.get('y'))

                category_total = safe_int(item.get('QSC'))
                if category_total is None:
                    category_total = safe_int(item.get('sc'))

                if not rank_position or not category_total:
                    continue

                period = str(item.get('period', 'overall') or 'overall').strip() or 'overall'
                normalized_records[(rank_date, period)] = {
                    'rank_date': rank_date,
                    'period': period,
                    'rank_position': rank_position,
                    'category_total': category_total,
                    'percentile': rank_position / category_total,
                }

            if not normalized_records:
                logger.debug(f"基金{fcode}排名历史无有效记录")
                return False

            archived_by_key, archive_lookup_error = self._load_archived_rank_records(
                fcode,
                normalized_records,
            )
            records_written = 0
            archive_reused_count = 0
            archive_conflict_count = 0
            with self.db_manager.get_session() as session:
                existing_records = session.query(FundRankHistory).filter_by(fcode=fcode).all()
                existing_by_key = {
                    (record.rank_date, str(record.period or 'overall').strip() or 'overall'): record
                    for record in existing_records
                }

                for (rank_date, period), payload in normalized_records.items():
                    existing = existing_by_key.get((rank_date, period))
                    if existing:
                        existing.rank_position = int(payload['rank_position'])
                        existing.category_total = int(payload['category_total'])
                        existing.percentile = float(payload['percentile'])
                    elif (rank_date, period) in archived_by_key:
                        archived = archived_by_key[(rank_date, period)]
                        if self._rank_payload_matches_archive(payload, archived):
                            archive_reused_count += 1
                            continue
                        archive_conflict_count += 1
                        session.add(
                            FundRankHistory(
                                fcode=fcode,
                                rank_date=rank_date,
                                period=period,
                                rank_position=int(payload['rank_position']),
                                category_total=int(payload['category_total']),
                                percentile=float(payload['percentile']),
                                created_at=datetime.now()
                            )
                        )
                    else:
                        session.add(
                            FundRankHistory(
                                fcode=fcode,
                                rank_date=rank_date,
                                period=period,
                                rank_position=int(payload['rank_position']),
                                category_total=int(payload['category_total']),
                                percentile=float(payload['percentile']),
                                created_at=datetime.now()
                            )
                        )
                    records_written += 1

            self._ensure_rank_api_source_evidence().record_archive_storage_decision(
                reused_record_count=archive_reused_count,
                conflict_record_count=archive_conflict_count,
                lookup_error=archive_lookup_error,
            )
            self._ensure_rank_api_source_evidence().record_saved_records(
                fcode=fcode,
                record_count=records_written,
            )
            logger.debug(
                "基金%s排名历史保存成功，热库写入%s条，冷库复用%s条，冷库冲突%s条",
                fcode,
                records_written,
                archive_reused_count,
                archive_conflict_count,
            )
            return True

        except Exception as e:
            self._ensure_rank_api_source_evidence().record_save_failure(
                fcode=fcode,
                message=str(e),
            )
            self.record_recent_outcome(
                fcode,
                success=False,
                status="save_exception",
                message=str(e),
            )
            logger.error(f"保存基金{fcode}排名历史失败: {e}")
            return False

    def get_rank_history(self, fcode: str, months: int = 12) -> List[Dict]:
        """
        获取基金最近N个月的排名历史

        Args:
            fcode: 基金代码
            months: 月数

        Returns:
            排名历史列表
        """
        with self.db_manager.get_session(readonly=True) as session:
            records = session.query(FundRankHistory).filter_by(
                fcode=fcode
            ).order_by(FundRankHistory.rank_date.desc()).limit(months).all()

            return [
                {
                    'date': r.rank_date,
                    'rank': r.rank_position,
                    'total': r.category_total,
                    'percentile': r.percentile
                }
                for r in records
            ]

    def check_rank_consistency(self, fcode: str, months: int = 12) -> Dict:
        """
        检查基金排名持续性

        Args:
            fcode: 基金代码
            months: 分析月数

        Returns:
            {
                'avg_percentile': float,  # 平均百分位
                'rank_std': float,        # 排名标准差
                'top_30_months': int,     # 排名前30%的月数
                'is_consistent': bool     # 是否持续优秀
            }
        """
        import numpy as np

        ranks = self.get_rank_history(fcode, months)

        if not ranks or len(ranks) < 3:
            return {
                'avg_percentile': None,
                'rank_std': None,
                'top_30_months': 0,
                'is_consistent': False
            }

        percentiles = [r['percentile'] for r in ranks if r['percentile'] is not None]

        if not percentiles:
            return {
                'avg_percentile': None,
                'rank_std': None,
                'top_30_months': 0,
                'is_consistent': False
            }

        avg_percentile = np.mean(percentiles)
        rank_std = np.std(percentiles)
        top_30_months = sum(1 for p in percentiles if p <= 0.30)

        # 持续优秀条件：平均排名前30% 且 波动不大（std<15%）
        is_consistent = avg_percentile <= 0.30 and rank_std < 0.15

        return {
            'avg_percentile': round(avg_percentile, 4),
            'rank_std': round(rank_std, 4),
            'top_30_months': top_30_months,
            'is_consistent': is_consistent
        }
