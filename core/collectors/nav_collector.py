"""
历史净值采集器 - 核心模块
支持: 分批处理、并发采集、断点续传
"""

import json
import logging
import os
import time
import sqlite3
import threading
from collections import Counter
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Dict, Optional
from sqlalchemy import text, func
from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
from collectors.base_collector import BaseCollector
from database.models import Fund, FundNavHistory, FundReturns
from utils import ProgressMonitor, BatchProgress, parse_date, safe_float, safe_int, chunks, json_dumps
from utils.tiantian_data_quality import inspect_tiantian_nav_history_payload

logger = logging.getLogger(__name__)


NAV_API_SOURCE_SAMPLE_LIMIT = 5


class NavCollector(BaseCollector):
    """历史净值采集器"""

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化净值采集器"""
        super().__init__(config, db_manager, api_client)

        # 采集配置
        collection_config = self.config.get('collection', {})
        self.history_config = collection_config.get('history', {})
        self.incremental_config = collection_config.get('incremental', {})

        self.default_years = self.history_config.get('default_years', 5)
        self.page_size = self.history_config.get('page_size', 100)

        # 增量采集配置
        self.incremental_workers = self.incremental_config.get('workers', 3)
        self.incremental_qps = self.incremental_config.get('qps', 80)
        self.stale_threshold_days = self.incremental_config.get('stale_threshold_days', 90)
        self.incremental_max_age_days = self.incremental_config.get(
            'max_age_days',
            self.stale_threshold_days,
        )
        self.incremental_only_active = self.incremental_config.get('only_active', False)
        self.empty_nav_cooldown_hours = max(
            0,
            int(
                self.incremental_config.get(
                    "empty_nav_payload_cooldown_hours",
                    os.environ.get("FUND_ANA_NAV_EMPTY_PAYLOAD_COOLDOWN_HOURS", "0"),
                )
                or 0
            ),
        )
        self.no_advance_nav_cooldown_minutes = max(
            0,
            int(
                self.incremental_config.get(
                    "no_advance_payload_cooldown_minutes",
                    os.environ.get("FUND_ANA_NAV_NO_ADVANCE_COOLDOWN_MINUTES", "0"),
                )
                or 0
            ),
        )
        self._empty_nav_lock = threading.Lock()
        self._empty_nav_records: List[Dict] = []
        self.empty_nav_report_path: Optional[str] = None
        self._no_advance_nav_lock = threading.Lock()
        self._no_advance_nav_records: List[Dict] = []
        self.no_advance_nav_report_path: Optional[str] = None
        self._updated_nav_codes_lock = threading.Lock()
        self._updated_nav_codes: set[str] = set()
        self._nav_api_source_evidence_lock = threading.Lock()
        self._nav_api_source_evidence = self._new_nav_api_source_evidence()
        self.empty_nav_report_dir = Path(
            os.environ.get(
                "NAV_EMPTY_REPORT_DIR",
                str(Path(__file__).resolve().parent.parent / "logs" / "tasks"),
            )
        )
        self.empty_nav_report_dir.mkdir(parents=True, exist_ok=True)
        self.no_advance_nav_report_dir = self.empty_nav_report_dir
        # 计算每个worker的请求间隔，确保总体QPS不超过限制
        # 总QPS = workers * (1 / interval)，因此 interval = workers / qps
        if self.incremental_qps > 0 and self.incremental_workers > 0:
            self.min_request_interval = max(0.01, self.incremental_workers / self.incremental_qps)
        else:
            self.min_request_interval = 0.01

    @staticmethod
    def _resolve_retry_workers(workers: int) -> int:
        """重试阶段使用更保守的并发数，避免反而高于主流程。"""
        try:
            normalized_workers = int(workers)
        except (TypeError, ValueError):
            normalized_workers = 1
        return max(1, normalized_workers // 2)

    @staticmethod
    def _resolve_submit_batch_size(workers: int, total_items: int) -> int:
        """限制单轮提交的 future 数量，降低大任务的内存与调度开销。"""
        try:
            normalized_workers = int(workers)
        except (TypeError, ValueError):
            normalized_workers = 1
        try:
            normalized_total = int(total_items)
        except (TypeError, ValueError):
            normalized_total = 0

        normalized_workers = max(1, normalized_workers)
        submit_batch_size = max(normalized_workers * 4, 200)
        if normalized_total > 0:
            return min(normalized_total, submit_batch_size)
        return submit_batch_size

    @staticmethod
    def _new_nav_api_source_evidence() -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "task": "collect-nav",
            "source": "fundMNHisNetList",
            "collection_mode": None,
            "total_fund_count": 0,
            "target_fund_count": 0,
            "skipped_new_fund_count": 0,
            "skipped_fresh_fund_count": 0,
            "skipped_out_of_window_count": 0,
            "skipped_stale_fund_count": 0,
            "skipped_recent_empty_payload_count": 0,
            "skipped_recent_no_advance_count": 0,
            "reference_nav_date": None,
            "local_reference_nav_date": None,
            "expected_reference_nav_date": None,
            "reference_strategy": None,
            "request_count": 0,
            "response_count": 0,
            "accepted_response_count": 0,
            "failed_response_count": 0,
            "has_data_response_count": 0,
            "empty_response_count": 0,
            "transport_exception_count": 0,
            "quality_status_counts": {},
            "quality_payload_type_counts": {},
            "requested_page_index_counts": {},
            "min_requested_page_size": None,
            "max_requested_page_size": None,
            "returned_nav_record_count": 0,
            "inserted_nav_record_count": 0,
            "updated_fund_count": 0,
            "no_new_nav_record_response_count": 0,
            "main_failed_fund_count": 0,
            "retry_attempted_fund_count": 0,
            "retry_failed_fund_count": 0,
            "final_failed_fund_count": 0,
            "earliest_payload_nav_date": None,
            "latest_payload_nav_date": None,
            "failure_samples": [],
            "empty_response_samples": [],
            "no_advance_samples": [],
        }

    def _ensure_nav_api_source_evidence_state(self) -> None:
        if not hasattr(self, "_nav_api_source_evidence_lock"):
            self._nav_api_source_evidence_lock = threading.Lock()
        if not hasattr(self, "_nav_api_source_evidence") or not isinstance(self._nav_api_source_evidence, dict):
            self._nav_api_source_evidence = self._new_nav_api_source_evidence()

    def _reset_nav_api_source_evidence(self, *, collection_mode: str) -> None:
        self._ensure_nav_api_source_evidence_state()
        evidence = self._new_nav_api_source_evidence()
        evidence["collection_mode"] = str(collection_mode or "").strip() or None
        with self._nav_api_source_evidence_lock:
            self._nav_api_source_evidence = evidence

    def _update_nav_api_source_evidence_context(self, **values: Any) -> None:
        self._ensure_nav_api_source_evidence_state()
        with self._nav_api_source_evidence_lock:
            for key, value in values.items():
                if hasattr(value, "isoformat"):
                    self._nav_api_source_evidence[key] = value.isoformat()
                else:
                    self._nav_api_source_evidence[key] = value

    @staticmethod
    def _increment_mapping_counter(mapping: Dict[str, int], key: Any, amount: int = 1) -> None:
        normalized_key = str(key or "unknown").strip() or "unknown"
        mapping[normalized_key] = safe_int(mapping.get(normalized_key), 0) + int(amount)

    @staticmethod
    def _append_limited_sample(samples: list, sample: Dict[str, Any]) -> None:
        if len(samples) >= NAV_API_SOURCE_SAMPLE_LIMIT:
            return
        samples.append(sample)

    def _record_nav_api_request(self, *, fcode: str, page_index: int, page_size: int) -> None:
        self._ensure_nav_api_source_evidence_state()
        page_size_int = safe_int(page_size)
        with self._nav_api_source_evidence_lock:
            evidence = self._nav_api_source_evidence
            evidence["request_count"] = safe_int(evidence.get("request_count")) + 1
            self._increment_mapping_counter(
                evidence.setdefault("requested_page_index_counts", {}),
                page_index,
            )
            if page_size_int > 0:
                current_min = evidence.get("min_requested_page_size")
                current_max = evidence.get("max_requested_page_size")
                evidence["min_requested_page_size"] = (
                    page_size_int
                    if current_min in (None, "")
                    else min(safe_int(current_min), page_size_int)
                )
                evidence["max_requested_page_size"] = (
                    page_size_int
                    if current_max in (None, "")
                    else max(safe_int(current_max), page_size_int)
                )

    def _record_nav_api_transport_failure(
        self,
        *,
        fcode: str,
        page_index: int,
        page_size: int,
        exc: Exception,
    ) -> None:
        self._ensure_nav_api_source_evidence_state()
        with self._nav_api_source_evidence_lock:
            evidence = self._nav_api_source_evidence
            evidence["transport_exception_count"] = safe_int(evidence.get("transport_exception_count")) + 1
            evidence["failed_response_count"] = safe_int(evidence.get("failed_response_count")) + 1
            self._increment_mapping_counter(evidence.setdefault("quality_status_counts", {}), "transport_exception")
            self._append_limited_sample(
                evidence.setdefault("failure_samples", []),
                {
                    "fcode": str(fcode or "").strip(),
                    "page_index": safe_int(page_index),
                    "page_size": safe_int(page_size),
                    "status": "transport_exception",
                    "message": str(exc)[:300],
                },
            )

    def _record_nav_api_response_quality(
        self,
        *,
        fcode: str,
        page_index: int,
        quality,
        records: list[Dict[str, Any]],
    ) -> None:
        self._ensure_nav_api_source_evidence_state()
        record_count = len(records or [])
        payload_dates: list[date] = []
        for item in records or []:
            if not isinstance(item, dict):
                continue
            parsed_date = parse_date(item.get("FSRQ"))
            if parsed_date:
                payload_dates.append(parsed_date)

        with self._nav_api_source_evidence_lock:
            evidence = self._nav_api_source_evidence
            evidence["response_count"] = safe_int(evidence.get("response_count")) + 1
            if bool(getattr(quality, "accepted", False)):
                evidence["accepted_response_count"] = safe_int(evidence.get("accepted_response_count")) + 1
            else:
                evidence["failed_response_count"] = safe_int(evidence.get("failed_response_count")) + 1

            if bool(getattr(quality, "has_data", False)):
                evidence["has_data_response_count"] = safe_int(evidence.get("has_data_response_count")) + 1
            else:
                evidence["empty_response_count"] = safe_int(evidence.get("empty_response_count")) + 1

            evidence["returned_nav_record_count"] = (
                safe_int(evidence.get("returned_nav_record_count")) + record_count
            )
            self._increment_mapping_counter(
                evidence.setdefault("quality_status_counts", {}),
                getattr(quality, "status", None),
            )
            self._increment_mapping_counter(
                evidence.setdefault("quality_payload_type_counts", {}),
                getattr(quality, "payload_type", None),
            )

            if payload_dates:
                local_min = min(payload_dates).isoformat()
                local_max = max(payload_dates).isoformat()
                current_min = str(evidence.get("earliest_payload_nav_date") or "").strip()
                current_max = str(evidence.get("latest_payload_nav_date") or "").strip()
                evidence["earliest_payload_nav_date"] = (
                    local_min if not current_min or local_min < current_min else current_min
                )
                evidence["latest_payload_nav_date"] = (
                    local_max if not current_max or local_max > current_max else current_max
                )

            sample = {
                "fcode": str(fcode or "").strip(),
                "page_index": safe_int(page_index),
                "status": str(getattr(quality, "status", "") or "").strip(),
                "message": str(getattr(quality, "message", "") or "").strip()[:300],
            }
            if not bool(getattr(quality, "accepted", False)):
                self._append_limited_sample(evidence.setdefault("failure_samples", []), sample)
            elif not bool(getattr(quality, "has_data", False)):
                self._append_limited_sample(evidence.setdefault("empty_response_samples", []), sample)

    def _record_nav_no_new_records(self, *, fcode: str, last_nav_date, latest_payload_date: str) -> None:
        self._ensure_nav_api_source_evidence_state()
        with self._nav_api_source_evidence_lock:
            evidence = self._nav_api_source_evidence
            evidence["no_new_nav_record_response_count"] = (
                safe_int(evidence.get("no_new_nav_record_response_count")) + 1
            )
            self._append_limited_sample(
                evidence.setdefault("no_advance_samples", []),
                {
                    "fcode": str(fcode or "").strip(),
                    "last_nav_date": last_nav_date.isoformat() if hasattr(last_nav_date, "isoformat") else str(last_nav_date or ""),
                    "latest_payload_date": str(latest_payload_date or "").strip(),
                },
            )

    def _record_nav_saved_records(self, *, fcode: str, saved_count: int) -> None:
        self._ensure_nav_api_source_evidence_state()
        with self._nav_api_source_evidence_lock:
            evidence = self._nav_api_source_evidence
            evidence["inserted_nav_record_count"] = (
                safe_int(evidence.get("inserted_nav_record_count")) + safe_int(saved_count)
            )

    def _request_nav_history(self, fcode: str, *, page_index: int, page_size: int):
        self._record_nav_api_request(
            fcode=fcode,
            page_index=page_index,
            page_size=page_size,
        )
        try:
            return self.api.get_nav_history(fcode, page_index=page_index, page_size=page_size)
        except Exception as exc:
            self._record_nav_api_transport_failure(
                fcode=fcode,
                page_index=page_index,
                page_size=page_size,
                exc=exc,
            )
            raise

    def get_nav_api_source_evidence(self, *, inserted_nav_record_count: int | None = None) -> Dict[str, Any]:
        self._ensure_nav_api_source_evidence_state()
        with self._nav_api_source_evidence_lock:
            evidence = dict(self._nav_api_source_evidence)
            for key in (
                "quality_status_counts",
                "quality_payload_type_counts",
                "requested_page_index_counts",
            ):
                evidence[key] = dict(evidence.get(key) or {})
            for key in ("failure_samples", "empty_response_samples", "no_advance_samples"):
                evidence[key] = list(evidence.get(key) or [])

        if inserted_nav_record_count is not None:
            evidence["inserted_nav_record_count"] = safe_int(inserted_nav_record_count)

        self._ensure_updated_nav_tracking_state()
        with self._updated_nav_codes_lock:
            evidence["updated_fund_count"] = len(self._updated_nav_codes)

        return evidence

    def get_nav_api_source_evidence_json(self, *, inserted_nav_record_count: int | None = None) -> str:
        return json_dumps(
            self.get_nav_api_source_evidence(
                inserted_nav_record_count=inserted_nav_record_count,
            )
        )

    def _extract_nav_records_or_raise(self, fcode: str, payload, *, page_index: int):
        """统一校验净值 payload，避免终态失败被误吞为“无更新”或“已清盘”."""
        records, quality = inspect_tiantian_nav_history_payload(
            payload,
            endpoint_label=f"/fundMNHisNetList FCODE={fcode} page={page_index}",
        )
        self._record_nav_api_response_quality(
            fcode=fcode,
            page_index=page_index,
            quality=quality,
            records=records,
        )
        if not quality.accepted:
            raise RuntimeError(f"基金{fcode}净值接口不可用: {quality.message}")
        return records, quality

    def _ensure_updated_nav_tracking_state(self) -> None:
        """兼容 __new__ 构造的测试替身，惰性补齐最近更新基金跟踪状态。"""
        if not hasattr(self, "_updated_nav_codes_lock"):
            self._updated_nav_codes_lock = threading.Lock()
        if not hasattr(self, "_updated_nav_codes") or not isinstance(self._updated_nav_codes, set):
            self._updated_nav_codes = set()

    def collect_all_incremental(self, workers=None):
        """
        增量采集所有基金的历史净值（仅采集缺失的日期）

        优化说明（v2）：
        - 批量预取所有基金的 last_nav_date（1 次 GROUP BY 替代 25000 次 SELECT）
        - 预分类：跳过已最新、无历史、疑似清盘的基金
        - 移除手动 sleep（API client 已内置 rate_limiter）
        - 动态 page_size（按需获取，而非固定 2000）
        - 所有读查询使用 readonly=True（避免全局写锁阻塞）

        Args:
            workers: 并发数（如果为None，则从配置文件读取）

        Returns:
            采集的净值记录数
        """
        if workers is None:
            workers = self.incremental_workers
        self._reset_nav_api_source_evidence(collection_mode="incremental")

        logger.info(
            "开始增量采集所有基金的历史净值（并发数: %s, QPS: %s, 最大账龄: %s天, 仅活跃: %s）...",
            workers,
            self.incremental_qps,
            self.incremental_max_age_days,
            self.incremental_only_active,
        )
        with self._empty_nav_lock:
            self._empty_nav_records = []
        self.empty_nav_report_path = None
        with self._no_advance_nav_lock:
            self._no_advance_nav_records = []
        self.no_advance_nav_report_path = None
        self._ensure_updated_nav_tracking_state()
        with self._updated_nav_codes_lock:
            self._updated_nav_codes = set()

        # 获取所有基金
        funds = self._get_all_funds(only_active=self.incremental_only_active)

        if not funds:
            logger.warning("没有找到基金数据，请先运行基金列表采集")
            self._update_nav_api_source_evidence_context(
                total_fund_count=0,
                target_fund_count=0,
            )
            return 0

        # ⚡ 优化：1 次批量查询替代 25000 次 N+1
        last_nav_dates = self._get_all_last_nav_dates()
        reference_nav_date = self._get_incremental_reference_nav_date()

        # 预分类：跳过无历史数据、已最新、疑似清盘的基金
        today = datetime.now().date()
        needs_update = []
        skip_new = 0
        skip_fresh = 0
        skip_out_of_window = 0
        skip_stale = 0
        skip_recent_empty_payload = 0
        skip_recent_no_advance = 0

        for fcode in funds:
            last_date = last_nav_dates.get(fcode)
            if not last_date:
                skip_new += 1
                continue
            skip_current, lag_days = self._is_incremental_candidate_current(
                last_date=last_date,
                reference_nav_date=reference_nav_date,
                fallback_date=today,
            )

            if skip_current:
                skip_fresh += 1
                continue
            if lag_days > self.incremental_max_age_days:
                if lag_days > self.stale_threshold_days:
                    skip_stale += 1
                else:
                    skip_out_of_window += 1
                continue
            if lag_days > self.stale_threshold_days:
                skip_stale += 1
                continue
            needs_update.append((fcode, last_date))

        if needs_update:
            recent_empty_payload_codes = self._load_recent_empty_nav_payload_codes(
                [fcode for fcode, _last_date in needs_update]
            )
            if recent_empty_payload_codes:
                filtered_pairs = []
                for fcode, last_date in needs_update:
                    if fcode in recent_empty_payload_codes:
                        skip_recent_empty_payload += 1
                        continue
                    filtered_pairs.append((fcode, last_date))
                needs_update = filtered_pairs
        if needs_update:
            recent_no_advance_codes = self._load_recent_no_advance_nav_codes(
                needs_update,
                reference_nav_date=reference_nav_date,
            )
            if recent_no_advance_codes:
                filtered_pairs = []
                for fcode, last_date in needs_update:
                    if fcode in recent_no_advance_codes:
                        skip_recent_no_advance += 1
                        continue
                    filtered_pairs.append((fcode, last_date))
                needs_update = filtered_pairs

        logger.info(
            f"共{len(funds)}只基金：参考净值日={reference_nav_date or 'fallback_today'}，需更新{len(needs_update)}，"
            f"已最新{skip_fresh}，新基金{skip_new}，"
            f"超出增量窗口{skip_out_of_window}，疑似清盘{skip_stale}，"
            f"近期空净值冷却{skip_recent_empty_payload}，"
            f"近期未推进冷却{skip_recent_no_advance}"
        )
        self._update_nav_api_source_evidence_context(
            total_fund_count=len(funds),
            target_fund_count=len(needs_update),
            skipped_new_fund_count=skip_new,
            skipped_fresh_fund_count=skip_fresh,
            skipped_out_of_window_count=skip_out_of_window,
            skipped_stale_fund_count=skip_stale,
            skipped_recent_empty_payload_count=skip_recent_empty_payload,
            skipped_recent_no_advance_count=skip_recent_no_advance,
            reference_nav_date=reference_nav_date,
        )

        # 并发采集（仅需要更新的基金）
        total_records, failed_funds = self._collect_funds_concurrent_v2(
            needs_update, workers, reference_nav_date=reference_nav_date
        )
        main_failed_count = len(failed_funds or [])
        retry_attempted_count = 0
        retry_failed_count = main_failed_count

        # 重试失败的基金
        if failed_funds:
            logger.info(f"\n增量采集主阶段完成，开始重试失败的{len(failed_funds)}只基金")

            failed_pairs = [
                (f['fcode'], last_nav_dates.get(f['fcode']))
                for f in failed_funds
                if last_nav_dates.get(f['fcode'])
            ]
            retry_workers = self._resolve_retry_workers(workers)
            retry_attempted_count = len(failed_pairs)

            import time
            time.sleep(10)  # 短暂等待

            retry_records, still_failed = self._collect_funds_concurrent_v2(
                failed_pairs, retry_workers, reference_nav_date=reference_nav_date
            )
            total_records += retry_records
            retry_failed_count = len(still_failed or [])

            if still_failed:
                logger.warning(f"增量采集完成后仍有{len(still_failed)}只基金失败")
            else:
                logger.info("重试成功，所有基金已更新")
        self._update_nav_api_source_evidence_context(
            main_failed_fund_count=main_failed_count,
            retry_attempted_fund_count=retry_attempted_count,
            retry_failed_fund_count=retry_failed_count,
            final_failed_fund_count=retry_failed_count,
            inserted_nav_record_count=total_records,
        )

        self.print_stats()
        empty_nav_report = self._write_empty_nav_report()
        if empty_nav_report:
            logger.info("空净值候选报告: %s", empty_nav_report)
        no_advance_nav_report = self._write_no_advance_nav_report()
        if no_advance_nav_report:
            logger.info("未推进净值候选报告: %s", no_advance_nav_report)
        return total_records

    def get_recent_updated_nav_codes(self) -> list[str]:
        self._ensure_updated_nav_tracking_state()
        with self._updated_nav_codes_lock:
            return sorted(self._updated_nav_codes)

    def collect_all_full(self, workers=3, batch_size=2000, batch_interval=1800):
        """
        全量采集所有基金的历史净值（首次采集）

        Args:
            workers: 并发数
            batch_size: 每批基金数量
            batch_interval: 批次间休息时间（秒）

        Returns:
            采集的净值记录数
        """
        self._reset_nav_api_source_evidence(collection_mode="full")
        logger.info(f"开始全量采集历史净值（近{self.default_years}年）...")
        logger.info(f"配置: workers={workers}, batch_size={batch_size}, interval={batch_interval}s")

        # 获取所有基金
        funds = self._get_all_funds()

        if not funds:
            logger.warning("没有找到基金数据")
            self._update_nav_api_source_evidence_context(
                total_fund_count=0,
                target_fund_count=0,
            )
            return 0

        logger.info(f"共{len(funds)}只基金需要采集")
        self._update_nav_api_source_evidence_context(
            total_fund_count=len(funds),
            target_fund_count=len(funds),
        )

        # 分批处理
        batches = list(chunks(funds, batch_size))
        total_batches = len(batches)

        logger.info(f"分为{total_batches}批，每批{batch_size}只基金")

        total_records = 0
        all_failed_funds = []  # 跟踪所有批次的失败基金

        # 主采集阶段
        for batch_num, batch in enumerate(batches, 1):
            logger.info(f"{'='*60}")
            logger.info(f"开始批次 {batch_num}/{total_batches}")
            logger.info(f"{'='*60}")

            # 并发采集本批次
            records, failed = self._collect_funds_concurrent(batch, workers, incremental=False)
            total_records += records
            all_failed_funds.extend(failed)

            logger.info(f"批次{batch_num}完成: 采集{records:,}条净值记录")
            if failed:
                logger.warning(f"批次{batch_num}失败: {len(failed)}只基金")

            # 批次间休息（最后一批不休息）
            if batch_num < total_batches:
                import time
                logger.info(f"休息{batch_interval}秒后继续下一批...")
                time.sleep(batch_interval)

        # 重试阶段 - 自动重试失败的基金
        if all_failed_funds:
            logger.info(f"\n{'='*60}")
            logger.info(f"主采集完成，开始重试失败的基金")
            logger.info(f"失败基金数: {len(all_failed_funds)}")
            logger.info(f"{'='*60}")

            # 提取失败基金代码
            failed_codes = [f['fcode'] for f in all_failed_funds]

            # 使用较少的并发数重试，避免数据库连接池耗尽
            retry_workers = self._resolve_retry_workers(workers)
            max_retries = 2

            import time
            for retry_attempt in range(1, max_retries + 1):
                if not failed_codes:
                    break

                logger.info(f"\n重试第 {retry_attempt}/{max_retries} 轮，基金数: {len(failed_codes)}")
                logger.info(f"使用并发数: {retry_workers}")

                # 等待30秒后重试，让数据库连接恢复
                time.sleep(30)

                retry_records, still_failed = self._collect_funds_concurrent(
                    failed_codes, retry_workers, incremental=False
                )
                total_records += retry_records

                logger.info(f"重试轮次{retry_attempt}完成: 成功{len(failed_codes) - len(still_failed)}只，失败{len(still_failed)}只")

                # 更新失败列表
                failed_codes = [f['fcode'] for f in still_failed]

            # 最终汇总
            if failed_codes:
                logger.warning(f"\n⚠️  最终仍有 {len(failed_codes)} 只基金采集失败:")
                for fcode in failed_codes[:20]:  # 显示前20个
                    logger.warning(f"  - {fcode}")
                if len(failed_codes) > 20:
                    logger.warning(f"  ... 共 {len(failed_codes)} 只失败")
            else:
                logger.info("\n✅ 所有基金采集成功！")
            self._update_nav_api_source_evidence_context(
                main_failed_fund_count=len(all_failed_funds),
                retry_attempted_fund_count=len(all_failed_funds),
                retry_failed_fund_count=len(failed_codes),
                final_failed_fund_count=len(failed_codes),
            )
        else:
            self._update_nav_api_source_evidence_context(
                main_failed_fund_count=0,
                retry_attempted_fund_count=0,
                retry_failed_fund_count=0,
                final_failed_fund_count=0,
            )

        logger.info(f"\n全量采集完成: 共采集{total_records:,}条净值记录")
        self._update_nav_api_source_evidence_context(inserted_nav_record_count=total_records)
        self.print_stats()

        return total_records

    def collect_fund_history(self, fcode: str, years=None, force_update=False):
        """
        采集单只基金的历史净值

        Args:
            fcode: 基金代码
            years: 采集年限（默认5年）
            force_update: 强制更新（忽略已有数据）

        Returns:
            采集的记录数
        """
        years = years or self.default_years
        self._reset_nav_api_source_evidence(collection_mode="single")
        self._update_nav_api_source_evidence_context(
            total_fund_count=1,
            target_fund_count=1,
        )

        logger.info(f"采集基金{fcode}的历史净值（近{years}年）...")

        # 如果不是强制更新，检查已有数据
        if not force_update:
            last_date = self._get_last_nav_date(fcode)
            if last_date:
                logger.debug(f"基金{fcode}已有净值数据，最后日期: {last_date}")
                # 仅采集最后日期之后的数据（单只基金模式，不跳过新基金）
                return self._collect_incremental_nav(fcode, skip_new_funds=False)

        # 全量采集
        return self._collect_full_nav(fcode, years)

    def _get_all_funds(self, only_active: bool = False) -> List[str]:
        """
        获取所有基金代码

        Args:
            only_active: 是否仅返回活跃基金。当前以 is_buyable 作为活跃近似口径，
                NULL 视为未知，不直接排除。

        Returns:
            基金代码列表
        """
        with self.db_manager.get_session(readonly=True) as session:
            if not only_active:
                funds = session.query(Fund.fcode).all()
                result = []
                for row in funds:
                    try:
                        result.append(row[0])
                    except Exception:
                        result.append(getattr(row, 'fcode'))
                return result

            funds = session.query(Fund.fcode, Fund.is_buyable).all()

            result = []
            for row in funds:
                try:
                    fcode, is_buyable = row[0], row[1]
                except Exception:
                    fcode = getattr(row, 'fcode')
                    is_buyable = getattr(row, 'is_buyable', None)

                if is_buyable is False:
                    continue
                result.append(fcode)

            return result

    def _get_last_nav_date(self, fcode: str) -> Optional[datetime.date]:
        """
        获取基金最后的净值日期

        Args:
            fcode: 基金代码

        Returns:
            最后净值日期或None
        """
        with self.db_manager.get_session(readonly=True) as session:
            return session.query(func.max(FundNavHistory.nav_date)).filter_by(
                fcode=fcode
            ).scalar()

    def _get_all_last_nav_dates(self) -> dict:
        """
        批量获取所有基金的最后净值日期（1 次 GROUP BY 替代 N 次 SELECT）

        Returns:
            {fcode: last_nav_date} 字典
        """

        with self.db_manager.get_session(readonly=True) as session:
            results = session.query(
                FundNavHistory.fcode,
                func.max(FundNavHistory.nav_date)
            ).group_by(FundNavHistory.fcode).all()

            return {row[0]: row[1] for row in results}

    def _get_all_returns_latest_dates(self) -> dict:
        """
        批量获取收益表中的最新净值日期。

        用 fund_returns.latest_nav_date 作为“已知最新净值日”，
        避免增量窗口直接按墙上日期判断，导致周末/节假日或调度时点把真实
        仅落后 1~3 个交易日的基金误判为超窗。

        Returns:
            {fcode: latest_nav_date} 字典
        """
        with self.db_manager.get_session(readonly=True) as session:
            results = session.query(
                FundReturns.fcode,
                FundReturns.latest_nav_date,
            ).filter(
                FundReturns.latest_nav_date.isnot(None)
            ).all()

            return {row[0]: row[1] for row in results}

    @staticmethod
    def _select_incremental_reference_nav_date(
        distribution_rows: list[tuple[date | None, int]],
    ) -> date | None:
        normalized: list[tuple[date, int]] = []
        for raw_date, raw_count in distribution_rows:
            if raw_date is None:
                continue
            try:
                count = int(raw_count or 0)
            except (TypeError, ValueError):
                count = 0
            if count <= 0:
                continue
            normalized.append((raw_date, count))
        if not normalized:
            return None
        normalized.sort(key=lambda item: (item[1], item[0]), reverse=True)
        return normalized[0][0]

    @staticmethod
    def _previous_weekday(value: date) -> date:
        candidate = value - timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate -= timedelta(days=1)
        return candidate

    @classmethod
    def _resolve_expected_incremental_reference_nav_date(
        cls,
        *,
        now: datetime | None = None,
        same_day_publish_hour: int = 18,
    ) -> date:
        current = now or datetime.now()
        today = current.date()
        if today.weekday() >= 5:
            return cls._previous_weekday(today)
        try:
            publish_hour = max(0, min(23, int(same_day_publish_hour)))
        except (TypeError, ValueError):
            publish_hour = 18
        if current.hour >= publish_hour:
            return today
        return cls._previous_weekday(today)

    @classmethod
    def _resolve_incremental_reference_nav_date(
        cls,
        distribution_rows: list[tuple[date | None, int]],
        *,
        now: datetime | None = None,
        same_day_publish_hour: int = 18,
    ) -> tuple[date | None, dict[str, Any]]:
        local_reference = cls._select_incremental_reference_nav_date(distribution_rows)
        expected_reference = cls._resolve_expected_incremental_reference_nav_date(
            now=now,
            same_day_publish_hour=same_day_publish_hour,
        )
        if local_reference is None:
            strategy = "expected_business_date_no_local_distribution"
            reference = expected_reference
        elif local_reference < expected_reference:
            strategy = "expected_business_date_overrides_stale_local_peak"
            reference = expected_reference
        else:
            strategy = "local_distribution_peak"
            reference = local_reference
        return reference, {
            "local_reference_nav_date": local_reference,
            "expected_reference_nav_date": expected_reference,
            "reference_strategy": strategy,
        }

    @staticmethod
    def _is_incremental_candidate_current(
        *,
        last_date: date,
        reference_nav_date: date | None,
        fallback_date: date,
    ) -> tuple[bool, int]:
        reference_date = reference_nav_date or fallback_date
        lag_days = max((reference_date - last_date).days, 0)
        fresh_lag_threshold = 0 if reference_nav_date is not None else 1
        return lag_days <= fresh_lag_threshold, lag_days

    def _get_incremental_reference_nav_date(self) -> date | None:
        with self.db_manager.get_session(readonly=True) as session:
            results = session.query(
                FundReturns.latest_nav_date,
                func.count(FundReturns.fcode),
            ).filter(
                FundReturns.latest_nav_date.isnot(None)
            ).group_by(
                FundReturns.latest_nav_date
            ).all()
        reference_date, reference_context = self._resolve_incremental_reference_nav_date(
            [(row[0], row[1]) for row in results],
            same_day_publish_hour=self.incremental_config.get(
                "same_day_nav_publish_hour",
                18,
            ),
        )
        self._update_nav_api_source_evidence_context(
            reference_nav_date=reference_date,
            **reference_context,
        )
        return reference_date

    @staticmethod
    def _parse_iso_datetime(value) -> Optional[datetime]:
        normalized = str(value or "").strip()
        if not normalized:
            return None
        normalized = normalized.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    def _load_latest_empty_nav_report_payload(self) -> tuple[Optional[Path], Optional[Dict]]:
        if not self.empty_nav_report_dir.exists():
            return None, None

        candidates = sorted(
            self.empty_nav_report_dir.glob("nav_empty_payload_candidates_*.json"),
            key=lambda path: (path.stat().st_mtime, path.name),
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if isinstance(payload, dict):
                return path, payload
        return None, None

    def _load_recent_empty_nav_payload_codes(self, candidate_codes: list[str]) -> set[str]:
        if self.empty_nav_cooldown_hours <= 0:
            return set()

        normalized_codes = sorted({str(code or "").strip() for code in candidate_codes if str(code or "").strip()})
        if not normalized_codes:
            return set()

        _, report_payload = self._load_latest_empty_nav_report_payload()
        if not isinstance(report_payload, dict):
            return set()

        generated_at = self._parse_iso_datetime(report_payload.get("generated_at"))
        if generated_at is None:
            return set()
        if generated_at < datetime.now() - timedelta(hours=self.empty_nav_cooldown_hours):
            return set()

        fund_records = report_payload.get("funds") or []
        if not isinstance(fund_records, list):
            return set()

        cooled_codes: set[str] = set()
        for item in fund_records:
            if not isinstance(item, dict):
                continue
            fcode = str(item.get("fcode") or "").strip()
            if fcode and fcode in normalized_codes:
                cooled_codes.add(fcode)
        return cooled_codes

    def _load_latest_no_advance_nav_report_payload(self) -> tuple[Optional[Path], Optional[Dict]]:
        if not self.no_advance_nav_report_dir.exists():
            return None, None

        candidates = sorted(
            self.no_advance_nav_report_dir.glob("nav_no_advance_candidates_*.json"),
            key=lambda path: (path.stat().st_mtime, path.name),
            reverse=True,
        )
        for path in candidates:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
            if isinstance(payload, dict):
                return path, payload
        return None, None

    def _load_recent_no_advance_nav_codes(
        self,
        candidate_pairs: list[tuple[str, date]],
        *,
        reference_nav_date: date | None,
    ) -> set[str]:
        if self.no_advance_nav_cooldown_minutes <= 0 or reference_nav_date is None:
            return set()

        normalized_pairs = {
            str(fcode or "").strip(): last_date
            for fcode, last_date in candidate_pairs
            if str(fcode or "").strip()
        }
        if not normalized_pairs:
            return set()

        _, report_payload = self._load_latest_no_advance_nav_report_payload()
        if not isinstance(report_payload, dict):
            return set()

        generated_at = self._parse_iso_datetime(report_payload.get("generated_at"))
        if generated_at is None:
            return set()
        if generated_at < datetime.now() - timedelta(minutes=self.no_advance_nav_cooldown_minutes):
            return set()

        fund_records = report_payload.get("funds") or []
        if not isinstance(fund_records, list):
            return set()

        cooled_codes: set[str] = set()
        expected_reference_date = reference_nav_date.isoformat()
        for item in fund_records:
            if not isinstance(item, dict):
                continue
            fcode = str(item.get("fcode") or "").strip()
            if not fcode or fcode not in normalized_pairs:
                continue
            if str(item.get("reference_nav_date") or "").strip() != expected_reference_date:
                continue
            if str(item.get("last_nav_date") or "").strip() != normalized_pairs[fcode].isoformat():
                continue
            cooled_codes.add(fcode)
        return cooled_codes

    def _record_empty_nav_payload_candidate(
        self,
        *,
        fcode: str,
        last_nav_date,
        reference_nav_date: date | None,
        quality,
    ) -> None:
        normalized_fcode = str(fcode or "").strip()
        if not normalized_fcode:
            return
        with self._empty_nav_lock:
            self._empty_nav_records.append(
                {
                    "fcode": normalized_fcode,
                    "last_nav_date": last_nav_date.isoformat() if hasattr(last_nav_date, "isoformat") else str(last_nav_date or ""),
                    "reference_nav_date": reference_nav_date.isoformat() if hasattr(reference_nav_date, "isoformat") else str(reference_nav_date or ""),
                    "reason": str(getattr(quality, "status", "") or "empty_nav_records"),
                    "message": str(getattr(quality, "message", "") or "").strip(),
                    "captured_at": datetime.now().isoformat(timespec="seconds"),
                }
            )

    def _record_no_advance_nav_candidate(
        self,
        *,
        fcode: str,
        last_nav_date,
        reference_nav_date: date | None,
        latest_payload_date: str,
    ) -> None:
        normalized_fcode = str(fcode or "").strip()
        if not normalized_fcode:
            return
        with self._no_advance_nav_lock:
            self._no_advance_nav_records.append(
                {
                    "fcode": normalized_fcode,
                    "last_nav_date": last_nav_date.isoformat() if hasattr(last_nav_date, "isoformat") else str(last_nav_date or ""),
                    "reference_nav_date": reference_nav_date.isoformat() if hasattr(reference_nav_date, "isoformat") else str(reference_nav_date or ""),
                    "latest_payload_date": str(latest_payload_date or "").strip(),
                    "reason": "no_advance_nav_records",
                    "captured_at": datetime.now().isoformat(timespec="seconds"),
                }
            )

    def _write_empty_nav_report(self) -> Optional[str]:
        with self._empty_nav_lock:
            if not self._empty_nav_records:
                self.empty_nav_report_path = None
                return None
            records = list(self._empty_nav_records)

        deduped_records = {}
        for record in records:
            deduped_records.setdefault(record["fcode"], record)
        records = list(deduped_records.values())

        fcodes = [record["fcode"] for record in records]
        funds_by_code = {}
        with self.db_manager.get_session(readonly=True, task_name="nav_empty_report") as session:
            fund_rows = (
                session.query(
                    Fund.fcode,
                    Fund.shortname,
                    Fund.ftype,
                    Fund.type_category,
                )
                .filter(Fund.fcode.in_(fcodes))
                .all()
            )
            for row in fund_rows:
                funds_by_code[row.fcode] = {
                    "shortname": row.shortname,
                    "ftype": row.ftype,
                    "type_category": row.type_category,
                }

        enriched_records = []
        reason_counter = Counter()
        ftype_counter = Counter()
        for record in records:
            fund_info = funds_by_code.get(record["fcode"], {})
            enriched = {**record, **fund_info}
            enriched_records.append(enriched)
            reason_counter[record["reason"]] += 1
            ftype_counter[fund_info.get("ftype") or "UNKNOWN"] += 1

        report = {
            "report_type": "nav_empty_payload_candidates",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(enriched_records),
            "cooldown_hours": self.empty_nav_cooldown_hours,
            "reason_counts": dict(reason_counter),
            "ftype_counts": dict(ftype_counter),
            "funds": sorted(enriched_records, key=lambda item: item["fcode"]),
        }

        report_path = self.empty_nav_report_dir / (
            f"nav_empty_payload_candidates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.empty_nav_report_path = str(report_path)
        return self.empty_nav_report_path

    def _write_no_advance_nav_report(self) -> Optional[str]:
        with self._no_advance_nav_lock:
            if not self._no_advance_nav_records:
                self.no_advance_nav_report_path = None
                return None
            records = list(self._no_advance_nav_records)

        deduped_records = {}
        for record in records:
            deduped_records.setdefault(record["fcode"], record)
        records = list(deduped_records.values())

        fcodes = [record["fcode"] for record in records]
        funds_by_code = {}
        with self.db_manager.get_session(readonly=True, task_name="nav_no_advance_report") as session:
            fund_rows = (
                session.query(
                    Fund.fcode,
                    Fund.shortname,
                    Fund.ftype,
                    Fund.type_category,
                )
                .filter(Fund.fcode.in_(fcodes))
                .all()
            )
            for row in fund_rows:
                funds_by_code[row.fcode] = {
                    "shortname": row.shortname,
                    "ftype": row.ftype,
                    "type_category": row.type_category,
                }

        enriched_records = []
        ftype_counter = Counter()
        for record in records:
            fund_info = funds_by_code.get(record["fcode"], {})
            enriched = {**record, **fund_info}
            enriched_records.append(enriched)
            ftype_counter[fund_info.get("ftype") or "UNKNOWN"] += 1

        report = {
            "report_type": "nav_no_advance_candidates",
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(enriched_records),
            "cooldown_minutes": self.no_advance_nav_cooldown_minutes,
            "ftype_counts": dict(ftype_counter),
            "funds": sorted(enriched_records, key=lambda item: item["fcode"]),
        }

        report_path = self.no_advance_nav_report_dir / (
            f"nav_no_advance_candidates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.no_advance_nav_report_path = str(report_path)
        return self.no_advance_nav_report_path

    def _collect_funds_concurrent(self, fcodes: List[str], workers: int, incremental: bool) -> tuple:
        """
        并发采集多只基金的净值

        Args:
            fcodes: 基金代码列表
            workers: 并发数
            incremental: 是否增量更新

        Returns:
            (总记录数, 失败基金列表)
        """
        total_records = 0
        failed_funds = []  # 跟踪失败的基金

        with ProgressMonitor(len(fcodes), desc="采集净值", unit="只基金") as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(fcodes))
                for fcode_batch in chunks(fcodes, submit_batch_size):
                    futures = {}
                    for fcode in fcode_batch:
                        if incremental:
                            future = executor.submit(self._collect_incremental_nav, fcode)
                        else:
                            future = executor.submit(self._collect_full_nav, fcode, self.default_years)

                        futures[future] = fcode

                    # 收集结果
                    for future in as_completed(futures):
                        fcode = futures[future]

                        try:
                            records = future.result()
                            total_records += records

                            progress.update(1, success=True)
                            progress.set_postfix(
                                fcode=fcode,
                                records=f"{total_records:,}"
                            )

                            self.update_stats(success=True)

                        except Exception as e:
                            logger.error(f"采集基金{fcode}失败: {e}")
                            progress.update(1, success=False)
                            self.update_stats(success=False)

                            # 添加到失败列表
                            failed_funds.append({
                                'fcode': fcode,
                                'error': str(e)
                            })

                            # 记录失败日志
                            self.log_collection(
                                task_type='nav_collection',
                                fcode=fcode,
                                status='failed',
                                message=str(e)
                            )

        return total_records, failed_funds

    def _collect_funds_concurrent_v2(
        self,
        fund_date_pairs: list,
        workers: int,
        *,
        reference_nav_date: date | None = None,
    ) -> tuple:
        """
        并发采集（优化版：接收预取的 last_date 列表，无 N+1 查询）

        Args:
            fund_date_pairs: [(fcode, last_date), ...]
            workers: 并发数

        Returns:
            (总记录数, 失败基金列表)
        """
        total_records = 0
        failed_funds = []

        with ProgressMonitor(len(fund_date_pairs), desc="采集净值", unit="只基金") as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(fund_date_pairs))
                for fund_batch in chunks(fund_date_pairs, submit_batch_size):
                    futures = {}
                    for fcode, last_date in fund_batch:
                        future = executor.submit(
                            self._collect_incremental_nav_v2,
                            fcode,
                            last_date,
                            reference_nav_date,
                        )
                        futures[future] = fcode

                    for future in as_completed(futures):
                        fcode = futures[future]
                        try:
                            records = future.result()
                            total_records += records
                            progress.update(1, success=True)
                            progress.set_postfix(
                                fcode=fcode,
                                records=f"{total_records:,}"
                            )
                            self.update_stats(success=True)
                        except Exception as e:
                            logger.error(f"采集基金{fcode}失败: {e}")
                            progress.update(1, success=False)
                            self.update_stats(success=False)
                            failed_funds.append({
                                'fcode': fcode,
                                'error': str(e)
                            })
                            self.log_collection(
                                task_type='nav_collection',
                                fcode=fcode,
                                status='failed',
                                message=str(e)
                            )

        return total_records, failed_funds

    def _collect_incremental_nav(self, fcode: str, skip_new_funds: bool = True) -> int:
        """
        增量采集净值（仅采集缺失的日期）- 优化版

        修复说明：
        - 原代码：新基金会触发5年全量采集，严重拖慢增量更新任务
        - 优化：增量更新时跳过新基金，由专门的任务处理新基金采集

        Args:
            fcode: 基金代码
            skip_new_funds: 是否跳过新基金（默认True，增量更新时跳过）

        Returns:
            采集的记录数
        """

        # 获取最后日期
        last_date = self._get_last_nav_date(fcode)

        if not last_date:
            # 没有历史数据
            if skip_new_funds:
                # 增量更新时跳过新基金，避免拖慢整体进度
                logger.debug(f"基金{fcode}无历史数据，增量模式跳过（将由专门任务处理）")
                return 0
            else:
                # 非增量模式：全量采集
                return self._collect_full_nav(fcode, self.default_years)

        # 计算需要更新的天数
        days_since = (datetime.now().date() - last_date).days

        if days_since <= 1:
            # 已是最新，跳过
            logger.debug(f"基金{fcode}净值已是最新")
            return 0

        # ⚡ 优化：跳过长期未更新的基金（可能已清盘或处于封闭期）
        if days_since > self.stale_threshold_days:
            logger.info(f"基金{fcode}超过{days_since}天未更新，疑似已清盘或封闭期，跳过采集")
            return 0

        # ⚡ 优化：移除冗余sleep — API client 已内置 rate_limiter.acquire()
        # 仅请求第一页（最近数据）
        nav_data = self._request_nav_history(fcode, page_index=1, page_size=self.page_size)
        nav_records, quality = self._extract_nav_records_or_raise(fcode, nav_data, page_index=1)

        if not quality.has_data:
            return 0

        # 筛选出新数据（日期大于last_date）
        new_records = []
        for item in nav_records:
            nav_date = parse_date(item.get('FSRQ'))
            if nav_date and nav_date > last_date:
                new_records.append(item)

        if not new_records:
            return 0

        # 保存新数据
        saved = self._save_nav_records(fcode, new_records)
        self._record_nav_saved_records(fcode=fcode, saved_count=saved)
        logger.debug(f"基金{fcode}增量更新{saved}条净值")

        return saved

    def _collect_incremental_nav_v2(self, fcode: str, last_date, reference_nav_date: date | None = None) -> int:
        """
        增量采集净值（优化版：无 sleep、小 page_size、last_date 已预取）

        优化点：
        1. last_date 已批量预取，无需再查数据库（消除 N+1）
        2. 移除 time.sleep() — API client 已内置 rate_limiter.acquire()
        3. 动态 page_size（按实际需要的天数 + 余量，而非固定 2000）

        Args:
            fcode: 基金代码
            last_date: 预取的最后净值日期

        Returns:
            采集的记录数
        """
        days_since = (datetime.now().date() - last_date).days

        # 动态 page_size：按实际需要的天数 × 2（留余量），最小 10，上限 self.page_size
        page_size = min(max(days_since * 2, 10), self.page_size)

        # API 请求（rate limiter 由 api client 自动控制，无需手动 sleep）
        nav_data = self._request_nav_history(fcode, page_index=1, page_size=page_size)
        nav_records, quality = self._extract_nav_records_or_raise(fcode, nav_data, page_index=1)

        if not quality.has_data:
            self.record_recent_outcome(
                fcode,
                success=False,
                status=str(quality.status or "empty_nav_records"),
                message=quality.message,
            )
            self._record_empty_nav_payload_candidate(
                fcode=fcode,
                last_nav_date=last_date,
                reference_nav_date=reference_nav_date,
                quality=quality,
            )
            return 0

        # 筛选新数据（日期大于 last_date）
        new_records = []
        for item in nav_records:
            nav_date = parse_date(item.get('FSRQ'))
            if nav_date and nav_date > last_date:
                new_records.append(item)

        if not new_records:
            latest_payload_date = ""
            if nav_records:
                latest_payload_date = str(nav_records[0].get("FSRQ") or "").strip()
            self._record_nav_no_new_records(
                fcode=fcode,
                last_nav_date=last_date,
                latest_payload_date=latest_payload_date,
            )
            self.record_recent_outcome(
                fcode,
                success=True,
                status="no_new_nav_records",
                message=f"last_nav_date={last_date}",
            )
            if latest_payload_date and latest_payload_date == last_date.isoformat():
                self._record_no_advance_nav_candidate(
                    fcode=fcode,
                    last_nav_date=last_date,
                    reference_nav_date=reference_nav_date,
                    latest_payload_date=latest_payload_date,
                )
            return 0

        saved = self._save_nav_records(fcode, new_records)
        self._record_nav_saved_records(fcode=fcode, saved_count=saved)
        self._ensure_updated_nav_tracking_state()
        with self._updated_nav_codes_lock:
            self._updated_nav_codes.add(str(fcode))
        self.record_recent_outcome(
            fcode,
            success=True,
            status="ok",
            message=f"records={saved}",
        )
        logger.debug(f"基金{fcode}增量更新{saved}条净值")
        return saved

    def _collect_full_nav(self, fcode: str, years: int) -> int:
        """
        全量采集净值

        Args:
            fcode: 基金代码
            years: 采集年限

        Returns:
            采集的记录数
        """
        # 第一页
        first_page = self._request_nav_history(fcode, page_index=1, page_size=self.page_size)
        first_page_records, first_page_quality = self._extract_nav_records_or_raise(
            fcode,
            first_page,
            page_index=1,
        )

        if not first_page_quality.has_data:
            return 0

        total_count = safe_int(first_page.get('TotalCount')) if isinstance(first_page, dict) else None
        effective_total_count = max(total_count or len(first_page_records), len(first_page_records))

        # 计算需要的记录数（约years*252个交易日）
        needed_records = years * 252

        # 计算需要的页数
        pages_needed = min(
            (needed_records + self.page_size - 1) // self.page_size,
            (effective_total_count + self.page_size - 1) // self.page_size
        )

        all_records = list(first_page_records)

        # 收集所有页的数据
        for page in range(2, pages_needed + 1):
            page_data = self._request_nav_history(fcode, page_index=page, page_size=self.page_size)
            page_records, page_quality = self._extract_nav_records_or_raise(
                fcode,
                page_data,
                page_index=page,
            )
            if not page_quality.has_data:
                raise RuntimeError(
                    f"基金{fcode}净值接口返回意外空页: page={page} expected_pages={pages_needed}"
                )

            all_records.extend(page_records)

            # 检查是否已获取足够数据
            if len(all_records) >= needed_records:
                break

        # 保存数据
        saved = self._save_nav_records(fcode, all_records)
        self._record_nav_saved_records(fcode=fcode, saved_count=saved)
        logger.debug(f"基金{fcode}全量采集{saved}条净值")

        return saved

    def _save_nav_records(self, fcode: str, records: List[Dict]) -> int:
        """
        保存净值记录到数据库（批量插入）

        Args:
            fcode: 基金代码
            records: 净值记录列表

        Returns:
            保存的记录数
        """
        if not records:
            return 0

        # 解析数据
        params = []
        for item in records:
            try:
                nav_date = parse_date(item.get('FSRQ'))
                unit_nav = safe_float(item.get('DWJZ'))

                if not nav_date or unit_nav is None:
                    continue

                params.append({
                    'fcode': fcode,
                    'nav_date': nav_date,
                    'unit_nav': unit_nav,
                    'accum_nav': safe_float(item.get('LJJZ')),
                    'daily_growth': safe_float(item.get('JZZZL')),
                })

            except Exception as e:
                logger.error(f"解析净值数据失败: {e}")
                continue

        if not params:
            return 0

        insert_sql = text(
            """INSERT OR REPLACE INTO fund_nav_history
               (fcode, nav_date, unit_nav, accum_nav, daily_growth)
               VALUES (:fcode, :nav_date, :unit_nav, :accum_nav, :daily_growth)"""
        )
        latest_param = max(params, key=lambda item: item["nav_date"])
        update_returns_sql = text(
            """UPDATE fund_returns
               SET latest_nav_date = :latest_nav_date,
                   latest_nav = :latest_nav,
                   daily_growth = :daily_growth,
                   last_updated_at = :last_updated_at
               WHERE fcode = :fcode
                 AND (
                   latest_nav_date IS NULL
                   OR TRIM(CAST(latest_nav_date AS TEXT)) = ''
                   OR latest_nav_date <= :latest_nav_date
                 )"""
        )
        insert_returns_sql = text(
            """INSERT OR IGNORE INTO fund_returns
               (fcode, latest_nav, latest_nav_date, daily_growth, last_updated_at)
               VALUES (:fcode, :latest_nav, :latest_nav_date, :daily_growth, :last_updated_at)"""
        )

        # 批量插入（使用 SQLAlchemy Core 的 session.execute 保持事务一致性）
        # 添加重试机制处理并发写冲突
        saved_count = 0
        max_retries = 3
        retry_delay = 0.1  # 初始延迟100ms

        for attempt in range(max_retries):
            try:
                with self.db_manager.get_session() as session:
                    # 批量执行（SQLAlchemy 会自动管理事务）
                    session.execute(insert_sql, params)
                    returns_params = {
                        "fcode": fcode,
                        "latest_nav_date": latest_param["nav_date"],
                        "latest_nav": latest_param["unit_nav"],
                        "daily_growth": latest_param["daily_growth"],
                        "last_updated_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    update_result = session.execute(update_returns_sql, returns_params)
                    if int(getattr(update_result, "rowcount", 0) or 0) <= 0:
                        session.execute(insert_returns_sql, returns_params)
                    saved_count = len(params)

                # 成功则退出重试循环
                break

            except (sqlite3.OperationalError, SQLAlchemyOperationalError) as e:
                error_msg = str(e).lower()

                # 检查是否是数据库锁定错误
                is_lock_error = (
                    'database is locked' in error_msg or
                    'cannot commit' in error_msg or
                    'no transaction is active' in error_msg
                )

                if is_lock_error and attempt < max_retries - 1:
                    # 指数退避重试
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(
                        f"数据库写入冲突（尝试{attempt + 1}/{max_retries}），"
                        f"{wait_time:.2f}秒后重试: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"批量保存净值失败（{fcode}）: {e}")
                    raise

            except Exception as e:
                # 非数据库操作错误（编程错误等），直接记录并抛出，不重试
                logger.error(f"批量保存净值时发生未预期错误（{fcode}）: {type(e).__name__}: {e}")
                raise

        return saved_count
