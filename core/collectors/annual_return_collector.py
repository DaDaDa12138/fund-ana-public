"""
年度涨幅历史采集器 - P0-R2新增
采集基金的年度涨幅历史数据，用于分析年度表现稳定性
"""

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional, Dict, List
from collectors.base_collector import BaseCollector
from collectors.source_evidence import ApiSourceEvidence
from database.models import Fund, FundAnnualReturns
from utils import ProgressMonitor, chunks, safe_float, safe_int
from utils.tiantian_data_quality import inspect_tiantian_period_increase_payload

logger = logging.getLogger(__name__)


class AnnualReturnCollector(BaseCollector):
    """年度涨幅历史采集器"""

    @staticmethod
    def _extract_year(item: Dict) -> Optional[int]:
        """兼容 Tiantian 当前字段(title/syl)与历史字段(sdate/data)提取年份。"""
        raw_title = str(item.get('title') or item.get('sdate') or '').strip()
        if not raw_title:
            return None

        year_token = raw_title[:4]
        if year_token.isdigit():
            return int(year_token)
        return None

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化年度涨幅采集器"""
        super().__init__(config, db_manager, api_client)

        # 采集配置
        collection_config = self.config.get('collection', {})
        self.annual_config = collection_config.get('annual_return', {})
        self.default_workers = self.annual_config.get('workers', 5)
        self._annual_api_source_evidence = ApiSourceEvidence(
            task="collect-annual-returns",
            source="fundMNPeriodIncrease",
        )

    def _ensure_annual_api_source_evidence(self) -> ApiSourceEvidence:
        if not hasattr(self, "_annual_api_source_evidence") or self._annual_api_source_evidence is None:
            self._annual_api_source_evidence = ApiSourceEvidence(
                task="collect-annual-returns",
                source="fundMNPeriodIncrease",
            )
        return self._annual_api_source_evidence

    def reset_api_source_evidence(self, **context: Any) -> None:
        self._ensure_annual_api_source_evidence().reset(**context)

    def update_api_source_evidence_context(self, **context: Any) -> None:
        self._ensure_annual_api_source_evidence().update_context(**context)

    def get_api_source_evidence(self, **overrides: Any) -> dict[str, Any]:
        return self._ensure_annual_api_source_evidence().snapshot(**overrides)

    def get_api_source_evidence_json(self, **overrides: Any) -> str:
        return self._ensure_annual_api_source_evidence().snapshot_json(**overrides)

    def collect_all_annual_returns(self, workers=None, force_update=False):
        """
        采集所有基金的年度涨幅历史

        Args:
            workers: 并发数
            force_update: 强制更新（重新采集所有数据）

        Returns:
            采集成功的基金数
        """
        workers = workers or self.default_workers
        logger.info(f"开始采集所有基金的年度涨幅历史（并发数={workers}）...")

        # 获取所有基金代码
        with self.db_manager.get_session(readonly=True) as session:
            if force_update:
                # 强制更新：采集所有基金
                fcodes = [f.fcode for f in session.query(Fund.fcode).all()]
            else:
                # 仅采集没有年度数据的基金
                existing_fcodes = session.query(FundAnnualReturns.fcode).distinct().all()
                existing_set = {f[0] for f in existing_fcodes}
                fcodes = [
                    f.fcode for f in session.query(Fund.fcode).all()
                    if f.fcode not in existing_set
                ]

        if not fcodes:
            logger.info("所有基金年度涨幅数据已采集完成")
            self.reset_api_source_evidence(
                collection_mode="bulk",
                force_update=bool(force_update),
                target_count=0,
            )
            return 0

        logger.info(f"共{len(fcodes)}只基金需要采集年度涨幅历史")
        self.reset_api_source_evidence(
            collection_mode="bulk",
            force_update=bool(force_update),
            target_count=len(fcodes),
        )

        # 并发采集
        success_count = 0

        with ProgressMonitor(len(fcodes), desc="采集年度涨幅", unit="只基金") as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(fcodes))

                for fcode_batch in chunks(fcodes, submit_batch_size):
                    futures = {
                        executor.submit(self.collect_fund_annual_returns, fcode): fcode
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
                            logger.error(f"采集基金{fcode}年度涨幅失败: {e}")
                            self.update_stats(success=False)
                            progress.update(1, success=False)

        self.print_stats()
        return success_count

    def collect_fund_annual_returns(self, fcode: str) -> bool:
        """
        采集单只基金的年度涨幅历史

        Args:
            fcode: 基金代码

        Returns:
            是否成功
        """
        # 获取年度涨幅数据
        evidence = self._ensure_annual_api_source_evidence()
        evidence.record_request()
        try:
            period_data = self.api.get_period_increase(fcode, range_value='n')
        except Exception as exc:
            evidence.record_transport_exception(fcode=fcode, exc=exc)
            raise

        records, quality = inspect_tiantian_period_increase_payload(
            period_data,
            endpoint_label=f"/fundMNPeriodIncrease FCODE={fcode} RANGE=n",
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
            logger.warning(f"获取基金{fcode}年度涨幅失败: {quality.message}")
            return False
        if not quality.has_data:
            self.record_recent_outcome(
                fcode,
                success=False,
                status=str(quality.status or 'empty_period_records'),
                message=quality.message,
            )
            logger.warning(f"获取基金{fcode}年度涨幅失败")
            return False

        # 解析并保存
        result = self._save_annual_returns(fcode, records)
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
                status="no_effective_annual_records",
                message=f"records={len(records)}",
            )
        return result

    def _save_annual_returns(self, fcode: str, data_list: List[Dict]) -> bool:
        """
        保存年度涨幅数据到数据库

        Args:
            fcode: 基金代码
            data_list: 年度涨幅数据列表

        Returns:
            是否成功
        """
        if not data_list:
            logger.debug(f"基金{fcode}没有年度涨幅数据")
            return False

        try:
            records_written = 0
            with self.db_manager.get_session() as session:
                for item in data_list:
                    year = self._extract_year(item)
                    if year is None:
                        continue

                    annual_return_val = safe_float(item.get('syl'))
                    if annual_return_val is None:
                        annual_return_val = safe_float(item.get('data'))

                    rank_val = safe_int(item.get('rank'))
                    count_val = safe_int(item.get('sc'))

                    # 检查是否已存在
                    existing = session.query(FundAnnualReturns).filter_by(
                        fcode=fcode, year=year
                    ).first()

                    if existing:
                        # 更新现有记录
                        existing.annual_return = annual_return_val
                        existing.rank_in_category = rank_val
                        existing.category_count = count_val
                        if existing.rank_in_category and existing.category_count:
                            existing.percentile = existing.rank_in_category / existing.category_count
                        else:
                            existing.percentile = None
                        records_written += 1
                    else:
                        # 创建新记录
                        percentile_val = rank_val / count_val if rank_val and count_val else None

                        record = FundAnnualReturns(
                            fcode=fcode,
                            year=year,
                            annual_return=annual_return_val,
                            rank_in_category=rank_val,
                            category_count=count_val,
                            percentile=percentile_val,
                            created_at=datetime.now()
                        )
                        session.add(record)
                        records_written += 1

            # get_session()上下文管理器退出时自动commit，无需显式调用
            if records_written == 0:
                logger.debug(f"基金{fcode}年度涨幅无有效记录")
                return False

            self._ensure_annual_api_source_evidence().record_saved_records(
                fcode=fcode,
                record_count=records_written,
            )
            logger.debug(f"基金{fcode}年度涨幅保存成功，写入{records_written}条记录")
            return True

        except Exception as e:
            self._ensure_annual_api_source_evidence().record_save_failure(
                fcode=fcode,
                message=str(e),
            )
            self.record_recent_outcome(
                fcode,
                success=False,
                status="save_exception",
                message=str(e),
            )
            logger.error(f"保存基金{fcode}年度涨幅失败: {e}")
            return False

    def get_annual_returns(self, fcode: str, years: int = 5) -> List[float]:
        """
        获取基金最近N年的年度涨幅列表

        Args:
            fcode: 基金代码
            years: 年数

        Returns:
            年度涨幅列表（按年份降序）
        """
        with self.db_manager.get_session(readonly=True) as session:
            records = session.query(FundAnnualReturns).filter_by(
                fcode=fcode
            ).order_by(FundAnnualReturns.year.desc()).limit(years).all()

            return [r.annual_return for r in records if r.annual_return is not None]

    def check_annual_consistency(self, fcode: str, years: int = 5) -> Dict:
        """
        检查基金年度表现稳定性

        Args:
            fcode: 基金代码
            years: 分析年数

        Returns:
            {
                'positive_years': int,  # 正收益年数
                'total_years': int,     # 总年数
                'annual_std': float,    # 年度收益标准差
                'is_consistent': bool   # 是否稳定（≥4年正收益 且 std<5%）
            }
        """
        import numpy as np

        returns = self.get_annual_returns(fcode, years)

        if not returns or len(returns) < 2:
            return {
                'positive_years': 0,
                'total_years': 0,
                'annual_std': None,
                'is_consistent': False
            }

        positive_years = sum(1 for r in returns if r > 0)
        annual_std = np.std(returns)

        return {
            'positive_years': positive_years,
            'total_years': len(returns),
            'annual_std': round(annual_std, 4),
            'is_consistent': positive_years >= 4 and annual_std < 5.0
        }
