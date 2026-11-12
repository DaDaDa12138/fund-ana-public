"""
成立日期补充采集器
从API补充所有基金的成立日期数据
"""

import logging
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
from collectors.base_collector import BaseCollector
from database.models import Fund
from utils import ProgressMonitor, chunks
from utils.fund_exclusion_rules import get_runtime_excluded_fund_codes, is_runtime_excluded_fund
from utils.service_checks import check_tiantian_api_ready
from utils.tiantian_data_quality import inspect_tiantian_detail_payload

logger = logging.getLogger(__name__)


class EstabDateCollector(BaseCollector):
    """成立日期补充采集器"""

    _INVALID_ESTABDATE_MARKERS = {
        '', '--', '---', '暂无', '未知',
        'n/a', 'na', 'null', 'none'
    }

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化成立日期采集器"""
        super().__init__(config, db_manager, api_client)

    def supplement_all(self, workers=10, force_update=False):
        """
        补充所有基金的成立日期

        Args:
            workers: 并发数
            force_update: 是否强制更新(即使已有数据)

        Returns:
            采集的基金数
        """
        logger.info("开始补充基金成立日期...")

        # 获取需要补充的基金列表
        funds_to_update = self._get_funds_to_update(force_update)

        if not funds_to_update:
            logger.info("所有基金的成立日期已完整,无需补充")
            return 0

        logger.info(f"共{len(funds_to_update)}只基金需要补充成立日期")

        # 并发采集
        updated_count, skipped_count = self._supplement_concurrent(funds_to_update, workers)
        failed_count = len(funds_to_update) - updated_count - skipped_count

        logger.info(f"成立日期补充完成: 成功{updated_count}只, 跳过{skipped_count}只, 失败{failed_count}只")
        self.print_stats()

        return updated_count

    def _get_funds_to_update(self, force_update=False) -> List[str]:
        """
        获取需要补充成立日期的基金列表

        Args:
            force_update: 是否强制更新

        Returns:
            基金代码列表
        """
        with self.db_manager.get_session(readonly=True) as session:
            if force_update:
                # 强制更新所有基金
                funds = session.query(Fund.fcode).all()
            else:
                # 仅更新成立日期为NULL的基金
                funds = session.query(Fund.fcode).filter(
                    Fund.estab_date.is_(None)
                ).all()

            excluded_codes = set(get_runtime_excluded_fund_codes())
            return [f.fcode for f in funds if f.fcode not in excluded_codes]

    def _supplement_concurrent(self, fcodes: List[str], workers: int) -> Tuple[int, int]:
        """
        并发补充多只基金的成立日期

        Args:
            fcodes: 基金代码列表
            workers: 并发数

        Returns:
            (成功更新数, 跳过数)
        """
        updated_count = 0
        skipped_count = 0

        with ProgressMonitor(len(fcodes), desc="补充成立日期", unit="只基金") as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(fcodes))

                for fcode_batch in chunks(fcodes, submit_batch_size):
                    futures = {
                        executor.submit(self._supplement_single_fund, fcode): fcode
                        for fcode in fcode_batch
                    }

                    # 收集结果
                    for future in as_completed(futures):
                        fcode = futures[future]

                        try:
                            status, reason = future.result()

                            if status == 'updated':
                                updated_count += 1
                                progress.update(1, success=True)
                                self.update_stats(success=True)
                            elif status == 'skipped':
                                skipped_count += 1
                                progress.update(1, skipped=True)
                                self.update_stats(skipped=True)
                            else:
                                progress.update(1, success=False)
                                self.update_stats(success=False)
                                self.log_collection(
                                    task_type='estab_date_supplement',
                                    fcode=fcode,
                                    status='failed',
                                    message=reason
                                )

                            progress.set_postfix(
                                fcode=fcode,
                                updated=updated_count,
                                skipped=skipped_count
                            )

                        except Exception as e:
                            logger.error(f"采集基金{fcode}成立日期失败: {e}")
                            progress.update(1, success=False)
                            self.update_stats(success=False)

                            # 记录失败日志
                            self.log_collection(
                                task_type='estab_date_supplement',
                                fcode=fcode,
                                status='failed',
                                message=str(e)
                            )

        return updated_count, skipped_count

    def _supplement_single_fund(self, fcode: str) -> Tuple[str, str]:
        """
        补充单只基金的成立日期

        Args:
            fcode: 基金代码

        Returns:
            (状态, 原因)
        """
        try:
            normalized_fcode = str(fcode or '').strip()
            if not normalized_fcode:
                logger.warning("成立日期补充收到空基金代码，跳过请求")
                return 'skipped', 'FCODE_EMPTY'

            if is_runtime_excluded_fund(normalized_fcode):
                logger.info("基金%s已在运行时排除名单，跳过成立日期补充", normalized_fcode)
                return 'skipped', 'RUNTIME_EXCLUDED'

            # 获取基金详情
            detail_data = self.api.get_fund_detail(normalized_fcode)
            detail_payload, quality = inspect_tiantian_detail_payload(
                detail_data,
                endpoint_label=f"/fundMNDetailInformation FCODE={normalized_fcode}",
            )

            if not quality.accepted:
                logger.warning("基金%s详情不可用，跳过成立日期补充: %s", normalized_fcode, quality.message)
                if quality.status in {'request_failed', 'upstream_business_failure', 'upstream_business_degraded'}:
                    return 'failed', quality.status.upper()
                if quality.status in {'invalid_datas_type', 'invalid_root_type'}:
                    return 'skipped', 'DETAIL_PAYLOAD_INVALID'
                return 'skipped', 'DETAIL_MISSING'

            if not isinstance(detail_payload, dict):
                logger.warning(f"基金{normalized_fcode}详情为空或格式异常: Datas={type(detail_payload).__name__}")
                return 'skipped', 'DETAIL_PAYLOAD_INVALID'

            # 提取成立日期
            estab_date_str = self._normalize_estab_date_str(detail_payload.get('ESTABDATE'))

            if not estab_date_str:
                logger.warning(f"基金{normalized_fcode}无成立日期数据")
                return 'skipped', 'ESTABDATE_MISSING'

            # 解析日期
            estab_date = self._parse_date(estab_date_str)

            if not estab_date:
                logger.warning(f"基金{normalized_fcode}成立日期格式错误: {estab_date_str}")
                return 'skipped', 'ESTABDATE_PARSE_FAILED'

            # 验证日期合理性
            if not self._validate_date(estab_date):
                logger.warning(f"基金{normalized_fcode}成立日期不合理: {estab_date}")
                return 'skipped', 'ESTABDATE_OUT_OF_RANGE'

            # 更新数据库
            if not self._update_estab_date(normalized_fcode, estab_date):
                return 'failed', 'FUND_NOT_FOUND'

            logger.debug(f"基金{normalized_fcode}成立日期更新为: {estab_date}")
            return 'updated', 'UPDATED'

        except Exception as e:
            logger.error(f"补充基金{fcode}成立日期失败: {e}")
            return 'failed', str(e)

    def _normalize_estab_date_str(self, value: Optional[object]) -> Optional[str]:
        """标准化成立日期字段并过滤占位符。"""
        if value is None:
            return None
        normalized = str(value).strip()
        if not normalized:
            return None
        if normalized.lower() in self._INVALID_ESTABDATE_MARKERS:
            return None
        return normalized

    def _parse_date(self, date_str: str) -> date:
        """
        解析日期字符串

        Args:
            date_str: 日期字符串(支持多种格式)

        Returns:
            date对象或None
        """
        if not date_str:
            return None

        # 去除空格
        date_str = str(date_str).strip()

        # 尝试多种日期格式
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%Y%m%d',
            '%Y.%m.%d',
            '%Y年%m月%d日',
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.date()
            except ValueError:
                continue

        # 如果所有格式都失败,返回None
        return None

    def _validate_date(self, estab_date: date) -> bool:
        """
        验证成立日期的合理性

        Args:
            estab_date: 成立日期

        Returns:
            是否合理
        """
        # 成立日期应该在1990-01-01到今天之间
        min_date = date(1990, 1, 1)
        max_date = date.today()

        return min_date <= estab_date <= max_date

    def _update_estab_date(self, fcode: str, estab_date: date) -> bool:
        """
        更新基金的成立日期

        Args:
            fcode: 基金代码
            estab_date: 成立日期
        """
        with self.db_manager.get_session() as session:
            fund = session.query(Fund).filter_by(fcode=fcode).first()

            if fund:
                fund.estab_date = estab_date
                fund.last_updated_at = datetime.now()
                # session会自动commit(由get_session管理)
                return True
            else:
                logger.warning(f"基金{fcode}不存在于数据库中")
                return False


def main():
    """主函数 - 用于独立运行"""
    from pathlib import Path
    from utils.project_runtime_config import load_runtime_config

    # 加载配置
    config_path = Path(__file__).resolve().parents[2] / 'config' / 'project.yaml'
    config = load_runtime_config(config_path)

    # 创建采集器
    collector = EstabDateCollector(config=config)

    ready, msg = check_tiantian_api_ready(base_url=collector.api_client.base_url)
    if not ready:
        logger.error(f"前置检查失败: {msg}")
        return 2

    # 执行补充
    collector.supplement_all(workers=10, force_update=False)
    return 0


if __name__ == '__main__':
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    raise SystemExit(main())
