"""
持有期信息补充采集器
从基金名称中提取持有期限制信息
"""

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Dict, Tuple, Optional
from sqlalchemy import and_, func, or_

from collectors.base_collector import BaseCollector
from database.models import Fund
from utils import ProgressMonitor, chunks
from utils.holding_period_parser import (
    build_holding_update_fields,
    parse_holding_detail_struct,
    parse_holding_match,
    parse_holding_with_priority,
)
from utils.service_checks import check_tiantian_api_ready

logger = logging.getLogger(__name__)

NO_SIGNAL_REVIEWED_SOURCE = "no_signal_reviewed"


class HoldingPeriodCollector(BaseCollector):
    """持有期信息补充采集器"""

    def __init__(self, config=None, db_manager=None, api_client=None):
        """初始化持有期采集器"""
        super().__init__(config, db_manager, api_client)
        self.last_run_summary: Dict[str, int] | None = None

    def supplement_all(self, workers=10, force_update=False):
        """
        补充所有基金的持有期信息

        Args:
            workers: 并发数
            force_update: 是否强制更新(即使已有数据)

        Returns:
            更新的基金数
        """
        logger.info("开始补充基金持有期信息...")

        # 获取需要补充的基金列表
        funds_to_update = self._get_funds_to_update(force_update)

        if not funds_to_update:
            logger.info("所有基金的持有期信息已完整,无需补充")
            return 0

        logger.info(f"共{len(funds_to_update)}只基金需要分析持有期")

        # 并发处理
        summary = self._supplement_concurrent(funds_to_update, workers)
        self.last_run_summary = summary

        logger.info(
            "持有期信息补充完成: 直接更新%s只, 份额族传播%s只, 跳过%s只, 失败%s只",
            summary['direct_updated'],
            summary['share_family_propagated'],
            summary['skipped'],
            summary['failed'],
        )
        self._print_run_summary(summary)

        return int(summary['updated'])

    def _get_funds_to_update(self, force_update=False) -> List[Dict]:
        """
        获取需要补充持有期的基金列表

        Args:
            force_update: 是否强制更新

        Returns:
            基金信息列表 [{'fcode': xxx, 'shortname': xxx}, ...]
        """
        with self.db_manager.get_session(readonly=True) as session:
            # P1-01 / B13: 多选 4 级识别需要的 CYCLE / PRSVPERIOD / BUYTIME 字段，
            # 以及 company_id（L4 份额族传播 key 的一部分）。
            base_cols = [
                Fund.fcode,
                Fund.shortname,
                Fund.fullname,
                Fund.ftype,
                Fund.investment_target,
                Fund.investment_strategy,
                Fund.has_holding_period,
                Fund.holding_days,
                Fund.holding_keywords,
                Fund.cycle,
                Fund.preserve_period,
                Fund.buy_time,
                Fund.holding_period_source,
                Fund.company_id,
            ]
            if force_update:
                funds = session.query(*base_cols).filter(Fund.ftype.like('%债%')).all()
            else:
                has_struct_detail = or_(
                    and_(Fund.cycle.is_not(None), func.trim(Fund.cycle) != ""),
                    and_(Fund.preserve_period.is_not(None), func.trim(Fund.preserve_period) != ""),
                    and_(Fund.buy_time.is_not(None), func.trim(Fund.buy_time) != ""),
                )
                needs_keyword_backfill = and_(
                    Fund.has_holding_period == 1,
                    Fund.holding_keywords.is_(None),
                    Fund.holding_period_source.is_not(None),
                    func.trim(func.coalesce(Fund.holding_period_source, "")) != "",
                )
                needs_source_backfill = or_(
                    needs_keyword_backfill,
                    Fund.holding_period_source.is_(None),
                    func.trim(func.coalesce(Fund.holding_period_source, "")) == "",
                )
                needs_struct_refresh = and_(
                    has_struct_detail,
                    or_(
                        Fund.holding_period_source.is_(None),
                        func.trim(func.coalesce(Fund.holding_period_source, "")) == "",
                        Fund.holding_period_source == NO_SIGNAL_REVIEWED_SOURCE,
                        Fund.holding_period_source != "detail_struct",
                    ),
                )
                funds = session.query(*base_cols).filter(
                    Fund.ftype.like('%债%'),
                    or_(needs_source_backfill, needs_struct_refresh)
                ).all()

            funds_to_consider = [
                {
                    'fcode': f.fcode,
                    'shortname': f.shortname,
                    'fullname': f.fullname,
                    'ftype': f.ftype,
                    'investment_target': f.investment_target,
                    'investment_strategy': f.investment_strategy,
                    'has_holding_period': f.has_holding_period,
                    'holding_days': f.holding_days,
                    'holding_keywords': f.holding_keywords,
                    'cycle': f.cycle,
                    'preserve_period': f.preserve_period,
                    'buy_time': f.buy_time,
                    'holding_period_source': f.holding_period_source,
                    'company_id': f.company_id,
                }
                for f in funds
            ]
            if force_update:
                return funds_to_consider
            return [fund for fund in funds_to_consider if self._should_scan_fund_for_holding_refresh(fund)]

    @staticmethod
    def _has_struct_detail_values(fund: Dict) -> bool:
        return any(str(fund.get(key) or "").strip() for key in ("cycle", "preserve_period", "buy_time"))

    @staticmethod
    def _needs_keyword_backfill_for_confirmed_holding(fund: Dict) -> bool:
        current_source = str(fund.get("holding_period_source") or "").strip()
        return (
            int(fund.get("has_holding_period") or 0) == 1
            and fund.get("holding_keywords") is None
            and bool(current_source)
        )

    @staticmethod
    def _normalize_optional_int(value: Any) -> Optional[int]:
        if isinstance(value, int):
            return int(value)
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    def _is_actionable_struct_refresh_candidate(self, fund: Dict) -> bool:
        if not self._has_struct_detail_values(fund):
            return False

        struct_match = parse_holding_detail_struct(
            cycle=fund.get("cycle"),
            preserve_period=fund.get("preserve_period"),
            buy_time=fund.get("buy_time"),
        )
        if struct_match is None:
            return False

        current_source = str(fund.get("holding_period_source") or "").strip()
        current_has = int(fund.get("has_holding_period") or 0)
        current_days = self._normalize_optional_int(fund.get("holding_days"))
        target_days = int(struct_match.days)

        if current_has != 1 or current_days is None:
            return True
        if current_days == target_days:
            return current_source != "detail_struct"
        return target_days > current_days

    def _should_scan_fund_for_holding_refresh(self, fund: Dict) -> bool:
        current_source = str(fund.get("holding_period_source") or "").strip()
        if not current_source:
            return True
        if self._needs_keyword_backfill_for_confirmed_holding(fund):
            return True
        return self._is_actionable_struct_refresh_candidate(fund)

    def _supplement_concurrent(self, funds: List[Dict], workers: int) -> Dict[str, int]:
        """
        并发补充多只基金的持有期信息

        Args:
            funds: 基金信息列表
            workers: 并发数

        Returns:
            本轮扫描摘要
        """
        direct_updated_count = 0
        skipped_count = 0
        failed_count = 0
        propagated_count = 0
        pending_updates: List[Dict] = []
        outcome_by_fcode: Dict[str, str] = {}

        with ProgressMonitor(
            len(funds),
            desc="分析持有期",
            unit="只基金",
            show_completion_summary=False,
        ) as progress:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                submit_batch_size = self._resolve_submit_batch_size(workers, len(funds))

                for fund_batch in chunks(funds, submit_batch_size):
                    futures = {
                        executor.submit(self._build_holding_update, fund): fund
                        for fund in fund_batch
                    }

                    # 收集结果
                    for future in as_completed(futures):
                        fund = futures[future]

                        try:
                            update_payload = future.result()

                            if update_payload:
                                pending_updates.append(update_payload)
                                direct_updated_count += 1
                                outcome_by_fcode[str(fund.get('fcode') or '').strip()] = 'updated'
                                progress.update(1, success=True)
                                self.update_stats(success=True)
                            else:
                                skipped_count += 1
                                outcome_by_fcode[str(fund.get('fcode') or '').strip()] = 'skipped'
                                progress.update(1, skipped=True)
                                self.update_stats(skipped=True)

                            progress.set_postfix(
                                fcode=fund['fcode'],
                                updated=direct_updated_count
                            )

                        except Exception as e:
                            logger.error(f"分析基金{fund['fcode']}持有期失败: {e}")
                            failed_count += 1
                            outcome_by_fcode[str(fund.get('fcode') or '').strip()] = 'failed'
                            progress.update(1, success=False)
                            self.update_stats(success=False)

            # P1-01.4 / B13: 同 mother_code/品牌系列下若 A 类已识别、C/E 未识别，
            # 保守继承（长持有期优先）。放在 concurrent 扫描之后、bulk_update 之前。
            propagated_updates = self._apply_share_family_propagation(funds, pending_updates)
            propagated_count = len(propagated_updates)
            if propagated_count:
                pending_updates.extend(propagated_updates)
                for item in propagated_updates:
                    fcode = str(item.get('fcode') or '').strip()
                    previous_status = outcome_by_fcode.get(fcode)
                    outcome_by_fcode[fcode] = 'propagated'
                    with progress._lock:
                        progress.success_count += 1
                        if previous_status == 'skipped':
                            progress.skipped_count = max(progress.skipped_count - 1, 0)
                        elif previous_status == 'failed':
                            progress.failure_count = max(progress.failure_count - 1, 0)
                    with self._stats_lock:
                        self.stats['success'] += 1
                        if previous_status == 'skipped':
                            skipped_count = max(skipped_count - 1, 0)
                            self.stats['skipped'] = max(self.stats['skipped'] - 1, 0)
                        elif previous_status == 'failed':
                            failed_count = max(failed_count - 1, 0)
                            self.stats['failed'] = max(self.stats['failed'] - 1, 0)
            if pending_updates:
                self._bulk_update_holding_info(pending_updates)

        return {
            'total': len(funds),
            'updated': direct_updated_count + propagated_count,
            'direct_updated': direct_updated_count,
            'share_family_propagated': propagated_count,
            'skipped': skipped_count,
            'failed': failed_count,
        }

    def _print_run_summary(self, summary: Dict[str, int]) -> None:
        total = int(summary.get('total') or 0)
        updated = int(summary.get('updated') or 0)
        direct_updated = int(summary.get('direct_updated') or 0)
        propagated = int(summary.get('share_family_propagated') or 0)
        skipped = int(summary.get('skipped') or 0)
        failed = int(summary.get('failed') or 0)
        updated_rate = (updated / total * 100) if total > 0 else 0.0
        failed_rate = (failed / total * 100) if total > 0 else 0.0

        print(f"\n{'='*50}")
        print("HoldingPeriodCollector 统计")
        print(f"{'='*50}")
        print(f"总数:         {total:,}")
        print(f"更新:         {updated:,}")
        print(f"直接更新:     {direct_updated:,}")
        print(f"份额族传播:   {propagated:,}")
        print(f"跳过:         {skipped:,}")
        print(f"失败:         {failed:,}")
        print(f"更新命中率:   {updated_rate:.2f}%")
        print(f"失败率:       {failed_rate:.2f}%")
        print(f"{'='*50}\n")

    @staticmethod
    def _family_key(fund: Dict) -> str:
        """P1-01.4: 按 (company_id + normalize_base_fund_name) 构造族 key。

        V4 保护：若 ``company_id`` 为空或缺失，返回空字符串以标志"该基金不参与
        份额族传播"。否则不同公司同名基金会被错误聚合（历史导入缺 company_id 的
        边缘数据集）。
        """
        from bond_fund_screening.share_class_utils import normalize_base_fund_name  # type: ignore

        company_id = str(fund.get("company_id") or "").strip()
        if not company_id:
            return ""
        base = normalize_base_fund_name(fund.get("shortname") or "").strip()
        if not base:
            return ""
        return f"{company_id}|{base}"

    def _apply_share_family_propagation(
        self,
        scanned_funds: List[Dict],
        pending_updates: List[Dict],
    ) -> List[Dict]:
        """
        P1-01.4 / B13: 在同一母基金 + 品牌系列下（company_id + base_fund_name），
        若 A 类已识别出持有期但 C/E/I 等份额未识别，按**长持有期优先**的保守规则
        传播。传播生成的 update 标 ``holding_period_source='share_family'``。

        参数：
        - scanned_funds: _get_funds_to_update 返回的**全部**基金快照，用于族内分组。
        - pending_updates: 已通过 L1/L2/L3 识别产出的 update 载荷。

        返回：属于 share_family 层的新 update 列表；同一 fcode 不重复。
        """
        if not scanned_funds:
            return []

        pending_fcodes = {str(item.get("fcode") or "").strip() for item in pending_updates}

        # 对 pending_updates 建 fcode → holding_days 索引
        pending_days_by_fcode: Dict[str, int] = {}
        for item in pending_updates:
            fc = str(item.get("fcode") or "").strip()
            days = item.get("holding_days")
            if fc and isinstance(days, int) and days > 0:
                pending_days_by_fcode[fc] = days

        # 已识别份额（本轮 + 历史 has_holding_period=1）按族分组
        confirmed_days_by_family: Dict[str, int] = {}
        for fund in scanned_funds:
            fc = str(fund.get("fcode") or "").strip()
            if not fc:
                continue
            key = self._family_key(fund)
            if not key:
                continue
            # 本轮刚识别的 holding_days 优先
            new_days = pending_days_by_fcode.get(fc)
            if new_days is None:
                current_has = int(fund.get("has_holding_period") or 0)
                current_days = fund.get("holding_days")
                if current_has == 1 and isinstance(current_days, int) and current_days > 0:
                    new_days = current_days
            if new_days is None or new_days <= 0:
                continue
            existing = confirmed_days_by_family.get(key, 0)
            if new_days > existing:
                confirmed_days_by_family[key] = new_days

        if not confirmed_days_by_family:
            return []

        # 对 pending_updates 外的、仍未识别的族成员做传播
        propagated: List[Dict] = []
        for fund in scanned_funds:
            fc = str(fund.get("fcode") or "").strip()
            if not fc or fc in pending_fcodes:
                continue
            current_has = int(fund.get("has_holding_period") or 0)
            current_days = fund.get("holding_days")
            if current_has == 1 and isinstance(current_days, int) and current_days > 0:
                continue  # 已经有确认值
            key = self._family_key(fund)
            days = confirmed_days_by_family.get(key)
            if not days or days <= 0:
                continue
            propagated.append(
                {
                    "fcode": fc,
                    "has_holding_period": 1,
                    "holding_days": int(days),
                    "holding_keywords": f"share_family_propagated:{days}d",
                    "holding_period_source": "share_family",
                }
            )
        return propagated

    def _build_holding_update(self, fund: Dict) -> Dict | None:
        """
        构建基金持有期更新载荷（仅解析，不执行数据库写入）。

        P1-01 / B13: 识别优先级改为 4 级：
            L1 detail_struct: CYCLE / PRSVPERIOD / BUYTIME 原生字段
            L2 name: fund 名称关键词（原有逻辑）
            L3 strategy_text: investment_target + investment_strategy 关键词
            L4 share_family: 份额族传播（在 _apply_share_family_propagation 阶段做）

        Args:
            fund: 基金信息

        Returns:
            可用于 bulk_update_mappings 的字典
        """
        parsed, source_layer = parse_holding_with_priority(
            shortname=fund.get('shortname'),
            fullname=fund.get('fullname'),
            investment_target=fund.get('investment_target'),
            investment_strategy=fund.get('investment_strategy'),
            cycle=fund.get('cycle'),
            preserve_period=fund.get('preserve_period'),
            buy_time=fund.get('buy_time'),
        )
        holding_update = build_holding_update_fields(
            current_has_holding_period=fund.get('has_holding_period'),
            current_days=fund.get('holding_days'),
            current_keyword=fund.get('holding_keywords'),
            parsed=parsed,
        )
        current_source = str(fund.get("holding_period_source") or "").strip()
        source_update_only = False
        if not holding_update:
            if parsed is None:
                # Only a NULL row with no positive API/previously verified
                # signal can be recorded as a completed no-signal scan. Never
                # relabel an existing holding-period fact as "normal" merely
                # because the current name parser did not reproduce it.
                current_days = self._normalize_optional_int(fund.get("holding_days"))
                current_has = int(fund.get("has_holding_period") or 0)
                if (
                    current_source == NO_SIGNAL_REVIEWED_SOURCE
                    or current_days is not None
                    or current_has == 1
                ):
                    return None
                return {
                    "fcode": fund.get("fcode"),
                    "holding_period_source": NO_SIGNAL_REVIEWED_SOURCE,
                }
            current_has = int(fund.get("has_holding_period") or 0)
            current_days = fund.get("holding_days")
            normalized_current_days = int(current_days) if isinstance(current_days, int) else None
            if (
                source_layer != "none"
                and current_source != source_layer
                and current_has == 1
                and normalized_current_days == int(parsed.days)
            ):
                holding_update = {}
                source_update_only = True
            else:
                return None

        # P1-01: 若解析到持有期，记录来源层；解析为"无持有期"（parsed=None）时清空来源。
        if parsed is not None and source_layer != "none":
            holding_update["holding_period_source"] = source_layer
            # detail_struct 应该覆盖旧的 legacy name parse；若当前天数不同，显式提升为结构化结果。
            if source_layer == "detail_struct":
                current_days = fund.get("holding_days")
                normalized_current_days = int(current_days) if isinstance(current_days, int) else None
                if normalized_current_days != int(parsed.days):
                    holding_update["has_holding_period"] = 1
                    holding_update["holding_days"] = int(parsed.days)
                    holding_update["holding_keywords"] = parsed.keyword
                    source_update_only = False
        else:
            holding_update["holding_period_source"] = None

        if source_update_only and set(holding_update.keys()) == {"holding_period_source"} and current_source == source_layer:
            return None
        return {
            'fcode': fund.get('fcode'),
            **holding_update,
        }

    def _bulk_update_holding_info(self, updates: List[Dict], batch_size: int = 2000):
        """
        批量写入持有期信息，降低事务数量和写锁竞争。

        Args:
            updates: 更新载荷列表
            batch_size: 单批写入数量
        """
        if not updates:
            return

        if batch_size <= 0:
            batch_size = len(updates)

        total = len(updates)
        now = datetime.now()
        logger.info(f"开始批量写入持有期信息: {total}只基金, 批大小={batch_size}")

        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            batch = updates[start:end]
            payload = []
            for item in batch:
                item_payload = dict(item)
                item_payload['last_updated_at'] = now
                payload.append(item_payload)

            with self.db_manager.get_session(task_name='supplement_holding_period_bulk') as session:
                session.bulk_update_mappings(Fund, payload)

        logger.info("持有期批量写入完成")


def main():
    """主函数 - 用于独立运行"""
    from pathlib import Path
    from utils.project_runtime_config import load_runtime_config

    # 加载配置
    config_path = Path(__file__).resolve().parents[2] / 'config' / 'project.yaml'
    config = load_runtime_config(config_path)

    # 创建采集器
    collector = HoldingPeriodCollector(config=config)
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
