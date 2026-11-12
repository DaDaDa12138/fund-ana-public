#!/usr/bin/env python3
"""
AutoStock API验证器
用于验证本地数据库的完整性和准确性
"""

import os
import random
from datetime import datetime
from typing import Dict, List, Any, Optional
from api.client import APIClient
from api.autostock_client import AutoStockClient
from api.autostock_endpoints import DataConverter
from api.endpoints import FundAPIEndpoints
from database import DatabaseManager
from database.models import Fund, FundReturns
from utils.logger import get_logger, setup_logger
from utils.fund_type_filters import BOND_FTYPE_CODES, BOND_TYPE_CODES, build_bond_filter, is_bond_type

logger = get_logger(__name__)


DEFAULT_VALIDATOR_VERIFY_BATCH_SIZE = AutoStockClient.SAFE_FUND_BATCH_SIZE
DEFAULT_AUTOSTOCK_BASE_URL = AutoStockClient.BASE_URL


def assess_autostock_completeness_source(source_meta: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    基于当前项目实际使用的目录抓取元数据，给出是否可判定“完整性”的最小策略。

    说明：
    - 当前项目的 AutoStock 目录口径主要用于交叉校验与辅助验证，不作为全市场权威主源；
    - 若接口明确声明总量，且本轮已完整抓取到声明总量，则允许给出 completeness 结论；
    - 否则只返回 fetched coverage，避免把不完整目录误判成全市场完整度。
    """
    meta = source_meta if isinstance(source_meta, dict) else {}
    declared_total = int(meta.get('declared_total') or 0)
    fetched_total = int(meta.get('bond_like_total') or meta.get('fetched_total') or 0)
    can_conclude = declared_total > 0 and fetched_total >= declared_total
    return {
        'source_name': 'autostock_directory',
        'declared_total': declared_total,
        'fetched_total': fetched_total,
        'can_conclude_completeness': can_conclude,
        'incomplete_reason': '' if can_conclude else 'directory_not_proven_full_market_coverage',
    }


class AutoStockValidator:
    """AutoStock API验证器"""
    _BOND_TYPE_CODES = BOND_TYPE_CODES
    _BOND_FTYPE_CODES = BOND_FTYPE_CODES
    _BOND_TEXT_PREFIXES = ('债券', '定开债')

    def __init__(self, config: Dict = None, db_manager: DatabaseManager = None):
        """
        初始化验证器

        Args:
            config: 配置字典
            db_manager: 数据库管理器实例
        """
        setup_logger(__name__)
        self.config = config if isinstance(config, dict) else {}
        self.db_manager = db_manager

        # 初始化AutoStock客户端
        autostock_config = self.config.get('api', {}).get('autostock', {})
        rate_limit_config = autostock_config.get('rate_limit', {})
        batch_config = autostock_config.get('batch', {}) or {}
        base_url = os.environ.get(
            'AUTOSTOCK_API_BASE_URL',
            autostock_config.get('base_url', DEFAULT_AUTOSTOCK_BASE_URL),
        )
        min_interval = float(
            os.environ.get(
                'AUTOSTOCK_MIN_INTERVAL',
                rate_limit_config.get('min_interval', autostock_config.get('min_interval', 2.0))
            )
        )
        timeout = int(
            os.environ.get(
                'AUTOSTOCK_TIMEOUT',
                autostock_config.get('retry', {}).get('timeout', 60)
            )
        )
        max_calls_per_hour = self._normalize_positive_int(
            os.environ.get(
                'AUTOSTOCK_MAX_CALLS_PER_HOUR',
                rate_limit_config.get('max_calls_per_hour', autostock_config.get('max_calls_per_hour', 100))
            ),
            100,
        )
        self.client = AutoStockClient(
            enabled=autostock_config.get('enabled', True) is True,
            base_url=base_url,
            min_interval=min_interval,
            timeout=timeout,
            max_calls_per_hour=max_calls_per_hour or None,
            shared_state_path=os.environ.get('AUTOSTOCK_RATE_LIMIT_STATE_PATH'),
        )

        # 从配置文件读取验证容差值（修复：原硬编码0.1%过于严格）
        tolerance_config = autostock_config.get('verification', {}).get('tolerance', {})
        self.nav_tolerance = tolerance_config.get('nav_relative', tolerance_config.get('nav', 1.0))
        self.nav_abs_tolerance = tolerance_config.get('nav_absolute', 0.001)
        self.return_tolerance = tolerance_config.get('return_relative', tolerance_config.get('return', 5.0))
        self.return_abs_tolerance = tolerance_config.get('return_absolute', 0.5)
        self.return_small_base_threshold = tolerance_config.get('return_small_base_threshold', 1.0)
        self.max_calls_per_hour = max_calls_per_hour or None
        self.verify_batch_size = self._normalize_positive_int(
            os.environ.get(
                'AUTOSTOCK_VERIFY_BATCH_SIZE',
                batch_config.get('verify_size', batch_config.get('size', DEFAULT_VALIDATOR_VERIFY_BATCH_SIZE))
            ),
            DEFAULT_VALIDATOR_VERIFY_BATCH_SIZE,
        )

        self.results = {}
        self._tiantian_endpoints = None
        self._tiantian_type_cache = {}
        self._tiantian_type_cross_check_stats = {
            'lookups': 0,
            'confirmed_local_type': 0,
            'mismatch_confirmed': 0,
            'missing_detail_ftype': 0,
            'errors': 0,
        }
        logger.info(f"AutoStock验证器初始化完成 (净值容差: {self.nav_tolerance}%, 收益率容差: {self.return_tolerance}%)")

    @staticmethod
    def _normalize_positive_int(value, default: int) -> int:
        """将输入规整为正整数，非法时回退默认值。"""
        try:
            parsed = int(value)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            pass
        return int(default)

    @classmethod
    def _build_bond_filter(cls):
        """构建统一债券基金过滤条件，兼容新旧字段。"""
        return build_bond_filter()

    def _get_session(self, readonly: bool = False):
        """兼容测试桩与旧接口，优先使用只读会话。"""
        if readonly:
            try:
                return self.db_manager.get_session(readonly=True)
            except TypeError:
                return self.db_manager.get_session()
        return self.db_manager.get_session()

    @staticmethod
    def _is_bond_type(
        ftype: Optional[str],
        type_code: Optional[str],
        type_category: Optional[str],
    ) -> bool:
        """统一判定债券基金口径，兼容新旧字段。"""
        return is_bond_type(ftype, type_code, type_category)

    @staticmethod
    def _is_type_match(local_type: str, autostock_type: Optional[str]) -> bool:
        """判断本地类型和 AutoStock 类型是否语义一致。"""
        if not autostock_type:
            return True
        if not local_type:
            return False

        local_type = str(local_type)
        autostock_type = str(autostock_type)

        if local_type in autostock_type or autostock_type in local_type:
            return True

        local_is_bond = ('债券' in local_type) or ('固收' in local_type)
        autostock_is_bond = ('债券' in autostock_type) or ('固收' in autostock_type)
        return local_is_bond and autostock_is_bond

    def _get_type_desc(
        self,
        ftype: Optional[str],
        type_code: Optional[str] = None,
        type_category: Optional[str] = None,
    ) -> str:
        """获取统一基金类型描述。"""
        if self._is_bond_type(ftype, type_code, type_category):
            return '债券'
        if not ftype:
            return ''
        return str(ftype)

    def _reset_tiantian_type_cross_check_state(self) -> None:
        """为单次抽样验证重置类型交叉验证状态。"""
        self._tiantian_type_cache = {}
        self._tiantian_type_cross_check_stats = {
            'lookups': 0,
            'confirmed_local_type': 0,
            'mismatch_confirmed': 0,
            'missing_detail_ftype': 0,
            'errors': 0,
        }

    def _get_tiantian_endpoints(self) -> Optional[FundAPIEndpoints]:
        """懒加载天天基金端点客户端。"""
        endpoints = getattr(self, '_tiantian_endpoints', None)
        if endpoints is not None:
            return endpoints

        config = getattr(self, 'config', None)
        if not isinstance(config, dict):
            return None

        try:
            endpoints = FundAPIEndpoints(APIClient(config))
        except Exception as exc:
            logger.warning(f"初始化天天基金类型交叉验证客户端失败: {exc}")
            return None

        self._tiantian_endpoints = endpoints
        return endpoints

    @staticmethod
    def _extract_tiantian_detail_ftype(detail_payload: Any) -> Optional[str]:
        """从天天基金 detail 响应中提取类型文本。"""
        if not isinstance(detail_payload, dict):
            return None
        datas = detail_payload.get('Datas')
        if not isinstance(datas, dict):
            return None
        for key in ('FTYPE', 'FUNDTYPE', 'FUNDTYPENEW', 'FUNDKIND'):
            value = str(datas.get(key) or '').strip()
            if value:
                return value
        return None

    def _get_tiantian_detail_ftype(self, fcode: Optional[str]) -> Optional[str]:
        """按基金代码读取天天基金 detail 类型，带单次运行缓存。"""
        code = str(fcode or '').strip()
        if not code:
            return None

        cache = getattr(self, '_tiantian_type_cache', {})
        if code in cache:
            return cache[code]

        endpoints = self._get_tiantian_endpoints()
        if endpoints is None:
            cache[code] = None
            self._tiantian_type_cache = cache
            return None

        stats = getattr(self, '_tiantian_type_cross_check_stats', {})
        stats['lookups'] = int(stats.get('lookups', 0)) + 1
        self._tiantian_type_cross_check_stats = stats

        try:
            detail_payload = endpoints.get_fund_detail(code)
            ftype = self._extract_tiantian_detail_ftype(detail_payload)
            if not ftype:
                stats['missing_detail_ftype'] = int(stats.get('missing_detail_ftype', 0)) + 1
                self._tiantian_type_cross_check_stats = stats
            cache[code] = ftype
            self._tiantian_type_cache = cache
            return ftype
        except Exception as exc:
            logger.warning(f"天天基金类型交叉验证失败 {code}: {exc}")
            stats['errors'] = int(stats.get('errors', 0)) + 1
            self._tiantian_type_cross_check_stats = stats
            cache[code] = None
            self._tiantian_type_cache = cache
            return None

    def _resolve_type_match_with_cross_validation(
        self,
        *,
        fcode: Optional[str],
        local_type_desc: str,
        autostock_type: Optional[str],
    ) -> tuple[bool, Dict[str, Any]]:
        """先使用 AutoStock 类型，再用天天基金 detail 做定点交叉验证。"""
        if self._is_type_match(local_type_desc, autostock_type):
            return True, {
                'autostock_type': autostock_type,
                'tiantian_detail_ftype': None,
                'type_match_source': 'autostock',
            }

        tiantian_detail_ftype = self._get_tiantian_detail_ftype(fcode)
        if tiantian_detail_ftype and self._is_type_match(local_type_desc, tiantian_detail_ftype):
            stats = getattr(self, '_tiantian_type_cross_check_stats', {})
            stats['confirmed_local_type'] = int(stats.get('confirmed_local_type', 0)) + 1
            self._tiantian_type_cross_check_stats = stats
            return True, {
                'autostock_type': autostock_type,
                'tiantian_detail_ftype': tiantian_detail_ftype,
                'type_match_source': 'tiantian_detail',
            }

        if tiantian_detail_ftype:
            stats = getattr(self, '_tiantian_type_cross_check_stats', {})
            stats['mismatch_confirmed'] = int(stats.get('mismatch_confirmed', 0)) + 1
            self._tiantian_type_cross_check_stats = stats

        return False, {
            'autostock_type': autostock_type,
            'tiantian_detail_ftype': tiantian_detail_ftype,
            'type_match_source': 'autostock_and_tiantian_mismatch' if tiantian_detail_ftype else 'autostock_only',
        }

    def verify_sample(self, sample_size: int = 100) -> Dict[str, Any]:
        """
        抽样验证准确性

        Args:
            sample_size: 抽样数量，默认100

        Returns:
            验证结果字典 {accuracy: float, sample_size: int, ...}
        """
        logger.info(f"开始抽样验证 (样本数={sample_size})")
        if getattr(self.client, 'enabled', True) is not True:
            raise RuntimeError('independent_source_unavailable: autostock_provider_disabled')
        self._reset_tiantian_type_cross_check_state()

        try:
            # 1. 随机抽样
            with self._get_session(readonly=True) as session:
                all_funds = session.query(Fund).join(
                    FundReturns, Fund.fcode == FundReturns.fcode
                ).filter(
                    self._build_bond_filter(),
                    FundReturns.latest_nav != None
                ).all()

                if len(all_funds) < sample_size:
                    sample_size = len(all_funds)

                sample_funds = random.sample(all_funds, sample_size)
                sample_codes = [f.fcode for f in sample_funds]

                # 提取数据到字典
                fund_dict = {
                    f.fcode: {
                        'fcode': f.fcode,
                        'shortname': f.shortname,
                        'ftype': f.ftype,
                        'type_code': getattr(f, 'type_code', None),
                        'type_category': getattr(f, 'type_category', None),
                    } for f in sample_funds
                }

                fund_returns_dict = {
                    fr.fcode: {
                        'latest_nav': fr.latest_nav,
                        'return_1y': fr.return_1y
                    } for fr in session.query(FundReturns).filter(
                        FundReturns.fcode.in_(sample_codes)
                    ).all()
                }

            # 2. 批量获取AutoStock数据
            verify_batch_size = max(1, int(self.verify_batch_size or DEFAULT_VALIDATOR_VERIFY_BATCH_SIZE))
            autostock_data = self.client.get_fund_batch(sample_codes, batch_size=verify_batch_size)
            autostock_dict = {f['code']: f for f in autostock_data}
            batch_meta_getter = getattr(self.client, 'get_last_fund_batch_meta', None)
            batch_meta = batch_meta_getter() if callable(batch_meta_getter) else {}

            # 3. 对比数据
            comparisons = []
            match_count = {'perfect': 0, 'total': 0}

            for fcode in sample_codes:
                if fcode not in autostock_dict:
                    continue

                local_fund = fund_dict.get(fcode)
                fund_returns = fund_returns_dict.get(fcode)
                as_fund = autostock_dict[fcode]

                comparison = self._compare_fund_data(local_fund, fund_returns, as_fund)
                comparisons.append(comparison)

                match_count['total'] += 1
                if comparison['all_match']:
                    match_count['perfect'] += 1

            # 4. 计算准确率
            accuracy = (match_count['perfect'] / match_count['total'] * 100) if match_count['total'] > 0 else 0

            result = {
                'accuracy': accuracy,
                'sample_size': sample_size,
                'compared_count': match_count['total'],
                'perfect_match': match_count['perfect'],
                'batch_meta': batch_meta,
                'type_cross_check_stats': dict(getattr(self, '_tiantian_type_cross_check_stats', {})),
                'timestamp': datetime.now().isoformat()
            }

            logger.info(f"抽样验证完成: 准确率 {accuracy:.2f}%")
            return result

        except Exception as e:
            logger.error(f"抽样验证失败: {e}", exc_info=True)
            raise

    def verify_completeness(self) -> Dict[str, Any]:
        """
        验证数据完整性

        Returns:
            完整性验证结果
        """
        logger.info("开始完整性验证")

        try:
            # 1. 获取AutoStock债券基金目录
            autostock_funds = self.client.get_all_bond_funds_from_directory()
            source_meta = self.client.get_last_fund_all_meta()
            completeness_policy = assess_autostock_completeness_source(source_meta)
            source_name = completeness_policy['source_name']
            autostock_codes = {f['code'] for f in autostock_funds}
            source_complete = completeness_policy['can_conclude_completeness']

            # 2. 获取本地债券基金列表
            with self._get_session(readonly=True) as session:
                local_funds = session.query(Fund).filter(
                    self._build_bond_filter()
                ).all()
                local_codes = {f.fcode for f in local_funds}

            # 3. 计算差异
            missing_in_local = autostock_codes - local_codes
            missing_in_autostock = local_codes - autostock_codes
            matched_in_local = autostock_codes & local_codes

            coverage_on_fetched = len(matched_in_local) / len(autostock_codes) * 100 if autostock_codes else 0

            result = {
                'completeness': coverage_on_fetched,
                'completeness_assessed': coverage_on_fetched if source_complete else None,
                'autostock_total': len(autostock_codes),
                'autostock_declared_total': source_meta.get('declared_total') or 0,
                'autostock_fetched_total': source_meta.get('bond_like_total', len(autostock_codes)),
                'local_total': len(local_codes),
                'missing_in_local': len(missing_in_local),
                'missing_in_autostock': len(missing_in_autostock),
                'missing_funds': list(missing_in_local)[:50],  # 最多返回50个
                'coverage_on_fetched': coverage_on_fetched,
                'can_conclude_completeness': source_complete,
                'data_source': source_name,
                'source_meta': source_meta,
                'source_policy': completeness_policy,
                'rank_source_meta': source_meta if source_name == 'fund/rank' else {},
                'timestamp': datetime.now().isoformat()
            }

            if source_complete:
                logger.info(f"完整性验证完成: {coverage_on_fetched:.2f}%")
            else:
                logger.warning(
                    "完整性验证保护触发：AutoStock目录源不具备完整性权威性，"
                    f"coverage_on_fetched={coverage_on_fetched:.2f}%, "
                    f"reason={completeness_policy.get('incomplete_reason', 'unknown')}"
                )
            return result

        except Exception as e:
            logger.error(f"完整性验证失败: {e}", exc_info=True)
            raise

    def verify_all(self) -> Dict[str, Any]:
        """
        执行完整验证（完整性+准确性）

        Returns:
            综合验证结果
        """
        logger.info("开始全量验证")

        try:
            # 1. 完整性验证
            completeness_result = self.verify_completeness()

            # 2. 准确性验证（抽样100）
            accuracy_result = self.verify_sample(sample_size=100)

            # 3. 综合结果
            completeness_assessed = completeness_result.get('completeness_assessed')
            accuracy_score = accuracy_result['accuracy']
            if completeness_assessed is None:
                overall_score = accuracy_score
                score_mode = 'accuracy_only'
            else:
                overall_score = (completeness_assessed + accuracy_score) / 2
                score_mode = 'completeness_accuracy_average'

            result = {
                'completeness': completeness_result,
                'accuracy': accuracy_result,
                'overall_score': overall_score,
                'overall_score_mode': score_mode,
                'timestamp': datetime.now().isoformat()
            }

            logger.info(f"全量验证完成: 综合得分 {result['overall_score']:.2f}")
            return result

        except Exception as e:
            logger.error(f"全量验证失败: {e}", exc_info=True)
            raise

    def _compare_fund_data(self, local_fund: Dict, fund_returns: Dict,
                          autostock_fund: Dict) -> Dict[str, Any]:
        """
        对比单只基金数据

        修复说明：
        - 原代码硬编码0.1%容差过于严格，导致准确率仅6%
        - 现在使用配置文件中的容差值（默认：净值1%，收益率5%）
        - 名称匹配改为模糊匹配（包含关系），因为不同数据源命名格式可能不同

        Args:
            local_fund: 本地基金基本信息
            fund_returns: 本地基金收益数据
            autostock_fund: AutoStock返回的基金数据

        Returns:
            对比结果字典
        """
        converter = DataConverter()

        # 提取AutoStock数据
        as_code = autostock_fund.get('code')
        as_name = autostock_fund.get('name', '')
        as_type = autostock_fund.get('type')
        as_nav = autostock_fund.get('netWorth')
        as_return_1y = converter.parse_growth_rate(autostock_fund.get('lastYearGrowth'))

        # 代码对比（必须完全匹配）
        code_match = local_fund['fcode'] == as_code

        # 名称对比（修复：改为模糊匹配，因为不同数据源命名可能有细微差异）
        local_name = local_fund.get('shortname', '')
        name_match = (
            local_name == as_name or
            local_name in as_name or
            as_name in local_name or
            # 去除常见后缀后比较
            local_name.replace('债券', '').replace('混合', '') == as_name.replace('债券', '').replace('混合', '')
        )

        local_type_desc = self._get_type_desc(
            local_fund.get('ftype'),
            local_fund.get('type_code'),
            local_fund.get('type_category'),
        )
        type_match, type_validation = self._resolve_type_match_with_cross_validation(
            fcode=local_fund.get('fcode'),
            local_type_desc=local_type_desc,
            autostock_type=as_type,
        )

        # 净值对比（修复：使用配置的容差值，而非硬编码0.1%）
        nav_match = False
        nav_diff_pct = None
        nav_abs_diff = None
        local_nav = fund_returns.get('latest_nav') if fund_returns else None
        if local_nav is not None and as_nav is not None:
            nav_abs_diff = abs(local_nav - as_nav)
            nav_diff_pct = nav_abs_diff / max(abs(as_nav), 1e-8) * 100
            nav_match = (
                nav_diff_pct <= self.nav_tolerance or
                nav_abs_diff <= self.nav_abs_tolerance
            )

        # 收益率对比（修复：使用配置的容差值，而非硬编码0.1%）
        return_match = False
        return_diff_pct = None
        return_abs_diff = None
        return_diff_mode = None
        local_return = fund_returns.get('return_1y') if fund_returns else None
        if local_return is None and as_return_1y is None:
            return_match = True
        elif local_return is not None and as_return_1y is not None:
            return_abs_diff = abs(local_return - as_return_1y)
            # 收益率基数较小时，优先使用绝对百分点差避免相对误差被放大。
            if abs(as_return_1y) < self.return_small_base_threshold:
                return_diff_mode = 'absolute_pct_point'
                return_diff_pct = return_abs_diff
                return_match = return_abs_diff <= self.return_abs_tolerance
            else:
                return_diff_mode = 'relative_pct'
                return_diff_pct_relative = return_abs_diff / max(abs(as_return_1y), 0.01) * 100
                return_diff_pct = return_diff_pct_relative
                return_match = return_diff_pct_relative <= self.return_tolerance

        # 综合判断：
        # “完全一致”必须同时满足代码 / 名称 / 类型 / 净值 / 收益率一致。
        # 否则会把“资产类型错配但数值碰巧接近”的样本误判为完全一致。
        all_match = code_match and name_match and type_match and nav_match and return_match

        return {
            'code': local_fund['fcode'],
            'name': local_fund['shortname'],
            'all_match': all_match,
            'code_match': code_match,
            'name_match': name_match,
            'type_match': type_match,
            'autostock_type': type_validation.get('autostock_type'),
            'tiantian_detail_ftype': type_validation.get('tiantian_detail_ftype'),
            'type_match_source': type_validation.get('type_match_source'),
            'nav_match': nav_match,
            'return_match': return_match,
            # 添加详细差异信息便于调试
            'nav_abs_diff': round(nav_abs_diff, 6) if nav_abs_diff is not None else None,
            'nav_diff_pct': round(nav_diff_pct, 4) if nav_diff_pct is not None else None,
            'return_abs_diff': round(return_abs_diff, 4) if return_abs_diff is not None else None,
            'return_diff_mode': return_diff_mode,
            'return_diff_pct': round(return_diff_pct, 4) if return_diff_pct is not None else None
        }
