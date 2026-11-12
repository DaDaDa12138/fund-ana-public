"""
通用基金列表采集器 - FundCollector

支持所有基金类型采集:
- 股票型 (25)
- 指数型 (26)
- 混合型 (27)
- 债券型 (31, 32)
- 货币型 (35)
- QDII (36)
- FOF (37)
- ETF (3)
- LOF (4)
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from collectors.base_collector import BaseCollector
from database.models import Fund, FundNavHistory, FundReturns
from config.api_config import FundType, SortColumn
from utils import ProgressMonitor, extract_bond_subtype, safe_float, safe_int, parse_date, json_dumps
from utils.fund_exclusion_rules import is_runtime_excluded_fund
from utils.buyability import normalize_buyability
from utils.fund_list_pagination_audit import (
    finalize_rank_pagination_evidence,
    observed_total,
    record_rank_page_observation,
)
from utils.fund_type_filters import (
    has_specific_structured_type_state,
    infer_high_confidence_negotiable_cd_type_fields,
    is_low_confidence_product_form_state,
    is_low_confidence_generic_numeric_state,
    normalize_fund_type_fields,
)
from utils.tiantian_data_quality import (
    inspect_tiantian_period_increase_payload,
    inspect_tiantian_rank_payload,
)
from utils.period_return_snapshot import (
    RETURN_FIELDS, build_period_snapshot, period_number,
    select_return_update,
)

# 导入基金类型注册表（可选）
try:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    from config.fund_type_registry import (
        FundTypeCode,
        cli_type_to_fund_types,
        resolve_alias,
    )
    HAS_REGISTRY = True
except ImportError:
    HAS_REGISTRY = False
    FundTypeCode = None
    cli_type_to_fund_types = None
    resolve_alias = None

logger = logging.getLogger(__name__)
PLACEHOLDER_TEXT_VALUES = {'--', '---', '暂无', 'N/A', 'n/a', 'null', 'NULL'}
MAX_RANK_RETURN_AGE_DAYS = 30
FUND_LIST_PAGE_RETRY_MAX_ATTEMPTS = 3
FUND_LIST_PAGE_RETRYABLE_STATUSES = {
    'request_failed',
    'upstream_business_degraded',
}
FUND_LIST_PAGE_RETRYABLE_FAILURE_KINDS = {
    'timeout',
    'connection_error',
    'invalid_json',
    'http_error',
}
PERIOD_INCREASE_TITLE_TO_RETURN_FIELD = {
    'Z': 'return_1w',
    'Y': 'return_1m',
    '3Y': 'return_3m',
    '6Y': 'return_6m',
    '1N': 'return_1y',
    '2N': 'return_2y',
    '3N': 'return_3y',
    '5N': 'return_5y',
    'JN': 'return_ytd',
    'LN': 'return_since',
}
RANK_REQUIRED_IDENTITY_FIELDS = ('FCODE', 'SHORTNAME')
RANK_SNAPSHOT_FIELDS = ('FSRQ', 'DWJZ')
RANK_SNAPSHOT_FIELD_META = {
    'FSRQ': {
        'business_field': 'nav_date',
        'count_key': 'missing_nav_date_count',
    },
    'DWJZ': {
        'business_field': 'nav_value',
        'count_key': 'missing_nav_value_count',
    },
}
MAX_RANK_SNAPSHOT_MISSING_SAMPLES = 10


# FundType API代码到FundTypeCode的映射
FUND_TYPE_API_MAP = {
    25: FundType.STOCK,
    26: FundType.INDEX,
    27: FundType.HYBRID,
    31: FundType.BOND,
    32: FundType.BOND_FIXED,
    35: FundType.MONEY,
    36: FundType.QDII,
    37: FundType.FOF,
    3: FundType.ETF,
    4: FundType.LOF,
}


class FundCollector(BaseCollector):
    """
    通用基金列表采集器

    支持所有基金类型的采集，是 BondFundCollector 的通用化版本。
    """

    def collect_all(
        self,
        fund_types: Optional[List[Union[int, str, 'FundTypeCode']]] = None,
        only_buyable: bool = False
    ) -> int:
        """
        采集基金列表

        Args:
            fund_types: 基金类型列表，支持以下格式:
                - int: API代码 (如 25, 31)
                - str: 别名 (如 'stock', 'bond')
                - FundTypeCode: 枚举值
                - None: 默认采集债券型 [31, 32]
            only_buyable: 仅可购买的基金（默认False，采集全量基金）

        Returns:
            采集的基金数量
        """
        # 解析基金类型
        resolved_types = self._resolve_fund_types(fund_types)

        if not resolved_types:
            logger.warning("没有有效的基金类型，使用默认债券型")
            resolved_types = [FundType.BOND, FundType.BOND_FIXED]

        # 获取类型名称用于日志
        type_names = [str(t) for t in resolved_types]
        logger.info(f"开始采集基金列表, 类型={type_names}, 仅可购={only_buyable}")

        all_funds = []
        api_source_evidence = []

        for fund_type in resolved_types:
            self._last_collect_by_type_evidence = None
            funds = self._collect_by_type(fund_type, only_buyable)
            all_funds.extend(funds)
            type_evidence = getattr(self, "_last_collect_by_type_evidence", None)
            if isinstance(type_evidence, dict):
                api_source_evidence.append(type_evidence)

        unique_funds, duplicate_count = self._deduplicate_fund_records(all_funds)
        if duplicate_count:
            logger.info("基金列表采集阶段合并重复基金记录: %s 条", duplicate_count)

        logger.info(f"去重后共{len(unique_funds)}只基金")
        source_payload = {
            "schema_version": 3,
            "task": "collect-funds",
            "source": "fundMNRank",
            "fund_types": type_names,
            "only_buyable": bool(only_buyable),
            "type_count": len(api_source_evidence),
            "total_page_request_count": sum(
                safe_int(item.get("page_request_count")) for item in api_source_evidence
            ),
            "total_accepted_page_count": sum(
                safe_int(item.get("accepted_page_count")) for item in api_source_evidence
            ),
            "total_empty_page_count": sum(
                safe_int(item.get("empty_page_count")) for item in api_source_evidence
            ),
            "total_failed_page_count": sum(
                safe_int(item.get("failed_page_count")) for item in api_source_evidence
            ),
            "total_declared_count": sum(
                safe_int(item.get("declared_total_count")) for item in api_source_evidence
            ),
            "total_raw_record_count": sum(
                safe_int(item.get("raw_record_count")) for item in api_source_evidence
            ),
            "total_collected_record_count": sum(
                safe_int(item.get("collected_record_count")) for item in api_source_evidence
            ),
            "total_missing_fcode_count": sum(
                safe_int(item.get("missing_fcode_count")) for item in api_source_evidence
            ),
            "total_missing_shortname_count": sum(
                safe_int(item.get("missing_shortname_count")) for item in api_source_evidence
            ),
            "total_missing_nav_date_count": sum(
                safe_int(item.get("missing_nav_date_count")) for item in api_source_evidence
            ),
            "total_missing_nav_value_count": sum(
                safe_int(item.get("missing_nav_value_count")) for item in api_source_evidence
            ),
            "snapshot_missing_semantics": self._combine_rank_snapshot_missing_semantics(api_source_evidence),
            "total_invalid_record_count": sum(
                safe_int(item.get("invalid_record_count")) for item in api_source_evidence
            ),
            "total_intra_type_duplicate_code_count": sum(
                safe_int(item.get("duplicate_code_count")) for item in api_source_evidence
            ),
            "total_short_page_before_last_count": sum(
                safe_int(item.get("short_page_before_last_count")) for item in api_source_evidence
            ),
            "total_last_page_overflow_count": sum(
                safe_int(item.get("last_page_overflow_count")) for item in api_source_evidence
            ),
            "total_declared_collected_shortfall": sum(
                safe_int(item.get("declared_collected_shortfall")) for item in api_source_evidence
            ),
            "total_declared_collected_overflow": sum(
                safe_int(item.get("declared_collected_overflow")) for item in api_source_evidence
            ),
            "duplicate_record_count": duplicate_count,
            "unique_record_count": len(unique_funds),
            "pagination_warning_type_count": sum(bool(item.get("pagination_warning_codes")) for item in api_source_evidence),
            "pagination_coverage_basis": "per_type_unique_raw_codes_cross_type_overlap_is_not_a_pagination_error",
            "types": api_source_evidence,
        }
        source_payload_text = json_dumps(source_payload)
        logger.info("api_source_evidence=%s", source_payload_text)
        print(f"api_source_evidence={source_payload_text}", flush=True)

        # 保存到数据库
        save_stats = self._save_funds(unique_funds, return_stats=True)
        if save_stats['failed_records']:
            raise RuntimeError(f"基金目录落库未完成: {save_stats}")
        saved_count = save_stats['saved_count']

        self.print_stats()
        return saved_count

    @staticmethod
    def _count_non_empty_values(data: Optional[dict]) -> int:
        """统计字典中非空字段数量，用于重复记录的兜底优选。"""
        if not data:
            return 0
        return sum(1 for value in data.values() if value not in (None, "", []))

    @staticmethod
    def _compact_mapping(data: Optional[dict]) -> dict:
        """普通字段省略空值；带日期的完整收益快照必须保留明确NULL。"""
        if not data:
            return {}
        compacted = {}
        for key, value in data.items():
            if key in RETURN_FIELDS and 'period_return_date' in data:
                compacted[key] = value
                continue
            if value in (None, "", []):
                continue
            if isinstance(value, str):
                normalized = value.strip()
                if normalized == "" or normalized in PLACEHOLDER_TEXT_VALUES:
                    continue
                compacted[key] = normalized
                continue
            compacted[key] = value
        return compacted

    @staticmethod
    def _extract_existing_fcodes(rows) -> set:
        """从 SQLAlchemy 单列查询结果中提取基金代码集合。"""
        existing_codes = set()
        for row in rows or []:
            try:
                fcode = row[0]
            except Exception:
                fcode = getattr(row, 'fcode', None)
            if fcode:
                existing_codes.add(fcode)
        return existing_codes

    @staticmethod
    def _build_existing_returns_state_map(rows) -> Dict[str, Dict[str, Any]]:
        """提取已有收益快照的关键字段，避免旧快照回滚较新的记录。"""
        state_map: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            fcode = getattr(row, 'fcode', None)
            latest_nav_date = getattr(row, 'latest_nav_date', None)

            if fcode is None and isinstance(row, (tuple, list)) and row:
                fcode = row[0]
                latest_nav_date = row[1] if len(row) > 1 else None

            fcode = str(fcode or '').strip()
            if not fcode:
                continue

            state_map[fcode] = {
                'latest_nav_date': latest_nav_date,
                'period_return_date': getattr(row, 'period_return_date', None),
                'period_return_observed_at': getattr(row, 'period_return_observed_at', None),
                'period_return_rejection_json': getattr(row, 'period_return_rejection_json', None),
                'period_return_rejection_sha256': getattr(row, 'period_return_rejection_sha256', None),
            }
        return state_map

    @staticmethod
    def _select_search_result(search_results: Optional[List[dict]], fcode: str) -> Optional[dict]:
        """从 fundSearch 结果中优先选择精确 code 命中的记录。"""
        if not search_results:
            return None

        target_code = str(fcode or '').strip()
        for item in search_results:
            base_info = item.get('FundBaseInfo') or {}
            candidate_codes = [
                base_info.get('FCODE'),
                item.get('FCODE'),
                item.get('CODE'),
                item.get('_id'),
            ]
            if any(str(code).strip() == target_code for code in candidate_codes if code):
                return item

        return search_results[0]

    @staticmethod
    def _extract_search_base_info(search_result: Optional[dict]) -> Optional[dict]:
        """只接受 fundSearch 中可用的基金基础信息，跳过非基金命中。"""
        if not isinstance(search_result, dict):
            return None

        if 'FundBaseInfo' not in search_result:
            return search_result

        base_info = search_result.get('FundBaseInfo')
        if isinstance(base_info, dict) and base_info:
            return base_info
        return None

    @staticmethod
    def _parse_buyable_flag(base_info: Optional[dict]) -> Optional[bool]:
        """统一解析基金可购标记，优先当前 BUY 字段，兼容旧 ISBUY。"""
        if not isinstance(base_info, dict):
            return None

        for field in ('BUY', 'ISBUY'):
            value = normalize_buyability(base_info.get(field))
            if value is not None:
                return value
        return None

    @staticmethod
    def _derive_purchase_status(is_buyable: Optional[bool]) -> Optional[str]:
        """按当前二值数据模型，从可购标记映射申购状态。"""
        if is_buyable is True:
            return '开放申购'
        if is_buyable is False:
            return '暂停申购'
        return None

    @staticmethod
    def _parse_abnormal_rank_flag(item: Optional[dict]) -> bool:
        """解析 fundMNRank 的异常标记。"""
        if not isinstance(item, dict):
            return False

        normalized = str(item.get('ISABNORMAL') or '').strip().lower()
        return normalized in {'1', 'true', 'yes', 'y'}

    @classmethod
    def _build_rank_returns(
        cls,
        item: Dict[str, Any],
        fcode: str,
        collected_at: datetime,
    ) -> Dict[str, Any]:
        """构建排行接口收益快照，并对异常/陈旧数据做最小化保护。"""
        latest_nav_date = parse_date(item.get('FSRQ'))

        if cls._parse_abnormal_rank_flag(item):
            logger.warning(
                "fundMNRank 标记异常，跳过收益快照: %s %s",
                fcode,
                item.get('SHORTNAME') or '',
            )
            return {}

        base_snapshot = cls._compact_mapping({
            'fcode': fcode,
            'latest_nav': safe_float(item.get('DWJZ')),
            'latest_nav_date': latest_nav_date,
            'last_updated_at': collected_at,
        })

        if latest_nav_date is None or latest_nav_date > collected_at.date():
            logger.warning("fundMNRank 缺少有效业务日期，不保存周期收益: %s", fcode)
            return base_snapshot
        if latest_nav_date is not None:
            nav_age_days = (collected_at.date() - latest_nav_date).days
            if nav_age_days > MAX_RANK_RETURN_AGE_DAYS:
                logger.info(
                    "fundMNRank 快照过旧(%s天)，仅保留净值日期: %s %s (%s)",
                    nav_age_days,
                    fcode,
                    item.get('SHORTNAME') or '',
                    latest_nav_date,
                )
                return base_snapshot if len(base_snapshot) > 2 else {}

        returns = {
            'fcode': fcode,
            'latest_nav': safe_float(item.get('DWJZ')),
            'latest_nav_date': latest_nav_date,
            'daily_growth': safe_float(item.get('RZDF')),
            'return_1w': period_number(item.get('SYL_Z')),
            'return_1m': period_number(item.get('SYL_Y')),
            'return_3m': period_number(item.get('SYL_3Y')),
            'return_6m': period_number(item.get('SYL_6Y')),
            'return_1y': period_number(item.get('SYL_1N')),
            'return_2y': period_number(item.get('SYL_2N')),
            'return_3y': period_number(item.get('SYL_3N')),
            'return_5y': period_number(item.get('SYL_5N')),
            'return_ytd': period_number(item.get('SYL_JN')),
            'return_since': period_number(item.get('SYL_LN')),
            'last_updated_at': collected_at,
        }
        if any(returns[field] is not None for field in RETURN_FIELDS):
            returns.update(build_period_snapshot(
                returns, as_of_date=latest_nav_date, observed_at=collected_at,
                source='fundMNRank', source_payload=item,
            ))
        return cls._compact_mapping(returns)

    @staticmethod
    def _should_skip_older_returns_update(
        existing_state: Optional[Dict[str, Any]],
        incoming_returns: Optional[Dict[str, Any]],
    ) -> bool:
        """避免使用更旧的收益快照覆盖数据库里更新的记录。"""
        if not existing_state or not incoming_returns:
            return False

        existing_latest_date = parse_date(existing_state.get('latest_nav_date'))
        incoming_latest_date = parse_date(incoming_returns.get('latest_nav_date'))
        if existing_latest_date is None or incoming_latest_date is None:
            return False

        return incoming_latest_date < existing_latest_date

    @staticmethod
    def _has_exact_search_match(search_results: Optional[List[dict]], fcode: str) -> bool:
        """判断 fundSearch 结果中是否存在精确 code 命中。"""
        target_code = str(fcode or '').strip()
        if not target_code or not search_results:
            return False

        for item in search_results:
            if not isinstance(item, dict):
                continue
            base_info = item.get('FundBaseInfo') or {}
            candidate_codes = [
                base_info.get('FCODE'),
                item.get('FCODE'),
                item.get('CODE'),
                item.get('_id'),
            ]
            if any(str(code).strip() == target_code for code in candidate_codes if code):
                return True

        return False

    def _get_rank_code_exact_match_cache(self) -> Dict[str, bool]:
        """延迟初始化 rank 精确命中缓存。"""
        cache = getattr(self, '_rank_code_exact_match_cache', None)
        if cache is None:
            cache = {}
            self._rank_code_exact_match_cache = cache
        return cache

    def _get_rank_name_resolution_cache(self) -> Dict[str, Optional[str]]:
        """延迟初始化 rank 名称归一化缓存。"""
        cache = getattr(self, '_rank_name_resolution_cache', None)
        if cache is None:
            cache = {}
            self._rank_name_resolution_cache = cache
        return cache

    def _resolve_canonical_rank_fcode(
        self,
        fcode: Optional[str],
        shortname: Optional[str],
    ) -> str:
        """
        对 fundMNRank 中少量 97xxxx 别名码做低风险主键归一化。

        只在以下条件同时满足时改写：
        1. 当前 code 以 97 开头；
        2. `fundSearch` 对当前 code 不存在精确命中；
        3. `fundSearchInfoByName` 对 shortname 存在唯一精确命中，且返回的是另一个 code。
        """
        normalized_code = str(fcode or '').strip()
        normalized_name = str(shortname or '').strip()
        if not normalized_code or not normalized_name or not normalized_code.startswith('97'):
            return normalized_code

        api = getattr(self, 'api', None)
        if api is None:
            return normalized_code

        code_cache = self._get_rank_code_exact_match_cache()
        exact_found = code_cache.get(normalized_code)
        if exact_found is None:
            try:
                exact_found = self._has_exact_search_match(
                    api.search_funds(normalized_code, search_type='1'),
                    normalized_code,
                )
            except Exception as exc:
                logger.warning(
                    "fundMNRank 别名码精确搜索失败 %s (%s): %s",
                    normalized_code,
                    normalized_name,
                    exc,
                )
                exact_found = False
            code_cache[normalized_code] = exact_found

        if exact_found:
            return normalized_code

        name_cache = self._get_rank_name_resolution_cache()
        cached_code = name_cache.get(normalized_name)
        if cached_code is not None:
            return cached_code

        resolved_code = normalized_code
        search_by_name = getattr(api, 'search_funds_by_name', None)
        if callable(search_by_name):
            try:
                results = search_by_name(
                    normalized_name,
                    order_type='2',
                    page_index=1,
                    page_size=5,
                ) or []
                exact_codes = sorted({
                    str(item.get('fcode')).strip()
                    for item in results
                    if isinstance(item, dict)
                    and str(item.get('shortname') or '').strip() == normalized_name
                    and str(item.get('fcode') or '').strip()
                })
                if len(exact_codes) == 1 and exact_codes[0] != normalized_code:
                    resolved_code = exact_codes[0]
                    logger.info(
                        "fundMNRank 别名主键归一化 %s -> %s (%s)",
                        normalized_code,
                        resolved_code,
                        normalized_name,
                    )
            except Exception as exc:
                logger.warning(
                    "fundMNRank 名称归一化搜索失败 %s (%s): %s",
                    normalized_code,
                    normalized_name,
                    exc,
                )

        name_cache[normalized_name] = resolved_code
        return resolved_code

    @classmethod
    def _merge_duplicate_fund_records(cls, existing: dict, candidate: dict) -> dict:
        """
        合并重复基金记录，优先保留更具体的分类结果。

        同一基金可能同时出现在 31/32 接口中，若直接保留首次出现记录，
        会把定开债(32/008)吞并成普通债券(31/003)。
        """
        existing_code = str(existing.get('type_code') or '').strip()
        candidate_code = str(candidate.get('type_code') or '').strip()
        existing_ftype = str(existing.get('ftype') or '').strip()
        candidate_ftype = str(candidate.get('ftype') or '').strip()
        existing_fixed_income = existing.get('type_category') == 'fixed_income'
        candidate_fixed_income = candidate.get('type_category') == 'fixed_income'

        prefer_candidate = False
        if candidate_code == FundType.BOND_FIXED and existing_code != FundType.BOND_FIXED:
            prefer_candidate = True
        elif existing_code == FundType.BOND_FIXED and candidate_code != FundType.BOND_FIXED:
            prefer_candidate = False
        elif candidate_fixed_income != existing_fixed_income:
            prefer_candidate = candidate_fixed_income
        elif candidate_ftype == '008' and existing_ftype != '008':
            prefer_candidate = True
        elif existing_ftype == '008' and candidate_ftype != '008':
            prefer_candidate = False
        else:
            prefer_candidate = (
                cls._count_non_empty_values(candidate)
                >= cls._count_non_empty_values(existing)
            )

        preferred = candidate if prefer_candidate else existing
        fallback = existing if prefer_candidate else candidate

        merged = fallback.copy()
        for key, value in preferred.items():
            if key == 'returns':
                continue
            if value not in (None, "", []):
                merged[key] = value

        preferred_returns = preferred.get('returns') or {}
        fallback_returns = fallback.get('returns') or {}
        if preferred_returns or fallback_returns:
            merged_returns = fallback_returns.copy()
            merged_returns.update(select_return_update(fallback_returns, preferred_returns))
            merged['returns'] = merged_returns

        return merged

    @classmethod
    def _deduplicate_fund_records(cls, funds: Optional[List[dict]]) -> Tuple[List[dict], int]:
        """按 fcode 合并重复基金记录，作为最终落库前的幂等保护。"""
        unique_funds: Dict[str, dict] = {}
        ordered_codes: List[str] = []
        passthrough_records: List[dict] = []
        duplicate_count = 0

        for fund in funds or []:
            if not isinstance(fund, dict):
                passthrough_records.append(fund)
                continue

            fcode = str(fund.get('fcode') or '').strip()
            if not fcode:
                passthrough_records.append(fund)
                continue

            normalized_fund = fund.copy()
            normalized_fund['fcode'] = fcode
            if fcode not in unique_funds:
                unique_funds[fcode] = normalized_fund
                ordered_codes.append(fcode)
                continue

            duplicate_count += 1
            unique_funds[fcode] = cls._merge_duplicate_fund_records(
                unique_funds[fcode],
                normalized_fund,
            )

        merged_records = [unique_funds[fcode] for fcode in ordered_codes]
        merged_records.extend(passthrough_records)
        return merged_records, duplicate_count

    def _search_funds_with_quality(self, fcode: str) -> Tuple[List[Dict[str, Any]], Any]:
        """Call fundSearch while preserving compatibility with injected test APIs."""
        search_func = getattr(self.api, "search_funds")
        try:
            result = search_func(fcode, search_type='1', return_quality=True)
        except TypeError:
            result = search_func(fcode, search_type='1')

        if (
            isinstance(result, tuple)
            and len(result) == 2
            and isinstance(result[0], list)
        ):
            return result[0], result[1]
        return list(result or []), None

    @staticmethod
    def _increment_count(stats: Dict[str, int], key: str, amount: int = 1) -> None:
        stats[key] = safe_int(stats.get(key), 0) + int(amount)

    def build_search_fallback_records(
        self,
        existing_funds: List[Dict[str, Any]],
    ) -> Tuple[List[dict], Dict[str, int]]:
        """
        使用 `fundSearch` 为列表接口未覆盖的已存在基金补充最新净值快照。

        该接口不返回多周期 `SYL_*` 字段，因此这里只补 `latest_nav/latest_nav_date`
        以及基础信息中确定性较高的字段，避免把旧收益字段覆盖为空。
        """
        fallback_records: List[dict] = []
        stats = {
            'targets': len(existing_funds or []),
            'queried': 0,
            'matched': 0,
            'missing': 0,
            'failed': 0,
            'skipped_excluded': 0,
            'period_return_attempted': 0,
            'period_return_matched': 0,
            'period_return_missing': 0,
            'period_return_failed': 0,
            'period_return_date_mismatch': 0,
            'api_response_accepted': 0,
            'api_response_has_data': 0,
            'api_response_empty': 0,
            'api_response_quality_unavailable': 0,
        }
        api_source_evidence = {
            'schema_version': 1,
            'task': 'fundSearch-fallback',
            'source': 'fundSearch',
            'target_count': len(existing_funds or []),
            'queried_count': 0,
            'accepted_response_count': 0,
            'has_data_response_count': 0,
            'empty_response_count': 0,
            'failed_response_count': 0,
            'quality_unavailable_count': 0,
            'exact_match_count': 0,
            'matched_record_count': 0,
            'missing_record_count': 0,
            'failed_query_count': 0,
            'quality_status_counts': {},
            'quality_payload_type_counts': {},
        }
        collected_at = datetime.now()

        for existing in existing_funds or []:
            fcode = str(existing.get('fcode') or '').strip()
            if not fcode:
                stats['missing'] += 1
                continue
            if is_runtime_excluded_fund(fcode):
                stats['skipped_excluded'] += 1
                continue

            stats['queried'] += 1
            api_source_evidence['queried_count'] += 1
            try:
                search_results, search_quality = self._search_funds_with_quality(fcode)
                if search_quality is None:
                    stats['api_response_quality_unavailable'] += 1
                    api_source_evidence['quality_unavailable_count'] += 1
                else:
                    quality_evidence = self._quality_evidence(search_quality)
                    status = str(quality_evidence.get('status') or 'unknown')
                    payload_type = str(quality_evidence.get('payload_type') or 'unknown')
                    self._increment_count(api_source_evidence['quality_status_counts'], status)
                    self._increment_count(api_source_evidence['quality_payload_type_counts'], payload_type)
                    if quality_evidence.get('accepted'):
                        stats['api_response_accepted'] += 1
                        api_source_evidence['accepted_response_count'] += 1
                    else:
                        api_source_evidence['failed_response_count'] += 1
                    if quality_evidence.get('has_data'):
                        stats['api_response_has_data'] += 1
                        api_source_evidence['has_data_response_count'] += 1
                    else:
                        stats['api_response_empty'] += 1
                        api_source_evidence['empty_response_count'] += 1
                exact_found = self._has_exact_search_match(search_results, fcode)
                matched = self._select_search_result(search_results, fcode)
            except Exception as exc:
                logger.warning("fundSearch fallback 查询失败 %s: %s", fcode, exc)
                stats['failed'] += 1
                api_source_evidence['failed_query_count'] += 1
                continue

            if not exact_found or not matched:
                stats['missing'] += 1
                api_source_evidence['missing_record_count'] += 1
                continue
            api_source_evidence['exact_match_count'] += 1

            base_info = self._extract_search_base_info(matched)
            if not base_info:
                stats['missing'] += 1
                api_source_evidence['missing_record_count'] += 1
                continue

            resolved_ftype = base_info.get('FTYPE') or existing.get('ftype')
            normalized_type_code, normalized_type_category = normalize_fund_type_fields(
                resolved_ftype,
                existing.get('type_code'),
                existing.get('type_category'),
            )
            latest_nav = safe_float(base_info.get('DWJZ'))
            latest_nav_date = parse_date(base_info.get('FSRQ'))
            buyable_flag = self._parse_buyable_flag(base_info)

            fund_update = self._compact_mapping({
                'fcode': fcode,
                'shortname': base_info.get('SHORTNAME') or matched.get('NAME') or existing.get('shortname'),
                'ftype': resolved_ftype,
                'sub_type': extract_bond_subtype(resolved_ftype),
                'type_code': normalized_type_code or existing.get('type_code') or FundType.BOND,
                'type_category': normalized_type_category or existing.get('type_category') or 'fixed_income',
                'company_name': base_info.get('JJGS'),
                'manager_names': base_info.get('JJJL'),
                'is_buyable': buyable_flag,
                'purchase_status': self._derive_purchase_status(buyable_flag),
                'data_source': 'fundSearch',
                'last_updated_at': collected_at,
            })

            returns_update = self._compact_mapping({
                'fcode': fcode,
                'latest_nav': latest_nav,
                'latest_nav_date': latest_nav_date,
                'last_updated_at': collected_at,
            })
            period_returns, period_stats = self._fetch_period_increase_returns(
                fcode,
                expected_nav_date=latest_nav_date,
            )
            stats['period_return_attempted'] += period_stats['attempted']
            stats['period_return_matched'] += period_stats['matched']
            stats['period_return_missing'] += period_stats['missing']
            stats['period_return_failed'] += period_stats['failed']
            stats['period_return_date_mismatch'] += period_stats['date_mismatch']
            if period_returns:
                returns_update.update(period_returns)

            if len(returns_update) <= 2 and len(fund_update) <= 2:
                stats['missing'] += 1
                continue

            if len(returns_update) > 2:
                fund_update['returns'] = returns_update

            fallback_records.append(fund_update)
            stats['matched'] += 1
            api_source_evidence['matched_record_count'] += 1

        logger.info("api_source_evidence=%s", json_dumps(api_source_evidence))
        return fallback_records, stats

    @staticmethod
    def _map_period_increase_returns(datas: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
        """将 `fundMNPeriodIncrease` 的 title/syl 记录映射为本地收益字段。"""
        returns: Dict[str, Any] = {field: None for field in RETURN_FIELDS}
        seen = set()
        for item in datas or []:
            if not isinstance(item, dict):
                continue
            title = str(item.get('title') or '').strip()
            field_name = PERIOD_INCREASE_TITLE_TO_RETURN_FIELD.get(title)
            if not field_name:
                continue
            if field_name in seen:
                raise ValueError(f"fundMNPeriodIncrease 重复周期: {title}")
            seen.add(field_name)
            value = period_number(item.get('syl'))
            if value is None:
                continue
            returns[field_name] = value
        return returns

    def _fetch_period_increase_returns(
        self,
        fcode: str,
        *,
        expected_nav_date=None,
    ) -> Tuple[Dict[str, Any], Dict[str, int]]:
        """
        为排行当前不可见的基金补拉单基金周期收益。

        仅在 `fundMNPeriodIncrease` 的 Expansion.TIME 与当前净值日一致时接受，
        避免把不同基准日的收益快照和净值快照混写到同一行。
        """
        stats = {
            'attempted': 1,
            'matched': 0,
            'missing': 0,
            'failed': 0,
            'date_mismatch': 0,
        }
        self._last_period_return_response = None
        try:
            period_data = self.api.get_period_increase(fcode)
            self._last_period_return_response = period_data
        except Exception as exc:
            logger.warning("fundMNPeriodIncrease 查询失败 %s: %s", fcode, exc)
            stats['failed'] += 1
            return {}, stats

        datas, quality = inspect_tiantian_period_increase_payload(
            period_data,
            endpoint_label=f"/fundMNPeriodIncrease FCODE={fcode}",
        )
        if not quality.accepted:
            logger.warning("fundMNPeriodIncrease 不可用 %s: %s", fcode, quality.message)
            stats['failed'] += 1
            return {}, stats
        if not quality.has_data:
            stats['missing'] += 1
            return {}, stats

        try:
            period_returns = self._map_period_increase_returns(datas)
        except (ValueError, TypeError):
            stats['failed'] += 1
            return {}, stats
        if not any(value is not None for value in period_returns.values()):
            stats['missing'] += 1
            return {}, stats

        expected_date = parse_date(expected_nav_date)
        expansion = period_data.get('Expansion') if isinstance(period_data, dict) else None
        period_as_of_date = parse_date(expansion.get('TIME')) if isinstance(expansion, dict) else None
        if period_as_of_date is None or period_as_of_date > datetime.now().date():
            stats['date_mismatch'] += 1
            return {}, stats
        if expected_date is not None and period_as_of_date != expected_date:
            logger.info(
                "fundMNPeriodIncrease 基准日与搜索补源净值日不一致，跳过收益补源: %s period=%s nav=%s",
                fcode,
                period_as_of_date,
                expected_date,
            )
            stats['date_mismatch'] += 1
            return {}, stats

        try:
            snapshot = build_period_snapshot(
                period_returns, as_of_date=period_as_of_date, observed_at=datetime.now(),
                source='fundMNPeriodIncrease',
                source_payload={'fcode': fcode, 'response': period_data},
            )
        except (TypeError, ValueError):
            stats['failed'] += 1
            return {}, stats
        stats['matched'] += 1
        return snapshot, stats

    def build_nav_history_fallback_records(
        self,
        existing_funds: List[Dict[str, Any]],
    ) -> Tuple[List[dict], Dict[str, int]]:
        """
        使用本地 `fund_nav_history` 的最新记录回填 `fund_returns.latest_nav/latest_nav_date`。

        该补源只覆盖本地已经采集过净值历史、但 `fund_returns` 仍滞后的基金。
        不在这里推导多周期收益率，避免扩大脚本职责。
        """
        targets = [fund for fund in (existing_funds or []) if fund.get('fcode')]
        stats = {
            'targets': len(targets),
            'matched': 0,
            'missing': 0,
            'skipped_excluded': 0,
        }
        if not targets:
            return [], stats

        collected_at = datetime.now()
        target_codes = []
        for fund in targets:
            fcode = str(fund['fcode']).strip()
            if is_runtime_excluded_fund(fcode):
                stats['skipped_excluded'] += 1
                continue
            target_codes.append(fcode)
        stats['targets'] = len(target_codes)
        if not target_codes:
            return [], stats
        latest_nav_rows = {}

        with self.db_manager.get_session(readonly=True) as session:
            for fcode in target_codes:
                row = (
                    session.query(FundNavHistory)
                    .filter(FundNavHistory.fcode == fcode)
                    .order_by(FundNavHistory.nav_date.desc(), FundNavHistory.id.desc())
                    .first()
                )
                if row:
                    latest_nav_rows[fcode] = row

        fallback_records: List[dict] = []
        for existing in targets:
            fcode = str(existing['fcode']).strip()
            if is_runtime_excluded_fund(fcode):
                continue
            latest_nav_row = latest_nav_rows.get(fcode)
            if latest_nav_row is None:
                stats['missing'] += 1
                continue

            latest_nav_date = latest_nav_row.nav_date
            existing_latest_date = existing.get('latest_nav_date')
            if existing_latest_date and latest_nav_date and latest_nav_date <= existing_latest_date:
                stats['missing'] += 1
                continue

            returns_update = self._compact_mapping({
                'fcode': fcode,
                'latest_nav': latest_nav_row.unit_nav,
                'latest_nav_date': latest_nav_date,
                'daily_growth': latest_nav_row.daily_growth,
                'last_updated_at': collected_at,
            })
            if len(returns_update) <= 2:
                stats['missing'] += 1
                continue

            fallback_records.append({
                'fcode': fcode,
                'last_updated_at': collected_at,
                'returns': returns_update,
            })
            stats['matched'] += 1

        return fallback_records, stats

    def merge_with_newer_nav_history_records(
        self,
        existing_records: List[dict],
        existing_funds: List[Dict[str, Any]],
    ) -> Tuple[List[dict], Dict[str, int]]:
        """
        当本地 `fund_nav_history` 比当前候选记录更新时，用本地净值覆盖候选的最新净值字段。
        """
        records = [record.copy() for record in (existing_records or [])]
        fund_by_code = {
            str(fund.get('fcode')).strip(): fund
            for fund in (existing_funds or [])
            if fund.get('fcode')
        }
        stats = {
            'targets': len(records),
            'overridden': 0,
            'unchanged': len(records),
        }
        if not records:
            return records, stats

        nav_records, nav_stats = self.build_nav_history_fallback_records(list(fund_by_code.values()))
        nav_by_code = {
            str(record.get('fcode')).strip(): record
            for record in nav_records
            if record.get('fcode')
        }

        for record in records:
            fcode = str(record.get('fcode') or '').strip()
            nav_record = nav_by_code.get(fcode)
            if nav_record is None:
                continue

            candidate_returns = record.get('returns') or {}
            nav_returns = nav_record.get('returns') or {}
            candidate_date = candidate_returns.get('latest_nav_date')
            nav_date = nav_returns.get('latest_nav_date')
            if not nav_date:
                continue
            if candidate_date and nav_date <= candidate_date:
                continue

            merged_returns = candidate_returns.copy()
            for key, value in nav_returns.items():
                if value is not None:
                    merged_returns[key] = value
            record['returns'] = merged_returns
            if nav_record.get('last_updated_at') is not None:
                record['last_updated_at'] = nav_record['last_updated_at']
            stats['overridden'] += 1

        stats['unchanged'] = stats['targets'] - stats['overridden']
        stats['nav_history_candidates'] = nav_stats.get('matched', 0)
        return records, stats

    def _resolve_fund_types(
        self,
        fund_types: Optional[List[Union[int, str, 'FundTypeCode']]]
    ) -> List[FundType]:
        """
        解析基金类型参数为FundType列表

        Args:
            fund_types: 输入的基金类型列表

        Returns:
            解析后的FundType列表
        """
        if fund_types is None:
            return [FundType.BOND, FundType.BOND_FIXED]

        resolved = []

        for ft in fund_types:
            # `bond` 作为项目级别别名应覆盖 31 + 32。
            # 显式数值 31/32 仍保持单一口径，避免吞掉调用方的精确意图。
            if isinstance(ft, str) and ft.lower().strip() == 'bond':
                for api_fund_type in (FundType.BOND, FundType.BOND_FIXED):
                    if api_fund_type not in resolved:
                        resolved.append(api_fund_type)
                continue

            api_fund_type = self._convert_to_fund_type(ft)
            if api_fund_type and api_fund_type not in resolved:
                resolved.append(api_fund_type)

        return resolved

    def _convert_to_fund_type(
        self,
        fund_type: Union[int, str, 'FundTypeCode']
    ) -> Optional[FundType]:
        """
        将单个基金类型转换为FundType

        Args:
            fund_type: 基金类型（int/str/FundTypeCode）

        Returns:
            对应的FundType枚举值
        """
        # 如果是整数API代码
        if isinstance(fund_type, int):
            return FUND_TYPE_API_MAP.get(fund_type)

        # 如果是字符串
        if isinstance(fund_type, str):
            # 尝试作为API代码解析
            try:
                code = int(fund_type)
                return FUND_TYPE_API_MAP.get(code)
            except ValueError:
                pass

            # 尝试作为别名解析
            if HAS_REGISTRY and resolve_alias:
                fund_type_code = resolve_alias(fund_type)
                if fund_type_code:
                    code = int(fund_type_code.value)
                    return FUND_TYPE_API_MAP.get(code)

            # 直接别名映射
            alias_map = {
                'stock': FundType.STOCK,
                'index': FundType.INDEX,
                'hybrid': FundType.HYBRID,
                'bond': FundType.BOND,
                'money': FundType.MONEY,
                'qdii': FundType.QDII,
                'fof': FundType.FOF,
                'etf': FundType.ETF,
                'lof': FundType.LOF,
            }
            return alias_map.get(fund_type.lower().strip())

        # 如果是FundTypeCode枚举
        if HAS_REGISTRY and FundTypeCode and isinstance(fund_type, FundTypeCode):
            code = int(fund_type.value)
            return FUND_TYPE_API_MAP.get(code)

        # 如果已经是FundType
        if isinstance(fund_type, FundType):
            return fund_type

        return None

    @staticmethod
    def _should_retry_rank_page_quality(quality) -> bool:
        status = str(getattr(quality, 'status', '') or '').strip()
        failure_kind = str(getattr(quality, 'failure_kind', '') or '').strip()
        if status in FUND_LIST_PAGE_RETRYABLE_STATUSES:
            return (
                failure_kind in FUND_LIST_PAGE_RETRYABLE_FAILURE_KINDS
                or status == 'upstream_business_degraded'
            )
        return False

    def _fetch_rank_page_with_retry(
        self,
        *,
        fund_type: FundType,
        page: int,
        page_size: int,
        only_buyable: bool,
    ) -> Tuple[dict, list, Any]:
        endpoint_label = f"/fundMNRank FundType={fund_type} page={page}"
        last_page_data = {}
        last_records = []
        last_quality = None

        for attempt in range(1, FUND_LIST_PAGE_RETRY_MAX_ATTEMPTS + 1):
            page_data = self.api.get_bond_fund_rank(
                fund_type=fund_type,
                sort_column=SortColumn.RETURN_1Y,
                page_index=page,
                page_size=page_size,
                only_buyable=only_buyable
            )
            page_records, page_quality = inspect_tiantian_rank_payload(
                page_data,
                endpoint_label=endpoint_label,
            )
            if page_quality.accepted:
                return page_data, page_records, page_quality

            last_page_data = page_data
            last_records = page_records
            last_quality = page_quality
            should_retry = (
                attempt < FUND_LIST_PAGE_RETRY_MAX_ATTEMPTS
                and self._should_retry_rank_page_quality(page_quality)
            )
            if not should_retry:
                break

            wait_seconds = min(2.0 * attempt, 5.0)
            logger.warning(
                "基金列表分页拉取失败，准备重试: FundType=%s page=%s attempt=%s/%s reason=%s",
                fund_type,
                page,
                attempt,
                FUND_LIST_PAGE_RETRY_MAX_ATTEMPTS,
                page_quality.message,
            )
            time.sleep(wait_seconds)

        return last_page_data, last_records, last_quality

    @staticmethod
    def _quality_evidence(quality: Any) -> Dict[str, Any]:
        """Serialize TiantianPayloadQuality without raw payload content."""
        return {
            "accepted": bool(getattr(quality, "accepted", False)),
            "has_data": bool(getattr(quality, "has_data", False)),
            "status": str(getattr(quality, "status", "") or ""),
            "payload_type": getattr(quality, "payload_type", None),
            "failure_kind": getattr(quality, "failure_kind", None),
            "error_type": getattr(quality, "error_type", None),
            "http_status": getattr(quality, "http_status", None),
            "upstream_business_code": getattr(quality, "upstream_business_code", None),
            "record_count": getattr(quality, "record_count", None),
        }

    @staticmethod
    def _record_missing_business_value(item: dict, field: str) -> bool:
        value = item.get(field)
        if value is None:
            return True
        if isinstance(value, str):
            normalized = value.strip()
            return not normalized or normalized in PLACEHOLDER_TEXT_VALUES
        return False

    @staticmethod
    def _missing_business_value_reason(item: dict, field: str) -> str:
        if field not in item:
            return "raw_field_absent"
        value = item.get(field)
        if value is None:
            return "raw_value_null"
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return "raw_value_empty_string"
            if normalized in PLACEHOLDER_TEXT_VALUES:
                return "raw_value_placeholder_text"
        return ""

    @classmethod
    def _new_rank_snapshot_missing_semantics(cls) -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "source": "fundMNRank",
            "truth_policy": "preserve_missing_semantics",
            "missing_value_handling": (
                "Missing snapshot fields are recorded as upstream missing evidence; "
                "the collector does not synthesize nav_date/nav_value or convert them to 0."
            ),
            "fields": {
                raw_field: {
                    "raw_field": raw_field,
                    "business_field": meta["business_field"],
                    "missing_count": 0,
                    "raw_field_present_count": 0,
                    "raw_field_absent_count": 0,
                    "non_missing_count": 0,
                    "value_type_counts": {},
                    "missing_reason_counts": {},
                    "missing_samples": [],
                }
                for raw_field, meta in RANK_SNAPSHOT_FIELD_META.items()
            },
        }

    @classmethod
    def _ensure_rank_snapshot_missing_semantics(cls, source_evidence: Dict[str, Any]) -> Dict[str, Any]:
        semantics = source_evidence.get("snapshot_missing_semantics")
        if not isinstance(semantics, dict):
            semantics = cls._new_rank_snapshot_missing_semantics()
            source_evidence["snapshot_missing_semantics"] = semantics
        fields = semantics.setdefault("fields", {})
        for raw_field, meta in RANK_SNAPSHOT_FIELD_META.items():
            fields.setdefault(
                raw_field,
                {
                    "raw_field": raw_field,
                    "business_field": meta["business_field"],
                    "missing_count": 0,
                    "raw_field_present_count": 0,
                    "raw_field_absent_count": 0,
                    "non_missing_count": 0,
                    "value_type_counts": {},
                    "missing_reason_counts": {},
                    "missing_samples": [],
                },
            )
        return semantics

    @classmethod
    def _record_rank_snapshot_field_evidence(
        cls,
        source_evidence: Dict[str, Any],
        item: dict,
        raw_field: str,
        *,
        page: int,
    ) -> bool:
        semantics = cls._ensure_rank_snapshot_missing_semantics(source_evidence)
        field_evidence = semantics["fields"][raw_field]
        value = item.get(raw_field)
        value_type = type(value).__name__
        value_type_counts = field_evidence.setdefault("value_type_counts", {})
        value_type_counts[value_type] = safe_int(value_type_counts.get(value_type), 0) + 1
        if raw_field in item:
            field_evidence["raw_field_present_count"] = safe_int(
                field_evidence.get("raw_field_present_count"), 0
            ) + 1
        else:
            field_evidence["raw_field_absent_count"] = safe_int(
                field_evidence.get("raw_field_absent_count"), 0
            ) + 1

        missing_reason = cls._missing_business_value_reason(item, raw_field)
        if not missing_reason:
            field_evidence["non_missing_count"] = safe_int(field_evidence.get("non_missing_count"), 0) + 1
            return False

        field_evidence["missing_count"] = safe_int(field_evidence.get("missing_count"), 0) + 1
        reason_counts = field_evidence.setdefault("missing_reason_counts", {})
        reason_counts[missing_reason] = safe_int(reason_counts.get(missing_reason), 0) + 1
        samples = field_evidence.setdefault("missing_samples", [])
        if len(samples) < MAX_RANK_SNAPSHOT_MISSING_SAMPLES:
            samples.append(
                {
                    "page": page,
                    "fcode": str(item.get("FCODE") or "").strip() or None,
                    "raw_field_present": raw_field in item,
                    "raw_value_type": value_type,
                    "missing_reason": missing_reason,
                }
            )
        return True

    @classmethod
    def _combine_rank_snapshot_missing_semantics(
        cls,
        api_source_evidence: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        combined = cls._new_rank_snapshot_missing_semantics()
        combined_fields = combined["fields"]
        for evidence in api_source_evidence or []:
            semantics = evidence.get("snapshot_missing_semantics")
            if not isinstance(semantics, dict):
                continue
            fields = semantics.get("fields")
            if not isinstance(fields, dict):
                continue
            for raw_field in RANK_SNAPSHOT_FIELD_META:
                source_field = fields.get(raw_field)
                if not isinstance(source_field, dict):
                    continue
                target_field = combined_fields[raw_field]
                for key in (
                    "missing_count",
                    "raw_field_present_count",
                    "raw_field_absent_count",
                    "non_missing_count",
                ):
                    target_field[key] = safe_int(target_field.get(key), 0) + safe_int(source_field.get(key), 0)
                for counter_key in ("value_type_counts", "missing_reason_counts"):
                    source_counts = source_field.get(counter_key)
                    target_counts = target_field.setdefault(counter_key, {})
                    if not isinstance(source_counts, dict):
                        continue
                    for count_key, count_value in source_counts.items():
                        key_text = str(count_key)
                        target_counts[key_text] = safe_int(target_counts.get(key_text), 0) + safe_int(count_value, 0)
                target_samples = target_field.setdefault("missing_samples", [])
                for sample in source_field.get("missing_samples") or []:
                    if len(target_samples) >= MAX_RANK_SNAPSHOT_MISSING_SAMPLES:
                        break
                    if isinstance(sample, dict):
                        target_samples.append(dict(sample))
        return combined

    @classmethod
    def _update_rank_page_source_evidence(
        cls,
        source_evidence: Dict[str, Any],
        page_records: list,
        *,
        page: int,
        page_size: int,
        total_pages: int,
        declared_total_count: int,
        seen_codes: set,
        page_declared_total_count: Any = None,
        response_payload: Any = None,
    ) -> None:
        record_rank_page_observation(
            source_evidence, page_records or [], page=page, previous_codes=seen_codes,
            declared_total=page_declared_total_count, sort_column=SortColumn.RETURN_1Y,
            response_payload=response_payload,
        )
        record_count = len(page_records or [])
        source_evidence["raw_record_count"] = safe_int(source_evidence.get("raw_record_count")) + record_count
        min_count = source_evidence.get("accepted_page_record_count_min")
        max_count = source_evidence.get("accepted_page_record_count_max")
        source_evidence["accepted_page_record_count_min"] = (
            record_count if min_count in (None, "") else min(safe_int(min_count), record_count)
        )
        source_evidence["accepted_page_record_count_max"] = (
            record_count if max_count in (None, "") else max(safe_int(max_count), record_count)
        )

        if page < total_pages and record_count < page_size:
            source_evidence["short_page_before_last_count"] = (
                safe_int(source_evidence.get("short_page_before_last_count")) + 1
            )
            samples = source_evidence.setdefault("short_page_before_last_samples", [])
            if len(samples) < 5:
                samples.append({"page": page, "record_count": record_count, "expected_min": page_size})

        if page == total_pages and declared_total_count > 0:
            expected_last_page_count = declared_total_count - (page_size * max(total_pages - 1, 0))
            if expected_last_page_count > 0 and record_count > expected_last_page_count:
                source_evidence["last_page_overflow_count"] = (
                    safe_int(source_evidence.get("last_page_overflow_count")) + 1
                )
                samples = source_evidence.setdefault("last_page_overflow_samples", [])
                if len(samples) < 5:
                    samples.append(
                        {
                            "page": page,
                            "record_count": record_count,
                            "expected_max": expected_last_page_count,
                        }
                    )

        for item in page_records or []:
            if not isinstance(item, dict):
                source_evidence["invalid_record_count"] = safe_int(source_evidence.get("invalid_record_count")) + 1
                continue

            missing_identity = [
                field for field in RANK_REQUIRED_IDENTITY_FIELDS
                if cls._record_missing_business_value(item, field)
            ]
            if missing_identity:
                for field in missing_identity:
                    key = "missing_fcode_count" if field == "FCODE" else "missing_shortname_count"
                    source_evidence[key] = safe_int(source_evidence.get(key)) + 1
                samples = source_evidence.setdefault("missing_identity_samples", [])
                if len(samples) < 5:
                    samples.append(
                        {
                            "page": page,
                            "missing_fields": missing_identity,
                            "fcode": str(item.get("FCODE") or "").strip() or None,
                        }
                    )

            if cls._record_rank_snapshot_field_evidence(source_evidence, item, "FSRQ", page=page):
                source_evidence["missing_nav_date_count"] = safe_int(source_evidence.get("missing_nav_date_count")) + 1
            if cls._record_rank_snapshot_field_evidence(source_evidence, item, "DWJZ", page=page):
                source_evidence["missing_nav_value_count"] = safe_int(source_evidence.get("missing_nav_value_count")) + 1

            code = str(item.get("FCODE") or "").strip()
            if not code:
                continue
            if code in seen_codes:
                source_evidence["duplicate_code_count"] = safe_int(source_evidence.get("duplicate_code_count")) + 1
                samples = source_evidence.setdefault("duplicate_code_samples", [])
                if len(samples) < 10 and code not in samples:
                    samples.append(code)
            seen_codes.add(code)

    def _collect_by_type(self, fund_type: FundType, only_buyable: bool) -> List[dict]:
        """
        按类型采集基金列表

        Args:
            fund_type: 基金类型
            only_buyable: 仅可购买

        Returns:
            基金列表
        """
        logger.info(f"采集基金类型: {fund_type}")

        # 第一次请求获取总数
        source_evidence: Dict[str, Any] = {
            "source": "fundMNRank",
            "fund_type": str(fund_type),
            "only_buyable": bool(only_buyable),
            "page_size": 30,
            "sort_column": SortColumn.RETURN_1Y,
            "sort_direction": "desc",
            "page_observations": [],
            "declared_total_count": 0,
            "expected_total_pages": 0,
            "page_request_count": 0,
            "accepted_page_count": 0,
            "empty_page_count": 0,
            "failed_page_count": 0,
            "raw_record_count": 0,
            "collected_record_count": 0,
            "accepted_page_record_count_min": None,
            "accepted_page_record_count_max": None,
            "missing_fcode_count": 0,
            "missing_shortname_count": 0,
            "missing_nav_date_count": 0,
            "missing_nav_value_count": 0,
            "snapshot_missing_semantics": self._new_rank_snapshot_missing_semantics(),
            "invalid_record_count": 0,
            "duplicate_code_count": 0,
            "short_page_before_last_count": 0,
            "last_page_overflow_count": 0,
            "declared_collected_shortfall": 0,
            "declared_collected_overflow": 0,
            "first_page_quality": {},
            "quality_status_counts": {},
            "non_ok_quality_status_counts": {},
            "early_stop_page": None,
        }
        self._last_collect_by_type_evidence = source_evidence
        seen_codes: set = set()

        first_page, first_page_records, first_page_quality = self._fetch_rank_page_with_retry(
            fund_type=fund_type,
            page=1,
            page_size=30,
            only_buyable=only_buyable
        )
        source_evidence["page_request_count"] += 1
        source_evidence["first_page_quality"] = self._quality_evidence(first_page_quality)
        source_evidence["first_page_declared_total_count"] = observed_total(
            first_page.get("TotalCount") if isinstance(first_page, dict) else None
        )
        if first_page_quality.accepted:
            source_evidence["accepted_page_count"] += 1
            status = str(getattr(first_page_quality, "status", "") or "unknown")
            status_counts = source_evidence["quality_status_counts"]
            status_counts[status] = safe_int(status_counts.get(status), 0) + 1
        else:
            source_evidence["failed_page_count"] += 1
        if not first_page_quality.accepted:
            raise RuntimeError(f"基金列表接口不可用: {first_page_quality.message}")
        if not first_page_quality.has_data or not first_page_records:
            logger.info("基金类型%s 当前未返回排行数据", fund_type)
            source_evidence["empty_page_count"] += 1
            record_rank_page_observation(source_evidence, first_page_records or [], page=1,
                                         previous_codes=seen_codes,
                                         declared_total=source_evidence["first_page_declared_total_count"],
                                         sort_column=SortColumn.RETURN_1Y,
                                         response_payload=first_page)
            finalize_rank_pagination_evidence(source_evidence, seen_codes)
            return []

        if not first_page:
            logger.error(f"获取基金列表失败: type={fund_type}")
            source_evidence["failed_page_count"] += 1
            return []

        total_count = observed_total(first_page.get('TotalCount'))
        if total_count is None or total_count == 0:
            # Nonempty data with no valid count cannot be paginated safely.
            # In particular, an absent count must not silently drop real rows.
            finalize_rank_pagination_evidence(source_evidence, seen_codes)
            raise RuntimeError(f"基金类型{fund_type}返回非空列表但TotalCount缺失、无效或为0")
        source_evidence["declared_total_count"] = safe_int(total_count)
        logger.info(f"基金类型{fund_type}共{total_count}只")

        # 计算总页数
        page_size = 30
        total_pages = (total_count + page_size - 1) // page_size
        source_evidence["page_size"] = page_size
        source_evidence["expected_total_pages"] = safe_int(total_pages)

        all_funds = []

        # 使用进度条
        with ProgressMonitor(total_pages, desc=f"采集基金列表(类型{fund_type})", unit="页") as progress:
            for page in range(1, total_pages + 1):
                if page == 1:
                    # 第一页已经获取
                    page_data = first_page
                    page_records = first_page_records
                else:
                    # 请求后续页
                    page_data, page_records, page_quality = self._fetch_rank_page_with_retry(
                        fund_type=fund_type,
                        page=page,
                        page_size=page_size,
                        only_buyable=only_buyable
                    )
                    source_evidence["page_request_count"] += 1
                    if not page_quality.accepted:
                        source_evidence["failed_page_count"] += 1
                        status = str(getattr(page_quality, "status", "") or "unknown")
                        status_counts = source_evidence["non_ok_quality_status_counts"]
                        status_counts[status] = safe_int(status_counts.get(status), 0) + 1
                        raise RuntimeError(f"基金列表接口不可用: {page_quality.message}")
                    source_evidence["accepted_page_count"] += 1
                    status = str(getattr(page_quality, "status", "") or "unknown")
                    status_counts = source_evidence["quality_status_counts"]
                    status_counts[status] = safe_int(status_counts.get(status), 0) + 1
                    if not page_quality.has_data or not page_records:
                        source_evidence["empty_page_count"] += 1
                        source_evidence["early_stop_page"] = page
                        record_rank_page_observation(source_evidence, page_records or [], page=page,
                                                     previous_codes=seen_codes,
                                                     declared_total=page_data.get("TotalCount"),
                                                     sort_column=SortColumn.RETURN_1Y,
                                                     response_payload=page_data)
                        logger.info("基金类型%s 第%s页返回空排行记录，提前结束分页", fund_type, page)
                        progress.update(1, success=True)
                        break

                self._update_rank_page_source_evidence(
                    source_evidence,
                    page_records,
                    page=page,
                    page_size=page_size,
                    total_pages=total_pages,
                    declared_total_count=safe_int(total_count),
                    seen_codes=seen_codes,
                    page_declared_total_count=page_data.get("TotalCount"),
                    response_payload=page_data,
                )
                funds = self._parse_fund_list(page_records, fund_type)
                all_funds.extend(funds)
                progress.update(1, success=True)

        logger.info(f"基金类型{fund_type}采集完成: {len(all_funds)}只")
        source_evidence["collected_record_count"] = len(all_funds)
        raw_record_count = safe_int(source_evidence.get("raw_record_count"))
        declared_total_count = safe_int(source_evidence.get("declared_total_count"))
        if declared_total_count > raw_record_count:
            source_evidence["declared_collected_shortfall"] = declared_total_count - raw_record_count
        elif raw_record_count > declared_total_count > 0:
            source_evidence["declared_collected_overflow"] = raw_record_count - declared_total_count
        finalize_rank_pagination_evidence(source_evidence, seen_codes)
        if source_evidence["pagination_warning_codes"]:
            logger.warning(
                "基金目录唯一覆盖待核验: FundType=%s declared=%s unique=%s duplicates=%s issues=%s",
                fund_type, source_evidence["first_page_declared_total_count"],
                source_evidence["unique_raw_code_count"], source_evidence["duplicate_code_count"],
                ",".join(source_evidence["pagination_warning_codes"]),
            )
        return all_funds

    def _parse_fund_list(self, datas: list, fund_type: FundType) -> List[dict]:
        """
        解析基金列表数据

        Args:
            datas: API返回的数据列表
            fund_type: 基金类型（用于填充type_code）

        Returns:
            解析后的基金列表
        """
        funds = []
        collected_at = datetime.now()

        # 获取type_code和type_category
        type_code = str(fund_type.value) if hasattr(fund_type, 'value') else str(fund_type)
        type_category = self._get_type_category(fund_type)

        for item in datas:
            try:
                raw_fcode = item.get('FCODE')
                fcode = self._resolve_canonical_rank_fcode(
                    raw_fcode,
                    item.get('SHORTNAME'),
                )
                if not fcode:
                    continue

                normalized_type_code, normalized_type_category = normalize_fund_type_fields(
                    item.get('FUNDTYPE'),
                    type_code,
                    type_category,
                )
                negotiable_cd_type_code, negotiable_cd_type_category = infer_high_confidence_negotiable_cd_type_fields(
                    item.get('SHORTNAME'),
                )
                if negotiable_cd_type_code:
                    normalized_type_code = negotiable_cd_type_code
                if negotiable_cd_type_category:
                    normalized_type_category = negotiable_cd_type_category
                buyable_flag = self._parse_buyable_flag({
                    'BUY': item.get('BUY'),
                    'ISBUY': item.get('ISBUY'),
                })

                # 基础信息
                fund = {
                    'fcode': fcode,
                    'shortname': item.get('SHORTNAME'),
                    'ftype': item.get('FUNDTYPE'),
                    'sub_type': extract_bond_subtype(item.get('FUNDTYPE')),
                    'type_code': normalized_type_code,
                    'type_category': normalized_type_category,
                    'risk_level': safe_int(item.get('RISKLEVEL')),
                    'rating': item.get('RLEVEL_SZ'),
                    'company_name': item.get('JJGS'),
                    'manager_names': item.get('JJJL'),
                    'is_buyable': buyable_flag,
                    'purchase_status': self._derive_purchase_status(buyable_flag),
                    'data_source': 'fundMNRank',
                    'last_updated_at': collected_at,
                }
                # `fundMNRank` 已返回当前规模（`ENDNAV`, 单位为元），日常调度本身会跑
                # collect-funds 但不会跑 collect-details；若不在这里回写，规模字段会
                # 长期停留在旧报告期，直接污染规模门槛与前端展示。
                from collectors.scale_refresh import _parse_scale
                dated_scale, _scale_reason = _parse_scale(item)
                if dated_scale:
                    fund.update(dated_scale)
                # P0-02: 当前代理下 fundMNRank 已实测可返回 CYCLE / FEATURE / BFUNDTYPE。
                # 这些字段直接进入 funds 表，作为持有期结构化识别和类型兜底的数据源。
                # 仍保持最小写入：占位值 '--' / 空串由 _compact_mapping 过滤，不覆盖 detail 真值。
                fund['cycle'] = item.get('CYCLE')
                fund['feature_tags'] = item.get('FEATURE')
                fund['feature_code'] = item.get('FEATURE')
                fund['base_fund_type_code'] = item.get('BFUNDTYPE')
                fund['rating_shanghai'] = item.get('RLEVEL_SZ')

                returns = self._build_rank_returns(item, fcode, collected_at)
                if returns:
                    fund['returns'] = returns
                funds.append(fund)

            except Exception as e:
                logger.error(f"解析基金数据失败: {item.get('FCODE')} - {e}")
                continue

        return funds

    def _get_type_category(self, fund_type: FundType) -> str:
        """获取基金大类"""
        category_map = {
            FundType.STOCK: 'equity',
            FundType.INDEX: 'equity',
            FundType.ETF: 'equity',
            FundType.LOF: 'equity',
            FundType.BOND: 'fixed_income',
            FundType.BOND_FIXED: 'fixed_income',
            FundType.MONEY: 'money_market',
            FundType.HYBRID: 'hybrid',
            FundType.QDII: 'alternative',
            FundType.FOF: 'multi_asset',
        }
        return category_map.get(fund_type, 'unknown')

    @staticmethod
    def _build_existing_fund_state_map(rows) -> Dict[str, Dict[str, Optional[str]]]:
        """提取已有基金的主记录关键字段，供增量更新时做低风险保护。"""
        state_map: Dict[str, Dict[str, Optional[str]]] = {}
        for row in rows or []:
            fcode = getattr(row, 'fcode', None)
            ftype = getattr(row, 'ftype', None)
            sub_type = getattr(row, 'sub_type', None)
            type_code = getattr(row, 'type_code', None)
            type_category = getattr(row, 'type_category', None)

            if fcode is None and isinstance(row, (tuple, list)) and row:
                fcode = row[0]
                ftype = row[1] if len(row) > 1 else None
                sub_type = row[2] if len(row) > 2 else None
                type_code = row[3] if len(row) > 3 else None
                type_category = row[4] if len(row) > 4 else None

            fcode = str(fcode or '').strip()
            if not fcode:
                continue

            state_map[fcode] = {
                'ftype': str(ftype).strip() if ftype not in (None, '') else None,
                'sub_type': str(sub_type).strip() if sub_type not in (None, '') else None,
                'type_code': str(type_code).strip() if type_code not in (None, '') else None,
                'type_category': str(type_category).strip() if type_category not in (None, '') else None,
            }
        return state_map

    @staticmethod
    def _is_numeric_ftype(ftype: Optional[str]) -> bool:
        normalized = str(ftype or '').strip()
        return bool(normalized) and normalized.isdigit()

    @classmethod
    def _should_preserve_existing_ftype(
        cls,
        existing_state: Optional[Dict[str, Optional[str]]],
        incoming_state: Dict[str, Optional[str]],
    ) -> bool:
        """避免 fundMNRank 的数字类型码回退已由详情链路写入的语义化类型文本。"""
        if not existing_state:
            return False

        existing_ftype = str(existing_state.get('ftype') or '').strip()
        incoming_ftype = str(incoming_state.get('ftype') or '').strip()
        if not existing_ftype or not incoming_ftype:
            return False
        if cls._is_numeric_ftype(existing_ftype):
            return False
        if not cls._is_numeric_ftype(incoming_ftype):
            return False

        existing_type_code = str(existing_state.get('type_code') or '').strip()
        incoming_type_code = str(incoming_state.get('type_code') or '').strip()
        if existing_type_code and incoming_type_code and existing_type_code != incoming_type_code:
            return False

        existing_type_category = str(existing_state.get('type_category') or '').strip()
        incoming_type_category = str(incoming_state.get('type_category') or '').strip()
        if existing_type_category and incoming_type_category and existing_type_category != incoming_type_category:
            return False

        return True

    @classmethod
    def _merge_existing_semantic_fields(
        cls,
        fund_data: Dict[str, Any],
        existing_state: Optional[Dict[str, Optional[str]]],
    ) -> Dict[str, Any]:
        """保护已有详情采集出的语义化字段，避免被列表接口低信息量字段覆盖。"""
        if not existing_state:
            return fund_data

        merged = fund_data.copy()
        if cls._should_preserve_existing_ftype(existing_state, merged):
            merged['ftype'] = existing_state.get('ftype')

        existing_has_specific_state = has_specific_structured_type_state(
            existing_state.get('ftype'),
            existing_state.get('type_code'),
            existing_state.get('type_category'),
        )
        low_confidence_incoming = (
            is_low_confidence_product_form_state(
                merged.get('ftype'),
                merged.get('type_code'),
            )
            or is_low_confidence_generic_numeric_state(
                merged.get('ftype'),
                merged.get('type_code'),
                merged.get('type_category'),
            )
        )

        if low_confidence_incoming and existing_has_specific_state:
            for key in ('ftype', 'sub_type', 'type_code', 'type_category'):
                existing_value = existing_state.get(key)
                if existing_value not in (None, ''):
                    merged[key] = existing_value
            return merged

        if not merged.get('sub_type') and existing_state.get('sub_type'):
            merged['sub_type'] = existing_state.get('sub_type')

        return merged

    def _save_funds(self, funds: List[dict], return_stats: bool = False):
        """
        保存基金列表到数据库

        分批提交（每500只一个事务），每批独立计数和异常处理，
        避免单一大事务回滚导致计数不准确。

        Args:
            funds: 基金列表
            return_stats: 是否返回详细统计信息

        Returns:
            保存的数量，或详细统计字典
        """
        deduplicated_funds, duplicate_count = self._deduplicate_fund_records(funds)
        if duplicate_count:
            logger.warning("批量落库前检测到重复基金代码，已自动合并: %s 条", duplicate_count)

        logger.info(f"开始保存{len(deduplicated_funds)}只基金到数据库...")

        saved_count = 0
        batch_size = 500
        save_stats: Dict[str, int] = {
            'saved_count': 0,
            'fund_updates': 0,
            'fund_inserts': 0,
            'returns_updates': 0,
            'returns_inserts': 0,
            'failed_records': 0,
        }

        # 分批提交，每批独立事务
        for batch_start in range(0, len(deduplicated_funds), batch_size):
            batch_funds = deduplicated_funds[batch_start:batch_start + batch_size]
            batch_saved = 0
            batch_failed = 0

            try:
                with self.db_manager.get_session() as session:
                    # 批量预取本批次已存在的主键集合，避免加载整 ORM 对象和逐字段 setattr
                    batch_fcodes_list = [f.get('fcode') for f in batch_funds if f.get('fcode')]
                    existing_fund_codes = self._extract_existing_fcodes(
                        session.query(Fund.fcode).filter(Fund.fcode.in_(batch_fcodes_list)).all()
                    )
                    existing_fund_state_map = self._build_existing_fund_state_map(
                        session.query(
                            Fund.fcode,
                            Fund.ftype,
                            Fund.sub_type,
                            Fund.type_code,
                            Fund.type_category,
                        ).filter(Fund.fcode.in_(batch_fcodes_list)).all()
                    )
                    existing_returns_codes = self._extract_existing_fcodes(
                        session.query(FundReturns.fcode).filter(FundReturns.fcode.in_(batch_fcodes_list)).all()
                    )
                    existing_returns_state_map = self._build_existing_returns_state_map(
                        session.query(
                            FundReturns.fcode,
                            FundReturns.latest_nav_date,
                            FundReturns.period_return_date,
                            FundReturns.period_return_observed_at,
                            FundReturns.period_return_rejection_json,
                            FundReturns.period_return_rejection_sha256,
                        ).filter(FundReturns.fcode.in_(batch_fcodes_list)).all()
                    )

                    fund_updates = []
                    fund_inserts = []
                    returns_updates = []
                    returns_inserts = []

                    for fund_data in batch_funds:
                        try:
                            fund_data = fund_data.copy()  # 避免原地修改传入参数
                            returns_data = fund_data.pop('returns', None)
                            fcode = fund_data['fcode']

                            # Validate before queuing either half of a row.
                            # NAV freshness and period-return freshness are
                            # independent; an older NAV must not suppress a
                            # newer, explicitly dated period snapshot.
                            if returns_data:
                                returns_data = select_return_update(
                                    existing_returns_state_map.get(fcode), returns_data,
                                )

                            if fcode in existing_fund_codes:
                                fund_data = self._merge_existing_semantic_fields(
                                    fund_data,
                                    existing_fund_state_map.get(fcode),
                                )
                                fund_update = self._compact_mapping(fund_data)
                                if len(fund_update) > 1:
                                    fund_updates.append(fund_update)
                            else:
                                if 'first_collected_at' not in fund_data:
                                    fund_data['first_collected_at'] = fund_data.get('last_updated_at') or datetime.now()
                                fund_inserts.append(Fund(**fund_data))

                            if returns_data:
                                if fcode in existing_returns_codes:
                                    if len(returns_data) > 1:
                                        returns_updates.append(returns_data)
                                elif returns_data:
                                    returns_inserts.append(FundReturns(**returns_data))

                            batch_saved += 1

                        except Exception as e:
                            logger.error(f"保存基金失败 {fund_data.get('fcode')}: {e}")
                            batch_failed += 1
                            continue

                    if fund_updates:
                        session.bulk_update_mappings(Fund, fund_updates)
                    if returns_updates:
                        session.bulk_update_mappings(FundReturns, returns_updates)
                    if fund_inserts:
                        session.add_all(fund_inserts)
                    if returns_inserts:
                        session.add_all(returns_inserts)

                saved_count += batch_saved
                save_stats['saved_count'] += batch_saved
                save_stats['fund_updates'] += len(fund_updates)
                save_stats['fund_inserts'] += len(fund_inserts)
                save_stats['returns_updates'] += len(returns_updates)
                save_stats['returns_inserts'] += len(returns_inserts)
                save_stats['failed_records'] += batch_failed
                for _ in range(batch_saved):
                    self.update_stats(success=True)
                for _ in range(batch_failed):
                    self.update_stats(success=False)

            except Exception as e:
                logger.error(f"批次提交失败 (batch {batch_start}-{batch_start + len(batch_funds)}): {e}")
                save_stats['failed_records'] += len(batch_funds)
                for _ in batch_funds:
                    self.update_stats(success=False)

        logger.info(f"基金列表保存完成: {saved_count}只")
        if return_stats:
            return save_stats
        return saved_count


# 向后兼容：BondFundCollector 别名
class BondFundCollector(FundCollector):
    """
    债券型基金列表采集器（向后兼容别名）

    这是 FundCollector 的别名类，默认采集债券型基金。
    建议直接使用 FundCollector。
    """

    def collect_all(self, fund_types=None, only_buyable=False):
        """
        采集债券型基金列表

        如果未指定fund_types，默认采集债券型[31, 32]
        """
        if fund_types is None:
            fund_types = [FundType.BOND, FundType.BOND_FIXED]

        return super().collect_all(fund_types=fund_types, only_buyable=only_buyable)
