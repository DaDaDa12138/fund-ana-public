#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据加载器模块 - DataLoader

负责从数据库加载基金数据，包括：
- 基金基础信息
- 收益率数据
- 历史净值数据

支持按基金类型加载（通用基金分析系统扩展）
"""

import json
import sqlite3
import pandas as pd
from datetime import date as date_type, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, List, Tuple, Union

import sys
from bond_fund_screening.config import (
    DB_PATH,
    TABLE_FUNDS,
    TABLE_RETURNS,
    TABLE_NAV_HISTORY,
    FIELD_MAPPING,
TRADING_DAYS_PER_YEAR,
)
from utils.bond_benchmark_classifier import infer_high_confidence_bond_semantic_ftype
from utils.calculation_capture import capture_call
from utils.fund_exclusion_rules import build_sql_exclusion_clause
from utils.period_return_snapshot import RETURN_PUBLIC_FIELDS, RETURN_VALIDATION_FIELDS, attach_period_return_status
from utils.negotiable_cd_policy import (
    build_hybrid_negotiable_cd_exclusion_sql,
)
from utils.runtime_entry_guards import (
    build_explicit_fof_name_sql,
    build_explicit_qdii_name_sql,
    build_hybrid_commodity_exclusion_sql,
)
from utils.runtime_query_guards import (
    apply_non_index_runtime_guard,
    collect_runtime_request_flags,
    compose_type_condition_with_runtime_guards,
)
from utils.runtime_ftype_resolver import (
    apply_runtime_ftype_overlays,
    normalize_bare_primary_ftype_labels,
    normalize_numeric_ftype_labels,
)
from bond_fund_screening.domain.runtime_semantics import attach_runtime_semantics_columns

UNIT_ACCUM_SCALE_MISMATCH_RATIO = 20.0
UNIT_ACCUM_SCALE_STABILITY_TOLERANCE = 0.2
RESOLVED_GAP_REGRESSION_TOLERANCE = 0.05
RESOLVED_GAP_END_TOLERANCE = 0.001
RESOLVED_TOTAL_RETURN_DIFF_TOLERANCE = 0.005

# 导入基金类型注册表
from database.nav_history_queries import NAV_HISTORY_FCODE_COUNT_SQL
try:
    from config.fund_type_registry import FundTypeCode, FundCategory, get_profile
except ImportError:
    # 向后兼容：如果注册表未安装，提供基本类型
    FundTypeCode = None
    FundCategory = None
    get_profile = None


class DataLoader:
    """
    数据加载器类

    提供从SQLite数据库加载基金数据的方法
    """

    def __init__(self, db_path: Optional[Path] = None):
        """
        初始化数据加载器

        Args:
            db_path: 数据库路径，默认使用配置文件中的路径
        """
        self.db_path = db_path or DB_PATH

        if not self.db_path.exists():
            raise FileNotFoundError(f"数据库文件不存在: {self.db_path}")

        self.conn = None
        self._table_columns_cache = {}
        self._sales_fee_validation_cache: Optional[Dict[str, str]] = None

    def connect(self):
        """建立数据库连接"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def close(self):
        """关闭数据库连接"""
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        self._table_columns_cache = {}
        self._sales_fee_validation_cache = None

    def _load_sales_fee_validation_classification_map(self) -> Dict[str, str]:
        """
        读取最新 sales_fee 重采验证审计分类。

        说明：
        - 仅作为筛选链路的语义辅助，不覆盖数据库原始数值；
        - 若本地不存在审计产物，则保持空映射，回退到原有严格缺失语义。
        """
        if self._sales_fee_validation_cache is not None:
            return dict(self._sales_fee_validation_cache)

        audit_dir = Path(__file__).resolve().parent.parent.parent / 'audit'
        candidates = sorted(audit_dir.glob('sales_fee_refetch_validation_audit_*.json'))
        if not candidates:
            self._sales_fee_validation_cache = {}
            return {}

        latest_path = candidates[-1]
        payload = json.loads(latest_path.read_text(encoding='utf-8'))
        mapping = {}
        for row in payload.get('rows') or []:
            fcode = str(row.get('fcode') or '').strip()
            classification = str(row.get('classification') or '').strip()
            if not fcode or not classification:
                continue
            mapping[fcode] = classification

        self._sales_fee_validation_cache = mapping
        return dict(mapping)

    def __enter__(self):
        """上下文管理器：进入"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器：退出"""
        self.close()

    @staticmethod
    def _has_reliable_accum_nav_window(nav_df: pd.DataFrame, *, regression_tolerance: float = 0.01) -> bool:
        """累计净值与单位净值关系异常回退时，不再继续信任累计净值。"""
        if nav_df is None or getattr(nav_df, "empty", True):
            return False
        if 'accum_nav' not in nav_df.columns:
            return False

        accum = pd.to_numeric(nav_df['accum_nav'], errors='coerce')
        if accum.isna().any() or (accum <= 0).any():
            return False

        if 'unit_nav' not in nav_df.columns:
            return True

        unit = pd.to_numeric(nav_df['unit_nav'], errors='coerce')
        if unit.isna().any() or (unit <= 0).any():
            return True

        if DataLoader._has_stable_unit_accum_scale_mismatch(unit, accum):
            return True

        dividend_gap = accum - unit
        if dividend_gap.isna().any() or (dividend_gap < -1e-6).any():
            return False

        max_regression = float((dividend_gap.cummax() - dividend_gap).max() or 0.0)
        if max_regression <= regression_tolerance:
            return True

        return DataLoader._has_resolved_small_gap_regression(
            unit,
            accum,
            dividend_gap=dividend_gap,
            max_regression=max_regression,
            regression_tolerance=regression_tolerance,
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

    @staticmethod
    def _can_use_daily_growth_window(nav_df: pd.DataFrame) -> bool:
        """仅在样本密度接近日频时才直接用 daily_growth 复利。"""
        if nav_df is None or getattr(nav_df, "empty", True):
            return False
        if 'daily_growth' not in nav_df.columns or 'nav_date' not in nav_df.columns:
            return False

        dg = pd.to_numeric(nav_df['daily_growth'], errors='coerce').iloc[1:].dropna()
        if len(dg) < 10:
            return False

        nav_dates = pd.to_datetime(nav_df['nav_date'], errors='coerce')
        if nav_dates.isna().any():
            return False

        span_days = max(int((nav_dates.iloc[-1] - nav_dates.iloc[0]).days), 1)
        actual_years = max(span_days / 365.25, 1 / 365.25)
        estimated_points_per_year = len(dg) / actual_years
        minimum_daily_like_points = min(float(TRADING_DAYS_PER_YEAR), 180.0)
        return estimated_points_per_year >= minimum_daily_like_points

    @staticmethod
    def _is_hybrid_fund_type_request(
        fund_type: Optional[Union[str, 'FundTypeCode', List]],
    ) -> bool:
        """判断本次查询是否包含混合型筛选请求。"""
        targets = fund_type if isinstance(fund_type, list) else [fund_type]
        for target in targets:
            if target is None:
                continue
            if isinstance(target, str) and target.lower().strip() == 'hybrid':
                return True
            if hasattr(target, 'name') and str(target.name).upper() == 'HYBRID':
                return True
            if hasattr(target, 'value') and str(target.value).strip() == '27':
                return True
        return False

    @staticmethod
    def _is_qdii_fund_type_request(
        fund_type: Optional[Union[str, 'FundTypeCode', List]],
    ) -> bool:
        """判断本次查询是否包含 QDII 筛选请求。"""
        targets = fund_type if isinstance(fund_type, list) else [fund_type]
        for target in targets:
            if target is None:
                continue
            if isinstance(target, str) and target.lower().strip() == 'qdii':
                return True
            if hasattr(target, 'name') and str(target.name).upper() == 'QDII':
                return True
            if hasattr(target, 'value') and str(target.value).strip() == '36':
                return True
        return False

    def _build_hybrid_commodity_exclusion_condition(
        self,
        table_alias: str = "f",
    ) -> Tuple[str, tuple]:
        """
        构建“商品/贵金属/期货联接基金”混入混合池时的排除条件。

        背景：
        - fundMNRank 的 27=混合型入口会混入部分商品/贵金属/期货联接产品；
        - 这批基金在本地库里长期停留为 `ftype='002' + type_code='27'`，
          会被主链错误翻译为 `混合型-未分类`，进而污染混合型筛选 Top。

        约束：
        - 只对历史低信息量状态 `ftype='002'` 生效，避免误伤已经语义化分类的基金；
        - 名称命中优先使用高置信商品联接关键词；
        - benchmark 仅在缺少明显股债宽基信号时作为补充兜底，避免误杀含少量黄金暴露的普通混合基金。
        """
        fund_columns = self._get_table_columns(TABLE_FUNDS)
        return build_hybrid_commodity_exclusion_sql(
            table_alias=table_alias,
            available_columns=fund_columns,
        )

    def _build_hybrid_negotiable_cd_exclusion_condition(
        self,
        table_alias: str = "f",
    ) -> Tuple[str, tuple]:
        """
        构建“同业存单/存单持有类产品误入混合池”的高置信排除条件。

        背景：
        - 部分同业存单指数与存单持有类产品在本地库中长期停留为 `ftype='002' + type_code='27'`；
        - 它们会被 hybrid 入口误收，短周期下呈现 0 回撤、极低波动、极高 Sortino 等失真表现；
        - 这些产品本质上更接近现金替代/固收工具，不属于当前混合型基金筛选的目标池。

        约束：
        - 名称命中 `同业存单` 视为高置信信号，可覆盖极少数已被语义化但仍属同业存单的样本；
        - benchmark 仅作为补充，要求同时命中 `同业存单` 与 `AAA`，且只对 `ftype='002'` 生效，
          避免扩大误杀范围。
        """
        fund_columns = self._get_table_columns(TABLE_FUNDS)
        return build_hybrid_negotiable_cd_exclusion_sql(
            table_alias=table_alias,
            available_columns=fund_columns,
        )

    def _build_explicit_fof_name_condition(
        self,
        table_alias: str = "f",
    ) -> Tuple[str, tuple]:
        """
        构建“显式 FOF 名称但仍停留在数值混合型状态”的高置信条件。

        背景：
        - 少量新基金在本地库里仍停留为 `ftype='002' + type_code='27'`；
        - 但名称已经明确带 `(FOF)`，live detail 也会返回 `FOF-*`；
        - 若继续进入 hybrid 分析器，会造成跨池错分。

        用途：
        - hybrid 查询时将其排除，避免跨池污染；
        - fof 查询时将其补充纳入，避免基金被完全漏掉。
        """
        fund_columns = self._get_table_columns(TABLE_FUNDS)
        return build_explicit_fof_name_sql(
            table_alias=table_alias,
            available_columns=fund_columns,
        )

    def _build_explicit_qdii_name_condition(
        self,
        table_alias: str = "f",
    ) -> Tuple[str, tuple]:
        """
        构建“显式 QDII 名称但仍停留在 ETF/LOF 产品形态码”的高置信条件。

        背景：
        - 少量 `QDII-LOF / QDII-FOF-LOF` 主代码在本地库里仍停留为 `type_code=3/4`；
        - 运行时只按 `type_code=36` 取数会导致这批基金完全漏出 qdii 分析器；
        - 当前先只在 qdii 查询时补入，不改变 lof / etf 产品形态视图。
        """
        fund_columns = self._get_table_columns(TABLE_FUNDS)
        return build_explicit_qdii_name_sql(
            table_alias=table_alias,
            available_columns=fund_columns,
        )

    def _get_hybrid_commodity_exclusion_observability(self, cursor) -> Dict[str, Union[int, List[Dict[str, str]]]]:
        """读取混合池商品联接排除口径的只读统计，供正式审计产物复用。"""
        condition, params = self._build_hybrid_commodity_exclusion_condition("f")
        if not condition:
            return {
                'excluded_high_confidence_commodity_candidates': 0,
                'excluded_high_confidence_commodity_candidate_samples': [],
            }

        count_sql = f"""
            SELECT COUNT(*)
            FROM {TABLE_FUNDS} f
            WHERE f.type_code = '27'
              AND f.type_category = 'hybrid'
              AND ({condition})
        """
        cursor.execute(count_sql, params)
        excluded_count = int(cursor.fetchone()[0] or 0)

        sample_sql = f"""
            SELECT f.fcode, f.shortname
            FROM {TABLE_FUNDS} f
            WHERE f.type_code = '27'
              AND f.type_category = 'hybrid'
              AND ({condition})
            ORDER BY f.shortname ASC, f.fcode ASC
            LIMIT 5
        """
        cursor.execute(sample_sql, params)
        sample_rows = [
            {
                'fcode': str(row[0]).strip(),
                'shortname': str(row[1]).strip() if row[1] not in (None, '') else '',
            }
            for row in cursor.fetchall()
        ]
        return {
            'excluded_high_confidence_commodity_candidates': excluded_count,
            'excluded_high_confidence_commodity_candidate_samples': sample_rows,
        }

    def _get_hybrid_explicit_fof_exclusion_observability(self, cursor) -> Dict[str, Union[int, List[Dict[str, str]]]]:
        """读取混合池显式 FOF 排除口径的只读统计。"""
        condition, params = self._build_explicit_fof_name_condition("f")
        if not condition:
            return {
                'excluded_explicit_fof_candidates': 0,
                'excluded_explicit_fof_candidate_samples': [],
            }

        count_sql = f"""
            SELECT COUNT(*)
            FROM {TABLE_FUNDS} f
            WHERE f.type_code = '27'
              AND f.type_category = 'hybrid'
              AND ({condition})
        """
        cursor.execute(count_sql, params)
        excluded_count = int(cursor.fetchone()[0] or 0)

        sample_sql = f"""
            SELECT f.fcode, f.shortname
            FROM {TABLE_FUNDS} f
            WHERE f.type_code = '27'
              AND f.type_category = 'hybrid'
              AND ({condition})
            ORDER BY f.shortname ASC, f.fcode ASC
            LIMIT 5
        """
        cursor.execute(sample_sql, params)
        sample_rows = [
            {
                'fcode': str(row[0]).strip(),
                'shortname': str(row[1]).strip() if row[1] not in (None, '') else '',
            }
            for row in cursor.fetchall()
        ]
        return {
            'excluded_explicit_fof_candidates': excluded_count,
            'excluded_explicit_fof_candidate_samples': sample_rows,
        }

    def _get_hybrid_negotiable_cd_exclusion_observability(self, cursor) -> Dict[str, Union[int, List[Dict[str, str]]]]:
        """读取混合池同业存单排除口径的只读统计。"""
        condition, params = self._build_hybrid_negotiable_cd_exclusion_condition("f")
        if not condition:
            return {
                'excluded_high_confidence_negotiable_cd_candidates': 0,
                'excluded_high_confidence_negotiable_cd_candidate_samples': [],
            }

        count_sql = f"""
            SELECT COUNT(*)
            FROM {TABLE_FUNDS} f
            WHERE f.type_code = '27'
              AND f.type_category = 'hybrid'
              AND ({condition})
        """
        cursor.execute(count_sql, params)
        excluded_count = int(cursor.fetchone()[0] or 0)

        sample_sql = f"""
            SELECT f.fcode, f.shortname
            FROM {TABLE_FUNDS} f
            WHERE f.type_code = '27'
              AND f.type_category = 'hybrid'
              AND ({condition})
            ORDER BY f.shortname ASC, f.fcode ASC
            LIMIT 5
        """
        cursor.execute(sample_sql, params)
        sample_rows = [
            {
                'fcode': str(row[0]).strip(),
                'shortname': str(row[1]).strip() if row[1] not in (None, '') else '',
            }
            for row in cursor.fetchall()
        ]
        return {
            'excluded_high_confidence_negotiable_cd_candidates': excluded_count,
            'excluded_high_confidence_negotiable_cd_candidate_samples': sample_rows,
        }

    def _build_type_condition(
        self,
        fund_type: Optional[Union[str, 'FundTypeCode', List]],
        fund_type_pattern: str
    ) -> Tuple[str, tuple]:
        """
        构建基金类型筛选条件

        Args:
            fund_type: 基金类型参数
            fund_type_pattern: 类型匹配模式（向后兼容）

        Returns:
            (SQL条件字符串, 参数元组)
        """
        exclusion_clause, exclusion_params = build_sql_exclusion_clause("f.fcode")

        if fund_type is None:
            # 向后兼容：使用 fund_type_pattern
            condition = "(f.ftype LIKE ? OR f.ftype IN ('003', '008'))"
            params = (fund_type_pattern,)
            if exclusion_clause:
                condition = f"({condition}) AND {exclusion_clause}"
                params += exclusion_params
            return condition, params

        # 处理新的 fund_type 参数
        type_codes = []

        if isinstance(fund_type, list):
            for t in fund_type:
                codes = self._resolve_fund_type(t)
                type_codes.extend(codes)
        else:
            type_codes = self._resolve_fund_type(fund_type)

        if not type_codes:
            # 默认债券型
            type_codes = ['31', '32']

        # 构建 IN 条件
        placeholders = ','.join(['?' for _ in type_codes])
        condition = f"f.type_code IN ({placeholders})"

        flags = collect_runtime_request_flags(fund_type)
        if flags["is_non_index"]:
            condition = apply_non_index_runtime_guard(
                condition,
                includes_bond_request=flags["includes_bond_request"],
            )

        explicit_fof_guard = self._build_explicit_fof_name_condition("f")
        explicit_qdii_guard = self._build_explicit_qdii_name_condition("f")
        hybrid_commodity_guard = self._build_hybrid_commodity_exclusion_condition("f")
        hybrid_negotiable_cd_guard = self._build_hybrid_negotiable_cd_exclusion_condition("f")

        condition, extra_params = compose_type_condition_with_runtime_guards(
            base_condition=condition,
            flags=flags,
            explicit_fof_guard=explicit_fof_guard,
            explicit_qdii_guard=explicit_qdii_guard,
            hybrid_commodity_guard=hybrid_commodity_guard,
            hybrid_negotiable_cd_guard=hybrid_negotiable_cd_guard,
        )

        params = tuple(type_codes) + extra_params
        if exclusion_clause:
            condition = f"({condition}) AND {exclusion_clause}"
            params += exclusion_params

        return condition, params

    def _resolve_fund_type(self, fund_type: Union[str, 'FundTypeCode']) -> List[str]:
        """
        解析基金类型为API代码列表

        Args:
            fund_type: 基金类型

        Returns:
            API代码列表
        """
        # 使用duck typing检查是否有value属性（兼容不同模块实例的枚举）
        if hasattr(fund_type, 'value'):
            return [str(fund_type.value)]

        if isinstance(fund_type, str):
            # 尝试作为别名解析
            alias_map = {
                'bond': ['31', '32'],
                'stock': ['25'],
                'index': ['26'],
                'hybrid': ['27'],
                'money': ['35'],
                'qdii': ['36'],
                'fof': ['37'],
                'etf': ['3'],
                'lof': ['4'],
            }
            alias = fund_type.lower().strip()
            if alias in alias_map:
                return alias_map[alias]

            # 尝试作为直接代码
            if fund_type in ['25', '26', '27', '31', '32', '35', '36', '37', '3', '4']:
                return [fund_type]

        return []

    def _get_table_columns(self, table_name: str) -> set:
        """获取指定表的列名集合，并做进程内缓存。"""
        self.connect()
        cached = self._table_columns_cache.get(table_name)
        if cached is not None:
            return cached

        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = {str(row[1]).strip() for row in rows if len(row) > 1 and row[1]}
        self._table_columns_cache[table_name] = columns
        return columns

    @staticmethod
    def _split_manager_tokens(value) -> List[str]:
        """将逗号分隔的经理 ID / 姓名字符串拆成顺序去重列表。"""
        normalized = str(value or '').replace('，', ',').strip()
        if not normalized:
            return []

        tokens = []
        seen = set()
        for raw in normalized.split(','):
            token = raw.strip()
            if not token or token in seen:
                continue
            seen.add(token)
            tokens.append(token)
        return tokens

    def _hydrate_manager_names_from_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        使用本地 fund_managers 表回填缺失/残缺的 manager_names。

        只在以下场景补显，避免覆盖已有正常文本：
        1. manager_names 为空
        2. manager_ids 为多经理，但 manager_names 仍只有单个姓名
        """
        if len(df) == 0 or 'manager_ids' not in df.columns or 'manager_names' not in df.columns:
            return df

        manager_table_columns = self._get_table_columns('fund_managers')
        if 'mgr_id' not in manager_table_columns or 'name' not in manager_table_columns:
            return df

        manager_ids_series = df['manager_ids'].fillna('').astype(str)
        manager_names_series = df['manager_names'].fillna('').astype(str)

        target_mask = manager_ids_series.str.strip().ne('') & (
            manager_names_series.str.strip().eq('')
            | (
                manager_ids_series.str.contains(',', regex=False)
                & ~manager_names_series.str.contains(r',|，', regex=True)
            )
        )
        if not target_mask.any():
            return df

        unique_manager_ids = []
        seen_manager_ids = set()
        for manager_ids in manager_ids_series[target_mask].tolist():
            for manager_id in self._split_manager_tokens(manager_ids):
                if manager_id in seen_manager_ids:
                    continue
                seen_manager_ids.add(manager_id)
                unique_manager_ids.append(manager_id)

        if not unique_manager_ids:
            return df

        placeholders = ','.join(['?' for _ in unique_manager_ids])
        manager_name_query = f"""
            SELECT mgr_id, name
            FROM fund_managers
            WHERE mgr_id IN ({placeholders})
              AND name IS NOT NULL
              AND TRIM(name) != ''
        """
        manager_name_rows = self.conn.execute(manager_name_query, unique_manager_ids).fetchall()
        manager_name_map = {
            str(row[0]).strip(): str(row[1]).strip()
            for row in manager_name_rows
            if len(row) >= 2 and row[0] and row[1]
        }
        if not manager_name_map:
            return df

        for idx in df.index[target_mask]:
            manager_ids = self._split_manager_tokens(df.at[idx, 'manager_ids'])
            if not manager_ids:
                continue

            resolved_names = [manager_name_map.get(manager_id) for manager_id in manager_ids]
            if not all(resolved_names):
                continue

            current_names = str(df.at[idx, 'manager_names'] or '').strip()
            if not current_names or (len(manager_ids) > 1 and '，' not in current_names and ',' not in current_names):
                df.at[idx, 'manager_names'] = ','.join(resolved_names)

        return df

    @capture_call('screening.fund_rows', attributes=('db_path',))
    def load_funds_data(
        self,
        period_years: Optional[int] = None,
        period_days: Optional[int] = None,
        fund_type_pattern: str = "债券%",
        fund_type: Optional[Union[str, 'FundTypeCode', List]] = None,
        as_of_date: Optional[date_type] = None
    ) -> pd.DataFrame:
        """
        加载基金数据（关联基础信息和收益率）

        Args:
            period_years: 分析周期（年），与period_days二选一
            period_days: 分析周期（天），与period_years二选一，优先使用
            fund_type_pattern: 基金类型匹配模式，默认"债券%"（向后兼容）
            fund_type: 基金类型，支持以下格式：
                - str: 'bond', 'stock', 'hybrid' 等别名
                - FundTypeCode: 枚举值
                - List: 多个类型的列表
                如果指定此参数，将忽略 fund_type_pattern
            as_of_date: 回测截断日期（date对象），默认None=今天。
                用于回测时将 establish_days 基于此日期计算。

        Returns:
            包含基金完整信息的DataFrame
        """
        self.connect()

        # 参数验证
        if period_days is None and period_years is None:
            raise ValueError("必须提供period_years或period_days参数")

        # 构建类型筛选条件
        type_condition, type_params = self._build_type_condition(fund_type, fund_type_pattern)
        fund_columns = self._get_table_columns(TABLE_FUNDS)
        fullname_select = "f.fullname" if "fullname" in fund_columns else "NULL AS fullname"
        benchmark_select = "f.benchmark" if "benchmark" in fund_columns else "NULL AS benchmark"
        holding_source_select = (
            "f.holding_period_source"
            if "holding_period_source" in fund_columns
            else "NULL AS holding_period_source"
        )
        manager_ids_select = "f.manager_ids" if "manager_ids" in fund_columns else "'' AS manager_ids"
        sales_fee_select = "f.sales_fee_rate" if "sales_fee_rate" in fund_columns else "NULL AS sales_fee_rate"
        return_columns = self._get_table_columns(TABLE_RETURNS)
        provenance_select = ", ".join(
            f"r.{field}" if field in return_columns else f"NULL AS {field}"
            for field in (*RETURN_PUBLIC_FIELDS, *RETURN_VALIDATION_FIELDS)
        )

        # 构建SQL查询（关联funds和fund_returns表）
        # as_of_date 支持：回测时用指定日期替代 'now' 计算 establish_days
        date_ref = f"'{as_of_date.isoformat()}'" if as_of_date else "'now'"
        query = f"""
            SELECT
                f.fcode,
                f.shortname,
                {fullname_select},
                f.ftype,
                f.type_code,
                f.type_category,
                f.estab_date,
                CAST(JULIANDAY({date_ref}) - JULIANDAY(f.estab_date) AS INTEGER) as establish_days,
                f.scale_billion,
                f.has_holding_period,
                f.holding_days,
                f.holding_keywords,
                {holding_source_select},
                f.manager_names,
                {manager_ids_select},
                f.rating,
                r.return_1w,
                r.return_1m,
                r.return_3m,
                r.return_6m,
                r.return_1y,
                r.return_2y,
                r.return_3y,
                r.return_5y,
                r.return_ytd,
                r.return_since,
                r.latest_nav,
                r.latest_nav_date,
                {provenance_select},
                f.mgr_fee_rate,
                f.trust_fee_rate,
                {sales_fee_select},
                {benchmark_select}
            FROM {TABLE_FUNDS} f
            LEFT JOIN {TABLE_RETURNS} r ON f.fcode = r.fcode
            WHERE {type_condition}
        """

        # 执行查询
        df = pd.read_sql_query(query, self.conn, params=type_params)
        df = attach_period_return_status(df, mask_unknown=True)

        # 根据周期选择主收益率字段
        # 优先使用period_days
        if period_days is not None:
            # 短期周期（按天数）
            if period_days <= 7:
                df['main_return'] = df['return_1w']
            elif period_days <= 30:
                df['main_return'] = df['return_1m']
            elif period_days <= 90:
                df['main_return'] = df['return_3m']
            elif period_days <= 180:
                df['main_return'] = df['return_6m']
            elif period_days <= 365:
                df['main_return'] = df['return_1y']
            elif period_days <= 730:
                df['main_return'] = df['return_2y']
            elif period_days <= 1095:
                df['main_return'] = df['return_3y']
            elif period_days <= 1825:
                df['main_return'] = df['return_5y']
            else:
                # 超长期，需要从历史净值计算
                df['main_return'] = None
        elif period_years is not None:
            # 长期周期（按年数）
            if period_years == 1:
                df['main_return'] = df['return_1y']
            elif period_years == 2:
                df['main_return'] = df['return_2y']
            elif period_years == 3:
                df['main_return'] = df['return_3y']
            elif period_years == 4:
                # A real four-year window cannot be inferred from 3Y/5Y returns.
                df['main_return'] = self._batch_four_year_returns(
                    df['fcode'], end_date=as_of_date.isoformat() if as_of_date else None,
                )
                df['main_return_mapped_from'] = 'verified_daily_growth_4y'
            elif period_years == 5:
                df['main_return'] = df['return_5y']
            else:
                raise ValueError(f"不支持的周期: {period_years}年")

        # 数据类型转换
        df['establish_days'] = pd.to_numeric(df['establish_days'], errors='coerce')
        df['scale_billion'] = pd.to_numeric(df['scale_billion'], errors='coerce')
        df['has_holding_period'] = pd.to_numeric(df['has_holding_period'], errors='coerce').fillna(0).astype(int)
        # NULL means the contractual holding period was not verified.  Keep it
        # missing so the scoring engine cannot award the verified zero-day
        # liquidity score; publication gates still decide whether the row may
        # proceed to an official list.
        df['holding_days'] = pd.to_numeric(df['holding_days'], errors='coerce')

        # v1.9.60 P1-4: 费率字段处理
        df['mgr_fee_rate'] = pd.to_numeric(df['mgr_fee_rate'], errors='coerce')
        df['trust_fee_rate'] = pd.to_numeric(df['trust_fee_rate'], errors='coerce')
        df['sales_fee_rate'] = pd.to_numeric(df['sales_fee_rate'], errors='coerce')

        # 生产库若已提供销售服务费，则 total_fee_rate 只表示“精确持续持有成本”；
        # 对官方明确给出 `---` 占位值的样本，不伪造 total_fee_rate，而是在筛选层额外提供
        # `fee_screening_rate` 作为保守下界（管理费+托管费），避免把未知值伪装成精确值。
        has_sales_fee_column = 'sales_fee_rate' in fund_columns
        core_fee_ready_mask = df['mgr_fee_rate'].notna() & df['trust_fee_rate'].notna()
        df['core_fee_rate'] = float('nan')
        df.loc[core_fee_ready_mask, 'core_fee_rate'] = (
            df.loc[core_fee_ready_mask, 'mgr_fee_rate'] + df.loc[core_fee_ready_mask, 'trust_fee_rate']
        )

        df['sales_fee_source_status'] = 'core_fee_missing'
        df['fee_screening_rate'] = float('nan')
        df['fee_screening_uses_floor'] = False

        if not has_sales_fee_column:
            df.loc[core_fee_ready_mask, 'sales_fee_source_status'] = 'legacy_schema_without_sales_fee'
            df['total_fee_rate'] = df['core_fee_rate']
            df['fee_screening_rate'] = df['core_fee_rate']
        else:
            df['total_fee_rate'] = float('nan')
            exact_fee_mask = core_fee_ready_mask & df['sales_fee_rate'].notna()
            df.loc[exact_fee_mask, 'sales_fee_source_status'] = 'exact'
            df.loc[exact_fee_mask, 'total_fee_rate'] = (
                df.loc[exact_fee_mask, 'core_fee_rate'] + df.loc[exact_fee_mask, 'sales_fee_rate']
            )
            df.loc[exact_fee_mask, 'fee_screening_rate'] = df.loc[exact_fee_mask, 'total_fee_rate']

            validation_map = self._load_sales_fee_validation_classification_map()
            official_placeholder_codes = {
                code for code, classification in validation_map.items()
                if classification == 'official_placeholder_confirmed'
            }
            shadow_code_candidates = {
                code for code, classification in validation_map.items()
                if classification == 'shadow_code_candidate'
            }

            missing_sales_mask = core_fee_ready_mask & df['sales_fee_rate'].isna()
            official_placeholder_mask = missing_sales_mask & df['fcode'].isin(official_placeholder_codes)
            shadow_code_mask = missing_sales_mask & df['fcode'].isin(shadow_code_candidates)
            unresolved_missing_mask = missing_sales_mask & ~official_placeholder_mask & ~shadow_code_mask

            df.loc[official_placeholder_mask, 'sales_fee_source_status'] = 'official_placeholder_confirmed'
            df.loc[official_placeholder_mask, 'fee_screening_rate'] = df.loc[official_placeholder_mask, 'core_fee_rate']
            df.loc[official_placeholder_mask, 'fee_screening_uses_floor'] = True

            df.loc[shadow_code_mask, 'sales_fee_source_status'] = 'shadow_code_candidate'
            df.loc[unresolved_missing_mask, 'sales_fee_source_status'] = 'sales_fee_missing_unresolved'

        # 填充空值
        df['holding_keywords'] = df['holding_keywords'].fillna('')
        df['manager_names'] = df['manager_names'].fillna('')
        if 'manager_ids' in df.columns:
            df['manager_ids'] = df['manager_ids'].fillna('')
            df = self._hydrate_manager_names_from_ids(df)

        # v1.9.56 P0-3: ftype 数字编码规范化为中文名称
        # 数据库中存在 ftype 为 001/002/003/005/007 等数字编码的基金
        from bond_fund_screening.config import FUND_TYPE_MAPPING
        df = normalize_numeric_ftype_labels(df, FUND_TYPE_MAPPING)
        df = normalize_bare_primary_ftype_labels(df)
        df = apply_runtime_ftype_overlays(
            df,
            is_qdii_request=self._is_qdii_fund_type_request(fund_type),
        )
        df = attach_runtime_semantics_columns(df)

        # Step 5B: 合并净值新鲜度列
        df = self._merge_freshness_columns(df, as_of_date=as_of_date)

        return df

    def _merge_freshness_columns(self, df: pd.DataFrame, as_of_date: Optional[date_type] = None) -> pd.DataFrame:
        """
        合并净值新鲜度列到 DataFrame（Step 5B）

        Args:
            df: 基金数据 DataFrame
            as_of_date: 回测截断日期，默认None=今天

        Returns:
            添加了新鲜度列的 DataFrame
        """
        from datetime import date
        from bond_fund_screening.config import FRESHNESS_CONFIG

        if len(df) == 0:
            return df

        # 获取所有基金的最新净值日期
        fcodes = df['fcode'].tolist()
        nav_dates_df = self.get_fund_nav_latest_dates(fcodes)

        # 合并到主 DataFrame（左连接，保留所有基金）
        df = df.merge(nav_dates_df, on='fcode', how='left')

        # 计算新鲜度标记（使用 as_of_date 或今天）
        today = as_of_date or date.today()
        freshness_flags = compute_nav_freshness_flags(
            latest_dates=df['nav_latest_date'],
            today=today,
            max_age_days=FRESHNESS_CONFIG['max_nav_age_days'],
            skip_weekends=FRESHNESS_CONFIG['skip_weekends'],
            shadow_thresholds=FRESHNESS_CONFIG['shadow_max_nav_age_days']
        )

        # 合并新鲜度列
        for col in freshness_flags.columns:
            df[col] = freshness_flags[col].values

        return df

    @capture_call('screening.nav_query_single', attributes=('db_path',))
    def get_fund_nav_history(
        self,
        fcode: str,
        days: int,
        end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        获取单只基金的历史净值数据

        Args:
            fcode: 基金代码
            days: 需要获取的天数（从end_date往前推）
            end_date: 结束日期（YYYY-MM-DD），默认为今天

        Returns:
            包含历史净值的DataFrame，列：nav_date, unit_nav, daily_growth
        """
        self.connect()

        if end_date is None:
            end_date = datetime.now().date().isoformat()

        # 计算起始日期
        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)
        start_date = start_dt.date().isoformat()

        query = f"""
            SELECT
                nav_date,
                unit_nav,
                accum_nav,
                daily_growth
            FROM {TABLE_NAV_HISTORY}
            WHERE fcode = ?
              AND nav_date >= ?
              AND nav_date <= ?
            ORDER BY nav_date ASC
        """

        df = pd.read_sql_query(
            query,
            self.conn,
            params=(fcode, start_date, end_date)
        )

        # 数据类型转换
        df['nav_date'] = pd.to_datetime(df['nav_date'])
        df['unit_nav'] = pd.to_numeric(df['unit_nav'], errors='coerce')
        df['accum_nav'] = pd.to_numeric(df['accum_nav'], errors='coerce')
        df['daily_growth'] = pd.to_numeric(df['daily_growth'], errors='coerce')

        return df

    @capture_call('screening.nav_query_batch', attributes=('db_path',))
    def get_batch_nav_history(
        self,
        fcodes: List[str],
        days: int,
        end_date: Optional[str] = None
    ) -> dict:
        """
        批量获取多只基金的历史净值数据

        Args:
            fcodes: 基金代码列表
            days: 需要获取的天数
            end_date: 结束日期，默认为今天

        Returns:
            字典 {fcode: DataFrame}
        """
        self.connect()

        # 空列表守卫：避免生成无效SQL `WHERE fcode IN ()`
        if not fcodes:
            return {}

        if end_date is None:
            end_date = datetime.now().date().isoformat()

        # 计算起始日期
        end_dt = datetime.fromisoformat(end_date)
        start_dt = end_dt - timedelta(days=days)
        start_date = start_dt.date().isoformat()

        # 批量查询（使用IN子句）
        placeholders = ','.join(['?' for _ in fcodes])
        query = f"""
            SELECT
                fcode,
                nav_date,
                unit_nav,
                accum_nav,
                daily_growth
            FROM {TABLE_NAV_HISTORY}
            WHERE fcode IN ({placeholders})
              AND nav_date >= ?
              AND nav_date <= ?
            ORDER BY fcode, nav_date ASC
        """

        params = fcodes + [start_date, end_date]
        df = pd.read_sql_query(query, self.conn, params=params)

        # 数据类型转换
        df['nav_date'] = pd.to_datetime(df['nav_date'])
        df['unit_nav'] = pd.to_numeric(df['unit_nav'], errors='coerce')
        df['accum_nav'] = pd.to_numeric(df['accum_nav'], errors='coerce')
        df['daily_growth'] = pd.to_numeric(df['daily_growth'], errors='coerce')

        # 查询结果已按 fcode, nav_date 排序，按连续区间切片比 groupby 更轻量
        base_df = df.drop(columns=['fcode'])
        empty_template = base_df.iloc[0:0].copy()
        grouped = {}
        if not df.empty:
            fcode_values = df['fcode'].to_numpy()
            start_idx = 0
            current_fcode = fcode_values[0]

            for idx in range(1, len(fcode_values) + 1):
                if idx == len(fcode_values) or fcode_values[idx] != current_fcode:
                    grouped[current_fcode] = base_df.iloc[start_idx:idx].copy()
                    if idx < len(fcode_values):
                        current_fcode = fcode_values[idx]
                        start_idx = idx

        result = {
            fcode: grouped.get(fcode, empty_template.copy())
            for fcode in fcodes
        }

        return result

    def _batch_four_year_returns(self, codes: pd.Series, end_date: Optional[str] = None) -> pd.Series:
        """Calendar-aligned 4Y total return from complete observed growth only.

        No NAV-price fallback: accumulated NAV is not a dividend-reinvestment
        wealth index. Missing or sparse history stays missing, never extrapolated.
        """
        import numpy as np
        from bond_fund_screening.core.trading_calendar import continuity_evidence

        results = {}
        values = list(dict.fromkeys(codes.astype(str)))
        for offset in range(0, len(values), 200):
            batch = values[offset:offset + 200]
            histories = self.get_batch_nav_history(batch, 1492, end_date=end_date)
            for code, frame in histories.items():
                results[code] = None
                if frame.empty or len(frame) < 2:
                    continue
                frame = frame.sort_values('nav_date').reset_index(drop=True)
                dates = pd.to_datetime(frame['nav_date'], errors='coerce')
                if dates.isna().any() or dates.duplicated().any():
                    continue
                target = dates.iloc[-1] - pd.DateOffset(years=4)
                before = frame.loc[dates.le(target)]
                if before.empty:
                    continue
                start = before.index[-1]
                if (target - dates.iloc[start]).days > 7:
                    continue
                window = frame.iloc[start:]
                span = (dates.iloc[-1] - dates.iloc[start]).days / 365.25
                growth = (
                    pd.to_numeric(window['daily_growth'].iloc[1:], errors='coerce')
                    if 'daily_growth' in window
                    else pd.Series(dtype=float)
                )
                if (span <= 0 or abs(span - 4.0) > 7.0 / 365.25
                        or len(window) - 1 < 180 * span
                        or not continuity_evidence(window['nav_date'])['verified']):
                    continue
                if np.isfinite(growth).all() and not growth.empty and not growth.le(-100).any():
                    result = float(np.expm1(np.log1p(growth.to_numpy() / 100).sum()) * 100)
                else:
                    continue
                if np.isfinite(result):
                    results[code] = result
        return codes.astype(str).map(results).astype(float)

    def calculate_period_return(
        self,
        fcode: str,
        days: int,
        end_date: Optional[str] = None
    ) -> Optional[float]:
        """
        计算基金指定周期的收益率（从历史净值计算）

        优先使用 daily_growth 复利累积，回退用 accum_nav，最后用 unit_nav。
        与 RiskCalculator 保持一致的净值口径。

        Args:
            fcode: 基金代码
            days: 周期天数
            end_date: 结束日期（YYYY-MM-DD），默认为今天。回测时传入截断日期。

        Returns:
            收益率（百分比），如果数据不足则返回None
        """
        nav_df = self.get_fund_nav_history(fcode, days + 30, end_date=end_date)  # 多取30天作为缓冲

        if len(nav_df) < 2:
            return None

        # 找到最接近N天前的日期（优先选择 target_date 之前的最近日期）
        latest_row = nav_df.iloc[-1]
        target_date = latest_row['nav_date'] - timedelta(days=days)

        # 优先选 target_date 之前（含）的最近日期
        before_target = nav_df[nav_df['nav_date'] <= target_date]
        if len(before_target) > 0:
            closest_idx = before_target['nav_date'].idxmax()
        else:
            # 无早于 target_date 的数据时，选最早可用的
            closest_idx = nav_df['nav_date'].idxmin()

        start_row = nav_df.loc[closest_idx]

        # 方案1: 优先使用 daily_growth 复利累积（最准确，消除分红除息影响）
        period_df = nav_df.loc[closest_idx:]
        if self._can_use_daily_growth_window(period_df):
            # 先按位置跳过起始日（该涨幅相对前一天，不属于分析区间），再清理NaN
            # 注意：不能先 dropna 再 iloc[1:]，否则起始日为NaN时会多跳一个有效数据点
            dg = period_df['daily_growth'].iloc[1:]
            dg = dg.dropna()
            if len(dg) >= 2:
                # daily_growth 是百分比形式，转小数后复利累积
                cumulative = (1 + dg / 100).prod() - 1
                return round(cumulative * 100, 2)

        # 方案2: 累计净值比（并非分红再投资总回报）。
        if 'accum_nav' in nav_df.columns and self._has_reliable_accum_nav_window(period_df):
            latest_accum = latest_row.get('accum_nav')
            start_accum = start_row.get('accum_nav')
            if pd.notna(latest_accum) and pd.notna(start_accum) and start_accum > 0:
                period_return = ((latest_accum / start_accum) - 1) * 100
                return round(period_return, 2)

        # 方案3: 最后回退用 unit_nav
        if 'unit_nav' not in nav_df:
            return None
        latest_nav = latest_row['unit_nav']
        start_nav = start_row['unit_nav']

        if pd.isna(latest_nav) or pd.isna(start_nav) or start_nav == 0:
            return None

        period_return = ((latest_nav / start_nav) - 1) * 100
        return round(period_return, 2)

    @capture_call('screening.return_from_nav')
    def _calc_return_from_nav(
        self,
        nav_df: pd.DataFrame,
        days: int
    ) -> Optional[float]:
        """
        从已有的净值 DataFrame 计算指定周期收益率（纯计算，不查数据库）。

        与 calculate_period_return() 逻辑一致，供批量计算场景使用。

        Args:
            nav_df: 净值 DataFrame（需包含 nav_date, unit_nav 列，可选 daily_growth, accum_nav）
            days: 周期天数

        Returns:
            收益率（百分比），数据不足返回 None
        """
        if len(nav_df) < 2:
            return None

        latest_row = nav_df.iloc[-1]
        target_date = latest_row['nav_date'] - timedelta(days=days)

        before_target = nav_df[nav_df['nav_date'] <= target_date]
        if len(before_target) > 0:
            closest_idx = before_target['nav_date'].idxmax()
        else:
            closest_idx = nav_df['nav_date'].idxmin()

        start_row = nav_df.loc[closest_idx]

        # 方案1: daily_growth 复利累积
        period_df = nav_df.loc[closest_idx:]
        if self._can_use_daily_growth_window(period_df):
            dg = period_df['daily_growth'].iloc[1:]
            dg = dg.dropna()
            if len(dg) >= 2:
                cumulative = (1 + dg / 100).prod() - 1
                return round(cumulative * 100, 2)

        # 方案2: accum_nav
        if 'accum_nav' in nav_df.columns and self._has_reliable_accum_nav_window(period_df):
            latest_accum = latest_row.get('accum_nav')
            start_accum = start_row.get('accum_nav')
            if pd.notna(latest_accum) and pd.notna(start_accum) and start_accum > 0:
                return round(((latest_accum / start_accum) - 1) * 100, 2)

        # 方案3: unit_nav
        if 'unit_nav' not in nav_df:
            return None
        latest_nav = latest_row['unit_nav']
        start_nav = start_row['unit_nav']
        if pd.isna(latest_nav) or pd.isna(start_nav) or start_nav == 0:
            return None
        return round(((latest_nav / start_nav) - 1) * 100, 2)

    def _count_funds_by_type(self, cursor, fund_type: Optional[Union[str, 'FundTypeCode', List]]) -> Optional[int]:
        """
        按当前分析基金类型统计基金数量。

        Returns:
            匹配基金数；fund_type 为 None 时返回 None 表示不做类型细分。
        """
        if fund_type is None:
            return None

        type_condition, type_params = self._build_type_condition(fund_type, "债券%")
        cursor.execute(
            f"SELECT COUNT(*) FROM {TABLE_FUNDS} f WHERE {type_condition}",
            type_params,
        )
        return cursor.fetchone()[0]

    def get_database_stats(
        self,
        fund_type: Optional[Union[str, 'FundTypeCode', List]] = None
    ) -> dict:
        """
        获取数据库统计信息

        Returns:
            包含统计信息的字典
        """
        self.connect()
        cursor = self.conn.cursor()

        stats = {}

        # 基金总数
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_FUNDS}")
        stats['total_funds'] = cursor.fetchone()[0]

        fund_type_count = self._count_funds_by_type(cursor, fund_type)
        if fund_type_count is not None:
            stats['selected_funds'] = fund_type_count
        if self._is_hybrid_fund_type_request(fund_type):
            stats.update(self._get_hybrid_commodity_exclusion_observability(cursor))
            stats.update(self._get_hybrid_negotiable_cd_exclusion_observability(cursor))
            stats.update(self._get_hybrid_explicit_fof_exclusion_observability(cursor))

        # 净值记录总数
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAV_HISTORY}")
        stats['total_nav_records'] = cursor.fetchone()[0]

        # 有净值数据的基金数
        cursor.execute(NAV_HISTORY_FCODE_COUNT_SQL)
        stats['funds_with_nav'] = cursor.fetchone()[0]

        # 数据时间跨度
        cursor.execute(f"SELECT MIN(nav_date), MAX(nav_date) FROM {TABLE_NAV_HISTORY}")
        min_date, max_date = cursor.fetchone()
        stats['date_range'] = (min_date, max_date)

        return stats

    def get_fund_nav_latest_dates(self, fcodes: List[str], batch_size: int = 500) -> pd.DataFrame:
        """
        获取多只基金的最新净值日期（Step 5B）

        Args:
            fcodes: 基金代码列表
            batch_size: 分批查询大小，默认500避免IN过长

        Returns:
            DataFrame，列：fcode, nav_latest_date
        """
        self.connect()

        if not fcodes:
            return pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        nav_history_columns = self._get_table_columns(TABLE_NAV_HISTORY)
        has_nav_history = {'fcode', 'nav_date'}.issubset(nav_history_columns)
        returns_df = self._read_fund_returns_latest_nav_dates(
            fcodes,
            batch_size=batch_size,
            verify_history=has_nav_history,
        )
        resolved_codes = {
            str(code).strip()
            for code in returns_df.get('fcode', pd.Series(dtype=object)).tolist()
            if str(code).strip()
        }
        missing_fcodes = [
            fcode
            for fcode in fcodes
            if str(fcode or '').strip() and str(fcode or '').strip() not in resolved_codes
        ]
        if not missing_fcodes:
            return returns_df

        # fund_nav_history 在生产库中应存在，但极简测试库/兼容场景可能缺失。
        # 这里回退为空结果，避免新鲜度辅助信息缺失时阻断主筛选链路。
        if not has_nav_history:
            return returns_df

        history_df = self._read_fund_nav_history_latest_nav_dates(missing_fcodes, batch_size=batch_size)
        if returns_df.empty:
            return history_df
        if history_df.empty:
            return returns_df
        return pd.concat([returns_df, history_df], ignore_index=True)

    def _read_fund_returns_latest_nav_dates(
        self,
        fcodes: List[str],
        batch_size: int = 500,
        *,
        verify_history: bool = False,
    ) -> pd.DataFrame:
        """优先从 fund_returns.latest_nav_date 快速读取净值日期摘要。"""
        columns = self._get_table_columns(TABLE_RETURNS)
        if not {'fcode', 'latest_nav_date'}.issubset(columns):
            return pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        all_results = []
        for i in range(0, len(fcodes), batch_size):
            batch_fcodes = fcodes[i:i + batch_size]
            if not batch_fcodes:
                continue
            placeholders = ','.join(['?' for _ in batch_fcodes])
            if verify_history:
                query = f"""
                    SELECT fcode, nav_latest_date
                    FROM (
                        SELECT
                            fcode,
                            history_latest_nav_date,
                            CASE
                                WHEN history_latest_nav_date IS NOT NULL
                                  AND (
                                      returns_latest_nav_date IS NULL
                                      OR TRIM(CAST(returns_latest_nav_date AS TEXT)) = ''
                                      OR history_latest_nav_date > returns_latest_nav_date
                                  )
                                THEN history_latest_nav_date
                                ELSE returns_latest_nav_date
                            END AS nav_latest_date
                        FROM (
                            SELECT
                                r.fcode,
                                r.latest_nav_date AS returns_latest_nav_date,
                                (
                                    SELECT h.nav_date
                                    FROM {TABLE_NAV_HISTORY} h
                                    WHERE h.fcode = r.fcode
                                    ORDER BY h.nav_date DESC
                                    LIMIT 1
                                ) AS history_latest_nav_date
                            FROM {TABLE_RETURNS} r
                            WHERE r.fcode IN ({placeholders})
                        )
                    )
                    WHERE nav_latest_date IS NOT NULL
                      AND TRIM(CAST(nav_latest_date AS TEXT)) <> ''
                      AND history_latest_nav_date IS NOT NULL
                """
            else:
                query = f"""
                    SELECT fcode, latest_nav_date AS nav_latest_date
                    FROM {TABLE_RETURNS}
                    WHERE fcode IN ({placeholders})
                      AND latest_nav_date IS NOT NULL
                      AND TRIM(CAST(latest_nav_date AS TEXT)) <> ''
                """
            batch_df = pd.read_sql_query(query, self.conn, params=batch_fcodes)
            all_results.append(batch_df)

        if all_results:
            result_df = pd.concat(all_results, ignore_index=True)
        else:
            result_df = pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        return self._normalize_nav_latest_date_frame(result_df)

    def _read_fund_nav_history_latest_nav_dates(self, fcodes: List[str], batch_size: int = 500) -> pd.DataFrame:
        """从 fund_nav_history 回退计算最新净值日期。"""
        all_results = []

        # 分批查询避免 IN 子句过长
        for i in range(0, len(fcodes), batch_size):
            batch_fcodes = fcodes[i:i + batch_size]
            if not batch_fcodes:
                continue
            placeholders = ','.join(['?' for _ in batch_fcodes])

            query = f"""
                SELECT fcode, MAX(nav_date) AS nav_latest_date
                FROM {TABLE_NAV_HISTORY}
                WHERE fcode IN ({placeholders})
                GROUP BY fcode
            """

            batch_df = pd.read_sql_query(query, self.conn, params=batch_fcodes)
            all_results.append(batch_df)

        if all_results:
            result_df = pd.concat(all_results, ignore_index=True)
        else:
            result_df = pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        return self._normalize_nav_latest_date_frame(result_df)

    @staticmethod
    def _normalize_nav_latest_date_frame(result_df: pd.DataFrame) -> pd.DataFrame:
        """标准化净值日期摘要，保留缺失语义并过滤无法解析的日期。"""
        if result_df is None or result_df.empty:
            return pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        result_df = result_df.copy()
        result_df['fcode'] = result_df.get('fcode', pd.Series(dtype=object)).astype(str).str.strip()
        result_df = result_df[result_df['fcode'] != ""].copy()
        if result_df.empty:
            return pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        if len(result_df) > 0:
            result_df['nav_latest_date'] = pd.to_datetime(
                result_df.get('nav_latest_date', pd.Series(dtype=object)),
                errors='coerce',
            ).dt.date
            result_df = result_df.dropna(subset=['nav_latest_date'])

        if result_df.empty:
            return pd.DataFrame(columns=['fcode', 'nav_latest_date'])

        result_df = result_df.drop_duplicates(subset=['fcode'], keep='last')
        return result_df[['fcode', 'nav_latest_date']].reset_index(drop=True)


# ============================================================================
# 辅助函数
# ============================================================================

def count_weekends_in_range(start_date, end_date) -> int:
    """
    计算日期区间内的周末天数（周六和周日）

    v1.9.57 P2-3: 从 O(n) 逐日循环优化为 O(1) 数学计算

    Args:
        start_date: 起始日期（date对象）
        end_date: 结束日期（date对象）

    Returns:
        周末天数
    """
    from datetime import timedelta

    if start_date is None or end_date is None:
        return 0

    if start_date > end_date:
        return 0

    days = (end_date - start_date).days + 1
    full_weeks, remaining = divmod(days, 7)
    weekend_count = full_weeks * 2

    # 处理不足一周的剩余天数
    start_weekday = start_date.weekday()
    for i in range(remaining):
        if (start_weekday + i) % 7 in (5, 6):
            weekend_count += 1

    return weekend_count


def compute_nav_freshness_flags(
    latest_dates: pd.Series,
    today,
    max_age_days: int,
    skip_weekends: bool,
    shadow_thresholds: List[int]
) -> pd.DataFrame:
    """
    计算净值新鲜度标记（Step 5B 纯函数）

    Args:
        latest_dates: 最新净值日期序列（date对象或NaT）
        today: 当前日期（date对象）
        max_age_days: 最大过期天数
        skip_weekends: 是否扣除周末
        shadow_thresholds: 影子评估阈值列表，例如 [7, 14, 30]

    Returns:
        DataFrame，包含列：
        - nav_age_days: 净值过期天数（扣除周末后，如果skip_weekends=True）
        - is_fresh: 是否新鲜（True/False）
        - freshness_reason: 原因代码（OK/NO_NAV_HISTORY/NAV_TOO_OLD）
        - shadow_age_fail_Nd: 影子评估列（N来自shadow_thresholds）
    """
    from datetime import date, timedelta

    result = pd.DataFrame(index=latest_dates.index)

    # 计算原始天数差
    def calc_age_days(latest_date):
        if pd.isna(latest_date) or latest_date is None:
            return None
        if isinstance(latest_date, str):
            latest_date = date.fromisoformat(latest_date)
        raw_days = (today - latest_date).days
        if skip_weekends:
            # 扣除周末天数
            weekend_days = count_weekends_in_range(latest_date + timedelta(days=1), today)
            return max(0, raw_days - weekend_days)
        return raw_days

    result['nav_age_days'] = latest_dates.apply(calc_age_days)

    # 判断是否新鲜
    def check_fresh(row):
        age = row['nav_age_days']
        if age is None:
            return False
        return age <= max_age_days

    result['is_fresh'] = result.apply(check_fresh, axis=1)

    # 确定原因代码
    def get_reason(row):
        if pd.isna(row['nav_age_days']):
            return 'NO_NAV_HISTORY'
        elif row['is_fresh']:
            return 'OK'
        else:
            return 'NAV_TOO_OLD'

    result['freshness_reason'] = result.apply(get_reason, axis=1)

    # 生成影子评估列
    for threshold in shadow_thresholds:
        col_name = f'shadow_age_fail_{threshold}d'
        # 如果 nav_age_days 为 NaN/None 或 > threshold，则标记为失败
        result[col_name] = result['nav_age_days'].apply(
            lambda x: True if pd.isna(x) or x > threshold else False
        )

    return result


def test_data_loader():
    """测试数据加载器"""
    print("="*80)
    print("测试 DataLoader 模块")
    print("="*80)

    with DataLoader() as loader:
        # 测试1: 获取数据库统计
        print("\n【测试1】数据库统计信息")
        stats = loader.get_database_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # 测试2: 加载1年期基金数据
        print("\n【测试2】加载1年期债券基金数据")
        df = loader.load_funds_data(period_years=1)
        print(f"  加载基金数: {len(df)}")
        print(f"  数据列: {list(df.columns)}")
        print(f"  前5行:")
        print(df.head())

        # 测试3: 获取单只基金历史净值
        print("\n【测试3】获取单只基金历史净值")
        if len(df) > 0:
            test_fcode = df.iloc[0]['fcode']
            nav_df = loader.get_fund_nav_history(test_fcode, days=365)
            print(f"  基金代码: {test_fcode}")
            print(f"  净值记录数: {len(nav_df)}")
            if len(nav_df) > 0:
                print(f"  最早日期: {nav_df.iloc[0]['nav_date']}")
                print(f"  最新日期: {nav_df.iloc[-1]['nav_date']}")

        # 测试4: 计算4年期收益率
        print("\n【测试4】计算4年期收益率")
        if len(df) > 0:
            test_fcode = df.iloc[0]['fcode']
            return_4y = loader.calculate_period_return(test_fcode, days=1460)
            print(f"  基金代码: {test_fcode}")
            print(f"  4年期收益率: {return_4y}%" if return_4y else "  4年期收益率: 数据不足")

    print("\n" + "="*80)
    print("✅ DataLoader 测试完成")
    print("="*80)


if __name__ == "__main__":
    test_data_loader()
