#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础分析器类 - BaseAnalyzer

整合所有核心引擎，提供完整的基金分析流程：
1. 数据加载
2. 10层筛选
3. 双评分计算
4. 结果排序
5. 统计分析
6. 报告生成

支持多种基金类型分析（债券、股票、混合、指数等）
所有周期分析脚本都继承此类
"""

import pandas as pd
import numpy as np
from datetime import date as date_type, datetime
from pathlib import Path
import sys
import logging
import json
import math
import importlib.util
import re
from typing import Dict, Optional, Tuple, Union

from bond_fund_screening.config import (
    DB_PATH,
    OUTPUT_DIR,
    PERIOD_CONFIGS,
    DEFAULT_TOP_N,
    VERBOSE,
    FUND_TYPE_MAPPING,
    SCORING_POLICY_RUNTIME,
    SCORING_POLICY_RECOMMENDED,
)
from bond_fund_screening.core.data_loader import DataLoader
from bond_fund_screening.core.risk_calculator import RiskCalculator
from utils.calculation_capture import capture_call, capture_context, capture_session
from bond_fund_screening.core.filtering_engine import FilteringEngine, stable_sort_desc
from bond_fund_screening.core.scoring_engine import (
    ScoringEngine,
    canonicalize_runtime_fund_type_key,
    resolve_per_fund_type_score_key,
)
from bond_fund_screening.report_catalog import (
    atomic_write_json_file,
    rebuild_report_manifests_for_paths,
)
from bond_fund_screening.tasks.output_retention import (
    cleanup_same_day_family_versions,
    cleanup_same_day_versions_for_file,
)
from bond_fund_screening.report_artifact_metadata import (
    collect_source_runs_from_stats_file,
    serialize_output_path_reference,
    write_derived_report_meta,
)

# 导入Profile支持
# 注意：需要使用项目根路径优先，避免 "utils" 解析到 bond_fund_screening/utils
_app_root = str(Path(__file__).parent.parent.parent)
while _app_root in sys.path:
    sys.path.remove(_app_root)
sys.path.insert(0, _app_root)

try:
    from utils.log_dir import resolve_log_dir
except ModuleNotFoundError:
    # 兼容 tests 中将 bond_fund_screening 路径置顶导致的 utils 命名冲突
    _log_dir_path = Path(_app_root) / 'utils' / 'log_dir.py'
    _log_dir_spec = importlib.util.spec_from_file_location('_app_utils_log_dir', _log_dir_path)
    _log_dir_module = importlib.util.module_from_spec(_log_dir_spec)
    _log_dir_spec.loader.exec_module(_log_dir_module)
    resolve_log_dir = _log_dir_module.resolve_log_dir

try:
    # 统一使用 canonical registry，避免分析主链与 API/任务链读取到不同的 Profile 集合。
    import bond_fund_screening.profiles  # noqa: F401
    from config.fund_type_registry import (
        FundTypeCode,
        FundTypeProfile,
        FundCategory,
        get_profile,
        resolve_alias,
    )
    HAS_PROFILE = True
except Exception:
    FundTypeCode = None
    FundTypeProfile = None
    FundCategory = None
    get_profile = None
    resolve_alias = None
    HAS_PROFILE = False


def _safe_float(v):
    """将数值转为 float，NaN/Inf/None 转为 None（JSON 安全）"""
    if v is None:
        return None
    f = float(v)
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _safe_series_stat(series: pd.Series, stat_name: str):
    """对全空序列做防护，避免 `Mean of empty slice` 运行时告警。"""
    if series is None:
        return None
    cleaned = series.dropna()
    if cleaned.empty:
        return None

    if stat_name == 'min':
        return _safe_float(cleaned.min())
    if stat_name == 'max':
        return _safe_float(cleaned.max())
    if stat_name == 'mean':
        return _safe_float(cleaned.mean())
    if stat_name == 'median':
        return _safe_float(cleaned.median())
    if stat_name == 'sum':
        return _safe_float(cleaned.sum())
    raise ValueError(f"不支持的统计类型: {stat_name}")


class BaseAnalyzer:
    """
    基础分析器类

    提供完整的分析流程，所有周期分析脚本继承此类
    支持多种基金类型（债券、股票、混合等）
    """

    def __init__(
        self,
        period_years: Optional[int] = None,
        period_days: Optional[int] = None,
        top_n: Optional[int] = DEFAULT_TOP_N,
        output_dir: Path = OUTPUT_DIR,
        verbose: bool = VERBOSE,
        fund_type: Optional[Union[str, 'FundTypeCode']] = None,
        as_of_date: Optional[date_type] = None,
        score_key_override: Optional[str] = None,
        enable_tracking: bool = True,
        save_outputs: bool = True,
        collect_db_stats: bool = True,
        freshness_strategy_override: Optional[str] = None,
    ):
        """
        初始化分析器

        Args:
            period_years: 分析周期（年），与period_days二选一
            period_days: 分析周期（天），与period_days二选一，优先使用
            top_n: 输出基金数量上限；None 表示输出全部达标基金
            output_dir: 输出目录
            verbose: 是否打印详细信息
            fund_type: 基金类型，支持以下格式：
                - str: 'bond', 'stock', 'hybrid', 'index', 'money', 'qdii', 'fof' 等别名
                - FundTypeCode: 枚举值（如 FundTypeCode.BOND）
                - None: 默认使用债券型（向后兼容）
            as_of_date: 回测截断日期（date对象），默认None=使用最新数据。
                传入时，筛选引擎将截断净值数据到此日期。
            enable_tracking: 是否启用筛选追踪落库。
            save_outputs: 是否保存 CSV/JSON/screening_picks 等单次分析产物。
            collect_db_stats: 是否采集数据库全表统计信息。
            freshness_strategy_override: 新鲜度策略运行时覆写（soft/hard）。
        """
        # 参数验证
        if period_days is None and period_years is None:
            raise ValueError("必须提供period_years或period_days参数")

        # 优先使用period_days
        if period_days is not None:
            self.period_days = period_days
            self.period_years = period_days / 365
        elif period_years is not None:
            self.period_years = period_years
            self.period_days = round(period_years * 365)

        # 判断分析类型
        self.is_short_term = self.period_days <= 180

        self.top_n = None if top_n in (None, 0) else int(top_n)
        self.output_dir = Path(output_dir)
        self.verbose = verbose

        # 加载基金类型Profile
        self.fund_type = fund_type
        self.as_of_date = as_of_date  # 回测截断日期
        self.score_key_override = score_key_override  # CLI --score-key 覆盖
        self.enable_tracking = enable_tracking
        self.save_outputs = save_outputs
        self.collect_db_stats = collect_db_stats
        self.freshness_strategy_override = freshness_strategy_override
        self._resolve_score_policy_runtime()  # Step 7-5: 运行时开关解析
        self.profile = self._load_profile(fund_type)
        self.fund_type_name = self._get_fund_type_name()

        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 获取周期配置（支持小数key用于短期周期）
        # 对于短期周期，使用天数匹配配置
        if self.is_short_term:
            # 短期周期天数映射到配置key
            days_to_key = {
                7: 0.02,    # 一周
                30: 0.08,   # 一月
                90: 0.25,   # 三月
                180: 0.49,  # 半年
            }
            config_key = days_to_key.get(int(self.period_days))
            if config_key:
                self.period_config = PERIOD_CONFIGS[config_key]
            else:
                # 如果不在预定义的短期周期中，使用默认配置
                self.period_config = {
                    'days': int(self.period_days),
                    'name': f'{int(self.period_days)}天期',
                    'output_name': f'{int(self.period_days)}天期'
                }
        else:
            # 长期周期使用年数匹配
            self.period_config = PERIOD_CONFIGS.get(
                int(self.period_years),
                {
                    'days': int(self.period_days),
                    'name': f'{int(self.period_years)}年期',
                    'output_name': f'{int(self.period_years)}年期'
                }
            )

        # 初始化核心引擎
        self.data_loader = None
        self.filtering_engine = None
        self.scoring_engine = None

        # 存储分析结果
        self.df_result = None
        self.stats = {}
        self.last_output_base_filename = None
        self.last_stats_file = None
        self.last_bond_short_term_observation_base_filename = None
        self._last_loaded_df = None

        # 设置日志
        self._setup_logging()

    def _load_profile(
        self,
        fund_type: Optional[Union[str, 'FundTypeCode']]
    ) -> Optional['FundTypeProfile']:
        """
        加载基金类型Profile配置

        Args:
            fund_type: 基金类型

        Returns:
            FundTypeProfile实例，如果类型无效或Profile系统未安装则返回None
        """
        if not HAS_PROFILE:
            # Profile系统未安装，使用默认配置
            return None

        if fund_type is None:
            # 默认使用债券型（向后兼容）
            return get_profile(FundTypeCode.BOND)

        # 如果是字符串别名，解析为FundTypeCode
        if isinstance(fund_type, str):
            resolved = resolve_alias(fund_type)
            if resolved is None:
                # 无效别名，回退到债券型
                return get_profile(FundTypeCode.BOND)
            return get_profile(resolved)

        # 如果是FundTypeCode枚举
        if isinstance(fund_type, FundTypeCode):
            return get_profile(fund_type)

        # 无效类型
        return None

    def _resolve_score_policy_runtime(self):
        """
        Step 7-5/7-8/7-14: 解析运行时评分策略开关。

        优先级（从高到低）：
        1) 显式参数 score_key_override（CLI / API 传入）
        2) SCORING_POLICY_RUNTIME override_env 环境变量
        3) policy_override.json（Step 7-14 补丁执行器写入）
        4) SCORING_POLICY_RECOMMENDED per_fund_type（Step 7-8）
        5) SCORING_POLICY per_fund_type（静态默认策略，由 ScoringEngine 处理）
        6) fallback combo_score（最终兜底）

        runtime 未启用 -> 强制 score_key_override='combo_score'（零回归）
        """
        import os
        runtime_cfg = SCORING_POLICY_RUNTIME
        recommended_cfg = SCORING_POLICY_RECOMMENDED

        # 判断 runtime 是否启用
        env_toggle = runtime_cfg.get('env_toggle', '')
        env_val = os.environ.get(env_toggle, '').lower() if env_toggle else ''
        self.policy_enabled = (
            env_val in ('true', '1', 'yes')
            or runtime_cfg.get('enabled', False)
        )

        # 追溯信息
        self.policy_override_used = False
        self.policy_override_value = None
        self.policy_source = 'fallback'  # Step 7-8: 追溯 override 来源
        self.recommended_runtime_cfg = recommended_cfg  # 快照用

        if not self.policy_enabled:
            # runtime 关闭：强制 baseline（除非 CLI 显式指定）
            if self.score_key_override:
                self.policy_source = 'override'
            else:
                self.score_key_override = 'combo_score'
                self.policy_source = 'fallback'
            return

        # 优先级 1: CLI 显式指定
        if self.score_key_override:
            self.policy_source = 'override'
            return

        # 优先级 2: 环境变量 override
        override_env = runtime_cfg.get('override_env', '')
        env_override = os.environ.get(override_env, '') if override_env else ''
        if env_override:
            self.score_key_override = env_override
            self.policy_override_used = True
            self.policy_override_value = env_override
            self.policy_source = 'runtime'
            return

        # 优先级 3: Step 7-14 policy_override.json
        override_key = self._read_policy_override_key()
        if override_key:
            self.score_key_override = override_key
            self.policy_override_used = True
            self.policy_override_value = override_key
            self.policy_source = 'policy_override'
            return

        # 优先级 4: Step 7-8 推荐策略注入
        if recommended_cfg.get('enabled', False):
            ft = canonicalize_runtime_fund_type_key(self.fund_type, default='bond')
            period_key = self.period_days if self.period_days <= 180 else int(self.period_years)
            recommended_key = resolve_per_fund_type_score_key(
                recommended_cfg.get('per_fund_type', {}),
                fund_type=ft,
                period_years=period_key,
            )
            if recommended_key:
                self.score_key_override = recommended_key
                self.policy_override_used = True
                self.policy_override_value = recommended_key
                self.policy_source = 'recommended'
                return

        # 优先级 5: 交给 ScoringEngine 的 select_score_key（SCORING_POLICY）
        self.policy_source = 'policy'

    @staticmethod
    def _describe_fund_type_value(value) -> Optional[Union[str, list]]:
        """将 fund_type / FundTypeCode 转换为稳定、可观测的描述。"""
        if value is None:
            return None
        if isinstance(value, list):
            return [BaseAnalyzer._describe_fund_type_value(item) for item in value]
        if hasattr(value, 'name') and hasattr(value, 'value'):
            return f"{value.name}({value.value})"
        return str(value)

    def _build_fund_universe_metadata(
        self,
        fund_type_param,
        fund_type_pattern: str,
        db_stats: Optional[Dict] = None,
    ) -> Dict:
        """构建基金池来源口径摘要，写入正式 stats 产物供后续审计复用。"""
        if fund_type_param is None:
            selector_source = 'legacy_pattern'
        elif self.profile is not None and self._describe_fund_type_value(fund_type_param) == self._describe_fund_type_value(self.profile.fund_type):
            selector_source = 'profile_default' if self.fund_type is None else 'profile_resolved'
        else:
            selector_source = 'explicit_fund_type'

        resolved_type_codes = []
        if fund_type_param is not None and self.data_loader is not None:
            if isinstance(fund_type_param, list):
                for item in fund_type_param:
                    resolved_type_codes.extend(self.data_loader._resolve_fund_type(item))
            else:
                resolved_type_codes.extend(self.data_loader._resolve_fund_type(fund_type_param))

        resolved_type_codes = list(dict.fromkeys(str(code) for code in resolved_type_codes))

        metadata = {
            'selector_source': selector_source,
            'requested_fund_type': self._describe_fund_type_value(self.fund_type),
            'profile_fund_type': self._describe_fund_type_value(self.profile.fund_type) if self.profile is not None else None,
            'effective_fund_type': self._describe_fund_type_value(fund_type_param),
            'effective_type_codes': resolved_type_codes,
            'fund_type_pattern': fund_type_pattern,
        }
        if db_stats and db_stats.get('selected_funds') is not None:
            metadata['selected_funds'] = int(db_stats['selected_funds'])
        metadata['excluded_high_confidence_commodity_candidates'] = int(
            (db_stats or {}).get('excluded_high_confidence_commodity_candidates') or 0
        )
        metadata['excluded_high_confidence_commodity_candidate_samples'] = list(
            (db_stats or {}).get('excluded_high_confidence_commodity_candidate_samples') or []
        )
        metadata['excluded_high_confidence_negotiable_cd_candidates'] = int(
            (db_stats or {}).get('excluded_high_confidence_negotiable_cd_candidates') or 0
        )
        metadata['excluded_high_confidence_negotiable_cd_candidate_samples'] = list(
            (db_stats or {}).get('excluded_high_confidence_negotiable_cd_candidate_samples') or []
        )
        metadata['excluded_explicit_fof_candidates'] = int(
            (db_stats or {}).get('excluded_explicit_fof_candidates') or 0
        )
        metadata['excluded_explicit_fof_candidate_samples'] = list(
            (db_stats or {}).get('excluded_explicit_fof_candidate_samples') or []
        )
        return metadata

    def _get_loader_fund_type_param(self):
        """
        返回实际传给 DataLoader 的 fund_type。

        规则：
        - 用户显式传入 fund_type 时，必须保留其原始入口语义（例如 bond -> 31+32）
        - 仅在未显式传入时，才回退到 profile 默认枚举口径（例如 BOND -> 31）
        """
        if self.fund_type is not None:
            return self.fund_type
        if self.profile is not None:
            return self.profile.fund_type
        return None

    def _read_policy_override_key(self) -> Optional[str]:
        """
        Step 7-14: 从 config/policy_override.json 读取 score_key。

        文件由 apply_policy_patch.py 写入，结构：
        {
            "enabled": true,
            "applied_at": "...",
            "patch": {
                "SCORING_POLICY": {
                    "default_score_key": "...",
                    "per_fund_type": {"bond": "...", ...}
                }
            }
        }

        Returns:
            score_key 字符串，或 None（文件不存在/未启用/无匹配）
        """
        override_path = Path(__file__).parent.parent.parent / 'config' / 'policy_override.json'
        if not override_path.exists():
            return None

        try:
            with open(override_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            self.logger.warning(f"读取 policy_override.json 失败: {e}")
            return None

        if not isinstance(data, dict) or not data.get('enabled', False):
            return None

        patch = data.get('patch', {})
        if not isinstance(patch, dict):
            return None

        sp = patch.get('SCORING_POLICY', {})
        if not isinstance(sp, dict):
            return None

        # 优先 per_fund_type 匹配当前 fund_type
        ft = canonicalize_runtime_fund_type_key(self.fund_type, default='bond')
        period_key = self.period_days if self.period_days <= 180 else int(self.period_years)
        resolved = resolve_per_fund_type_score_key(
            sp.get('per_fund_type', {}),
            fund_type=ft,
            period_years=period_key,
        )
        if resolved:
            return resolved

        # fallback: default_score_key
        dsk = sp.get('default_score_key')
        if isinstance(dsk, str) and dsk:
            return dsk

        return None

    def _get_fund_type_name(self) -> str:
        """获取基金类型显示名称"""
        if self.profile is not None:
            return self.profile.display_name
        return "债券型"  # 默认

    def _setup_logging(self):
        """设置日志"""
        log_format = '%(asctime)s [%(levelname)s] %(message)s'
        log_level = logging.INFO if self.verbose else logging.WARNING

        # 创建logger（短期用天数，长期用年数）
        if self.is_short_term:
            logger_name = f'Analyzer_{int(self.period_days)}D'
            log_suffix = f'{int(self.period_days)}day'
        else:
            logger_name = f'Analyzer_{int(self.period_years)}Y'
            log_suffix = f'{int(self.period_years)}year'
        fund_type_slug = self.fund_type_name.replace("型", "").replace("/", "_").replace(" ", "_")
        logger_name = f"{logger_name}_{fund_type_slug}"

        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)

        # 清理并关闭已有 handler，避免文件句柄泄漏（测试中会触发 ResourceWarning）
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass

        # 控制台handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(logging.Formatter(log_format))
        self.logger.addHandler(console_handler)

        # 文件handler（可选）：分析运行日志统一落到 logs/，避免污染业务输出目录。
        resolved_log_dir = Path(resolve_log_dir('logs'))
        log_file = resolved_log_dir / f'analyzer_{fund_type_slug}_{log_suffix}_{datetime.now().strftime("%Y%m%d")}.log'
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(logging.Formatter(log_format))
            self.logger.addHandler(file_handler)
        except Exception:
            pass  # 静默降级：仅保留控制台输出

    def run(self) -> Tuple[pd.DataFrame, Dict]:
        """
        执行完整的分析流程

        Returns:
            (结果DataFrame, 统计信息字典)
        """
        return self.run_pipeline(apply_final_selection=True)

    def _finish_tracker_run(
        self,
        fund_count_pass: int,
        success: bool,
        fail_reason: Optional[str] = None,
    ) -> None:
        """统一在总流程收尾时完成 screening run。"""
        tracker = getattr(getattr(self, 'filtering_engine', None), 'tracker', None)
        if tracker is None or getattr(tracker, 'is_finished', False):
            return

        try:
            tracker.finish_run(
                fund_count_pass=fund_count_pass,
                success=success,
                fail_reason=fail_reason,
            )
        except Exception as e:
            self.logger.warning(f"⚠️ screening run 收尾失败（不影响主流程）: {e}")

    @capture_session('screening_period', attributes=(
        'period_years', 'period_days', 'period_config', 'fund_type', 'fund_type_name',
        'as_of_date', 'top_n', 'freshness_strategy_override', 'score_key_override',
        'enable_tracking', 'save_outputs',
    ))
    @capture_call('screening.pipeline')
    def run_pipeline(self, apply_final_selection: bool = True) -> Tuple[pd.DataFrame, Dict]:
        """
        执行完整的分析流程。

        Args:
            apply_final_selection: 是否执行 Layer10 最终精选与 TopN 截断。
                True: 默认生产路径，返回最终输出结果。
                False: 返回评分后的完整候选集，供回测/A/B 分析使用。

        Returns:
            (结果DataFrame, 统计信息字典)
        """
        try:
            self.logger.info("="*80)
            self.logger.info(f"{self.fund_type_name}基金智能筛选系统 - {self.period_config['name']}分析")
            self.logger.info("="*80)
            self.logger.info(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # 短期周期优先显示天数，长期周期显示年数
            if self.is_short_term:
                self.logger.info(f"分析周期: {int(self.period_days)}天 ({self.period_years:.2f}年)")
            else:
                self.logger.info(f"分析周期: {int(self.period_years)}年 ({int(self.period_days)}天)")

            self.logger.info(f"基金类型: {self.fund_type_name}")
            self.logger.info(f"输出目录: {self.output_dir}")
            self.logger.info("")

            # 阶段1: 数据加载
            self.logger.info("【阶段1/5】数据加载")
            df = self._load_data()
            self.logger.info(f"✅ 加载完成，共 {len(df):,} 只{self.fund_type_name}基金\n")

            # 阶段2: 10层筛选
            self.logger.info("【阶段2/5】10层渐进式筛选")
            df_filtered = self._filter_funds(df)
            self.logger.info(f"✅ 筛选完成，剩余 {len(df_filtered):,} 只基金\n")

            # 筛选后无基金时提前返回，避免后续空 DataFrame 导致异常
            if df_filtered.empty:
                self.logger.warning(f"⚠️ 筛选后无基金通过，跳过评分/统计/保存阶段")
                self.stats['analysis'] = {'count': 0, 'period': self.period_config.get('name', f"{self.period_years}年")}
                self.df_result = df_filtered
                self._finish_tracker_run(fund_count_pass=0, success=True)
                return df_filtered, self.stats

            # 阶段3: 评分计算
            self.logger.info("【阶段3/5】双评分系统计算")
            df_scored = self._calculate_scores(df_filtered)
            self.logger.info(f"✅ 评分完成，共 {len(df_scored):,} 只基金\n")

            if apply_final_selection:
                # 阶段4: 第10层最终精选 + 排序和统计
                self.logger.info("【阶段4/5】第10层最终精选 + 结果排序和统计分析")
                df_layer10 = self._apply_layer10_selection(df_scored)
                df_final = self._sort_and_analyze(df_layer10)
                self.logger.info(f"✅ 分析完成，最终输出 {len(df_final):,} 只基金\n")
            else:
                # 回测/A-B 路径：保留评分后的完整候选集，避免主路径 Layer10 提前截断
                self.logger.info("【阶段4/5】跳过最终精选，保留评分后的完整候选集")
                df_final = df_scored.copy()
                self._analyze_statistics(df_final)
                self.logger.info(f"✅ 分析完成，保留 {len(df_final):,} 只评分候选基金\n")

            # 阶段5: 保存结果
            if self.save_outputs and apply_final_selection:
                self.logger.info("【阶段5/5】保存分析结果")
                self._save_results(df_final)
                self.logger.info("✅ 结果已保存\n")
            elif self.save_outputs and not apply_final_selection:
                self.logger.info("【阶段5/5】跳过结果落盘（完整候选集模式不保存最终产物）\n")
            else:
                self.logger.info("【阶段5/5】跳过结果落盘（save_outputs=False）\n")

            # 生成总结
            self._print_summary(df_final)

            self._finish_tracker_run(fund_count_pass=len(df_final), success=True)
            self.df_result = df_final
            return df_final, self.stats

        except Exception as e:
            self._finish_tracker_run(fund_count_pass=0, success=False, fail_reason=str(e))
            self.logger.error(f"❌ 分析过程出错: {str(e)}", exc_info=True)
            raise

        finally:
            # 关闭数据库连接
            if self.data_loader is not None:
                self.data_loader.close()

    @capture_call('screening.loaded_data', attributes=('period_years', 'period_days', 'as_of_date'))
    def _load_data(self) -> pd.DataFrame:
        """
        阶段1: 加载数据

        Returns:
            包含基金完整信息的DataFrame
        """
        self.logger.info("  正在连接数据库...")
        self.data_loader = DataLoader(DB_PATH)
        self.data_loader.connect()

        # DataLoader 的基金池选择应保留用户显式传入的入口语义；
        # Profile 仅用于加载阈值/权重，不应反向收窄基金池。
        fund_type_param = self._get_loader_fund_type_param()
        fund_type_pattern = "债券%"

        db_stats = None
        if self.collect_db_stats:
            db_stats = self.data_loader.get_database_stats(fund_type=fund_type_param)
            self.logger.info(f"  数据库统计:")
            self.logger.info(f"    - 基金总数: {db_stats['total_funds']:,}")
            if db_stats.get('selected_funds') is not None:
                self.logger.info(f"    - 当前类型基金数: {db_stats['selected_funds']:,}")
            if db_stats.get('excluded_high_confidence_commodity_candidates'):
                self.logger.info(
                    "    - 高置信商品联接排除候选: %s",
                    f"{db_stats['excluded_high_confidence_commodity_candidates']:,}",
                )
            if db_stats.get('excluded_high_confidence_negotiable_cd_candidates'):
                self.logger.info(
                    "    - 高置信同业存单排除候选: %s",
                    f"{db_stats['excluded_high_confidence_negotiable_cd_candidates']:,}",
                )
            if db_stats.get('excluded_explicit_fof_candidates'):
                self.logger.info(
                    "    - 显式FOF排除候选: %s",
                    f"{db_stats['excluded_explicit_fof_candidates']:,}",
                )
            self.logger.info(f"    - 净值记录: {db_stats['total_nav_records']:,}")
            self.logger.info(f"    - 数据时间跨度: {db_stats['date_range'][0]} ~ {db_stats['date_range'][1]}")
        else:
            self.logger.info("  数据库统计: 跳过（collect_db_stats=False）")

        self.logger.info(f"\n  正在加载{self.fund_type_name}{self.period_config['name']}基金数据...")

        # 短期周期使用period_days，长期周期使用period_years
        if self.is_short_term:
            df = self.data_loader.load_funds_data(
                period_days=int(self.period_days),
                fund_type=fund_type_param,
                fund_type_pattern=fund_type_pattern,  # 向后兼容
                as_of_date=self.as_of_date
            )
        else:
            df = self.data_loader.load_funds_data(
                period_years=int(self.period_years),
                fund_type=fund_type_param,
                fund_type_pattern=fund_type_pattern,  # 向后兼容
                as_of_date=self.as_of_date
            )

        fund_universe = self._build_fund_universe_metadata(
            fund_type_param=fund_type_param,
            fund_type_pattern=fund_type_pattern,
            db_stats=db_stats,
        )
        self.logger.info(
            "  基金池口径: source=%s, effective=%s, codes=%s, selected=%s, excluded_commodity_candidates=%s, excluded_negotiable_cd_candidates=%s, excluded_explicit_fof_candidates=%s",
            fund_universe.get('selector_source'),
            fund_universe.get('effective_fund_type'),
            fund_universe.get('effective_type_codes'),
            fund_universe.get('selected_funds', len(df)),
            fund_universe.get('excluded_high_confidence_commodity_candidates', 0),
            fund_universe.get('excluded_high_confidence_negotiable_cd_candidates', 0),
            fund_universe.get('excluded_explicit_fof_candidates', 0),
        )

        # DataLoader owns the strict calendar-aligned 4Y calculation. Never
        # overwrite missing verified growth with a less strict NAV-price return.
        if self.period_years == 4:
            self.logger.info("  四年收益使用已验证日增长序列: %s/%s", df['main_return'].notna().sum(), len(df))

        # 存储统计
        self.stats['load'] = {
            'total_funds': len(df),
            'db_stats': db_stats,
            'fund_universe': fund_universe,
        }
        self._last_loaded_df = df

        return df

    def _filter_funds(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        阶段2: 10层筛选

        Args:
            df: 输入的基金DataFrame

        Returns:
            筛选后的DataFrame
        """
        self.filtering_engine = FilteringEngine(
            period_years=self.period_years,
            data_loader=self.data_loader,
            verbose=self.verbose,
            profile=self.profile,  # 传递Profile配置
            enable_tracking=self.enable_tracking,
            fund_type=canonicalize_runtime_fund_type_key(self.fund_type, default='bond'),
            as_of_date=self.as_of_date,  # Step 7-1: 回测截断日期
            freshness_strategy_override=self.freshness_strategy_override,
        )

        # Layer10 延迟到评分后执行，避免策略评分列（selected_combo_score）被提前截断
        df_filtered, filter_stats = self.filtering_engine.run_all_layers(
            df,
            apply_layer10=False,
            defer_tracker_finish=True,
        )
        # 存储统计
        self.stats['filter'] = filter_stats

        return df_filtered

    def _build_bond_short_term_one_year_observation_candidates(self) -> pd.DataFrame:
        """
        构建债基短周期“未满1Y，仅观察”候选。

        设计约束：
        1. 不改变正式主链结果；
        2. 仅在债基短周期链路启用；
        3. 仅复用“关闭 1Y 可观测性门槛”后的影子筛选结果；
        4. 不写入 screening_picks，不进入正式 decision ledger。
        """
        if not self.is_short_term:
            return pd.DataFrame()
        if canonicalize_runtime_fund_type_key(self.fund_type, default='bond') != 'bond':
            return pd.DataFrame()
        if self.data_loader is None:
            return pd.DataFrame()
        if self.filtering_engine is None:
            return pd.DataFrame()

        blocked_df = getattr(
            self.filtering_engine,
            'layer4_one_year_observable_blocked_df',
            pd.DataFrame(),
        )
        if blocked_df is None or blocked_df.empty:
            return pd.DataFrame()

        blocked_fcodes = [
            str(code).strip()
            for code in blocked_df.get('fcode', pd.Series(dtype='object')).tolist()
            if str(code).strip()
        ]
        if not blocked_fcodes:
            return pd.DataFrame()

        candidate_input = self._last_loaded_df
        if not isinstance(candidate_input, pd.DataFrame) or candidate_input.empty:
            return pd.DataFrame()
        candidate_input = candidate_input[
            candidate_input['fcode'].astype(str).isin(blocked_fcodes)
        ].copy()
        if candidate_input.empty:
            return pd.DataFrame()

        shadow_engine = FilteringEngine(
            period_years=self.period_years,
            data_loader=self.data_loader,
            verbose=False,
            profile=self.profile,
            enable_tracking=False,
            fund_type=canonicalize_runtime_fund_type_key(self.fund_type, default='bond'),
            as_of_date=self.as_of_date,
            freshness_strategy_override=self.freshness_strategy_override,
            disable_bond_short_term_one_year_observability_gate=True,
        )
        shadow_filtered, _ = shadow_engine.run_all_layers(
            candidate_input,
            apply_layer10=False,
            defer_tracker_finish=True,
        )
        if shadow_filtered.empty:
            return pd.DataFrame()

        previous_score_stats = self.stats.get('score')
        previous_scoring_engine = self.scoring_engine
        try:
            shadow_scored = self._calculate_scores(shadow_filtered)
        finally:
            if previous_score_stats is None:
                self.stats.pop('score', None)
            else:
                self.stats['score'] = previous_score_stats
            self.scoring_engine = previous_scoring_engine
        sort_col = (
            'selected_combo_score'
            if 'selected_combo_score' in shadow_scored.columns
            else 'combo_score'
        )
        shadow_scored = stable_sort_desc(shadow_scored, sort_col).copy()

        blocked_meta = blocked_df.set_index('fcode')[
            [
                col for col in (
                    'bond_short_term_one_year_observation_status',
                    'bond_short_term_one_year_observation_reason_code',
                    'bond_short_term_one_year_observation_reason',
                    'bond_short_term_one_year_observation_required_gate',
                )
                if col in blocked_df.columns
            ]
        ]
        if not blocked_meta.empty:
            shadow_scored = shadow_scored.merge(
                blocked_meta,
                left_on='fcode',
                right_index=True,
                how='left',
            )

        shadow_scored['recommendation_status'] = 'watch_only'
        shadow_scored['list_tier'] = 'bond_new_observation'
        shadow_scored['watch_only_reason_code'] = 'BOND_SHORT_TERM_1Y_OBSERVABILITY'
        shadow_scored['watch_only_reason'] = shadow_scored.get(
            'bond_short_term_one_year_observation_reason',
            '未满1Y，仅观察',
        )
        shadow_scored['observation_score'] = shadow_scored.get(sort_col)
        return shadow_scored

    def _export_bond_short_term_one_year_observation_candidates(
        self,
        df_candidates: pd.DataFrame,
    ) -> tuple[Optional[Path], Optional[Path]]:
        """导出债基短周期未满1Y独立观察 CSV 与 sidecar。"""
        if df_candidates is None or df_candidates.empty:
            return None, None

        generated_at = datetime.now()
        timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
        base_filename = (
            f"{self.fund_type_name}基金_{self.period_config['output_name']}"
            f"_未满1Y观察_{len(df_candidates)}_{timestamp}"
        )
        observation_csv = self.output_dir / f"{base_filename}.csv"
        export_columns = [
            'fcode',
            'shortname',
            'ftype',
            'establish_days',
            'return_1y',
            'main_return',
            'scale_billion',
            'raw_sharpe_ratio',
            'raw_sortino_ratio',
            'sharpe_ratio',
            'sortino_ratio',
            'calmar_ratio',
            'max_drawdown',
            'max_drawdown_duration',
            'volatility',
            'consistency',
            'selected_combo_score',
            'selected_score_key',
            'selected_score_reason',
            'selected_score_fallback',
            'observation_score',
            'bond_short_term_one_year_observation_status',
            'bond_short_term_one_year_observation_reason_code',
            'bond_short_term_one_year_observation_reason',
            'bond_short_term_one_year_observation_required_gate',
            'recommendation_status',
            'list_tier',
            'watch_only_reason_code',
            'watch_only_reason',
        ]
        available_columns = [col for col in export_columns if col in df_candidates.columns]
        export_df = df_candidates[available_columns].copy()
        if 'raw_sharpe_ratio' in export_df.columns:
            export_df['sharpe_ratio'] = export_df['raw_sharpe_ratio'].combine_first(export_df.get('sharpe_ratio'))
        if 'raw_sortino_ratio' in export_df.columns:
            export_df['sortino_ratio'] = export_df['raw_sortino_ratio'].combine_first(export_df.get('sortino_ratio'))
        export_df.to_csv(
            observation_csv,
            index=False,
            encoding='utf-8-sig',
        )
        self.last_bond_short_term_observation_base_filename = base_filename

        source_runs = collect_source_runs_from_stats_file(
            self.last_stats_file,
            source_file=observation_csv,
            source_kind='bond_short_term_one_year_observation',
        ) if self.last_stats_file else []
        report_meta_path = write_derived_report_meta(
            observation_csv,
            artifact_type='bond_short_term_one_year_observation_report',
            source_runs=source_runs,
            derived_from_files=[self.last_stats_file] if self.last_stats_file else [],
            extra_meta={
                'fund_type': canonicalize_runtime_fund_type_key(self.fund_type, default='bond'),
                'fund_type_name': self.fund_type_name,
                'period': self.period_config.get('name', f"{self.period_years}年"),
                'period_output_name': self.period_config.get('output_name', self.period_config.get('name')),
                'period_years': float(self.period_years) if self.period_years is not None else None,
                'period_days': int(self.period_days) if self.period_days is not None else None,
                'recommendation_status': 'watch_only',
                'list_tier': 'bond_new_observation',
                'watch_only_reason_code': 'BOND_SHORT_TERM_1Y_OBSERVABILITY',
                'observation_candidate_count': int(len(df_candidates)),
                'observation_scope': 'shadow_only',
                'enters_formal_mainlist': False,
                'enters_formal_candidate_frame': False,
                'enters_formal_decision_ledger': False,
                'enters_default_accumulation_main_track': False,
            },
            refresh_manifest=False,
        )

        cleanup_results = [
            cleanup_same_day_versions_for_file(
                observation_csv,
                refresh_manifest=False,
            ),
            cleanup_same_day_versions_for_file(
                report_meta_path,
                refresh_manifest=False,
            ),
        ]
        deleted_count = sum(
            int(item.get('deleted_count', 0) or 0)
            for item in cleanup_results
        )
        if deleted_count > 0:
            self.logger.info(
                "  🧹 同业务日旧版未满1Y观察产物已清理: %s 个文件",
                deleted_count,
            )

        rebuild_report_manifests_for_paths(observation_csv)
        return observation_csv, report_meta_path

    def _apply_layer10_selection(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        在评分后执行 Layer10 最终精选。

        这样可保证 Layer10 与最终排序使用同一评分口径，避免先按 combo_score 截断、
        后按 selected_combo_score 重排导致的候选集损失。
        """
        sort_col = 'selected_combo_score' if 'selected_combo_score' in df.columns else 'combo_score'
        df_layer10 = self.filtering_engine._layer10_final_selection(
            df,
            sort_col=sort_col,
            top_n=self.top_n,
        )
        self.filtering_engine.stats['total_end'] = len(df_layer10)
        # Layer10 会更新 filtering_engine.stats，这里同步回总统计
        self.stats['filter'] = self.filtering_engine.stats
        return df_layer10

    @capture_call('screening.scores', attributes=('period_years', 'period_days', 'score_key_override'))
    def _calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        阶段3: 计算评分

        Args:
            df: 筛选后的基金DataFrame

        Returns:
            添加了评分的DataFrame
        """
        self.logger.info("  正在计算双评分...")

        # 根据分析类型选择权重配置
        from config import WEIGHT_PROFILES
        weight_profile = WEIGHT_PROFILES.get(int(round(self.period_days)), 'standard')

        # 创建评分引擎（传递Profile配置 + 周期信息）
        # v1.9.57 P0-5: period_years 通过构造函数传递，替代外部猴子补丁
        self.scoring_engine = ScoringEngine(
            weight_profile=weight_profile,
            fund_type_profile=self.profile,
            period_years=self.period_years,
        )

        # Step 7-4: 设置策略选择属性（select_score_key 使用）
        self.scoring_engine._fund_type = canonicalize_runtime_fund_type_key(self.fund_type, default='bond')
        # v1.9.57 P1-2: 短周期使用天数作为策略键，避免 int(0.02)=0 导致查找失败
        if self.period_days <= 180:
            self.scoring_engine._period_years = self.period_days
        else:
            self.scoring_engine._period_years = int(self.period_years) if self.period_years else 3
        self.scoring_engine._score_key_override = self.score_key_override
        self.scoring_engine._policy_enabled = getattr(self, 'policy_enabled', False)

        # 计算评分
        df_scored = self.scoring_engine.calculate_scores(df)

        self.logger.info(f"  短期潜力分范围: {df_scored['short_term_score'].min():.2f} ~ {df_scored['short_term_score'].max():.2f}")
        self.logger.info(f"  长期价值分范围: {df_scored['long_term_score'].min():.2f} ~ {df_scored['long_term_score'].max():.2f}")
        self.logger.info(f"  组合评分范围: {df_scored['combo_score'].min():.2f} ~ {df_scored['combo_score'].max():.2f}")

        # 添加维度排名
        df_scored = self.scoring_engine.add_dimension_ranks(df_scored)

        # 存储统计（使用 _safe_float 防止 NaN 导致非法 JSON）
        self.stats['score'] = {
            'short_term_score': {
                'min': _safe_float(df_scored['short_term_score'].min()),
                'max': _safe_float(df_scored['short_term_score'].max()),
                'mean': _safe_float(df_scored['short_term_score'].mean()),
                'median': _safe_float(df_scored['short_term_score'].median()),
            },
            'long_term_score': {
                'min': _safe_float(df_scored['long_term_score'].min()),
                'max': _safe_float(df_scored['long_term_score'].max()),
                'mean': _safe_float(df_scored['long_term_score'].mean()),
                'median': _safe_float(df_scored['long_term_score'].median()),
            },
            'combo_score': {
                'min': _safe_float(df_scored['combo_score'].min()),
                'max': _safe_float(df_scored['combo_score'].max()),
                'mean': _safe_float(df_scored['combo_score'].mean()),
                'median': _safe_float(df_scored['combo_score'].median()),
            },
        }

        return df_scored

    @capture_call('screening.sorted_output', attributes=('period_years', 'period_days', 'top_n'))
    def _sort_and_analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        阶段4: 排序和统计分析

        Args:
            df: 评分后的基金DataFrame

        Returns:
            排序后的达标基金
        """
        # 按评分降序排序（Step 7-4: 优先使用 selected_combo_score，向后兼容 combo_score）
        sort_col = 'selected_combo_score' if 'selected_combo_score' in df.columns else 'combo_score'
        df_sorted = stable_sort_desc(df, sort_col).copy()

        if self.top_n is None:
            df_final = df_sorted.copy()
            self.logger.info(f"  排序完成，保留全部达标基金 {len(df_final)} 只")
        else:
            top_n = min(self.top_n, len(df_sorted))
            df_final = df_sorted.head(top_n).copy()
            self.logger.info(f"  排序完成，取Top {top_n}")

        # 统计分析
        self._analyze_statistics(df_final)

        return df_final

    def _analyze_statistics(self, df: pd.DataFrame):
        """生成统计分析"""
        stats = {}

        # 基本统计
        stats['count'] = len(df)
        stats['period'] = self.period_config.get('name', f"{self.period_years}年")

        # 空 DataFrame 时只记录基本信息，跳过数值统计
        if df.empty:
            self.stats['analysis'] = stats
            self.logger.info(f"\n  统计分析: 无数据（筛选后 0 只基金）")
            return

        # 收益率统计（使用 _safe_float 防止 NaN 导致非法 JSON）
        if 'main_return' in df.columns:
            stats['return'] = {
                'min': _safe_series_stat(df['main_return'], 'min'),
                'max': _safe_series_stat(df['main_return'], 'max'),
                'mean': _safe_series_stat(df['main_return'], 'mean'),
                'median': _safe_series_stat(df['main_return'], 'median'),
            }

        # 规模统计
        if 'scale_billion' in df.columns:
            stats['scale'] = {
                'min': _safe_series_stat(df['scale_billion'], 'min'),
                'max': _safe_series_stat(df['scale_billion'], 'max'),
                'mean': _safe_series_stat(df['scale_billion'], 'mean'),
                'median': _safe_series_stat(df['scale_billion'], 'median'),
                'total': _safe_series_stat(df['scale_billion'], 'sum'),
            }

        # 风险指标统计
        risk_metrics = ['sharpe_ratio', 'sortino_ratio', 'calmar_ratio',
                       'max_drawdown', 'volatility', 'consistency']
        stats['risk_metrics'] = {}
        for metric in risk_metrics:
            if metric in df.columns:
                stats['risk_metrics'][metric] = {
                    'min': _safe_series_stat(df[metric], 'min'),
                    'max': _safe_series_stat(df[metric], 'max'),
                    'mean': _safe_series_stat(df[metric], 'mean'),
                    'median': _safe_series_stat(df[metric], 'median'),
                }

        # 基金类型分布
        if 'ftype' in df.columns:
            type_dist = df['ftype'].value_counts().to_dict()
            stats['type_distribution'] = {str(k): int(v) for k, v in type_dist.items()}

        self.stats['analysis'] = stats

        # 打印部分统计信息（防御 _safe_float 返回 None 的情况）
        def _fmt(v, suffix=''):
            return f"{v:.2f}{suffix}" if v is not None else "N/A"

        self.logger.info(f"\n  统计分析:")
        if 'return' in stats:
            self.logger.info(f"    收益率: {_fmt(stats['return']['min'], '%')} ~ {_fmt(stats['return']['max'], '%')} (平均: {_fmt(stats['return']['mean'], '%')})")
        if 'scale' in stats:
            self.logger.info(f"    规模: {_fmt(stats['scale']['min'], '亿')} ~ {_fmt(stats['scale']['max'], '亿')} (总计: {_fmt(stats['scale']['total'], '亿')})")

    def _get_tracker(self):
        """返回当前 filtering_engine 上的 tracker；不存在时返回 None。"""
        if hasattr(self, 'filtering_engine') and self.filtering_engine:
            return getattr(self.filtering_engine, 'tracker', None)
        return None

    def _resolve_tracker_run_binding(self) -> tuple[Optional[str], bool]:
        """返回当前分析应绑定的 run_id 及其是否来自已持久化 run。"""
        tracker = self._get_tracker()
        if not tracker:
            return None, False
        if hasattr(tracker, 'get_persisted_run_id'):
            try:
                run_id = tracker.get_persisted_run_id()
            except Exception:
                run_id = None
            run_id = run_id.strip() if isinstance(run_id, str) else None
            if run_id:
                return run_id, True
        if hasattr(tracker, 'get_persistence_skip_context'):
            try:
                skip_context = tracker.get_persistence_skip_context()
            except Exception:
                skip_context = {}
            if not isinstance(skip_context, dict):
                skip_context = {}
            matched_run_id = str(skip_context.get('matched_run_id') or '').strip()
            if matched_run_id:
                return matched_run_id, True
        run_id = getattr(tracker, 'run_id', None)
        run_id = str(run_id).strip() if run_id else None
        return run_id or None, False

    def _get_tracker_run_id(self) -> Optional[str]:
        """返回当前分析运行应绑定的 run_id；不存在时返回 None。"""
        run_id, _ = self._resolve_tracker_run_binding()
        return run_id

    def _build_artifact_metadata(
        self,
        *,
        generated_at: Optional[datetime] = None,
        actual_output_count: Optional[int] = None,
        base_filename: Optional[str] = None,
    ) -> dict:
        """构建正式产物元数据，供 stats.json / 报告 sidecar 复用。"""
        generated_at = generated_at or datetime.now()
        fund_type_key = canonicalize_runtime_fund_type_key(self.fund_type, default='bond')
        actual_count = int(actual_output_count if actual_output_count is not None else 0)
        return {
            'generated_at': generated_at.isoformat(),
            'run_id': self._get_tracker_run_id(),
            'fund_type': str(fund_type_key),
            'fund_type_name': self.fund_type_name,
            'period': self.period_config.get('name', f"{self.period_years}年"),
            'period_output_name': self.period_config.get('output_name', self.period_config.get('name')),
            'period_years': float(self.period_years) if self.period_years is not None else None,
            'period_days': int(self.period_days) if self.period_days is not None else None,
            'top_n': actual_count,
            'requested_top_n': int(self.top_n) if self.top_n is not None else None,
            'actual_output_count': actual_count,
            'base_filename': base_filename,
        }

    def _write_stats_file(self, stats_file: Path):
        """统一写出 stats.json，确保后续回填字段也能落盘。"""
        atomic_write_json_file(stats_file, self.stats)

    def _cleanup_same_day_output_family(self, base_filename: str) -> int:
        """清理同业务日旧版单周期分析产物。"""
        match = re.match(r'^(?P<prefix>.+)_(?P<day>20\d{6})_(?P<time>\d{6})$', str(base_filename))
        if not match:
            return 0

        prefix = str(match.group('prefix') or '').strip()
        day_token = str(match.group('day') or '').strip()
        if not prefix or not day_token:
            return 0

        deleted_count = 0

        for tail, suffix in (('_simple', '.csv'), ('_stats', '.json')):
            summary = cleanup_same_day_family_versions(
                self.output_dir,
                prefix=prefix,
                day_token=day_token,
                tail=tail,
                suffix=suffix,
            )
            deleted_count += int(summary.get('deleted_count', 0) or 0)

        summary = cleanup_same_day_family_versions(
            self.output_dir,
            prefix=prefix,
            day_token=day_token,
            tail='',
            suffix='.csv',
        )
        deleted_count += int(summary.get('deleted_count', 0) or 0)

        return deleted_count

    def register_excel_report(self, excel_file: Union[str, Path]) -> Optional[Path]:
        """
        为 Excel 正式报告写出 sidecar 元数据，并回填到 stats.json。

        返回:
            sidecar 元数据文件路径；若无有效路径则返回 None。
        """
        report_path = Path(excel_file) if excel_file else None
        if report_path is None:
            return None

        report_meta_path = report_path.parent / f"{report_path.stem}_report_meta.json"
        output_files = dict(self.stats.get('output_files') or {})
        stats_file_raw = output_files.get('stats')
        stats_file = Path(stats_file_raw) if stats_file_raw else None

        artifact_meta = self._build_artifact_metadata(
            generated_at=datetime.now(),
            actual_output_count=self.stats.get('actual_output_count'),
            base_filename=self.last_output_base_filename,
        )
        artifact_meta.update({
            'report_file': serialize_output_path_reference(report_path, output_dir=report_path.parent),
            'report_filename': report_path.name,
            'stats_file': serialize_output_path_reference(stats_file, output_dir=report_path.parent) if stats_file else None,
            'csv_file': serialize_output_path_reference(output_files.get('csv'), output_dir=report_path.parent),
            'simple_csv_file': serialize_output_path_reference(output_files.get('simple_csv'), output_dir=report_path.parent),
        })

        atomic_write_json_file(report_meta_path, artifact_meta)

        output_files['excel'] = str(report_path)
        output_files['report_meta'] = str(report_meta_path)
        self.stats['output_files'] = output_files
        self.stats['excel_file'] = str(report_path)
        self.stats['report_file'] = str(report_path)
        self.stats['report_meta_file'] = str(report_meta_path)

        existing_artifact_meta = self.stats.get('artifact_meta')
        if isinstance(existing_artifact_meta, dict):
            existing_artifact_meta.update({
                'report_file': str(report_path),
                'report_meta_file': str(report_meta_path),
            })
            self.stats['artifact_meta'] = existing_artifact_meta

        if stats_file:
            self._write_stats_file(stats_file)

        cleanup_summary = cleanup_same_day_versions_for_file(
            report_path,
            refresh_manifest=False,
        )
        if cleanup_summary.get('deleted_count', 0) > 0:
            self.logger.info(
                "  🧹 同业务日旧版 Excel 报告已清理: %s 个文件",
                cleanup_summary['deleted_count'],
            )

        rebuild_report_manifests_for_paths(report_path)
        return report_meta_path

    def _save_results(self, df: pd.DataFrame):
        """
        保存分析结果

        Args:
            df: 最终结果DataFrame
        """
        # 转换基金类型为中文
        if 'ftype' in df.columns:
            df['ftype'] = df['ftype'].apply(
                lambda x: FUND_TYPE_MAPPING.get(str(x), FUND_TYPE_MAPPING.get(int(x) if pd.notna(x) and str(x).isdigit() else x, str(x)))
            )

        # 生成文件名
        generated_at = datetime.now()
        timestamp = generated_at.strftime("%Y%m%d_%H%M%S")
        if self.top_n is None:
            selection_tag = f"达标全量{len(df)}"
        else:
            selection_tag = f"Top{len(df)}"
        base_filename = f"{self.fund_type_name}基金_{self.period_config['output_name']}_{selection_tag}_{timestamp}"

        # 保存CSV
        csv_file = self.output_dir / f"{base_filename}.csv"
        stats_file = self.output_dir / f"{base_filename}_stats.json"
        simple_csv = self.output_dir / f"{base_filename}_simple.csv"
        self.last_output_base_filename = base_filename
        self.last_stats_file = stats_file

        self.stats['output_files'] = {
            'csv': str(csv_file),
            'stats': str(stats_file),
            'simple_csv': str(simple_csv),
        }
        artifact_meta = self._build_artifact_metadata(
            generated_at=generated_at,
            actual_output_count=len(df),
            base_filename=base_filename,
        )
        self.stats.update({
            'generated_at': artifact_meta['generated_at'],
            'run_id': artifact_meta['run_id'],
            'fund_type': artifact_meta['fund_type'],
            'fund_type_name': artifact_meta['fund_type_name'],
            'period': artifact_meta['period'],
            'period_years': artifact_meta['period_years'],
            'period_days': artifact_meta['period_days'],
            'top_n': artifact_meta['top_n'],
            'requested_top_n': artifact_meta['requested_top_n'],
            'actual_output_count': artifact_meta['actual_output_count'],
            'artifact_meta': artifact_meta,
        })

        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        self.logger.info(f"  💾 CSV文件: {csv_file}")

        # 保存简化版（只保留关键列）
        # v1.9.59 P1-2: 补齐全部风险指标列（sortino_ratio, calmar_ratio, consistency）
        # v1.9.60: 添加 total_fee_rate 和 max_drawdown_duration
        key_columns = [
            'fcode', 'shortname', 'ftype',
            'main_return', 'scale_billion', 'total_fee_rate',
            'raw_sharpe_ratio', 'raw_sortino_ratio',
            'sharpe_ratio', 'sortino_ratio', 'calmar_ratio',
            'max_drawdown', 'max_drawdown_duration', 'volatility', 'consistency',
            'short_term_score', 'long_term_score',
            'combo_score', 'selected_combo_score',
            'selected_score_key', 'selected_score_reason', 'selected_score_fallback',
        ]
        available_columns = [col for col in key_columns if col in df.columns]
        df_simple = df[available_columns].copy()
        if 'raw_sharpe_ratio' in df_simple.columns:
            df_simple['sharpe_ratio'] = df_simple['raw_sharpe_ratio'].combine_first(df_simple.get('sharpe_ratio'))
        if 'raw_sortino_ratio' in df_simple.columns:
            df_simple['sortino_ratio'] = df_simple['raw_sortino_ratio'].combine_first(df_simple.get('sortino_ratio'))
        df_simple.to_csv(simple_csv, index=False, encoding='utf-8-sig')
        self.logger.info(f"  💾 简化版CSV: {simple_csv}")

        # 保存统计信息JSON
        self._write_stats_file(stats_file)
        self.logger.info(f"  💾 统计信息: {stats_file}")

        observation_csv = None
        observation_report_meta = None
        try:
            observation_candidates = self._build_bond_short_term_one_year_observation_candidates()
            observation_csv, observation_report_meta = (
                self._export_bond_short_term_one_year_observation_candidates(
                    observation_candidates
                )
            )
        except Exception as e:
            self.logger.warning(f"  ⚠️ 未满1Y独立观察产物写出失败（不影响主流程）: {e}")

        if observation_csv:
            self.stats['output_files']['bond_short_term_one_year_observation_csv'] = str(observation_csv)
        if observation_report_meta:
            self.stats['output_files']['bond_short_term_one_year_observation_report_meta'] = str(
                observation_report_meta
            )
        if observation_csv or observation_report_meta:
            self._write_stats_file(stats_file)

        deleted_count = self._cleanup_same_day_output_family(base_filename)
        if deleted_count > 0:
            self.logger.info(
                "  🧹 同业务日旧版单周期产物已清理: %s 个文件",
                deleted_count,
            )

        # Step 8-1: 写入 screening_picks 表（容错隔离）
        try:
            self._save_screening_picks(df)
        except Exception as e:
            self.logger.warning(f"  ⚠️ screening_picks 写入失败（不影响主流程）: {e}")

    def _print_summary(self, df: pd.DataFrame):
        """打印分析总结"""
        pass  # 由子类实现或使用 Excel 输出

    # Step 8-1: 写入 screening_picks
    def _save_screening_picks(self, df: pd.DataFrame):
        """
        将TopK结果写入screening_picks表，供前端展示

        容错隔离：失败不影响主流程
        """
        # 获取 run_id（来自 FilteringEngine.tracker）；重复命中时复用 matched_run_id 供产物 lineage 绑定，
        # 但不应再次向 screening_picks 追加同一 run 的重复记录。
        run_id, run_binding_persisted = self._resolve_tracker_run_binding()
        tracker = self._get_tracker()

        if not run_id:
            self.logger.debug("  ℹ️ 无 run_id，跳过 screening_picks 写入")
            return

        duplicate_rebind = False
        if tracker and hasattr(tracker, 'get_persisted_run_id') and hasattr(tracker, 'get_persistence_skip_context'):
            try:
                persisted_run_id = tracker.get_persisted_run_id()
            except Exception:
                persisted_run_id = None
            try:
                skip_context = tracker.get_persistence_skip_context()
            except Exception:
                skip_context = {}
            if not isinstance(skip_context, dict):
                skip_context = {}
            duplicate_rebind = (
                persisted_run_id is None
                and skip_context.get('reason') == 'duplicate_recent_success'
                and str(skip_context.get('matched_run_id') or '').strip() == run_id
            )

        if duplicate_rebind:
            self.logger.info(
                "  ℹ️ 检测到重复成功 run，复用已有 run_id=%s，跳过新增 screening_picks",
                f"{run_id[:8]}...",
            )
            return

        # v1.9.64: 统一日志目录权限检查与降级
        import os
        resolved_log_dir = resolve_log_dir(str(Path(__file__).parent.parent.parent / 'logs'))
        os.environ.setdefault('LOG_DIR_OVERRIDE', resolved_log_dir)

        # 使用共享模块导入，避免每次运行都重新加载 connection.py 导致单例失效。
        from database.connection import get_db_manager
        from database.models import ScreeningPick, ScreeningRun

        db_manager = get_db_manager()
        fund_type = canonicalize_runtime_fund_type_key(self.fund_type, default='bond')
        period_years = self.period_years
        now = datetime.now()

        picks = []
        for rank_idx, (_, row) in enumerate(df.iterrows(), start=1):
            # Step 8-5: 构建筛选时刻快照
            snapshot = self._build_pick_snapshot(row, fund_type)
            snapshot_str = None
            try:
                import json as _json
                snapshot_str = _json.dumps(snapshot, ensure_ascii=False, default=str)
            except Exception:
                pass

            pick = ScreeningPick(
                run_id=run_id,
                fcode=str(row.get('fcode', '')),
                rank=rank_idx,
                fund_type=fund_type,
                period_years=period_years,
                shortname=str(row.get('shortname', ''))[:100],
                ftype=str(row.get('ftype', ''))[:50] if pd.notna(row.get('ftype')) else None,
                # v1.9.60 P2-1: 统一使用 row.get 容错，避免列缺失时 KeyError
                scale_billion=float(row.get('scale_billion')) if pd.notna(row.get('scale_billion')) else None,
                main_return=float(row.get('main_return')) if pd.notna(row.get('main_return')) else None,
                combo_score=float(row.get('combo_score')) if pd.notna(row.get('combo_score')) else None,
                short_term_score=float(row.get('short_term_score')) if pd.notna(row.get('short_term_score')) else None,
                long_term_score=float(row.get('long_term_score')) if pd.notna(row.get('long_term_score')) else None,
                selected_combo_score=float(row.get('selected_combo_score')) if pd.notna(row.get('selected_combo_score')) else None,
                selected_score_key=str(row.get('selected_score_key')) if pd.notna(row.get('selected_score_key')) else None,
                sharpe_ratio=float(row.get('sharpe_ratio')) if pd.notna(row.get('sharpe_ratio')) else None,
                max_drawdown=float(row.get('max_drawdown')) if pd.notna(row.get('max_drawdown')) else None,
                volatility=float(row.get('volatility')) if pd.notna(row.get('volatility')) else None,
                snapshot_json=snapshot_str,
                picked_at=now,
            )
            picks.append(pick)

        with db_manager.get_session() as session:
            session.bulk_save_objects(picks)

            # 记录 run 与 picks 的口径信息，避免将 Layer10 通过数误解为最终导出数。
            run = session.query(ScreeningRun).filter_by(run_id=run_id).first()
            if run:
                actual_pick_count = len(picks)
                fallback_series = df.get('selected_score_fallback')
                fallback_reason_series = df.get('selected_score_reason')
                selected_score_keys = df.get('selected_score_key')

                selected_score_fallback_count = 0
                selected_score_fallback_reason_dist = {}
                selected_score_key_dist = {}

                if fallback_series is not None:
                    selected_score_fallback_count = int(fallback_series.fillna(False).astype(bool).sum())

                if fallback_reason_series is not None:
                    selected_score_fallback_reason_dist = {
                        str(k): int(v)
                        for k, v in fallback_reason_series.dropna().astype(str).value_counts().items()
                        if str(k) and str(k) != 'override_used'
                    }

                if selected_score_keys is not None:
                    selected_score_key_dist = {
                        str(k): int(v)
                        for k, v in selected_score_keys.dropna().astype(str).value_counts().items()
                    }

                # 在 config_snapshot_json 增补输出口径信息，便于追溯 top_n 来源
                try:
                    snapshot = json.loads(run.config_snapshot_json) if run.config_snapshot_json else {}
                    output_meta = snapshot.get('output_meta', {})
                    effective_selected_score_key = None
                    if selected_score_key_dist:
                        try:
                            effective_selected_score_key = max(
                                selected_score_key_dist.items(),
                                key=lambda item: item[1],
                            )[0]
                        except Exception:
                            effective_selected_score_key = None

                    # run.fund_count_pass 在当前调用时点可能尚未由 finish_run()
                    # 写回数据库；此时优先使用已知的最终导出数，避免写成 0。
                    run_fund_count_pass = int(run.fund_count_pass or 0)
                    if run_fund_count_pass <= 0:
                        run_fund_count_pass = actual_pick_count

                    output_meta.update({
                        'requested_top_n': int(self.top_n) if self.top_n is not None else None,
                        'actual_pick_count': actual_pick_count,
                        'run_fund_count_pass': run_fund_count_pass,
                        'period_years': float(self.period_years),
                        'fund_type': str(fund_type),
                        'selected_score_fallback_count': selected_score_fallback_count,
                        'selected_score_fallback_reason_dist': selected_score_fallback_reason_dist,
                        'selected_score_key_dist': selected_score_key_dist,
                    })
                    snapshot['output_meta'] = output_meta
                    scoring_policy_meta = snapshot.get('scoring_policy', {})
                    if isinstance(scoring_policy_meta, dict):
                        scoring_policy_meta['selected_score_key'] = (
                            effective_selected_score_key
                            or self.score_key_override
                            or 'combo_score'
                        )
                        scoring_policy_meta['policy_source'] = self.policy_source
                        scoring_policy_meta['policy_override_used'] = bool(self.policy_override_used)
                        scoring_policy_meta['policy_override_value'] = self.policy_override_value
                        snapshot['scoring_policy'] = scoring_policy_meta
                    run.config_snapshot_json = json.dumps(snapshot, sort_keys=True, ensure_ascii=False)
                except Exception:
                    pass

        if tracker:
            selected_fcodes = []
            if 'fcode' in df.columns:
                selected_fcodes = [str(fcode) for fcode in df['fcode'].dropna().tolist()]
            persisted_decision_count = tracker.persist_selected_decisions(selected_fcodes)
            if persisted_decision_count > 0:
                self.logger.info(
                    "  💾 已定向写入入选基金筛选路径: %s 条 decision",
                    persisted_decision_count,
                )

        try:
            from database.screening_run_storage_stats import refresh_screening_run_storage_stats

            refresh_screening_run_storage_stats(
                db_manager=db_manager,
                run_ids=[run_id],
                stats_source='save_screening_picks',
            )
        except Exception:
            self.logger.debug("  ℹ️ screening_run_storage_stats 刷新失败，已跳过", exc_info=True)

        try:
            from database.screening_pick_metric_snapshots import refresh_screening_pick_metric_snapshots

            refresh_screening_pick_metric_snapshots(
                db_manager=db_manager,
                run_ids=[run_id],
                stats_source='save_screening_picks',
            )
        except Exception:
            self.logger.debug("  ℹ️ screening_pick_metric_snapshots 刷新失败，已跳过", exc_info=True)

        try:
            from database.screening_decision_summaries import refresh_screening_decision_summaries

            refresh_screening_decision_summaries(
                db_manager=db_manager,
                run_ids=[run_id],
                stats_source='save_screening_picks',
            )
        except Exception:
            self.logger.debug("  ℹ️ screening_decision_summaries 刷新失败，已跳过", exc_info=True)

        self.logger.info(f"  💾 screening_picks: {len(picks)} 条写入 (run_id={run_id[:8]}...)")

    def _build_pick_snapshot(self, row, fund_type: str) -> dict:
        """
        构建单只基金的筛选时刻快照。

        从 df_result 行中提取 freshness、risk_metrics、returns、coverage 信息，
        并计算 investability flags。

        容错设计：任何子段失败返回空字典，不影响整体快照。
        """
        snapshot = {}

        def _parse_json_listish(value):
            if value is None or not pd.notna(value):
                return None
            if isinstance(value, (list, tuple)):
                return [str(item) for item in value]
            if isinstance(value, str):
                text = value.strip()
                if not text:
                    return None
                try:
                    parsed = json.loads(text)
                except (TypeError, ValueError, json.JSONDecodeError):
                    return [text]
                if isinstance(parsed, (list, tuple)):
                    return [str(item) for item in parsed]
                if parsed is None:
                    return None
                return [str(parsed)]
            return [str(value)]

        # 1. freshness 段
        try:
            snapshot['nav_latest_date'] = str(row.get('nav_latest_date', '')) if pd.notna(row.get('nav_latest_date')) else None
            snapshot['is_fresh'] = bool(row.get('is_fresh')) if pd.notna(row.get('is_fresh')) else None
            snapshot['nav_age_days'] = int(row.get('nav_age_days')) if pd.notna(row.get('nav_age_days')) else None
            snapshot['freshness_reason'] = str(row.get('freshness_reason')) if pd.notna(row.get('freshness_reason')) else None
            snapshot['data_cutoff_date'] = self.as_of_date.isoformat() if getattr(self, 'as_of_date', None) else None
        except Exception:
            pass

        # 2. risk_metrics 段
        try:
            risk_keys = ['sharpe_ratio', 'sortino_ratio', 'calmar_ratio',
                         'max_drawdown', 'max_drawdown_duration', 'volatility',
                         'consistency', 'positive_month_ratio',
                         'raw_sharpe_ratio', 'raw_sortino_ratio',
                         'sharpe_ratio_cap_hit', 'sortino_ratio_cap_hit']
            risk = {}
            for k in risk_keys:
                v = row.get(k)
                if v is not None and pd.notna(v):
                    if k.endswith('_cap_hit'):
                        risk[k] = bool(v)
                    else:
                        risk[k] = float(v)
            snapshot['risk_metrics'] = risk
        except Exception:
            snapshot['risk_metrics'] = {}

        # 3. returns 段
        try:
            ret_keys = ['return_1w', 'return_1m', 'return_3m', 'return_6m',
                        'return_1y', 'return_2y', 'return_3y', 'return_5y',
                        'return_ytd', 'return_since', 'main_return']
            returns = {}
            for k in ret_keys:
                v = row.get(k)
                if v is not None and pd.notna(v):
                    returns[k] = float(v)
            snapshot['returns'] = returns
        except Exception:
            snapshot['returns'] = {}

        # 4. coverage 段
        try:
            snapshot['coverage'] = {
                'is_fallback': bool(row['is_fallback']) if pd.notna(row.get('is_fallback')) else False,
            }
            # 若有 bucket 信息
            if pd.notna(row.get('bucket_key')):
                snapshot['coverage']['bucket_key'] = str(row['bucket_key'])
            if pd.notna(row.get('bucket_size')):
                snapshot['coverage']['bucket_size'] = int(row['bucket_size'])
            # Step 8-6: 补齐 coverage_ratio / coverage_days / period_days
            period_days = round(getattr(self, 'period_years', 0) * 365) if getattr(self, 'period_years', None) else None
            snapshot['coverage']['period_days'] = period_days
            # coverage_days = nav_count（第6层计算风险指标时记录的实际净值点数）
            nav_count_val = row.get('nav_count')
            if nav_count_val is not None and pd.notna(nav_count_val):
                coverage_days = int(nav_count_val)
                snapshot['coverage']['coverage_days'] = coverage_days
                if period_days and period_days > 0:
                    snapshot['coverage']['coverage_ratio'] = round(coverage_days / period_days, 4)
                else:
                    snapshot['coverage']['coverage_ratio'] = None
            else:
                snapshot['coverage']['coverage_days'] = None
                snapshot['coverage']['coverage_ratio'] = None
        except Exception:
            snapshot['coverage'] = {}

        # 5. 评分策略与筛选追溯
        try:
            if pd.notna(row.get('selected_score_key')):
                snapshot['selected_score_key'] = str(row.get('selected_score_key'))
            if pd.notna(row.get('selected_combo_score')):
                snapshot['selected_combo_score'] = float(row.get('selected_combo_score'))
            if pd.notna(row.get('selected_score_fallback')):
                snapshot['selected_score_fallback'] = bool(row.get('selected_score_fallback'))
            if pd.notna(row.get('selected_score_reason')):
                snapshot['selected_score_reason'] = str(row.get('selected_score_reason'))
            if pd.notna(row.get('pass_periods')):
                snapshot['pass_periods'] = int(row.get('pass_periods'))
            if pd.notna(row.get('main_return_mapped_from')):
                snapshot['main_return_mapped_from'] = str(row.get('main_return_mapped_from'))
            if pd.notna(row.get('risk_qc_issue_count')):
                snapshot['risk_qc_issue_count'] = int(row.get('risk_qc_issue_count'))
            windows_used = _parse_json_listish(row.get('windows_used'))
            if windows_used:
                snapshot['windows_used'] = windows_used
        except Exception:
            pass

        # 6. scale
        try:
            snapshot['scale_billion'] = float(row['scale_billion']) if pd.notna(row.get('scale_billion')) else None
        except Exception:
            pass

        # 7. is_fallback 顶层快捷访问
        try:
            snapshot['is_fallback'] = bool(row['is_fallback']) if pd.notna(row.get('is_fallback')) else False
        except Exception:
            snapshot['is_fallback'] = False

        # 8. investability 段（纯函数计算，容错）
        try:
            from bond_fund_screening.core.investability import evaluate_investability_flags
            inv = evaluate_investability_flags(snapshot, fund_type=fund_type)
            snapshot['investability'] = inv
        except Exception:
            snapshot['investability'] = {'verdict': 'unknown', 'flags': []}

        return snapshot


# ============================================================================
# 测试函数
# ============================================================================

def test_base_analyzer():
    """测试基础分析器"""
    print("="*80)
    print("测试 BaseAnalyzer 模块")
    print("="*80)

    try:
        # 测试3年期分析
        print("\n【测试】运行3年期分析（限制输出50只基金）")
        analyzer = BaseAnalyzer(
            period_years=3,
            top_n=50,
            verbose=True
        )

        df_result, stats = analyzer.run()

        print("\n【结果】")
        print(f"最终输出基金数: {len(df_result)}")
        print(f"统计信息keys: {list(stats.keys())}")

        if len(df_result) > 0:
            print("\nTop 5基金:")
            print(df_result.head()[['fcode', 'shortname', 'main_return', 'combo_score']])

        print("\n" + "="*80)
        print("✅ BaseAnalyzer 测试完成")
        print("="*80)

    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_base_analyzer()
