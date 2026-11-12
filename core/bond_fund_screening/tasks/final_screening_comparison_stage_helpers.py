#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选对比报告前半段编排辅助工具。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass(slots=True)
class ComparisonCrossSourceContext:
    """近期/长期 cross-source 补分上下文。"""

    short_scores: dict
    long_scores: dict
    short_period_counts: dict
    long_period_counts: dict
    short_base_scores: dict
    long_base_scores: dict
    short_base_period_counts: dict
    long_base_period_counts: dict


@dataclass(slots=True)
class ComparisonInputAnalysisResult:
    """对比报告输入分析阶段结果。"""

    df_short: pd.DataFrame
    df_long: pd.DataFrame
    code_stats: dict
    fund_type: str
    input_lineage: dict
    short_report_date: str | None
    long_report_date: str | None
    short_score_col: str | None
    long_score_col: str | None
    cross_source: ComparisonCrossSourceContext
    final_policy: dict
    df_weighted: pd.DataFrame
    candidate_summary: dict
    intersection_base_names: set[str]
    effective_only_short: set[str]
    effective_only_long: set[str]


@dataclass(slots=True)
class ComparisonReportExportResult:
    """对比报告导出阶段结果。"""

    timestamp: str
    output_file: Path
    csv_short: Path | None
    csv_long: Path | None


def _collect_category_codes(
    df_weighted: pd.DataFrame,
    *,
    category: str,
    normalize_fcode_fn,
) -> set[str]:
    """从加权候选池中提取指定类别的基金代码集合。"""
    if df_weighted.empty or 'fund_category' not in df_weighted.columns:
        return set()

    return {
        normalize_fcode_fn(value)
        for value in df_weighted.loc[
            df_weighted['fund_category'] == category,
            'fcode',
        ].tolist()
        if normalize_fcode_fn(value)
    }


def run_comparison_input_analysis_stage(
    *,
    short_term_file: Path,
    long_term_file: Path,
    today_date: str | None,
    min_cross_periods: int,
    load_final_screening_input_frame_fn,
    normalize_fcodes_in_df_fn,
    ensure_base_fund_name_column_fn,
    build_final_screening_code_stats_fn,
    infer_fund_type_fn,
    resolve_final_screening_direct_score_column_fn,
    build_input_lineage_meta_fn,
    load_cross_scores_fn,
    load_cross_period_counts_fn,
    peek_cross_score_base_maps_fn,
    peek_cross_period_count_base_maps_fn,
    resolve_final_screening_policy_fn,
    build_weighted_final_candidate_frame_fn,
    normalize_fcode_fn,
    print_report_step_header_fn,
    print_code_stats_fn,
    print_input_lineage_summary_fn,
    print_cross_score_summary_fn,
    print_cross_score_degraded_fn,
    print_candidate_summary_fn,
) -> dict:
    """执行输入读取、血缘识别、交并差分析与主榜候选构建。"""
    print_report_step_header_fn("步骤1: 读取Excel文件")

    df_short = load_final_screening_input_frame_fn(
        short_term_file,
        report_label='近期优质基金',
    )
    df_long = load_final_screening_input_frame_fn(
        long_term_file,
        report_label='长期优质基金',
    )

    df_short = normalize_fcodes_in_df_fn(df_short, "近期优质基金")
    df_long = normalize_fcodes_in_df_fn(df_long, "长期优质基金")
    df_short = ensure_base_fund_name_column_fn(df_short)
    df_long = ensure_base_fund_name_column_fn(df_long)

    print_report_step_header_fn("步骤2: 计算交集和差集")
    code_stats = build_final_screening_code_stats_fn(df_short, df_long)
    print_code_stats_fn(code_stats)

    fund_type = infer_fund_type_fn(short_term_file, long_term_file)
    short_score_col = resolve_final_screening_direct_score_column_fn(
        df_short,
        fund_type=fund_type,
    )
    long_score_col = resolve_final_screening_direct_score_column_fn(
        df_long,
        fund_type=fund_type,
    )

    cross_short_scores, cross_long_scores = {}, {}
    cross_short_period_counts, cross_long_period_counts = {}, {}
    cross_short_base_scores, cross_long_base_scores = {}, {}
    cross_short_base_period_counts, cross_long_base_period_counts = {}, {}

    input_lineage = build_input_lineage_meta_fn(
        short_term_file,
        long_term_file,
        today_date=today_date,
    )
    short_report_date = input_lineage['short_report_date']
    long_report_date = input_lineage['long_report_date']
    print_input_lineage_summary_fn(
        short_report_date=short_report_date,
        long_report_date=long_report_date,
        input_lineage=input_lineage,
    )

    try:
        cross_short_scores, cross_long_scores = load_cross_scores_fn(
            fund_type,
            short_target_date=short_report_date,
            long_target_date=long_report_date,
        )
        cross_short_period_counts, cross_long_period_counts = (
            load_cross_period_counts_fn(
                fund_type,
                short_target_date=short_report_date,
                long_target_date=long_report_date,
            )
        )
        cross_short_base_scores, cross_long_base_scores = (
            peek_cross_score_base_maps_fn()
        )
        (
            cross_short_base_period_counts,
            cross_long_base_period_counts,
        ) = peek_cross_period_count_base_maps_fn()
        print_cross_score_summary_fn(
            fund_type=fund_type,
            cross_short_scores=cross_short_scores,
            cross_long_scores=cross_long_scores,
            min_cross_periods=min_cross_periods,
        )
    except Exception as exc:
        print_cross_score_degraded_fn(exc)

    final_policy = resolve_final_screening_policy_fn(fund_type=fund_type)
    df_weighted, candidate_summary = build_weighted_final_candidate_frame_fn(
        df_short=df_short,
        df_long=df_long,
        fund_type=fund_type,
        short_score_col=short_score_col,
        long_score_col=long_score_col,
        cross_short_scores=cross_short_scores,
        cross_long_scores=cross_long_scores,
        cross_short_period_counts=cross_short_period_counts,
        cross_long_period_counts=cross_long_period_counts,
        cross_short_base_scores=cross_short_base_scores,
        cross_long_base_scores=cross_long_base_scores,
        cross_short_base_period_counts=cross_short_base_period_counts,
        cross_long_base_period_counts=cross_long_base_period_counts,
        policy=final_policy,
        min_cross_periods=min_cross_periods,
    )

    intersection_base_names = {
        str(value).strip()
        for value in df_weighted.loc[
            df_weighted['fund_category'] == '交集优选',
            'base_fund_name',
        ].tolist()
        if str(value).strip()
    }
    effective_only_short = _collect_category_codes(
        df_weighted,
        category='近期优质',
        normalize_fcode_fn=normalize_fcode_fn,
    )
    effective_only_long = _collect_category_codes(
        df_weighted,
        category='长期优质',
        normalize_fcode_fn=normalize_fcode_fn,
    )

    print_candidate_summary_fn(candidate_summary)

    return ComparisonInputAnalysisResult(
        df_short=df_short,
        df_long=df_long,
        code_stats=code_stats,
        fund_type=fund_type,
        input_lineage=input_lineage,
        short_report_date=short_report_date,
        long_report_date=long_report_date,
        short_score_col=short_score_col,
        long_score_col=long_score_col,
        cross_source=ComparisonCrossSourceContext(
            short_scores=cross_short_scores,
            long_scores=cross_long_scores,
            short_period_counts=cross_short_period_counts,
            long_period_counts=cross_long_period_counts,
            short_base_scores=cross_short_base_scores,
            long_base_scores=cross_long_base_scores,
            short_base_period_counts=cross_short_base_period_counts,
            long_base_period_counts=cross_long_base_period_counts,
        ),
        final_policy=final_policy,
        df_weighted=df_weighted,
        candidate_summary=candidate_summary,
        intersection_base_names=intersection_base_names,
        effective_only_short=effective_only_short,
        effective_only_long=effective_only_long,
    )


def run_comparison_report_export_stage(
    *,
    output_dir: Path,
    output_prefix: str | None,
    output_timestamp: str | None,
    short_term_file: Path,
    long_term_file: Path,
    df_short: pd.DataFrame,
    df_long: pd.DataFrame,
    code_stats: dict,
    candidate_summary: dict,
    input_lineage: dict,
    short_report_date: str | None,
    long_report_date: str | None,
    intersection_base_names: set[str],
    effective_only_short: set[str],
    effective_only_long: set[str],
    build_intersection_sheet_fn,
    build_single_side_sheet_fn,
    build_summary_sheet_fn,
    build_final_output_name_fn,
    write_comparison_report_workbook_fn,
    export_simple_csv_fn,
    print_report_step_header_fn,
    print_sheet_preparation_done_fn,
    print_comparison_report_written_fn,
    print_markdown_skipped_fn,
    print_simple_csv_written_fn,
) -> dict:
    """执行对比表格组装、Excel 写出与简版 CSV 导出。"""
    print_report_step_header_fn("步骤3: 准备各个对比表格")

    df_sheet1 = df_short.copy()
    df_sheet2 = df_long.copy()
    df_sheet3 = build_intersection_sheet_fn(
        df_short=df_short,
        df_long=df_long,
        intersection_base_names=intersection_base_names,
    )
    df_sheet4 = build_single_side_sheet_fn(
        df_short,
        effective_codes=effective_only_short,
        empty_message='所有近期优质基金都在长期优质列表中',
    )
    df_sheet5 = build_single_side_sheet_fn(
        df_long,
        effective_codes=effective_only_long,
        empty_message='所有长期优质基金都在近期优质列表中',
    )
    df_sheet6 = build_summary_sheet_fn(
        code_stats=code_stats,
        candidate_summary=candidate_summary,
        input_lineage=input_lineage,
        short_term_file=short_term_file,
        long_term_file=long_term_file,
        short_report_date=short_report_date,
        long_report_date=long_report_date,
    )
    print_sheet_preparation_done_fn()

    print_report_step_header_fn("步骤4: 生成Excel对比报告")
    timestamp = output_timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / build_final_output_name_fn(
        f"近期与长期优质基金对比报告_{timestamp}.xlsx",
        output_prefix=output_prefix,
    )
    write_comparison_report_workbook_fn(
        output_file=output_file,
        sheet_frames=(
            ('1-近期优质基金', df_sheet1),
            ('2-长期优质基金', df_sheet2),
            ('3-交集基金', df_sheet3),
            ('4-仅近期优质', df_sheet4),
            ('5-仅长期优质', df_sheet5),
            ('6-统计汇总', df_sheet6),
        ),
    )
    print_comparison_report_written_fn(output_file)

    print_report_step_header_fn("步骤5: 跳过Markdown对比报告")
    print_markdown_skipped_fn()

    print_report_step_header_fn("步骤6: 生成简化CSV文件")
    csv_short = None
    csv_long = None

    if effective_only_short:
        csv_short = output_dir / build_final_output_name_fn(
            f"仅近期优质_简版_{timestamp}.csv",
            output_prefix=output_prefix,
        )
        export_simple_csv_fn(df_sheet4, csv_short)
        print_simple_csv_written_fn(
            csv_short,
            label='仅近期优质CSV',
        )

    if effective_only_long:
        csv_long = output_dir / build_final_output_name_fn(
            f"仅长期优质_简版_{timestamp}.csv",
            output_prefix=output_prefix,
        )
        export_simple_csv_fn(df_sheet5, csv_long)
        print_simple_csv_written_fn(
            csv_long,
            label='仅长期优质CSV',
        )

    return ComparisonReportExportResult(
        timestamp=timestamp,
        output_file=output_file,
        csv_short=csv_short,
        csv_long=csv_long,
    )
