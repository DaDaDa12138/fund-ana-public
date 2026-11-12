#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终筛选任务控制台日志辅助工具。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


def _emit_lines(lines, *, print_fn=print) -> None:
    """按顺序输出多行日志。"""
    for line in lines:
        print_fn(line)


def print_enhanced_comparison_report_header(*, print_fn=print) -> None:
    """打印增强版对比分析标题。"""
    _emit_lines(
        [
            f"\n{'=' * 80}",
            "【增强版对比分析】",
            f"{'=' * 80}",
        ],
        print_fn=print_fn,
    )


def print_report_step_header(step_title: str, *, print_fn=print) -> None:
    """打印对比报告阶段标题。"""
    _emit_lines(
        [
            f"\n{step_title}",
            "-" * 80,
        ],
        print_fn=print_fn,
    )


def print_code_stats(code_stats: dict, *, print_fn=print) -> None:
    """打印长短期输入与交并集摘要。"""
    shared_base_names = code_stats.get('shared_base_names') or set()
    _emit_lines(
        [
            f"  近期优质基金: {len(code_stats.get('codes_short') or set())} 只（母基金 {len(code_stats.get('short_base_names') or set())} 只）",
            f"  长期优质基金: {len(code_stats.get('codes_long') or set())} 只（母基金 {len(code_stats.get('long_base_names') or set())} 只）",
            f"  代码级交集基金: {len(code_stats.get('raw_intersection') or set())} 只",
            f"  原始仅近期优质: {len(code_stats.get('raw_only_short') or set())} 只",
            f"  原始仅长期优质: {len(code_stats.get('raw_only_long') or set())} 只",
        ],
        print_fn=print_fn,
    )
    if shared_base_names:
        print_fn(f"\n  ✅ 发现 {len(shared_base_names)} 只母基金同时在近期和长期都表现优异！")
        return
    _emit_lines(
        [
            f"\n  ℹ️  近期优质基金和长期优质基金没有交集",
            f"     这说明短期表现好的基金和长期表现好的基金完全不同",
        ],
        print_fn=print_fn,
    )


def print_input_lineage_summary(
    *,
    short_report_date: str | None,
    long_report_date: str | None,
    input_lineage: dict,
    print_fn=print,
) -> None:
    """打印输入业务日审计摘要。"""
    if short_report_date or long_report_date:
        print_fn(
            "  🗓️  软交集补分日期对齐:"
            f" short={short_report_date or 'latest'}"
            f", long={long_report_date or 'latest'}"
        )
    print_fn(
        "  🧾 输入业务日审计:"
        f" 近期={short_report_date or '未知'}"
        f", 长期={long_report_date or '未知'}"
        f", 状态={input_lineage['input_pair_status_label']}"
    )
    input_pair_status = input_lineage.get('input_pair_status')
    if input_pair_status == 'stale':
        print_fn(f"  ⚠️ 检测到历史输入回退: {input_lineage['input_lineage_note']}")
    elif input_pair_status == 'mixed':
        print_fn(f"  ❌ 检测到跨业务日混用: {input_lineage['input_lineage_note']}")
    elif input_pair_status == 'unknown':
        print_fn(f"  ⚠️ 输入业务日待核验: {input_lineage['input_lineage_note']}")


def print_cross_score_summary(
    *,
    fund_type: str,
    cross_short_scores: dict,
    cross_long_scores: dict,
    min_cross_periods: int,
    print_fn=print,
) -> None:
    """打印 screening_picks 补分池加载摘要。"""
    _emit_lines(
        [
            f"  📊 从 screening_picks 补分池加载 ({fund_type}): 近期 {len(cross_short_scores)} 只, 长期 {len(cross_long_scores)} 只",
            "     说明: 该数量仅用于邻近交集补分，不等同于近期/长期输入文件规模",
            "     说明: 邻近交集回填仅接受对侧达到最小多周期支撑的基金，"
            f"当前下限={min_cross_periods} 个周期",
        ],
        print_fn=print_fn,
    )


def print_cross_score_degraded(exc: Exception, *, print_fn=print) -> None:
    """打印 screening_picks 降级提示。"""
    print_fn(f"  ⚠️ screening_picks 查询失败，软交集降级为空: {exc}")


def print_candidate_summary(candidate_summary: dict, *, print_fn=print) -> None:
    """打印母基金口径候选池摘要。"""
    print_fn(f"  母基金口径交集基金: {candidate_summary['intersection_count']} 只")
    if candidate_summary['soft_intersection_count'] > 0:
        print_fn(
            f"  🔗 发现 {candidate_summary['soft_intersection_count']} 只邻近交集基金"
            "（按母基金口径识别，且缺失侧满足最小多周期支撑）"
        )
    print_fn(f"  母基金口径仅近期优质: {candidate_summary['only_short_count']} 只")
    print_fn(f"  母基金口径仅长期优质: {candidate_summary['only_long_count']} 只")


def print_sheet_preparation_done(*, print_fn=print) -> None:
    """打印表格准备完成提示。"""
    print_fn(f"  ✅ 所有表格准备完成")


def print_comparison_report_written(output_file: Path, *, print_fn=print) -> None:
    """打印对比报告导出结果。"""
    _emit_lines(
        [
            f"  ✅ Excel文件美化完成,定开基金已用浅黄色标注",
            f"  ✅ Excel文件已生成: {output_file.name}",
        ],
        print_fn=print_fn,
    )


def print_markdown_skipped(*, print_fn=print) -> None:
    """打印 Markdown 跳过说明。"""
    print_fn("  ℹ️ 当前仅保留 Excel / CSV 产物供用户浏览和下载")


def print_simple_csv_written(csv_file: Path, *, label: str, print_fn=print) -> None:
    """打印简版 CSV 导出结果。"""
    print_fn(f"  ✅ {label}: {csv_file.name}")


def print_weighted_union_policy(
    *,
    mainlist_anchor_limit: int | None,
    watchlist_limit: int,
    short_weight: float,
    long_weight: float,
    hard_intersection_bonus: float,
    soft_intersection_bonus: float,
    mainlist_selection_mode: str = 'fixed_top_n',
    mainlist_score_band_delta: float = 0.0,
    print_fn=print,
) -> None:
    """打印加权并集策略参数。"""
    selection_note = f"mainlist_selection={mainlist_selection_mode}"
    if str(mainlist_selection_mode).strip() == 'all_eligible':
        selection_note = "mainlist_selection=all_eligible(全部达标候选直接进入正式主榜)"
    elif str(mainlist_selection_mode).strip() == 'score_band_topn_anchor':
        selection_note = (
            f"mainlist_selection={mainlist_selection_mode}"
            f"(anchor_limit={mainlist_anchor_limit}, score_band_delta={float(mainlist_score_band_delta):.2f})"
        )
    _emit_lines(
        [
            f"  🔧 采用加权并集策略筛选最终优质基金...",
            f"     - 参数: mainlist_anchor_limit={mainlist_anchor_limit}, watchlist_limit={watchlist_limit}, short_weight={short_weight:.2f}, "
            f"long_weight={long_weight:.2f}, hard_bonus={hard_intersection_bonus:.1f}, "
            f"soft_bonus={soft_intersection_bonus:.1f}, {selection_note}",
        ],
        print_fn=print_fn,
    )


def print_completeness_summary(
    completeness_summary: dict,
    incomplete_fcodes: set[str],
    *,
    print_fn=print,
) -> None:
    """打印关键指标完整性校验摘要。"""
    if completeness_summary.get('enabled'):
        print_fn(
            "  🔒 关键指标完整性校验："
            f" 已检查 {completeness_summary['checked_count']} 只候选，"
            f"过滤 {completeness_summary['filtered_count']} 只关键指标缺失基金"
        )
        if incomplete_fcodes:
            print_fn(
                "     - 缺失关键指标样本: "
                + ", ".join((completeness_summary.get('sample_fcodes') or [])[:5])
            )
        return
    print_fn(
        "  ⚠️ 关键指标完整性校验未生效，"
        f"原因: {completeness_summary.get('reason') or 'unknown'}"
    )


def print_autostock_validation_summary(
    autostock_validation_summary: dict,
    autostock_failed_fcodes: set[str],
    *,
    print_fn=print,
) -> None:
    """打印 AutoStock 双源交叉验证摘要。"""
    label = ('天天基金／东方财富跨接口一致性校验' if autostock_validation_summary.get('source') == 'eastmoney_cross_endpoint'
             else 'AutoStock 双源交叉验证')
    print_fn(
        f"  🔒 {label}："
        f" 已检查 {autostock_validation_summary['checked_count']} 只候选，"
        f"通过 {autostock_validation_summary['passed_count']} 只，"
        f"拦截 {autostock_validation_summary['filtered_count']} 只"
    )
    if autostock_failed_fcodes:
        print_fn(
            "     - 交叉验证失败样本: "
            + ", ".join((autostock_validation_summary.get('sample_fcodes') or [])[:5])
        )


def print_autostock_validation_skipped(fund_type: str, *, print_fn=print) -> None:
    """打印 AutoStock 校验跳过原因。"""
    print_fn(f"  ℹ️ 跳过 AutoStock 双源交叉验证（fund_type={fund_type}）")


def print_drawdown_duration_summary(drawdown_duration_summary: dict, *, print_fn=print) -> None:
    """打印最长回撤观测数分析摘要。"""
    if drawdown_duration_summary.get('enabled'):
        distribution = drawdown_duration_summary.get('distribution') or {}
        print_fn(
            "  📉 1年最长回撤观测数分析："
            f" 已检查 {drawdown_duration_summary['checked_count']} 只候选，"
            f"缺失 {drawdown_duration_summary['missing_count']} 只，"
            f"中位数 {distribution.get('median', 'N/A')} 次观测，"
            f"P90 {distribution.get('p90', 'N/A')} 次观测"
        )
        return
    print_fn(
        "  ⚠️ 1年最长回撤观测数分析未生效，"
        f"原因: {drawdown_duration_summary.get('reason') or 'unknown'}"
    )


def print_weighted_union_statistics(
    *,
    candidate_summary: dict,
    incomplete_fcodes: set[str],
    autostock_failed_fcodes: set[str],
    prevalidation_selected_pool_count: int,
    dedup_or_filtered_removed: int,
    top_n_trimmed: int,
    top_n_final: int,
    candidate_pool_reduction: dict,
    top_category_counts: dict[str, int],
    mainlist_selection_mode: str = 'fixed_top_n',
    mainlist_score_band_delta: float = 0.0,
    print_fn=print,
) -> None:
    """打印主榜候选池收敛与正式主榜结果。"""
    _emit_lines(
        [
            f"  📊 加权并集统计：",
            f"     - 并集总数：{candidate_summary['union_candidate_count']} 只",
            f"     - 交集基金：{candidate_summary['intersection_count']} 只",
            f"     - 邻近交集：{candidate_summary['soft_intersection_count']} 只",
            f"     - 仅近期优质：{candidate_summary['only_short_count']} 只",
            f"     - 仅长期优质：{candidate_summary['only_long_count']} 只",
        ],
        print_fn=print_fn,
    )
    if incomplete_fcodes:
        print_fn(f"     - 关键指标缺失过滤：{len(incomplete_fcodes)} 只")
    if autostock_failed_fcodes:
        print_fn(f"     - 双源交叉验证拦截：{len(autostock_failed_fcodes)} 只")
    if dedup_or_filtered_removed > 0:
        print_fn(f"     - 候选池去重/过滤后：{prevalidation_selected_pool_count} 只")
        print_fn(f"     - 去重/过滤移除：{dedup_or_filtered_removed} 条")
        if candidate_pool_reduction.get('holding_period_filtered', 0) > 0:
            print_fn(f"       · 定开/持有期过滤：{candidate_pool_reduction['holding_period_filtered']} 条")
        if candidate_pool_reduction.get('share_class_collapsed', 0) > 0:
            print_fn(f"       · 同母基金份额合并：{candidate_pool_reduction['share_class_collapsed']} 条")
    if top_n_trimmed > 0 and str(mainlist_selection_mode).strip() == 'score_band_topn_anchor':
        print_fn(
            f"     - 锚点分数带外淘汰：{top_n_trimmed} 只"
            f"（按末位分数带 {float(mainlist_score_band_delta):.2f} 分扩容后）"
        )
    elif top_n_trimmed > 0 and str(mainlist_selection_mode).strip() != 'all_eligible':
        print_fn(f"     - 主榜锚点截断淘汰：{top_n_trimmed} 只")
    if str(mainlist_selection_mode).strip() == 'all_eligible':
        print_fn(f"     - 最终输出：全部达标主榜 {top_n_final} 只")
    elif str(mainlist_selection_mode).strip() == 'score_band_topn_anchor':
        print_fn(f"     - 最终输出：{top_n_final} 只")
        print_fn(
            f"       · 主榜采用锚点末位分数带扩容，score_band_delta={float(mainlist_score_band_delta):.2f}"
        )
    else:
        print_fn(f"     - 最终输出：主榜 {top_n_final} 只")

    if top_category_counts:
        print_fn(f"  📊 最终主榜{top_n_final}只分布：")
        for category, count in top_category_counts.items():
            print_fn(f"     - {category}：{count} 只")


def print_watchlist_exports(
    *,
    short_watchlist_csv: Path | None,
    short_watchlist_count: int,
    long_watchlist_csv: Path | None,
    long_watchlist_count: int,
    print_fn=print,
) -> None:
    """打印观察池导出结果。"""
    if short_watchlist_csv:
        print_fn(f"  ✅ 仅近期观察池已导出: {short_watchlist_csv.name} ({short_watchlist_count} 只)")
    if long_watchlist_csv:
        print_fn(f"  ✅ 仅长期观察池已导出: {long_watchlist_csv.name} ({long_watchlist_count} 只)")


def print_complete_info_fetch_start(fund_count: int, *, print_fn=print) -> None:
    """打印数据库完整信息读取开始提示。"""
    print_fn(f"  正在从数据库获取 {fund_count} 只基金的完整信息...")


def print_complete_info_fetch_result(used_fallback: bool, *, print_fn=print) -> None:
    """打印数据库完整信息读取结果。"""
    if used_fallback:
        print_fn(f"  ⚠️ 无法从数据库获取基金信息,使用备用数据")
        return
    print_fn(f"  ✅ 已按综合加权得分排序")


def print_holding_period_filter_status(
    *,
    fund_type: str,
    filter_applied: bool,
    holding_count: int,
    print_fn=print,
) -> None:
    """打印持有期关键词过滤的一致性校验结果。"""
    if filter_applied:
        print_fn(f"  ℹ️ 持有期关键词过滤已在候选选择阶段完成")
        if holding_count > 0:
            print_fn(f"  ⚠️ 最终结果仍检测到 {holding_count} 只疑似持有期基金，请复核名称口径一致性")
        return
    print_fn(f"  ℹ️ 跳过持有期关键词过滤（fund_type={fund_type}）")


def print_export_cleanup_summary(export_cleanup_summary: dict, *, print_fn=print) -> None:
    """打印导出前清理摘要。"""
    print_fn(f"  正在美化数据...")
    if export_cleanup_summary.get('removed_manager_column'):
        print_fn(f"  已删除基金经理列")
    dropped_empty_columns = export_cleanup_summary.get('dropped_empty_columns') or []
    if dropped_empty_columns:
        print_fn(f"  已删除 {len(dropped_empty_columns)} 个空列")
    print_fn(f"  正在应用Excel样式...")


def print_final_fund_workbook_written(
    *,
    final_fund_file: Path,
    fund_count: int,
    column_count: int,
    holding_period_count: int,
    short_report_date: str | None,
    long_report_date: str | None,
    input_pair_status_label: str,
    print_fn=print,
) -> None:
    """打印最终主榜文件写出摘要。"""
    _emit_lines(
        [
            f"  ✅ 当日最终优质基金文件已生成: {final_fund_file.name}",
            f"     包含 {fund_count} 只基金，共 {column_count} 列数据",
            f"     定开基金: {holding_period_count} 只 (已用浅黄色标注)",
            f"     涨跌数值已应用红涨绿跌颜色",
            f"     输入业务日: 近期={short_report_date or '未知'}, 长期={long_report_date or '未知'}"
            f" ({input_pair_status_label})",
        ],
        print_fn=print_fn,
    )


def print_main_task_start(start_time: datetime, *, print_fn=print) -> None:
    """打印主任务开始摘要。"""
    _emit_lines(
        [
            "\n" + "=" * 80,
            "【每日最终筛选任务 - 增强版】",
            "=" * 80,
            f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
        ],
        print_fn=print_fn,
    )


def print_main_step_header(step_number: int, title: str, *, print_fn=print) -> None:
    """打印主任务步骤标题。"""
    _emit_lines(
        [
            f"\n{'=' * 80}",
            f"【步骤{step_number}】{title}",
            f"{'=' * 80}",
        ],
        print_fn=print_fn,
    )


def print_required_input_pair_not_found(
    *,
    output_dir: Path,
    long_term_pattern: str,
    short_term_pattern: str,
    error: Exception,
    print_fn=print,
) -> None:
    """打印输入文件缺失错误。"""
    _emit_lines(
        [
            f"❌ 错误: 未找到可配对的长短期优质基金文件",
            f"   搜索目录: {output_dir}",
            f"   搜索模式: {long_term_pattern} | {short_term_pattern}",
            f"   详细原因: {error}",
        ],
        print_fn=print_fn,
    )


def print_found_input_file(label: str, file_path: Path, *, print_fn=print) -> None:
    """打印输入文件定位结果。"""
    print_fn(f"  ✅ 找到{label}文件: {file_path.name}")


def print_report_generation_failed(*, print_fn=print) -> None:
    """打印对比报告生成失败。"""
    print_fn(f"\n❌ 对比报告生成失败")


def print_report_generation_exception(exc: Exception, *, print_fn=print) -> None:
    """打印对比报告生成异常。"""
    print_fn(f"\n❌ 对比报告生成异常: {exc}")


def print_main_task_completion(end_time: datetime, result: dict, *, print_fn=print) -> None:
    """打印主任务完成摘要。"""
    _emit_lines(
        [
            f"\n{'=' * 80}",
            "✅ 每日最终筛选任务完成！",
            f"{'=' * 80}",
            f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"\n生成文件:",
            f"  📊 {Path(result['output_file']).name}",
            f"\n统计信息:",
            f"  近期基金数: {result['short_term_count']}",
            f"  长期基金数: {result['long_term_count']}",
            f"  交集基金数: {result['intersection_count']}",
            f"  邻近交集数: {result['soft_intersection_count']}",
            f"  仅近期数量: {result['only_short_count']}",
            f"  仅长期数量: {result['only_long_count']}",
            f"  输入业务日: 近期={result['short_report_date'] or '未知'}, "
            f"长期={result['long_report_date'] or '未知'}",
            f"  输入状态: {result['input_pair_status_label']}",
            f"  来源说明: {result['input_lineage_note']}",
        ],
        print_fn=print_fn,
    )
