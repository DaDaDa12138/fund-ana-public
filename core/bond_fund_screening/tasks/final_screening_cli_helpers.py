#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多基金类型最终筛选 CLI 壳层辅助工具。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


def run_prefixed_final_screening_cli(
    *,
    display_name: str,
    script_prefix: str,
    output_prefix: str,
    output_dir,
    find_required_input_pair_fn,
    generate_enhanced_comparison_report_fn,
) -> int:
    """运行带基金类型前缀的最终筛选 CLI 主流程。"""
    print("\n" + "=" * 100)
    print(" " * 30 + f"【{display_name}基金最终筛选任务】")
    print("=" * 100)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    normalized_output_dir = Path(output_dir)
    today = datetime.now().strftime("%Y%m%d")
    long_term_pattern = f"长期优质{display_name}基金_详细报告_{today}_*.xlsx"
    fallback_long_term_pattern = f"长期优质{display_name}基金_详细报告_*.xlsx"
    short_term_pattern = f"近期优质{display_name}基金_详细报告_{today}_*.xlsx"
    fallback_short_term_pattern = f"近期优质{display_name}基金_详细报告_*.xlsx"

    print(f"\n{'=' * 100}")
    print(" " * 30 + f"【步骤1: 查找长期优质{display_name}基金文件】")
    print(f"{'=' * 100}")
    try:
        short_term_file, long_term_file = find_required_input_pair_fn(
            short_today_pattern=short_term_pattern,
            short_fallback_pattern=fallback_short_term_pattern,
            short_label=f'近期优质{display_name}基金',
            long_today_pattern=long_term_pattern,
            long_fallback_pattern=fallback_long_term_pattern,
            long_label=f'长期优质{display_name}基金',
            directory=normalized_output_dir,
        )
    except FileNotFoundError as exc:
        print(f"❌ 错误: 未找到可配对的长短期{display_name}基金文件")
        print(f"   搜索目录: {normalized_output_dir}")
        print(f"   搜索模式: {long_term_pattern} | {short_term_pattern}")
        print(
            f"   提示: 请先运行 {script_prefix}_long_term_screening.py "
            f"和 {script_prefix}_short_term_screening.py"
        )
        print(f"   详细原因: {exc}")
        return 1

    print(f"  ✅ 找到长期优质{display_name}基金文件: {long_term_file.name}")

    print(f"\n{'=' * 100}")
    print(" " * 30 + f"【步骤2: 查找近期优质{display_name}基金文件】")
    print(f"{'=' * 100}")
    print(f"  ✅ 找到近期优质{display_name}基金文件: {short_term_file.name}")

    print(f"\n{'=' * 100}")
    print(" " * 30 + "【步骤3: 生成对比报告】")
    print(f"{'=' * 100}")

    try:
        result = generate_enhanced_comparison_report_fn(
            short_term_file,
            long_term_file,
            normalized_output_dir,
            output_prefix=output_prefix,
        )
        if not result['success']:
            print("\n❌ 对比报告生成失败")
            return 1

        print(f"  ✅ 已生成: {Path(result['output_file']).name}")
        print(f"  ✅ 已生成: {Path(result['final_fund_file']).name}")
    except Exception as exc:
        print(f"\n❌ 对比报告生成异常: {exc}")
        import traceback
        traceback.print_exc()
        return 1

    print(f"\n{'=' * 100}")
    print(" " * 30 + f"✅ {display_name}基金最终筛选任务完成！")
    print(f"{'=' * 100}")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n生成文件:")
    print(f"  📊 {Path(result['output_file']).name}")
    print(f"  📋 {Path(result['final_fund_file']).name}")
    print("\n统计信息:")
    print(f"  近期基金数: {result['short_term_count']}")
    print(f"  长期基金数: {result['long_term_count']}")
    print(f"  交集基金数: {result['intersection_count']}")
    print(f"  仅近期数量: {result['only_short_count']}")
    print(f"  仅长期数量: {result['only_long_count']}")
    print(
        f"  输入业务日: 近期={result['short_report_date'] or '未知'}, "
        f"长期={result['long_report_date'] or '未知'}"
    )
    print(f"  输入状态: {result['input_pair_status_label']}")
    print(f"  来源说明: {result['input_lineage_note']}")
    return 0
