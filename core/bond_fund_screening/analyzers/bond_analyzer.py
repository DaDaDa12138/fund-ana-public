#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一债券基金周期分析入口

支持 1-5 年期分析，通过 --period 参数指定周期。
消除原 bond_analyzer_[1-5]year.py 的重复代码。

使用方法:
    python3 -m bond_fund_screening.analyzers.bond_analyzer --period 3
    python3 -m bond_fund_screening.analyzers.bond_analyzer --period 1 --top-n 100
"""

import argparse
import sys
from pathlib import Path

from bond_fund_screening.analyzers.base_analyzer import BaseAnalyzer
from bond_fund_screening.config import DEFAULT_TOP_N, OUTPUT_DIR
from bond_fund_screening.tasks.runtime_env import apply_period_analyzer_runtime_defaults
from bond_fund_screening.utils.excel_generator import ExcelGenerator


VALID_PERIODS = (1, 2, 3, 4, 5)


def run_analysis(period_years: int, top_n: int | None = DEFAULT_TOP_N,
                 output_dir: str = str(OUTPUT_DIR),
                 no_excel: bool = False, quiet: bool = False,
                 freshness_strategy: str = None) -> int:
    """
    运行指定周期的债券基金分析

    Args:
        period_years: 分析周期（1-5年）
        top_n: 可选输出基金数量上限；默认输出全部达标基金
        output_dir: 输出目录
        no_excel: 是否跳过 Excel 报告
        quiet: 静默模式
        freshness_strategy: 新鲜度策略运行时覆写（soft/hard）

    Returns:
        0 成功，1 失败
    """
    if period_years not in VALID_PERIODS:
        print(f"无效周期: {period_years}，支持 {VALID_PERIODS}")
        return 1

    apply_period_analyzer_runtime_defaults(
        fund_type='bond',
        period_years=period_years,
    )

    analyzer = BaseAnalyzer(
        period_years=period_years,
        top_n=top_n,
        output_dir=Path(output_dir),
        verbose=not quiet,
        freshness_strategy_override=freshness_strategy,
    )

    try:
        df_result, stats = analyzer.run()

        if not no_excel:
            print("\n生成Excel报告...")
            try:
                generator = ExcelGenerator(period_years=period_years)
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                selection_tag = (
                    f"达标全量{len(df_result)}" if top_n is None else f"Top{len(df_result)}"
                )
                excel_file = Path(output_dir) / f"债券基金_{period_years}年期_{selection_tag}_{timestamp}.xlsx"
                generator.generate_report(df_result, stats, excel_file)
                try:
                    analyzer.register_excel_report(excel_file)
                except Exception as meta_error:
                    print(f"报告元数据写入失败: {meta_error}")
                print(f"Excel报告已生成: {excel_file}")
            except Exception as e:
                print(f"Excel报告生成失败: {e}")
                print("   (CSV文件已正常生成)")

        print(f"\n{period_years}年期分析完成")
        return 0

    except Exception as e:
        print(f"\n分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(description='债券基金智能筛选分析（统一入口）')
    parser.add_argument('--period', type=int, required=True, choices=VALID_PERIODS,
                       help='分析周期（年）: 1, 2, 3, 4, 5')
    parser.add_argument(
        '--top-n',
        type=int,
        default=DEFAULT_TOP_N,
        help='仅用于实验性限量输出；默认输出全部达标基金',
    )
    parser.add_argument('--output-dir', type=str, default=str(OUTPUT_DIR),
                       help=f'输出目录（默认: {OUTPUT_DIR}）')
    parser.add_argument('--no-excel', action='store_true',
                       help='不生成Excel报告，只生成CSV')
    parser.add_argument('--quiet', action='store_true',
                       help='静默模式，减少输出')
    parser.add_argument('--freshness-strategy', choices=('soft', 'hard'),
                       help='运行时覆写新鲜度策略')

    args = parser.parse_args()
    return run_analysis(
        period_years=args.period,
        top_n=args.top_n,
        output_dir=args.output_dir,
        no_excel=args.no_excel,
        quiet=args.quiet,
        freshness_strategy=args.freshness_strategy,
    )


if __name__ == "__main__":
    sys.exit(main())
