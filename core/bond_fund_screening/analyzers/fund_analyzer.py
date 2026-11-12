#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用基金周期分析入口

支持多种基金类型（债券/股票/混合）和所有周期（短期+长期）。
通过 --fund-type 和 --period/--period-days 参数指定。

使用方法:
    # 长期（年）
    python3 -m bond_fund_screening.analyzers.fund_analyzer --fund-type stock --period 3
    # 短期（天）
    python3 -m bond_fund_screening.analyzers.fund_analyzer --fund-type hybrid --period-days 30
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from bond_fund_screening.analyzers.base_analyzer import BaseAnalyzer
from bond_fund_screening.config import DEFAULT_TOP_N, OUTPUT_DIR
from bond_fund_screening.tasks.runtime_env import apply_period_analyzer_runtime_defaults
from bond_fund_screening.utils.excel_generator import ExcelGenerator


VALID_FUND_TYPES = ('bond', 'stock', 'hybrid')
VALID_PERIOD_YEARS = (1, 2, 3, 4, 5)
VALID_PERIOD_DAYS = (7, 30, 90, 180)

# 基金类型中文名映射（用于文件名，BaseAnalyzer.fund_type_name 也会自动从 Profile 获取）
FUND_TYPE_NAMES = {
    'bond': '债券型',
    'stock': '股票型',
    'hybrid': '混合型',
}


def run_analysis(fund_type: str = 'bond',
                 period_years: int = None,
                 period_days: int = None,
                 top_n: int | None = DEFAULT_TOP_N,
                 output_dir: str = str(OUTPUT_DIR),
                 no_excel: bool = False,
                 quiet: bool = False,
                 freshness_strategy: str = None) -> int:
    """
    运行指定基金类型和周期的分析

    Args:
        fund_type: 基金类型 (bond/stock/hybrid)
        period_years: 长期周期（1-5年），与 period_days 二选一
        period_days: 短期周期（7/30/90/180天），与 period_years 二选一
        top_n: 可选输出基金数量上限；默认输出全部达标基金
        output_dir: 输出目录
        no_excel: 是否跳过 Excel 报告
        quiet: 静默模式
        freshness_strategy: 新鲜度策略运行时覆写（soft/hard）

    Returns:
        0 成功，1 失败
    """
    if fund_type not in VALID_FUND_TYPES:
        print(f"无效基金类型: {fund_type}，支持 {VALID_FUND_TYPES}")
        return 1

    if period_years is None and period_days is None:
        print("必须指定 --period 或 --period-days")
        return 1

    apply_period_analyzer_runtime_defaults(
        fund_type=fund_type,
        period_years=period_years,
        period_days=period_days,
    )

    # 构建 BaseAnalyzer 参数
    kwargs = dict(
        top_n=top_n,
        output_dir=Path(output_dir),
        verbose=not quiet,
        fund_type=fund_type,
        freshness_strategy_override=freshness_strategy,
    )
    if period_days is not None:
        kwargs['period_days'] = period_days
    else:
        kwargs['period_years'] = period_years

    analyzer = BaseAnalyzer(**kwargs)

    try:
        df_result, stats = analyzer.run()

        if not no_excel:
            print("\n生成Excel报告...")
            try:
                gen_kwargs = {}
                if period_days is not None:
                    gen_kwargs['period_days'] = period_days
                else:
                    gen_kwargs['period_years'] = period_years
                generator = ExcelGenerator(**gen_kwargs)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                type_name = analyzer.fund_type_name
                period_label = analyzer.period_config['output_name']
                selection_tag = (
                    f"达标全量{len(df_result)}" if top_n is None else f"Top{len(df_result)}"
                )
                excel_file = Path(output_dir) / f"{type_name}基金_{period_label}_{selection_tag}_{timestamp}.xlsx"

                generator.generate_report(df_result, stats, excel_file)
                try:
                    analyzer.register_excel_report(excel_file)
                except Exception as meta_error:
                    print(f"报告元数据写入失败: {meta_error}")
                print(f"Excel报告已生成: {excel_file}")
            except Exception as e:
                print(f"Excel报告生成失败: {e}")
                print("   (CSV文件已正常生成)")

        type_name = FUND_TYPE_NAMES.get(fund_type, fund_type)
        period_desc = f"{period_days}天" if period_days else f"{period_years}年期"
        print(f"\n{type_name} {period_desc}分析完成")
        return 0

    except Exception as e:
        print(f"\n分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main():
    """CLI 入口"""
    parser = argparse.ArgumentParser(description='通用基金智能筛选分析')
    parser.add_argument('--fund-type', type=str, default='bond',
                        choices=VALID_FUND_TYPES,
                        help='基金类型: bond(债券), stock(股票), hybrid(混合)')

    period_group = parser.add_mutually_exclusive_group(required=True)
    period_group.add_argument('--period', type=int, choices=VALID_PERIOD_YEARS,
                              help='长期分析周期（年）: 1, 2, 3, 4, 5')
    period_group.add_argument('--period-days', type=int, choices=VALID_PERIOD_DAYS,
                              help='短期分析周期（天）: 7, 30, 90, 180')

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
    parser.add_argument('--freshness-strategy', choices=VALID_FUND_TYPES[:0] + ('soft', 'hard'),
                        help='运行时覆写新鲜度策略')

    args = parser.parse_args()
    return run_analysis(
        fund_type=args.fund_type,
        period_years=args.period,
        period_days=args.period_days,
        top_n=args.top_n,
        output_dir=args.output_dir,
        no_excel=args.no_excel,
        quiet=args.quiet,
        freshness_strategy=args.freshness_strategy,
    )


if __name__ == "__main__":
    sys.exit(main())
