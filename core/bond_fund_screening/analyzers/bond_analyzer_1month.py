#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1月期债券基金分析脚本（短期投资-激进型）

使用方法:
    python3 -m bond_fund_screening.analyzers.bond_analyzer_1month
或
    python3 analyzers/bond_analyzer_1month.py
"""

import argparse
from pathlib import Path
import sys

from bond_fund_screening.analyzers.base_analyzer import BaseAnalyzer
from bond_fund_screening.config import DEFAULT_TOP_N, OUTPUT_DIR
from bond_fund_screening.utils.excel_generator import ExcelGenerator


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='1月期债券基金智能筛选分析（短期投资策略）')
    parser.add_argument('--top-n', type=int, default=DEFAULT_TOP_N,
                       help='仅用于实验性限量输出；默认输出全部达标基金')
    parser.add_argument('--output-dir', type=str, default=str(OUTPUT_DIR),
                       help=f'输出目录（默认: {OUTPUT_DIR}）')
    parser.add_argument('--no-excel', action='store_true',
                       help='不生成Excel报告，只生成CSV')
    parser.add_argument('--quiet', action='store_true',
                       help='静默模式，减少输出')
    parser.add_argument('--freshness-strategy', choices=('soft', 'hard'),
                       help='运行时覆写新鲜度策略')

    args = parser.parse_args()

    # 创建分析器（使用period_days参数）
    analyzer = BaseAnalyzer(
        period_days=30,  # 1月 = 30天
        top_n=args.top_n,
        output_dir=Path(args.output_dir),
        verbose=not args.quiet,
        freshness_strategy_override=args.freshness_strategy,
    )

    # 运行分析
    try:
        df_result, stats = analyzer.run()

        # 生成Excel报告
        if not args.no_excel:
            print("\n生成Excel报告...")
            try:
                generator = ExcelGenerator(period_days=30)

                # 生成输出文件名
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                selection_tag = (
                    f"达标全量{len(df_result)}" if args.top_n is None else f"Top{len(df_result)}"
                )
                excel_file = Path(args.output_dir) / f"债券基金_一月期_{selection_tag}_{timestamp}.xlsx"

                generator.generate_report(df_result, stats, excel_file)
                try:
                    analyzer.register_excel_report(excel_file)
                except Exception as meta_error:
                    print(f"⚠️  报告元数据写入失败: {meta_error}")
                print(f"✅ Excel报告已生成: {excel_file}")
            except Exception as e:
                print(f"⚠️  Excel报告生成失败: {e}")
                print("   (CSV文件已正常生成)")

        print("\n✅ 1月期分析完成！")
        return 0

    except Exception as e:
        print(f"\n❌ 分析失败: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
