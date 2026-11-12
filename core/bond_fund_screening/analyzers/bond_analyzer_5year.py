#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
5年期债券基金分析脚本（向后兼容包装）

推荐使用统一入口:
    python3 -m bond_fund_screening.analyzers.bond_analyzer --period 5
"""

import sys
from bond_fund_screening.analyzers.bond_analyzer import run_analysis, DEFAULT_TOP_N, OUTPUT_DIR
import argparse


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='5年期债券基金智能筛选分析')
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
    return run_analysis(period_years=5, top_n=args.top_n, output_dir=args.output_dir,
                       no_excel=args.no_excel, quiet=args.quiet,
                       freshness_strategy=args.freshness_strategy)


if __name__ == "__main__":
    sys.exit(main())
