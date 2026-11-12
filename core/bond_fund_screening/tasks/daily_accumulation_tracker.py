#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日累积跟踪任务 - daily_accumulation_tracker

功能：
1. 读取当日最终优质基金Excel文件
2. 更新累积跟踪Excel文件（固定文件名）
3. 基金重复出现时，出现次数+1
4. 记录总对比次数+1

累积跟踪Excel结构：
- Row 1 (Metadata): A1="总对比次数" B1=数值
- Row 2 (Headers): 基金代码 | 基金名称 | 出现次数 | 首次出现日期 | 最后出现日期 | ...
- Data Rows: 累积基金数据

使用方法：
    python3 -m bond_fund_screening.tasks.daily_accumulation_tracker
"""

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """尽早解析命令行参数，确保 `--help` 不触发后续重型导入。"""
    parser = argparse.ArgumentParser(description='债券基金每日累积跟踪任务')
    parser.add_argument(
        '--input-file',
        default='',
        help='同一主流水线显式传入的“当日最终优质基金”Excel；提供后禁止按日期猜测',
    )
    return parser.parse_args(argv)


if __name__ == "__main__" and any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
    parse_args()
    raise SystemExit(0)

from bond_fund_screening.tasks.bootstrap import ensure_project_venv


ensure_project_venv(PROJECT_ROOT)

from datetime import datetime
from bond_fund_screening.config import DAILY_SCREENING_CONFIG, OUTPUT_DIR
from bond_fund_screening.tasks.accumulation_file_utils import (
    ACCUMULATION_INPUT_FILE_ENV,
    build_today_pattern,
    deduplicate_files_by_date,
    find_required_final_file,
    resolve_explicit_final_file,
)
from bond_fund_screening.utils.accumulation_tracker import AccumulationTracker


def _deduplicate_files_by_date(files: list) -> list:
    """同一业务日期仅保留最新文件。"""
    return deduplicate_files_by_date(files)


def find_all_final_files(directory: Path) -> list:
    """
    查找所有当日最终优质基金文件

    Args:
        directory: 搜索目录

    Returns:
        按时间排序的文件路径列表（从旧到新）
    """
    pattern = "当日最终优质基金_*.xlsx"
    files = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime)
    return _deduplicate_files_by_date(files)


def find_latest_final_file(
    directory: Path,
    *,
    explicit_input_file: str | Path | None = None,
) -> Path:
    """
    查找最新的当日最终优质基金文件

    Args:
        directory: 搜索目录

    Returns:
        最新文件的路径，如果未找到返回None
    """
    explicit_path = str(explicit_input_file or '').strip()
    if explicit_path:
        return resolve_explicit_final_file(
            directory,
            explicit_path,
            label='当日最终优质基金',
        )

    today_pattern = build_today_pattern("当日最终优质基金")
    fallback_pattern = "当日最终优质基金_*.xlsx"
    return find_required_final_file(
        directory=directory,
        today_pattern=today_pattern,
        fallback_pattern=fallback_pattern,
        label='当日最终优质基金',
    )


def main(argv: list[str] | None = None):
    """主函数"""
    args = parse_args(argv)
    print("\n" + "="*80)
    print("【每日累积跟踪任务】")
    print("="*80)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 获取配置
    config = DAILY_SCREENING_CONFIG
    output_dir = config['output_dir']
    tracker_file = config['accumulation_tracker_file']

    # 步骤1: 查找当日最终优质基金文件
    print(f"\n{'='*80}")
    print(f"【步骤1】查找当日最终优质基金文件")
    print(f"{'='*80}")

    # 检查累积跟踪文件是否存在
    is_first_run = not tracker_file.exists()

    if is_first_run:
        # 首次运行：处理所有历史文件
        all_files = find_all_final_files(output_dir)

        if not all_files:
            print(f"❌ 错误: 未找到当日最终优质基金文件")
            print(f"   搜索目录: {output_dir}")
            print(f"   搜索模式: 当日最终优质基金_*.xlsx")
            return 1

        print(f"  ℹ️  首次运行，将处理所有历史文件")
        print(f"  ✅ 找到 {len(all_files)} 个当日最终优质基金文件:")
        for i, file in enumerate(all_files, 1):
            print(f"     {i}. {file.name}")

        files_to_process = all_files
    else:
        # 非首次运行：只处理最新文件
        try:
            final_file = find_latest_final_file(
                output_dir,
                explicit_input_file=(
                    args.input_file
                    or os.environ.get(ACCUMULATION_INPUT_FILE_ENV, '')
                ),
            )
        except FileNotFoundError as e:
            print(f"❌ 错误: 未找到当日最终优质基金文件")
            print(f"   搜索目录: {output_dir}")
            print(f"   搜索模式: 当日最终优质基金_*.xlsx")
            print(f"   详细原因: {e}")
            return 1

        print(f"  ✅ 找到最新当日最终优质基金文件: {final_file.name}")
        files_to_process = [final_file]

    # 步骤2: 更新累积跟踪
    print(f"\n{'='*80}")
    print(f"【步骤2】更新累积跟踪")
    print(f"{'='*80}")

    # 创建累积跟踪器
    tracker = AccumulationTracker(
        tracker_file=str(tracker_file),
        verbose=True
    )

    # 处理所有文件
    total_new_funds = 0
    total_updated_funds = 0

    for i, file in enumerate(files_to_process, 1):
        if len(files_to_process) > 1:
            print(f"\n  --- 处理第 {i}/{len(files_to_process)} 个文件 ---")

        # 执行更新
        result = tracker.update_from_daily_result(
            daily_result_file=str(file)
        )

        if not result['success']:
            print(f"\n❌ 累积跟踪更新失败: {result.get('message', '未知错误')}")
            return 1

        total_new_funds += result['new_funds']
        total_updated_funds += result['updated_funds']

    # 完成
    print(f"\n{'='*80}")
    print("✅ 每日累积跟踪任务完成！")
    print(f"{'='*80}")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n累积跟踪文件:")
    print(f"  📊 {tracker_file}")
    print(f"\n统计信息:")
    print(f"  处理文件数: {result.get('processed_result_count', len(files_to_process))}")
    print(f"  总对比次数: {result['total_comparisons']} 份\"当日最终优质基金\"")
    print(f"  重复出现基金数（Excel中显示）: {result['total_funds']}")
    print(f"  内部记录基金数（包括单次）: {result.get('total_internal_funds', result['total_funds'])}")
    print(f"  累计新增: {total_new_funds}")
    print(f"  累计更新: {total_updated_funds}")
    print(f"\n💡 说明: 只有在至少2份文件中都出现的基金才会显示在Excel中")

    return 0
if __name__ == "__main__":
    sys.exit(main())
