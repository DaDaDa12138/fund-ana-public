#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日短期筛选任务 - daily_short_term_screening

功能：
1. 逐一运行4个短期分析筛选脚本（1week/1month/3month/6month）
2. 对生成的4个短周期Excel文件进行交集分析
3. 生成最终的近期优质基金Excel表格（带时间戳）

执行顺序：
- 一周期分析 → 一月期分析 → 三月期分析 → 半年期分析
- 多周期交集合并分析
- 生成近期优质基金_{timestamp}.xlsx

使用方法：
    python3 -m bond_fund_screening.tasks.daily_short_term_screening
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """尽早解析命令行参数，确保 `--help` 不触发后续重型导入。"""
    parser = argparse.ArgumentParser(description='债券基金每日短期筛选任务')
    return parser.parse_args(argv)


if __name__ == "__main__" and any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
    parse_args()
    raise SystemExit(0)

from bond_fund_screening.tasks.bootstrap import ensure_project_venv


ensure_project_venv(PROJECT_ROOT)

from datetime import datetime
import subprocess
from bond_fund_screening.tasks.runtime_env import (
    apply_screening_runtime_defaults,
    screening_report_date,
)
from bond_fund_screening.tasks.subprocess_logging import print_result_tail, run_structured_subprocess


apply_screening_runtime_defaults(
    trigger_source='direct_bond_short_term_script',
    task_id='direct_bond_short_term_screening',
    task_name='direct_bond_short_term_screening',
)

from bond_fund_screening.config import (
    DAILY_SCREENING_CONFIG,
    OUTPUT_DIR,
    SHORT_TERM_PERIOD_CONFIGS
)
from bond_fund_screening.analyzers.multi_period_merger import MultiPeriodMerger
from utils.core_csv_storage import glob_artifacts
from bond_fund_screening.tasks.period_file_utils import (
    build_period_simple_csv_pattern,
    find_current_run_output_file,
    resolve_current_run_output,
    snapshot_matching_files,
)


def run_short_period_analysis(period_key: str, top_n: int | None = None) -> tuple:
    """
    运行单个短期周期的分析

    Args:
        period_key: 周期键（'1week', '1month', '3month', '6month'）
        top_n: 可选输出基金数量上限；默认输出全部达标基金

    Returns:
        (是否成功, 本轮输出的 simple.csv 路径)
    """
    period_config = SHORT_TERM_PERIOD_CONFIGS[period_key]
    period_name = period_config['name']
    pattern = build_period_simple_csv_pattern("债券型基金", period_key)
    before_files = snapshot_matching_files(OUTPUT_DIR, pattern)

    print(f"\n{'='*80}")
    print(f"运行 {period_name} 分析...")
    print(f"{'='*80}")

    try:
        # 调用对应周期的分析器脚本
        script_name = f'bond_analyzer_{period_key}'
        cmd = [
            sys.executable,
            '-m', f'bond_fund_screening.analyzers.{script_name}',
        ]
        if top_n is not None:
            cmd.extend(['--top-n', str(top_n)])
        cmd.extend(['--freshness-strategy', 'hard'])

        result = run_structured_subprocess(
            task_name=f"short-{period_key}",
            cmd=cmd,
            cwd=CORE_ROOT,
            timeout=3600,
        )
        print_result_tail(result, stdout_limit=2000, stderr_limit=2000)
        if not result.success:
            if result.timed_out:
                raise subprocess.TimeoutExpired(cmd, timeout=3600)
            raise subprocess.CalledProcessError(
                result.return_code,
                cmd,
                output="\n".join(result.stdout_tail),
                stderr="\n".join(result.stderr_tail),
            )

        print(f"✅ {period_name}分析完成")
        success, output_file, empty_result = resolve_current_run_output(
            OUTPUT_DIR,
            pattern,
            before_files,
            stdout_text=result.stdout_text,
        )
        if not success:
            print(f"❌ {period_name}分析完成但未发现本轮输出文件: {pattern}")
            return False, None
        if empty_result:
            print(
                f"❌ {period_name}分析本轮无基金通过；配置周期缺失会改变短期多周期"
                "交叉验证口径，停止本轮发布链"
            )
            return False, None
        return True, output_file

    except subprocess.TimeoutExpired as e:
        print(f"❌ {period_name}分析超时（>3600秒）")
        if getattr(e, "output", None):
            print(f"   标准输出(末尾):\n{str(e.output)[-1000:]}")
        if getattr(e, "stderr", None):
            print(f"   错误输出:\n{str(e.stderr)[-2000:]}")
        return False, None
    except subprocess.CalledProcessError as e:
        print(f"❌ {period_name}分析失败")
        print(f"   返回码: {e.returncode}")
        if e.stderr:
            print(f"   错误输出:\n{e.stderr[-2000:]}")
        if e.stdout:
            print(f"   标准输出(末尾):\n{e.stdout[-1000:]}")
        return False, None
    except Exception as e:
        print(f"❌ {period_name}分析异常: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def run_multi_period_merger(periods: list, output_name: str, period_files: dict = None) -> tuple:
    """
    运行短期多周期交集合并分析

    Args:
        periods: 周期列表（['1week', '1month', ...]）
        output_name: 输出文件名前缀

    Returns:
        (是否成功, CSV文件路径, Excel文件路径)
    """
    print(f"\n{'='*80}")
    print(f"运行短期多周期交集合并分析...")
    print(f"{'='*80}")

    try:
        merger = MultiPeriodMerger(
            input_dir=OUTPUT_DIR,
            verbose=True,
            is_short_term=True,  # 短期分析
            period_files=period_files,
            collapse_share_classes=True,
        )

        # 运行分析
        stability_ranking, intersections = merger.run(
            periods=periods,
            output_name=output_name
        )

        print(f"✅ 短期多周期交集分析完成")
        print(f"   基金数量: {len(stability_ranking)}")

        # 查找生成的文件
        timestamp_pattern = screening_report_date()
        csv_files = sorted(glob_artifacts(OUTPUT_DIR, f"{output_name}_{timestamp_pattern}*.csv"), reverse=True)
        excel_files = sorted(OUTPUT_DIR.glob(f"{output_name}_详细报告_{timestamp_pattern}*.xlsx"), reverse=True)

        csv_file = csv_files[0] if csv_files else None
        excel_file = excel_files[0] if excel_files else None

        return True, csv_file, excel_file

    except Exception as e:
        print(f"❌ 短期多周期交集分析失败: {e}")
        import traceback
        traceback.print_exc()
        return False, None, None


def main():
    """主函数"""
    print("\n" + "="*80)
    print("【每日短期筛选任务】")
    print("="*80)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 获取配置
    config = DAILY_SCREENING_CONFIG
    short_term_periods = config['short_term_periods']
    top_n = config.get('top_n')

    # 记录本轮成功产物，禁止后续合并混入旧文件
    period_files = {}
    # 步骤1: 逐一运行4个短期周期分析
    print(f"\n{'='*80}")
    print(f"【步骤1】运行 {len(short_term_periods)} 个短期周期分析")
    print(f"{'='*80}")

    for period_key in short_term_periods:
        success, output_file = run_short_period_analysis(period_key, top_n)
        if not success:
            print(f"\n❌ {period_key} 分析失败，已启用严格串行流水线，停止后续周期和合并步骤")
            return 1
        period_files[period_key] = output_file

    # 步骤2: 短期多周期交集合并
    print(f"\n{'='*80}")
    print(f"【步骤2】短期多周期交集合并分析")
    print(f"{'='*80}")

    successful_periods = list(period_files.keys())
    if len(successful_periods) != len(short_term_periods):
        print("❌ 本轮短期周期产物不完整，停止多周期交集分析")
        print(f"   期望周期数: {len(short_term_periods)}，实际周期数: {len(successful_periods)}")
        return 1

    # 生成输出文件名前缀（时间戳由 MultiPeriodMerger.save_results 统一追加）
    output_name = "近期优质基金"

    success, csv_file, excel_file = run_multi_period_merger(
        successful_periods,
        output_name,
        period_files=period_files,
    )

    if not success:
        print(f"\n❌ 短期多周期交集分析失败")
        return 1

    # 完成
    print(f"\n{'='*80}")
    print("✅ 每日短期筛选任务完成！")
    print(f"{'='*80}")
    print(f"结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n生成文件:")
    if csv_file:
        print(f"  📄 CSV: {csv_file}")
    if excel_file:
        print(f"  📊 Excel: {excel_file}")

    return 0
if __name__ == "__main__":
    sys.exit(main())
