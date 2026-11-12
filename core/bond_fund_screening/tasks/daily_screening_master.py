#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日筛选主编排任务 - daily_screening_master

功能：
作为主控脚本，按顺序执行4个子任务：
1. 长期筛选任务 (daily_long_term_screening)
2. 短期筛选任务 (daily_short_term_screening)
3. 最终筛选任务 (daily_final_screening)
4. 累积跟踪任务 (daily_accumulation_tracker)

预计执行时间：
- 任务1（长期筛选）: ~24分钟（5个年期分析 + 交集合并）
- 任务2（短期筛选）: ~12分钟（4个短期分析 + 交集合并）
- 任务3（最终筛选）: ~30秒（交集对比）
- 任务4（累积跟踪）: ~10秒（更新跟踪表）
- 总计：约37-40分钟

使用方法：
    python3 -m bond_fund_screening.tasks.daily_screening_master
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
    parser = argparse.ArgumentParser(description='债券基金每日筛选主编排任务')
    return parser.parse_args(argv)


if __name__ == "__main__" and any(arg in {"-h", "--help"} for arg in sys.argv[1:]):
    parse_args()
    raise SystemExit(0)

from bond_fund_screening.tasks.bootstrap import ensure_project_venv


ensure_project_venv(PROJECT_ROOT)

from datetime import datetime
import subprocess
import traceback
from bond_fund_screening.tasks.runtime_env import apply_screening_runtime_defaults
from bond_fund_screening.tasks.accumulation_file_utils import ACCUMULATION_INPUT_FILE_ENV
from bond_fund_screening.tasks.subprocess_logging import (
    print_result_tail,
    run_structured_subprocess,
)


apply_screening_runtime_defaults(
    trigger_source='direct_bond_master',
    task_id='direct_bond_screening_master',
    task_name='direct_bond_screening_master',
)


FINAL_OUTPUT_MARKER = 'FUND_ANA_FINAL_OUTPUT='


def extract_final_output_path(stdout_text: str) -> Path | None:
    """Extract the exact final workbook emitted by the successful final stage."""
    for raw_line in reversed(str(stdout_text or '').splitlines()):
        line = raw_line.strip()
        if not line.startswith(FINAL_OUTPUT_MARKER):
            continue
        path_text = line[len(FINAL_OUTPUT_MARKER):].strip()
        if path_text:
            return Path(path_text)
    return None


def run_task(task_name: str, task_module: str) -> tuple:
    """
    运行单个子任务

    Args:
        task_name: 任务名称（用于显示）
        task_module: 任务模块路径

    Returns:
        (是否成功, 返回码, 执行时间（秒）)
    """
    print(f"\n{'='*80}")
    print(f"【{task_name}】")
    print(f"{'='*80}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start_time = datetime.now()

    try:
        cmd = [sys.executable, '-m', task_module]
        result = run_structured_subprocess(
            task_name=task_name,
            cmd=cmd,
            cwd=CORE_ROOT,
            timeout=3600,
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

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

        if task_module == 'bond_fund_screening.tasks.daily_final_screening':
            final_output_path = extract_final_output_path(result.stdout_text)
            if final_output_path is None or not final_output_path.is_file():
                raise RuntimeError(
                    '最终筛选虽返回成功，但缺少可验证的同轮正式 Excel 路径；停止累积跟踪'
                )
            os.environ[ACCUMULATION_INPUT_FILE_ENV] = str(final_output_path.resolve())
            print(f"   同轮累积跟踪输入: {final_output_path.name}")

        print(f"\n✅ {task_name} 完成")
        print(f"   执行时间: {duration:.1f}秒")

        return True, 0, duration

    except subprocess.CalledProcessError as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n❌ {task_name} 失败")
        print(f"   返回码: {e.returncode}")
        if e.stderr:
            print(f"   错误输出:\n{e.stderr[-2000:]}")
        if e.stdout:
            print(f"   标准输出(末尾):\n{e.stdout[-1000:]}")
        print(f"   执行时间: {duration:.1f}秒")

        return False, e.returncode, duration

    except subprocess.TimeoutExpired as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n❌ {task_name} 超时（超过3600秒）")
        if getattr(e, "output", None):
            print(f"   标准输出(末尾):\n{str(e.output)[-1000:]}")
        if getattr(e, "stderr", None):
            print(f"   错误输出:\n{str(e.stderr)[-2000:]}")
        print(f"   执行时间: {duration:.1f}秒")

        return False, -2, duration

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n❌ {task_name} 异常: {e}")
        print(f"   执行时间: {duration:.1f}秒")
        traceback.print_exc()

        return False, -1, duration


def main():
    """主函数"""
    print("\n" + "="*100)
    print(" " * 30 + "【每日筛选主编排任务】")
    print("="*100)
    print(f"总任务开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"预计执行时间: 37-40分钟")

    overall_start_time = datetime.now()
    # 每轮都必须由本轮最终筛选重新建立精确输入契约，禁止继承父进程残留路径。
    os.environ.pop(ACCUMULATION_INPUT_FILE_ENV, None)

    # 定义任务列表
    tasks = [
        ("任务1: 长期筛选", "bond_fund_screening.tasks.daily_long_term_screening"),
        ("任务2: 短期筛选", "bond_fund_screening.tasks.daily_short_term_screening"),
        ("任务3: 最终筛选", "bond_fund_screening.tasks.daily_final_screening"),
        ("任务4: 累积跟踪", "bond_fund_screening.tasks.daily_accumulation_tracker"),
    ]

    stop_on_first_failure = True

    # 记录任务结果
    task_results = []

    # 逐一执行任务
    for idx, (task_name, task_module) in enumerate(tasks, start=1):
        success, return_code, duration = run_task(task_name, task_module)

        task_results.append({
            'name': task_name,
            'success': success,
            'return_code': return_code,
            'duration': duration
        })

        if not success:
            print(f"\n⚠️  警告: {task_name} 失败")
            if stop_on_first_failure:
                print("   已启用严格串行流水线：停止后续任务，避免混用旧产物")
                break

    # 计算总执行时间
    overall_end_time = datetime.now()
    total_duration = (overall_end_time - overall_start_time).total_seconds()

    # 打印执行摘要
    print(f"\n{'='*100}")
    print(" " * 35 + "【执行摘要】")
    print(f"{'='*100}")

    print(f"\n总执行时间: {total_duration:.1f}秒 ({total_duration/60:.1f}分钟)")
    print(f"结束时间: {overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"\n任务执行结果:")
    print(f"{'  序号':<8} {'任务名称':<25} {'状态':<10} {'执行时间':<15} {'返回码':<10}")
    print(f"  {'-'*70}")

    all_success = True
    for idx, result in enumerate(task_results, start=1):
        status = "✅ 成功" if result['success'] else "❌ 失败"
        duration_str = f"{result['duration']:.1f}秒"
        return_code = result['return_code']

        print(f"  {idx:<8} {result['name']:<25} {status:<10} {duration_str:<15} {return_code:<10}")

        if not result['success']:
            all_success = False

    # 最终结论
    print(f"\n{'='*100}")
    if all_success:
        print(" " * 35 + "✅ 全部任务执行成功！")
    else:
        failed_count = sum(1 for r in task_results if not r['success'])
        print(f" " * 30 + f"⚠️  {failed_count}/{len(task_results)} 个任务失败")
    print(f"{'='*100}\n")

    # 返回码：0表示全部成功，1表示有失败
    return 0 if all_success else 1
if __name__ == "__main__":
    sys.exit(main())
