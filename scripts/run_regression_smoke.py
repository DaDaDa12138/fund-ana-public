#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_VENV_BOOTSTRAP_ENV = "BOND_SCREENING_VENV_BOOTSTRAPPED"


def _ensure_project_venv() -> None:
    if not VENV_PYTHON.exists():
        return
    current_prefix = Path(getattr(sys, "prefix", "") or "").resolve()
    target_prefix = (PROJECT_ROOT / ".venv").resolve()
    if current_prefix == target_prefix:
        return
    attempted_prefix = os.environ.get(_VENV_BOOTSTRAP_ENV, "")
    if attempted_prefix == str(target_prefix):
        return
    if Path(sys.executable) != VENV_PYTHON:
        os.environ[_VENV_BOOTSTRAP_ENV] = str(target_prefix)
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

REPORT_ROOT = PROJECT_ROOT / "reports" / "audit" / "regression_smoke"


@dataclass(frozen=True)
class CommandSpec:
    label: str
    argv: list[str]


@dataclass(frozen=True)
class GroupSpec:
    key: str
    description: str
    commands: list[CommandSpec]


def _discover_python_files() -> list[str]:
    python_files: list[str] = []
    for folder_name in ("backend", "scripts", "tests"):
        folder = PROJECT_ROOT / folder_name
        if not folder.exists():
            continue
        python_files.extend(
            str(path.relative_to(PROJECT_ROOT))
            for path in sorted(folder.rglob("*.py"))
            if path.is_file()
        )
    return python_files


def _build_smoke_groups() -> list[GroupSpec]:
    python_bin = str(Path(sys.executable))
    node_bin = shutil.which("node")
    python_files = _discover_python_files()

    syntax_commands = [
        CommandSpec(
            label="python_py_compile",
            argv=[python_bin, "-m", "py_compile", *python_files],
        )
    ]
    if node_bin:
        syntax_commands.extend(
            [
                CommandSpec(
                    label="frontend_shared_js_check",
                    argv=[node_bin, "--check", "frontend/shared.js"],
                ),
                CommandSpec(
                    label="frontend_operations_js_check",
                    argv=[node_bin, "--check", "frontend/operations.js"],
                ),
            ]
        )

    return [
        GroupSpec(
            key="syntax",
            description="语法与前端脚本静态检查",
            commands=syntax_commands,
        ),
        GroupSpec(
            key="workflow",
            description="工作流与调度编排核心烟雾回归",
            commands=[
                CommandSpec(
                    label="pytest_workflow_core",
                    argv=[
                        python_bin,
                        "-m",
                        "pytest",
                        "-q",
                        "tests/test_regression_guards.py::test_screening_snapshot_changed_blocks_same_date_rescreen_when_only_future_outliers_changed",
                        "tests/test_regression_guards.py::test_screening_snapshot_changed_allows_same_date_rescreen_without_future_outliers",
                        "tests/test_regression_guards.py::test_build_core_commands_uses_changed_nav_only_force_risk_refresh_for_screening",
                        "tests/test_regression_guards.py::test_build_core_commands_adds_changed_nav_only_risk_refresh_after_nav_collection",
                        "tests/test_regression_guards.py::test_build_core_commands_can_skip_risk_calculation_during_collection",
                        "tests/test_regression_guards.py::test_build_core_commands_weekly_uses_bounded_detail_and_manager_refresh",
                    ],
                )
            ],
        ),
        GroupSpec(
            key="scheduler_guard",
            description="调度状态、自动化护栏与官方入口口径回归",
            commands=[
                CommandSpec(
                    label="pytest_scheduler_guard",
                    argv=[
                        python_bin,
                        "-m",
                        "pytest",
                        "-q",
                        "tests/test_regression_guards.py::test_health_endpoint_exposes_database_source_audit",
                        "tests/test_regression_guards.py::test_scheduler_status_endpoint_promotes_overview_runs_to_top_level",
                        "tests/test_regression_guards.py::test_scheduler_status_endpoint_exposes_pending_manual_run_requests_and_recovery_status",
                        "tests/test_regression_guards.py::test_build_scheduler_status_payload_prefers_latest_weekly_run_over_stale_schedule_slot",
                    ],
                )
            ],
        ),
        GroupSpec(
            key="dashboard_payload",
            description="Dashboard 关键字段与治理摘要回归",
            commands=[
                CommandSpec(
                    label="pytest_dashboard_payload",
                    argv=[
                        python_bin,
                        "-m",
                        "pytest",
                        "-q",
                        "tests/test_regression_guards.py::test_dashboard_latest_uses_scheduler_overview_api_summary",
                        "tests/test_regression_guards.py::test_dashboard_latest_exposes_long_term_governance_summary",
                        "tests/test_regression_guards.py::test_dashboard_latest_exposes_publish_reason_summary",
                    ],
                )
            ],
        ),
        GroupSpec(
            key="publication_integrity",
            description="发布注册表与产物一致性回归",
            commands=[
                CommandSpec(
                    label="pytest_publication_integrity",
                    argv=[
                        python_bin,
                        "-m",
                        "pytest",
                        "-q",
                        "tests/test_regression_guards.py::test_publication_registry_keeps_latest_publishable_run",
                        "tests/test_regression_guards.py::test_publication_registry_publishes_latest_contraction_and_records_watch_signal",
                        "tests/test_regression_guards.py::test_load_publication_registry_exposes_automation_layers_and_blockers",
                        "tests/test_regression_guards.py::test_artifact_consistency_audit_accepts_self_consistent_outputs",
                        "tests/test_regression_guards.py::test_artifact_consistency_audit_does_not_require_report_files_preflushed",
                        "tests/test_regression_guards.py::test_artifact_consistency_audit_flags_count_mismatch",
                    ],
                )
            ],
        ),
        GroupSpec(
            key="scheduler_failure_drill",
            description="调度失败态非破坏性演练与可观测闭环回归",
            commands=[
                CommandSpec(
                    label="audit_scheduler_failure_drill",
                    argv=[python_bin, "scripts/audit_scheduler_failure_drill.py"],
                )
            ],
        ),
        GroupSpec(
            key="strategy_reason_data_quality",
            description="筛选原因解释与风险数据质量联动审计",
            commands=[
                CommandSpec(
                    label="audit_strategy_reason_data_quality",
                    argv=[python_bin, "scripts/audit_strategy_reason_data_quality.py"],
                )
            ],
        ),
        GroupSpec(
            key="dynamic_quality_gate_overlap",
            description="动态质量门槛重叠与疑似冗余审计",
            commands=[
                CommandSpec(
                    label="audit_dynamic_quality_gate_overlap",
                    argv=[python_bin, "scripts/audit_dynamic_quality_gate_overlap.py"],
                )
            ],
        ),
        GroupSpec(
            key="score_cluster_explainability",
            description="综合/近期/长期得分簇职责拆分审计",
            commands=[
                CommandSpec(
                    label="audit_score_cluster_explainability",
                    argv=[python_bin, "scripts/audit_score_cluster_explainability.py"],
                )
            ],
        ),
        GroupSpec(
            key="annual_return_score_sharpe_coupling",
            description="年化收益与得分/夏普耦合职责拆分审计",
            commands=[
                CommandSpec(
                    label="audit_annual_return_score_sharpe_coupling",
                    argv=[python_bin, "scripts/audit_annual_return_score_sharpe_coupling.py"],
                )
            ],
        ),
        GroupSpec(
            key="risk_residual_warnings",
            description="风险数据残余告警审计",
            commands=[
                CommandSpec(
                    label="audit_risk_residual_warnings",
                    argv=[python_bin, "scripts/audit_risk_residual_warnings.py"],
                )
            ],
        ),
        GroupSpec(
            key="high_score_friction_gates",
            description="高分摩擦门禁复核审计",
            commands=[
                CommandSpec(
                    label="audit_high_score_friction_gates",
                    argv=[python_bin, "scripts/audit_high_score_friction_gates.py"],
                )
            ],
        ),
        GroupSpec(
            key="risk_adjusted_metric_stack",
            description="风险调整收益指标栈复核审计",
            commands=[
                CommandSpec(
                    label="audit_risk_adjusted_metric_stack",
                    argv=[python_bin, "scripts/audit_risk_adjusted_metric_stack.py"],
                )
            ],
        ),
        GroupSpec(
            key="final_selection_contraction_shadow",
            description="最终精选收敛风格边界 shadow 审计",
            commands=[
                CommandSpec(
                    label="audit_final_selection_contraction_shadow",
                    argv=[python_bin, "scripts/audit_final_selection_contraction_shadow.py"],
                )
            ],
        ),
    ]


def _select_groups(
    available_groups: list[GroupSpec],
    requested_group_keys: list[str],
) -> list[GroupSpec]:
    group_map = {group.key: group for group in available_groups}
    if not requested_group_keys or requested_group_keys == ["all"]:
        return available_groups

    selected: list[GroupSpec] = []
    for key in requested_group_keys:
        normalized_key = str(key or "").strip()
        if normalized_key == "all":
            return available_groups
        group = group_map.get(normalized_key)
        if group is None:
            raise SystemExit(f"未知 smoke 分组: {normalized_key}")
        selected.append(group)
    return selected


def _sanitize_filename(text: str) -> str:
    keep_chars = []
    for char in str(text or "").strip().lower():
        if char.isalnum() or char in {"-", "_"}:
            keep_chars.append(char)
        else:
            keep_chars.append("_")
    return "".join(keep_chars).strip("_") or "artifact"


def _group_status_from_command_results(command_results: list[dict[str, Any]]) -> str:
    statuses = [str(item.get("status") or "") for item in command_results]
    if any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "passed" for status in statuses):
        return "passed"
    return "skipped"


_ISSUE_CLASSIFICATION_LABELS = {
    "passed": "全部通过",
    "expected_warning": "预期 warning",
    "test_expectation_mismatch": "测试口径不匹配",
    "production_degradation": "真实退化",
    "environment_issue": "运行环境问题",
    "unknown_failure": "未知失败",
    "mixed": "混合问题",
}


def _collect_failure_snippets(stdout_text: str, stderr_text: str, max_snippets: int = 8) -> list[str]:
    snippets: list[str] = []
    for raw_line in [*(stdout_text or "").splitlines(), *(stderr_text or "").splitlines()]:
        line = str(raw_line or "").strip()
        if not line:
            continue
        lowered = line.lower()
        if (
            "assertionerror" in lowered
            or line.startswith("E   ")
            or line.startswith("FAILED ")
            or "traceback" in lowered
            or "exception" in lowered
            or "error" in lowered
            or "warning" in lowered and "ready" in lowered
        ):
            snippets.append(line[:500])
        if len(snippets) >= max_snippets:
            break
    return snippets


def _classify_command_issue(
    *,
    returncode: int,
    stdout_text: str,
    stderr_text: str,
) -> dict[str, Any]:
    if int(returncode or 0) == 0:
        return {
            "issue_classification_status": "passed",
            "issue_classification_label": _ISSUE_CLASSIFICATION_LABELS["passed"],
            "issue_classification_detail": "命令退出码为 0。",
            "evidence_snippets": [],
        }

    combined = f"{stdout_text or ''}\n{stderr_text or ''}"
    lowered = combined.lower()
    evidence_snippets = _collect_failure_snippets(stdout_text, stderr_text)
    normalized = combined.replace('"', "'")

    if "assert 'warning' == 'ready'" in normalized or (
        "assertionerror" in lowered and "'warning'" in normalized and "'ready'" in normalized
    ):
        status = "test_expectation_mismatch"
        detail = "真实运行态返回 warning，而 smoke 断言仍期望 ready；需要复核测试口径或业务 warning 语义。"
    elif ("modulenotfounderror" in lowered or "importerror" in lowered or "no module named" in lowered
          or "isolation_error" in lowered or "unsafe_pytest_entrypoint" in lowered):
        status = "environment_issue"
        detail = "命令失败形态指向依赖或运行环境问题。"
    elif (
        "database is locked" in lowered
        or "operationalerror" in lowered
        or "connectionerror" in lowered
        or "traceback" in lowered
    ):
        status = "production_degradation"
        detail = "命令失败包含运行时异常或外部资源错误，需要按真实退化复核。"
    else:
        status = "unknown_failure"
        detail = "命令失败原因无法通过固定规则归类，需查看原始日志。"

    return {
        "issue_classification_status": status,
        "issue_classification_label": _ISSUE_CLASSIFICATION_LABELS.get(status, status),
        "issue_classification_detail": detail,
        "evidence_snippets": evidence_snippets,
    }


def _build_smoke_issue_summary(group_results: list[dict[str, Any]]) -> dict[str, Any]:
    failed_groups: list[dict[str, Any]] = []
    issue_category_counts: dict[str, int] = {}
    failed_command_total = 0

    for group in group_results:
        if str((group or {}).get("status") or "").strip() != "failed":
            continue
        commands = list((group or {}).get("commands") or [])
        failed_commands = [
            command
            for command in commands
            if str((command or {}).get("status") or "").strip() == "failed"
        ]
        command_categories: list[str] = []
        snippets: list[str] = []
        for command in failed_commands:
            failed_command_total += 1
            category = str(command.get("issue_classification_status") or "unknown_failure").strip()
            command_categories.append(category)
            issue_category_counts[category] = issue_category_counts.get(category, 0) + 1
            snippets.extend(list(command.get("evidence_snippets") or [])[:3])

        unique_categories = sorted(set(command_categories))
        if not unique_categories:
            group_status = "unknown_failure"
        elif len(unique_categories) == 1:
            group_status = unique_categories[0]
        else:
            group_status = "mixed"
        failed_groups.append(
            {
                "group": group.get("group"),
                "description": group.get("description"),
                "status": group.get("status"),
                "failed_command_total": len(failed_commands),
                "issue_classification_status": group_status,
                "issue_classification_label": _ISSUE_CLASSIFICATION_LABELS.get(group_status, group_status),
                "issue_category_counts": {
                    category: command_categories.count(category)
                    for category in sorted(set(command_categories))
                },
                "evidence_snippets": snippets[:6],
            }
        )

    if not failed_groups:
        overall_status = "passed"
    else:
        categories = set(issue_category_counts)
        if len(categories) == 1:
            overall_status = next(iter(categories))
        elif "production_degradation" in categories:
            overall_status = "mixed"
        else:
            overall_status = "mixed"

    return {
        "issue_classification_status": overall_status,
        "issue_classification_label": _ISSUE_CLASSIFICATION_LABELS.get(overall_status, overall_status),
        "issue_category_counts": issue_category_counts,
        "failed_command_total": failed_command_total,
        "failed_group_classifications": failed_groups,
        "expected_warning_total": int(issue_category_counts.get("expected_warning") or 0),
        "test_expectation_mismatch_total": int(issue_category_counts.get("test_expectation_mismatch") or 0),
        "production_degradation_total": int(issue_category_counts.get("production_degradation") or 0),
        "environment_issue_total": int(issue_category_counts.get("environment_issue") or 0),
        "unknown_failure_total": int(issue_category_counts.get("unknown_failure") or 0),
    }


def _write_command_log(
    log_path: Path,
    *,
    argv: list[str],
    returncode: int,
    elapsed_seconds: float,
    stdout_text: str,
    stderr_text: str,
) -> None:
    log_lines = [
        f"$ {shlex.join(argv)}",
        f"# exit_code: {returncode}",
        f"# elapsed_seconds: {elapsed_seconds:.2f}",
        "",
        "[stdout]",
        stdout_text.rstrip(),
        "",
        "[stderr]",
        stderr_text.rstrip(),
        "",
    ]
    log_path.write_text("\n".join(log_lines), encoding="utf-8")


def _run_group(group: GroupSpec, artifact_dir: Path) -> dict[str, Any]:
    print(f"[smoke] running group={group.key} ({group.description})")
    command_results: list[dict[str, Any]] = []

    for index, command in enumerate(group.commands, start=1):
        command_log_path = artifact_dir / f"{group.key}_{index:02d}_{_sanitize_filename(command.label)}.log"
        argv = list(command.argv)
        unit_regression = argv[1:3] == ["-m", "pytest"]
        if unit_regression:
            argv = [argv[0], "scripts/run_isolated_tests.py", "--artifact-dir",
                    str(artifact_dir / f"{group.key}_{index:02d}_isolated"), "--", *argv[3:]]
        started_at = time.time()
        completed = subprocess.run(
            argv,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )
        elapsed_seconds = round(time.time() - started_at, 2)
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        _write_command_log(
            command_log_path,
            argv=argv,
            returncode=completed.returncode,
            elapsed_seconds=elapsed_seconds,
            stdout_text=stdout_text,
            stderr_text=stderr_text,
        )
        status = "passed" if completed.returncode == 0 else "failed"
        print(
            f"[smoke]   {command.label}: {status} "
            f"(exit={completed.returncode}, elapsed={elapsed_seconds:.2f}s)"
        )
        issue_classification = _classify_command_issue(
            returncode=completed.returncode,
            stdout_text=stdout_text,
            stderr_text=stderr_text,
        )
        command_results.append(
            {
                "label": command.label,
                "argv": argv,
                "execution_scope": "isolated_unit_regression" if unit_regression else "runtime_audit_or_syntax",
                "status": status,
                "returncode": completed.returncode,
                "elapsed_seconds": elapsed_seconds,
                "log_path": str(command_log_path),
                **issue_classification,
            }
        )

    group_issue_summary = _build_smoke_issue_summary(
        [
            {
                "group": group.key,
                "description": group.description,
                "status": _group_status_from_command_results(command_results),
                "commands": command_results,
            }
        ]
    )
    group_classification = (group_issue_summary.get("failed_group_classifications") or [{}])[0]
    return {
        "group": group.key,
        "description": group.description,
        "status": _group_status_from_command_results(command_results),
        "command_count": len(command_results),
        "commands": command_results,
        "issue_classification_status": group_classification.get("issue_classification_status", "passed"),
        "issue_classification_label": group_classification.get("issue_classification_label", _ISSUE_CLASSIFICATION_LABELS["passed"]),
        "issue_category_counts": group_classification.get("issue_category_counts", {}),
        "evidence_snippets": group_classification.get("evidence_snippets", []),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="执行 Fund-ANA 关键 smoke 回归并输出固定日志/摘要。")
    parser.add_argument(
        "--group",
        action="append",
        default=[],
        help="仅执行指定 smoke 分组，可重复；默认执行全部分组。使用 --list-groups 查看可用分组。",
    )
    parser.add_argument(
        "--list-groups",
        action="store_true",
        help="只打印可用分组并退出",
    )
    args = parser.parse_args()

    available_groups = _build_smoke_groups()
    if args.list_groups:
        for group in available_groups:
            print(f"{group.key}: {group.description}")
        return 0

    selected_groups = _select_groups(available_groups, list(args.group or []))
    generated_at = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_dir = REPORT_ROOT / f"regression_smoke_{generated_at}"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    group_results = [_run_group(group, artifact_dir) for group in selected_groups]
    overall_status = "failed" if any(item["status"] == "failed" for item in group_results) else "passed"
    issue_summary = _build_smoke_issue_summary(group_results)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "project_root": str(PROJECT_ROOT),
        "artifact_dir": str(artifact_dir),
        "overall_status": overall_status,
        "selected_groups": [group.key for group in selected_groups],
        "group_results": group_results,
        **issue_summary,
    }
    summary_path = artifact_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[smoke] overall_status={overall_status}")
    print(f"[smoke] summary={summary_path}")
    return 0 if overall_status == "passed" else 1


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
