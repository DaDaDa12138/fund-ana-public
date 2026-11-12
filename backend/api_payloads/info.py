from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
from typing import Any

from backend.risk_metric_contract import risk_metric_formula_contract
from backend.upstream_api_contract import upstream_api_source_contract


API_ENDPOINT_CONTRACTS: tuple[dict[str, Any], ...] = (
    {
        "endpoint": "/api/info",
        "method": "GET",
        "default_scope": "lightweight",
        "default_heavy": False,
        "purpose": "版本、发布指针、运行环境和端点契约元信息",
    },
    {
        "endpoint": "/api/health/summary",
        "method": "GET",
        "default_scope": "lightweight",
        "default_heavy": False,
        "purpose": "前端页头自动化与发布摘要",
    },
    {
        "endpoint": "/api/storage/usage",
        "method": "GET",
        "default_scope": "saved_storage_observations",
        "default_heavy": False,
        "purpose": "读取定期容量采样、分项占用和实际净增长；请求不扫描目录或数据库",
    },
    {
        "endpoint": "/api/bootstrap/summary",
        "method": "GET",
        "default_scope": "lightweight",
        "default_heavy": False,
        "purpose": "前端启动用轻量健康与工作流摘要",
    },
    {
        "endpoint": "/api/status/summary",
        "method": "GET",
        "default_scope": "workflow_summary",
        "default_heavy": False,
        "purpose": "任务中心工作流状态摘要",
    },
    {
        "endpoint": "/api/settings/apis",
        "method": "GET/POST",
        "default_scope": "runtime_config",
        "default_heavy": False,
        "purpose": "读取或更新 API 接口地址配置，不读取或生成基金业务数据",
    },
    {
        "endpoint": "/api/settings/apis/probe",
        "method": "POST",
        "default_scope": "tiantian_probe",
        "default_heavy": False,
        "purpose": "轻量 Tiantian 业务探针，只验证接口可消费状态，不写入数据库",
    },
    {
        "endpoint": "/api/dashboard/overview",
        "method": "GET",
        "default_scope": "sidecar_summary",
        "default_heavy": False,
        "purpose": "dashboard 首屏概览",
        "diagnostics": ["database_freshness_summary.source", "dashboard_build_profile"],
    },
    {
        "endpoint": "/api/dashboard/latest",
        "method": "GET",
        "default_scope": "compact_diagnostic_summary",
        "default_heavy": False,
        "purpose": "dashboard latest 默认诊断摘要",
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/dashboard/latest?detail_scope=full",
                "reason": "构建完整 dashboard detail payload",
            },
            {
                "endpoint": "/api/dashboard/latest?detail_scope=audit_full",
                "reason": "构建审计级 dashboard detail payload",
            },
            {
                "endpoint": "/api/dashboard/latest?full=1",
                "reason": "保留完整样本字段，响应体扩大",
            },
        ],
    },
    {
        "endpoint": "/api/dashboard/latest/reports",
        "method": "GET",
        "default_scope": "reports",
        "default_heavy": False,
        "purpose": "报告中心快照详情",
    },
    {
        "endpoint": "/api/dashboard/latest/detail/full-audit",
        "method": "GET",
        "default_scope": "audit_full",
        "default_heavy": True,
        "purpose": "兼容审计路径，仅显式深度审计使用",
        "recommended_alternative": "/api/dashboard/latest?detail_scope=diagnostic_summary&sample_limit=0",
    },
    {
        "endpoint": "/api/scheduler/status/detail",
        "method": "GET",
        "default_scope": "compact",
        "default_heavy": False,
        "purpose": "调度状态 detail 入口，默认轻量",
        "diagnostics": ["scheduler_detail_build_profile", "scheduler_summary_build_profile"],
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/scheduler/status/detail?full=1",
                "reason": "legacy 完整聚合，可能读取大量历史状态和闭环样本",
            }
        ],
        "recommended_alternative": "/api/scheduler/status/detail?section=<section>&sample_limit=0",
    },
    {
        "endpoint": "/api/scheduler/status/detail?detail=1",
        "method": "GET",
        "default_scope": "detail_index",
        "default_heavy": False,
        "purpose": "轻量 detail index，不触发 legacy full 聚合",
        "diagnostics": ["scheduler_detail_build_profile"],
    },
    {
        "endpoint": "/api/scheduler/status/detail?full=1",
        "method": "GET",
        "default_scope": "legacy_full",
        "default_heavy": True,
        "purpose": "legacy 完整调度诊断，仅显式深度审计使用",
        "recommended_alternative": "/api/scheduler/status/detail?detail=1&sample_limit=0",
    },
    {
        "endpoint": "/api/scheduler/status",
        "method": "GET",
        "default_scope": "compact",
        "default_heavy": False,
        "purpose": "调度状态紧凑摘要",
    },
    {
        "endpoint": "/api/recommendations/latest",
        "method": "GET",
        "default_scope": "summary",
        "default_heavy": False,
        "purpose": "组合建议首屏摘要",
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/recommendations/latest?scope=detail",
                "reason": "兼容 detail 明细路径，依赖 run-local sidecar 或运行时构建",
            },
            {
                "endpoint": "/api/recommendations/latest?full=1",
                "reason": "兼容 full detail 路径",
            },
        ],
        "recommended_alternative": "/api/recommendations/latest/detail?group=<group>",
    },
    {
        "endpoint": "/api/recommendations/latest/detail",
        "method": "GET",
        "default_scope": "detail_sidecar",
        "default_heavy": False,
        "purpose": "组合建议明细，带 sidecar/cache 诊断",
        "diagnostics": ["payload_scope.recommendations_detail_diagnostics"],
    },
    {
        "endpoint": "/api/tracker/current",
        "method": "GET",
        "default_scope": "compact",
        "default_heavy": False,
        "purpose": "tracker 当前在榜首屏摘要",
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/tracker/current?full=1",
                "reason": "完整明细路径，依赖 run-local derived fields sidecar 或运行时构建",
            }
        ],
        "recommended_alternative": "/api/tracker/current?page_size=50&page=1",
    },
    {
        "endpoint": "/api/tracker/current?full=1",
        "method": "GET",
        "default_scope": "detail_sidecar",
        "default_heavy": True,
        "purpose": "tracker 当前在榜完整明细，仅用户显式打开",
        "diagnostics": ["payload_scope.tracker_current_detail_diagnostics"],
        "recommended_alternative": "/api/tracker/current?page_size=50&page=1",
    },
    {
        "endpoint": "/api/reports",
        "method": "GET",
        "default_scope": "compact",
        "default_heavy": False,
        "purpose": "报告列表摘要",
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/reports?full=1",
                "reason": "manager.list_reports 完整列表兼容路径",
            }
        ],
    },
    {
        "endpoint": "/api/reports/latest",
        "method": "GET",
        "default_scope": "published_report_summary",
        "default_heavy": False,
        "purpose": "当前 latest 发布报告摘要，必须与 publication registry 指针对齐",
    },
    {
        "endpoint": "/api/publication/summary",
        "method": "GET",
        "default_scope": "summary",
        "default_heavy": False,
        "purpose": "发布注册表紧凑摘要",
    },
    {
        "endpoint": "/api/publication",
        "method": "GET",
        "default_scope": "summary",
        "default_heavy": False,
        "purpose": "发布注册表摘要",
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/publication?full=1",
                "reason": "完整 publication registry，含自动化层和完整一致性详情",
            }
        ],
    },
    {
        "endpoint": "/api/favorites/score",
        "method": "POST",
        "default_scope": "compact",
        "default_heavy": False,
        "purpose": "收藏评分默认紧凑响应",
        "explicit_heavy_modes": [
            {
                "endpoint": "/api/favorites/score?full=1",
                "reason": "逐行返回年度收益明细和风格参考对象，响应体显著扩大",
            },
            {
                "endpoint": "/api/favorites/score?detail=1",
                "reason": "兼容 full detail 响应",
            },
        ],
        "recommended_alternative": "/api/favorites/metric-details",
    },
)


def _safe_json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _project_relative_path_text(path: Path | str | None, *, project_root: Path) -> str | None:
    if path is None:
        return None
    current = Path(path)
    try:
        return str(current.relative_to(project_root))
    except ValueError:
        return str(current)


def build_api_info_payload(
    *,
    settings: Any,
    version: str,
    version_info: dict[str, Any],
    changelog: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    runtime_started_at: str,
    manual_operations_enabled: bool,
    publication: dict[str, Any] | None,
    latest_report_snapshot: dict[str, Any] | None,
    scheduler_state_file: Path | str,
) -> dict[str, Any]:
    publication_payload = publication or {}
    latest_snapshot = latest_report_snapshot or {}
    latest_changelog = dict(changelog[0]) if changelog else {}
    latest_changes = latest_changelog.get("changes") if isinstance(latest_changelog.get("changes"), list) else []
    latest_change_preview = [
        str(change).strip()
        for change in latest_changes
        if str(change).strip()
    ][:5]
    project_root = Path(settings.project_root)
    build_info_path = Path(os.environ.get("FUND_ANA_BUILD_INFO_FILE") or project_root / "build_info.json")
    build_info = _safe_json_load(build_info_path)
    latest_run_id = str(latest_snapshot.get("run_id") or "").strip() or None
    current_published_run_id = str(publication_payload.get("current_published_run_id") or "").strip() or None
    latest_canonical_run_id = str(publication_payload.get("latest_canonical_run_id") or "").strip() or None
    return {
        "status": "ok",
        "service": "fund-ana",
        "runtime_started_at": runtime_started_at,
        "manual_operations_enabled": bool(manual_operations_enabled),
        "version": {
            "code_version": version,
            "title": version_info.get("title") or latest_changelog.get("title"),
            "release_date": version_info.get("release_date") or latest_changelog.get("date"),
            "released_at": version_info.get("released_at") or latest_changelog.get("released_at"),
            "release_status": latest_changelog.get("release_status") or "released",
            "changelog_file": f"core/docs/CHANGELOG_v{version}.md",
            "latest_changelog": {
                "version": latest_changelog.get("version") or version,
                "title": latest_changelog.get("title") or version_info.get("title"),
                "released_at": latest_changelog.get("released_at") or version_info.get("released_at"),
                "change_count": len(latest_changelog.get("changes") or []),
                "changes": latest_change_preview,
            },
        },
        "publication": {
            "latest_report_run_id": latest_run_id,
            "latest_canonical_run_id": latest_canonical_run_id,
            "current_published_run_id": current_published_run_id,
            "latest_report_matches_current_published": bool(
                latest_run_id and current_published_run_id and latest_run_id == current_published_run_id
            ),
            "latest_report_matches_latest_canonical": bool(
                latest_run_id and latest_canonical_run_id and latest_run_id == latest_canonical_run_id
            ),
            "latest_canonical_report_path": publication_payload.get("latest_canonical_report_path"),
            "current_published_report_path": publication_payload.get("current_published_report_path"),
            "canonical_run_count": len(publication_payload.get("canonical_runs") or []),
            "publication_registry_file": _project_relative_path_text(
                settings.publication_registry_file,
                project_root=project_root,
            ),
            "latest_report_file": _project_relative_path_text(settings.latest_report_file, project_root=project_root),
        },
        "runtime": {
            "project_root": str(project_root),
            "reports_dir": _project_relative_path_text(settings.reports_dir, project_root=project_root),
            "runs_dir": _project_relative_path_text(settings.runs_dir, project_root=project_root),
            "database_path": _project_relative_path_text(settings.database_path, project_root=project_root),
            "scheduler_state_file": _project_relative_path_text(scheduler_state_file, project_root=project_root),
            "disable_embedded_scheduler": os.environ.get("FUND_ANA_DISABLE_EMBEDDED_SCHEDULER", "").strip(),
            "web_port": os.environ.get("FUND_ANA_WEB_PORT", ""),
            "container_role": os.environ.get("FUND_ANA_CONTAINER_ROLE", ""),
        },
        "build": {
            "build_time": os.environ.get("FUND_ANA_BUILD_TIME") or build_info.get("build_time"),
            "code_version": build_info.get("code_version") or version,
            "title": build_info.get("title") or version_info.get("title") or latest_changelog.get("title"),
            "image_id": os.environ.get("FUND_ANA_IMAGE_ID") or build_info.get("image_id"),
            "image_tag": os.environ.get("FUND_ANA_IMAGE_TAG") or build_info.get("image_tag"),
            "running_in_container": Path("/.dockerenv").exists(),
            "build_info_file": (
                _project_relative_path_text(build_info_path, project_root=project_root)
                if build_info_path.exists()
                else None
            ),
            "build_info_available": bool(build_info),
        },
        "endpoint_contracts": deepcopy(API_ENDPOINT_CONTRACTS),
        "heavy_endpoint_count": sum(1 for item in API_ENDPOINT_CONTRACTS if item.get("default_heavy")),
        "heavy_endpoint_policy": {
            "default_contract": "compact_or_sidecar_summary",
            "explicit_heavy_query_flags": ["full=1", "detail_scope=full", "detail_scope=audit_full"],
            "user_interface_default": "do_not_call_heavy_endpoints_without_explicit_user_action",
        },
        "truth_sources": {
            "publication_truth": [
                "reports/publication_registry.json",
                "reports/latest.json",
                "current canonical run artifacts",
            ],
            "business_data_truth": [
                "local sqlite database",
                "real upstream APIs",
                "real runtime artifacts",
            ],
            "missing_value_policy": "preserve_missing_semantics",
        },
        "upstream_api_source_contract": upstream_api_source_contract(settings=settings),
        "risk_metric_formula_contract": risk_metric_formula_contract(),
        "payload_scope": {
            "mode": "lightweight_info",
            "database_heavy_read": False,
            "scheduler_heavy_read": False,
            "report_builder_invoked": False,
        },
    }
