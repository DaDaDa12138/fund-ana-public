from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "project.yaml"


@dataclass(slots=True)
class Settings:
    project_root: Path
    config_path: Path
    core_root: Path
    database_path: Path
    state_file: Path
    reports_dir: Path
    runs_dir: Path
    core_output_dir: Path
    latest_report_file: Path
    publication_registry_file: Path
    favorites_history_file: Path
    archived_runs_dir: Path
    tiantian_base_url: str
    tiantian_valuation_primary_url: str
    tiantian_valuation_fallback_url: str
    autostock_base_url: str
    autostock_rate_limit_max_calls_per_hour: int
    autostock_rate_limit_min_interval_seconds: float
    autostock_rate_limit_shared_state_path: Path
    autostock_directory_cache_path: Path
    autostock_directory_cache_ttl_seconds: int
    autostock_report_validation_cache_ttl_seconds: int
    autostock_batch_fund_default: int
    autostock_batch_fund_detail_default: int
    run_collection_by_default: bool
    run_screening_by_default: bool
    collect_details: bool
    collect_managers: bool
    collect_companies: bool
    collect_risk: bool
    collect_nav_incremental: bool
    nav_workers: int
    nav_batch_size: int
    nav_batch_interval: int
    detail_workers: int
    manager_workers: int
    company_workers: int
    risk_workers: int
    holding_workers: int
    workflow_task_timeout_seconds: dict[str, int]
    workflow_task_idle_timeout_seconds: dict[str, int]
    exclude_holding_days_gt: int
    preview_top_n: int
    final_selection_target_max: int
    final_selection_group_min_ratio: float
    final_selection_high_score_new_fund_threshold: float
    final_selection_low_score_new_fund_threshold: float
    web_host: str
    web_port: int
    timezone: str
    scheduler_enabled: bool
    scheduler_host_mode: str
    scheduler_timezone: str
    scheduler_state_file: Path
    scheduler_heartbeat_file: Path
    scheduler_pid_file: Path
    scheduler_lock_file: Path
    scheduler_service_unit_name: str
    web_service_unit_name: str
    scheduler_heartbeat_interval_seconds: int
    scheduler_heartbeat_stale_after_seconds: int
    scheduler_blocked_snapshot_retry_cooldown_minutes: int
    scheduler_failed_screening_retry_cooldown_minutes: int
    scheduler_workflow_busy_retry_cooldown_seconds: int
    scheduler_daily_hour: int
    scheduler_daily_minute: int
    scheduler_daily_nav_history_sync_enabled: bool
    scheduler_daily_nav_history_sync_limit: int
    scheduler_daily_long_term_history_enabled: bool
    scheduler_daily_long_term_history_limit: int
    scheduler_daily_long_term_history_refresh_stale: bool
    scheduler_intraday_refresh_enabled: bool
    scheduler_intraday_hour: int
    scheduler_intraday_minute: int
    scheduler_intraday_nav_history_sync_enabled: bool
    scheduler_intraday_nav_history_sync_limit: int
    scheduler_intraday_nav_workers: int
    scheduler_intraday_long_term_history_enabled: bool
    scheduler_intraday_long_term_history_limit: int
    scheduler_intraday_long_term_history_refresh_stale: bool
    scheduler_weekly_day_of_week: str
    scheduler_weekly_hour: int
    scheduler_weekly_minute: int
    scheduler_weekly_nav_history_enabled: bool
    scheduler_weekly_nav_history_limit: int
    scheduler_weekly_long_term_history_enabled: bool
    scheduler_weekly_long_term_history_limit: int
    scheduler_weekly_long_term_history_refresh_stale: bool
    scheduler_weekly_plain_open_tail_follow_up_enabled: bool
    scheduler_weekly_plain_open_tail_follow_up_category: str
    scheduler_weekly_plain_open_tail_follow_up_skip_risk_refresh: bool
    scheduler_weekly_future_outlier_monitor_enabled: bool
    scheduler_weekly_future_outlier_monitor_category: str
    scheduler_weekly_holding_period_edge_audit_enabled: bool
    scheduler_weekly_nav_history_consistency_audit_enabled: bool
    scheduler_weekly_task_history_governance_preview_enabled: bool
    scheduler_weekly_task_history_governance_preview_retention_days: int
    scheduler_weekly_task_history_governance_preview_recent_keep_per_task_status: int
    scheduler_weekly_task_history_governance_preview_large_extra_data_threshold_bytes: int
    scheduler_weekly_task_history_governance_preview_max_sample_rows: int
    scheduler_weekly_task_history_governance_preview_max_daily_summary_days: int
    scheduler_weekly_task_history_cleanup_execution_enabled: bool
    scheduler_weekly_task_history_cleanup_execution_auto_apply_enabled: bool
    scheduler_weekly_task_history_cleanup_execution_min_apply_candidate_total: int
    scheduler_weekly_task_history_cleanup_execution_retention_days: int
    scheduler_weekly_task_history_cleanup_execution_recent_keep_per_task_status: int
    scheduler_weekly_task_history_cleanup_execution_max_sample_rows: int
    scheduler_weekly_task_history_cleanup_execution_compress_backup: bool
    scheduler_weekly_screening_storage_growth_audit_enabled: bool
    scheduler_weekly_screening_storage_growth_audit_days: int
    scheduler_weekly_screening_storage_growth_audit_fund_types: tuple[str, ...]
    scheduler_weekly_screening_storage_growth_audit_top_runs: int
    scheduler_weekly_screening_storage_growth_audit_max_runs: int
    scheduler_weekly_screening_storage_growth_audit_include_table_totals: bool
    scheduler_weekly_screening_history_cleanup_preview_enabled: bool
    scheduler_weekly_screening_history_cleanup_preview_recent_days: int
    scheduler_weekly_screening_history_cleanup_preview_fund_types: tuple[str, ...]
    scheduler_weekly_screening_history_cleanup_runbook_enabled: bool
    scheduler_weekly_screening_history_cleanup_runbook_delete_batch_run_limit: int
    scheduler_weekly_screening_history_cleanup_preflight_enabled: bool
    scheduler_weekly_screening_history_cleanup_execution_enabled: bool
    scheduler_weekly_screening_history_cleanup_execution_mode: str
    scheduler_weekly_screening_history_cleanup_execution_start_batch: int
    scheduler_weekly_screening_history_cleanup_execution_end_batch: int | None
    scheduler_weekly_screening_history_cleanup_execution_auto_create_backup_if_missing: bool
    scheduler_weekly_screening_history_cleanup_execution_backup_prefix: str
    scheduler_weekly_screening_history_cleanup_execution_backup_compress: bool
    scheduler_weekly_screening_history_archive_preview_enabled: bool
    scheduler_weekly_screening_history_archive_execution_enabled: bool
    scheduler_weekly_screening_history_archive_execution_mode: str
    scheduler_weekly_screening_history_archive_execution_archive_target_path: str
    scheduler_weekly_screening_history_archive_execution_chunk_size: int
    scheduler_weekly_screening_history_archive_rotation_enabled: bool
    scheduler_weekly_screening_history_archive_rotation_mode: str
    scheduler_weekly_screening_history_archive_rotation_max_active_size_gb: float
    scheduler_weekly_screening_history_archive_rotation_min_active_run_count: int
    scheduler_weekly_screening_history_archive_rotation_compute_sha256: bool
    scheduler_weekly_screening_history_archive_rotation_columnar_compression: bool
    scheduler_weekly_nav_history_archive_preview_enabled: bool
    scheduler_weekly_nav_history_archive_preview_mode: str
    scheduler_weekly_nav_history_archive_preview_page_limit: int
    scheduler_weekly_nav_history_archive_preview_max_pages_per_run: int
    scheduler_weekly_nav_history_archive_preview_auto_bind_latest_backup: bool
    scheduler_weekly_nav_history_archive_preview_auto_create_backup_if_missing: bool
    scheduler_weekly_nav_history_archive_preview_backup_prefix: str
    scheduler_weekly_nav_history_archive_preview_backup_compress: bool
    scheduler_weekly_nav_history_archive_vacuum_enabled: bool
    scheduler_weekly_nav_history_archive_vacuum_mode: str
    scheduler_weekly_nav_history_archive_vacuum_confirm_replace: bool
    scheduler_weekly_nav_history_archive_vacuum_min_free_space_multiplier: float
    scheduler_weekly_rank_history_governance_audit_enabled: bool
    scheduler_weekly_rank_history_archive_enabled: bool
    scheduler_weekly_rank_history_archive_mode: str
    scheduler_weekly_rank_history_archive_page_limit: int
    scheduler_weekly_rank_history_archive_max_pages_per_run: int
    scheduler_weekly_rank_history_archive_auto_create_backup_if_missing: bool
    scheduler_weekly_rank_history_archive_backup_prefix: str
    scheduler_weekly_rank_history_archive_backup_compress: bool
    scheduler_weekly_rank_history_archive_vacuum_enabled: bool
    scheduler_weekly_rank_history_archive_vacuum_mode: str
    scheduler_weekly_rank_history_archive_vacuum_confirm_replace: bool
    scheduler_weekly_rank_history_archive_vacuum_min_free_space_multiplier: float
    scheduler_weekly_incremental_vacuum_enabled: bool
    scheduler_weekly_incremental_vacuum_max_pages: int
    scheduler_weekly_incremental_vacuum_min_freelist_pages: int
    scheduler_weekly_incremental_vacuum_max_seconds: float
    scheduler_weekly_regression_smoke_enabled: bool
    scheduler_weekly_regression_smoke_groups: tuple[str, ...]
    scheduler_weekly_database_backup_cleanup_preview_enabled: bool
    scheduler_weekly_database_backup_cleanup_preview_scheduled_keep_count: int
    scheduler_weekly_database_backup_cleanup_preview_unmanaged_retention_days: int
    scheduler_weekly_database_backup_cleanup_preview_keep_latest_manual_snapshot_per_family: int
    scheduler_weekly_database_backup_cleanup_preview_max_sample_files: int
    scheduler_weekly_database_backup_cleanup_execution_enabled: bool
    scheduler_weekly_database_backup_cleanup_execution_mode: str
    scheduler_weekly_database_backup_cleanup_execution_checkpoint_every: int
    scheduler_weekly_maintenance_backup_retention_enabled: bool
    scheduler_weekly_maintenance_backup_retention_keep_per_purpose: int
    scheduler_weekly_maintenance_backup_retention_min_age_days: int
    scheduler_weekly_maintenance_backup_retention_max_delete_files: int
    scheduler_weekly_database_backup_compression_preview_enabled: bool
    scheduler_weekly_database_backup_compression_preview_min_compress_size_mb: float
    scheduler_weekly_database_backup_compression_preview_max_sample_files: int
    scheduler_weekly_database_backup_compression_execution_enabled: bool
    scheduler_weekly_database_backup_compression_execution_mode: str
    scheduler_weekly_database_backup_compression_execution_checkpoint_every: int
    scheduler_weekly_database_governance_health_audit_enabled: bool
    scheduler_weekly_database_optimization_completion_audit_enabled: bool
    scheduler_weekly_disk_cleanup_archive_keep_count: int
    scheduler_weekly_disk_cleanup_archive_keep_days: int
    scheduler_weekly_disk_cleanup_logs_keep_days: int
    scheduler_weekly_disk_cleanup_maintenance_candidate_keep_count: int
    scheduler_weekly_disk_cleanup_maintenance_candidate_min_age_hours: int
    scheduler_weekly_disk_cleanup_core_csv_enabled: bool
    scheduler_weekly_disk_cleanup_core_csv_keep_days: int
    scheduler_long_term_history_empty_payload_retry_days: int
    scheduler_long_term_history_empty_payload_upstream_limited_failures: int
    scheduler_long_term_history_rank_stale_days: int
    scheduler_long_term_history_stale_refresh_retry_days: int
    scheduler_long_term_history_stale_refresh_upstream_limited_failures: int
    scheduler_long_term_history_high_priority_stale_backlog_trigger: int
    scheduler_long_term_history_high_priority_stale_limit: int
    effective_nav_coverage_ratio: float
    effective_nav_weekday_only: bool
    scheduler_snapshot_watchdog_enabled: bool
    scheduler_snapshot_watchdog_interval_minutes: int
    storage_observation_enabled: bool
    storage_observation_interval_hours: int
    storage_observation_timeout_seconds: int
    storage_observation_retain_observations: int
    valuation_max_age_seconds: int = 1800
    valuation_max_base_age_days: int = 10
    publication_max_snapshot_age_days: int = 10
    autostock_enabled: bool = True
    publication_validation_provider: str = "autostock"
    eastmoney_fund_info_url: str = "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo"
    fund_status_url: str = "https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx"
    report_history_database_path: Path | None = None
    storage_lifecycle_enabled: bool = False
    storage_lifecycle_archive_root: Path = Path('/mnt/Project-Archives/Fund-ANA')

    def ensure_directories(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.core_output_dir.mkdir(parents=True, exist_ok=True)
        self.publication_registry_file.parent.mkdir(parents=True, exist_ok=True)
        self.favorites_history_file.parent.mkdir(parents=True, exist_ok=True)
        self.archived_runs_dir.mkdir(parents=True, exist_ok=True)
        self.scheduler_state_file.parent.mkdir(parents=True, exist_ok=True)
        self.scheduler_heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
        self.scheduler_pid_file.parent.mkdir(parents=True, exist_ok=True)
        self.scheduler_lock_file.parent.mkdir(parents=True, exist_ok=True)


def build_database_source_audit(settings: Settings) -> dict[str, Any]:
    canonical_path = settings.database_path.resolve()
    compatibility_shell_candidates = [
        settings.project_root / "backend" / "data" / "fund_ana.db",
        settings.project_root / "backend" / "data" / "fund_analysis.db",
        settings.project_root / "backend" / "fund_analysis.db",
    ]

    compatibility_shells: list[dict[str, Any]] = []
    warnings: list[str] = []

    for path in compatibility_shell_candidates:
        exists = path.exists()
        size_bytes = path.stat().st_size if exists else None
        is_zero_byte_shell = bool(exists and size_bytes == 0)
        compatibility_shells.append(
            {
                "path": str(path.resolve()),
                "exists": exists,
                "size_bytes": size_bytes,
                "is_zero_byte_shell": is_zero_byte_shell,
            }
        )
        if is_zero_byte_shell:
            warnings.append(f"compatibility shell file present: {path.resolve()}")

    canonical_exists = canonical_path.exists()
    canonical_size_bytes = canonical_path.stat().st_size if canonical_exists else None
    canonical_is_zero_byte = bool(canonical_exists and canonical_size_bytes == 0)
    if not canonical_exists:
        warnings.append(f"canonical database missing: {canonical_path}")
    elif canonical_is_zero_byte:
        warnings.append(f"canonical database is zero-byte: {canonical_path}")

    return {
        "canonical_database_path": str(canonical_path),
        "canonical_exists": canonical_exists,
        "canonical_size_bytes": canonical_size_bytes,
        "canonical_is_zero_byte": canonical_is_zero_byte,
        "compatibility_shells": compatibility_shells,
        "warnings": warnings,
        "ok": canonical_exists and not canonical_is_zero_byte,
    }



def _resolve_path(base: Path, raw_value: str) -> Path:
    path = Path(raw_value)
    return path if path.is_absolute() else (base / path).resolve()


def _positive_int_mapping(raw_value: Any) -> dict[str, int]:
    if not isinstance(raw_value, dict):
        return {}
    result: dict[str, int] = {}
    for key, value in raw_value.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        try:
            normalized_value = int(value)
        except (TypeError, ValueError):
            continue
        if normalized_value > 0:
            result[normalized_key] = normalized_value
    return result



def load_settings(config_path: str | Path | None = None) -> Settings:
    config_file = Path(config_path or DEFAULT_CONFIG_PATH)
    if not config_file.is_absolute():
        config_file = (PROJECT_ROOT / config_file).resolve()

    payload: dict[str, Any] = {}
    if config_file.exists():
        payload = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}

    project_root = PROJECT_ROOT
    paths = payload.get("paths", {}) or {}
    apis = payload.get("apis", {}) or {}
    workflow = payload.get("workflow", {}) or {}
    web = payload.get("web", {}) or {}
    project = payload.get("project", {}) or {}
    scheduler = payload.get("scheduler", {}) or {}
    autostock_cfg = (apis.get("autostock") or {}) if isinstance(apis.get("autostock"), dict) else {}
    autostock_rate_limit_cfg = (
        (autostock_cfg.get("rate_limit") or {}) if isinstance(autostock_cfg.get("rate_limit"), dict) else {}
    )
    autostock_batch_cfg = (
        (autostock_cfg.get("batch") or {}) if isinstance(autostock_cfg.get("batch"), dict) else {}
    )
    autostock_directory_cache_cfg = autostock_cfg.get("directory_cache") or {}
    autostock_report_validation_cache_cfg = autostock_cfg.get("report_validation_cache") or {}
    daily_refresh = scheduler.get("daily_refresh", {}) or {}
    intraday_refresh = scheduler.get("intraday_refresh", {}) or {}
    weekly_maintenance = scheduler.get("weekly_maintenance", {}) or {}
    weekly_plain_open_tail_follow_up = weekly_maintenance.get("plain_open_tail_follow_up", {}) or {}
    weekly_future_outlier_monitor = weekly_maintenance.get("future_outlier_monitor", {}) or {}
    weekly_holding_period_edge_audit = weekly_maintenance.get("holding_period_edge_audit", {}) or {}
    weekly_nav_history_consistency_audit = weekly_maintenance.get("nav_history_consistency_audit", {}) or {}
    weekly_task_history_governance_preview = weekly_maintenance.get("task_history_governance_preview", {}) or {}
    weekly_task_history_cleanup_execution = weekly_maintenance.get("task_history_cleanup_execution", {}) or {}
    weekly_screening_storage_growth_audit = weekly_maintenance.get("screening_storage_growth_audit", {}) or {}
    weekly_screening_history_cleanup_preview = weekly_maintenance.get("screening_history_cleanup_preview", {}) or {}
    weekly_screening_history_cleanup_runbook = weekly_maintenance.get("screening_history_cleanup_runbook", {}) or {}
    weekly_screening_history_cleanup_preflight = weekly_maintenance.get("screening_history_cleanup_preflight", {}) or {}
    weekly_screening_history_cleanup_execution = weekly_maintenance.get("screening_history_cleanup_execution", {}) or {}
    weekly_screening_history_archive_preview = weekly_maintenance.get("screening_history_archive_preview", {}) or {}
    weekly_screening_history_archive_execution = weekly_maintenance.get("screening_history_archive_execution", {}) or {}
    weekly_screening_history_archive_rotation = weekly_maintenance.get("screening_history_archive_rotation", {}) or {}
    weekly_nav_history_archive_preview = weekly_maintenance.get("nav_history_archive_preview", {}) or {}
    weekly_nav_history_archive_vacuum = weekly_maintenance.get("nav_history_archive_vacuum", {}) or {}
    weekly_rank_history_governance_audit = weekly_maintenance.get("rank_history_governance_audit", {}) or {}
    weekly_rank_history_archive = weekly_maintenance.get("rank_history_archive", {}) or {}
    weekly_rank_history_archive_vacuum = weekly_maintenance.get("rank_history_archive_vacuum", {}) or {}
    weekly_regression_smoke = weekly_maintenance.get("regression_smoke", {}) or {}
    weekly_database_backup_cleanup_preview = (
        weekly_maintenance.get("database_backup_cleanup_preview", {}) or {}
    )
    weekly_database_backup_cleanup_execution = (
        weekly_maintenance.get("database_backup_cleanup_execution", {}) or {}
    )
    weekly_maintenance_backup_retention = weekly_maintenance.get("maintenance_backup_retention", {}) or {}
    weekly_database_backup_compression_preview = (
        weekly_maintenance.get("database_backup_compression_preview", {}) or {}
    )
    weekly_database_backup_compression_execution = (
        weekly_maintenance.get("database_backup_compression_execution", {}) or {}
    )
    weekly_database_governance_health_audit = (
        weekly_maintenance.get("database_governance_health_audit", {}) or {}
    )
    weekly_database_optimization_completion_audit = (
        weekly_maintenance.get("database_optimization_completion_audit", {}) or {}
    )
    weekly_disk_cleanup = weekly_maintenance.get("disk_cleanup", {}) or {}
    daily_nav_history_sync_limit = int(daily_refresh.get("nav_history_sync_limit", 500))
    daily_long_term_history_limit = int(daily_refresh.get("long_term_history_limit", 0))
    intraday_nav_history_sync_limit = int(intraday_refresh.get("nav_history_sync_limit", 0))
    intraday_long_term_history_limit = int(intraday_refresh.get("long_term_history_limit", 0))
    weekly_nav_history_limit = int(weekly_maintenance.get("nav_history_limit", 0))
    weekly_long_term_history_limit = int(weekly_maintenance.get("long_term_history_limit", 0))
    workflow_task_timeout_seconds = _positive_int_mapping(workflow.get("task_timeout_seconds", {}))
    workflow_task_idle_timeout_seconds = _positive_int_mapping(
        workflow.get("task_idle_timeout_seconds", {})
    )

    settings = Settings(
        project_root=project_root,
        config_path=config_file,
        core_root=_resolve_path(project_root, paths.get("core_root", "core")),
        database_path=_resolve_path(project_root, paths.get("database_path", "backend/data/bond_funds.db")),
        state_file=_resolve_path(project_root, paths.get("state_file", "backend/data/workflow_state.json")),
        reports_dir=_resolve_path(project_root, paths.get("reports_dir", "reports")),
        runs_dir=_resolve_path(project_root, paths.get("runs_dir", "reports/runs")),
        core_output_dir=_resolve_path(project_root, paths.get("core_output_dir", "reports/core_output")),
        # Keep the configured alias, not its current immutable revision target.
        # Resolving this symlink pins readers and lets alias recovery replace the
        # committed report itself after a process restart.
        latest_report_file=Path(os.path.abspath(project_root / paths.get("latest_report_file", "reports/latest.json"))),
        publication_registry_file=_resolve_path(project_root, paths.get("publication_registry_file", "reports/publication_registry.json")),
        favorites_history_file=_resolve_path(project_root, paths.get("favorites_history_file", "backend/data/favorite_fund_queries.json")),
        archived_runs_dir=_resolve_path(project_root, paths.get("archived_runs_dir", "reports/archive/superseded_runs")),
        tiantian_base_url=str(apis.get("tiantian_base_url", "http://tiantian-api:3000")).rstrip("/"),
        valuation_max_age_seconds=int((apis.get("valuation_validation") or {}).get("max_age_seconds", 1800)),
        valuation_max_base_age_days=int((apis.get("valuation_validation") or {}).get("max_base_age_days", 10)),
        publication_max_snapshot_age_days=int(workflow.get("publication_max_snapshot_age_days", 10)),
        tiantian_valuation_primary_url=str(
            apis.get(
                "tiantian_valuation_primary_url",
                "https://fundcomapi.tiantianfunds.com/mm/newCore/FundValuationLast",
            )
        ).strip(),
        tiantian_valuation_fallback_url=str(
            apis.get(
                "tiantian_valuation_fallback_url",
                "https://fundcomapi.eastmoney.com/mm/newCore/FundValuationLast",
            )
        ).strip(),
        autostock_base_url=str(apis.get("autostock_base_url", "https://api.autostock.cn/v1")).rstrip("/"),
        autostock_enabled=autostock_cfg.get("enabled", True) is True,
        publication_validation_provider=str((apis.get("publication_validation") or {}).get("provider", "autostock")).strip(),
        eastmoney_fund_info_url=str(apis.get("eastmoney_fund_info_url", "https://fundmobapi.eastmoney.com/FundMNewApi/FundMNFInfo")).strip(),
        fund_status_url=str(apis.get("fund_status_url", "https://fund.eastmoney.com/Data/Fund_JJJZ_Data.aspx")).strip(),
        autostock_rate_limit_max_calls_per_hour=int(
            autostock_rate_limit_cfg.get("max_calls_per_hour", 100) or 100
        ),
        autostock_rate_limit_min_interval_seconds=float(
            autostock_rate_limit_cfg.get("min_interval_seconds", 2.0) or 2.0
        ),
        autostock_rate_limit_shared_state_path=_resolve_path(
            project_root,
            autostock_rate_limit_cfg.get(
                "shared_state_path",
                "backend/data/autostock_rate_limit_state.json",
            ),
        ),
        autostock_batch_fund_default=int(
            autostock_batch_cfg.get("fund_default", 50) or 50
        ),
        autostock_directory_cache_path=_resolve_path(
            project_root, autostock_directory_cache_cfg.get("path", "backend/data/autostock_fund_directory_cache.json"),
        ),
        autostock_directory_cache_ttl_seconds=int(autostock_directory_cache_cfg.get("ttl_seconds", 3600)),
        autostock_report_validation_cache_ttl_seconds=max(
            0, int(autostock_report_validation_cache_cfg.get("ttl_seconds", 14400)),
        ),
        autostock_batch_fund_detail_default=int(
            autostock_batch_cfg.get("fund_detail_default", 50) or 50
        ),
        run_collection_by_default=bool(workflow.get("run_collection_by_default", True)),
        run_screening_by_default=bool(workflow.get("run_screening_by_default", True)),
        collect_details=bool(workflow.get("collect_details", True)),
        collect_managers=bool(workflow.get("collect_managers", True)),
        collect_companies=bool(workflow.get("collect_companies", True)),
        collect_risk=bool(workflow.get("collect_risk", True)),
        collect_nav_incremental=bool(workflow.get("collect_nav_incremental", True)),
        nav_workers=int(workflow.get("nav_workers", 5)),
        nav_batch_size=int(workflow.get("nav_batch_size", 500)),
        nav_batch_interval=int(workflow.get("nav_batch_interval", 10)),
        detail_workers=int(workflow.get("detail_workers", 8)),
        manager_workers=int(workflow.get("manager_workers", 5)),
        company_workers=max(1, min(3, int(((payload.get("collection") or {}).get("company") or {}).get("workers", 2)))),
        risk_workers=int(workflow.get("risk_workers", 5)),
        holding_workers=int(workflow.get("holding_workers", 10)),
        workflow_task_timeout_seconds=workflow_task_timeout_seconds,
        workflow_task_idle_timeout_seconds=workflow_task_idle_timeout_seconds,
        exclude_holding_days_gt=int(workflow.get("exclude_holding_days_gt", 90)),
        preview_top_n=int(workflow.get("preview_top_n", 15)),
        final_selection_target_max=int(workflow.get("final_selection_target_max", 320)),
        final_selection_group_min_ratio=float(workflow.get("final_selection_group_min_ratio", 0.25)),
        final_selection_high_score_new_fund_threshold=float(
            workflow.get("final_selection_high_score_new_fund_threshold", 85.0)
        ),
        final_selection_low_score_new_fund_threshold=float(
            workflow.get("final_selection_low_score_new_fund_threshold", 75.0)
        ),
        web_host=str(web.get("host", "0.0.0.0")),
        web_port=int(web.get("port", 8000)),
        timezone=str(project.get("timezone", "Asia/Shanghai")),
        scheduler_enabled=bool(scheduler.get("enabled", False)),
        scheduler_host_mode=str(scheduler.get("host_mode", "embedded_web")).strip().lower() or "embedded_web",
        scheduler_timezone=str(scheduler.get("timezone", project.get("timezone", "Asia/Shanghai"))),
        scheduler_state_file=_resolve_path(project_root, scheduler.get("state_file", "backend/data/scheduler_state.json")),
        scheduler_heartbeat_file=_resolve_path(project_root, scheduler.get("heartbeat_file", "backend/data/scheduler_heartbeat.json")),
        scheduler_pid_file=_resolve_path(project_root, scheduler.get("pid_file", "backend/data/scheduler.pid")),
        scheduler_lock_file=_resolve_path(
            project_root,
            scheduler.get("lock_file", "backend/data/scheduler.lease"),
        ),
        scheduler_service_unit_name=str(
            scheduler.get("scheduler_service_unit_name", "fund-ana-scheduler.service")
        ).strip() or "fund-ana-scheduler.service",
        web_service_unit_name=str(
            scheduler.get("web_service_unit_name", "fund-ana-web.service")
        ).strip() or "fund-ana-web.service",
        scheduler_heartbeat_interval_seconds=int(scheduler.get("heartbeat_interval_seconds", 60)),
        scheduler_heartbeat_stale_after_seconds=max(
            int(scheduler.get("heartbeat_interval_seconds", 60)) * 3,
            int(scheduler.get("heartbeat_stale_after_seconds", 180) or 180),
        ),
        scheduler_blocked_snapshot_retry_cooldown_minutes=max(
            0,
            int(scheduler.get("blocked_snapshot_retry_cooldown_minutes", 120) or 0),
        ),
        scheduler_failed_screening_retry_cooldown_minutes=max(
            0,
            int(scheduler.get("failed_screening_retry_cooldown_minutes", 120) or 0),
        ),
        scheduler_workflow_busy_retry_cooldown_seconds=max(
            0,
            int(scheduler.get("workflow_busy_retry_cooldown_seconds", 300) or 0),
        ),
        scheduler_daily_hour=int(daily_refresh.get("hour", 6)),
        scheduler_daily_minute=int(daily_refresh.get("minute", 10)),
        scheduler_daily_nav_history_sync_enabled=bool(daily_refresh.get("nav_history_sync_enabled", True)),
        scheduler_daily_nav_history_sync_limit=daily_nav_history_sync_limit,
        scheduler_daily_long_term_history_enabled=bool(
            daily_refresh.get("long_term_history_enabled", daily_long_term_history_limit > 0)
        ),
        scheduler_daily_long_term_history_limit=daily_long_term_history_limit,
        scheduler_daily_long_term_history_refresh_stale=bool(
            daily_refresh.get("long_term_history_refresh_stale", True)
        ),
        scheduler_intraday_refresh_enabled=bool(intraday_refresh.get("enabled", True)),
        scheduler_intraday_hour=int(intraday_refresh.get("hour", 13)),
        scheduler_intraday_minute=int(intraday_refresh.get("minute", 45)),
        scheduler_intraday_nav_history_sync_enabled=bool(intraday_refresh.get("nav_history_sync_enabled", True)),
        scheduler_intraday_nav_history_sync_limit=intraday_nav_history_sync_limit,
        scheduler_intraday_nav_workers=int(
            intraday_refresh.get("nav_workers", workflow.get("nav_workers", 5)) or workflow.get("nav_workers", 5)
        ),
        scheduler_intraday_long_term_history_enabled=bool(
            intraday_refresh.get("long_term_history_enabled", intraday_long_term_history_limit > 0)
        ),
        scheduler_intraday_long_term_history_limit=intraday_long_term_history_limit,
        scheduler_intraday_long_term_history_refresh_stale=bool(
            intraday_refresh.get("long_term_history_refresh_stale", False)
        ),
        scheduler_weekly_day_of_week=str(weekly_maintenance.get("day_of_week", "sun")).strip().lower() or "sun",
        scheduler_weekly_hour=int(weekly_maintenance.get("hour", 3)),
        scheduler_weekly_minute=int(weekly_maintenance.get("minute", 0)),
        scheduler_weekly_nav_history_enabled=bool(weekly_maintenance.get("nav_history_enabled", True)),
        scheduler_weekly_nav_history_limit=weekly_nav_history_limit,
        scheduler_weekly_long_term_history_enabled=bool(
            weekly_maintenance.get("long_term_history_enabled", weekly_long_term_history_limit > 0)
        ),
        scheduler_weekly_long_term_history_limit=weekly_long_term_history_limit,
        scheduler_weekly_long_term_history_refresh_stale=bool(
            weekly_maintenance.get("long_term_history_refresh_stale", True)
        ),
        scheduler_weekly_plain_open_tail_follow_up_enabled=bool(
            weekly_plain_open_tail_follow_up.get("enabled", False)
        ),
        scheduler_weekly_plain_open_tail_follow_up_category=str(
            weekly_plain_open_tail_follow_up.get("category", "plain_open_tail_follow_up")
        ).strip()
        or "plain_open_tail_follow_up",
        scheduler_weekly_plain_open_tail_follow_up_skip_risk_refresh=bool(
            weekly_plain_open_tail_follow_up.get("skip_risk_refresh", False)
        ),
        scheduler_weekly_future_outlier_monitor_enabled=bool(
            weekly_future_outlier_monitor.get("enabled", False)
        ),
        scheduler_weekly_future_outlier_monitor_category=str(
            weekly_future_outlier_monitor.get("category", "future_outlier_monitor")
        ).strip()
        or "future_outlier_monitor",
        scheduler_weekly_holding_period_edge_audit_enabled=bool(
            weekly_holding_period_edge_audit.get("enabled", True)
        ),
        scheduler_weekly_nav_history_consistency_audit_enabled=bool(
            weekly_nav_history_consistency_audit.get("enabled", True)
        ),
        scheduler_weekly_task_history_governance_preview_enabled=bool(
            weekly_task_history_governance_preview.get("enabled", True)
        ),
        scheduler_weekly_task_history_governance_preview_retention_days=max(
            1,
            int(weekly_task_history_governance_preview.get("retention_days", 14)),
        ),
        scheduler_weekly_task_history_governance_preview_recent_keep_per_task_status=max(
            1,
            int(weekly_task_history_governance_preview.get("recent_keep_per_task_status", 20)),
        ),
        scheduler_weekly_task_history_governance_preview_large_extra_data_threshold_bytes=max(
            1,
            int(
                weekly_task_history_governance_preview.get(
                    "large_extra_data_threshold_bytes",
                    262_144,
                )
            ),
        ),
        scheduler_weekly_task_history_governance_preview_max_sample_rows=max(
            1,
            int(weekly_task_history_governance_preview.get("max_sample_rows", 20)),
        ),
        scheduler_weekly_task_history_governance_preview_max_daily_summary_days=max(
            1,
            int(weekly_task_history_governance_preview.get("max_daily_summary_days", 30)),
        ),
        scheduler_weekly_task_history_cleanup_execution_enabled=bool(
            weekly_task_history_cleanup_execution.get("enabled", True)
        ),
        scheduler_weekly_task_history_cleanup_execution_auto_apply_enabled=bool(
            weekly_task_history_cleanup_execution.get("auto_apply_enabled", True)
        ),
        scheduler_weekly_task_history_cleanup_execution_min_apply_candidate_total=max(
            1,
            int(weekly_task_history_cleanup_execution.get("min_apply_candidate_total", 100)),
        ),
        scheduler_weekly_task_history_cleanup_execution_retention_days=max(
            1,
            int(weekly_task_history_cleanup_execution.get("retention_days", 14)),
        ),
        scheduler_weekly_task_history_cleanup_execution_recent_keep_per_task_status=max(
            1,
            int(weekly_task_history_cleanup_execution.get("recent_keep_per_task_status", 20)),
        ),
        scheduler_weekly_task_history_cleanup_execution_max_sample_rows=max(
            1,
            int(weekly_task_history_cleanup_execution.get("max_sample_rows", 20)),
        ),
        scheduler_weekly_task_history_cleanup_execution_compress_backup=bool(
            weekly_task_history_cleanup_execution.get("compress_backup", True)
        ),
        scheduler_weekly_screening_storage_growth_audit_enabled=bool(
            weekly_screening_storage_growth_audit.get("enabled", True)
        ),
        scheduler_weekly_screening_storage_growth_audit_days=max(
            1,
            int(weekly_screening_storage_growth_audit.get("days", 30)),
        ),
        scheduler_weekly_screening_storage_growth_audit_fund_types=tuple(
            str(item).strip()
            for item in (weekly_screening_storage_growth_audit.get("fund_types") or ["bond"])
            if str(item).strip()
        ),
        scheduler_weekly_screening_storage_growth_audit_top_runs=max(
            1,
            int(weekly_screening_storage_growth_audit.get("top_runs", 20)),
        ),
        scheduler_weekly_screening_storage_growth_audit_max_runs=max(
            0,
            int(weekly_screening_storage_growth_audit.get("max_runs", 0) or 0),
        ),
        scheduler_weekly_screening_storage_growth_audit_include_table_totals=bool(
            weekly_screening_storage_growth_audit.get("include_table_totals", True)
        ),
        scheduler_weekly_screening_history_cleanup_preview_enabled=bool(
            weekly_screening_history_cleanup_preview.get("enabled", True)
        ),
        scheduler_weekly_screening_history_cleanup_preview_recent_days=max(
            1,
            int(weekly_screening_history_cleanup_preview.get("recent_days", 30)),
        ),
        scheduler_weekly_screening_history_cleanup_preview_fund_types=tuple(
            str(item).strip()
            for item in (weekly_screening_history_cleanup_preview.get("fund_types") or ["bond"])
            if str(item).strip()
        ),
        scheduler_weekly_screening_history_cleanup_runbook_enabled=bool(
            weekly_screening_history_cleanup_runbook.get("enabled", True)
        ),
        scheduler_weekly_screening_history_cleanup_runbook_delete_batch_run_limit=max(
            1,
            int(weekly_screening_history_cleanup_runbook.get("delete_batch_run_limit", 100)),
        ),
        scheduler_weekly_screening_history_cleanup_preflight_enabled=bool(
            weekly_screening_history_cleanup_preflight.get("enabled", True)
        ),
        scheduler_weekly_screening_history_cleanup_execution_enabled=bool(
            weekly_screening_history_cleanup_execution.get("enabled", True)
        ),
        scheduler_weekly_screening_history_cleanup_execution_mode=(
            str(weekly_screening_history_cleanup_execution.get("mode", "dry-run"))
            .strip()
            .lower()
            .replace("_", "-")
            or "dry-run"
        ),
        scheduler_weekly_screening_history_cleanup_execution_start_batch=max(
            1,
            int(weekly_screening_history_cleanup_execution.get("start_batch", 1) or 1),
        ),
        scheduler_weekly_screening_history_cleanup_execution_end_batch=(
            max(
                1,
                int(weekly_screening_history_cleanup_execution.get("end_batch")),
            )
            if weekly_screening_history_cleanup_execution.get("end_batch") is not None
            and str(weekly_screening_history_cleanup_execution.get("end_batch")).strip() != ""
            else None
        ),
        scheduler_weekly_screening_history_cleanup_execution_auto_create_backup_if_missing=bool(
            weekly_screening_history_cleanup_execution.get("auto_create_backup_if_missing", True)
        ),
        scheduler_weekly_screening_history_cleanup_execution_backup_prefix=(
            str(
                weekly_screening_history_cleanup_execution.get(
                    "backup_prefix",
                    "bond_funds_pre_screening_cleanup",
                )
            ).strip()
            or "bond_funds_pre_screening_cleanup"
        ),
        scheduler_weekly_screening_history_cleanup_execution_backup_compress=bool(
            weekly_screening_history_cleanup_execution.get("backup_compress", True)
        ),
        scheduler_weekly_screening_history_archive_preview_enabled=bool(
            weekly_screening_history_archive_preview.get("enabled", True)
        ),
        scheduler_weekly_screening_history_archive_execution_enabled=bool(
            weekly_screening_history_archive_execution.get("enabled", True)
        ),
        scheduler_weekly_screening_history_archive_execution_mode=(
            str(weekly_screening_history_archive_execution.get("mode", "archive-export"))
            .strip()
            .lower()
            .replace("_", "-")
            or "archive-export"
        ),
        scheduler_weekly_screening_history_archive_execution_archive_target_path=(
            str(
                weekly_screening_history_archive_execution.get(
                    "archive_target_path",
                    weekly_screening_history_archive_preview.get(
                        "archive_target_path",
                        "backend/data/archives/screening_history_details_archive.db",
                    ),
                )
            ).strip()
            or "backend/data/archives/screening_history_details_archive.db"
        ),
        scheduler_weekly_screening_history_archive_execution_chunk_size=max(
            1,
            int(weekly_screening_history_archive_execution.get("chunk_size", 500)),
        ),
        scheduler_weekly_screening_history_archive_rotation_enabled=bool(
            weekly_screening_history_archive_rotation.get("enabled", True)
        ),
        scheduler_weekly_screening_history_archive_rotation_mode=(
            str(weekly_screening_history_archive_rotation.get("mode", "auto"))
            .strip()
            .lower()
            .replace("_", "-")
            or "auto"
        ),
        scheduler_weekly_screening_history_archive_rotation_max_active_size_gb=max(
            0.001,
            float(
                weekly_screening_history_archive_rotation.get(
                    "max_active_size_gb",
                    8,
                )
                or 8
            ),
        ),
        scheduler_weekly_screening_history_archive_rotation_min_active_run_count=max(
            1,
            int(
                weekly_screening_history_archive_rotation.get(
                    "min_active_run_count",
                    1,
                )
                or 1
            ),
        ),
        scheduler_weekly_screening_history_archive_rotation_compute_sha256=bool(
            weekly_screening_history_archive_rotation.get("compute_sha256", True)
        ),
        scheduler_weekly_screening_history_archive_rotation_columnar_compression=bool(
            weekly_screening_history_archive_rotation.get("columnar_compression", False)
        ),
        scheduler_weekly_nav_history_archive_preview_enabled=bool(
            weekly_nav_history_archive_preview.get("enabled", True)
        ),
        scheduler_weekly_nav_history_archive_preview_mode=(
            str(weekly_nav_history_archive_preview.get("mode", "dry_run")).strip()
            or "dry_run"
        ),
        scheduler_weekly_nav_history_archive_preview_page_limit=max(
            1,
            int(weekly_nav_history_archive_preview.get("page_limit", 100_000)),
        ),
        scheduler_weekly_nav_history_archive_preview_max_pages_per_run=max(
            1,
            int(weekly_nav_history_archive_preview.get("max_pages_per_run", 1)),
        ),
        scheduler_weekly_nav_history_archive_preview_auto_bind_latest_backup=bool(
            weekly_nav_history_archive_preview.get("auto_bind_latest_backup", True)
        ),
        scheduler_weekly_nav_history_archive_preview_auto_create_backup_if_missing=bool(
            weekly_nav_history_archive_preview.get("auto_create_backup_if_missing", False)
        ),
        scheduler_weekly_nav_history_archive_preview_backup_prefix=(
            str(
                weekly_nav_history_archive_preview.get(
                    "backup_prefix",
                    "bond_funds_pre_nav_archive",
                )
            ).strip()
            or "bond_funds_pre_nav_archive"
        ),
        scheduler_weekly_nav_history_archive_preview_backup_compress=bool(
            weekly_nav_history_archive_preview.get("backup_compress", True)
        ),
        scheduler_weekly_nav_history_archive_vacuum_enabled=bool(
            weekly_nav_history_archive_vacuum.get("enabled", True)
        ),
        scheduler_weekly_nav_history_archive_vacuum_mode=(
            str(weekly_nav_history_archive_vacuum.get("mode", "auto")).strip() or "auto"
        ),
        scheduler_weekly_nav_history_archive_vacuum_confirm_replace=bool(
            weekly_nav_history_archive_vacuum.get("confirm_replace", True)
        ),
        scheduler_weekly_nav_history_archive_vacuum_min_free_space_multiplier=float(
            weekly_nav_history_archive_vacuum.get("min_free_space_multiplier", 1.25)
        ),
        scheduler_weekly_rank_history_governance_audit_enabled=bool(
            weekly_rank_history_governance_audit.get("enabled", True)
        ),
        scheduler_weekly_rank_history_archive_enabled=bool(
            weekly_rank_history_archive.get("enabled", True)
        ),
        scheduler_weekly_rank_history_archive_mode=(
            str(weekly_rank_history_archive.get("mode", "auto")).strip() or "auto"
        ),
        scheduler_weekly_rank_history_archive_page_limit=max(
            1,
            int(weekly_rank_history_archive.get("page_limit", 500_000)),
        ),
        scheduler_weekly_rank_history_archive_max_pages_per_run=max(
            1, min(6, int(weekly_rank_history_archive.get("max_pages_per_run", 1))),
        ),
        scheduler_weekly_rank_history_archive_auto_create_backup_if_missing=bool(
            weekly_rank_history_archive.get("auto_create_backup_if_missing", True)
        ),
        scheduler_weekly_rank_history_archive_backup_prefix=(
            str(
                weekly_rank_history_archive.get(
                    "backup_prefix",
                    "bond_funds_pre_rank_archive",
                )
            ).strip()
            or "bond_funds_pre_rank_archive"
        ),
        scheduler_weekly_rank_history_archive_backup_compress=bool(
            weekly_rank_history_archive.get("backup_compress", True)
        ),
        scheduler_weekly_rank_history_archive_vacuum_enabled=bool(
            weekly_rank_history_archive_vacuum.get("enabled", True)
        ),
        scheduler_weekly_rank_history_archive_vacuum_mode=(
            str(weekly_rank_history_archive_vacuum.get("mode", "auto")).strip() or "auto"
        ),
        scheduler_weekly_rank_history_archive_vacuum_confirm_replace=bool(
            weekly_rank_history_archive_vacuum.get("confirm_replace", True)
        ),
        scheduler_weekly_rank_history_archive_vacuum_min_free_space_multiplier=float(
            weekly_rank_history_archive_vacuum.get("min_free_space_multiplier", 1.25)
        ),
        scheduler_weekly_incremental_vacuum_enabled=bool(
            weekly_maintenance.get("incremental_vacuum", {}).get("enabled", True)
        ),
        scheduler_weekly_incremental_vacuum_max_pages=max(
            1,
            int(weekly_maintenance.get("incremental_vacuum", {}).get("max_pages", 5_000)),
        ),
        scheduler_weekly_incremental_vacuum_min_freelist_pages=max(
            0,
            int(
                weekly_maintenance.get("incremental_vacuum", {}).get(
                    "min_freelist_pages", 1_000
                )
            ),
        ),
        scheduler_weekly_incremental_vacuum_max_seconds=max(
            1.0,
            float(
                weekly_maintenance.get("incremental_vacuum", {}).get(
                    "max_seconds", 180
                )
                or 180
            ),
        ),
        scheduler_weekly_regression_smoke_enabled=bool(
            weekly_regression_smoke.get("enabled", True)
        ),
        scheduler_weekly_regression_smoke_groups=tuple(
            str(item).strip()
            for item in (
                weekly_regression_smoke.get("groups")
                or [
                    "scheduler_guard",
                    "dashboard_payload",
                    "publication_integrity",
                    "scheduler_failure_drill",
                    "final_selection_contraction_shadow",
                ]
            )
            if str(item).strip()
        ),
        scheduler_weekly_database_backup_cleanup_preview_enabled=bool(
            weekly_database_backup_cleanup_preview.get("enabled", True)
        ),
        scheduler_weekly_database_backup_cleanup_preview_scheduled_keep_count=max(
            1,
            int(weekly_database_backup_cleanup_preview.get("scheduled_keep_count", 6)),
        ),
        scheduler_weekly_database_backup_cleanup_preview_unmanaged_retention_days=max(
            1,
            int(weekly_database_backup_cleanup_preview.get("unmanaged_retention_days", 14)),
        ),
        scheduler_weekly_database_backup_cleanup_preview_keep_latest_manual_snapshot_per_family=max(
            1,
            int(
                weekly_database_backup_cleanup_preview.get(
                    "keep_latest_manual_snapshot_per_family",
                    1,
                )
            ),
        ),
        scheduler_weekly_database_backup_cleanup_preview_max_sample_files=max(
            1,
            int(weekly_database_backup_cleanup_preview.get("max_sample_files", 20)),
        ),
        scheduler_weekly_database_backup_cleanup_execution_enabled=bool(
            weekly_database_backup_cleanup_execution.get("enabled", False)
        ),
        scheduler_weekly_database_backup_cleanup_execution_mode=str(
            weekly_database_backup_cleanup_execution.get("mode", "apply")
        ).strip()
        or "apply",
        scheduler_weekly_database_backup_cleanup_execution_checkpoint_every=max(
            1,
            int(weekly_database_backup_cleanup_execution.get("checkpoint_every", 50)),
        ),
        scheduler_weekly_maintenance_backup_retention_enabled=bool(
            weekly_maintenance_backup_retention.get("enabled", False)),
        scheduler_weekly_maintenance_backup_retention_keep_per_purpose=max(
            1, int(weekly_maintenance_backup_retention.get("keep_per_purpose", 1))),
        scheduler_weekly_maintenance_backup_retention_min_age_days=max(
            1, int(weekly_maintenance_backup_retention.get("min_age_days", 14))),
        scheduler_weekly_maintenance_backup_retention_max_delete_files=max(
            1, int(weekly_maintenance_backup_retention.get("max_delete_files", 2))),
        scheduler_weekly_database_backup_compression_preview_enabled=bool(
            weekly_database_backup_compression_preview.get("enabled", True)
        ),
        scheduler_weekly_database_backup_compression_preview_min_compress_size_mb=max(
            0.0,
            float(
                weekly_database_backup_compression_preview.get(
                    "min_compress_size_mb",
                    128.0,
                )
            ),
        ),
        scheduler_weekly_database_backup_compression_preview_max_sample_files=max(
            1,
            int(weekly_database_backup_compression_preview.get("max_sample_files", 20)),
        ),
        scheduler_weekly_database_backup_compression_execution_enabled=bool(
            weekly_database_backup_compression_execution.get("enabled", True)
        ),
        scheduler_weekly_database_backup_compression_execution_mode=str(
            weekly_database_backup_compression_execution.get("mode", "apply")
        ).strip()
        or "apply",
        scheduler_weekly_database_backup_compression_execution_checkpoint_every=max(
            1,
            int(weekly_database_backup_compression_execution.get("checkpoint_every", 1)),
        ),
        scheduler_weekly_database_governance_health_audit_enabled=bool(
            weekly_database_governance_health_audit.get("enabled", True)
        ),
        scheduler_weekly_database_optimization_completion_audit_enabled=bool(
            weekly_database_optimization_completion_audit.get("enabled", True)
        ),
        scheduler_weekly_disk_cleanup_archive_keep_count=int(
            weekly_disk_cleanup.get("archive_keep_count", 60)
        ),
        scheduler_weekly_disk_cleanup_archive_keep_days=int(
            weekly_disk_cleanup.get("archive_keep_days", 30)
        ),
        scheduler_weekly_disk_cleanup_logs_keep_days=int(
            weekly_disk_cleanup.get("logs_keep_days", 14)
        ),
        scheduler_weekly_disk_cleanup_maintenance_candidate_keep_count=int(
            weekly_disk_cleanup.get("maintenance_candidate_keep_count", 0)
        ),
        scheduler_weekly_disk_cleanup_maintenance_candidate_min_age_hours=int(
            weekly_disk_cleanup.get("maintenance_candidate_min_age_hours", 6)
        ),
        scheduler_weekly_disk_cleanup_core_csv_enabled=bool(
            weekly_disk_cleanup.get("core_csv_compression_enabled", True)
        ),
        scheduler_weekly_disk_cleanup_core_csv_keep_days=int(
            weekly_disk_cleanup.get("core_csv_keep_plain_days", 30)
        ),
        scheduler_long_term_history_empty_payload_retry_days=int(
            scheduler.get("long_term_history_empty_payload_retry_days", 7)
        ),
        scheduler_long_term_history_empty_payload_upstream_limited_failures=int(
            scheduler.get("long_term_history_empty_payload_upstream_limited_failures", 3)
        ),
        scheduler_long_term_history_rank_stale_days=int(
            scheduler.get("long_term_history_rank_stale_days", 30)
        ),
        scheduler_long_term_history_stale_refresh_retry_days=int(
            scheduler.get("long_term_history_stale_refresh_retry_days", 28)
        ),
        scheduler_long_term_history_stale_refresh_upstream_limited_failures=int(
            scheduler.get("long_term_history_stale_refresh_upstream_limited_failures", 2)
        ),
        scheduler_long_term_history_high_priority_stale_backlog_trigger=int(
            scheduler.get("long_term_history_high_priority_stale_backlog_trigger", 400)
        ),
        scheduler_long_term_history_high_priority_stale_limit=int(
            scheduler.get("long_term_history_high_priority_stale_limit", 160)
        ),
        effective_nav_coverage_ratio=float(scheduler.get("effective_nav_coverage_ratio", 0.9)),
        effective_nav_weekday_only=bool(scheduler.get("effective_nav_weekday_only", True)),
        scheduler_snapshot_watchdog_enabled=bool(
            (scheduler.get("snapshot_watchdog") or {}).get("enabled", True)
        ),
        scheduler_snapshot_watchdog_interval_minutes=int(
            (scheduler.get("snapshot_watchdog") or {}).get("interval_minutes", 15) or 15
        ),
        report_history_database_path=_resolve_path(project_root, paths.get('report_history_database', 'backend/data/report_history.sqlite')),
        storage_lifecycle_enabled=bool((scheduler.get('storage_lifecycle') or {}).get('enabled', False)),
        storage_lifecycle_archive_root=Path(os.environ.get('FUND_ANA_STORAGE_ARCHIVE_ROOT') or (scheduler.get('storage_lifecycle') or {}).get('archive_root', '/mnt/Project-Archives/Fund-ANA')),
        storage_observation_enabled=bool(
            (scheduler.get("storage_observation") or {}).get("enabled", True)
        ),
        storage_observation_interval_hours=int(
            (scheduler.get("storage_observation") or {}).get("interval_hours", 6)
        ),
        storage_observation_timeout_seconds=int(
            (scheduler.get("storage_observation") or {}).get("timeout_seconds", 45)
        ),
        storage_observation_retain_observations=int(
            (scheduler.get("storage_observation") or {}).get("retain_observations", 120)
        ),
    )
    settings.ensure_directories()
    return settings
