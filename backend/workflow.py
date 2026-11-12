from __future__ import annotations

import json
import os
import subprocess
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from backend.config import Settings, load_settings
from backend.schema_bootstrap import ensure_database_schema
from backend.publication import (
    archive_superseded_runs,
    build_publication_registry,
    load_current_published_report_payload,
    load_canonical_report_payloads,
    load_latest_canonical_report_payload,
    load_publication_registry,
)
from backend.publication_transaction import (
    commit_report_revision,
    expose_report_alias,
    publication_io_lock,
    recover_publication_aliases,
    resolve_committed_path,
)
from backend.report_builder import build_lightweight_report_payload, build_report_payload
from backend.recommendations_sidecar import materialize_recommendations_detail_sidecar
from backend.state_normalizer import normalize_snapshot_payload
from backend.standalone_runner import run_project_pipeline
from backend.storage import (
    WorkflowLockBusyError,
    WorkflowOwnershipError,
    WorkflowStore,
)


def _workflow_publication_control(options: dict[str, Any]) -> dict[str, Any]:
    current = dict(options or {})
    reasons: list[str] = []
    if bool(current.get("reuse_existing_outputs")):
        reasons.append("reuse_existing_outputs")
    if bool(current.get("skip_risk_calculation")):
        reasons.append("skip_risk_calculation")
    if not bool(current.get("run_screening", True)):
        reasons.append("skip_screening")

    if not reasons:
        return {
            "mode": "formal",
            "eligible_for_formal_publication": True,
            "artifact_checks_required": True,
            "workflow_options": current,
        }

    return {
        "mode": "diagnostic_only",
        "reason": ",".join(reasons),
        "eligible_for_formal_publication": False,
        "workflow_options": current,
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_payload, _ = normalize_snapshot_payload(payload)
    temp_file = path.parent / f"{path.name}.{uuid4().hex}.tmp"
    temp_file.write_text(json.dumps(normalized_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_file.replace(path)


class WorkflowManager:
    STALE_HEARTBEAT_SECONDS = 300

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        collection_event_callback: Callable[[str], None] | None = None,
    ):
        self.settings = settings or load_settings()
        self.store = WorkflowStore(self.settings.state_file)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._schema_ready = False
        self._collection_event_callback = collection_event_callback

    def _append_pipeline_log(self, message: str) -> None:
        self.store.append_log(message)
        # A scheduler subprocess must receive the terminal evidence before
        # later progress evicts it from the bounded workflow state log.
        callback = getattr(self, "_collection_event_callback", None)
        if callback is not None and "api_source_evidence=" in message:
            callback(message)

    def _ensure_schema_once(self) -> None:
        # P0-01 / B1: 在真正进入 workflow 执行之前幂等触发一次 schema 迁移，
        # 避免 import backend.workflow 这条副作用去改动生产 DB。
        # `ensure_database_schema` 自带进程级缓存，重复调用零代价。
        if self._schema_ready:
            return
        ensure_database_schema(self.settings)
        self._schema_ready = True

    def status(self) -> dict[str, Any]:
        self._recover_if_stale()
        return self.store.snapshot()

    def status_summary(self) -> dict[str, Any]:
        self._recover_if_stale()
        return self.store.snapshot_summary()

    def is_running(self) -> bool:
        self._recover_if_stale()
        if self.store.has_active_lock():
            return True
        state = self.store.snapshot()
        return state.get("status") == "running"

    @staticmethod
    def _pid_is_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _heartbeat_is_fresh(self, state: dict[str, Any]) -> bool:
        last_updated_at = self._parse_timestamp(state.get("last_updated_at"))
        if last_updated_at is None:
            return False
        return (datetime.now() - last_updated_at).total_seconds() < self.STALE_HEARTBEAT_SECONDS

    def _recover_if_stale(self) -> None:
        state = self.store.snapshot()
        if state.get("status") != "running":
            return
        # 只要跨进程 workflow 锁仍被持有，就优先认定真实执行仍然存活。
        # 这比 state 文件里的 pid / heartbeat 更可靠，可避免长任务阶段
        # 因瞬时 pid 可见性或状态文件中途缺口被误回收。
        if self.store.has_active_lock():
            return
        pid = int(state.get("pid") or 0)
        if pid:
            if self._pid_is_alive(pid):
                return
            self.store.recover_stale_run("workflow_process_missing")
            return
        if self._heartbeat_is_fresh(state):
            return
        self.store.recover_stale_run("workflow_process_missing")

    def _heartbeat_loop(self, stop_event: threading.Event) -> None:
        while not stop_event.wait(15):
            try:
                self.store.touch()
            except Exception:
                break

    def _execute_with_heartbeat(self, run_id: str, options: dict[str, Any]) -> dict[str, Any]:
        stop_event = threading.Event()
        heartbeat = threading.Thread(target=self._heartbeat_loop, args=(stop_event,), daemon=True)
        heartbeat.start()
        try:
            return self._execute(run_id, options)
        finally:
            stop_event.set()
            heartbeat.join(timeout=1)

    def start(self, options: dict[str, Any] | None = None) -> str:
        options = options or {}
        with self._lock:
            if self.is_running():
                raise RuntimeError("已有筛选任务正在运行，请等待完成后再发起新任务。")

            self._ensure_schema_once()
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
            try:
                self.store.start(run_id, options)
            except WorkflowLockBusyError as exc:
                raise RuntimeError("已有筛选任务正在运行，请等待完成后再发起新任务。") from exc
            self._thread = threading.Thread(
                target=self._run,
                args=(run_id, options),
                daemon=True,
            )
            self._thread.start()
            return run_id

    def run_sync(self, options: dict[str, Any] | None = None) -> dict[str, Any]:
        options = options or {}
        with self._lock:
            if self.is_running():
                raise RuntimeError("已有筛选任务正在运行，请等待完成后再发起新任务。")
            self._ensure_schema_once()
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_sync"
            try:
                self.store.start(run_id, options)
            except WorkflowLockBusyError as exc:
                raise RuntimeError("已有筛选任务正在运行，请等待完成后再发起新任务。") from exc
            try:
                report = self._execute_with_heartbeat(run_id, options)
            except BaseException as exc:
                try:
                    self.store.append_log(traceback.format_exc())
                except WorkflowOwnershipError:
                    pass
                try:
                    self.store.finish_failure(str(exc))
                except WorkflowOwnershipError:
                    pass
                raise
            self.store.finish_success(report)
            return report

    def list_reports(self) -> list[dict[str, Any]]:
        return load_canonical_report_payloads(self.settings)

    def latest_report(self) -> dict[str, Any] | None:
        payload = load_latest_canonical_report_payload(self.settings)
        if payload:
            return payload
        reports = self.list_reports()
        return reports[0] if reports else None

    def _run(self, run_id: str, options: dict[str, Any]) -> None:
        try:
            report = self._execute_with_heartbeat(run_id, options)
            self.store.finish_success(report)
        except Exception as exc:  # pragma: no cover - defensive logging path
            try:
                self.store.append_log(traceback.format_exc())
            except WorkflowOwnershipError:
                pass
            try:
                self.store.finish_failure(str(exc))
            except WorkflowOwnershipError:
                pass

    def _run_annual_return_gate_shadow_audit(
        self, run_id: str, *, report: dict[str, Any] | None = None,
    ) -> None:
        from backend.annual_return_gate_shadow import load_incomplete_annual_shadow_terminal
        from backend.core_runner import build_subprocess_log_index, run_structured_subprocess

        cmd = [
            os.sys.executable,
            "scripts/audit_annual_return_gate_shadow.py",
        ]
        if report is not None:
            report_path = resolve_committed_path(self.settings, report["artifacts"]["report_json"])
            cmd.extend(["--report", str(report_path)])
        else:
            cmd.extend(["--run-id", run_id])
        log_dir = self.settings.runs_dir / run_id / "logs" / "subprocess"
        try:
            completed = run_structured_subprocess(
                task_name="annual-return-gate-shadow",
                cmd=cmd,
                cwd=self.settings.project_root,
                log=self.store.append_log,
                log_dir=log_dir,
            )
        except OSError as exc:
            self.store.append_log(f"[annual-return-gate-shadow] 启动失败: {exc}")
            return

        build_subprocess_log_index(
            log_dir=log_dir,
            project_root=self.settings.project_root,
            run_id=run_id,
        )
        for line in completed.stdout_tail[-8:]:
            if line:
                self.store.append_log(f"[annual-return-gate-shadow] {line}")
        for line in completed.stderr_tail[-8:]:
            if line:
                self.store.append_log(f"[annual-return-gate-shadow][stderr] {line}")
        if not completed.success:
            incomplete = load_incomplete_annual_shadow_terminal(
                self.settings, run_id=run_id, stdout_tail=completed.stdout_tail, return_code=completed.return_code)
            if incomplete:
                self.store.append_log(
                    f"[annual-return-gate-shadow] 历史证据核验未完成，status={incomplete['status']}, "
                    f"scenario={incomplete['scenario_status']}, returncode={completed.return_code}, "
                    f"artifact={incomplete['artifact']}；保留正式发布，后续影子分析暂缓"
                )
                return
            self.store.append_log(
                f"[annual-return-gate-shadow] 执行失败，returncode={completed.return_code}, meta={completed.meta_path}"
            )
            return
        self.store.append_log("[annual-return-gate-shadow] 审计完成")
        self._run_score_dedup_shadow_audit(run_id, report=report)

    def _run_score_dedup_shadow_audit(self, run_id: str, *, report: dict[str, Any] | None = None) -> None:
        """持续积累评分去冗余前瞻窗口；失败不阻断正式发布。"""
        from backend.core_runner import build_subprocess_log_index, run_structured_subprocess
        from backend.frozen_strategy_audit import load_incomplete_score_shadow_terminal

        cmd = [
            os.sys.executable,
            "scripts/audit_score_dedup_shadow.py",
        ]
        if report is not None:
            report_path = resolve_committed_path(self.settings, report["artifacts"]["report_json"])
            cmd.extend(["--report", str(report_path)])
        else:
            cmd.extend(["--run-id", run_id])
        log_dir = self.settings.runs_dir / run_id / "logs" / "subprocess"
        try:
            completed = run_structured_subprocess(
                task_name="score-dedup-shadow",
                cmd=cmd,
                cwd=self.settings.project_root,
                log=self.store.append_log,
                log_dir=log_dir,
            )
        except OSError as exc:
            self.store.append_log(f"[score-dedup-shadow] 启动失败: {exc}")
            return

        build_subprocess_log_index(
            log_dir=log_dir,
            project_root=self.settings.project_root,
            run_id=run_id,
        )
        for line in completed.stdout_tail[-8:]:
            if line:
                self.store.append_log(f"[score-dedup-shadow] {line}")
        for line in completed.stderr_tail[-8:]:
            if line:
                self.store.append_log(f"[score-dedup-shadow][stderr] {line}")
        if not completed.success:
            incomplete = load_incomplete_score_shadow_terminal(self.settings, run_id=run_id,
                stdout_tail=completed.stdout_tail, return_code=completed.return_code)
            if incomplete:
                self.store.append_log(
                    f"[score-dedup-shadow] 冻结基线已核验，情景历史输入不足，scenario={incomplete['scenario_status']}, "
                    f"missing={incomplete['missing_input_count']}, returncode={completed.return_code}, "
                    f"artifact={incomplete['artifact']}；生产评分保持不变"
                )
                return
            self.store.append_log(
                f"[score-dedup-shadow] 执行失败，returncode={completed.return_code}, meta={completed.meta_path}"
            )
            return
        self.store.append_log("[score-dedup-shadow] 审计完成；生产评分保持不变")

    def _materialize_recommendations_sidecar(self, report: dict[str, Any] | None) -> None:
        if not isinstance(report, dict) or not report:
            return
        try:
            result = materialize_recommendations_detail_sidecar(self.settings, report)
        except Exception as exc:
            self.store.append_log(f"[recommendations-detail-sidecar] 生成失败: {type(exc).__name__}: {exc}")
            return
        if bool(result.get("written")):
            self.store.append_log(
                "[recommendations-detail-sidecar] 已生成 "
                f"{result.get('project_path') or result.get('path')} "
                f"rows={result.get('row_count')}"
            )
        else:
            self.store.append_log(
                "[recommendations-detail-sidecar] 未生成: "
                f"{result.get('reason') or 'unknown'}"
            )

    def _run_frozen_baseline_audit(self, report: dict[str, Any]) -> None:
        """Accumulate an independent proof for each new committed snapshot."""
        if not (report.get("artifacts") or {}).get("screening_input_snapshot_json_gz"):
            return
        from backend.core_runner import (
            _workflow_task_timeout, build_subprocess_log_index, run_structured_subprocess,
        )

        run_id = report["run_id"]
        log_dir = self.settings.runs_dir / run_id / "logs" / "subprocess"
        try:
            report_path = resolve_committed_path(self.settings, report["artifacts"]["report_json"])
            self.store.set_phase("verifying-frozen-report-baseline")
            completed = run_structured_subprocess(
                task_name="frozen-baseline-replay",
                cmd=[os.sys.executable, "scripts/audit_screening_baseline_replay.py", "--report", str(report_path)],
                cwd=self.settings.project_root, log=self.store.append_log, log_dir=log_dir,
                timeout=_workflow_task_timeout(self.settings, "frozen-baseline-replay") or 180,
            )
            build_subprocess_log_index(log_dir=log_dir, project_root=self.settings.project_root, run_id=run_id)
            self.store.append_log(
                f"[frozen-baseline-replay] {'核对通过' if completed.success else '未通过，保留差异/异常证据'} "
                f"return_code={completed.return_code} meta={completed.meta_path}; "
                "范围仅为报告筛选，不代表上游风险重算或策略效果已验证"
            )
        except Exception as exc:
            self.store.append_log(f"[frozen-baseline-replay] 验证无法完成: {exc}；不标记基线一致")

    def _share_final_report_artifacts(self, run_id: str, report: dict[str, Any]) -> None:
        from backend.report_artifact_storage import share_finalized_report

        try:
            result = share_finalized_report(self.settings, report)
            receipt = self.settings.runs_dir / run_id / "logs" / "report_artifact_storage.json"
            _write_json_atomic(receipt, result)
            self.store.append_log(
                f"[report-artifact-storage] status={result['status']} shared={result['shared_files']} "
                f"removed_allocated_bytes_estimate={result['removed_allocated_bytes_estimate']} "
                f"errors={len(result['errors'])}; receipt={receipt}"
            )
        except Exception as exc:
            # Storage optimization cannot revoke a valid publication. Atomic
            # file replacement keeps all local paths usable after interruption.
            self.store.append_log(f"[report-artifact-storage] 共享未完全执行: {exc}；报告保留原路径及内容")

    def _commit_formal_report(self, run_id: str, report: dict[str, Any]) -> None:
        with publication_io_lock(self.settings):
            self._materialize_recommendations_sidecar(report)
            from backend.report_builder import _build_artifact_consistency_audit

            revalidated = _build_artifact_consistency_audit(settings=self.settings, summary=report)
            if not revalidated.get("aligned"):
                raise RuntimeError(f"formal_publication_revalidation_failed: {revalidated.get('blockers')}")
            report_path = resolve_committed_path(self.settings, report["artifacts"]["report_json"])
            prospective = build_publication_registry(self.settings, candidate_report=(report_path, report))
            if prospective.get("current_published_run_id") != run_id:
                records = [*prospective.get("canonical_runs", []), *prospective.get("superseded_runs", [])]
                candidate = next((item for item in records if item.get("run_id") == run_id), {})
                codes = [item.get("code") for item in candidate.get("publish_blockers", []) if item.get("blocking")]
                raise RuntimeError(f"formal_publication_not_current: workflow_run_id={run_id}, blockers={codes}")
            legacy_report = load_current_published_report_payload(self.settings)
            commit_report_revision(self.settings, report, legacy_report=legacy_report)

    def _publish_formal_report(
        self,
        *,
        run_id: str,
        report: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        self._commit_formal_report(run_id, report)
        registry = archive_superseded_runs(self.settings, keep_run_id=run_id)
        latest_canonical_report = load_latest_canonical_report_payload(self.settings) or report
        self.store.set_published_report(latest_canonical_report)
        refreshed_registry = load_publication_registry(self.settings)
        effective_registry = refreshed_registry or registry
        published_run_id = str(
            effective_registry.get("current_published_run_id")
            or effective_registry.get("latest_canonical_run_id")
            or ""
        ).strip()
        latest_report_run_id = str((latest_canonical_report or {}).get("run_id") or "").strip()
        if published_run_id != run_id or latest_report_run_id != run_id:
            raise RuntimeError(
                "formal_publication_not_current: "
                f"workflow_run_id={run_id}, "
                f"current_published_run_id={published_run_id or '-'}, "
                f"latest_report_run_id={latest_report_run_id or '-'}"
            )
        return effective_registry, latest_canonical_report

    def _execute(self, run_id: str, options: dict[str, Any]) -> dict[str, Any]:
        from backend.collection_calculation_capture import validate_collection_capture_reference

        references = options.get("collection_calculation_captures") or []
        if not isinstance(references, list) or len(references) > 8:
            raise ValueError("invalid_parent_collection_captures")
        collection_captures = [validate_collection_capture_reference(self.settings, item) for item in references]
        if len({item["capture_run_id"] for item in collection_captures}) != len(collection_captures):
            raise ValueError("duplicate_parent_collection_capture")
        self.store.set_phase("running-core-workflow")
        self.store.append_log(f"开始运行批次 {run_id}")
        self.store.append_log(f"天天基金 API: {self.settings.tiantian_base_url}")
        self.store.append_log(f"AutoStock API: {self.settings.autostock_base_url}")
        self.store.append_log(f"AutoStock启用: {getattr(self.settings, 'autostock_enabled', True)}；正式校验来源: {getattr(self.settings, 'publication_validation_provider', 'autostock')}")

        pipeline_options = dict(options)
        pipeline_options["workflow_run_id"] = run_id
        artifacts = run_project_pipeline(self.settings, pipeline_options, self._append_pipeline_log)
        if collection_captures:
            artifacts["collection_calculation_input_captures"] = collection_captures
        self.store.set_phase("building-report")
        self.store.append_log("核心阶段已结束，开始构建报告；本轮执行范围以子进程记录为准")

        reuse_existing_outputs = bool(options.get("reuse_existing_outputs", False))
        skip_annual_return_gate_shadow_audit = bool(
            options.get("skip_annual_return_gate_shadow_audit", False)
        )
        publication_control = _workflow_publication_control(options)
        formal_publication_eligible = publication_control.get("eligible_for_formal_publication") is not False
        registry: dict[str, Any] = {}
        latest_canonical_report: dict[str, Any] | None = None
        formal_report_published_before_shadow = False
        if reuse_existing_outputs:
            self.store.append_log("当前批次复用既有核心产物，执行轻量报告刷新")
            base_payload = load_latest_canonical_report_payload(self.settings)
            report = build_lightweight_report_payload(
                settings=self.settings,
                run_id=run_id,
                artifacts=artifacts,
                base_payload=base_payload or {},
            )
        else:
            report = build_report_payload(
                settings=self.settings,
                run_id=run_id,
                artifacts=artifacts,
                publication_control=publication_control,
            )
            if formal_publication_eligible:
                self.store.set_phase("publishing-formal-report")
                registry, latest_canonical_report = self._publish_formal_report(
                    run_id=run_id,
                    report=report,
                )
                formal_report_published_before_shadow = True
                self.store.append_log("当前批次已通过正式发布门禁并写入正式发布指针")
            if skip_annual_return_gate_shadow_audit:
                self.store.append_log("当前批次跳过 annual-return-gate-shadow 审计")
            else:
                self.store.set_phase("building-annual-return-gate-shadow-audit")
                self._run_annual_return_gate_shadow_audit(run_id, report=report)
                self.store.append_log("影子审计独立绑定当前报告版本；正式报告与基金输入保持本次生成结果")
        if not formal_publication_eligible:
            if (report.get("publication_control") or {}).get("commit_protocol"):
                with publication_io_lock(self.settings):
                    expose_report_alias(self.settings, report)
            registry = load_publication_registry(self.settings)
            latest_canonical_report = load_latest_canonical_report_payload(self.settings)
            if latest_canonical_report:
                with publication_io_lock(self.settings):
                    recover_publication_aliases(self.settings)
                self.store.set_published_report(latest_canonical_report)
            self.store.append_log("当前批次为诊断/轻量刷新，不改写正式发布指针")
        elif not formal_report_published_before_shadow:
            self.store.set_phase("publishing-formal-report")
            registry, latest_canonical_report = self._publish_formal_report(
                run_id=run_id,
                report=report,
            )
        canonical_run_id = str((registry.get("latest_canonical_run_id") or "")).strip()
        if canonical_run_id and canonical_run_id != run_id:
            self.store.append_log(f"当前批次 {run_id} 已完成，但发布口径仍沿用更优版本 {canonical_run_id}")
        self.store.append_log(f"当前批次报告生成完成: {report['artifacts']['report_markdown']}")
        if latest_canonical_report:
            self.store.append_log(f"当前发布报告: {latest_canonical_report['artifacts']['report_markdown']}")
        if formal_publication_eligible and not reuse_existing_outputs:
            self._run_frozen_baseline_audit(report)
        self._share_final_report_artifacts(run_id, report)
        return report
