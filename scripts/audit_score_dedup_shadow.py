from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.audit_support import write_compressed_audit_csv  # noqa: E402
from backend.artifact_paths import project_relative_artifact_path  # noqa: E402
from backend.forward_validation import (  # noqa: E402
    END_NAV_MAX_LAG_DAYS,
    FORWARD_VALIDATION_WINDOWS,
    _build_window_metrics_frame,
    _cohort_metrics,
    _load_nav_history_for_codes,
    _safe_date,
)
from backend.report_builder import (  # noqa: E402
    FORMAL_SCORE_LONG_TERM_WEIGHT,
    FORMAL_SCORE_SHORT_TERM_WEIGHT,
    PROJECT_SCORE_COMPONENTS,
)
from backend.report_inputs import (  # noqa: E402
    normalize_code,
)
from backend.frozen_strategy_audit import (  # noqa: E402
    FROZEN_STRATEGY_CONTRACT,
    FROZEN_SOURCE_KEYS,
    bind_frozen_report_sources,
    missing_scenario_inputs,
    run_frozen_strategy_scenario,
    scenario_gap_frame,
)
from backend.screening_baseline_replay import file_sha256, verify_frozen_baseline  # noqa: E402
from backend.screening_replay_snapshot import freeze_frame, restore_frame  # noqa: E402
from backend.replay_evidence import REQUIRED_BASELINE_CHECKS  # noqa: E402
from backend.score_dedup_shadow import (  # noqa: E402
    SHADOW_SCORE_COMPONENTS,
    SHADOW_SCORE_CONTRACT_VERSION,
    attach_score_differences,
    build_duplicate_return_correlation,
    build_membership_comparison,
    build_ranking_stability,
    build_risk_exposure_comparison,
    build_shadow_contract,
    calculate_scores,
)
from scripts.audit_annual_return_gate_shadow import (  # noqa: E402
    _apply_holding_period_baseline_gate,
)
from scripts.audit_runtime_utils import (  # noqa: E402
    build_audit_artifact_path,
    resolve_run_payload,
)


MIN_FORWARD_COHORT_COVERAGE_PCT = 80.0
MIN_MATURE_SHADOW_SNAPSHOTS = 3
REQUIRED_FORWARD_WINDOWS = {"1个月", "3个月", "6个月"}


def _ensure_project_venv() -> None:
    if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
        os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])


def _code_set(df: pd.DataFrame) -> set[str]:
    return {
        normalize_code(code)
        for code in df.get("基金代码", pd.Series(dtype=object)).tolist()
        if normalize_code(code)
    }


def _membership_delta(baseline_df: pd.DataFrame, scenario_df: pd.DataFrame) -> dict[str, Any]:
    baseline = _code_set(baseline_df)
    scenario = _code_set(scenario_df)
    return {
        "aligned": baseline == scenario,
        "added_count": int(len(scenario - baseline)),
        "removed_count": int(len(baseline - scenario)),
        "added_codes": sorted(scenario - baseline),
        "removed_codes": sorted(baseline - scenario),
    }


def _enrich_official_cohort(official_df: pd.DataFrame, scored_df: pd.DataFrame) -> pd.DataFrame:
    current = official_df.copy()
    current["基金代码"] = current.get("基金代码", pd.Series(dtype=object)).map(normalize_code)
    source = scored_df.copy()
    source["基金代码"] = source.get("基金代码", pd.Series(dtype=object)).map(normalize_code)
    enrichment_columns = [
        "基金代码",
        "债基风格标签",
        "return_1y",
        "annual_return",
        "return_3y",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "max_drawdown",
        "volatility",
        "positive_ratio",
        "fee_total_numeric",
        "scale_billion",
    ]
    source = source[[column for column in enrichment_columns if column in source.columns]].drop_duplicates(
        subset=["基金代码"], keep="first"
    )
    merged = current.merge(source, on="基金代码", how="left", suffixes=("", "_shadow_source"))
    for column in enrichment_columns:
        fallback = f"{column}_shadow_source"
        if column == "基金代码" or fallback not in merged.columns:
            continue
        if column not in merged.columns:
            merged[column] = merged[fallback]
        else:
            existing = merged[column]
            if existing.dtype == object:
                blank = existing.fillna("").astype(str).str.strip().eq("")
                merged.loc[blank, column] = merged.loc[blank, fallback]
            else:
                merged[column] = existing.combine_first(merged[fallback])
        merged = merged.drop(columns=[fallback])
    return merged


def _replace_with_shadow_scores(df: pd.DataFrame) -> pd.DataFrame:
    current = df.copy()
    current["正式投资评分"] = pd.to_numeric(current.get("shadow_formal_score"), errors="coerce")
    current["综合加权得分"] = pd.to_numeric(current.get("shadow_formal_score"), errors="coerce")
    current["综合评分"] = pd.to_numeric(current.get("shadow_formal_score"), errors="coerce")
    current["近期得分"] = pd.to_numeric(current.get("shadow_short_score"), errors="coerce")
    current["长期得分"] = pd.to_numeric(current.get("shadow_long_score"), errors="coerce")
    current["评分模式"] = "去冗余影子评分/不参与生产发布"
    return current


def _forward_validation(
    *,
    snapshot_date: str,
    added_codes: list[str],
    removed_codes: list[str],
    nav_df: pd.DataFrame,
) -> dict[str, Any]:
    snapshot_dt = _safe_date(snapshot_date)
    base = {
        "snapshot_date": snapshot_date,
        "windows": [],
        "ready_window_count": 0,
        "required_ready_window_count": int(len(REQUIRED_FORWARD_WINDOWS)),
        "minimum_coverage_pct": float(MIN_FORWARD_COHORT_COVERAGE_PCT),
        "production_switch_supported_now": False,
    }
    if snapshot_dt is None:
        return {**base, "status": "snapshot_date_missing", "blocking_reason": "snapshot_date_missing"}
    all_codes = sorted(set(added_codes) | set(removed_codes))
    if not all_codes:
        return {**base, "status": "no_membership_change", "blocking_reason": "no_forward_comparison_cohort"}

    nav_map = {
        str(code): group.sort_values("nav_date", kind="mergesort").reset_index(drop=True)
        for code, group in nav_df.groupby("fcode")
    }
    ready_required = 0
    rows: list[dict[str, Any]] = []
    for label, days in FORWARD_VALIDATION_WINDOWS:
        metrics_df = _build_window_metrics_frame(
            all_codes,
            nav_map,
            snapshot_date=snapshot_dt,
            horizon_days=days,
        )
        added_summary, _added_detail = _cohort_metrics("shadow_added", added_codes, metrics_df)
        removed_summary, _removed_detail = _cohort_metrics("shadow_removed", removed_codes, metrics_df)
        cohorts_ready = bool(
            added_summary.get("status") == "ready"
            and removed_summary.get("status") == "ready"
            and float(added_summary.get("coverage_ratio_pct") or 0.0) >= MIN_FORWARD_COHORT_COVERAGE_PCT
            and float(removed_summary.get("coverage_ratio_pct") or 0.0) >= MIN_FORWARD_COHORT_COVERAGE_PCT
        )
        if cohorts_ready and label in REQUIRED_FORWARD_WINDOWS:
            ready_required += 1
        rows.append(
            {
                "window": label,
                "horizon_days": int(days),
                "target_date": (snapshot_dt + timedelta(days=days)).date().isoformat(),
                "cohorts_ready": cohorts_ready,
                "added": added_summary,
                "removed": removed_summary,
            }
        )
    latest_nav_date = ""
    if not nav_df.empty:
        latest_nav = pd.to_datetime(nav_df.get("nav_date"), errors="coerce").max()
        if pd.notna(latest_nav):
            latest_nav_date = latest_nav.date().isoformat()
    return {
        **base,
        "status": "ready" if ready_required == len(REQUIRED_FORWARD_WINDOWS) else "future_data_pending",
        "blocking_reason": (
            ""
            if ready_required == len(REQUIRED_FORWARD_WINDOWS)
            else "required_real_forward_nav_windows_not_mature"
        ),
        "latest_available_nav_date": latest_nav_date,
        "ready_window_count": int(ready_required),
        "windows": rows,
    }


def _selection_sha256(binding: dict[str, Any], contract: dict[str, Any], membership: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps({"source_binding": binding, "contract": contract,
        "added_codes": membership["added_codes"], "removed_codes": membership["removed_codes"]},
        ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _validate_forward_nav_packet(packet: dict[str, Any]) -> pd.DataFrame:
    snapshot = _safe_date(packet["snapshot_date"])
    observed = datetime.fromisoformat(packet["observed_at"])
    if snapshot is None or observed.tzinfo is None or snapshot.date() > observed.date():
        raise ValueError("forward_nav_observation_time_invalid")
    max_days = max(days for _label, days in FORWARD_VALIDATION_WINDOWS)
    expected_start = (snapshot - timedelta(days=7)).date().isoformat()
    expected_end = min(observed.date(), (snapshot + timedelta(days=max_days + END_NAV_MAX_LAG_DAYS)).date()).isoformat()
    if packet["query_start_date"] != expected_start or packet["query_end_date"] != expected_end:
        raise ValueError("forward_nav_query_window_mismatch")
    frame = restore_frame(packet["frame"])
    if not {"fcode", "nav_date", "unit_nav"}.issubset(frame.columns):
        raise ValueError("forward_nav_fields_missing")
    dates = pd.to_datetime(frame["nav_date"], errors="coerce")
    if (dates.isna().any() or not dates.between(pd.Timestamp(expected_start), pd.Timestamp(expected_end)).all()
            or not set(frame["fcode"].astype(str)).issubset(packet["codes"])
            or frame.assign(nav_date=dates).duplicated(subset=["fcode", "nav_date"]).any()):
        raise ValueError("forward_nav_rows_outside_bound_cohort_or_dates")
    return frame


def _load_forward_nav_evidence(
    settings, *, snapshot_date: str, codes: list[str], selection_sha256: str,
    output_path: Path, source_path: Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Keep observed outcomes separate from the historical selection inputs."""
    if source_path is not None:
        before = file_sha256(source_path)
        with gzip.open(source_path, "rt", encoding="utf-8") as stream:
            packet = json.load(stream)
        if (packet.get("schema_version") != 1 or packet.get("snapshot_date") != snapshot_date
                or packet.get("codes") != codes or packet.get("selection_sha256") != selection_sha256):
            raise ValueError("forward_nav_evidence_selection_mismatch")
        frame = _validate_forward_nav_packet(packet)
        if file_sha256(source_path) != before:
            raise ValueError("forward_nav_evidence_changed_during_read")
        path, live_read = source_path, False
    else:
        snapshot_dt = _safe_date(snapshot_date)
        if snapshot_dt is None:
            raise ValueError("forward_snapshot_date_missing")
        observed_at = datetime.now().astimezone()
        max_days = max(days for _label, days in FORWARD_VALIDATION_WINDOWS)
        start_date = (snapshot_dt - timedelta(days=7)).date().isoformat()
        end_date = min(observed_at.date(), (snapshot_dt + timedelta(days=max_days + END_NAV_MAX_LAG_DAYS)).date()).isoformat()
        frame = _load_nav_history_for_codes(settings.database_path, codes, start_date=start_date, end_date=end_date)
        packet = {"schema_version": 1, "scope": "real_ex_post_nav_outcomes_not_selection_inputs",
                  "selection_sha256": selection_sha256, "snapshot_date": snapshot_date, "codes": codes,
                  "observed_at": observed_at.isoformat(), "source_database": str(settings.database_path),
                  "query_start_date": start_date, "query_end_date": end_date, "frame": freeze_frame(frame)}
        _validate_forward_nav_packet(packet)
        with output_path.open("xb") as stream, gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed:
            compressed.write(json.dumps(packet, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
        path, live_read = output_path, True
    absolute = path.resolve()
    root = Path(settings.project_root).resolve()
    recorded_path = str(absolute.relative_to(root)) if absolute.is_relative_to(root) else str(absolute)
    return frame, {"path": recorded_path, "sha256": file_sha256(path), "rows": len(frame),
                   "observed_at": packet["observed_at"], "selection_sha256": selection_sha256,
                   "query_start_date": packet["query_start_date"], "query_end_date": packet["query_end_date"],
                   "live_database_read_for_outcomes_only": live_read}


def _historical_shadow_readiness(audit_dir: Path, *, project_root: Path) -> dict[str, Any]:
    mature_snapshots: set[str] = set()
    reviewed_artifacts = 0
    unverified_artifacts = 0
    for path in sorted(audit_dir.glob("score_dedup_shadow_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if not isinstance(payload, dict) or not isinstance(payload.get("contract"), dict):
            continue
        if str(((payload.get("contract") or {}).get("contract_version") or "")) != SHADOW_SCORE_CONTRACT_VERSION:
            continue
        reviewed_artifacts += 1
        try:
            baseline = payload.get("baseline_verification") or {}
            checks = baseline.get("checks") or []
            if (payload.get("input_contract") != FROZEN_STRATEGY_CONTRACT
                    or payload.get("passed") is not True
                    or baseline.get("baseline_verified") is not True
                    or not isinstance(checks, list) or not all(isinstance(check, dict) for check in checks)
                    or not REQUIRED_BASELINE_CHECKS.issubset({check.get("name") for check in checks})
                    or not all(check.get("aligned") is True for check in checks)
                    or (payload.get("shadow_replay") or {}).get("status") != "computed"):
                raise ValueError("historical_strategy_baseline_not_verified")
            forward = payload.get("forward_window_readiness") or {}
            if str(forward.get("status") or "") != "ready":
                continue
            if int(forward.get("ready_window_count") or 0) < len(REQUIRED_FORWARD_WINDOWS):
                raise ValueError("historical_forward_windows_incomplete")
            binding = payload.get("source_binding")
            source_files = binding.get("files") if isinstance(binding, dict) else None
            nav_evidence = forward.get("nav_evidence") or {}
            if (not isinstance(source_files, dict) or not set(FROZEN_SOURCE_KEYS).issubset(source_files)
                    or not isinstance(nav_evidence, dict) or not nav_evidence
                    or not all(isinstance(row, dict) for row in source_files.values())
                    or baseline.get("snapshot_sha256") != source_files["screening_input_snapshot_json_gz"].get("sha256")):
                raise ValueError("historical_strategy_sources_incomplete")
            sources = list(source_files.values())
            if any(file_sha256(project_root / row["path"]) != row["sha256"] for row in sources + [nav_evidence]):
                raise ValueError("historical_strategy_evidence_changed")
            selection_hash = _selection_sha256(payload["source_binding"], payload["contract"], payload["membership_impact"])
            with gzip.open(project_root / nav_evidence["path"], "rt", encoding="utf-8") as stream:
                packet = json.load(stream)
            membership = payload["membership_impact"]
            if (packet.get("schema_version") != 1 or packet.get("selection_sha256") != selection_hash
                    or nav_evidence.get("selection_sha256") != selection_hash
                    or packet.get("snapshot_date") != forward["snapshot_date"]
                    or packet.get("codes") != sorted(set(membership["added_codes"]) | set(membership["removed_codes"]))):
                raise ValueError("historical_forward_selection_mismatch")
            actual_forward = _forward_validation(snapshot_date=packet["snapshot_date"],
                added_codes=membership["added_codes"], removed_codes=membership["removed_codes"],
                nav_df=_validate_forward_nav_packet(packet))
            if actual_forward != {key: value for key, value in forward.items() if key != "nav_evidence"}:
                raise ValueError("historical_forward_metrics_not_reproduced")
            snapshot = str(forward.get("snapshot_date") or "").strip()
            if snapshot:
                mature_snapshots.add(snapshot)
        except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, EOFError):
            # A malformed or truncated old artifact is unverified evidence. It
            # must not count as maturity or abort a later, valid experiment.
            unverified_artifacts += 1
    return {
        "reviewed_same_contract_artifact_count": int(reviewed_artifacts),
        "excluded_unverified_artifact_count": int(unverified_artifacts),
        "mature_distinct_snapshot_count": int(len(mature_snapshots)),
        "minimum_mature_snapshot_count": int(MIN_MATURE_SHADOW_SNAPSHOTS),
        "multi_snapshot_requirement_met": len(mature_snapshots) >= MIN_MATURE_SHADOW_SNAPSHOTS,
    }


def _write_markdown(payload: dict[str, Any], path: Path) -> None:
    membership = payload.get("membership_impact") or {}
    replay = payload.get("replay_alignment") or {}
    forward = payload.get("forward_window_readiness") or {}
    ranking = payload.get("ranking_stability") or {}
    lines = [
        "# 正式评分去冗余影子审计",
        "",
        f"- run_id：`{payload.get('run_id')}`",
        f"- 审计状态：`{payload.get('status')}`",
        f"- 影子契约：`{((payload.get('contract') or {}).get('contract_version'))}`",
        f"- 正式评分是否被修改：`{bool((payload.get('contract') or {}).get('production_score_mutated'))}`",
        f"- 无改动基线精确回放：`{replay.get('aligned') if replay.get('aligned') is not None else '未核验'}`",
        f"- 基线 / shadow 正式榜：`{membership.get('baseline_count')}` / `{membership.get('shadow_count')}`",
        f"- shadow 入榜 / 退榜：`{membership.get('added_count')}` / `{membership.get('removed_count')}`",
        f"- 全局 Spearman：`{ranking.get('overall_spearman')}`",
        "",
        "## 生产切换结论",
        "",
        f"- 当前允许切换：`{bool(payload.get('production_switch_supported_now'))}`",
        f"- 前瞻窗口状态：`{forward.get('status')}`，成熟必需窗口 `{forward.get('ready_window_count')}/{forward.get('required_ready_window_count')}`。",
        f"- 阻断原因：`{payload.get('production_switch_blocking_reasons')}`",
        "- 本产物只用于旁路校准；正式评分常量、阈值和发布文件均未写入。",
        "",
        "## 因子职责",
        "",
        "- `return_1y`：仅承担近期一年累计收益职责。",
        "- `annual_return`：仅承担长期一年年化收益职责。",
        "- 释放权重转向 3 年收益、Sharpe/Sortino/Calmar 与费率摩擦。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="先复现冻结基线，再比较评分去冗余情景；后验净值单独绑定。")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--run-id", default="", help="指定 run；默认读取当前正式报告。")
    source.add_argument("--report", type=Path, help="显式报告 JSON，保留其正式或诊断身份。")
    parser.add_argument("--baseline-only", action="store_true", help="仅核验冻结基线，不计算情景或读取后验净值。")
    parser.add_argument("--forward-nav-snapshot", type=Path, help="重用此情景已保存的后验净值，避免重读当前数据库。")
    args = parser.parse_args()
    settings = load_settings()
    try:
        if args.report:
            report = json.loads(args.report.read_text(encoding="utf-8"))
            if not isinstance(report, dict) or not report.get("run_id"):
                raise ValueError("report_identity_missing")
            run_id = report["run_id"]
        else:
            run_id, report, _, _ = resolve_run_payload(settings, run_id=args.run_id, project_root=settings.project_root)
    except (OSError, ValueError, RuntimeError) as exc:
        print(json.dumps({"passed": False, "status": "report_unavailable", "error": str(exc)}, ensure_ascii=False))
        return 2

    audit_dir = settings.reports_dir / "audit" / "score_dedup_shadow"
    json_path = build_audit_artifact_path(audit_dir, "score_dedup_shadow", run_id=run_id)
    contract = build_shadow_contract(production_components=PROJECT_SCORE_COMPONENTS,
        short_term_weight=FORMAL_SCORE_SHORT_TERM_WEIGHT, long_term_weight=FORMAL_SCORE_LONG_TERM_WEIGHT)
    payload: dict[str, Any] = {
        "schema_version": 2, "input_contract": FROZEN_STRATEGY_CONTRACT,
        "generated_at": datetime.now().isoformat(timespec="seconds"), "run_id": run_id,
        "source_publication_control": report.get("publication_control"),
        "scope": "frozen_report_selection_with_separately_observed_forward_nav",
        "selection_later_database_reads": False, "runtime_upstream_requests": 0,
        "wall_clock_used_for_replayed_gates": False, "missing_business_value_imputation": False,
        "status": "baseline_not_verified", "passed": False, "contract": contract,
        "candidate_count": None, "scoring_universe_count": None,
        "baseline_verification": {}, "replay_alignment": {"aligned": None},
        "shadow_replay": {"status": "not_evaluated", "final_count": None},
        "membership_impact": None, "risk_exposure": None, "ranking_stability": None,
        "duplicate_return_correlation": None,
        "forward_window_readiness": {"status": "not_evaluated", "ready_window_count": None,
            "required_ready_window_count": len(REQUIRED_FORWARD_WINDOWS), "production_switch_supported_now": False},
        "production_switch_supported_now": False,
        "production_switch_blocking_reasons": ["baseline_not_verified"],
        "governance_decision": "observe_only_production_switch_forbidden",
    }
    exports: dict[str, pd.DataFrame] = {}
    try:
        binding = bind_frozen_report_sources(settings, report)
        payload["source_binding"] = binding
        baseline, context = verify_frozen_baseline(settings, report)
        payload["baseline_verification"] = baseline
        payload["replay_precision"] = {"status": baseline["status"], "baseline_reproduction_verified": baseline["baseline_verified"]}
        if baseline["baseline_verified"]:
            frames = context["frames"]
            universe = frames["scored_universe_before_holding_gate"].copy()
            actual = frames["formal_after_all_filters"].copy()
            payload.update(snapshot_date=context["snapshot"]["snapshot_date"],
                evaluation_times=baseline["evaluation_times"], scoring_universe_count=len(universe),
                replay_alignment={**_membership_delta(actual, context["result"]["official"]),
                    "key_values_verified": True, "baseline_count": len(actual)})
            if args.baseline_only:
                payload.update(status="baseline_verified", passed=True,
                    production_switch_blocking_reasons=["scenario_not_requested"])
                payload["shadow_replay"]["status"] = "not_requested"
                exports["baseline_members"] = actual
            else:
                # Match production's percentile population: score first, then
                # apply holding-period eligibility. Never re-rank a filtered pool.
                scored_all = attach_score_differences(calculate_scores(universe,
                    components=SHADOW_SCORE_COMPONENTS, short_term_weight=FORMAL_SCORE_SHORT_TERM_WEIGHT,
                    long_term_weight=FORMAL_SCORE_LONG_TERM_WEIGHT))
                scored = _apply_holding_period_baseline_gate(scored_all, context["settings"].exclude_holding_days_gt)
                payload.update(candidate_count=len(scored),
                    score_universe_policy="complete_frozen_universe_before_holding_gate",
                    duplicate_return_correlation=build_duplicate_return_correlation(scored),
                    ranking_stability=build_ranking_stability(scored))
                scenario = run_frozen_strategy_scenario(context, scored_universe=_replace_with_shadow_scores(scored_all))
                payload["shadow_replay"] = {"status": scenario["status"], "final_count": None,
                    "candidate_count": len(scenario["candidate"]), "publish_gate_count": len(scenario["post_review"]),
                    **missing_scenario_inputs(scenario)}
                exports.update(fund_detail=scored, candidate_decisions=scenario["candidate"], post_review=scenario["post_review"])
                if scenario["status"] != "computed":
                    payload.update(status="scenario_inputs_incomplete",
                        production_switch_blocking_reasons=[scenario["status"]])
                    gaps = scenario_gap_frame(scenario)
                    if not gaps.empty:
                        exports["missing_inputs"] = gaps
                else:
                    actual_enriched = _enrich_official_cohort(actual, scored)
                    shadow_enriched = _enrich_official_cohort(scenario["official"], scored)
                    membership, scored = build_membership_comparison(scored, actual_enriched, shadow_enriched)
                    added_codes, removed_codes = membership["added_codes"], membership["removed_codes"]
                    selection_hash = _selection_sha256(binding, contract, membership)
                    outcome_codes = sorted(set(added_codes) | set(removed_codes))
                    nav_frame = pd.DataFrame(columns=["fcode", "nav_date"])
                    nav_evidence = None
                    if outcome_codes:
                        nav_frame, nav_evidence = _load_forward_nav_evidence(settings,
                            snapshot_date=payload["snapshot_date"], codes=outcome_codes, selection_sha256=selection_hash,
                            output_path=json_path.with_name(f"{json_path.stem}_forward_nav.json.gz"),
                            source_path=args.forward_nav_snapshot)
                    forward = _forward_validation(snapshot_date=payload["snapshot_date"], added_codes=added_codes,
                        removed_codes=removed_codes, nav_df=nav_frame)
                    forward["nav_evidence"] = nav_evidence
                    history = _historical_shadow_readiness(audit_dir, project_root=settings.project_root)
                    exposure = build_risk_exposure_comparison(actual_enriched, shadow_enriched)
                    exposure["replacement_cohort"] = build_risk_exposure_comparison(
                        actual_enriched.loc[actual_enriched["基金代码"].isin(removed_codes)],
                        shadow_enriched.loc[shadow_enriched["基金代码"].isin(added_codes)])
                    reasons = ["out_of_sample_cost_and_benchmark_review_not_completed"]
                    if forward["status"] != "ready":
                        reasons.append("required_forward_windows_not_mature")
                    if not history["multi_snapshot_requirement_met"]:
                        reasons.append("insufficient_mature_shadow_snapshots")
                    payload.update(status="computed", passed=True, membership_impact=membership,
                        risk_exposure=exposure, forward_window_readiness=forward, historical_readiness=history,
                        production_switch_blocking_reasons=reasons)
                    payload["shadow_replay"]["final_count"] = len(shadow_enriched)
                    exports["fund_detail"] = scored
                    exports["membership_change"] = scored.loc[scored["membership_change"].isin(["shadow_added", "shadow_removed"])].copy()
                    exports["scenario_members"] = shadow_enriched
        if binding != bind_frozen_report_sources(settings, report):
            raise ValueError("frozen_source_changed_during_replay")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        payload.update(status="frozen_audit_failed", passed=False, error=f"{type(exc).__name__}: {exc}",
            candidate_count=None, scoring_universe_count=None, membership_impact=None, risk_exposure=None,
            ranking_stability=None, duplicate_return_correlation=None,
            replay_alignment={"aligned": None}, shadow_replay={"status": "not_verified", "final_count": None},
            forward_window_readiness={"status": "not_verified", "ready_window_count": None,
                "required_ready_window_count": len(REQUIRED_FORWARD_WINDOWS), "production_switch_supported_now": False},
            production_switch_blocking_reasons=["frozen_audit_failed"])
        exports = {}

    payload["artifacts"] = {}
    for name, frame in exports.items():
        path = json_path.with_name(f"{json_path.stem}_{name}.csv.gz")
        metadata = write_compressed_audit_csv(path, frame)
        payload["artifacts"][name] = {"path": str(path.relative_to(settings.project_root)), **metadata}
    with json_path.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    _write_markdown(payload, json_path.with_suffix(".md"))
    membership_summary = ({key: payload["membership_impact"][key] for key in
        ("baseline_count", "shadow_count", "added_count", "removed_count", "jaccard")}
        if payload["membership_impact"] is not None else None)
    print(json.dumps({"passed": payload["passed"], "status": payload["status"], "run_id": run_id,
        "artifact_path": project_relative_artifact_path(json_path, project_root=settings.project_root),
        "artifact_sha256": file_sha256(json_path),
        "baseline_replay_aligned": payload["replay_alignment"]["aligned"],
        "scenario_status": payload["shadow_replay"]["status"], "membership_impact": membership_summary,
        "production_switch_supported_now": False}, ensure_ascii=False))
    return 0 if payload["passed"] else 2


if __name__ == "__main__":
    _ensure_project_venv()
    raise SystemExit(main())
