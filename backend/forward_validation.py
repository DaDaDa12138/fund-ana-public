from __future__ import annotations

from dataclasses import dataclass
import hashlib
import io
import json
import math
import numpy as np
import sqlite3
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

from backend.config import Settings
from core.utils import report_history_store as history_store
from backend.incremental_review_replay import CONTRACT as INCREMENTAL_CAPTURE_CONTRACT, project_review_input, read_observed_source
from backend.forward_validation_cache import (
    LOGGER,
    WorkbookSourceChanged,
    cache_io_lock,
    copy_verified_workbook,
    file_fingerprint,
    prune_reference_indexes,
    replace_cache_json,
    resolve_workbook_reference,
    workbook_reference,
)
from backend.publication import _resolve_publish_blockers
from backend.publication_transaction import visible_report_paths
from backend.project_formal_groups import (
    ENHANCED_BOND_GROUP_LABEL,
    ENHANCED_STYLE_TAGS,
    FORMAL_GROUP_LABEL,
    FORMAL_SCORE_LABEL,
    STABLE_BOND_GROUP_LABEL,
    formal_group_from_row,
)

CORE_ROOT = Path(__file__).resolve().parent.parent / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from database.nav_history_queries import (  # noqa: E402
    attach_nav_history_archive_if_available,
    build_nav_history_union_source_sql,
    detach_nav_history_archive,
    load_latest_nav_history_archive_binding,
    nav_history_archive_binding_matches_database,
)

FORWARD_VALIDATION_WINDOWS: list[tuple[str, int]] = [
    ("1个月", 30),
    ("3个月", 90),
    ("6个月", 180),
    ("12个月", 365),
]
MAX_FORWARD_VALIDATION_SNAPSHOTS = 24
START_NAV_MAX_LAG_DAYS = 7
END_NAV_MAX_LAG_DAYS = 10
INCREMENTAL_REVIEW_ALERT_PERCENTILE = 25.0
INCREMENTAL_REVIEW_SEVERE_ALERT_PERCENTILE = 10.0
INCREMENTAL_REVIEW_SINGLE_ANNUAL_RELIEF_MIN_SCORE_PERCENTILE = 60.0
INCREMENTAL_REVIEW_SINGLE_ANNUAL_RELIEF_MIN_MDD_PERCENTILE = 60.0
INCREMENTAL_REVIEW_SINGLE_ANNUAL_RELIEF_MIN_VOL_PERCENTILE = 60.0
INCREMENTAL_REVIEW_SINGLE_SCORE_RELIEF_MIN_ANNUAL_PERCENTILE = 30.0
INCREMENTAL_REVIEW_SINGLE_SCORE_RELIEF_MIN_MDD_PERCENTILE = 40.0
INCREMENTAL_REVIEW_SINGLE_SCORE_RELIEF_MIN_VOL_PERCENTILE = 55.0
INCREMENTAL_REVIEW_SINGLE_VOL_RELIEF_MIN_SCORE_PERCENTILE = 70.0
INCREMENTAL_REVIEW_SINGLE_VOL_RELIEF_MIN_ANNUAL_PERCENTILE = 70.0
INCREMENTAL_REVIEW_SINGLE_VOL_RELIEF_MIN_MDD_PERCENTILE = 25.0
INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_SCORE = 67.0
INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_MDD_PERCENTILE = 60.0
INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_VOL_PERCENTILE = 75.0
INCREMENTAL_REVIEW_DOUBLE_ALERT_MAX_SCORE_ALERT_PERCENTILE = 10.0
INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_SCORE_PERCENTILE = 7.5
INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_MDD_PERCENTILE_FLEX = 55.0
INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_VOL_PERCENTILE_FLEX = 74.0
INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS = "轻度跟踪"
INCREMENTAL_REVIEW_TRACK_STATUS = "需持续跟踪"
INCREMENTAL_REVIEW_HEALTHY_STATUS = "当前未见显著拖累"
FORWARD_VALIDATION_CANDIDATE_POOL_DEFINITION = "历史项目二次门禁候选池（正式推荐 + 自动诊断），用于替代未逐期持久化的历史全市场可筛选宇宙。"
EMPTY_NAV_HISTORY_FRAME = pd.DataFrame(columns=["fcode", "nav_date", "unit_nav", "daily_growth"])
FORWARD_VALIDATION_CACHE_FORMAT_VERSION = "v5-history-total-return"
FORWARD_RETURN_CONTRACT = {
    "version": "forward-total-return-2026-10-10.1",
    "source": "fund_nav_history.daily_growth", "raw_unit": "percent",
    "return_formula": "100*(prod(1+daily_growth[1:]/100)-1)",
    "drawdown_formula": "max(1-wealth/running_max(wealth)); wealth starts at 1",
    "volatility_formula": "sample_std(daily_growth[1:]/100)*sqrt(252)*100",
    "frequency": "daily observations", "annualization_factor": 252,
    "missing_policy": "no NAV price fallback; incomplete returns or gaps over 10 days are not ready",
    "verification_boundary": "provider daily returns; independent total-return validation remains required",
}


@dataclass(slots=True)
class ForwardValidationSnapshot:
    run_id: str
    generated_at: str
    snapshot_date: str
    filtered_excel: Path
    watch_excel: Path | None
    final_count: int
    stable_final_count: int
    enhanced_final_count: int


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def _safe_json_load(path: Path, *, source_capture: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    try:
        payload = (json.loads(path.read_text(encoding="utf-8")) if source_capture is None else
                   read_observed_source(path, source_capture, lambda raw: json.loads(raw.decode("utf-8")), role="snapshot_report"))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _forward_validation_cache_dir(settings: Settings) -> Path:
    cache_dir = settings.runs_dir / ".forward_validation_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _forward_validation_cache_key(
    *,
    current_snapshot: ForwardValidationSnapshot,
    history_signature: str,
) -> str:
    payload = json.dumps(
        {
            "format_version": FORWARD_VALIDATION_CACHE_FORMAT_VERSION,
            "snapshot_date": current_snapshot.snapshot_date,
            "current_signature": _build_snapshot_content_signature(current_snapshot),
            "history_signature": history_signature,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _forward_validation_cache_json_path(settings: Settings, cache_key: str) -> Path:
    return _forward_validation_cache_dir(settings) / f"{cache_key}.json"


def _forward_validation_cache_excel_path(settings: Settings, cache_key: str) -> Path:
    return _forward_validation_cache_dir(settings) / f"{cache_key}.xlsx"


def _load_forward_validation_cache(
    settings: Settings,
    *,
    cache_key: str,
) -> dict[str, Any] | None:
    json_path = _forward_validation_cache_json_path(settings, cache_key)
    excel_path = _forward_validation_cache_excel_path(settings, cache_key)
    if not json_path.exists():
        return None
    payload = _safe_json_load(json_path)
    if not payload:
        return None
    if not isinstance(payload.get("summary"), dict):
        return None
    if "excel_reference" in payload:
        # A damaged reference is a miss, even if an older physical cache file
        # still exists. Never combine a new preview with that older workbook.
        excel_path = resolve_workbook_reference(settings, payload["excel_reference"])
        if excel_path is None:
            return None
        payload["excel_fingerprint"] = {
            key: payload["excel_reference"][key] for key in ("sha256", "size_bytes")
        }
    else:
        try:
            payload["excel_fingerprint"] = file_fingerprint(excel_path)
        except OSError:
            return None
    payload["excel_path"] = excel_path
    return payload


def _write_forward_validation_cache(
    settings: Settings,
    *,
    cache_key: str,
    summary: dict[str, Any],
    snapshot_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    source_excel_path: Path,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Path]:
    cache_dir = _forward_validation_cache_dir(settings)
    excel_path = source_excel_path.resolve()
    json_path = _forward_validation_cache_json_path(settings, cache_key)
    artifact_payload = {
        "summary": {**summary, "return_contract": FORWARD_RETURN_CONTRACT},
        "snapshot_rows": _stringify_records(snapshot_df.head(24)),
        "comparison_rows": _stringify_records(comparison_df.head(24)),
        "detail_rows": _stringify_records(detail_df.head(24)),
        "detail_row_count": int(len(detail_df.index)),
        "detail_rows_truncated": bool(len(detail_df.index) > 24),
        "detail_sheet_name": "基金明细",
        "excel_reference": workbook_reference(settings, excel_path),
    }
    if metadata:
        artifact_payload["metadata"] = {**metadata, "nav_input_signature": _forward_nav_input_signature(settings)}
    with cache_io_lock(cache_dir):
        replace_cache_json(json_path, artifact_payload)
        retention = prune_reference_indexes(settings, protected_key=cache_key)
    if retention["removed"] or retention["remaining_above_limit"]:
        LOGGER.info("Forward validation cache indexes: %s", retention)
    return {"excel": excel_path, "json": json_path, "cache_dir": cache_dir}


def _resolve_snapshot_artifact_path(report_path: Path, raw_path: Any) -> Path | None:
    raw_text = str(raw_path or "").strip()
    if not raw_text:
        return None
    candidate = Path(raw_text)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    # Resolve relative project paths for legacy, archived and revision reports.
    for parent in report_path.parents:
        if parent.name == "reports":
            candidate_path = parent.parent / candidate
            if candidate_path.exists() or history_store.available(candidate_path, 'formal') or history_store.available(candidate_path, 'watch'):
                return candidate_path.resolve()
            break
    resolved_parent = report_path.resolve().parent
    revision_fallback = resolved_parent / candidate.name
    if revision_fallback.exists():
        return revision_fallback
    fallback = report_path.parent / candidate.name
    if fallback.exists():
        return fallback.resolve()
    return None


def _safe_date(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    try:
        parsed = pd.to_datetime(text, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _formal_group_from_row(row: dict[str, Any]) -> str:
    return formal_group_from_row(row)


def _stringify_frame(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    safe_df = df.copy().astype(object)
    return safe_df.where(safe_df.notna(), "").astype(str)


def _stringify_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    return _stringify_frame(df).to_dict(orient="records")


def _markdown_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "_暂无数据_"
    available_columns = [column for column in columns if column in df.columns]
    if not available_columns:
        return "_暂无数据_"
    table_df = _stringify_frame(df[available_columns])
    header = "| " + " | ".join(available_columns) + " |"
    separator = "| " + " | ".join(["---"] * len(available_columns)) + " |"
    lines = [header, separator]
    for _, row in table_df.iterrows():
        lines.append("| " + " | ".join(row[column] for column in available_columns) + " |")
    return "\n".join(lines)


def _load_snapshot_records(
    settings: Settings, current_snapshot: ForwardValidationSnapshot, *,
    selection_capture: dict[str, Any] | None = None,
) -> list[ForwardValidationSnapshot]:
    records: list[ForwardValidationSnapshot] = []
    observed_sources: list[dict[str, Any]] = []
    selected_sources: dict[tuple[str, str, str], dict[str, Any]] = {}
    issues: list[dict[str, Any]] = []
    from backend.report_history import mode as history_mode
    strict_history = history_mode(settings) == 'ledger'
    for report_path in visible_report_paths(settings):
        payload = _safe_json_load(report_path, source_capture=observed_sources) if selection_capture is not None else _safe_json_load(report_path)
        if not payload:
            if strict_history:
                raise history_store.HistoryError('historical_forward_report_unreadable:' + str(report_path))
            if selection_capture is not None:
                issues.append({"code": "snapshot_report_unreadable_or_empty", "path": str(report_path)})
            continue
        artifacts = payload.get("artifacts") or {}
        filtered_excel = artifacts.get("filtered_excel")
        if not filtered_excel:
            continue
        snapshot_date = str((((payload.get("database_freshness") or {}).get("effective_nav_snapshot") or {}).get("candidate_date") or "")).strip()
        generated_at = str(payload.get("generated_at") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        if not run_id or not generated_at or not snapshot_date:
            continue
        if (generated_at, run_id) > (current_snapshot.generated_at, current_snapshot.run_id):
            continue
        if snapshot_date > current_snapshot.snapshot_date or _resolve_publish_blockers(payload):
            continue
        filtered_path = _resolve_snapshot_artifact_path(report_path, filtered_excel)
        if filtered_path is None or not history_store.available(filtered_path, 'formal'):
            if strict_history:
                raise history_store.HistoryError('historical_forward_source_unavailable:' + str(report_path))
            if selection_capture is not None:
                issues.append({"code": "selected_snapshot_workbook_unavailable", "path": str(report_path)})
            continue
        history_options=history_store.project_options(filtered_path)
        if history_options and history_options[2]=='ledger' and history_store.view(filtered_path,'formal') is None:
            raise history_store.HistoryError('historical_forward_source_not_imported:'+str(filtered_path))
        watch_excel = artifacts.get("watch_excel")
        watch_path = _resolve_snapshot_artifact_path(report_path, watch_excel) if watch_excel else None
        if strict_history and watch_excel and (watch_path is None or not history_store.available(watch_path,'watch')
                                               or history_store.view(watch_path,'watch') is None):
            raise history_store.HistoryError('historical_forward_candidates_unavailable:' + str(report_path))
        counts = payload.get("counts") or {}
        records.append(
            ForwardValidationSnapshot(
                run_id=run_id,
                generated_at=generated_at,
                snapshot_date=snapshot_date,
                filtered_excel=filtered_path.resolve(),
                watch_excel=watch_path if watch_path and history_store.available(watch_path, 'watch') else None,
                final_count=int(counts.get("final_count") or 0),
                stable_final_count=int(counts.get("stable_bond_final_count") or 0),
                enhanced_final_count=int(counts.get("enhanced_bond_final_count") or 0),
            )
        )
        if selection_capture is not None:
            control = payload.get("publication_control")
            selected_sources[(run_id, generated_at, snapshot_date)] = {
                "report_source": dict(observed_sources[-1]),
                "revision": control.get("revision") if isinstance(control, dict) else None,
            }

    records = [item for item in records if item.run_id != current_snapshot.run_id]
    records.append(current_snapshot)
    deduped: dict[str, ForwardValidationSnapshot] = {}
    for item in records:
        current = deduped.get(item.snapshot_date)
        if current is None or (item.generated_at, item.run_id) > (current.generated_at, current.run_id):
            deduped[item.snapshot_date] = item
    result = sorted(
        deduped.values(),
        key=lambda item: (item.snapshot_date, item.generated_at, item.run_id),
        reverse=True,
    )[:MAX_FORWARD_VALIDATION_SNAPSHOTS]
    if selection_capture is not None:
        selected = [{"run_id": item.run_id, "generated_at": item.generated_at,
                     "snapshot_date": item.snapshot_date, "filtered_excel": str(item.filtered_excel),
                     **selected_sources.get((item.run_id, item.generated_at, item.snapshot_date), {})}
                    for item in result]
        if not result or result[0] != current_snapshot:
            issues.append({"code": "current_snapshot_not_first"})
        selection_capture.update(policy="canonical_reports_at_selection_time_nav_date_dedup_latest",
                                 selected_snapshots=selected, observed_report_sources=observed_sources,
                                 max_snapshots=MAX_FORWARD_VALIDATION_SNAPSHOTS, issues=issues)
    return result


def _merge_detail_columns(main_df: pd.DataFrame, detail_df: pd.DataFrame) -> pd.DataFrame:
    current = main_df.copy()
    if "基金代码" not in current.columns:
        return current
    if detail_df.empty or "基金代码" not in detail_df.columns:
        return current
    detail = detail_df.copy()
    detail["基金代码"] = detail["基金代码"].map(_normalize_code)
    preferred_columns = [FORMAL_GROUP_LABEL, "债基风格标签", "是否含权益/转债增强", "基金名称"]
    merge_columns = [column for column in preferred_columns if column in detail.columns and column not in current.columns]
    if not merge_columns:
        return current
    current["基金代码"] = current["基金代码"].map(_normalize_code)
    merged = current.merge(detail[["基金代码", *merge_columns]], on="基金代码", how="left")
    return merged


def _load_snapshot_csv_frame(path: Path | None, *, read_capture: dict[str, Any] | None = None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    csv_path = path.with_suffix(".csv")
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        if read_capture is None:
            return pd.read_csv(csv_path, dtype=str).fillna("")
        return read_observed_source(
            csv_path, read_capture.setdefault("sources", []),
            lambda raw: pd.read_csv(io.BytesIO(raw), dtype=str).fillna(""), role="formal_csv",
        )
    except Exception:
        return pd.DataFrame()


def _load_formal_frame(path: Path, *, read_capture: dict[str, Any] | None = None) -> pd.DataFrame:
    observations = []
    frame = history_store.read_frame(path, 'formal',
        lambda: _legacy_load_formal_frame(path, read_capture=read_capture), observations=observations)
    if read_capture is not None and observations:
        read_capture.update(status='complete', sources=observations, effective_source=observations[-1])
    return frame


def _load_watch_frame(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    return history_store.read_frame(path, 'watch', lambda: _legacy_load_watch_frame(path))


def _legacy_load_formal_frame(path: Path, *, read_capture: dict[str, Any] | None = None) -> pd.DataFrame:
    if read_capture is not None:
        read_capture.update(status="unreadable", sources=[], effective_source=None)
    current = _load_snapshot_csv_frame(path, read_capture=read_capture) if read_capture is not None else _load_snapshot_csv_frame(path)
    if read_capture is not None and not current.empty:
        read_capture["effective_source"] = dict(read_capture["sources"][-1])
    if current.empty:
        try:
            if read_capture is None:
                main_df = pd.read_excel(path, sheet_name="Sheet1", dtype=str).fillna("")
            else:
                main_df = read_observed_source(
                    path, read_capture["sources"],
                    lambda raw: pd.read_excel(io.BytesIO(raw), sheet_name="Sheet1", dtype=str).fillna(""), role="formal_excel_main",
                )
                read_capture["effective_source"] = dict(read_capture["sources"][-1])
        except Exception:
            return pd.DataFrame()
        try:
            if read_capture is None:
                detail_df = pd.read_excel(path, sheet_name="推荐理由与风格", dtype=str).fillna("")
            else:
                detail_df = read_observed_source(
                    path, read_capture["sources"],
                    lambda raw: pd.read_excel(io.BytesIO(raw), sheet_name="推荐理由与风格", dtype=str).fillna(""), role="formal_excel_detail",
                )
        except Exception:
            detail_df = pd.DataFrame()
        current = _merge_detail_columns(main_df, detail_df)
    if "基金代码" not in current.columns:
        return pd.DataFrame()
    current["基金代码"] = current["基金代码"].map(_normalize_code)
    current[FORMAL_GROUP_LABEL] = [
        _formal_group_from_row(row)
        for row in current.to_dict(orient="records")
    ]
    current["样本池标签"] = "正式推荐整体"
    if read_capture is not None:
        read_capture["status"] = "complete"
    return current.drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)


def _legacy_load_watch_frame(path: Path | None) -> pd.DataFrame:
    if not path or not path.exists():
        return pd.DataFrame()
    current = _load_snapshot_csv_frame(path)
    if current.empty:
        try:
            current = pd.read_excel(path, sheet_name="Sheet1", dtype=str).fillna("")
        except Exception:
            return pd.DataFrame()
    if current.empty or "基金代码" not in current.columns:
        return pd.DataFrame()
    current["基金代码"] = current["基金代码"].map(_normalize_code)
    current[FORMAL_GROUP_LABEL] = [
        _formal_group_from_row(row)
        for row in current.to_dict(orient="records")
    ]
    current["样本池标签"] = "项目候选池"
    return current.drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)


def _load_nav_history_for_codes(
    db_path: Path,
    codes: list[str],
    *,
    start_date: str,
    end_date: str,
    chunk_size: int = 800,
) -> pd.DataFrame:
    if not db_path.exists() or not codes:
        return pd.DataFrame(columns=["fcode", "nav_date", "unit_nav", "daily_growth"])
    conn = sqlite3.connect(str(db_path))
    frames: list[pd.DataFrame] = []
    try:
        nav_archive_binding = load_latest_nav_history_archive_binding()
        if not nav_history_archive_binding_matches_database(nav_archive_binding, db_path):
            nav_archive_binding = {}
        nav_archive_attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=nav_archive_binding.get("archive_path"),
            archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
        )
        hot_columns = {row[1] for row in conn.execute("PRAGMA table_info(fund_nav_history)")}
        growth_present = "daily_growth" in hot_columns
        growth_select = "daily_growth" if growth_present else "NULL AS daily_growth"
        nav_source_sql = build_nav_history_union_source_sql(
            nav_archive_attach,
            columns=("fcode", "nav_date", "unit_nav", "daily_growth") if growth_present else ("fcode", "nav_date", "unit_nav"),
        )
        for index in range(0, len(codes), chunk_size):
            chunk = codes[index:index + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            query = f"""
            SELECT
                fcode,
                nav_date,
                unit_nav,
                {growth_select}
            FROM ({nav_source_sql}) AS nav_history
            WHERE fcode IN ({placeholders})
              AND nav_date >= ?
              AND nav_date <= ?
            ORDER BY fcode, nav_date
            """
            frame = pd.read_sql_query(query, conn, params=[*chunk, start_date, end_date])
            if not frame.empty:
                frames.append(frame)
    finally:
        if "nav_archive_attach" in locals() and nav_archive_attach.get("attached"):
            detach_nav_history_archive(
                conn,
                alias=str(nav_archive_attach.get("alias") or "nav_archive_db"),
            )
        conn.close()

    if not frames:
        return pd.DataFrame(columns=["fcode", "nav_date", "unit_nav", "daily_growth"])
    nav_df = pd.concat(frames, ignore_index=True)
    nav_df["fcode"] = nav_df["fcode"].map(_normalize_code)
    nav_df["nav_date"] = pd.to_datetime(nav_df["nav_date"], errors="coerce")
    nav_df["unit_nav"] = pd.to_numeric(nav_df["unit_nav"], errors="coerce")
    nav_df["daily_growth"] = pd.to_numeric(nav_df["daily_growth"], errors="coerce")
    nav_df = nav_df.dropna(subset=["fcode", "nav_date"])
    return nav_df.sort_values(["fcode", "nav_date"], kind="mergesort").reset_index(drop=True)


def _evaluate_code_forward_metrics(
    code_nav_df: pd.DataFrame,
    *,
    snapshot_date: datetime,
    horizon_days: int,
) -> dict[str, Any]:
    if code_nav_df.empty:
        return {"status": "missing_nav_history"}
    history = code_nav_df
    if not history["nav_date"].is_monotonic_increasing:
        history = history.sort_values("nav_date", kind="mergesort").reset_index(drop=True)
    nav_dates = history["nav_date"].to_numpy(copy=False)
    unit_nav_values = history["unit_nav"].to_numpy(copy=False)
    if len(nav_dates) == 0 or len(unit_nav_values) == 0:
        return {"status": "missing_nav_history"}

    snapshot_ts = np.datetime64(snapshot_date)
    start_idx = int(np.searchsorted(nav_dates, snapshot_ts, side="right") - 1)
    if start_idx < 0:
        return {"status": "start_nav_missing"}

    start_nav_date = pd.Timestamp(nav_dates[start_idx]).to_pydatetime()
    start_gap_days = int((snapshot_date - start_nav_date).days)
    if start_gap_days > START_NAV_MAX_LAG_DAYS:
        return {"status": "start_nav_stale", "start_gap_days": start_gap_days}

    target_date = snapshot_date + timedelta(days=horizon_days)
    target_ts = np.datetime64(target_date)
    end_idx = int(np.searchsorted(nav_dates, target_ts, side="right") - 1)
    if end_idx < 0:
        return {"status": "end_nav_missing", "start_gap_days": start_gap_days}

    end_nav_date = pd.Timestamp(nav_dates[end_idx]).to_pydatetime()
    end_gap_days = int((target_date - end_nav_date).days)
    if end_gap_days > END_NAV_MAX_LAG_DAYS:
        return {
            "status": "future_window_incomplete",
            "start_gap_days": start_gap_days,
            "end_gap_days": end_gap_days,
        }

    start_nav = _safe_float(unit_nav_values[start_idx])
    end_nav = _safe_float(unit_nav_values[end_idx])
    if end_idx <= start_idx:
        return {"status": "window_empty"}
    price_change_pct = ((end_nav / start_nav - 1) * 100
                        if start_nav is not None and end_nav is not None and start_nav > 0 and end_nav > 0 else None)
    common = {"formula_version": FORWARD_RETURN_CONTRACT["version"],
              "return_source": "fund_nav_history.daily_growth",
              "unit_nav_price_change_pct": round(price_change_pct, 4) if price_change_pct is not None else None}
    window = history.iloc[start_idx:end_idx + 1]
    if window["nav_date"].duplicated().any():
        return {"status": "duplicate_nav_dates", **common}
    if window["nav_date"].diff().dt.days.dropna().gt(10).any():
        return {"status": "history_gap", **common}
    if "daily_growth" not in window:
        return {"status": "total_return_missing", **common}
    # The first row's return precedes the investment window. Every later
    # observation is required; dropping an interior null loses real returns.
    daily_returns = pd.to_numeric(window["daily_growth"].iloc[1:], errors="coerce").to_numpy(dtype=float) / 100
    if daily_returns.size == 0 or not np.isfinite(daily_returns).all():
        return {"status": "total_return_missing", **common}
    if (daily_returns <= -1).any():
        return {"status": "invalid_total_return", **common}
    wealth = np.concatenate(([1.0], np.cumprod(1 + daily_returns)))
    if not np.isfinite(wealth).all() or (wealth <= 0).any():
        return {"status": "invalid_total_return", **common}
    return_pct = (wealth[-1] - 1) * 100
    max_drawdown_pct = abs(float((wealth / np.maximum.accumulate(wealth) - 1).min()) * 100)
    volatility_pct = (float(np.std(daily_returns, ddof=1) * math.sqrt(252) * 100)
                      if daily_returns.size >= 2 else None)

    return {
        "status": "ready",
        **common,
        "start_nav_date": start_nav_date.date().isoformat(),
        "end_nav_date": end_nav_date.date().isoformat(),
        "start_gap_days": start_gap_days,
        "end_gap_days": end_gap_days,
        "window_span_days": int((end_nav_date - start_nav_date).days),
        "return_pct": round(return_pct, 4),
        "max_drawdown_pct": None if max_drawdown_pct is None else round(max_drawdown_pct, 4),
        "volatility_pct": None if volatility_pct is None else round(volatility_pct, 4),
    }


def _build_window_metrics_frame(
    codes: list[str],
    nav_map: dict[str, pd.DataFrame],
    *,
    snapshot_date: datetime,
    horizon_days: int,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for code in codes:
        metrics = _evaluate_code_forward_metrics(
            nav_map.get(code, EMPTY_NAV_HISTORY_FRAME),
            snapshot_date=snapshot_date,
            horizon_days=horizon_days,
        )
        records.append({"基金代码": code, **metrics})
    return pd.DataFrame(records)


def _cohort_metrics(
    cohort_label: str,
    codes: list[str],
    window_metrics_df: pd.DataFrame,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    code_order = {
        _normalize_code(code): index
        for index, code in enumerate(codes)
        if _normalize_code(code)
    }
    if code_order and not window_metrics_df.empty and "基金代码" in window_metrics_df.columns:
        metrics_df = window_metrics_df[window_metrics_df["基金代码"].isin(code_order)].copy()
        if not metrics_df.empty:
            metrics_df["_code_order"] = metrics_df["基金代码"].map(code_order)
            metrics_df = metrics_df.sort_values("_code_order", kind="mergesort").drop(columns=["_code_order"])
    else:
        metrics_df = pd.DataFrame(columns=window_metrics_df.columns if not window_metrics_df.empty else ["基金代码", "status"])

    ready_df = metrics_df[metrics_df.get("status", pd.Series(dtype=object)) == "ready"].copy() if not metrics_df.empty else pd.DataFrame()
    total_fund_count = len(codes)
    valid_fund_count = int(len(ready_df.index))
    coverage_ratio_pct = round(valid_fund_count / total_fund_count * 100.0, 2) if total_fund_count > 0 else 0.0

    summary = {
        "return_contract": FORWARD_RETURN_CONTRACT,
        "cohort_label": cohort_label,
        "total_fund_count": total_fund_count,
        "valid_fund_count": valid_fund_count,
        "coverage_ratio_pct": coverage_ratio_pct,
        "avg_return_pct": None,
        "median_return_pct": None,
        "win_rate_pct": None,
        "avg_max_drawdown_pct": None,
        "avg_volatility_pct": None,
        "status": "future_data_pending",
    }
    if not ready_df.empty:
        summary.update(
            {
                "avg_return_pct": round(float(ready_df["return_pct"].mean()), 4),
                "median_return_pct": round(float(ready_df["return_pct"].median()), 4),
                "win_rate_pct": round(float((ready_df["return_pct"] > 0).mean() * 100.0), 2),
                "avg_max_drawdown_pct": round(float(ready_df["max_drawdown_pct"].dropna().mean()), 4) if not ready_df["max_drawdown_pct"].dropna().empty else None,
                "avg_volatility_pct": round(float(ready_df["volatility_pct"].dropna().mean()), 4) if not ready_df["volatility_pct"].dropna().empty else None,
                "status": "ready",
            }
        )
    summary["evaluation_status_counts"] = metrics_df["status"].value_counts().to_dict() if "status" in metrics_df else {}
    return summary, metrics_df.to_dict(orient="records") if not metrics_df.empty else []


def _percentile_rank(
    values: pd.Series,
    current_value: Any,
    *,
    higher_is_better: bool,
) -> float | None:
    numeric_value = _safe_float(current_value)
    if numeric_value is None:
        return None
    numeric_series = pd.to_numeric(values, errors="coerce").dropna()
    if numeric_series.empty:
        return None
    if higher_is_better:
        rank_count = int((numeric_series <= numeric_value).sum())
    else:
        rank_count = int((numeric_series >= numeric_value).sum())
    return round(rank_count / len(numeric_series.index) * 100.0, 2)


def _resolve_incremental_review_status(
    *,
    alerts: list[str],
    formal_group: str,
    formal_score: float | None,
    score_pct: float | None,
    annual_pct: float | None,
    mdd_pct: float | None,
    vol_pct: float | None,
) -> str:
    if not alerts:
        return INCREMENTAL_REVIEW_HEALTHY_STATUS

    percentiles = [value for value in [score_pct, annual_pct, mdd_pct, vol_pct] if value is not None]
    min_pct = min(percentiles) if percentiles else None
    weak_signal_count = len(alerts)

    if weak_signal_count == 1:
        if (
            "评分分位偏后" in alerts
            and (annual_pct is None or annual_pct >= INCREMENTAL_REVIEW_SINGLE_SCORE_RELIEF_MIN_ANNUAL_PERCENTILE)
            and (mdd_pct is None or mdd_pct >= INCREMENTAL_REVIEW_SINGLE_SCORE_RELIEF_MIN_MDD_PERCENTILE)
            and (vol_pct is None or vol_pct >= INCREMENTAL_REVIEW_SINGLE_SCORE_RELIEF_MIN_VOL_PERCENTILE)
        ):
            return INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS
        if (
            "年化分位偏后" in alerts
            and (score_pct is None or score_pct >= INCREMENTAL_REVIEW_SINGLE_ANNUAL_RELIEF_MIN_SCORE_PERCENTILE)
            and (mdd_pct is None or mdd_pct >= INCREMENTAL_REVIEW_SINGLE_ANNUAL_RELIEF_MIN_MDD_PERCENTILE)
            and (vol_pct is None or vol_pct >= INCREMENTAL_REVIEW_SINGLE_ANNUAL_RELIEF_MIN_VOL_PERCENTILE)
        ):
            return INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS
        if (
            "波动安全分位偏后" in alerts
            and (score_pct is None or score_pct >= INCREMENTAL_REVIEW_SINGLE_VOL_RELIEF_MIN_SCORE_PERCENTILE)
            and (annual_pct is None or annual_pct >= INCREMENTAL_REVIEW_SINGLE_VOL_RELIEF_MIN_ANNUAL_PERCENTILE)
            and (mdd_pct is None or mdd_pct >= INCREMENTAL_REVIEW_SINGLE_VOL_RELIEF_MIN_MDD_PERCENTILE)
        ):
            return INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS

    if (
        weak_signal_count == 2
        and set(alerts) == {"评分分位偏后", "年化分位偏后"}
        and formal_group == STABLE_BOND_GROUP_LABEL
        and (formal_score is None or formal_score >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_SCORE)
        and (score_pct is None or score_pct >= INCREMENTAL_REVIEW_DOUBLE_ALERT_MAX_SCORE_ALERT_PERCENTILE)
        and (mdd_pct is None or mdd_pct >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_MDD_PERCENTILE)
        and (vol_pct is None or vol_pct >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_VOL_PERCENTILE)
    ):
        return INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS

    if (
        weak_signal_count == 2
        and set(alerts) == {"评分分位偏后", "年化分位偏后"}
        and formal_group == STABLE_BOND_GROUP_LABEL
        and (formal_score is None or formal_score >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_SCORE)
        and (score_pct is None or score_pct >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_SCORE_PERCENTILE)
        and (mdd_pct is None or mdd_pct >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_MDD_PERCENTILE_FLEX)
        and (vol_pct is None or vol_pct >= INCREMENTAL_REVIEW_DOUBLE_ALERT_RELIEF_MIN_VOL_PERCENTILE_FLEX)
    ):
        return INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS

    if weak_signal_count >= 2:
        return INCREMENTAL_REVIEW_TRACK_STATUS
    if min_pct is not None and min_pct < INCREMENTAL_REVIEW_SEVERE_ALERT_PERCENTILE:
        return INCREMENTAL_REVIEW_TRACK_STATUS
    return INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS


def _incremental_review_status_sort_key(value: Any) -> int:
    text = str(value or "").strip()
    order = {
        INCREMENTAL_REVIEW_TRACK_STATUS: 0,
        INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS: 1,
        INCREMENTAL_REVIEW_HEALTHY_STATUS: 2,
    }
    return order.get(text, 9)


def _build_incremental_review_rows(
    current_formal_df: pd.DataFrame,
    previous_formal_df: pd.DataFrame,
) -> pd.DataFrame:
    if current_formal_df.empty or "基金代码" not in current_formal_df.columns:
        return pd.DataFrame()

    previous_codes = set(previous_formal_df.get("基金代码", pd.Series(dtype=object)).astype(str).tolist())
    current = current_formal_df.copy()
    current["基金代码"] = current["基金代码"].map(_normalize_code)
    review_df = current[~current["基金代码"].isin(previous_codes)].copy()
    if review_df.empty:
        return pd.DataFrame()

    current[FORMAL_SCORE_LABEL] = pd.to_numeric(current.get(FORMAL_SCORE_LABEL), errors="coerce")
    current["1年-年化收益(%)"] = pd.to_numeric(current.get("1年-年化收益(%)"), errors="coerce")
    current["1年-最大回撤(%)"] = pd.to_numeric(current.get("1年-最大回撤(%)"), errors="coerce")
    current["1年-波动率"] = pd.to_numeric(current.get("1年-波动率"), errors="coerce")
    review_df[FORMAL_SCORE_LABEL] = pd.to_numeric(review_df.get(FORMAL_SCORE_LABEL), errors="coerce")
    review_df["1年-年化收益(%)"] = pd.to_numeric(review_df.get("1年-年化收益(%)"), errors="coerce")
    review_df["1年-最大回撤(%)"] = pd.to_numeric(review_df.get("1年-最大回撤(%)"), errors="coerce")
    review_df["1年-波动率"] = pd.to_numeric(review_df.get("1年-波动率"), errors="coerce")

    rows: list[dict[str, Any]] = []
    for row in review_df.to_dict(orient="records"):
        group_label = str(row.get(FORMAL_GROUP_LABEL) or "").strip() or STABLE_BOND_GROUP_LABEL
        group_df = current[current[FORMAL_GROUP_LABEL].astype(str).eq(group_label)].copy()
        if group_df.empty:
            group_df = current.copy()

        score_pct = _percentile_rank(group_df.get(FORMAL_SCORE_LABEL, pd.Series(dtype=float)), row.get(FORMAL_SCORE_LABEL), higher_is_better=True)
        annual_pct = _percentile_rank(group_df.get("1年-年化收益(%)", pd.Series(dtype=float)), row.get("1年-年化收益(%)"), higher_is_better=True)
        mdd_pct = _percentile_rank(group_df.get("1年-最大回撤(%)", pd.Series(dtype=float)), row.get("1年-最大回撤(%)"), higher_is_better=False)
        vol_pct = _percentile_rank(group_df.get("1年-波动率", pd.Series(dtype=float)), row.get("1年-波动率"), higher_is_better=False)

        alerts: list[str] = []
        if score_pct is not None and score_pct < INCREMENTAL_REVIEW_ALERT_PERCENTILE:
            alerts.append("评分分位偏后")
        if annual_pct is not None and annual_pct < INCREMENTAL_REVIEW_ALERT_PERCENTILE:
            alerts.append("年化分位偏后")
        if mdd_pct is not None and mdd_pct < INCREMENTAL_REVIEW_ALERT_PERCENTILE:
            alerts.append("回撤安全分位偏后")
        if vol_pct is not None and vol_pct < INCREMENTAL_REVIEW_ALERT_PERCENTILE:
            alerts.append("波动安全分位偏后")

        rows.append(
            {
                "基金代码": row.get("基金代码"),
                "基金名称": row.get("基金名称"),
                FORMAL_GROUP_LABEL: group_label,
                FORMAL_SCORE_LABEL: row.get(FORMAL_SCORE_LABEL),
                "评分分位(%)": score_pct,
                "1年-年化收益(%)": row.get("1年-年化收益(%)"),
                "年化分位(%)": annual_pct,
                "1年-最大回撤(%)": row.get("1年-最大回撤(%)"),
                "回撤安全分位(%)": mdd_pct,
                "1年-波动率": row.get("1年-波动率"),
                "波动安全分位(%)": vol_pct,
                "持有期状态": row.get("持有期状态", ""),
                "即时体检结论": _resolve_incremental_review_status(
                    alerts=alerts,
                    formal_group=group_label,
                    formal_score=_safe_float(row.get(FORMAL_SCORE_LABEL)),
                    score_pct=score_pct,
                    annual_pct=annual_pct,
                    mdd_pct=mdd_pct,
                    vol_pct=vol_pct,
                ),
                "体检备注": "；".join(alerts),
            }
        )

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["_review_status_sort"] = result["即时体检结论"].map(_incremental_review_status_sort_key)
    result = result.sort_values(
        by=["_review_status_sort", FORMAL_GROUP_LABEL, "评分分位(%)", FORMAL_SCORE_LABEL, "基金代码"],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    )
    return result.drop(columns=["_review_status_sort"], errors="ignore").reset_index(drop=True)


def _build_incremental_quality_review(
    snapshots: list[ForwardValidationSnapshot],
    *,
    preview_top_n: int,
) -> dict[str, Any]:
    summary = {
        "return_contract": FORWARD_RETURN_CONTRACT,
        "available": False,
        "current_run_id": snapshots[0].run_id if snapshots else "",
        "current_snapshot_date": snapshots[0].snapshot_date if snapshots else "",
        "baseline_run_id": "",
        "baseline_snapshot_date": "",
        "new_fund_count": 0,
        "healthy_count": 0,
        "light_track_count": 0,
        "track_needed_count": 0,
        "review_preview": [],
        "blocking_reason": "insufficient_snapshot_history",
    }
    if len(snapshots) < 2:
        return summary

    current_snapshot = snapshots[0]
    baseline_snapshot = snapshots[1]
    current_formal_df = _load_formal_frame(current_snapshot.filtered_excel)
    baseline_formal_df = _load_formal_frame(baseline_snapshot.filtered_excel)
    review_df = _build_incremental_review_rows(current_formal_df, baseline_formal_df)

    summary.update(
        {
            "available": True,
            "baseline_run_id": baseline_snapshot.run_id,
            "baseline_snapshot_date": baseline_snapshot.snapshot_date,
            "new_fund_count": int(len(review_df.index)),
            "healthy_count": int((review_df.get("即时体检结论", pd.Series(dtype=object)) == INCREMENTAL_REVIEW_HEALTHY_STATUS).sum()) if not review_df.empty else 0,
            "track_needed_count": int((review_df.get("即时体检结论", pd.Series(dtype=object)) == INCREMENTAL_REVIEW_TRACK_STATUS).sum()) if not review_df.empty else 0,
            "light_track_count": int((review_df.get("即时体检结论", pd.Series(dtype=object)) == INCREMENTAL_REVIEW_LIGHT_SIGNAL_STATUS).sum()) if not review_df.empty else 0,
            "review_preview": _stringify_records(review_df.head(preview_top_n)),
            "blocking_reason": "",
        }
    )
    return summary


def build_incremental_review_frame(
    settings: Settings,
    *,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
    filtered_excel: Path,
    watch_excel: Path | None,
    final_count: int,
    stable_final_count: int,
    enhanced_final_count: int,
    current_formal_df: pd.DataFrame | None = None,
    input_capture: dict[str, Any] | None = None,
) -> pd.DataFrame:
    current_snapshot = ForwardValidationSnapshot(
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
        filtered_excel=filtered_excel.resolve(),
        watch_excel=watch_excel.resolve() if watch_excel and watch_excel.exists() else None,
        final_count=final_count,
        stable_final_count=stable_final_count,
        enhanced_final_count=enhanced_final_count,
    )
    selection_capture: dict[str, Any] = {}
    snapshots = (_load_snapshot_records(settings, current_snapshot, selection_capture=selection_capture)
                 if input_capture is not None else _load_snapshot_records(settings, current_snapshot))
    if input_capture is not None:
        input_capture.update(schema_version=1, contract=INCREMENTAL_CAPTURE_CONTRACT,
                             run_id=run_id, selection_generated_at=generated_at, snapshot_date=snapshot_date,
                             selection=selection_capture, status="incomplete", previous_snapshot=None,
                             previous_member_count=None,
                             current_source="in_memory_formal_frame" if isinstance(current_formal_df, pd.DataFrame) else "snapshot_file")
        if isinstance(current_formal_df, pd.DataFrame):
            input_capture["current_input"] = project_review_input(current_formal_df)
    if len(snapshots) < 2:
        if input_capture is not None:
            input_capture.update(status="incomplete" if selection_capture.get("issues") else "no_previous_snapshot",
                                 reason="selection_sources_incomplete" if selection_capture.get("issues") else "no_previous_snapshot",
                                 review_result=pd.DataFrame())
        return pd.DataFrame()
    baseline_snapshot = snapshots[1]
    current_df = current_formal_df.copy() if isinstance(current_formal_df, pd.DataFrame) else _load_formal_frame(current_snapshot.filtered_excel)
    previous_frame_read: dict[str, Any] = {}
    baseline_formal_df = (_load_formal_frame(baseline_snapshot.filtered_excel, read_capture=previous_frame_read)
                         if input_capture is not None else _load_formal_frame(baseline_snapshot.filtered_excel))
    result = _build_incremental_review_rows(current_df, baseline_formal_df)
    if input_capture is not None:
        available = not selection_capture.get("issues") and previous_frame_read.get("status") == "complete"
        input_capture.update(status="captured" if available else "incomplete",
                             reason="" if available else "selection_or_previous_frame_unreadable",
                             current_input=project_review_input(current_df),
                             previous_snapshot=selection_capture["selected_snapshots"][1],
                             previous_frame_read=previous_frame_read, review_result=result.copy())
        if "基金代码" in baseline_formal_df:
            input_capture["previous_members"] = baseline_formal_df[["基金代码"]].copy()
            input_capture["previous_member_count"] = len(baseline_formal_df)
    return result


def _select_snapshot_groups(formal_df: pd.DataFrame, watch_df: pd.DataFrame) -> dict[str, list[str]]:
    candidate_pool_df = pd.concat([formal_df, watch_df], ignore_index=True) if not watch_df.empty else formal_df.copy()
    if candidate_pool_df.empty:
        return {
            "正式推荐整体": [],
            "项目候选池": [],
            STABLE_BOND_GROUP_LABEL: [],
            ENHANCED_BOND_GROUP_LABEL: [],
            "纯债候选池": [],
            "增强债候选池": [],
        }

    candidate_pool_df = candidate_pool_df.drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)
    formal_df = formal_df.drop_duplicates(subset=["基金代码"], keep="first").reset_index(drop=True)
    return {
        "正式推荐整体": formal_df["基金代码"].tolist(),
        "项目候选池": candidate_pool_df["基金代码"].tolist(),
        STABLE_BOND_GROUP_LABEL: formal_df[formal_df[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL]["基金代码"].tolist(),
        ENHANCED_BOND_GROUP_LABEL: formal_df[formal_df[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL]["基金代码"].tolist(),
        "纯债候选池": candidate_pool_df[candidate_pool_df[FORMAL_GROUP_LABEL] == STABLE_BOND_GROUP_LABEL]["基金代码"].tolist(),
        "增强债候选池": candidate_pool_df[candidate_pool_df[FORMAL_GROUP_LABEL] == ENHANCED_BOND_GROUP_LABEL]["基金代码"].tolist(),
    }


def _build_snapshot_group_signature(formal_df: pd.DataFrame, watch_df: pd.DataFrame) -> str:
    groups = _select_snapshot_groups(formal_df, watch_df)
    normalized_groups = {
        str(label): sorted({_normalize_code(code) for code in (codes or []) if _normalize_code(code)})
        for label, codes in sorted(groups.items(), key=lambda item: item[0])
    }
    payload = json.dumps(normalized_groups, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _resolve_report_meta_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    return path.with_name(f"{path.stem}_report_meta.json")


def _load_report_meta_content_signature(path: Path | None) -> str:
    meta_path = _resolve_report_meta_path(path)
    if meta_path is None or not meta_path.exists():
        return ""
    payload = _safe_json_load(meta_path)
    return str(payload.get("content_signature") or "").strip()


def _load_report_meta_legacy_signature(
    path: Path | None,
    *,
    snapshot: ForwardValidationSnapshot,
    sample_kind: str,
) -> str:
    meta_path = _resolve_report_meta_path(path)
    if meta_path is None or not meta_path.exists():
        return ""
    payload = _safe_json_load(meta_path)
    if not payload:
        return ""
    counts = payload.get("counts")
    if not isinstance(counts, dict) or not counts:
        return ""
    legacy_payload = json.dumps(
        {
            "sample_kind": sample_kind,
            "snapshot_date": snapshot.snapshot_date,
            "run_id": snapshot.run_id,
            "generated_at": snapshot.generated_at,
            "report_type": str(payload.get("report_type") or "").strip(),
            "output_file": str(payload.get("output_file") or "").strip(),
            "counts": counts,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(legacy_payload.encode("utf-8")).hexdigest()


def _build_snapshot_content_signature_from_meta(snapshot: ForwardValidationSnapshot) -> str:
    formal_signature = _load_report_meta_content_signature(snapshot.filtered_excel)
    if not formal_signature:
        formal_signature = _load_report_meta_legacy_signature(
            snapshot.filtered_excel,
            snapshot=snapshot,
            sample_kind="formal",
        )
    watch_path = snapshot.watch_excel if snapshot.watch_excel and snapshot.watch_excel.exists() else None
    watch_signature = _load_report_meta_content_signature(watch_path)
    if watch_path is not None and not watch_signature:
        watch_signature = _load_report_meta_legacy_signature(
            watch_path,
            snapshot=snapshot,
            sample_kind="watch",
        )
    if not formal_signature:
        return ""
    if watch_path is not None and not watch_signature:
        return ""

    payload = json.dumps(
        {
            "snapshot_date": snapshot.snapshot_date,
            "final_count": int(snapshot.final_count or 0),
            "stable_final_count": int(snapshot.stable_final_count or 0),
            "enhanced_final_count": int(snapshot.enhanced_final_count or 0),
            "filtered_excel_signature": formal_signature,
            "watch_excel_signature": watch_signature,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _build_snapshot_content_signature(snapshot: ForwardValidationSnapshot) -> str:
    meta_signature = _build_snapshot_content_signature_from_meta(snapshot)
    if meta_signature:
        return meta_signature
    formal_df = _load_formal_frame(snapshot.filtered_excel)
    watch_df = _load_watch_frame(snapshot.watch_excel if snapshot.watch_excel and snapshot.watch_excel.exists() else None)
    return _build_snapshot_group_signature(formal_df, watch_df)


def _forward_nav_input_signature(settings: Settings) -> str:
    observations = []
    db_path = getattr(settings, "database_path", None)
    if db_path is None:
        return "database_source_not_supplied"
    for path in (db_path, Path(str(db_path) + "-wal")):
        try:
            stat = path.stat()
            observations.append((str(path), stat.st_ino, stat.st_size, stat.st_mtime_ns))
        except OSError:
            observations.append((str(path), None))
    binding = load_latest_nav_history_archive_binding(project_root=getattr(settings, "project_root", None))
    archive = binding.get("archive_path") if isinstance(binding, dict) else None
    if archive:
        try:
            stat = Path(archive).stat()
            observations.append((str(archive), stat.st_size, stat.st_mtime_ns))
        except OSError:
            observations.append((str(archive), None))
    return hashlib.sha256(json.dumps(observations, sort_keys=True).encode()).hexdigest()


def _build_forward_validation_history_signature(
    settings: Settings,
    *,
    current_snapshot: ForwardValidationSnapshot,
    snapshots: list[ForwardValidationSnapshot] | None = None,
) -> str:
    signature_rows: list[dict[str, Any]] = []
    snapshot_records = snapshots if snapshots is not None else _load_snapshot_records(settings, current_snapshot)
    for snapshot in snapshot_records:
        row = {
            "snapshot_date": snapshot.snapshot_date,
            "content_signature": _build_snapshot_content_signature(snapshot),
        }
        if snapshot.snapshot_date != current_snapshot.snapshot_date:
            row["run_id"] = snapshot.run_id
            row["generated_at"] = snapshot.generated_at
            row["final_count"] = int(snapshot.final_count or 0)
            row["stable_final_count"] = int(snapshot.stable_final_count or 0)
            row["enhanced_final_count"] = int(snapshot.enhanced_final_count or 0)
        signature_rows.append(row)
    payload = json.dumps({"formula_version": FORWARD_RETURN_CONTRACT["version"], "snapshots": signature_rows, "nav_input_signature": _forward_nav_input_signature(settings)}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _forward_validation_history_inputs_unchanged_since_artifact(
    settings: Settings,
    *,
    current_snapshot: ForwardValidationSnapshot,
    artifact_path: Path,
    snapshots: list[ForwardValidationSnapshot] | None = None,
) -> bool:
    try:
        artifact_mtime = artifact_path.stat().st_mtime
    except OSError:
        return False
    snapshot_records = snapshots if snapshots is not None else _load_snapshot_records(settings, current_snapshot)
    for snapshot in snapshot_records:
        for path in [snapshot.filtered_excel, snapshot.watch_excel]:
            if path is None:
                continue
            try:
                if path.stat().st_mtime > artifact_mtime:
                    return False
            except OSError:
                return False
    return True


def _load_forward_validation_payload(path: Path) -> dict[str, Any] | None:
    payload = _safe_json_load(path)
    if not payload:
        return None
    if not isinstance(payload.get("summary"), dict):
        return None
    for key in ["snapshot_rows", "comparison_rows", "detail_rows"]:
        value = payload.get(key)
        if value is None:
            payload[key] = []
        elif not isinstance(value, list):
            return None
    return payload


def _load_forward_validation_excel_frames(
    excel_path: Path | None,
    *,
    load_detail: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    if excel_path is None or not excel_path.exists():
        return None
    try:
        snapshot_df = pd.read_excel(excel_path, sheet_name="快照清单", dtype=str).fillna("")
        comparison_df = pd.read_excel(excel_path, sheet_name="验证摘要", dtype=str).fillna("")
        detail_df = (
            pd.read_excel(excel_path, sheet_name="基金明细", dtype=str).fillna("")
            if load_detail
            else pd.DataFrame()
        )
    except Exception:
        return None
    return snapshot_df, comparison_df, detail_df


def _rebase_reused_forward_validation_payload(
    payload: dict[str, Any],
    *,
    settings: Settings,
    source_run_id: str,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
    final_count: int,
    stable_final_count: int,
    enhanced_final_count: int,
    excel_path: Path | None = None,
    load_detail_from_excel: bool = True,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    loaded_frames = _load_forward_validation_excel_frames(
        excel_path,
        load_detail=load_detail_from_excel,
    )
    if loaded_frames is not None:
        snapshot_df, comparison_df, loaded_detail_df = loaded_frames
        detail_df = loaded_detail_df if load_detail_from_excel else pd.DataFrame(payload.get("detail_rows") or [])
    else:
        snapshot_df = pd.DataFrame(payload.get("snapshot_rows") or [])
        comparison_df = pd.DataFrame(payload.get("comparison_rows") or [])
        detail_df = pd.DataFrame(payload.get("detail_rows") or [])

    if not snapshot_df.empty and "快照日期" in snapshot_df.columns:
        snapshot_mask = snapshot_df["快照日期"].astype(str).eq(snapshot_date)
        if snapshot_mask.any():
            if "运行批次" in snapshot_df.columns:
                snapshot_df.loc[snapshot_mask, "运行批次"] = run_id
            if "生成时间" in snapshot_df.columns:
                snapshot_df.loc[snapshot_mask, "生成时间"] = generated_at
            if "正式推荐数量" in snapshot_df.columns:
                snapshot_df.loc[snapshot_mask, "正式推荐数量"] = int(final_count)
            if "纯债稳健榜数量" in snapshot_df.columns:
                snapshot_df.loc[snapshot_mask, "纯债稳健榜数量"] = int(stable_final_count)
            if "增强债弹性榜数量" in snapshot_df.columns:
                snapshot_df.loc[snapshot_mask, "增强债弹性榜数量"] = int(enhanced_final_count)

    for df in [comparison_df, detail_df]:
        if df.empty or "快照日期" not in df.columns or "运行批次" not in df.columns:
            continue
        row_mask = df["快照日期"].astype(str).eq(snapshot_date)
        if row_mask.any():
            df.loc[row_mask, "运行批次"] = run_id

    source_summary = dict(payload.get("summary") or {})
    source_generated_at = str(source_summary.get("generated_at") or "").strip()
    summary = dict(source_summary)
    summary["generated_at"] = generated_at
    summary["snapshot_total"] = int(summary.get("snapshot_total") or len(snapshot_df.index))
    summary["latest_final_count"] = int(final_count)
    summary["latest_stable_final_count"] = int(stable_final_count)
    summary["latest_enhanced_final_count"] = int(enhanced_final_count)
    if not snapshot_df.empty:
        latest_row = snapshot_df.iloc[0]
        summary["latest_snapshot_date"] = str(latest_row.get("快照日期") or "")
        summary["latest_snapshot_run_id"] = str(latest_row.get("运行批次") or "")
        summary["snapshot_preview"] = _stringify_records(snapshot_df.head(settings.preview_top_n))
    else:
        summary["latest_snapshot_date"] = ""
        summary["latest_snapshot_run_id"] = ""
        summary["snapshot_preview"] = []
    summary["comparison_preview"] = _stringify_records(comparison_df.head(settings.preview_top_n))
    summary["reused_from_run_id"] = source_run_id
    summary["reused_from_generated_at"] = source_generated_at
    return summary, snapshot_df, comparison_df, detail_df


def _write_forward_validation_artifacts(
    *,
    settings: Settings,
    run_id: str,
    summary: dict[str, Any],
    snapshot_df: pd.DataFrame,
    comparison_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    metadata: dict[str, Any] | None = None,
    reused_excel_from: Path | None = None,
    reused_excel_fingerprint: dict[str, Any] | None = None,
    detail_row_count: int | None = None,
    detail_rows_truncated: bool | None = None,
    output_dir: Path | None = None,
) -> dict[str, str]:
    run_dir = output_dir or settings.runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    excel_path = run_dir / f"正式榜前瞻验证_{run_id}.xlsx"
    json_path = run_dir / f"正式榜前瞻验证_{run_id}.json"
    markdown_path = run_dir / f"正式榜前瞻验证_{run_id}.md"
    comparison_preview_df = comparison_df.head(settings.preview_top_n).copy()
    incremental_review = summary.get("incremental_review") or {}
    incremental_review_preview_df = pd.DataFrame(incremental_review.get("review_preview") or [])

    if reused_excel_from is not None:
        copy_verified_workbook(reused_excel_from, excel_path, reused_excel_fingerprint)
        _refresh_reused_forward_validation_excel(
            excel_path=excel_path,
            summary=summary,
            snapshot_date=str(summary.get("latest_snapshot_date") or ""),
            source_run_id=str(summary.get("reused_from_run_id") or ""),
            source_generated_at=str(summary.get("reused_from_generated_at") or ""),
            run_id=run_id,
            generated_at=str(summary.get("generated_at") or ""),
            final_count=int(summary.get("latest_final_count") or 0),
            stable_final_count=int(summary.get("latest_stable_final_count") or 0),
            enhanced_final_count=int(summary.get("latest_enhanced_final_count") or 0),
            incremental_review_preview_df=incremental_review_preview_df,
        )
    else:
        with pd.ExcelWriter(excel_path) as writer:
            snapshot_df.to_excel(writer, sheet_name="快照清单", index=False)
            comparison_df.to_excel(writer, sheet_name="验证摘要", index=False)
            detail_df.to_excel(writer, sheet_name="基金明细", index=False)
            pd.DataFrame(summary.get("window_summaries") or []).to_excel(writer, sheet_name="窗口摘要", index=False)
            incremental_review_preview_df.to_excel(writer, sheet_name="新增样本体检", index=False)

    actual_detail_row_count = int(detail_row_count) if detail_row_count is not None else int(len(detail_df.index))
    detail_preview_rows = _stringify_records(detail_df.head(settings.preview_top_n))
    # A cache preview (24 rows) and a report preview can have different limits.
    # Describe the rows actually emitted, not the source's truncation flag.
    actual_detail_rows_truncated = actual_detail_row_count > len(detail_preview_rows)
    artifact_payload = {
        "summary": {**summary, "return_contract": FORWARD_RETURN_CONTRACT},
        "snapshot_rows": _stringify_records(snapshot_df.head(settings.preview_top_n)),
        "comparison_rows": _stringify_records(comparison_df.head(settings.preview_top_n)),
        "detail_rows": detail_preview_rows,
        "detail_row_count": actual_detail_row_count,
        "detail_rows_truncated": actual_detail_rows_truncated,
        "detail_sheet_name": "基金明细",
    }
    if metadata:
        artifact_payload["metadata"] = {**metadata, "nav_input_signature": _forward_nav_input_signature(settings)}
    json_path.write_text(json.dumps(artifact_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    artifact_paths = {
        "excel": str(excel_path.resolve()),
        "json": str(json_path.resolve()),
        "markdown": str(markdown_path.resolve()),
    }
    markdown_path.write_text(
        _build_forward_validation_markdown(summary, snapshot_df, comparison_preview_df, artifact_paths),
        encoding="utf-8",
    )
    return artifact_paths


def _cache_source_is_published(
    settings: Settings, source_run_id: str, source_generated_at: str,
    *, cache_payload: dict[str, Any], current_snapshot: ForwardValidationSnapshot,
) -> bool:
    for path in visible_report_paths(settings):
        payload = _safe_json_load(path)
        if payload.get("run_id") != source_run_id or _resolve_publish_blockers(payload):
            continue
        report_generated_at = str(payload.get("generated_at") or "").strip()
        if not report_generated_at or (report_generated_at, source_run_id) > (
            current_snapshot.generated_at, current_snapshot.run_id
        ):
            continue
        # The cache records the forward stage time, which precedes report
        # completion. Bind it to the artifact declared by the published report;
        # do not equate those two timestamps or accept another run revision.
        artifacts = payload.get("artifacts") or {}
        source_json = _resolve_snapshot_artifact_path(path, artifacts.get("forward_validation_json"))
        source_excel = _resolve_snapshot_artifact_path(path, artifacts.get("forward_validation_excel"))
        if source_json is None or source_excel is None:
            continue
        source = _safe_json_load(source_json)
        if (source.get("summary") or {}).get("generated_at") != source_generated_at:
            continue
        if any(source.get(key) != cache_payload.get(key)
               for key in ("summary", "detail_row_count", "detail_sheet_name")):
            continue
        if "excel_reference" in cache_payload and source_excel.resolve() != Path(cache_payload["excel_path"]).resolve():
            continue
        return True
    return False


def _try_reuse_forward_validation_package(
    settings: Settings,
    *,
    current_snapshot: ForwardValidationSnapshot,
    snapshots: list[ForwardValidationSnapshot],
) -> dict[str, Any] | None:
    current_signature = _build_snapshot_content_signature(current_snapshot)
    current_history_signature = _build_forward_validation_history_signature(
        settings,
        current_snapshot=current_snapshot,
        snapshots=snapshots,
    )
    cache_key = _forward_validation_cache_key(
        current_snapshot=current_snapshot,
        history_signature=current_history_signature,
    )
    cached = _load_forward_validation_cache(settings, cache_key=cache_key)
    if cached is not None:
        source_run_id = str((cached.get("metadata") or {}).get("source_run_id") or "").strip()
        source_generated_at = str((cached.get("metadata") or {}).get("source_generated_at") or "").strip()
        # Reuse observations, but rebase every presentation to the current run.
        # An as-of replay must never inherit a later run's cached provenance.
        if source_run_id and source_generated_at and _cache_source_is_published(
            settings, source_run_id, source_generated_at,
            cache_payload=cached, current_snapshot=current_snapshot,
        ) and (source_generated_at, source_run_id) <= (
            current_snapshot.generated_at, current_snapshot.run_id
        ):
            cached_summary, snapshot_df, comparison_df, detail_df = _rebase_reused_forward_validation_payload(
                cached, settings=settings, source_run_id=source_run_id,
                run_id=current_snapshot.run_id, generated_at=current_snapshot.generated_at,
                snapshot_date=current_snapshot.snapshot_date, final_count=current_snapshot.final_count,
                stable_final_count=current_snapshot.stable_final_count,
                enhanced_final_count=current_snapshot.enhanced_final_count,
                excel_path=cached.get("excel_path"), load_detail_from_excel=False,
            )
            return {
                "summary": cached_summary,
                "snapshot_df": snapshot_df,
                "comparison_df": comparison_df,
                "detail_df": detail_df,
                "detail_row_count": int(cached.get("detail_row_count") or len(detail_df.index)),
                "detail_rows_truncated": bool(cached.get("detail_rows_truncated")),
                "reused_excel_from": Path(cached.get("excel_path")),
                "reused_excel_fingerprint": cached["excel_fingerprint"],
                "metadata": {
                    "input_signature": _build_snapshot_content_signature(current_snapshot),
                    "history_signature": current_history_signature,
                    "reused_from_run_id": str((cached.get("metadata") or {}).get("source_run_id") or "").strip(),
                    "reused_from_forward_validation_excel": str(cached.get("excel_path") or ""),
                    "reused_from_forward_validation_json": str(cached.get("metadata", {}).get("source_json_path") or ""),
                },
            }
    run_id = current_snapshot.run_id
    generated_at = current_snapshot.generated_at
    snapshot_date = current_snapshot.snapshot_date
    final_count = int(current_snapshot.final_count or 0)
    stable_final_count = int(current_snapshot.stable_final_count or 0)
    enhanced_final_count = int(current_snapshot.enhanced_final_count or 0)

    candidates: list[tuple[str, str, Path, dict[str, Any]]] = []
    for report_path in visible_report_paths(settings):
        payload = _safe_json_load(report_path)
        if not payload:
            continue
        candidate_run_id = str(payload.get("run_id") or "").strip()
        candidate_generated_at = str(payload.get("generated_at") or "").strip()
        candidate_snapshot_date = str((((payload.get("database_freshness") or {}).get("effective_nav_snapshot") or {}).get("candidate_date") or "")).strip()
        if not candidate_run_id or candidate_run_id == run_id or candidate_snapshot_date != snapshot_date:
            continue
        if (candidate_generated_at, candidate_run_id) > (generated_at, run_id) or _resolve_publish_blockers(payload):
            continue
        candidates.append((candidate_generated_at, candidate_run_id, report_path, payload))

    for _, candidate_run_id, report_path, report_payload in sorted(candidates, reverse=True):
        artifacts = report_payload.get("artifacts") or {}
        filtered_path = _resolve_snapshot_artifact_path(report_path, artifacts.get("filtered_excel"))
        if filtered_path is None or not history_store.available(filtered_path, 'formal'):
            continue
        watch_path = _resolve_snapshot_artifact_path(report_path, artifacts.get("watch_excel"))
        forward_validation_excel = _resolve_snapshot_artifact_path(report_path, artifacts.get("forward_validation_excel"))
        forward_validation_json = _resolve_snapshot_artifact_path(report_path, artifacts.get("forward_validation_json"))
        if (
            forward_validation_json is None or not forward_validation_json.exists()
            or forward_validation_excel is None or not forward_validation_excel.exists()
        ):
            continue
        reused_payload = _load_forward_validation_payload(forward_validation_json)
        if reused_payload is None or (reused_payload.get("summary") or {}).get("return_contract") != FORWARD_RETURN_CONTRACT:
            continue
        metadata = reused_payload.get("metadata") if isinstance(reused_payload.get("metadata"), dict) else {}
        if metadata.get("nav_input_signature") != _forward_nav_input_signature(settings):
            continue
        candidate_snapshot = ForwardValidationSnapshot(
            run_id=candidate_run_id,
            generated_at=str(report_payload.get("generated_at") or "").strip(),
            snapshot_date=snapshot_date,
            filtered_excel=filtered_path.resolve(),
            watch_excel=watch_path.resolve() if watch_path and watch_path.exists() else None,
            final_count=int((report_payload.get("counts") or {}).get("final_count") or 0),
            stable_final_count=int((report_payload.get("counts") or {}).get("stable_bond_final_count") or 0),
            enhanced_final_count=int((report_payload.get("counts") or {}).get("enhanced_bond_final_count") or 0),
        )
        candidate_signature = str(metadata.get("input_signature") or "").strip()
        if candidate_signature != current_signature:
            candidate_signature = _build_snapshot_content_signature(candidate_snapshot)
        if candidate_signature != current_signature:
            continue
        candidate_history_signature = str(metadata.get("history_signature") or "").strip()
        if candidate_history_signature != current_history_signature:
            if not _forward_validation_history_inputs_unchanged_since_artifact(
                settings,
                current_snapshot=candidate_snapshot,
                artifact_path=forward_validation_json,
                snapshots=snapshots,
            ):
                continue
            candidate_history_signature = _build_forward_validation_history_signature(
                settings,
                current_snapshot=candidate_snapshot,
                snapshots=snapshots,
            )
        if candidate_history_signature != current_history_signature:
            continue
        try:
            excel_fingerprint = file_fingerprint(forward_validation_excel)
        except OSError:
            continue
        summary, snapshot_df, comparison_df, detail_df = _rebase_reused_forward_validation_payload(
            reused_payload,
            settings=settings,
            source_run_id=candidate_run_id,
            run_id=run_id,
            generated_at=generated_at,
            snapshot_date=snapshot_date,
            final_count=final_count,
            stable_final_count=stable_final_count,
            enhanced_final_count=enhanced_final_count,
            excel_path=forward_validation_excel,
            load_detail_from_excel=False,
        )
        return {
            "summary": {**summary, "return_contract": FORWARD_RETURN_CONTRACT},
            "snapshot_df": snapshot_df,
            "comparison_df": comparison_df,
            "detail_df": detail_df,
            "detail_row_count": int(reused_payload.get("detail_row_count") or len(detail_df.index)),
            "detail_rows_truncated": bool(reused_payload.get("detail_rows_truncated")),
            "reused_excel_from": forward_validation_excel.resolve(),
            "reused_excel_fingerprint": excel_fingerprint,
            "metadata": {
                "input_signature": current_signature,
                "history_signature": current_history_signature,
                "reused_from_run_id": candidate_run_id,
                "reused_from_forward_validation_excel": str(forward_validation_excel.resolve()),
                "reused_from_forward_validation_json": str(forward_validation_json.resolve()),
            },
        }
    return None


def _worksheet_header_map(worksheet: Any) -> dict[str, int]:
    header_map: dict[str, int] = {}
    for cell in worksheet[1]:
        header = str(cell.value or "").strip()
        if header:
            header_map[header] = int(cell.column)
    return header_map


def _update_reused_forward_validation_sheet_rows(
    worksheet: Any,
    *,
    snapshot_date: str,
    updates: dict[str, Any],
) -> None:
    if worksheet.max_row < 2:
        return
    header_map = _worksheet_header_map(worksheet)
    snapshot_date_col = header_map.get("快照日期")
    if snapshot_date_col is None:
        return
    for row_idx in range(2, worksheet.max_row + 1):
        current_snapshot_date = str(worksheet.cell(row=row_idx, column=snapshot_date_col).value or "").strip()
        if current_snapshot_date != snapshot_date:
            continue
        for column_name, value in updates.items():
            column_idx = header_map.get(column_name)
            if column_idx is None:
                continue
            worksheet.cell(row=row_idx, column=column_idx, value=value)


def _replace_forward_validation_sheet_with_dataframe(
    workbook: Any,
    *,
    sheet_name: str,
    dataframe: pd.DataFrame,
) -> None:
    insert_index = len(workbook.sheetnames)
    if sheet_name in workbook.sheetnames:
        insert_index = workbook.sheetnames.index(sheet_name)
        worksheet = workbook[sheet_name]
        workbook.remove(worksheet)
    worksheet = workbook.create_sheet(title=sheet_name, index=insert_index)
    for row in dataframe_to_rows(dataframe.fillna(""), index=False, header=True):
        worksheet.append(list(row))


def _replace_xlsx_text_values(
    excel_path: Path,
    replacements: dict[str, str],
) -> bool:
    normalized_replacements = {
        str(old): str(new)
        for old, new in replacements.items()
        if str(old or "").strip() and str(new or "").strip() and str(old) != str(new)
    }
    if not normalized_replacements:
        return True

    tmp_path = excel_path.with_name(f"{excel_path.name}.tmp")
    replacement_hits = {old: 0 for old in normalized_replacements}
    try:
        with zipfile.ZipFile(excel_path, "r") as source_zip:
            with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as target_zip:
                for item in source_zip.infolist():
                    data = source_zip.read(item.filename)
                    if item.filename.endswith((".xml", ".rels")):
                        try:
                            text = data.decode("utf-8")
                        except UnicodeDecodeError:
                            text = ""
                        if text:
                            for old, new in normalized_replacements.items():
                                replacement_hits[old] += text.count(old)
                                text = text.replace(old, new)
                            data = text.encode("utf-8")
                    target_zip.writestr(item, data)
        if not all(count > 0 for count in replacement_hits.values()):
            tmp_path.unlink(missing_ok=True)
            return False
        tmp_path.replace(excel_path)
        return True
    except Exception:
        tmp_path.unlink(missing_ok=True)
        return False


def _refresh_reused_forward_validation_excel(
    *,
    excel_path: Path,
    summary: dict[str, Any],
    snapshot_date: str,
    source_run_id: str,
    source_generated_at: str,
    run_id: str,
    generated_at: str,
    final_count: int,
    stable_final_count: int,
    enhanced_final_count: int,
    incremental_review_preview_df: pd.DataFrame,
) -> None:
    fast_replaced = _replace_xlsx_text_values(
        excel_path,
        {
            source_run_id: run_id,
            source_generated_at: generated_at,
        },
    )
    if fast_replaced:
        return

    workbook = load_workbook(excel_path)
    if "快照清单" in workbook.sheetnames:
        _update_reused_forward_validation_sheet_rows(
            workbook["快照清单"],
            snapshot_date=snapshot_date,
            updates={
                "运行批次": run_id,
                "生成时间": generated_at,
                "正式推荐数量": final_count,
                "纯债稳健榜数量": stable_final_count,
                "增强债弹性榜数量": enhanced_final_count,
            },
        )
    for sheet_name in ["验证摘要", "基金明细"]:
        if sheet_name not in workbook.sheetnames:
            continue
        _update_reused_forward_validation_sheet_rows(
            workbook[sheet_name],
            snapshot_date=snapshot_date,
            updates={"运行批次": run_id},
        )
    _replace_forward_validation_sheet_with_dataframe(
        workbook,
        sheet_name="窗口摘要",
        dataframe=pd.DataFrame(summary.get("window_summaries") or []),
    )
    _replace_forward_validation_sheet_with_dataframe(
        workbook,
        sheet_name="新增样本体检",
        dataframe=incremental_review_preview_df,
    )
    workbook.save(excel_path)


def _build_forward_validation_markdown(
    summary: dict[str, Any],
    snapshot_df: pd.DataFrame,
    comparison_preview_df: pd.DataFrame,
    artifacts: dict[str, str],
) -> str:
    lines = [
        "# 债券基金正式榜前瞻验证摘要",
        "",
        f"- 生成时间：{summary.get('generated_at', '')}",
        f"- 去重后快照数：**{summary.get('snapshot_total', 0)}**",
        f"- 最新快照日期：**{summary.get('latest_snapshot_date', '') or '暂无'}**",
        f"- 基准口径：{summary.get('candidate_pool_definition', '')}",
        f"- 当前状态：**{summary.get('status_label', '')}**",
    ]
    if summary.get("blocking_reason"):
        lines.append(f"- 当前阻塞原因：`{summary.get('blocking_reason')}`")

    lines.extend(["", "## 窗口摘要", ""])
    window_df = pd.DataFrame(summary.get("window_summaries") or [])
    lines.append(
        _markdown_table(
            window_df,
            ["窗口", "天数", "已就绪快照数", "快照总数", "就绪比例(%)", "最新正式榜有效基金数", "最新候选池有效基金数", "状态"],
        )
    )

    lines.extend(["", "## 快照预览", ""])
    lines.append(
        _markdown_table(
            snapshot_df,
            ["快照日期", "运行批次", "正式推荐数量", "纯债稳健榜数量", "增强债弹性榜数量", "1个月状态", "3个月状态", "6个月状态", "12个月状态"],
        )
    )

    lines.extend(["", "## 最新验证预览", ""])
    lines.append(
        _markdown_table(
            comparison_preview_df,
            [
                "快照日期",
                "评价窗口",
                "样本组",
                "样本总数",
                "有效基金数",
                "覆盖率(%)",
                "平均未来收益率(%)",
                "基准组",
                "基准平均未来收益率(%)",
                "相对基准收益超额(%)",
                "状态",
            ],
        )
    )

    incremental_review = summary.get("incremental_review") or {}
    lines.extend(["", "## 最新新增样本即时体检", ""])
    if not incremental_review.get("available"):
        lines.append(f"- 当前阻塞原因：`{incremental_review.get('blocking_reason') or 'insufficient_snapshot_history'}`")
    else:
        lines.append(
            f"- 相对上一快照 `{incremental_review.get('baseline_run_id') or '暂无'}`，"
            f"本轮新增正式样本 **{incremental_review.get('new_fund_count', 0)}** 只，"
            f"当前未见显著拖累 **{incremental_review.get('healthy_count', 0)}** 只，"
            f"轻度跟踪 **{incremental_review.get('light_track_count', 0)}** 只，"
            f"需持续跟踪 **{incremental_review.get('track_needed_count', 0)}** 只。"
        )
        lines.append(
            _markdown_table(
                pd.DataFrame(incremental_review.get("review_preview") or []),
                [
                    "基金代码",
                    "基金名称",
                    FORMAL_GROUP_LABEL,
                    FORMAL_SCORE_LABEL,
                    "评分分位(%)",
                    "年化分位(%)",
                    "回撤安全分位(%)",
                    "波动安全分位(%)",
                    "即时体检结论",
                    "体检备注",
                ],
            )
        )

    lines.extend(
        [
            "",
            "## 产物",
            "",
            f"- Excel：`{artifacts.get('excel', '')}`",
            f"- JSON：`{artifacts.get('json', '')}`",
            f"- Markdown：`{artifacts.get('markdown', '')}`",
            "",
            "说明：当前前瞻验证只使用本项目历史正式推荐产物、自动诊断池产物和本地 `fund_nav_history`，不引入新外部数据源。",
        ]
    )
    return "\n".join(lines)


def build_forward_validation_package(
    settings: Settings,
    *,
    run_id: str,
    generated_at: str,
    snapshot_date: str,
    filtered_excel: Path,
    watch_excel: Path,
    final_count: int,
    stable_final_count: int,
    enhanced_final_count: int,
) -> dict[str, Any]:
    current_snapshot = ForwardValidationSnapshot(
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
        filtered_excel=filtered_excel.resolve(),
        watch_excel=watch_excel.resolve() if watch_excel.exists() else None,
        final_count=final_count,
        stable_final_count=stable_final_count,
        enhanced_final_count=enhanced_final_count,
    )
    snapshots = _load_snapshot_records(settings, current_snapshot)
    reused = _try_reuse_forward_validation_package(
        settings,
        current_snapshot=current_snapshot,
        snapshots=snapshots,
    )
    if reused is not None:
        incremental_review = _build_incremental_quality_review(
            snapshots,
            preview_top_n=settings.preview_top_n,
        )
        reused_summary = dict(reused.get("summary") or {})
        reused_summary["candidate_pool_definition"] = FORWARD_VALIDATION_CANDIDATE_POOL_DEFINITION
        reused_summary["incremental_review"] = incremental_review
        try:
            artifact_paths = _write_forward_validation_artifacts(
                settings=settings,
                run_id=run_id,
                output_dir=filtered_excel.parent,
                summary=reused_summary,
                snapshot_df=reused.get("snapshot_df") if isinstance(reused.get("snapshot_df"), pd.DataFrame) else pd.DataFrame(),
                comparison_df=reused.get("comparison_df") if isinstance(reused.get("comparison_df"), pd.DataFrame) else pd.DataFrame(),
                detail_df=reused.get("detail_df") if isinstance(reused.get("detail_df"), pd.DataFrame) else pd.DataFrame(),
                metadata=(reused.get("metadata") if isinstance(reused.get("metadata"), dict) else None),
                reused_excel_from=reused.get("reused_excel_from") if isinstance(reused.get("reused_excel_from"), Path) else None,
                reused_excel_fingerprint=reused.get("reused_excel_fingerprint"),
                detail_row_count=(int(reused.get("detail_row_count")) if reused.get("detail_row_count") is not None else None),
                detail_rows_truncated=(bool(reused.get("detail_rows_truncated")) if reused.get("detail_rows_truncated") is not None else None),
            )
        except WorkbookSourceChanged as exc:
            # Recompute all observations from real history; the cache preview
            # is not a replacement for a missing or changed full workbook.
            LOGGER.warning("Forward validation cache source unavailable; recomputing: %s", exc)
        else:
            return {"summary": reused_summary, "artifacts": artifact_paths}

    snapshot_rows: list[dict[str, Any]] = []
    comparison_rows: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    window_summary_counter = {
        label: {
            "窗口": label,
            "天数": days,
            "已就绪快照数": 0,
            "快照总数": 0,
            "就绪比例(%)": 0.0,
            "最新正式榜有效基金数": 0,
            "最新候选池有效基金数": 0,
            "状态": "future_data_pending",
        }
        for label, days in FORWARD_VALIDATION_WINDOWS
    }

    for snapshot in snapshots:
        formal_df = _load_formal_frame(snapshot.filtered_excel)
        watch_df = _load_watch_frame(snapshot.watch_excel)
        groups = _select_snapshot_groups(formal_df, watch_df)
        codes = sorted({code for values in groups.values() for code in values if code})
        snapshot_dt = _safe_date(snapshot.snapshot_date)
        if snapshot_dt is None:
            continue
        nav_df = _load_nav_history_for_codes(
            settings.database_path,
            codes,
            start_date=(snapshot_dt - timedelta(days=START_NAV_MAX_LAG_DAYS)).date().isoformat(),
            end_date=(snapshot_dt + timedelta(days=max(days for _, days in FORWARD_VALIDATION_WINDOWS) + END_NAV_MAX_LAG_DAYS)).date().isoformat(),
        )
        nav_map = {
            code: group.copy()
            for code, group in nav_df.groupby("fcode", sort=False)
        } if not nav_df.empty else {}

        snapshot_row = {
            "快照日期": snapshot.snapshot_date,
            "运行批次": snapshot.run_id,
            "生成时间": snapshot.generated_at,
            "正式推荐数量": snapshot.final_count,
            "纯债稳健榜数量": snapshot.stable_final_count,
            "增强债弹性榜数量": snapshot.enhanced_final_count,
        }

        for window_label, horizon_days in FORWARD_VALIDATION_WINDOWS:
            window_summary_counter[window_label]["快照总数"] += 1
            window_metrics_df = _build_window_metrics_frame(
                codes,
                nav_map,
                snapshot_date=snapshot_dt,
                horizon_days=horizon_days,
            )
            formal_summary, formal_detail = _cohort_metrics(
                "正式推荐整体",
                groups.get("正式推荐整体") or [],
                window_metrics_df,
            )
            pool_summary, pool_detail = _cohort_metrics(
                "项目候选池",
                groups.get("项目候选池") or [],
                window_metrics_df,
            )
            stable_summary, stable_detail = _cohort_metrics(
                STABLE_BOND_GROUP_LABEL,
                groups.get(STABLE_BOND_GROUP_LABEL) or [],
                window_metrics_df,
            )
            stable_pool_summary, stable_pool_detail = _cohort_metrics(
                "纯债候选池",
                groups.get("纯债候选池") or [],
                window_metrics_df,
            )
            enhanced_summary, enhanced_detail = _cohort_metrics(
                ENHANCED_BOND_GROUP_LABEL,
                groups.get(ENHANCED_BOND_GROUP_LABEL) or [],
                window_metrics_df,
            )
            enhanced_pool_summary, enhanced_pool_detail = _cohort_metrics(
                "增强债候选池",
                groups.get("增强债候选池") or [],
                window_metrics_df,
            )

            bundle = [
                (formal_summary, pool_summary, "正式推荐整体", "项目候选池", formal_detail),
                (stable_summary, stable_pool_summary, STABLE_BOND_GROUP_LABEL, "纯债候选池", stable_detail),
                (enhanced_summary, enhanced_pool_summary, ENHANCED_BOND_GROUP_LABEL, "增强债候选池", enhanced_detail),
            ]
            window_status = "future_data_pending"
            if formal_summary.get("valid_fund_count", 0) > 0:
                window_summary_counter[window_label]["已就绪快照数"] += 1
                window_summary_counter[window_label]["状态"] = "ready"
                window_status = "ready"
            if snapshot == snapshots[0]:
                window_summary_counter[window_label]["最新正式榜有效基金数"] = int(formal_summary.get("valid_fund_count") or 0)
                window_summary_counter[window_label]["最新候选池有效基金数"] = int(pool_summary.get("valid_fund_count") or 0)

            snapshot_row[f"{window_label}状态"] = "可评估" if window_status == "ready" else "窗口或总回报输入未就绪"

            for sample_summary, benchmark_summary, sample_label, benchmark_label, sample_detail in bundle:
                benchmark_return = benchmark_summary.get("avg_return_pct")
                sample_return = sample_summary.get("avg_return_pct")
                benchmark_drawdown = benchmark_summary.get("avg_max_drawdown_pct")
                sample_drawdown = sample_summary.get("avg_max_drawdown_pct")
                comparison_rows.append(
                    {
                        "快照日期": snapshot.snapshot_date,
                        "运行批次": snapshot.run_id,
                        "评价窗口": window_label,
                        "样本组": sample_label,
                        "基准组": benchmark_label,
                        "样本总数": int(sample_summary.get("total_fund_count") or 0),
                        "有效基金数": int(sample_summary.get("valid_fund_count") or 0),
                        "覆盖率(%)": sample_summary.get("coverage_ratio_pct"),
                        "平均未来收益率(%)": sample_return,
                        "中位未来收益率(%)": sample_summary.get("median_return_pct"),
                        "正收益占比(%)": sample_summary.get("win_rate_pct"),
                        "平均未来最大回撤(%)": sample_summary.get("avg_max_drawdown_pct"),
                        "平均未来波动率(%)": sample_summary.get("avg_volatility_pct"),
                        "基准有效基金数": int(benchmark_summary.get("valid_fund_count") or 0),
                        "基准平均未来收益率(%)": benchmark_return,
                        "基准平均未来最大回撤(%)": benchmark_drawdown,
                        "相对基准收益超额(%)": None if sample_return is None or benchmark_return is None else round(float(sample_return) - float(benchmark_return), 4),
                        "相对基准回撤差(%)": None if sample_drawdown is None or benchmark_drawdown is None else round(float(sample_drawdown) - float(benchmark_drawdown), 4),
                        "状态": sample_summary.get("status"),
                        "不可评价原因统计": json.dumps(sample_summary.get("evaluation_status_counts") or {}, ensure_ascii=False, sort_keys=True),
                        "收益公式版本": FORWARD_RETURN_CONTRACT["version"],
                    }
                )
                for item in sample_detail:
                    detail_rows.append(
                        {
                            "快照日期": snapshot.snapshot_date,
                            "运行批次": snapshot.run_id,
                            "评价窗口": window_label,
                            "样本组": sample_label,
                            **item,
                        }
                    )

        snapshot_rows.append(snapshot_row)

    for item in window_summary_counter.values():
        total = int(item.get("快照总数") or 0)
        ready = int(item.get("已就绪快照数") or 0)
        item["就绪比例(%)"] = round(ready / total * 100.0, 2) if total > 0 else 0.0

    snapshot_df = pd.DataFrame(snapshot_rows)
    comparison_df = pd.DataFrame(comparison_rows)
    detail_df = pd.DataFrame(detail_rows)
    comparison_preview_df = comparison_df.head(settings.preview_top_n).copy()

    ready_windows = [item for item in window_summary_counter.values() if item.get("已就绪快照数", 0) > 0]
    status_label = "可滚动评估" if ready_windows else "样本积累中"
    blocking_reason = "" if ready_windows else "historical_forward_window_not_ready"

    summary = {
        "return_contract": FORWARD_RETURN_CONTRACT,
        "available": True,
        "generated_at": generated_at,
        "snapshot_total": int(len(snapshot_df.index)),
        "latest_snapshot_date": str(snapshot_df.iloc[0]["快照日期"]) if not snapshot_df.empty else "",
        "latest_snapshot_run_id": str(snapshot_df.iloc[0]["运行批次"]) if not snapshot_df.empty else "",
        "latest_final_count": int(final_count),
        "latest_stable_final_count": int(stable_final_count),
        "latest_enhanced_final_count": int(enhanced_final_count),
        "candidate_pool_definition": FORWARD_VALIDATION_CANDIDATE_POOL_DEFINITION,
        "status_label": status_label,
        "blocking_reason": blocking_reason,
        "window_summaries": list(window_summary_counter.values()),
        "snapshot_preview": _stringify_records(snapshot_df.head(settings.preview_top_n)),
        "comparison_preview": _stringify_records(comparison_preview_df),
        "incremental_review": _build_incremental_quality_review(
            snapshots,
            preview_top_n=settings.preview_top_n,
        ),
    }
    current_snapshot = ForwardValidationSnapshot(
        run_id=run_id,
        generated_at=generated_at,
        snapshot_date=snapshot_date,
        filtered_excel=filtered_excel.resolve(),
        watch_excel=watch_excel.resolve() if watch_excel.exists() else None,
        final_count=final_count,
        stable_final_count=stable_final_count,
        enhanced_final_count=enhanced_final_count,
    )
    input_signature = _build_snapshot_content_signature(current_snapshot)
    history_signature = _build_forward_validation_history_signature(
        settings,
        current_snapshot=current_snapshot,
    )
    artifact_paths = _write_forward_validation_artifacts(
        settings=settings,
        run_id=run_id,
        output_dir=filtered_excel.parent,
        summary=summary,
        snapshot_df=snapshot_df,
        comparison_df=comparison_df,
        detail_df=detail_df,
        metadata={
            "input_signature": input_signature,
            "history_signature": history_signature,
        },
    )
    _write_forward_validation_cache(
        settings,
        cache_key=_forward_validation_cache_key(
            current_snapshot=current_snapshot,
            history_signature=history_signature,
        ),
        summary=summary,
        snapshot_df=snapshot_df,
        comparison_df=comparison_df,
        detail_df=detail_df,
        source_excel_path=Path(artifact_paths["excel"]),
        metadata={
            "source_run_id": run_id,
            "source_generated_at": generated_at,
            "source_json_path": str(artifact_paths["json"]),
        },
    )

    return {
        "summary": {**summary, "return_contract": FORWARD_RETURN_CONTRACT},
        "artifacts": artifact_paths,
    }
