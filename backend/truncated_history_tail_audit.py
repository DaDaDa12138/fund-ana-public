from __future__ import annotations

import json
import math
import sys
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.config import Settings
from scripts.audit_runtime_utils import build_audit_artifact_path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from api.client import APIClient  # noqa: E402
from api.endpoints import FundAPIEndpoints  # noqa: E402
from utils.tiantian_data_quality import inspect_tiantian_nav_history_payload  # noqa: E402


AUDIT_SAMPLE_LIMIT = 50
DEFAULT_PAGE_SIZE = 100
DEFAULT_AUDIT_FRESHNESS_DAYS = 30


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _build_api(settings: Settings) -> FundAPIEndpoints:
    config = {
        "api": {
            "tiantian": {
                "base_url": settings.tiantian_base_url,
                "retry": {
                    "max_attempts": 1,
                    "timeout": 5,
                },
            }
        }
    }
    client = APIClient(config)
    return FundAPIEndpoints(client)


def _parse_iso_date(value: Any):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _read_truncated_history_candidates(db_path: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            WITH nav_stats AS (
                SELECT
                    fcode,
                    COUNT(*) AS nav_count,
                    MIN(nav_date) AS first_nav_date,
                    MAX(nav_date) AS last_nav_date
                FROM fund_nav_history
                GROUP BY fcode
            )
            SELECT
                f.fcode,
                COALESCE(f.shortname, ''),
                COALESCE(f.ftype, ''),
                f.estab_date,
                COALESCE(ns.nav_count, 0),
                ns.first_nav_date,
                ns.last_nav_date
            FROM funds f
            LEFT JOIN nav_stats ns ON ns.fcode = f.fcode
            WHERE f.type_code IN ('31', '32')
            ORDER BY f.fcode
            """
        ).fetchall()
    finally:
        conn.close()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        code = _normalize_code(row[0])
        if not code:
            continue
        estab_date = str(row[3] or "").strip() or None
        nav_count = _safe_int(row[4], default=0)
        first_nav_date = str(row[5] or "").strip() or None
        last_nav_date = str(row[6] or "").strip() or None
        if nav_count <= 0 or not estab_date or not first_nav_date:
            continue
        estab_dt = _parse_iso_date(estab_date)
        first_dt = _parse_iso_date(first_nav_date)
        if estab_dt is None or first_dt is None:
            continue
        estab_gap_days = (first_dt - estab_dt).days
        if estab_gap_days <= 365:
            continue
        candidates.append(
            {
                "fcode": code,
                "shortname": row[1],
                "ftype": row[2],
                "estab_date": estab_date,
                "nav_count": nav_count,
                "first_nav_date": first_nav_date,
                "last_nav_date": last_nav_date,
                "estab_to_first_nav_gap_days": estab_gap_days,
            }
        )
    return candidates


def _resolve_target_candidates(
    settings: Settings,
    *,
    limit: int,
    target_codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    truncated_candidates = _read_truncated_history_candidates(settings.database_path)
    if target_codes:
        normalized_targets = {_normalize_code(code) for code in target_codes if _normalize_code(code)}
        truncated_candidates = [row for row in truncated_candidates if row.get("fcode") in normalized_targets]
    truncated_candidates.sort(
        key=lambda row: (
            -_safe_int(row.get("estab_to_first_nav_gap_days"), default=-1),
            str(row.get("fcode") or ""),
        )
    )
    if limit > 0:
        return truncated_candidates[:limit]
    return truncated_candidates


def classify_truncated_history_tail(
    *,
    local_first_nav_date: str | None,
    oldest_upstream_nav_date: str | None,
    estab_gap_days: int | None,
    request_status: str,
) -> str:
    if request_status != "ok":
        return "request_failed"

    local_first = _parse_iso_date(local_first_nav_date)
    upstream_oldest = _parse_iso_date(oldest_upstream_nav_date)
    if upstream_oldest is None:
        return "empty_upstream_history"
    if local_first is None:
        return "local_first_nav_missing"
    if upstream_oldest < local_first:
        return "local_collection_gap_suspected"
    if upstream_oldest > local_first:
        return "local_history_ahead_of_current_upstream"
    if _safe_int(estab_gap_days, default=0) > 365:
        return "confirmed_upstream_history_limited"
    return "not_truncated_by_upstream_probe"


def _probe_upstream_oldest_nav(
    api: FundAPIEndpoints,
    *,
    fcode: str,
    page_size: int,
) -> dict[str, Any]:
    first_page_raw = api.get_nav_history(fcode, page_index=1, page_size=page_size)
    first_page_records, first_page_quality = inspect_tiantian_nav_history_payload(
        first_page_raw,
        endpoint_label=f"/fundMNHisNetList FCODE={fcode} page=1",
    )
    if not first_page_quality.accepted:
        return {
            "request_status": first_page_quality.status,
            "request_message": first_page_quality.message,
            "page_size": page_size,
        }

    total_count = max(
        _safe_int((first_page_raw or {}).get("TotalCount"), default=0),
        len(first_page_records),
    )
    page_count = max(1, math.ceil(total_count / max(1, page_size)))
    last_page_raw = api.get_nav_history(fcode, page_index=page_count, page_size=page_size)
    last_page_records, last_page_quality = inspect_tiantian_nav_history_payload(
        last_page_raw,
        endpoint_label=f"/fundMNHisNetList FCODE={fcode} page={page_count}",
    )
    if not last_page_quality.accepted:
        return {
            "request_status": last_page_quality.status,
            "request_message": last_page_quality.message,
            "page_size": page_size,
            "total_count": total_count,
            "page_count": page_count,
        }

    newest_upstream_nav_date = (
        str(first_page_records[0].get("FSRQ") or "").strip() if first_page_records else None
    )
    oldest_upstream_nav_date = (
        str(last_page_records[-1].get("FSRQ") or "").strip() if last_page_records else None
    )
    return {
        "request_status": "ok",
        "request_message": "ok",
        "page_size": page_size,
        "total_count": total_count,
        "page_count": page_count,
        "first_page_count": len(first_page_records),
        "last_page_count": len(last_page_records),
        "newest_upstream_nav_date": newest_upstream_nav_date or None,
        "last_page_first_nav_date": (
            str(last_page_records[0].get("FSRQ") or "").strip() if last_page_records else None
        ),
        "oldest_upstream_nav_date": oldest_upstream_nav_date or None,
    }


def build_truncated_history_tail_audit(
    settings: Settings,
    *,
    limit: int = 10,
    target_codes: list[str] | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    log=lambda _message: None,
) -> dict[str, Any]:
    generated_at = datetime.now().isoformat(timespec="seconds")
    candidates = _resolve_target_candidates(
        settings,
        limit=max(0, int(limit or 0)),
        target_codes=target_codes,
    )
    api = _build_api(settings)

    result_rows: list[dict[str, Any]] = []
    classification_counts: dict[str, int] = {}
    for row in candidates:
        code = _normalize_code(row.get("fcode"))
        log(f"审计 truncated_history 尾部样本: {code}")
        probe = _probe_upstream_oldest_nav(
            api,
            fcode=code,
            page_size=max(1, int(page_size or DEFAULT_PAGE_SIZE)),
        )
        classification = classify_truncated_history_tail(
            local_first_nav_date=row.get("first_nav_date"),
            oldest_upstream_nav_date=probe.get("oldest_upstream_nav_date"),
            estab_gap_days=row.get("estab_to_first_nav_gap_days"),
            request_status=str(probe.get("request_status") or ""),
        )
        classification_counts[classification] = classification_counts.get(classification, 0) + 1

        estab_date = str(row.get("estab_date") or "").strip() or None
        oldest_upstream_nav_date = str(probe.get("oldest_upstream_nav_date") or "").strip() or None
        estab_to_upstream_oldest_gap_days = None
        estab_dt = _parse_iso_date(estab_date)
        oldest_dt = _parse_iso_date(oldest_upstream_nav_date)
        if estab_dt and oldest_dt:
            estab_to_upstream_oldest_gap_days = (oldest_dt - estab_dt).days

        local_first_nav_date = str(row.get("first_nav_date") or "").strip() or None
        local_dt = _parse_iso_date(local_first_nav_date)
        local_vs_upstream_oldest_gap_days = None
        if local_dt and oldest_dt:
            local_vs_upstream_oldest_gap_days = (local_dt - oldest_dt).days

        result_rows.append(
            {
                "fcode": code,
                "shortname": row.get("shortname"),
                "ftype": row.get("ftype"),
                "estab_date": estab_date,
                "local_first_nav_date": local_first_nav_date,
                "local_last_nav_date": row.get("last_nav_date"),
                "local_nav_count": int(row.get("nav_count") or 0),
                "local_estab_to_first_nav_gap_days": row.get("estab_to_first_nav_gap_days"),
                "upstream_probe": probe,
                "estab_to_upstream_oldest_gap_days": estab_to_upstream_oldest_gap_days,
                "local_vs_upstream_oldest_gap_days": local_vs_upstream_oldest_gap_days,
                "oldest_upstream_matches_local_first": (
                    bool(oldest_upstream_nav_date and local_first_nav_date and oldest_upstream_nav_date == local_first_nav_date)
                ),
                "classification": classification,
            }
        )

    payload = {
        "generated_at": generated_at,
        "tiantian_base_url": settings.tiantian_base_url,
        "page_size": max(1, int(page_size or DEFAULT_PAGE_SIZE)),
        "requested_target_codes": [_normalize_code(code) for code in (target_codes or []) if _normalize_code(code)],
        "audited_candidate_total": len(result_rows),
        "results_total": len(result_rows),
        "classification_counts": classification_counts,
        "confirmed_upstream_history_limited_total": classification_counts.get("confirmed_upstream_history_limited", 0),
        "local_collection_gap_suspected_total": classification_counts.get("local_collection_gap_suspected", 0),
        "request_failed_total": classification_counts.get("request_failed", 0),
        "results": result_rows,
        "samples": result_rows[:AUDIT_SAMPLE_LIMIT],
    }

    artifact_dir = settings.reports_dir / "audit" / "truncated_history_tail_audit"
    artifact_path = build_audit_artifact_path(artifact_dir, "truncated_history_tail_audit")
    _write_json(artifact_path, payload)
    payload["artifact_path"] = str(artifact_path)
    return payload


def load_latest_truncated_history_tail_audit_payload(settings: Settings) -> tuple[Path | None, dict[str, Any] | None]:
    artifact_dir = settings.reports_dir / "audit" / "truncated_history_tail_audit"
    if not artifact_dir.exists():
        return None, None
    files = sorted(artifact_dir.glob("truncated_history_tail_audit_*.json"), reverse=True)
    if not files:
        return None, None
    path = files[0]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return path, None
    return path, payload if isinstance(payload, dict) else None


def _resolve_audit_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("results")
    if not isinstance(rows, list):
        rows = payload.get("samples")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def resolve_confirmed_upstream_limited_codes(
    settings: Settings,
    *,
    freshness_days: int = DEFAULT_AUDIT_FRESHNESS_DAYS,
) -> set[str]:
    path, payload = load_latest_truncated_history_tail_audit_payload(settings)
    if not isinstance(payload, dict):
        return set()

    generated_at = _parse_iso_date(str(payload.get("generated_at") or "")[:10])
    freshness_reference = generated_at
    try:
        if path is not None and path.exists():
            file_date = datetime.fromtimestamp(path.stat().st_mtime).date()
            if freshness_reference is None or file_date > freshness_reference:
                freshness_reference = file_date
    except Exception:
        pass

    if freshness_days > 0 and freshness_reference is not None:
        age_days = (datetime.now().date() - freshness_reference).days
        if age_days > max(0, int(freshness_days or 0)):
            return set()

    confirmed_codes = set()
    for row in _resolve_audit_rows(payload):
        code = _normalize_code(row.get("fcode"))
        classification = str(row.get("classification") or "").strip()
        if code and classification == "confirmed_upstream_history_limited":
            confirmed_codes.add(code)
    return confirmed_codes
