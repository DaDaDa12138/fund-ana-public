from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


DATABASE_FRESHNESS_SUMMARY_SCHEMA_VERSION = 2
DATABASE_FRESHNESS_SUMMARY_FILENAME = "latest_database_freshness_summary.json"
SAFE_SQL_IDENTIFIER_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


def _path_cache_token(path: Path | str | None) -> str:
    if not path:
        return "missing"
    try:
        stat = Path(path).stat()
    except OSError:
        return "missing"
    return f"{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


def sqlite_file_fingerprint(db_path: Path | str | None) -> dict[str, Any]:
    if not db_path:
        return {
            "database_path": None,
            "database_file": "missing",
            "wal_file": "missing",
            "fingerprint": "missing",
        }
    base = Path(db_path)
    wal = Path(f"{base}-wal")
    wal_token = "empty_or_missing_wal"
    try:
        wal_stat = wal.stat()
        if int(wal_stat.st_size) > 0:
            wal_token = f"{int(wal_stat.st_mtime_ns)}:{int(wal_stat.st_size)}"
    except OSError:
        pass
    payload = {
        "database_path": str(base),
        "database_file": _path_cache_token(base),
        "wal_file": wal_token,
    }
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    payload["fingerprint"] = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return payload


def database_freshness_summary_path(settings: Any) -> Path:
    reports_dir = Path(getattr(settings, "reports_dir", "reports"))
    return reports_dir / "audit" / "database_freshness" / DATABASE_FRESHNESS_SUMMARY_FILENAME


def _query_scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row else None


def _safe_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    if numerator <= 0 or denominator <= 0:
        return None
    return round(float(numerator) / float(denominator), 6)


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not _table_exists(conn, table_name):
        return False
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except sqlite3.Error:
        return False
    return any(str(row[1] if len(row) > 1 else "").strip() == column_name for row in rows)


def _safe_count(
    conn: sqlite3.Connection,
    table_name: str,
    where_sql: str = "",
    params: tuple[Any, ...] = (),
) -> int:
    if not _table_exists(conn, table_name):
        return 0
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if where_sql:
        sql = f"{sql} {where_sql}"
    return _safe_int(_query_scalar(conn, sql, params))


def _safe_max(conn: sqlite3.Connection, table_name: str, column_name: str, where_sql: str = "") -> Any:
    if not _column_exists(conn, table_name, column_name):
        return None
    sql = f"SELECT MAX({column_name}) FROM {table_name}"
    if where_sql:
        sql = f"{sql} {where_sql}"
    return _query_scalar(conn, sql)


def _load_date_distribution(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    date_column: str,
    where_sql: str = "",
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not _column_exists(conn, table_name, date_column):
        return []
    sql = f"""
        SELECT {date_column} AS business_date, COUNT(*) AS row_count
        FROM {table_name}
        WHERE {date_column} IS NOT NULL AND TRIM({date_column}) <> ''
    """
    if where_sql:
        sql = f"{sql} AND {where_sql}"
    sql = f"{sql} GROUP BY {date_column} ORDER BY {date_column} DESC LIMIT ?"
    try:
        rows = conn.execute(sql, (max(1, int(limit)),)).fetchall()
    except sqlite3.Error:
        return []
    return [
        {
            "business_date": str(row[0] or ""),
            "row_count": _safe_int(row[1] if len(row) > 1 else 0),
        }
        for row in rows
    ]


def _risk_metric_ready_predicate(conn: sqlite3.Connection, alias: str) -> str:
    try:
        from backend.effective_nav import _risk_metric_ready_predicate as effective_nav_ready_predicate

        return effective_nav_ready_predicate(conn, alias)
    except Exception:
        return "1 = 1"


def _latest_risk_rowid_predicate(alias: str = "risk", latest_alias: str = "latest") -> str:
    try:
        from backend.effective_nav import _latest_risk_rowid_predicate as effective_nav_latest_predicate

        return effective_nav_latest_predicate(alias, latest_alias)
    except Exception:
        return f"""
            {alias}.rowid = (
                SELECT {latest_alias}.rowid
                FROM fund_risk_metrics {latest_alias}
                WHERE {latest_alias}.fcode = {alias}.fcode
                  AND {latest_alias}.period = {alias}.period
                ORDER BY COALESCE(TRIM({latest_alias}.end_date), '') DESC, {latest_alias}.rowid DESC
                LIMIT 1
            )
        """


def _load_ready_risk_date_distribution(
    conn: sqlite3.Connection,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not _column_exists(conn, "fund_risk_metrics", "end_date"):
        return []
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    try:
        rows = conn.execute(
            f"""
            SELECT risk.end_date AS business_date, COUNT(DISTINCT risk.fcode) AS fund_count
            FROM fund_risk_metrics risk
            WHERE risk.period = '1y'
              AND risk.end_date IS NOT NULL
              AND TRIM(risk.end_date) <> ''
              AND {risk_ready_predicate}
              AND {latest_rowid_predicate}
            GROUP BY risk.end_date
            ORDER BY risk.end_date DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [
        {
            "business_date": str(row[0] or ""),
            "fund_count": _safe_int(row[1] if len(row) > 1 else 0),
        }
        for row in rows
    ]


def _latest_ready_risk_business_date(conn: sqlite3.Connection) -> str | None:
    distribution = _load_ready_risk_date_distribution(conn, limit=1)
    if not distribution:
        return None
    value = str(distribution[0].get("business_date") or "").strip()
    return value or None


def _count_latest_ready_risk_funds(conn: sqlite3.Connection, business_date: str | None) -> int:
    date_value = str(business_date or "").strip()
    if not date_value or not _table_exists(conn, "fund_risk_metrics"):
        return 0
    risk_ready_predicate = _risk_metric_ready_predicate(conn, "risk")
    latest_rowid_predicate = _latest_risk_rowid_predicate("risk", "latest")
    try:
        row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT risk.fcode)
            FROM fund_risk_metrics risk
            WHERE risk.period = '1y'
              AND COALESCE(TRIM(risk.end_date), '') = ?
              AND {risk_ready_predicate}
              AND {latest_rowid_predicate}
            """,
            (date_value,),
        ).fetchone()
    except sqlite3.Error:
        return 0
    return _safe_int((row or [0])[0])


def _ensure_core_root_on_path(settings: Any) -> None:
    project_root = Path(getattr(settings, "project_root", Path(__file__).resolve().parent.parent))
    core_root = Path(getattr(settings, "core_root", project_root / "core"))
    try:
        resolved = str(core_root.resolve())
    except OSError:
        resolved = str(core_root)
    if resolved not in sys.path:
        sys.path.insert(0, resolved)


def _attach_nav_archive(conn: sqlite3.Connection, settings: Any) -> tuple[dict[str, Any], Any | None]:
    try:
        _ensure_core_root_on_path(settings)
        from database.nav_history_queries import (  # type: ignore
            attach_nav_history_archive_if_available,
            detach_nav_history_archive,
            load_latest_nav_history_archive_binding,
            nav_history_archive_binding_matches_database,
        )

        db_path = Path(getattr(settings, "database_path"))
        project_root = getattr(settings, "project_root", None)
        nav_archive_binding = load_latest_nav_history_archive_binding(project_root=project_root)
        if not nav_history_archive_binding_matches_database(
            nav_archive_binding,
            db_path,
            project_root=project_root,
        ):
            nav_archive_binding = {}
        nav_archive_attach = attach_nav_history_archive_if_available(
            conn,
            archive_path=nav_archive_binding.get("archive_path"),
            archive_table_name=nav_archive_binding.get("archive_table_name") or "fund_nav_history_archive",
            archive_cutoff_date=nav_archive_binding.get("archive_cutoff_date") or "",
        )
        return dict(nav_archive_attach or {}), detach_nav_history_archive
    except Exception:
        return {}, None


def _count_nav_archive_rows(conn: sqlite3.Connection, nav_archive_attach: dict[str, Any]) -> int:
    if not nav_archive_attach.get("attached"):
        return 0
    archive_alias = str(nav_archive_attach.get("alias") or "nav_archive_db").strip()
    archive_table_name = str(nav_archive_attach.get("archive_table_name") or "fund_nav_history_archive").strip()
    if (
        not archive_alias
        or not archive_table_name
        or any(ch not in SAFE_SQL_IDENTIFIER_CHARS for ch in archive_alias)
        or any(ch not in SAFE_SQL_IDENTIFIER_CHARS for ch in archive_table_name)
    ):
        return 0
    archive_cutoff_date = str(nav_archive_attach.get("archive_cutoff_date") or "").strip()
    try:
        if archive_cutoff_date:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {archive_alias}.{archive_table_name} WHERE nav_date < ?",
                (archive_cutoff_date,),
            ).fetchone()
            return _safe_int((row or [0])[0])
        return _safe_int(_query_scalar(conn, f"SELECT COUNT(*) FROM {archive_alias}.{archive_table_name}"))
    except sqlite3.Error:
        return 0


def build_database_freshness_summary(settings: Any) -> dict[str, Any]:
    db_path = Path(getattr(settings, "database_path", ""))
    started_fingerprint = sqlite_file_fingerprint(db_path)
    file_stat = db_path.stat() if db_path.exists() else None
    payload: dict[str, Any] = {
        "schema_version": DATABASE_FRESHNESS_SUMMARY_SCHEMA_VERSION,
        "artifact_type": "database_freshness_summary",
        "computed_at": datetime.now().isoformat(timespec="seconds"),
        "status": "missing_database" if not db_path.exists() else "ok",
        "source_db_fingerprint": started_fingerprint,
        "source_db_changed_during_build": False,
        "overview_fields": {
            "stored_fund_count": 0,
            "latest_nav_snapshot_count": 0,
            "nav_history_hot_record_count": 0,
            "nav_history_archive_record_count": 0,
            "nav_history_effective_record_count": 0,
            "nav_history_record_count": 0,
            "nav_history_archive_bound": False,
            "nav_history_archive_cutoff_date": None,
            "risk_metric_record_count": 0,
            "fund_meta_updated_at": None,
            "nav_data_updated_at": None,
            "risk_data_updated_at": None,
            "latest_nav_business_date": None,
            "risk_1y_business_date": None,
            "risk_1y_raw_business_date": None,
        },
        "nav_latest_distribution": [],
        "risk_1y_distribution": [],
        "risk_1y_raw_distribution": [],
        "rank_latest_distribution": [],
        "rank_history_summary": {
            "rank_history_record_count": 0,
            "latest_rank_business_date": None,
        },
        "coverage": {
            "latest_nav_covered_fund_count": 0,
            "latest_nav_coverage_ratio_vs_funds": None,
            "latest_nav_coverage_ratio_vs_returns": None,
            "risk_1y_latest_fund_count": 0,
            "risk_1y_latest_coverage_ratio_vs_funds": None,
        },
        "database_file": {
            "path": str(db_path),
            "size_bytes": int(file_stat.st_size) if file_stat else 0,
            "updated_at": (
                datetime.fromtimestamp(file_stat.st_mtime).isoformat(timespec="seconds")
                if file_stat
                else None
            ),
        },
    }
    if not db_path.exists():
        return payload

    conn = sqlite3.connect(str(db_path))
    nav_archive_attach: dict[str, Any] = {}
    detach_nav_history_archive_fn: Any | None = None
    try:
        nav_archive_attach, detach_nav_history_archive_fn = _attach_nav_archive(conn, settings)
        stored_fund_count = _safe_count(conn, "funds")
        latest_nav_snapshot_count = _safe_count(conn, "fund_returns")
        nav_history_hot_record_count = _safe_count(conn, "fund_nav_history")
        nav_history_archive_record_count = _count_nav_archive_rows(conn, nav_archive_attach)
        nav_history_effective_record_count = nav_history_hot_record_count + nav_history_archive_record_count
        risk_metric_record_count = _safe_count(conn, "fund_risk_metrics", "WHERE period = '1y'")
        latest_nav_business_date = _safe_max(conn, "fund_returns", "latest_nav_date")
        risk_1y_raw_business_date = _safe_max(conn, "fund_risk_metrics", "end_date", "WHERE period = '1y'")
        risk_1y_business_date = _latest_ready_risk_business_date(conn)
        latest_nav_covered_fund_count = (
            _safe_count(
                conn,
                "fund_returns",
                "WHERE latest_nav_date = ?",
                (latest_nav_business_date,),
            )
            if latest_nav_business_date
            else 0
        )
        risk_1y_latest_fund_count = _count_latest_ready_risk_funds(conn, risk_1y_business_date)

        overview_fields = payload["overview_fields"]
        overview_fields.update(
            {
                "stored_fund_count": stored_fund_count,
                "latest_nav_snapshot_count": latest_nav_snapshot_count,
                "nav_history_hot_record_count": nav_history_hot_record_count,
                "nav_history_archive_record_count": nav_history_archive_record_count,
                "nav_history_effective_record_count": nav_history_effective_record_count,
                "nav_history_record_count": nav_history_effective_record_count,
                "nav_history_archive_bound": bool(nav_archive_attach.get("attached")),
                "nav_history_archive_cutoff_date": (
                    str(nav_archive_attach.get("archive_cutoff_date") or "").strip() or None
                ),
                "risk_metric_record_count": risk_metric_record_count,
                "fund_meta_updated_at": _safe_max(conn, "funds", "last_updated_at"),
                "nav_data_updated_at": _safe_max(conn, "fund_returns", "last_updated_at"),
                "risk_data_updated_at": _safe_max(
                    conn,
                    "fund_risk_metrics",
                    "calculated_at",
                    "WHERE period = '1y'",
                ),
                "latest_nav_business_date": latest_nav_business_date,
                "risk_1y_business_date": risk_1y_business_date,
                "risk_1y_raw_business_date": risk_1y_raw_business_date,
            }
        )
        payload["nav_latest_distribution"] = _load_date_distribution(
            conn,
            table_name="fund_returns",
            date_column="latest_nav_date",
            limit=10,
        )
        payload["risk_1y_distribution"] = _load_ready_risk_date_distribution(
            conn,
            limit=10,
        )
        payload["risk_1y_raw_distribution"] = _load_date_distribution(
            conn,
            table_name="fund_risk_metrics",
            date_column="end_date",
            where_sql="period = '1y'",
            limit=10,
        )
        payload["rank_latest_distribution"] = _load_date_distribution(
            conn,
            table_name="fund_rank_history",
            date_column="rank_date",
            limit=10,
        )
        payload["rank_history_summary"] = {
            "rank_history_record_count": _safe_count(conn, "fund_rank_history"),
            "latest_rank_business_date": _safe_max(conn, "fund_rank_history", "rank_date"),
        }
        payload["coverage"] = {
            "latest_nav_covered_fund_count": latest_nav_covered_fund_count,
            "latest_nav_coverage_ratio_vs_funds": _safe_ratio(
                latest_nav_covered_fund_count,
                stored_fund_count,
            ),
            "latest_nav_coverage_ratio_vs_returns": _safe_ratio(
                latest_nav_covered_fund_count,
                latest_nav_snapshot_count,
            ),
            "risk_1y_latest_fund_count": risk_1y_latest_fund_count,
            "risk_1y_latest_coverage_ratio_vs_funds": _safe_ratio(
                risk_1y_latest_fund_count,
                stored_fund_count,
            ),
        }
    finally:
        if detach_nav_history_archive_fn and nav_archive_attach.get("attached"):
            detach_nav_history_archive_fn(
                conn,
                alias=str(nav_archive_attach.get("alias") or "nav_archive_db"),
            )
        conn.close()

    finished_fingerprint = sqlite_file_fingerprint(db_path)
    payload["source_db_fingerprint"] = finished_fingerprint
    payload["source_db_changed_during_build"] = (
        finished_fingerprint.get("fingerprint") != started_fingerprint.get("fingerprint")
    )
    if payload["source_db_changed_during_build"]:
        payload["status"] = "stale_during_build"
    return payload


def write_database_freshness_summary(settings: Any, payload: dict[str, Any]) -> Path:
    path = database_freshness_summary_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.parent / f"{path.name}.{uuid4().hex}.tmp"
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)
    return path


def load_database_freshness_summary(settings: Any) -> dict[str, Any] | None:
    path = database_freshness_summary_path(settings)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("schema_version") or 0) != DATABASE_FRESHNESS_SUMMARY_SCHEMA_VERSION:
        return None
    return payload


def load_valid_database_freshness_summary(settings: Any) -> dict[str, Any] | None:
    payload = load_database_freshness_summary(settings)
    if not payload:
        return None
    expected = sqlite_file_fingerprint(getattr(settings, "database_path", None))
    actual = payload.get("source_db_fingerprint") or {}
    if actual.get("fingerprint") != expected.get("fingerprint"):
        return None
    return payload


def summarize_database_freshness_payload(
    payload: dict[str, Any] | None,
    *,
    source: str,
    artifact_path: Path | str | None = None,
) -> dict[str, Any]:
    current = dict(payload or {})
    overview_fields = dict(current.get("overview_fields") or {})
    coverage = dict(current.get("coverage") or {})
    rank_summary = dict(current.get("rank_history_summary") or {})
    return {
        "schema_version": current.get("schema_version"),
        "status": current.get("status") or "missing",
        "source": str(source or "").strip() or "unknown",
        "artifact_path": str(artifact_path) if artifact_path else None,
        "computed_at": current.get("computed_at"),
        "source_db_fingerprint": current.get("source_db_fingerprint") or {},
        "source_db_changed_during_build": bool(current.get("source_db_changed_during_build")),
        "latest_nav_business_date": overview_fields.get("latest_nav_business_date"),
        "risk_1y_business_date": overview_fields.get("risk_1y_business_date"),
        "risk_1y_raw_business_date": overview_fields.get("risk_1y_raw_business_date"),
        "latest_rank_business_date": rank_summary.get("latest_rank_business_date"),
        "stored_fund_count": _safe_int(overview_fields.get("stored_fund_count")),
        "latest_nav_snapshot_count": _safe_int(overview_fields.get("latest_nav_snapshot_count")),
        "nav_history_record_count": _safe_int(overview_fields.get("nav_history_record_count")),
        "rank_history_record_count": _safe_int(rank_summary.get("rank_history_record_count")),
        "risk_metric_record_count": _safe_int(overview_fields.get("risk_metric_record_count")),
        "latest_nav_covered_fund_count": _safe_int(coverage.get("latest_nav_covered_fund_count")),
        "latest_nav_coverage_ratio_vs_funds": coverage.get("latest_nav_coverage_ratio_vs_funds"),
        "risk_1y_latest_fund_count": _safe_int(coverage.get("risk_1y_latest_fund_count")),
        "risk_1y_latest_coverage_ratio_vs_funds": coverage.get("risk_1y_latest_coverage_ratio_vs_funds"),
    }
