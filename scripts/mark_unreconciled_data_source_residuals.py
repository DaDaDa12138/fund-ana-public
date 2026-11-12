#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import load_settings  # noqa: E402
from backend.schema_bootstrap import ensure_database_schema  # noqa: E402


DEFAULT_FLAG = "api_unreconciled_historical_residual"


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _load_unresolved_codes(audit_path: Path) -> list[str]:
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    items = payload.get("items") or []
    codes: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("proposed_resolution") or "").strip() != "unresolved_keep_blank":
            continue
        code = _normalize_code(item.get("fcode"))
        if code:
            codes.append(code)
    return sorted(set(codes))


def _apply_flag(database_path: Path, *, codes: list[str], flag: str, dry_run: bool) -> dict[str, Any]:
    conn = sqlite3.connect(str(database_path))
    conn.row_factory = sqlite3.Row
    updated = 0
    already = 0
    missing = 0
    unchanged_non_blank_data_source = 0
    rows: list[dict[str, Any]] = []
    try:
        now_iso = datetime.now().isoformat(timespec="seconds")
        for code in codes:
            row = conn.execute(
                "SELECT fcode, shortname, data_source, data_source_governance_flag FROM funds WHERE fcode = ?",
                (code,),
            ).fetchone()
            if row is None:
                missing += 1
                rows.append({"fcode": code, "status": "missing_in_db"})
                continue
            current_source = str(row["data_source"] or "").strip()
            current_flag = str(row["data_source_governance_flag"] or "").strip()
            if current_flag == flag:
                already += 1
                rows.append(
                    {
                        "fcode": code,
                        "shortname": str(row["shortname"] or "").strip(),
                        "status": "already_flagged",
                        "data_source": current_source,
                        "data_source_governance_flag": current_flag,
                    }
                )
                continue
            if current_source:
                unchanged_non_blank_data_source += 1
            if not dry_run:
                conn.execute(
                    "UPDATE funds SET data_source_governance_flag = ?, last_updated_at = ? WHERE fcode = ?",
                    (flag, now_iso, code),
                )
            updated += 1
            rows.append(
                {
                    "fcode": code,
                    "shortname": str(row["shortname"] or "").strip(),
                    "status": "flagged",
                    "data_source": current_source,
                    "previous_data_source_governance_flag": current_flag,
                    "applied_data_source_governance_flag": flag,
                }
            )
        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return {
        "flag": flag,
        "target_total": len(codes),
        "updated_total": updated,
        "already_flagged_total": already,
        "missing_in_db_total": missing,
        "non_blank_data_source_rows": unchanged_non_blank_data_source,
        "dry_run": dry_run,
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="为 data_source 无法由真实 API 复证的历史残留样本打治理标签"
    )
    parser.add_argument(
        "--audit-path",
        default="reports/audit/blank_data_source_name_audit/latest.json",
        help="空来源专项审计 JSON 路径",
    )
    parser.add_argument(
        "--flag",
        default=DEFAULT_FLAG,
        help="写入 funds.data_source_governance_flag 的治理标签",
    )
    parser.add_argument(
        "--output",
        default="reports/audit/data_source_governance_residuals/latest.json",
        help="执行结果输出路径",
    )
    parser.add_argument("--dry-run", action="store_true", help="只生成审计结果，不写数据库")
    args = parser.parse_args()

    settings = load_settings()
    ensure_database_schema(settings)

    audit_path = (PROJECT_ROOT / args.audit_path).resolve()
    if not audit_path.exists():
        raise FileNotFoundError(f"找不到审计文件: {audit_path}")
    output_path = (PROJECT_ROOT / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    codes = _load_unresolved_codes(audit_path)
    result = _apply_flag(
        settings.database_path,
        codes=codes,
        flag=str(args.flag or "").strip() or DEFAULT_FLAG,
        dry_run=bool(args.dry_run),
    )
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(settings.database_path),
        "audit_path": str(audit_path),
        "summary": {
            "target_total": result["target_total"],
            "updated_total": result["updated_total"],
            "already_flagged_total": result["already_flagged_total"],
            "missing_in_db_total": result["missing_in_db_total"],
            "non_blank_data_source_rows": result["non_blank_data_source_rows"],
            "dry_run": result["dry_run"],
            "flag": result["flag"],
        },
        "rows": result["rows"],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
