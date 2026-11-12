#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
if VENV_PYTHON.exists() and Path(sys.executable) != VENV_PYTHON:
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), *sys.argv])

CORE_ROOT = PROJECT_ROOT / "core"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from api.client import APIClient  # type: ignore # noqa: E402
from api.endpoints import FundAPIEndpoints  # type: ignore # noqa: E402
from backend.config import load_settings  # noqa: E402
from database.connection import get_db_manager  # type: ignore # noqa: E402
from database.models import Fund  # type: ignore # noqa: E402
from sqlalchemy import func, or_  # type: ignore # noqa: E402


@dataclass(slots=True)
class BlankFundRow:
    fcode: str
    shortname: str


def _normalize_code(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text.isdigit() else text


def _normalize_name(value: Any) -> str:
    return str(value or "").strip()


def _select_exact_search_match(search_results: list[dict[str, Any]] | None, fcode: str) -> dict[str, Any] | None:
    target_code = _normalize_code(fcode)
    if not target_code:
        return None
    for item in search_results or []:
        if not isinstance(item, dict):
            continue
        base_info = item.get("FundBaseInfo") or {}
        candidate_codes = (
            base_info.get("FCODE"),
            item.get("FCODE"),
            item.get("CODE"),
            item.get("_id"),
        )
        if any(_normalize_code(code) == target_code for code in candidate_codes if code):
            return item
    return None


def _extract_search_base_info(search_result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(search_result, dict):
        return None
    if "FundBaseInfo" not in search_result:
        return search_result
    base_info = search_result.get("FundBaseInfo")
    if isinstance(base_info, dict) and base_info:
        return base_info
    return None


def _load_blank_source_funds(db_manager, *, limit: int | None = None) -> list[BlankFundRow]:
    with db_manager.get_session(readonly=True, task_name="backfill_data_source_scan") as session:
        query = (
            session.query(Fund.fcode, Fund.shortname)
            .filter(or_(Fund.data_source.is_(None), func.trim(Fund.data_source) == ""))
            .order_by(Fund.first_collected_at.asc(), Fund.fcode.asc())
        )
        if limit and limit > 0:
            query = query.limit(limit)
        rows = query.all()

    return [BlankFundRow(fcode=_normalize_code(row[0]), shortname=_normalize_name(row[1])) for row in rows]


def _backfill_fund_data_source(
    db_manager,
    api: FundAPIEndpoints,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    progress_every: int = 0,
) -> dict[str, Any]:
    rows = _load_blank_source_funds(db_manager, limit=limit)
    summary: dict[str, Any] = {
        "scanned": len(rows),
        "updated": 0,
        "already_resolved": 0,
        "exact_match_missing": 0,
        "base_info_missing": 0,
        "search_failed": 0,
        "errors": [],
        "updated_codes": [],
        "unresolved_codes": [],
        "dry_run": bool(dry_run),
        "assigned_data_source": "fundSearch",
    }

    if not rows:
        return summary

    normalized_progress_every = max(0, int(progress_every or 0))

    def _process_row(row: BlankFundRow, index: int, session=None) -> None:
        try:
            search_results = api.search_funds(row.fcode, search_type="1") or []
        except Exception as exc:  # pragma: no cover - exercised via test doubles
            summary["search_failed"] += 1
            summary["errors"].append(
                {
                    "fcode": row.fcode,
                    "shortname": row.shortname,
                    "stage": "search",
                    "error": str(exc),
                }
            )
            summary["unresolved_codes"].append(row.fcode)
            return

        matched = _select_exact_search_match(search_results, row.fcode)
        if not matched:
            summary["exact_match_missing"] += 1
            summary["unresolved_codes"].append(row.fcode)
            return

        base_info = _extract_search_base_info(matched)
        if not base_info:
            summary["base_info_missing"] += 1
            summary["unresolved_codes"].append(row.fcode)
            return

        if dry_run:
            summary["updated"] += 1
            summary["updated_codes"].append(row.fcode)
            return

        fund = session.query(Fund).filter(Fund.fcode == row.fcode).one_or_none()
        if fund is None:
            summary["unresolved_codes"].append(row.fcode)
            return

        current_source = _normalize_name(getattr(fund, "data_source", ""))
        if current_source:
            summary["already_resolved"] += 1
            return

        fund.data_source = "fundSearch"
        fund.last_updated_at = datetime.now()
        summary["updated"] += 1
        summary["updated_codes"].append(row.fcode)
        if normalized_progress_every and (index % normalized_progress_every == 0 or index == len(rows)):
            print(
                json.dumps(
                    {
                        "progress": {
                            "processed": index,
                            "total": len(rows),
                            "updated": summary["updated"],
                            "exact_match_missing": summary["exact_match_missing"],
                            "base_info_missing": summary["base_info_missing"],
                            "search_failed": summary["search_failed"],
                            "dry_run": bool(dry_run),
                        }
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    if dry_run:
        for index, row in enumerate(rows, start=1):
            _process_row(row, index)
    else:
        with db_manager.get_session(task_name="backfill_data_source_write") as session:
            for index, row in enumerate(rows, start=1):
                _process_row(row, index, session)

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="回填 funds.data_source 的真实 API 来源")
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条空来源记录，0 表示不限制")
    parser.add_argument("--dry-run", action="store_true", help="只输出预估结果，不写数据库")
    parser.add_argument(
        "--output",
        type=str,
        default="reports/audit/data_source_backfill/latest.json",
        help="审计结果输出路径",
    )
    parser.add_argument("--progress-every", type=int, default=25, help="每处理多少条输出一次进度，0 表示关闭")
    args = parser.parse_args()

    settings = load_settings()
    output_path = (PROJECT_ROOT / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    db_manager = get_db_manager(db_path=str(settings.database_path))
    api = FundAPIEndpoints(APIClient({"api": {"tiantian": {"base_url": settings.tiantian_base_url}}}))
    summary = _backfill_fund_data_source(
        db_manager,
        api,
        limit=max(0, int(args.limit or 0)) or None,
        dry_run=bool(args.dry_run),
        progress_every=max(0, int(args.progress_every or 0)),
    )
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(settings.database_path),
        "tiantian_base_url": settings.tiantian_base_url,
        "summary": summary,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
