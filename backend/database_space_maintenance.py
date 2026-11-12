from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any


def run_incremental_space_reclaim(
    database_path: Path,
    *,
    max_pages: int = 50_000,
    min_freelist_pages: int = 1_000,
    max_seconds: float = 180.0,
) -> dict[str, Any]:
    """Reclaim a bounded number of trailing free pages from an incremental-vacuum DB."""
    before_size = database_path.stat().st_size
    with sqlite3.connect(str(database_path), timeout=120) as conn:
        conn.execute("PRAGMA busy_timeout=120000")
        auto_vacuum = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0] or 0)
        page_size = int(conn.execute("PRAGMA page_size").fetchone()[0] or 0)
        freelist_before = int(conn.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        page_count_before = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
        if auto_vacuum != 2:
            return {
                "status": "conversion_required",
                "auto_vacuum": auto_vacuum,
                "freelist_pages_before": freelist_before,
                "page_count_before": page_count_before,
                "reclaimed_pages": 0,
                "reclaimed_bytes": 0,
            }
        if freelist_before < max(0, int(min_freelist_pages)):
            return {
                "status": "skipped_below_threshold",
                "auto_vacuum": auto_vacuum,
                "freelist_pages_before": freelist_before,
                "page_count_before": page_count_before,
                "reclaimed_pages": 0,
                "reclaimed_bytes": 0,
            }
        requested_pages = min(freelist_before, max(1, int(max_pages)))
        started = time.monotonic()
        time_budget_seconds = max(1.0, float(max_seconds or 180.0))
        attempted_steps = 0
        prior_page_count = page_count_before
        stopped_reason = "page_limit"
        for _ in range(requested_pages):
            if attempted_steps > 0 and time.monotonic() - started >= time_budget_seconds:
                stopped_reason = "time_budget"
                break
            conn.execute("PRAGMA incremental_vacuum(1)")
            attempted_steps += 1
            current_page_count = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
            if current_page_count >= prior_page_count:
                stopped_reason = "no_trailing_page"
                break
            prior_page_count = current_page_count
        freelist_after = int(conn.execute("PRAGMA freelist_count").fetchone()[0] or 0)
        page_count_after = int(conn.execute("PRAGMA page_count").fetchone()[0] or 0)
        readonly_probe = (
            "ok" if conn.execute("SELECT 1").fetchone()[0] == 1 else "unexpected_result"
        )
        elapsed_seconds = round(time.monotonic() - started, 3)
    after_size = database_path.stat().st_size
    return {
        "status": "reclaimed" if after_size < before_size else "no_trailing_pages_reclaimed",
        "auto_vacuum": auto_vacuum,
        "requested_pages": requested_pages,
        "attempted_steps": attempted_steps,
        "page_size": page_size,
        "freelist_pages_before": freelist_before,
        "freelist_pages_after": freelist_after,
        "page_count_before": page_count_before,
        "page_count_after": page_count_after,
        "reclaimed_pages": max(0, page_count_before - page_count_after),
        "reclaimed_bytes": max(0, before_size - after_size),
        "readonly_probe": readonly_probe,
        "full_quick_check": "deferred_to_stopped_service_compaction_window",
        "max_seconds": time_budget_seconds,
        "elapsed_seconds": elapsed_seconds,
        "stopped_reason": stopped_reason,
    }
