#!/usr/bin/env python3
"""Inspect or make bounded room in the disposable calculation lookup cache.

Only global lookup aliases with surviving local links may be evicted. Historical
capture directories, manifests and business rows are never modified. Repeat a
bounded apply if more headroom is required; the normal writer also does this.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "core")]

from backend.config import load_settings
from backend.storage import WorkflowStore
from backend.task_audit_storage import write_task_audit
from core.utils.calculation_pack_storage import (
    CATALOG, MAX_CATALOG_BYTES, MAX_POOL_BYTES, _catalog, _catalog_used_bytes,
    _make_cache_room, _pool_lock, _pool_size, is_pack_name,
)


def describe(pool: Path, connection: sqlite3.Connection) -> dict:
    return {
        "catalog_file_bytes": (pool / CATALOG).stat().st_size,
        "catalog_used_page_bytes": _catalog_used_bytes(connection),
        "catalog_freelist_pages": connection.execute("PRAGMA freelist_count").fetchone()[0],
        "indexed_packs": connection.execute("SELECT COUNT(*) FROM packs").fetchone()[0],
        "pool_logical_bytes": _pool_size(pool),
        "locally_referenced_lookup_packs": sum(
            path.lstat().st_nlink > 1 for path in pool.iterdir() if is_pack_name(path.name)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--reserve-mib", type=int, default=64, choices=range(1, 129), metavar="1..128")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    settings = load_settings()
    pool = settings.reports_dir / ".calculation_input_pool"
    if args.output.exists() or args.output.is_symlink():
        parser.error("output already exists")
    result = {"schema_version": 1, "mode": "apply" if args.apply else "dry-run",
              "reserve_bytes": args.reserve_mib * 1024**2,
              "max_catalog_bytes": MAX_CATALOG_BYTES, "max_pool_bytes": MAX_POOL_BYTES,
              "started_at": datetime.now().astimezone().isoformat(),
              "physical_pack_reclamation_claimed": False}
    if args.apply:
        store = WorkflowStore(settings.state_file)
        try:
            with store._lock:
                store._acquire_run_lock_locked("maintain_calculation_cache")
            with _pool_lock(pool):
                connection = _catalog(pool)
                try:
                    result["before"] = describe(pool, connection)
                    result["maintenance"] = _make_cache_room(connection, pool, 0, result["reserve_bytes"])
                    result["after"] = describe(pool, connection)
                finally:
                    connection.close()
        finally:
            store.close()
    else:
        with sqlite3.connect((pool / CATALOG).as_uri() + "?mode=ro", uri=True) as connection:
            result["before"] = describe(pool, connection)
    result["finished_at"] = datetime.now().astimezone().isoformat()
    write_task_audit(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
