#!/usr/bin/env python3
"""Share, compact, or restore decision metrics with resumable exact-row checks."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))
from backend.config import load_settings
from backend.storage import WorkflowStore
from database.screening_decision_payloads import (
    begin_write, collect_unreferenced_payloads, ensure_storage, resolve_metric_texts,
    restore_inline_rows, share_existing_rows,
)


def save(path: Path, payload: dict) -> None:
    descriptor, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def assert_scheduler_idle(settings) -> None:
    state = json.loads(settings.scheduler_state_file.read_text()) if settings.scheduler_state_file.exists() else {}
    if state.get("current_task"):
        raise RuntimeError("scheduler_task_active")
    with closing(sqlite3.connect(settings.database_path.as_uri() + "?mode=ro", uri=True)) as connection:
        if connection.execute("SELECT id FROM task_history WHERE status='running' AND task_name LIKE 'scheduled_%' LIMIT 1").fetchone():
            raise RuntimeError("scheduler_task_active_in_database")


def row_proof(connection, rows, columns):
    id_index, metric_index = columns.index("id"), columns.index("metrics_json")
    logical = resolve_metric_texts(connection, [(row[id_index], row[metric_index]) for row in rows])
    complete = []
    for row, metrics in zip(rows, logical):
        values = list(row)
        values[metric_index] = metrics
        complete.append(values)
    return hashlib.sha256(json.dumps(complete, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


COUNTERS = ("completed_pages", "selected_rows", "references_inserted", "inline_payloads_retired",
            "inline_payloads_restored", "references_removed", "unreferenced_objects_collected")


def validate_checkpoint(candidate, identity, mode, batch_rows, through_id, columns) -> None:
    """Reject incompatible/corrupt evidence without rewriting the original file."""
    if not isinstance(candidate, dict) or candidate.get("schema_version") != 1:
        raise ValueError("invalid_migration_checkpoint_schema")
    if not {"through_id", "minimum_id", "last_id", "columns", "business_value_chain_sha256"} <= candidate.keys():
        raise ValueError("invalid_migration_checkpoint_fields")
    if (candidate.get("database") != identity or candidate.get("mode") != mode
            or candidate.get("batch_rows") != batch_rows or candidate.get("columns") != columns
            or (through_id is not None and candidate.get("through_id") != through_id)):
        raise ValueError("migration_checkpoint_identity_mismatch")
    if candidate.get("status") not in {"prepared", "running", "partial_budget_exhausted", "failed", "complete"}:
        raise ValueError("invalid_migration_checkpoint_status")
    if any(type(candidate.get(key)) is not int or candidate[key] < 0 for key in COUNTERS):
        raise ValueError("invalid_migration_checkpoint_counters")
    cutoff, minimum, last_id = (candidate.get(key) for key in ("through_id", "minimum_id", "last_id"))
    for value in (cutoff, minimum, last_id):
        if value is not None and (type(value) is not int or not -(2**63) <= value < 2**63):
            raise ValueError("invalid_migration_checkpoint_range")
    if ((minimum is None) != (cutoff is None) or (minimum is not None and minimum > cutoff)
            or (last_id is not None and (minimum is None or not minimum <= last_id <= cutoff))
            or ((last_id is None) != (candidate["completed_pages"] == 0))
            or candidate["selected_rows"] < candidate["completed_pages"]
            or candidate["selected_rows"] > candidate["completed_pages"] * batch_rows):
        raise ValueError("invalid_migration_checkpoint_progress")
    digest = candidate.get("business_value_chain_sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(value not in "0123456789abcdef" for value in digest):
        raise ValueError("invalid_migration_checkpoint_digest")
    if candidate["completed_pages"]:
        page = candidate.get("last_page")
        if (not isinstance(page, dict) or page.get("last_id") != last_id
                or type(page.get("first_id")) is not int or not minimum <= page["first_id"] <= last_id
                or type(page.get("row_count")) is not int or not 1 <= page["row_count"] <= batch_rows):
            raise ValueError("invalid_migration_checkpoint_last_page")


def migrate(database: Path, checkpoint: Path, *, mode: str, batch_rows: int = 5000,
            through_id: int | None = None, max_seconds: float = 900, resume: bool = False) -> dict:
    """Migrate a bounded ID scope; callers must exclude competing maintenance."""
    if (mode not in {"share", "compact", "restore"} or type(batch_rows) is not int
            or not 1 <= batch_rows <= 5000 or not math.isfinite(max_seconds) or max_seconds <= 0
            or (through_id is not None and (type(through_id) is not int or not -(2**63) <= through_id < 2**63))):
        raise ValueError("invalid_migration_options")
    database = database.resolve(strict=True)
    info = database.stat()
    identity = {"path": str(database), "device": info.st_dev, "inode": info.st_ino}
    if checkpoint.is_symlink():
        raise ValueError("migration_checkpoint_symlink")
    checkpoint = checkpoint.resolve()
    protected = [database, *(Path(str(database) + suffix) for suffix in ("-wal", "-shm", "-journal"))]
    if any(checkpoint == path or (path.exists() and checkpoint.exists() and checkpoint.samefile(path)) for path in protected):
        raise ValueError("migration_checkpoint_conflicts_with_database")
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    result = None
    connection = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=30)
    started = time.monotonic()
    try:
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA cache_size=-32768")
        columns = [row[1] for row in connection.execute("PRAGMA table_info(screening_decisions)")]
        id_index, metric_index = columns.index("id"), columns.index("metrics_json")
        if resume:
            candidate = json.loads(checkpoint.read_text())
            validate_checkpoint(candidate, identity, mode, batch_rows, through_id, columns)
            if candidate["completed_pages"]:
                page = candidate["last_page"]
                with connection:
                    connection.execute("BEGIN")
                    last_rows = connection.execute(
                        "SELECT * FROM screening_decisions WHERE id>=? AND id<=? ORDER BY id",
                        (page["first_id"], page["last_id"]),
                    ).fetchall()
                    if (len(last_rows) != page["row_count"]
                            or row_proof(connection, last_rows, columns) != page.get("business_sha256")):
                        raise ValueError("migration_checkpoint_last_page_changed")
            result = candidate
        else:
            if checkpoint.exists():
                raise FileExistsError("migration_checkpoint_exists_use_resume")
            minimum = connection.execute("SELECT MIN(id) FROM screening_decisions").fetchone()[0]
            maximum = connection.execute("SELECT MAX(id) FROM screening_decisions").fetchone()[0]
            cutoff = maximum if through_id is None else through_id
            if through_id is not None and (maximum is None or not minimum <= cutoff <= maximum):
                raise ValueError("migration_cutoff_outside_database")
            result = {"schema_version": 1, "database": identity, "mode": mode, "batch_rows": batch_rows,
                      "through_id": cutoff, "minimum_id": minimum, "last_id": None, "columns": columns,
                      "completed_pages": 0, "selected_rows": 0,
                      "references_inserted": 0, "inline_payloads_retired": 0, "inline_payloads_restored": 0,
                      "references_removed": 0, "unreferenced_objects_collected": 0,
                      "business_value_chain_sha256": "0" * 64,
                      "change_count_basis": "Observed changes on checkpointed pages; a commit interrupted before checkpoint persistence can replay idempotently and omit its original change counters. Final database counts must be checked separately.",
                      "started_at": datetime.now().astimezone().isoformat(), "status": "prepared"}
            save(checkpoint, result)
        if result["status"] == "complete":
            return result
        with connection:
            ensure_storage(connection)
        result["status"] = "running"
        while time.monotonic() - started < max_seconds:
            with connection:
                begin_write(connection)
                predicate = "id<=?" if result["last_id"] is None else "id>? AND id<=?"
                parameters = ([result["through_id"]] if result["last_id"] is None
                              else [result["last_id"], result["through_id"]])
                rows = connection.execute(f"SELECT * FROM screening_decisions WHERE {predicate} ORDER BY id LIMIT ?",
                                          [*parameters, batch_rows]).fetchall()
                if not rows:
                    result["status"] = "complete"
                    break
                before = row_proof(connection, rows, columns)
                metrics = [(row[id_index], row[metric_index]) for row in rows]
                outcome = (restore_inline_rows(connection, metrics) if mode == "restore"
                           else share_existing_rows(connection, metrics, remove_inline=mode == "compact"))
                after_rows = connection.execute(
                    "SELECT * FROM screening_decisions WHERE id>=? AND id<=? ORDER BY id",
                    (rows[0][id_index], rows[-1][id_index]),
                ).fetchall()
                if len(after_rows) != len(rows) or row_proof(connection, after_rows, columns) != before:
                    raise ValueError("decision_full_business_row_changed")
            # Durable progress follows the committed transaction. Repeating a
            # committed page after an interrupted checkpoint is idempotent.
            result["last_id"] = rows[-1][id_index]
            result["completed_pages"] += 1
            for key in ("selected_rows", "references_inserted", "inline_payloads_retired", "inline_payloads_restored", "references_removed"):
                result[key] += outcome.get(key, 0)
            result["business_value_chain_sha256"] = hashlib.sha256(
                bytes.fromhex(result["business_value_chain_sha256"]) + bytes.fromhex(before)).hexdigest()
            result["last_page"] = {"first_id": rows[0][id_index], "last_id": rows[-1][id_index],
                                   "row_count": len(rows), "business_sha256": before}
            result["updated_at"] = datetime.now().astimezone().isoformat()
            save(checkpoint, result)
            if result["completed_pages"] % 20 == 0:
                print(json.dumps({key: result[key] for key in ("mode", "last_id", "selected_rows", "completed_pages")}), flush=True)
        if result["status"] != "complete":
            result["status"] = "partial_budget_exhausted"
        if mode == "restore" and result["status"] == "complete":
            result["status"] = "running"
            while time.monotonic() - started < max_seconds:
                with connection:
                    collected = collect_unreferenced_payloads(connection)
                result["unreferenced_objects_collected"] += collected
                save(checkpoint, result)
                if not collected:
                    break
            remaining = connection.execute(
                "SELECT COUNT(*) FROM screening_decision_payloads p WHERE NOT EXISTS "
                "(SELECT 1 FROM screening_decision_payload_refs r WHERE r.payload_id=p.id)"
            ).fetchone()[0]
            result["unreferenced_objects_remaining"] = remaining
            result["status"] = "partial_budget_exhausted" if remaining else "complete"
        result["last_invocation_seconds"] = round(time.monotonic() - started, 3)
        result["updated_at"] = datetime.now().astimezone().isoformat()
        result.pop("last_error", None)
        save(checkpoint, result)
        return result
    except Exception as error:
        connection.rollback()
        if result is not None:
            result.update(status="failed", last_error=f"{type(error).__name__}:{error}")
            save(checkpoint, result)
        raise
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, help="Defaults to the configured database")
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--mode", choices=("share", "compact", "restore"), required=True)
    parser.add_argument("--batch-rows", type=int, default=5000)
    parser.add_argument("--through-id", type=int)
    parser.add_argument("--max-seconds", type=float, default=900)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    settings = load_settings()
    database = (args.database or settings.database_path).resolve(strict=True)
    live = settings.database_path.exists() and database.samefile(settings.database_path)
    store = None
    acquired = False
    try:
        if live:
            assert_scheduler_idle(settings)
            store = WorkflowStore(settings.state_file)
            with store._lock:
                store._acquire_run_lock_locked("decision_payload_migration_" + args.mode)
                acquired = True
            assert_scheduler_idle(settings)
        result = migrate(database, args.checkpoint.absolute(), mode=args.mode, batch_rows=args.batch_rows,
                         through_id=args.through_id, max_seconds=args.max_seconds, resume=args.resume)
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return 0 if result["status"] == "complete" else 2
    finally:
        if store is not None and acquired:
            with store._lock:
                store._release_run_lock_locked()


if __name__ == "__main__":
    raise SystemExit(main())
