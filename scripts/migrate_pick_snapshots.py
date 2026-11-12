#!/usr/bin/env python3
"""Compress/restore pick TEXT in bounded transactions with exact-row proofs.

No rows, IDs, dates, numbers or JSON bytes are removed. This does not VACUUM or
claim physical disk savings. Live use owns the workflow lock; take an independent
recovery copy before migration, and restore TEXT before downgrading readers.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "core"))

from backend.config import load_settings
from backend.storage import WorkflowStore
from database.screening_pick_payloads import MAGIC, decode_snapshot, encode_snapshot
from scripts.migrate_decision_payloads import assert_scheduler_idle, save


def row_proof(rows, columns: list[str]) -> str:
    """Hash all values and SQLite storage types, decoding only snapshot storage."""
    index = columns.index("snapshot_json")
    digest = hashlib.sha256()
    for original in rows:
        row = list(original)
        row[index] = decode_snapshot(row[index])
        values = [[type(value).__name__, value.hex() if isinstance(value, (float, bytes)) else value]
                  for value in row]
        digest.update(json.dumps(values, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode() + b"\n")
    return digest.hexdigest()


def _next_chain(previous: str, page: str) -> str:
    return hashlib.sha256(bytes.fromhex(previous) + bytes.fromhex(page)).hexdigest()


COUNTERS = ("completed_pages", "selected_rows", "payloads_compressed", "payloads_restored",
            "payload_bytes_before", "payload_bytes_after")


def validate_checkpoint(candidate, identity, mode, batch_rows, through_id, schema) -> None:
    if not isinstance(candidate, dict) or candidate.get("schema_version") != 1 or candidate.get("format") != MAGIC.decode():
        raise ValueError("pick_migration_checkpoint_schema_invalid")
    if (candidate.get("database") != identity or candidate.get("mode") != mode
            or candidate.get("batch_rows") != batch_rows or candidate.get("table_schema") != schema
            or (through_id is not None and candidate.get("through_id") != through_id)):
        raise ValueError("pick_migration_checkpoint_identity_mismatch")
    if not {"minimum_id", "through_id", "last_id", "business_value_chain_sha256"} <= candidate.keys():
        raise ValueError("pick_migration_checkpoint_fields_missing")
    if candidate.get("status") not in {"prepared", "running", "partial_budget_exhausted", "failed", "complete"}:
        raise ValueError("pick_migration_checkpoint_status_invalid")
    if any(type(candidate.get(key)) is not int or candidate[key] < 0 for key in COUNTERS):
        raise ValueError("pick_migration_checkpoint_counters_invalid")
    minimum, cutoff, last = (candidate[key] for key in ("minimum_id", "through_id", "last_id"))
    if any(value is not None and (type(value) is not int or not -(2**63) <= value < 2**63)
           for value in (minimum, cutoff, last)):
        raise ValueError("pick_migration_checkpoint_range_invalid")
    if ((minimum is None) != (cutoff is None) or (minimum is not None and minimum > cutoff)
            or (last is not None and (minimum is None or not minimum <= last <= cutoff))
            or ((last is None) != (candidate["completed_pages"] == 0))
            or not candidate["completed_pages"] <= candidate["selected_rows"] <= candidate["completed_pages"] * batch_rows
            or candidate["payloads_compressed"] + candidate["payloads_restored"] > candidate["selected_rows"]):
        raise ValueError("pick_migration_checkpoint_progress_invalid")
    digest = candidate["business_value_chain_sha256"]
    if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError("pick_migration_checkpoint_digest_invalid")


def _verify_completed_prefix(connection, checkpoint, columns) -> None:
    """Resume checks every durable page, not just the most recently saved one."""
    chain, pages, count = "0" * 64, 0, 0
    with connection:
        connection.execute("BEGIN")
        cursor = connection.execute("SELECT * FROM screening_picks WHERE id<=? ORDER BY id", (checkpoint["last_id"],))
        while rows := cursor.fetchmany(checkpoint["batch_rows"]):
            chain = _next_chain(chain, row_proof(rows, columns))
            pages += 1
            count += len(rows)
    if (chain != checkpoint["business_value_chain_sha256"] or pages != checkpoint["completed_pages"]
            or count != checkpoint["selected_rows"]):
        raise ValueError("pick_migration_checkpoint_completed_values_changed")


def _payload_bytes(value) -> int:
    return 0 if value is None else len(value.encode("utf-8") if isinstance(value, str) else value)


def migrate(database: Path, checkpoint: Path, *, mode: str, batch_rows: int = 2000,
            through_id: int | None = None, max_seconds: float = 900, resume: bool = False) -> dict:
    """The caller excludes competing maintenance; new IDs above the cutoff stay untouched."""
    if (mode not in {"compress", "restore"} or type(batch_rows) is not int or not 1 <= batch_rows <= 5000
            or not math.isfinite(max_seconds) or max_seconds <= 0
            or (through_id is not None and (type(through_id) is not int or not -(2**63) <= through_id < 2**63))):
        raise ValueError("invalid_pick_migration_options")
    database = database.resolve(strict=True)
    stat = database.stat()
    identity = {"path": str(database), "device": stat.st_dev, "inode": stat.st_ino}
    if checkpoint.is_symlink():
        raise ValueError("pick_migration_checkpoint_symlink")
    checkpoint = checkpoint.resolve()
    protected = [database, *(Path(str(database) + suffix) for suffix in ("-wal", "-shm", "-journal"))]
    if any(checkpoint == path or (path.exists() and checkpoint.exists() and checkpoint.samefile(path)) for path in protected):
        raise ValueError("pick_migration_checkpoint_conflicts_with_database")
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    result = None
    connection = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=30)
    started = time.monotonic()
    try:
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA cache_size=-32768")
        schema = [list(row) for row in connection.execute("PRAGMA table_info(screening_picks)")]
        columns = [row[1] for row in schema]
        if not {"id", "snapshot_json"} <= set(columns):
            raise ValueError("pick_migration_required_columns_missing")
        id_column = next(row for row in schema if row[1] == "id")
        if id_column[2].upper() != "INTEGER" or id_column[5] != 1 or sum(bool(row[5]) for row in schema) != 1:
            raise ValueError("pick_migration_requires_integer_primary_key")
        id_index, snapshot_index = columns.index("id"), columns.index("snapshot_json")
        if resume:
            candidate = json.loads(checkpoint.read_text())
            validate_checkpoint(candidate, identity, mode, batch_rows, through_id, schema)
            _verify_completed_prefix(connection, candidate, columns)
            result = candidate
        else:
            if checkpoint.exists():
                raise FileExistsError("pick_migration_checkpoint_exists_use_resume")
            minimum, maximum = connection.execute("SELECT MIN(id),MAX(id) FROM screening_picks").fetchone()
            cutoff = maximum if through_id is None else through_id
            if through_id is not None and (maximum is None or not minimum <= cutoff <= maximum):
                raise ValueError("pick_migration_cutoff_outside_database")
            result = {"schema_version": 1, "format": MAGIC.decode(), "database": identity,
                      "mode": mode, "batch_rows": batch_rows, "table_schema": schema,
                      "minimum_id": minimum, "through_id": cutoff, "last_id": None,
                      **dict.fromkeys(COUNTERS, 0), "business_value_chain_sha256": "0" * 64,
                      "change_count_basis": "Checkpointed pages only; a commit interrupted before checkpoint persistence replays idempotently and may undercount original changes. Payload bytes are not SQLite physical disk savings.",
                      "started_at": datetime.now().astimezone().isoformat(), "status": "prepared"}
            save(checkpoint, result)
        if result["status"] == "complete":
            return result
        result["status"] = "running"
        while time.monotonic() - started < max_seconds:
            current = database.stat()
            if (current.st_dev, current.st_ino) != (stat.st_dev, stat.st_ino):
                raise ValueError("pick_migration_database_replaced")
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                predicate = "id<=?" if result["last_id"] is None else "id>? AND id<=?"
                parameters = ([result["through_id"]] if result["last_id"] is None
                              else [result["last_id"], result["through_id"]])
                rows = connection.execute(f"SELECT * FROM screening_picks WHERE {predicate} ORDER BY id LIMIT ?",
                                          [*parameters, batch_rows]).fetchall()
                if not rows:
                    result["status"] = "complete"
                    break
                before = row_proof(rows, columns)
                originals = {row[id_index]: row[snapshot_index] for row in rows}
                transform = encode_snapshot if mode == "compress" else decode_snapshot
                expected = {row_id: transform(value) for row_id, value in originals.items()}
                updates = [(value, row_id) for row_id, value in expected.items() if value != originals[row_id]]
                if updates:
                    connection.executemany("UPDATE screening_picks SET snapshot_json=? WHERE id=?", updates)
                after = connection.execute("SELECT * FROM screening_picks WHERE id>=? AND id<=? ORDER BY id",
                                           (rows[0][id_index], rows[-1][id_index])).fetchall()
                if len(after) != len(rows) or row_proof(after, columns) != before:
                    raise ValueError("pick_full_business_row_changed")
                if {row[id_index]: row[snapshot_index] for row in after} != expected:
                    raise ValueError("pick_migration_stored_representation_mismatch")
            result["last_id"] = rows[-1][id_index]
            result["completed_pages"] += 1
            result["selected_rows"] += len(rows)
            result["payloads_compressed" if mode == "compress" else "payloads_restored"] += len(updates)
            result["payload_bytes_before"] += sum(_payload_bytes(value) for value in originals.values())
            result["payload_bytes_after"] += sum(_payload_bytes(value) for value in expected.values())
            result["business_value_chain_sha256"] = _next_chain(result["business_value_chain_sha256"], before)
            result["updated_at"] = datetime.now().astimezone().isoformat()
            save(checkpoint, result)
            if result["completed_pages"] % 20 == 0:
                print(json.dumps({key: result[key] for key in ("mode", "last_id", "selected_rows", "completed_pages")}), flush=True)
        if result["status"] != "complete":
            result["status"] = "partial_budget_exhausted"
        result.update(last_invocation_seconds=round(time.monotonic() - started, 3),
                      updated_at=datetime.now().astimezone().isoformat())
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
    parser.add_argument("--mode", choices=("compress", "restore"), required=True)
    parser.add_argument("--batch-rows", type=int, default=2000)
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
                store._acquire_run_lock_locked("pick_snapshot_migration_" + args.mode)
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
