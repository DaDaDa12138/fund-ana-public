"""Evidence for rank copies committed before an external binding is refreshed.

Checkpoints share the cold copy transaction. They explain interrupted count
updates; they never waive unexplained mismatches or authorize hot-row deletion.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

TABLE = "rank_archive_reconcile_checkpoints"


def binding_fingerprint(payload: dict) -> str:
    return hashlib.sha256(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).hexdigest()


def read_checkpoint(conn: sqlite3.Connection, context: dict[str, Any]) -> dict[str, Any]:
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,),
    ).fetchone():
        return {}
    row = conn.execute(f"""SELECT id, before_rows, before_funds, after_rows, after_funds,
            inserted_rows, revision_rows
        FROM {TABLE} WHERE binding_sha256=? AND archive_table=? AND cutoff=?
          AND binding_rows=? AND binding_funds=? ORDER BY id DESC LIMIT 1""", (
        context["binding_sha256"], context["archive_table"], context["cutoff"],
        context["binding_rows"], context["binding_funds"],
    )).fetchone()
    if not row:
        return {}
    result = dict(zip(("id", "before_rows", "before_funds", "after_rows", "after_funds",
                       "inserted_rows", "revision_rows"), row))
    if (
        any(type(value) is not int or value < 0 for value in result.values())
        or result["after_rows"] != result["before_rows"] + result["inserted_rows"]
        or result["before_funds"] > result["before_rows"]
        or not result["before_funds"] <= result["after_funds"] <= result["after_rows"]
    ):
        raise sqlite3.IntegrityError("invalid_rank_reconcile_checkpoint_counts")
    return result


def scoped_counts(conn: sqlite3.Connection, table: str, cutoff: str) -> tuple[int, int]:
    if not table or not table.replace("_", "").isalnum():
        raise ValueError("unsafe_rank_archive_table")
    row = conn.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT fcode) FROM {table} WHERE rank_date < ?", (cutoff,),
    ).fetchone()
    return int(row[0]), int(row[1])


def record_checkpoint(
    conn: sqlite3.Connection, context: dict[str, Any], *, before: tuple[int, int],
    after: tuple[int, int], inserted_rows: int, revision_rows: int,
) -> int:
    if not conn.in_transaction:
        raise sqlite3.IntegrityError("rank_checkpoint_requires_copy_transaction")
    if after[0] != before[0] + inserted_rows:
        raise sqlite3.IntegrityError("rank_checkpoint_copy_count_mismatch")
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
        id INTEGER PRIMARY KEY, binding_sha256 TEXT NOT NULL, archive_table TEXT NOT NULL,
        cutoff TEXT NOT NULL, binding_rows INTEGER NOT NULL, binding_funds INTEGER NOT NULL,
        before_rows INTEGER NOT NULL, before_funds INTEGER NOT NULL,
        after_rows INTEGER NOT NULL, after_funds INTEGER NOT NULL,
        inserted_rows INTEGER NOT NULL, revision_rows INTEGER NOT NULL, committed_at TEXT NOT NULL
    )""")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_rank_reconcile_checkpoint_binding "
                 f"ON {TABLE} (binding_sha256, archive_table, cutoff, id)")
    cursor = conn.execute(f"""INSERT INTO {TABLE} (
        binding_sha256, archive_table, cutoff, binding_rows, binding_funds,
        before_rows, before_funds, after_rows, after_funds, inserted_rows, revision_rows, committed_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
        context["binding_sha256"], context["archive_table"], context["cutoff"],
        context["binding_rows"], context["binding_funds"], *before, *after,
        inserted_rows, revision_rows, datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
    ))
    return int(cursor.lastrowid)
