from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


# A rejected attempt is observable history, but cannot satisfy a schedule slot,
# replace an execution's warning, or establish a completed-run timestamp.
# Keep malformed/missing legacy metadata visible; only explicit busy skips are excluded.
SCHEDULER_EXECUTION_ROW_SQL = """NOT (
    status = 'skipped' AND COALESCE(
        CASE WHEN json_valid(extra_data) THEN json_extract(extra_data, '$.skip_reason') END, ''
    ) = 'scheduler_busy'
)"""


class SqliteTaskRecorder:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        self._ensure_tables(conn)
        return conn

    @staticmethod
    def _ensure_tables(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name VARCHAR(50) NOT NULL,
                status VARCHAR(20) NOT NULL,
                started_at DATETIME NOT NULL,
                completed_at DATETIME,
                duration INTEGER,
                records_processed INTEGER,
                records_inserted INTEGER,
                records_updated INTEGER,
                records_failed INTEGER,
                error_message TEXT,
                error_traceback TEXT,
                extra_data TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_history_started ON task_history (started_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_history_name ON task_history (task_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_task_history_status ON task_history (status)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_status (
                task_name VARCHAR(50) PRIMARY KEY,
                status VARCHAR(20),
                started_at DATETIME,
                completed_at DATETIME,
                progress INTEGER,
                error_message TEXT
            )
            """
        )

    def start_task(self, task_name: str) -> dict[str, Any]:
        started_at = datetime.now().isoformat(sep=" ", timespec="seconds")
        conn = self._connect()
        try:
            cursor = conn.execute(
                """
                INSERT INTO task_history (
                    task_name,
                    status,
                    started_at,
                    completed_at,
                    duration,
                    records_processed,
                    records_inserted,
                    records_updated,
                    records_failed,
                    error_message,
                    error_traceback,
                    extra_data
                )
                VALUES (?, 'running', ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """,
                (task_name, started_at),
            )
            conn.execute(
                """
                INSERT INTO task_status (
                    task_name,
                    status,
                    started_at,
                    completed_at,
                    progress,
                    error_message
                )
                VALUES (?, 'running', ?, NULL, 0, NULL)
                ON CONFLICT(task_name) DO UPDATE SET
                    status = excluded.status,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    progress = excluded.progress,
                    error_message = excluded.error_message
                """,
                (task_name, started_at),
            )
            conn.commit()
            return {
                "id": int(cursor.lastrowid),
                "task_name": task_name,
                "started_at": started_at,
            }
        finally:
            conn.close()

    def record_skipped_attempt(self, task_name: str, *, result: dict[str, Any]) -> dict[str, Any]:
        """Persist a terminal attempt without overwriting an active task_status."""
        if result.get("status") != "skipped":
            raise ValueError("skipped_attempt_requires_skipped_status")
        started_at = str(result["started_at"])
        conn = self._connect()
        try:
            cursor = conn.execute(
                """INSERT INTO task_history (
                    task_name, status, started_at, completed_at, duration,
                    records_processed, records_inserted, records_updated, records_failed, extra_data
                ) VALUES (?, 'skipped', ?, ?, 0, 0, 0, 0, 0, ?)""",
                (task_name, started_at, str(result["finished_at"]), json.dumps(result, ensure_ascii=False)),
            )
            conn.commit()
            return {"id": int(cursor.lastrowid), "task_name": task_name, "started_at": started_at}
        finally:
            conn.close()

    def finish_task(
        self,
        task_info: dict[str, Any],
        *,
        status: str,
        error_message: str | None = None,
        error_traceback: str | None = None,
        extra_data: dict[str, Any] | None = None,
        progress: int = 100,
        update_task_status: bool = True,
        completion_result_factory: Callable[[datetime], dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        """Record a fresh completion event after waiting for the database write lock.

        A scheduler factory projects this same instant into its final payload.
        Busy attempts and legacy callers retain their independent metadata policy.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            completed_at = datetime.now()
            if completion_result_factory is not None:
                extra_data = completion_result_factory(completed_at)
            started_at_text = str(task_info.get("started_at") or "").strip()
            try:
                started_at = datetime.fromisoformat(started_at_text)
                if started_at.tzinfo is not None:
                    started_at = started_at.astimezone().replace(tzinfo=None)
            except ValueError:
                started_at = None
            timing = (extra_data or {}).get("task_timing")
            elapsed = timing.get("elapsed_seconds") if isinstance(timing, dict) else None
            duration = None
            if elapsed is not None:
                duration = int(elapsed)
            elif started_at is not None:
                duration = max(0, int((completed_at - started_at).total_seconds()))
            completed_at_text = completed_at.isoformat(
                sep=" ", timespec="microseconds" if completion_result_factory is not None else "seconds")
            extra_json = json.dumps(extra_data, ensure_ascii=False) if extra_data is not None else None
            counts = dict(extra_data or {})
            # Only declared final counts: absence is unknown, not initialized 0.
            final_counts = [counts.get("records_processed"), counts.get("records_inserted"),
                            counts.get("records_updated", counts.get("records_succeeded")), counts.get("records_failed")]
            for field, value in zip(("records_processed", "records_inserted", "records_updated", "records_failed"), final_counts):
                if value is not None and (type(value) is not int or value < 0):
                    raise ValueError(f"task_history_count_invalid:{field}")
            conn.execute(
                """
                UPDATE task_history
                SET
                    status = ?,
                    completed_at = ?,
                    duration = ?,
                    records_processed = ?,
                    records_inserted = ?,
                    records_updated = ?,
                    records_failed = ?,
                    error_message = ?,
                    error_traceback = ?,
                    extra_data = ?
                WHERE id = ?
                """,
                (
                    status,
                    completed_at_text,
                    duration,
                    *final_counts,
                    error_message,
                    error_traceback,
                    extra_json,
                    int(task_info.get("id") or 0),
                ),
            )
            if update_task_status:
                conn.execute(
                    """
                    INSERT INTO task_status (
                        task_name,
                        status,
                        started_at,
                        completed_at,
                        progress,
                        error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(task_name) DO UPDATE SET
                        status = excluded.status,
                        started_at = excluded.started_at,
                        completed_at = excluded.completed_at,
                        progress = excluded.progress,
                        error_message = excluded.error_message
                    """,
                    (
                        str(task_info.get("task_name") or "").strip(),
                        status,
                        started_at_text or None,
                        completed_at_text,
                        int(progress),
                        error_message,
                    ),
                )
            conn.commit()
            return extra_data
        finally:
            conn.close()

    def update_task_extra_data(self, task_info: dict[str, Any], extra_data: dict[str, Any]) -> None:
        """Record final projection outcomes without backdating or retiming completion."""
        conn = self._connect()
        try:
            changed = conn.execute(
                "UPDATE task_history SET extra_data=? WHERE id=? AND completed_at IS NOT NULL",
                (json.dumps(extra_data, ensure_ascii=False), int(task_info.get("id") or 0)),
            ).rowcount
            if changed != 1:
                raise ValueError("completed_task_history_row_missing")
            conn.commit()
        finally:
            conn.close()

    def recover_stale_tasks(self, reason: str) -> dict[str, Any]:
        recovered_at = datetime.now()
        recovered_at_text = recovered_at.isoformat(sep=" ", timespec="seconds")
        recovered_rows: list[dict[str, Any]] = []

        conn = self._connect()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, task_name, started_at
                FROM task_history
                WHERE status = 'running'
                ORDER BY id
                """
            ).fetchall()
            if not rows:
                return {
                    "recovered_total": 0,
                    "recovered_task_names": [],
                    "recovered_at": recovered_at_text,
                    "reason": reason,
                }

            for row in rows:
                started_at_text = str(row["started_at"] or "").strip()
                try:
                    started_at = datetime.fromisoformat(started_at_text)
                except ValueError:
                    started_at = recovered_at
                duration = max(0, int((recovered_at - started_at).total_seconds()))
                task_name = str(row["task_name"] or "").strip()
                conn.execute(
                    """
                    UPDATE task_history
                    SET
                        status = 'failed',
                        completed_at = ?,
                        duration = ?,
                        error_message = ?,
                        error_traceback = NULL,
                        extra_data = ?
                    WHERE id = ?
                    """,
                    (
                        recovered_at_text,
                        duration,
                        reason,
                        json.dumps(
                            {
                                "recovered_at": recovered_at_text,
                                "recovered_reason": reason,
                            },
                            ensure_ascii=False,
                        ),
                        int(row["id"]),
                    ),
                )
                conn.execute(
                    """
                    UPDATE task_status
                    SET
                        status = 'failed',
                        completed_at = ?,
                        progress = 0,
                        error_message = ?
                    WHERE task_name = ? AND status = 'running'
                    """,
                    (
                        recovered_at_text,
                        reason,
                        task_name,
                    ),
                )
                recovered_rows.append(
                    {
                        "task_name": task_name,
                        "started_at": started_at_text,
                        "completed_at": recovered_at_text,
                    }
                )
            conn.commit()
        finally:
            conn.close()

        recovered_task_names = sorted(
            {
                str(item.get("task_name") or "").strip()
                for item in recovered_rows
                if str(item.get("task_name") or "").strip()
            }
        )
        return {
            "recovered_total": len(recovered_rows),
            "recovered_task_names": recovered_task_names,
            "recovered_rows": recovered_rows,
            "recovered_at": recovered_at_text,
            "reason": reason,
        }
