"""Revalidate complete recovery values before retiring maintenance backups."""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any

from utils.core_csv_storage import csv_storage_paths, has_stored_csv, read_stored_csv
from utils.screening_archive_access import (
    _row_signature, _run_value_check, open_parquet_shard, recovery_file_identities,
    sqlite_run_value_proof,
)
from backend.screening_archive_reader import TABLES
from backend.task_audit_storage import (
    is_legacy_task_audit_path, migration_paths, read_migrated_task_audit, read_task_audit_bytes,
    is_maintenance_audit_path, read_migrated_maintenance_audit,
    is_legacy_audit_csv_path, read_migrated_audit_csv,
)


class RecoveryAudit:
    def __init__(self, root: Path, *, max_seconds: float = 300):
        self.root = Path(root).resolve()
        self.deadline = time.monotonic() + max_seconds
        self.files: dict[str, dict[str, Any]] = {}
        self.control_links: dict[str, dict[str, Any]] = {}
        self.migrated_originals: set[Path] = set()

    def check_budget(self) -> None:
        if time.monotonic() > self.deadline:
            raise TimeoutError("backup_retention_verification_budget_exhausted")

    def _project_path(self, value: str | Path) -> Path:
        if not isinstance(value, (str, Path)) or not str(value):
            raise ValueError("recovery_path_missing")
        path = Path(value)
        if path.is_absolute() and not path.is_relative_to(self.root):
            # Artifacts produced in Docker store /app paths. Rebase only that
            # explicit project prefix, never an arbitrary absolute basename.
            if not path.is_relative_to("/app"):
                raise ValueError("recovery_path_outside_project")
            path = self.root / path.relative_to("/app")
        elif not path.is_absolute():
            path = self.root / path
        if not path.resolve().is_relative_to(self.root):
            raise ValueError("recovery_path_outside_project")
        return path

    def path(self, value: str | Path) -> Path:
        path = self._project_path(value)
        for part in [path, *path.parents]:
            if part == self.root:
                break
            if part.is_symlink():
                raise ValueError("recovery_symlink_not_supported")
        return path.resolve()

    @staticmethod
    def _link_identity(path: Path) -> dict[str, Any]:
        info = path.lstat()
        return {"device": info.st_dev, "inode": info.st_ino, "size_bytes": info.st_size,
                "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns, "target": str(path.readlink())}

    def bind_control(self, value: str | Path) -> Path:
        """Bind published aliases and every link in their project-local chain.

        Recovery inputs still reject symlinks. The production latest report
        intentionally resolves through .publication_current and a generation.
        """
        path = self._project_path(value)
        for _ in range(40):
            parts = []
            for part in [path, *path.parents]:
                if part == self.root:
                    break
                parts.append(part)
            link = next((part for part in reversed(parts) if part.is_symlink()), None)
            if link is None:
                return self.bind(path)
            identity = self._link_identity(link)
            saved = self.control_links.setdefault(str(link), identity)
            if saved != identity:
                raise ValueError("publication_control_link_changed")
            target = Path(identity["target"])
            if not target.is_absolute():
                target = link.parent / target
            path = self._project_path(target / path.relative_to(link))
        raise ValueError("publication_control_link_chain_too_long")

    def bind(self, value: str | Path, expected_sha: str | None = None) -> Path:
        self.check_budget()
        path = self.path(value)
        if not path.exists() and has_stored_csv(path):
            stored_path, receipt_path = csv_storage_paths(path)
            # Bind both physical files before decoding, so a changed receipt
            # cannot become the accepted identity after the original was read.
            self.bind(stored_path)
            self.bind(receipt_path)
            original, receipt = read_stored_csv(path)
            self.check_budget()
            if expected_sha is not None and hashlib.sha256(original).hexdigest() != expected_sha:
                raise ValueError("recovery_file_sha_or_identity_mismatch")
            self.bind(stored_path, receipt["stored_sha256"])
            self.migrated_originals.add(path)
            self.assert_stable()
            return path
        if not path.exists() and (is_legacy_task_audit_path(path) or is_maintenance_audit_path(path)
                                  or is_legacy_audit_csv_path(path)):
            reader = (read_migrated_audit_csv if is_legacy_audit_csv_path(path) else read_migrated_maintenance_audit
                      if is_maintenance_audit_path(path) else read_migrated_task_audit)
            original, _, receipt = reader(path)
            if expected_sha is not None and hashlib.sha256(original).hexdigest() != expected_sha:
                raise ValueError("recovery_file_sha_or_identity_mismatch")
            stored_path, receipt_path = migration_paths(path)
            self.bind(stored_path, receipt["stored_sha256"])
            self.bind(receipt_path)
            if is_legacy_audit_csv_path(path):
                context = receipt["audit_identity"]
                self.bind(path.parent / context["context_name"], context["context_sha256"])
            self.migrated_originals.add(path)
            return path
        before = recovery_file_identities([path])[0]
        key = str(path)
        if key in self.files:
            saved = self.files[key]
            if saved["identity"] != before or (expected_sha is not None and saved["sha256"] != expected_sha):
                raise ValueError("recovery_file_changed")
            return path
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                self.check_budget()
                digest.update(block)
        actual = digest.hexdigest()
        if (expected_sha is not None and actual != expected_sha) or recovery_file_identities([path])[0] != before:
            raise ValueError("recovery_file_sha_or_identity_mismatch")
        self.files[key] = {"identity": before, "sha256": actual}
        return path

    def read_json(self, value: str | Path, expected_sha: str | None = None) -> dict[str, Any]:
        path = self.bind(value, expected_sha)
        payload = json.loads(read_task_audit_bytes(path, max_bytes=64 * 1024 * 1024).decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("recovery_metadata_must_be_object")
        self.assert_stable()
        return payload

    def bind_sqlite(self, value: str | Path) -> Path:
        path = self.path(value)
        for row in recovery_file_identities([path, Path(f"{path}-wal"), Path(f"{path}-journal")]):
            old = self.files.setdefault(row["path"], {"identity": row, "sha256": None})
            if old["identity"] != row:
                raise ValueError("recovery_sqlite_changed")
        return path

    def assert_stable(self) -> None:
        self.check_budget()
        if any(path.exists() or path.is_symlink() for path in self.migrated_originals):
            raise ValueError("recovery_migrated_representation_changed")
        for path, identity in self.control_links.items():
            if self._link_identity(Path(path)) != identity:
                raise ValueError("publication_control_link_changed")
        for value in self.files.values():
            identity = value["identity"]
            if recovery_file_identities([Path(identity["path"])])[0] != identity:
                raise ValueError("recovery_files_changed_before_retirement")


def _verify_task_sidecar(raw: Any, task_id: int, audit: RecoveryAudit) -> dict | None:
    if not isinstance(raw, str):
        return None
    try:
        compact = json.loads(raw)
    except ValueError:
        return None  # A raw task text need not contain JSON.
    if not isinstance(compact, dict) or compact.get("task_history_extra_data_compacted") is not True:
        return None
    binding = compact.get("task_history_full_payload") or {}
    if compact.get("task_history_extra_data_schema_version") != 1 or binding.get("available") is not True:
        raise ValueError("task_recovery_sidecar_unavailable")
    path = binding.get("sidecar_project_path") or binding.get("sidecar_path_relative") or binding.get("sidecar_path")
    payload = audit.read_json(path)
    if binding.get("source") == "scheduler_task_run_audit":
        original = payload.get("task_result")
        recorded_id = (payload.get("task") or {}).get("task_history_id")
    else:
        original = payload.get("full_payload")
        recorded_id = payload.get("task_history_id")
    if recorded_id != task_id or not isinstance(original, dict):
        raise ValueError("task_recovery_sidecar_task_mismatch")
    def encoded(value: dict) -> bytes:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")

    def matches(data: bytes) -> bool:
        return (type(binding.get("full_payload_bytes")) is int and len(data) == binding["full_payload_bytes"]
                and hashlib.sha256(data).hexdigest() == binding.get("full_payload_sha256"))

    data = encoded(original)
    restored_fields = []
    restored_null_fields = []
    restored_publication_fields = []
    if (not matches(data) and binding.get("source") == "scheduler_task_run_audit"
            and payload.get("persistence_stage") != "completion_recorded"):
        # Before completion sealing, the writer appended these fields AFTER
        # creating the audit. The archived SQL row retains their values. Rebuild
        # only that producer order, and still require the original byte hash.
        # Some pre-sealing writers explicitly set error=None on successful
        # delivery, but the SQL summary omitted this empty field. Others ran
        # publication review after the audit and changed three status labels.
        # Try only these producer branches, using recorded values. Neither
        # an inferred NULL nor a later status is accepted without the exact
        # original full-payload length AND SHA. Sealed audits never use this.
        can_restore_null = (compact.get("task_run_audit_status") == "written"
                            and "task_run_audit_error" not in compact
                            and "task_run_audit_error" not in original)
        publication = compact.get("publication_result")
        can_restore_publication = (isinstance(publication, dict) and publication.get("status") == "blocked"
            and original.get("status") in {"success", "degraded"} and compact.get("status") == "degraded"
            and all(isinstance(compact.get(key), str) and compact[key]
                    for key in ("warning_reason", "degraded_stage")))
        for restore_null in ((False, True) if can_restore_null else (False,)):
            restored = dict(original)
            restored_fields, restored_null_fields, restored_publication_fields = [], [], []
            for field in ("task_run_audit_status", "task_run_audit_error", "task_run_audit", "task_run_audit_path",
                          "workflow_run_id", "publication_result", "status_detail"):
                if field in compact:
                    restored[field] = compact[field]
                    restored_fields.append(field)
                elif field == "task_run_audit_error" and restore_null:
                    restored[field] = None
                    restored_fields.append(field)
                    restored_null_fields.append(field)
            data = encoded(restored)
            if matches(data):
                break
            if can_restore_publication:
                for field in ("status", "warning_reason", "degraded_stage"):
                    if restored.get(field) != compact[field]:
                        restored[field] = compact[field]
                        restored_publication_fields.append(field)
                data = encoded(restored)
                if matches(data):
                    break
    if not matches(data):
        raise ValueError("task_recovery_sidecar_value_mismatch")
    proof = {"task_id": task_id, "path": str(audit.path(path)), "bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(), "restored_post_audit_fields": restored_fields,
            "method": "legacy_scheduler_fields_with_exact_original_sha" if restored_fields else "direct_full_payload"}
    if restored_null_fields:
        proof["restored_producer_null_fields"] = restored_null_fields
    if restored_publication_fields:
        proof["restored_publication_fields_from_compact"] = restored_publication_fields
    return proof


def verify_task_recovery(execution: dict, audit: RecoveryAudit) -> dict:
    from utils.task_history_cleanup_execution import _verify_archived_task_rows

    post = execution.get("post_apply_consistency") or {}
    archive = dict(execution.get("archive") or {})
    expected = archive.get("full_value_verification") or {}
    if (execution.get("mode") != "apply" or execution.get("status") != "success"
            or execution.get("guard_errors") or execution.get("transaction_rolled_back")
            or post.get("status") != "ok" or post.get("remaining_candidate_count") != 0
            or post.get("protected_missing_count") != 0
            or expected.get("status") != "verified"
            or expected != post.get("archive_verified_before_commit")):
        raise ValueError("task_complete_recovery_proof_required")
    rows_path = audit.bind(archive.get("rows_archive_path"), archive.get("rows_archive_sha256"))
    summary_path = audit.bind(archive.get("snapshot_watchdog_skipped_summary_path"), archive.get("snapshot_watchdog_skipped_summary_sha256"))
    rows = []
    sidecars = []
    with rows_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            audit.check_budget()
            item = json.loads(line)
            original = item["row"]
            rows.append({"id": original["id"], "row_sha256": item.get("row_sha256")})
            proof = _verify_task_sidecar(original.get("extra_data"), original["id"], audit)
            if proof is not None:
                sidecars.append(proof)
    if (not rows or len(rows) != post.get("deleted_count") or len(rows) != post.get("expected_delete_count")
            or len(rows) != (execution.get("summary") or {}).get("deleted_count")):
        raise ValueError("task_recovery_delete_scope_mismatch")
    archive.update(rows_archive_absolute_path=str(rows_path), snapshot_watchdog_skipped_summary_absolute_path=str(summary_path),
                   recovery_file_identities=recovery_file_identities([rows_path, summary_path]))
    actual = _verify_archived_task_rows(archive, rows)
    if actual != expected:
        raise ValueError("task_recovery_values_changed_since_cleanup")
    audit.assert_stable()
    return {"purpose": "task_history_cleanup", "row_count": len(rows), "verified_full_payload_sidecars": len(sidecars),
            "full_payload_proofs": sidecars,
            "value_proof": actual}


def _screening_expected(execution: dict) -> tuple[list[str], dict]:
    guard = execution.get("archive_guard") or {}
    post = execution.get("post_apply_consistency") or {}
    batches = execution.get("batches") or []
    if (execution.get("mode") != "apply" or execution.get("status") != "applied" or not batches
            or guard.get("status") != "ok" or guard.get("blockers") or post.get("status") != "ok"
            or any(row.get("status") != "applied" for row in batches)
            or (execution.get("summary") or {}).get("failed_batch_count") != 0):
        raise ValueError("screening_successful_cleanup_required")
    runs = [run for batch in batches for run in batch["run_ids"]]
    if not runs or len(set(runs)) != len(runs):
        raise ValueError("screening_recovery_scope_invalid")
    expected = {}
    for shard in guard.get("archive_shard_checks") or []:
        proof = shard.get("hot_value_verification") or {}
        if not proof.get("run_count"):
            continue
        if proof.get("basis") != "multiset_of_full_typed_row_sha256_excluding_storage_rowid":
            raise ValueError("screening_complete_value_proof_required")
        for check in proof.get("table_checks") or []:
            table = check["table_name"]
            if table not in TABLES or check.get("all_values_match") is not True:
                raise ValueError("screening_complete_value_proof_required")
            entry = expected.setdefault(table, {"columns": check["columns"], "table_exists": check["table_exists"], "runs": {}})
            if entry["columns"] != check["columns"] or entry["table_exists"] != check["table_exists"]:
                raise ValueError("screening_recovery_schema_conflict")
            for run in check["run_checks"]:
                if run["run_id"] in entry["runs"]:
                    raise ValueError("screening_recovery_duplicate_scope")
                entry["runs"][run["run_id"]] = run
    if set(expected) != set(TABLES) or any(set(entry["runs"]) != set(runs) for entry in expected.values()):
        raise ValueError("screening_complete_value_proof_required")
    for batch in batches:
        row_count = sum(expected[table]["runs"][run]["row_count"] for table in TABLES for run in batch["run_ids"])
        if batch.get("archive_rows_reverified_in_write_transaction") != row_count:
            raise ValueError("screening_write_transaction_proof_required")
    return sorted(runs), expected


def verify_screening_recovery(execution: dict, audit: RecoveryAudit, active_archive: Path) -> dict:
    from utils.screening_history_archive_manifest import load_screening_archive_manifest, queryable_archive_shards

    runs, expected = _screening_expected(execution)
    state = load_screening_archive_manifest(active_archive, project_root=audit.root)
    if state["status"] != "ready":
        raise ValueError("screening_current_archive_manifest_not_ready")
    audit.bind(state["manifest_path"])
    present_all = set()
    verified_rows = 0
    for shard in queryable_archive_shards(state):
        audit.check_budget()
        if shard["format"] == "parquet":
            reader = open_parquet_shard(shard)
            present = sorted(set(runs) & reader.run_ids)
            if not present:
                continue
            audit.bind(reader.manifest_path, shard["manifest_sha256"])
            for name, file in reader.files.items():
                audit.bind(reader._path(name), file["sha256"])
            checks = []
            for table in TABLES:
                columns = sorted(reader.columns(table)[1:])
                values = []
                for run in present:
                    audit.check_budget()
                    signatures = sorted(_row_signature(tuple(row[col] for col in columns))
                                        for frame in reader.iter_run_batches(table, run) for row in frame.to_pylist())
                    values.append(_run_value_check(run, signatures))
                checks.append({"table_name": table, "columns": columns, "table_exists": True, "run_checks": values})
        else:
            path = audit.bind_sqlite(shard["absolute_path"])
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
                conn.execute("PRAGMA query_only=ON")
                conn.execute("PRAGMA cache_size=-32768")
                conn.execute("BEGIN")
                schema = conn.execute("SELECT name FROM sqlite_master").fetchall()
                if not schema and shard.get("role") == "active":
                    # Rotation creates a genuinely empty writable database.
                    # Historical recovery must still close over all requested
                    # runs in the other, explicitly bound shards below.
                    continue
                present = []
                for offset in range(0, len(runs), 300):
                    batch = runs[offset:offset + 300]
                    marks = ",".join("?" for _ in batch)
                    present.extend(row[0] for row in conn.execute(f"SELECT run_id FROM screening_runs WHERE run_id IN ({marks})", batch))
                present.sort()
                checks = []
                if present:
                    for table in TABLES:
                        audit.check_budget()
                        checks.append(sqlite_run_value_proof(conn, table, present))
        if set(present) & present_all or len(set(present)) != len(present):
            raise ValueError("screening_recovery_duplicate_current_run")
        present_all.update(present)
        for check in checks:
            wanted = expected[check["table_name"]]
            if (check["columns"] != wanted["columns"] or check["table_exists"] != wanted["table_exists"]
                    or check["run_checks"] != [wanted["runs"][run] for run in present]):
                raise ValueError("screening_recovery_values_changed_since_cleanup")
            verified_rows += sum(row["row_count"] for row in check["run_checks"])
    if present_all != set(runs):
        raise ValueError("screening_current_recovery_scope_missing")
    audit.assert_stable()
    return {"purpose": "screening_history_cleanup", "run_count": len(runs), "row_count": verified_rows,
            "basis": "all_seven_tables_complete_typed_values_reverified_against_cleanup_proof"}
