"""Complete-value proofs for SQLite and Parquet archive recovery gates."""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Sequence

from backend.screening_archive_reader import ScreeningArchiveReader, TABLES


def _row_signature(row: Sequence[Any]) -> bytes:
    return hashlib.sha256(json.dumps(
        list(row), ensure_ascii=False, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")).digest()


def _run_value_check(run_id: str, signatures: Sequence[bytes]) -> dict[str, Any]:
    return {"run_id": run_id, "row_count": len(signatures),
            "sha256": hashlib.sha256(b"".join(signatures)).hexdigest()}


def recovery_file_identities(paths: Sequence[Path]) -> list[dict[str, Any]]:
    """Bind recovery files; absent/empty SQLite journals contain no committed rows."""
    result = []
    for path in paths:
        is_journal = str(path).endswith(("-wal", "-journal"))
        try:
            info = path.stat()
        except FileNotFoundError:
            if not is_journal:
                raise
            info = None
        if is_journal and (info is None or info.st_size == 0):
            result.append({"path": str(path), "empty_or_absent_journal": True})
            continue
        result.append({"path": str(path), "device": info.st_dev, "inode": info.st_ino,
                       "size_bytes": info.st_size, "mtime_ns": info.st_mtime_ns,
                       "ctime_ns": info.st_ctime_ns})
    return result


def sqlite_run_value_proof(
    connection: sqlite3.Connection, table: str, run_ids: Sequence[str],
) -> dict[str, Any]:
    """Hash full typed rows one run at a time, retaining duplicate multiplicity.

    All declared columns, including id, are business history here; only SQLite's
    implicit rowid is excluded. Logical decision payloads are resolved first.
    """
    from database.screening_decision_payloads import begin_read, canonical_source, verify_reference_scope

    if table not in TABLES:
        raise ValueError("unsupported_screening_archive_table")
    begin_read(connection)
    columns = sorted(row[1] for row in connection.execute(f'PRAGMA table_info("{table}")'))
    runs = sorted(set(run_ids))
    if columns and "run_id" not in columns:
        raise ValueError(f"sqlite_archive_run_id_column_missing:{table}")
    if table == "screening_decisions" and columns:
        verify_reference_scope(connection, runs)
    checks = []
    if columns:
        projection, source = canonical_source(connection, table, columns)
        for run_id in runs:
            signatures = sorted(_row_signature(row) for row in connection.execute(
                f'SELECT {projection} FROM {source} WHERE h.run_id=?', (run_id,),
            ))
            checks.append(_run_value_check(run_id, signatures))
    else:
        checks = [_run_value_check(run_id, []) for run_id in runs]
    return {"table_name": table, "table_exists": bool(columns), "columns": columns,
            "verified_row_count": sum(check["row_count"] for check in checks), "run_checks": checks}


def verify_sqlite_hot_values(
    archive_connection: sqlite3.Connection, hot_connection: sqlite3.Connection,
    run_ids: Sequence[str],
) -> dict[str, Any]:
    """Verify the SQLite branch with the same complete-value rule as Parquet."""
    started = time.monotonic()
    checks = []
    runs = sorted(set(run_ids))
    for table in TABLES:
        hot = sqlite_run_value_proof(hot_connection, table, runs)
        cold = sqlite_run_value_proof(archive_connection, table, runs)
        if hot["columns"] != cold["columns"] or hot["table_exists"] != cold["table_exists"]:
            raise ValueError(f"sqlite_hot_schema_mismatch:{table}")
        if hot["run_checks"] != cold["run_checks"]:
            changed = next(run_id for run_id, a, b in zip(runs, hot["run_checks"], cold["run_checks"]) if a != b)
            raise ValueError(f"sqlite_hot_values_mismatch:{table}:{changed}")
        checks.append({**hot, "all_values_match": True})
    return {"run_count": len(runs), "table_checks": checks,
            "row_count": sum(check["verified_row_count"] for check in checks),
            "basis": "multiset_of_full_typed_row_sha256_excluding_storage_rowid",
            "duration_seconds": round(time.monotonic() - started, 6)}


def verify_recovery_files_unchanged(shard_checks: Sequence[dict[str, Any]]) -> None:
    for shard in shard_checks:
        if not (shard.get("hot_value_verification") or {}).get("run_count"):
            continue
        identities = shard.get("recovery_file_identities")
        if not identities or recovery_file_identities([Path(row["path"]) for row in identities]) != identities:
            raise ValueError("archive_recovery_files_changed_during_delete")


def verify_hot_before_delete(
    connection: sqlite3.Connection, run_ids: Sequence[str], shard_checks: Sequence[dict[str, Any]],
) -> int:
    """Recheck guarded rows inside the caller's BEGIN IMMEDIATE transaction."""
    if not connection.in_transaction:
        raise ValueError("archive_delete_requires_write_transaction")
    verify_recovery_files_unchanged(shard_checks)
    runs = sorted(set(run_ids))
    expected: dict[str, dict[str, Any]] = {}
    for shard in shard_checks:
        proof = shard.get("hot_value_verification") or {}
        if not proof.get("run_count"):
            continue
        for check in proof.get("table_checks") or []:
            table = check["table_name"]
            if table not in TABLES or not check.get("all_values_match"):
                raise ValueError("archive_complete_value_proof_required")
            entry = expected.setdefault(table, {"columns": check["columns"], "table_exists": check["table_exists"], "runs": {}})
            if entry["columns"] != check["columns"] or entry["table_exists"] != check["table_exists"]:
                raise ValueError("archive_value_proof_schema_conflict")
            for run in check["run_checks"]:
                if run["run_id"] in entry["runs"]:
                    raise ValueError("archive_value_proof_duplicate_run")
                entry["runs"][run["run_id"]] = run
    verified = 0
    for table in TABLES:
        wanted = expected.get(table)
        if wanted is None or any(run_id not in wanted["runs"] for run_id in runs):
            raise ValueError(f"archive_complete_value_proof_missing:{table}")
        actual = sqlite_run_value_proof(connection, table, runs)
        if (actual["columns"] != wanted["columns"] or actual["table_exists"] != wanted["table_exists"]
                or actual["run_checks"] != [wanted["runs"][run_id] for run_id in runs]):
            raise ValueError(f"hot_values_changed_before_archive_delete:{table}")
        verified += actual["verified_row_count"]
    return verified


@lru_cache(maxsize=2)
def _reader(path_text: str, identity: tuple, manifest_sha256: str, source_sha256: str) -> ScreeningArchiveReader:
    return ScreeningArchiveReader(Path(path_text).parent,
        manifest_sha256=manifest_sha256, source_sha256=source_sha256)


def open_parquet_shard(shard: dict[str, Any]) -> ScreeningArchiveReader:
    if shard.get('format') != 'parquet' or shard.get('role') != 'immutable' or shard.get('state') != 'immutable':
        raise ValueError('parquet_shard_must_be_immutable')
    path = Path(shard['absolute_path'])
    if path.name != 'manifest.json':
        raise ValueError('parquet_shard_path_must_be_manifest')
    stat = path.stat()
    reader = _reader(str(path), (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns),
                     shard.get('manifest_sha256') or '', shard.get('source_sha256') or '')
    counts = shard.get('table_row_counts')
    actual = {table: reader.manifest['tables'][table]['row_count'] for table in TABLES}
    if not isinstance(counts, dict) or counts != actual:
        raise ValueError('parquet_shard_table_counts_not_bound')
    reader._check_manifest()
    return reader


def parquet_shard_storage_bytes(shard: dict[str, Any]) -> int:
    """Measure only declared files; undeclared neighbors never become data inputs."""
    reader = open_parquet_shard(shard)
    total = reader.manifest_path.stat().st_size
    for name, entry in reader.files.items():
        size = reader._path(name).stat().st_size
        if size != entry['bytes']:
            raise ValueError('parquet_shard_file_size_mismatch')
        total += size
    reader._check_manifest()
    return total


def probe_parquet_shard(shard: dict[str, Any], run_ids: Sequence[str]) -> dict[str, Any]:
    """Full file SHA verification precedes metadata counts used for hot deletion.

    File hashes are cached only for the same inode/size/mtime/ctime. Absence of a
    requested run means zero rows; absence of a declared file is an error.
    """
    started = time.monotonic()
    reader = open_parquet_shard(shard)
    recovery_paths = [reader.manifest_path, *(reader._path(name) for name in reader.files)]
    recovery_identities = recovery_file_identities(recovery_paths)
    checked = [reader._check_file(name) for name in reader.files]
    present = sorted(set(run_ids) & reader.run_ids)
    counts = {table: sum(reader.count(table, run_id) for run_id in present) for table in TABLES}
    for path, identity in checked:
        stat = path.stat()
        if (stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns) != identity:
            raise ValueError('parquet_shard_changed_during_recovery_probe')
    reader._check_manifest()
    stat = reader.manifest_path.stat()
    if recovery_file_identities(recovery_paths) != recovery_identities:
        raise ValueError('parquet_shard_changed_during_recovery_probe')
    return {
        'present_run_ids': present, 'table_counts': counts,
        'check': {
            'id': shard['id'], 'role': 'immutable', 'format': 'parquet',
            'path': str(reader.manifest_path), 'exists': True, 'file_size_bytes': stat.st_size,
            'file_mtime_ns': stat.st_mtime_ns, 'storage_size_bytes': parquet_shard_storage_bytes(shard),
            'manifest_sha256': shard['manifest_sha256'], 'source_sha256': shard['source_sha256'],
            'integrity_check': 'all_declared_parquet_files_sha256_verified', 'quick_check': None,
            'verified_file_count': len(checked), 'ok': True,
            'recovery_file_identities': recovery_identities,
            'duration_seconds': round(time.monotonic() - started, 6),
        },
    }


def verify_parquet_hot_values(
    shard: dict[str, Any], run_ids: Sequence[str], connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Compare every selected row before reusing/deleting a hot copy.

    SQLite rowids may change during archival, so compare multisets of complete
    typed rows instead. Each row has its own digest; only these small digests
    are retained for one run/table at a time. Counts alone cannot detect a
    revised decision, a NULL changed to zero, or a substituted fund code.
    """
    reader = open_parquet_shard(shard)
    from database.screening_decision_payloads import begin_read

    begin_read(connection)
    present = sorted(set(run_ids) & reader.run_ids)
    checks = []
    started = time.monotonic()

    for table in TABLES:
        columns = sorted(reader.columns(table)[1:])
        hot_columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
        if set(hot_columns) != set(columns):
            raise ValueError(f"parquet_hot_schema_mismatch:{table}")
        from database.screening_decision_payloads import canonical_source, verify_reference_scope

        if table == "screening_decisions":
            verify_reference_scope(connection, present)
        projection, source = canonical_source(connection, table, columns)
        total = 0
        run_checks = []
        for run_id in present:
            expected = sorted(
                _row_signature(tuple(row[column] for column in columns))
                for frame in reader.iter_run_batches(table, run_id)
                for row in frame.to_pylist()
            )
            actual = sorted(_row_signature(row) for row in connection.execute(
                f'SELECT {projection} FROM {source} WHERE h.run_id=?', (run_id,),
            ))
            if actual != expected:
                raise ValueError(f"parquet_hot_values_mismatch:{table}:{run_id}")
            total += len(expected)
            run_checks.append(_run_value_check(run_id, actual))
        checks.append({"table_name": table, "table_exists": True, "columns": columns,
                       "verified_row_count": total, "all_values_match": True, "run_checks": run_checks})
    reader._check_manifest()
    return {"run_count": len(present), "table_checks": checks,
            "row_count": sum(row["verified_row_count"] for row in checks),
            "basis": "multiset_of_full_typed_row_sha256_excluding_storage_rowid",
            "duration_seconds": round(time.monotonic() - started, 6)}
