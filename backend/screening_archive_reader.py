"""Read a sealed screening Parquet copy through its explicitly bound manifest.

No directory discovery, arbitrary SQL, live writes, or publication selection.
Production callers explicitly bind immutable shards; writable data stays in SQLite.
"""
from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import time
from typing import Any, Iterator, Sequence

TABLES = (
    "screening_runs", "screening_decisions", "screening_picks", "screening_pick_metric_snapshots",
    "screening_decision_reason_summary", "screening_run_latest_activity", "screening_run_storage_stats",
)


class ArchiveIntegrityError(ValueError):
    pass


def _file_identity(path: Path) -> tuple[int, int, int, int]:
    stat = path.stat()
    return stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


@lru_cache(maxsize=96)
def _verified_file(path_text: str, identity: tuple[int, int, int, int], expected: str) -> None:
    path = Path(path_text)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected or _file_identity(path) != identity:
        raise ArchiveIntegrityError("archive_file_hash_or_identity_mismatch")


def _row_hash(rows: list[tuple]) -> str:
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class ScreeningArchiveReader:
    def __init__(self, directory: Path, *, manifest_sha256: str, source_sha256: str):
        self.directory = Path(directory).resolve()
        self.manifest_path = self.directory / "manifest.json"
        if not all(re.fullmatch(r"[a-f0-9]{64}", value or "") for value in (manifest_sha256, source_sha256)):
            raise ArchiveIntegrityError("archive_manifest_and_source_hashes_required")
        self.manifest_sha256 = manifest_sha256
        self.manifest_identity = _file_identity(self.manifest_path)
        _verified_file(str(self.manifest_path), self.manifest_identity, manifest_sha256)
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        manifest = self.manifest
        if (manifest.get("schema_version") != 1 or manifest.get("status") != "verified"
                or manifest.get("source_sha256") != source_sha256
                or set(manifest.get("tables") or {}) != set(TABLES)):
            raise ArchiveIntegrityError("archive_manifest_contract_mismatch")
        self.files: dict[str, dict[str, Any]] = {}
        self.run_groups: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for table_name, table in manifest["tables"].items():
            columns = [column["name"] for column in table["columns"]]
            if (len(columns) != len(set(columns)) or "run_id" not in columns
                    or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) for column in columns)):
                raise ArchiveIntegrityError("archive_column_contract_invalid")
            table_files: set[str] = set()
            for entry in table["files"]:
                name = entry["path"]
                self._path(name)
                if name in self.files or not name.startswith(table_name + "/"):
                    raise ArchiveIntegrityError("archive_duplicate_or_misbound_file")
                self.files[name] = entry
                table_files.add(name)
            run_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            seen_groups = set()
            group_rows = 0
            for group in table["row_groups"]:
                key = group["path"], group["index"]
                if key in seen_groups or key[0] not in table_files:
                    raise ArchiveIntegrityError("archive_duplicate_or_missing_row_group")
                seen_groups.add(key)
                if sum(item["rows"] for item in group["runs"].values()) != group["rows"]:
                    raise ArchiveIntegrityError("archive_group_count_mismatch")
                group_rows += group["rows"]
                for run_id in group["runs"]:
                    run_groups[run_id].append(group)
            if group_rows != table["row_count"] or set(run_groups) != set(table["run_hashes"]):
                raise ArchiveIntegrityError("archive_table_count_or_run_closure_mismatch")
            for run_id, groups in run_groups.items():
                if sum(group["runs"][run_id]["rows"] for group in groups) != table["run_hashes"][run_id]["rows"]:
                    raise ArchiveIntegrityError("archive_run_count_mismatch")
            self.run_groups[table_name] = dict(run_groups)
        self.run_ids = frozenset(self.run_groups["screening_runs"])
        if any(set(runs) - self.run_ids for runs in self.run_groups.values()):
            raise ArchiveIntegrityError("archive_orphan_run")
        self.metrics = {"opened_file_count": 0, "read_row_group_count": 0, "returned_row_count": 0,
                        "query_count": 0, "query_elapsed_seconds": 0.0}

    def _path(self, name: str) -> Path:
        path = (self.directory / name).resolve()
        if Path(name).is_absolute() or path.suffix != ".parquet" or not path.is_relative_to(self.directory):
            raise ArchiveIntegrityError("archive_file_path_escapes_manifest")
        return path

    def _check_manifest(self) -> None:
        if _file_identity(self.manifest_path) != self.manifest_identity:
            raise ArchiveIntegrityError("archive_manifest_changed_during_read")

    def _check_file(self, name: str) -> tuple[Path, tuple[int, int, int, int]]:
        self._check_manifest()
        path = self._path(name)
        entry = self.files[name]
        identity = _file_identity(path)
        if identity[1] != entry["bytes"]:
            raise ArchiveIntegrityError("archive_file_size_mismatch")
        _verified_file(str(path), identity, entry["sha256"])
        return path, identity

    def _table(self, table_name: str) -> dict[str, Any]:
        if table_name not in TABLES:
            raise ValueError("unsupported_screening_archive_table")
        return self.manifest["tables"][table_name]

    def columns(self, table_name: str) -> list[str]:
        return ["__source_rowid", *[column["name"] for column in self._table(table_name)["columns"]]]

    def count(self, table_name: str, run_id: str) -> int:
        table = self._table(table_name)
        self._check_manifest()
        if run_id not in self.run_ids:
            raise KeyError("screening_archive_run_not_found")
        return int((table["run_hashes"].get(run_id) or {}).get("rows", 0))

    def iter_run_batches(self, table_name: str, run_id: str, *, columns: Sequence[str] | None = None) -> Iterator[Any]:
        """Yield verified Arrow groups; fully consume the iterator for run closure."""
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        expected_count = self.count(table_name, run_id)
        all_columns = self.columns(table_name)
        selected_columns = list(columns) if columns is not None else all_columns
        if not selected_columns or len(set(selected_columns)) != len(selected_columns) or set(selected_columns) - set(all_columns):
            raise ValueError("unsupported_or_duplicate_archive_projection")
        started = time.monotonic()
        self.metrics["query_count"] += 1
        count = 0
        digest = hashlib.sha256()
        previous_rowid = None
        try:
            for group in self.run_groups[table_name].get(run_id, []):
                path, identity = self._check_file(group["path"])
                with pq.ParquetFile(path) as reader:
                    self.metrics["opened_file_count"] += 1
                    frame = reader.read_row_group(group["index"], use_threads=False)
                frame = frame.filter(pc.equal(frame["run_id"], run_id))
                rows = list(zip(*(frame[column].to_pylist() for column in all_columns)))
                leaf = group["runs"][run_id]
                signature = _row_hash(rows)
                if len(rows) != leaf["rows"] or signature != leaf["sha256"]:
                    raise ArchiveIntegrityError("archive_run_values_mismatch")
                rowids = [row[0] for row in rows]
                if rowids != sorted(set(rowids)) or (previous_rowid is not None and rowids and rowids[0] <= previous_rowid):
                    raise ArchiveIntegrityError("archive_source_rowid_order_mismatch")
                if rowids:
                    previous_rowid = rowids[-1]
                if _file_identity(path) != identity:
                    raise ArchiveIntegrityError("archive_file_changed_during_read")
                self._check_manifest()
                digest.update(bytes.fromhex(signature))
                count += len(rows)
                self.metrics["read_row_group_count"] += 1
                self.metrics["returned_row_count"] += len(rows)
                yield frame.select(selected_columns)
            expected = self._table(table_name)["run_hashes"].get(run_id)
            if count != expected_count or (expected is not None and digest.hexdigest() != expected["sha256"]):
                raise ArchiveIntegrityError("archive_run_hash_closure_mismatch")
        finally:
            self.metrics["query_elapsed_seconds"] += time.monotonic() - started

    def grouped_counts(self, table_name: str, run_ids: Sequence[str], *, group_by: Sequence[str]) -> list[tuple]:
        """Bounded, parameterized analytical projection through DuckDB."""
        import duckdb

        self._table(table_name)
        codes = list(dict.fromkeys(run_ids))
        if not codes or len(codes) > 128 or set(codes) - self.run_ids:
            raise ValueError("archive_aggregation_requires_1_to_128_known_runs")
        fields = list(group_by)
        if not fields or len(fields) != len(set(fields)) or set(fields) - set(self.columns(table_name)):
            raise ValueError("unsupported_archive_grouping")
        names = sorted({group["path"] for code in codes for group in self.run_groups[table_name].get(code, [])})
        if not names:
            return []
        checked = [self._check_file(name) for name in names]
        paths = [str(path) for path, _ in checked]
        projection = ", ".join(f'"{field}"' for field in fields)
        ordering = ", ".join(f'"{field}" NULLS FIRST' for field in fields)
        query = (f"SELECT {projection}, COUNT(*) AS row_count FROM read_parquet(?, hive_partitioning=false) "
                 f"WHERE run_id IN (SELECT unnest(?)) GROUP BY {projection} ORDER BY {ordering}")
        started = time.monotonic()
        self.metrics["query_count"] += 1
        with duckdb.connect(":memory:", config={"threads": 1, "memory_limit": "192MB", "temp_directory": "",
                                                "autoinstall_known_extensions": False, "autoload_known_extensions": False}) as conn:
            result = conn.execute(query, [paths, codes]).fetchall()
        if sum(row[-1] for row in result) != sum(self.count(table_name, run_id) for run_id in codes):
            raise ArchiveIntegrityError("archive_aggregation_row_count_mismatch")
        for path, identity in checked:
            if _file_identity(path) != identity:
                raise ArchiveIntegrityError("archive_file_changed_during_aggregation")
        self._check_manifest()
        self.metrics["opened_file_count"] += len(paths)
        self.metrics["query_elapsed_seconds"] += time.monotonic() - started
        return result
