#!/usr/bin/env python3
"""Export an immutable screening shard to a verified Parquet analysis copy.

The SQLite source and production manifest are never mutated. Dates retain their
original text representation instead of trusting SQLite's NUM type affinity.
Every row group and every run is checked against SHA-256 of the original values.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))
from database.screening_decision_payloads import canonical_source, verify_reference_scope


TABLES = (
    "screening_runs", "screening_decisions", "screening_picks", "screening_pick_metric_snapshots",
    "screening_decision_reason_summary", "screening_run_latest_activity", "screening_run_storage_stats",
)


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rows_sha256(rows: list[tuple[Any, ...]]) -> str:
    # JSON preserves NULL, string, integer and floating values. Batch boundaries
    # are recorded in the manifest, making verification independent of Arrow's
    # buffer padding/dictionary layout and Parquet compression implementation.
    value = json.dumps(rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _source_connection(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro&immutable=1", uri=True)


def _schema(info: list[tuple]) -> pa.Schema:
    fields = [pa.field("__source_rowid", pa.int64(), nullable=False)]
    for column in info:
        name, affinity = column[1], column[2].upper()
        if affinity in {"INT", "INTEGER"} or name == "passed":
            field_type = pa.int64()
        elif affinity == "REAL":
            field_type = pa.float64()
        elif affinity in {"TEXT", "NUM"}:
            field_type = pa.string()
        else:
            raise ValueError(f"unsupported SQLite type: {name} {affinity}")
        fields.append(pa.field(name, field_type))
    return pa.schema(fields)


def _run_leaves(rows: list[tuple], run_index: int) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[tuple]] = defaultdict(list)
    for row in rows:
        groups[row[run_index]].append(row)
    return {run: {"rows": len(values), "sha256": _rows_sha256(values)} for run, values in groups.items()}


def _accumulate_run_hashes(target: dict, leaves: dict) -> None:
    for run_id, leaf in leaves.items():
        value = target.setdefault(run_id, {"rows": 0, "hash": hashlib.sha256()})
        value["rows"] += leaf["rows"]
        value["hash"].update(bytes.fromhex(leaf["sha256"]))


def _serialize_run_hashes(target: dict) -> dict:
    return {key: {"rows": value["rows"], "sha256": value["hash"].hexdigest()} for key, value in sorted(target.items())}


def _accumulate_column_stats(target: dict, frame: pa.Table) -> None:
    for field, column in zip(frame.schema, frame.columns):
        target["null_counts"][field.name] = target["null_counts"].get(field.name, 0) + column.null_count
        if (pa.types.is_integer(field.type) or pa.types.is_floating(field.type)
                or field.name in {"fcode", "run_id"} or field.name.endswith(("_date", "_at"))):
            bounds = pc.min_max(column).as_py()
            previous = target["ranges"].setdefault(field.name, {"min": None, "max": None})
            for key, compare in [("min", min), ("max", max)]:
                if bounds[key] is not None:
                    previous[key] = bounds[key] if previous[key] is None else compare(previous[key], bounds[key])


def _export_file(directory: Path, relative: str) -> Path:
    path = (directory / relative).resolve()
    if not path.is_relative_to(directory.resolve()) or path.suffix != ".parquet":
        raise ValueError(f"invalid export file: {relative}")
    return path


def verify_export(directory: Path, manifest: dict | None = None) -> dict[str, Any]:
    if manifest is None:
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("status") != "verified":
            raise ValueError("unverified export")
    if set(manifest.get("tables", {})) != set(TABLES):
        raise ValueError("export must contain all seven screening tables")
    rows_total = 0
    for table_name, table in manifest["tables"].items():
        verified_rows = 0
        run_hashes: dict = {}
        stats: dict = {"null_counts": {}, "ranges": {}}
        readers: dict[str, pq.ParquetFile] = {}
        seen_groups: set[tuple[str, int]] = set()
        try:
            for file in table["files"]:
                path = _export_file(directory, file["path"])
                if file["path"] in readers or _file_sha256(path) != file["sha256"]:
                    raise ValueError(f"Parquet file checksum/identity mismatch: {file['path']}")
                readers[file["path"]] = pq.ParquetFile(path)
            for group in table["row_groups"]:
                key = (group["path"], group["index"])
                if key in seen_groups:
                    raise ValueError(f"duplicate row group: {key}")
                seen_groups.add(key)
                frame = readers[group["path"]].read_row_group(group["index"])
                rows = list(zip(*(frame.column(i).to_pylist() for i in range(frame.num_columns))))
                if len(rows) != group["rows"] or _rows_sha256(rows) != group["sha256"]:
                    raise ValueError(f"source values mismatch: {table_name} {group['path']}:{group['index']}")
                leaves = _run_leaves(rows, frame.schema.get_field_index("run_id"))
                if leaves != group["runs"]:
                    raise ValueError(f"per-run values mismatch: {table_name}")
                _accumulate_run_hashes(run_hashes, leaves)
                _accumulate_column_stats(stats, frame)
                verified_rows += len(rows)
            expected_groups = {(path, i) for path, reader in readers.items() for i in range(reader.num_row_groups)}
            if seen_groups != expected_groups or any(stats[key] != table[key] for key in stats):
                raise ValueError(f"row groups/NULL/range closure mismatch: {table_name}")
        finally:
            for reader in readers.values():
                reader.close()
        if verified_rows != table["row_count"] or _serialize_run_hashes(run_hashes) != table["run_hashes"]:
            raise ValueError(f"table/run closure mismatch: {table_name}")
        rows_total += verified_rows
        print(json.dumps({"event": "table_verified", "table": table_name, "rows": verified_rows}), flush=True)
    return {"passed": True, "verified_table_count": len(manifest["tables"]), "verified_row_count": rows_total}


def export_shard(source: Path, destination: Path, shard: dict, *, batch_rows: int = 16384,
                 target_file_mib: int = 128) -> dict[str, Any]:
    source = source.resolve()
    if batch_rows < 1 or target_file_mib < 1:
        raise ValueError("batch_rows and target_file_mib must be positive")
    if destination.exists():
        raise FileExistsError(f"destination already exists: {destination}")
    stat = source.stat()
    if shard.get("state") != "immutable" or not shard.get("sha256"):
        raise ValueError("only manifest-sealed, SHA-256-bound immutable shards may be exported")
    if stat.st_size != shard.get("file_size_bytes") or stat.st_mtime_ns != shard.get("file_mtime_ns"):
        raise ValueError("immutable source no longer matches its manifest")
    if any(Path(str(source) + suffix).exists() for suffix in ("-wal", "-journal")):
        raise ValueError("immutable source has a journal/WAL")
    staging = destination.with_name(destination.name + ".partial-" + uuid4().hex[:8])
    staging.mkdir(parents=True)
    started = time.monotonic()
    manifest: dict[str, Any] = {
        "schema_version": 1, "status": "building", "analysis_only": True,
        "source": str(source), "source_sha256": shard["sha256"], "source_size_bytes": stat.st_size,
        "created_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(), "tables": {},
        "date_storage": "original SQLite text, without date coercion",
        "type_conversion_failure_count": 0,
        "run_hash_method": "SHA256(concatenated SHA256 digests of run rows in recorded source-row-group order)",
    }
    conn = _source_connection(source)
    try:
        run_partitions = {}
        for run_id, started_at, fund_type in conn.execute("SELECT run_id, started_at, fund_type FROM screening_runs"):
            if not run_id or run_id in run_partitions:
                raise ValueError(f"invalid or duplicate source run_id: {run_id!r}")
            month = str(started_at or "")[:7]
            if not re.fullmatch(r"\d{4}-\d{2}", month) or not re.fullmatch(r"[a-zA-Z0-9_-]+", str(fund_type or "")):
                raise ValueError(f"invalid run partition metadata: {run_id}")
            run_partitions[run_id] = f"fund_type={fund_type}/run_month={month}"
        verify_reference_scope(conn, list(run_partitions))
        for table_name in TABLES:
            info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            if not info:
                raise ValueError(f"missing source table: {table_name}")
            schema = _schema(info)
            run_index = schema.get_field_index("run_id")
            table: dict[str, Any] = {"columns": [{"name": r[1], "sqlite_type": r[2]} for r in info],
                                     "row_count": 0, "null_counts": {}, "ranges": {}, "row_groups": [], "files": []}
            manifest["tables"][table_name] = table
            writers: dict = {}
            run_hashes: dict = {}
            projection, table_source = canonical_source(conn, table_name, [row[1] for row in info])
            cursor = conn.execute(f"SELECT h.rowid,{projection} FROM {table_source} ORDER BY h.rowid")
            try:
                while rows := cursor.fetchmany(batch_rows):
                    partitions: dict[str, list[tuple]] = defaultdict(list)
                    for row in rows:
                        partitions[run_partitions[row[run_index]]].append(row)
                    for partition, values in partitions.items():
                        frame = pa.Table.from_arrays(
                            [pa.array(items, type=field.type, safe=True) for items, field in zip(zip(*values), schema)],
                            schema=schema,
                        )
                        state = writers.setdefault(partition, {"number": 0, "writer": None})
                        if state["writer"] is None:
                            relative = f"{table_name}/{partition}/part-{state['number']:05d}.parquet"
                            path = staging / relative
                            path.parent.mkdir(parents=True, exist_ok=True)
                            state.update(path=relative, group=0, writer=pq.ParquetWriter(
                                path, schema, compression="zstd", compression_level=6, use_dictionary=True,
                            ))
                        state["writer"].write_table(frame, row_group_size=len(values))
                        leaves = _run_leaves(values, run_index)
                        _accumulate_run_hashes(run_hashes, leaves)
                        table["row_groups"].append({"path": state["path"], "index": state["group"],
                                                   "rows": len(values), "sha256": _rows_sha256(values), "runs": leaves})
                        state["group"] += 1
                        table["row_count"] += len(values)
                        _accumulate_column_stats(table, frame)
                        if (staging / state["path"]).stat().st_size >= target_file_mib * 1024 * 1024:
                            state["writer"].close()
                            state["writer"] = None
                            state["number"] += 1
                    if table["row_count"] % (batch_rows * 64) < batch_rows:
                        print(json.dumps({"event": "export_progress", "table": table_name, "rows": table["row_count"]}), flush=True)
            finally:
                for state in writers.values():
                    if state["writer"] is not None:
                        state["writer"].close()
            expected = shard["table_row_counts"][table_name]
            if table["row_count"] != expected:
                raise ValueError(f"manifest source count mismatch for {table_name}: {table['row_count']} != {expected}")
            table["run_hashes"] = _serialize_run_hashes(run_hashes)
            for path in sorted((staging / table_name).rglob("*.parquet")):
                table["files"].append({"path": str(path.relative_to(staging)), "bytes": path.stat().st_size, "sha256": _file_sha256(path)})
            _write_json(staging / "manifest.json", manifest)
            print(json.dumps({"event": "table_exported", "table": table_name, "rows": table["row_count"]}), flush=True)
        manifest["verification"] = verify_export(staging, manifest)
        print(json.dumps({"event": "source_checksum_started", "source": str(source)}), flush=True)
        if (_file_sha256(source) != shard["sha256"] or source.stat().st_mtime_ns != stat.st_mtime_ns
                or source.stat().st_size != stat.st_size):
            raise ValueError("source checksum changed or mismatches immutable manifest")
        manifest["parquet_size_bytes"] = sum(f["bytes"] for t in manifest["tables"].values() for f in t["files"])
        manifest["compression_ratio"] = manifest["parquet_size_bytes"] / stat.st_size
        manifest["elapsed_seconds"] = round(time.monotonic() - started, 3)
        manifest["status"] = "verified"
        _write_json(staging / "manifest.json", manifest)
        staging.replace(destination)
        return {key: manifest[key] for key in ["status", "source_size_bytes", "parquet_size_bytes", "compression_ratio", "elapsed_seconds", "verification"]}
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = f"{type(exc).__name__}: {exc}"
        _write_json(staging / "manifest.json", manifest)
        raise
    finally:
        conn.close()


def restore_run(directory: Path, run_id: str, destination: Path) -> dict:
    """Restore one real run to a new SQLite file for recovery exercises."""
    if destination.exists():
        raise FileExistsError(destination)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "verified":
        raise ValueError("unverified export")
    if set(manifest.get("tables", {})) != set(TABLES):
        raise ValueError("export must contain all seven screening tables")
    if run_id not in manifest["tables"]["screening_runs"]["run_hashes"]:
        raise ValueError("run not present")
    counts = {}
    staging = destination.with_name(destination.name + ".partial-" + uuid4().hex[:8])
    destination.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(staging)
    try:
        for name, table in manifest["tables"].items():
            columns = table["columns"]
            conn.execute(f"CREATE TABLE {name} (" + ",".join(f'"{c["name"]}" {c["sqlite_type"]}' for c in columns) + ")")
            count = 0
            restored_hashes: dict = {}
            checked_files = set()
            for group in table["row_groups"]:
                if run_id not in group["runs"]:
                    continue
                path = _export_file(directory, group["path"])
                if path not in checked_files:
                    file = next(f for f in table["files"] if f["path"] == group["path"])
                    if _file_sha256(path) != file["sha256"]:
                        raise ValueError(f"restore source checksum mismatch: {path}")
                    checked_files.add(path)
                with pq.ParquetFile(path) as reader:
                    frame = reader.read_row_group(group["index"])
                selected = frame.filter(pc.equal(frame["run_id"], run_id))
                rows = list(zip(*(column.to_pylist() for column in selected.columns)))
                if len(rows) != group["runs"][run_id]["rows"] or _rows_sha256(rows) != group["runs"][run_id]["sha256"]:
                    raise ValueError(f"restore value hash mismatch: {name}")
                names = ["rowid", *(c["name"] for c in columns)]
                conn.executemany(f"INSERT INTO {name} ({','.join(names)}) VALUES ({','.join('?' for _ in names)})", rows)
                restored = conn.execute(
                    f"SELECT rowid, * FROM {name} WHERE run_id=? AND rowid BETWEEN ? AND ? ORDER BY rowid",
                    (run_id, rows[0][0], rows[-1][0]),
                ).fetchall()
                if _rows_sha256(restored) != group["runs"][run_id]["sha256"]:
                    raise ValueError(f"restored SQLite value hash mismatch: {name}")
                _accumulate_run_hashes(restored_hashes, _run_leaves(restored, frame.schema.get_field_index("run_id")))
                count += len(rows)
            expected = {run_id: table["run_hashes"][run_id]} if run_id in table["run_hashes"] else {}
            if _serialize_run_hashes(restored_hashes) != expected:
                raise ValueError(f"restore count mismatch: {name}")
            counts[name] = count
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("restored database quick_check failed")
        conn.commit()
    except Exception:
        conn.close()
        staging.unlink(missing_ok=True)
        raise
    finally:
        conn.close()
    staging.replace(destination)
    return {"run_id": run_id, "restored_counts": counts, "quick_check": "ok", "path": str(destination)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("backend/data/archives/screening_history_archive_manifest.json"))
    parser.add_argument("--shard-id", default="")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--restore-run", default="")
    parser.add_argument("--restore-db", type=Path)
    args = parser.parse_args()
    if args.restore_run:
        if not args.restore_db:
            parser.error("--restore-db is required")
        result = restore_run(args.output, args.restore_run, args.restore_db)
    elif args.verify_only:
        result = verify_export(args.output)
    else:
        source_manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        shards = [s for s in source_manifest.get("immutable_shards", []) if not args.shard_id or s["id"] == args.shard_id]
        if len(shards) != 1:
            parser.error("select exactly one immutable shard with --shard-id")
        shard = shards[0]
        result = export_shard(args.manifest.parent / shard["path"], args.output, shard)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
