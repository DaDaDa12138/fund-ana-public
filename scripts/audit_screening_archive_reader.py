#!/usr/bin/env python3
"""Compare SQLite/Parquet copies or verify each production columnar shard."""
from __future__ import annotations
import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'core'))
from backend.screening_archive_reader import ScreeningArchiveReader, TABLES, _row_hash
from backend.screening_archive_shadow import archive_observation_plan
from database.screening_decision_payloads import canonical_source, verify_reference_scope


def audit_copy(root: Path, binding: dict, *, repeats: int = 3, sample_offset: int = 0) -> dict:
    source_manifest = root / "backend/data/archives/screening_history_archive_manifest.json"
    shards = json.loads(source_manifest.read_text())["immutable_shards"]
    matches = [shard for shard in shards if shard["id"] == binding["shard_id"]]
    if len(matches) != 1 or matches[0]["sha256"] != binding["source_sha256"]:
        raise ValueError("shadow_copy_source_binding_mismatch")
    shard = matches[0]
    reference = shard.get("sqlite_reference") or shard
    source = source_manifest.parent / reference["path"]
    stat = source.stat()
    if stat.st_size != reference["file_size_bytes"] or stat.st_mtime_ns != reference["file_mtime_ns"]:
        raise ValueError("sealed_sqlite_source_identity_changed")
    directory = (root / binding["directory"]).resolve()
    if not directory.is_relative_to(root.resolve()):
        raise ValueError("shadow_copy_outside_project")
    started = time.monotonic()
    reader = ScreeningArchiveReader(directory, manifest_sha256=binding["manifest_sha256"], source_sha256=binding["source_sha256"])
    init_seconds = time.monotonic() - started
    results = []
    timings = []
    with closing(sqlite3.connect(f"{source.resolve().as_uri()}?mode=ro&immutable=1", uri=True)) as conn:
        all_runs = conn.execute("SELECT run_id,started_at FROM screening_runs ORDER BY started_at,run_id").fetchall()
        positions = sorted({sample_offset % len(all_runs), (len(all_runs) // 2 + sample_offset) % len(all_runs),
                            (len(all_runs) - 1 - sample_offset) % len(all_runs)})
        samples = [all_runs[index] for index in positions]
        verify_reference_scope(conn, [run[0] for run in samples])
        for run_id, started_at in samples:
            for table in TABLES:
                columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]
                projection, table_source = canonical_source(conn, table, columns)
                sql = f'SELECT h.rowid,{projection} FROM {table_source} WHERE h.run_id=? ORDER BY h.rowid'
                query_start = time.monotonic()
                reference = conn.execute(sql, (run_id,)).fetchall()
                reference_seconds = time.monotonic() - query_start
                expected_hash = _row_hash(reference)
                values = []
                for repeat in range(max(1, repeats)):
                    query_start = time.monotonic()
                    values = [tuple(item[column] for column in reader.columns(table))
                              for frame in reader.iter_run_batches(table, run_id) for item in frame.to_pylist()]
                    elapsed = time.monotonic() - query_start
                    timings.append({"run_id": run_id, "table": table, "repeat": repeat, "elapsed_seconds": elapsed,
                                    "includes_first_file_hash_check": repeat == 0})
                    if _row_hash(values) != expected_hash:
                        raise ValueError(f"parquet_sqlite_values_differ:{table}:{run_id}")
                results.append({"run_id": run_id, "started_at": started_at, "table": table, "rows": len(reference),
                                "sha256": expected_hash, "aligned": True, "sqlite_read_seconds": reference_seconds})
        run_ids = [run[0] for run in samples]
        grouped = reader.grouped_counts("screening_decisions", run_ids, group_by=["run_id", "step_id", "passed", "reason_code"])
        reference_groups = conn.execute(
            "SELECT run_id,step_id,passed,reason_code,COUNT(*) FROM screening_decisions WHERE run_id IN ("
            + ",".join("?" for _ in run_ids) + ") GROUP BY run_id,step_id,passed,reason_code ORDER BY run_id,step_id,passed,reason_code",
            run_ids,
        ).fetchall()
        if grouped != reference_groups:
            raise ValueError("parquet_sqlite_aggregation_differs")
    return {"status": "matched", "binding": binding, "production_reader_changed": False,
            "reference_integrity_basis": "previously_verified_sealed_source_sha256_and_current_size_mtime",
            "full_sqlite_file_rescanned": False, "reader_init_seconds": init_seconds,
            "samples": [{"run_id": run, "started_at": date} for run, date in samples], "comparisons": results,
            "aggregation_aligned": True, "aggregation_group_count": len(grouped), "query_timings": timings,
            "reader_metrics": reader.metrics, "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}


def verify_copy(root: Path, binding: dict, *, repeats: int = 3, sample_offset: int = 0) -> dict:
    """After retirement, verify against the sealed export; do not claim a SQLite comparison."""
    from utils.screening_archive_access import open_parquet_shard, probe_parquet_shard
    from utils.screening_history_archive_manifest import load_screening_archive_manifest

    state = load_screening_archive_manifest(root / 'backend/data/archives/screening_history_details_archive.db',
                                            project_root=root)
    if state['status'] != 'ready':
        raise ValueError('production_archive_manifest_not_ready')
    matches = [shard for shard in state['shards'] if shard['id'] == binding['shard_id']]
    if len(matches) != 1:
        raise ValueError('production_archive_shard_binding_missing')
    shard = matches[0]
    if (shard.get('manifest_sha256') != binding['manifest_sha256']
            or shard.get('source_sha256') != binding['source_sha256']
            or Path(shard['absolute_path']).parent != (root / binding['directory']).resolve()):
        raise ValueError('production_archive_binding_mismatch')
    reader = open_parquet_shard(shard)
    runs = sorted(reader.run_ids)
    if not runs:
        raise ValueError('production_archive_has_no_runs')
    positions = sorted({sample_offset % len(runs), (len(runs) // 2 + sample_offset) % len(runs),
                        (len(runs) - 1 - sample_offset) % len(runs)})
    selected = [runs[index] for index in positions]
    probe = probe_parquet_shard(shard, selected)
    comparisons, timings = [], []
    reference_groups = Counter()
    for run_id in selected:
        for table in TABLES:
            values = []
            for repeat in range(repeats):
                started = time.monotonic()
                values = [tuple(row[column] for column in reader.columns(table))
                          for frame in reader.iter_run_batches(table, run_id) for row in frame.to_pylist()]
                timings.append({'run_id': run_id, 'table': table, 'repeat': repeat,
                                'elapsed_seconds': time.monotonic() - started})
            if table == 'screening_decisions':
                columns = reader.columns(table)
                indices = [columns.index(field) for field in ['run_id', 'step_id', 'passed', 'reason_code']]
                reference_groups.update(tuple(row[index] for index in indices) for row in values)
            comparisons.append({'run_id': run_id, 'table': table, 'rows': len(values),
                                'sha256': _row_hash(values), 'aligned': True,
                                'reference': 'sealed_manifest_row_group_and_run_hashes'})
    grouped = reader.grouped_counts('screening_decisions', selected, group_by=['run_id', 'step_id', 'passed', 'reason_code'])
    if {tuple(row[:-1]): row[-1] for row in grouped} != dict(reference_groups):
        raise ValueError('verified_archive_aggregation_mismatch')
    return {'status': 'matched', 'binding': binding, 'production_reader_changed': False,
            'reference_integrity_basis': 'sealed_export_full_file_SHA_and_run_row_hashes',
            'sqlite_reference_read': False, 'samples': [{'run_id': run_id} for run_id in selected],
            'comparisons': comparisons, 'aggregation_aligned': True, 'aggregation_group_count': len(grouped),
            'query_timings': timings, 'full_shard_integrity': probe['check'],
            'reader_metrics': reader.metrics, 'peak_process_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bindings", type=Path, default=ROOT / "config/screening_parquet_bindings.json")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--sample-offset", type=int, default=0)
    parser.add_argument("--expected-bindings-sha256", default="")
    args = parser.parse_args()
    if not 1 <= args.repeats <= 30:
        parser.error("repeats must be between 1 and 30")
    binding_bytes = args.bindings.read_bytes()
    if args.expected_bindings_sha256 and hashlib.sha256(binding_bytes).hexdigest() != args.expected_bindings_sha256:
        raise ValueError("shadow_binding_changed_before_execution")
    bindings = json.loads(binding_bytes)
    if bindings.get("schema_version") != 1 or bindings.get("production_reader") not in {"sqlite", "hybrid", "parquet"}:
        raise ValueError("unsupported_shadow_binding_contract")
    if not bindings.get("shadow_copies"):
        raise ValueError("shadow_copies_required")
    output = ROOT / "reports/audit/screening_archive_reader"
    output.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now()
    plan = archive_observation_plan(ROOT, bindings)
    report = {"generated_at": timestamp.isoformat(), "mode": plan["mode"], "observation_plan": plan,
              "bindings_sha256": hashlib.sha256(binding_bytes).hexdigest(),
              "copies": [(verify_copy if mode == 'read_only_integrity' else audit_copy)(
                            ROOT, binding, repeats=args.repeats, sample_offset=args.sample_offset)
                         for binding, mode in zip(plan['copies'], plan['copy_modes'])]}
    report["passed"] = all(copy["status"] == "matched" for copy in report["copies"])
    path = output / f"reader_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
    print(json.dumps({"passed": report["passed"], "artifact_path": str(path.relative_to(ROOT)),
                      "copies": [{"comparison_count": len(copy["comparisons"]), "peak_rss_kib": copy["peak_process_rss_kib"],
                                  "reader_metrics": copy["reader_metrics"]} for copy in report["copies"]]}, ensure_ascii=False))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
