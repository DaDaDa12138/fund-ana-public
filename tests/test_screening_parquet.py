"""Recovery/integrity checks use isolated synthetic archives, never live data."""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3
import sys

import pytest

pytest.importorskip("pyarrow")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.export_screening_parquet import TABLES, export_shard, restore_run, verify_export


def archive_fixture(tmp_path: Path) -> tuple[Path, dict]:
    source = tmp_path / "sealed.db"
    with sqlite3.connect(source) as conn:
        conn.execute("CREATE TABLE screening_runs(id INT, run_id TEXT, started_at NUM, fund_type TEXT)")
        conn.executemany("INSERT INTO screening_runs VALUES (?,?,?,?)", [
            (1, "run-a", "2026-05-06 12:01:02.123456", "bond"),
            (2, "run-b", "2026-06-01 00:00:00", "bond"),
            (3, "run-c", "2026-05-06 13:00:00", "stock"),
        ])
        for name in TABLES[1:]:
            conn.execute(f"CREATE TABLE {name}(id INT, run_id TEXT, fcode TEXT, value REAL, passed NUM, created_at NUM, metrics_json TEXT)")
            if name == "screening_run_storage_stats":
                continue  # An empty table must still be represented and restorable.
            conn.executemany(f"INSERT INTO {name}(rowid,id,run_id,fcode,value,passed,created_at,metrics_json) VALUES (?,?,?,?,?,?,?,?)", [
                (1, 11, "run-a", "000001", 0.0, 1, "2026-05-06 12:01:02.123456", '{"说明":"测试"}'),
                (3, 13, "run-b", "000001", None, 0, None, None),
                (7, 17, "run-a", "000002", -0.12345678, None, "2026-05-06", ""),
                (12, 22, "run-c", "000003", 1.5, 1, "2026-05-06 13:00:00", "{}"),
            ])
        counts = {name: conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] for name in TABLES}
    stat = source.stat()
    return source, {"state": "immutable", "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                    "file_size_bytes": stat.st_size, "file_mtime_ns": stat.st_mtime_ns, "table_row_counts": counts}


def test_full_value_roundtrip_preserves_null_zero_dates_rowids_and_empty_tables(tmp_path):
    source, shard = archive_fixture(tmp_path)
    destination = tmp_path / "parquet"
    result = export_shard(source, destination, shard, batch_rows=2)
    assert result["status"] == "verified"
    manifest = json.loads((destination / "manifest.json").read_text())
    assert datetime.fromisoformat(manifest["created_at"]).utcoffset() == timedelta(hours=8)
    assert verify_export(destination)["verified_row_count"] == sum(shard["table_row_counts"].values())
    assert hashlib.sha256(source.read_bytes()).hexdigest() == shard["sha256"]
    assert len(list(destination.glob("screening_decisions/fund_type=*/run_month=*"))) == 3
    for run_id in ["run-a", "run-b", "run-c"]:
        restored = tmp_path / f"{run_id}.db"
        assert restore_run(destination, run_id, restored)["quick_check"] == "ok"
        with sqlite3.connect(source) as original, sqlite3.connect(restored) as recovered:
            for table in TABLES:
                sql = f"SELECT rowid,* FROM {table} WHERE run_id=? ORDER BY rowid"
                assert recovered.execute(sql, (run_id,)).fetchall() == original.execute(sql, (run_id,)).fetchall()


@pytest.mark.parametrize("failure", ["active", "mtime", "checksum", "count", "bad_date_type"])
def test_incorrect_source_never_publishes_analysis_copy(tmp_path, failure):
    source, shard = archive_fixture(tmp_path)
    if failure == "active":
        shard["state"] = "active"
    elif failure == "mtime":
        shard["file_mtime_ns"] -= 1
    elif failure == "checksum":
        shard["sha256"] = "0" * 64
    elif failure == "count":
        shard["table_row_counts"]["screening_decisions"] += 1
    else:
        with sqlite3.connect(source) as conn:
            conn.execute("UPDATE screening_decisions SET created_at=42 WHERE rowid=1")
        shard.update(sha256=hashlib.sha256(source.read_bytes()).hexdigest(), file_mtime_ns=source.stat().st_mtime_ns)
    destination = tmp_path / "parquet"
    with pytest.raises((ValueError, TypeError)):
        export_shard(source, destination, shard, batch_rows=2)
    assert not destination.exists()


def test_corrupt_export_blocks_verification_and_atomic_restore(tmp_path):
    source, shard = archive_fixture(tmp_path)
    destination = tmp_path / "parquet"
    export_shard(source, destination, shard, batch_rows=2)
    path = next(destination.glob("screening_picks/fund_type=bond/run_month=2026-05/*.parquet"))
    with path.open("ab") as stream:
        stream.write(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        verify_export(destination)
    restored = tmp_path / "restore.db"
    with pytest.raises(ValueError, match="checksum"):
        restore_run(destination, "run-a", restored)
    assert not restored.exists()
    assert not list(tmp_path.glob("restore.db.partial-*"))


def test_incomplete_manifest_or_modified_stats_are_rejected(tmp_path):
    source, shard = archive_fixture(tmp_path)
    destination = tmp_path / "parquet"
    export_shard(source, destination, shard, batch_rows=2)
    path = destination / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["tables"]["screening_decisions"]["null_counts"]["value"] += 1
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="closure mismatch"):
        verify_export(destination)
    path.write_text(json.dumps({"status": "verified", "tables": {}}))
    with pytest.raises(ValueError, match="seven"):
        verify_export(destination)
