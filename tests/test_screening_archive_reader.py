"""Compare bounded reader results against isolated, synthetic SQLite archives."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.screening_archive_reader import ArchiveIntegrityError, ScreeningArchiveReader, TABLES
from test_screening_parquet import archive_fixture
from scripts.export_screening_parquet import export_shard


@pytest.fixture
def archive(tmp_path):
    source, shard = archive_fixture(tmp_path)
    directory = tmp_path / "parquet"
    export_shard(source, directory, shard, batch_rows=2)
    signature = hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()
    return source, directory, shard, signature


def reader_for(archive):
    _, directory, shard, signature = archive
    return ScreeningArchiveReader(directory, manifest_sha256=signature, source_sha256=shard["sha256"])


def test_all_tables_preserve_full_values_and_row_order_without_sqlite_restore(archive):
    source, _, _, _ = archive
    reader = reader_for(archive)
    with sqlite3.connect(source) as conn:
        for run_id in ["run-a", "run-b", "run-c"]:
            for name in TABLES:
                rows = [tuple(row[column] for column in reader.columns(name))
                        for frame in reader.iter_run_batches(name, run_id) for row in frame.to_pylist()]
                expected = conn.execute(f'SELECT rowid,* FROM "{name}" WHERE run_id=? ORDER BY rowid', (run_id,)).fetchall()
                assert rows == expected
                assert reader.count(name, run_id) == len(expected)
        expected = conn.execute("SELECT value,passed,COUNT(*) FROM screening_decisions GROUP BY value,passed ORDER BY value,passed").fetchall()
        assert reader.grouped_counts("screening_decisions", ["run-a", "run-b", "run-c"], group_by=["value", "passed"]) == expected
    projected = [row for frame in reader.iter_run_batches("screening_decisions", "run-b", columns=["fcode", "value"]) for row in frame.to_pylist()]
    assert projected == [{"fcode": "000001", "value": None}]


def test_manifest_is_explicit_and_not_discovered_from_directories(archive):
    _, directory, shard, _ = archive
    with pytest.raises(ArchiveIntegrityError, match="hash_or_identity"):
        ScreeningArchiveReader(directory, manifest_sha256="0" * 64, source_sha256=shard["sha256"])
    reader = reader_for(archive)
    with pytest.raises(KeyError, match="not_found"):
        list(reader.iter_run_batches("screening_picks", "another-run"))
    with pytest.raises(ValueError, match="table"):
        list(reader.iter_run_batches('screening_picks; DELETE FROM funds', 'run-a'))
    with pytest.raises(ValueError, match="projection"):
        list(reader.iter_run_batches("screening_picks", "run-a", columns=["invented"]))


@pytest.mark.parametrize("change", ["missing", "same_size_corruption", "manifest_replaced"])
def test_changed_files_fail_explicitly_after_an_earlier_successful_read(archive, change):
    reader = reader_for(archive)
    list(reader.iter_run_batches("screening_decisions", "run-a"))
    if change == "manifest_replaced":
        path = reader.manifest_path
        path.write_bytes(path.read_bytes() + b'\n')
    else:
        name = reader.run_groups["screening_decisions"]["run-a"][0]["path"]
        path = reader.directory / name
        if change == "missing":
            path.unlink()
        else:
            data = bytearray(path.read_bytes())
            data[8] ^= 1
            path.write_bytes(data)
    with pytest.raises((ArchiveIntegrityError, FileNotFoundError)):
        list(reader.iter_run_batches("screening_decisions", "run-a"))


def test_invalid_manifest_closure_is_rejected_even_with_matching_file_signature(archive):
    _, directory, shard, _ = archive
    path = directory / "manifest.json"
    manifest = json.loads(path.read_text())
    manifest["tables"]["screening_decisions"]["row_count"] += 1
    path.write_text(json.dumps(manifest))
    signature = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ArchiveIntegrityError, match="closure"):
        ScreeningArchiveReader(directory, manifest_sha256=signature, source_sha256=shard["sha256"])
