"""Synthetic storage contracts; no fixture is evidence of fund performance."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import gzip
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import shutil
import sqlite3
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.utils import calculation_capture as capture
from core.utils import calculation_pack_storage as packs
from scripts import cleanup_disk_usage


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    source = tmp_path / "synthetic_source.py"
    source.write_text("# Synthetic storage fixture; no fund data.\n")
    empty = tmp_path / "synthetic_empty.py"
    empty.write_bytes(b"")
    pool = tmp_path / "pool"
    monkeypatch.setattr(capture, "PROJECT_ROOT", tmp_path)
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", "synthetic_pack_capture")
    monkeypatch.delenv(capture.ENV_CAPTURE_RUN_ID, raising=False)
    monkeypatch.delenv(capture.ENV_CAPTURE_SCOPE, raising=False)
    monkeypatch.setenv(packs.ENV_SHARED_DIRECTORY, str(pool))
    return SimpleNamespace(root=tmp_path, sources=[source, empty], pool=pool)


def frame():
    result = pd.DataFrame({"nullable": pd.Series([None, 0, 3], dtype="Int64"),
                           "raw": pd.Series([None, np.nan, pd.NA], dtype=object),
                           "float": [0.0, -0.0, np.nan],
                           "code": pd.Series(["synthetic_a", None, ""], dtype="string")})
    result.index = pd.Index([7, 0, 3], name="original_order")
    return result


def writer(fixture, directory="first"):
    return capture.CaptureWriter(fixture.root / directory, "synthetic", {}, code_paths=fixture.sources)


def finish(fixture, directory="first", value=None):
    current = writer(fixture, directory)
    current.event("storage_fixture", frame() if value is None else value)
    manifest = current.finish("returned")
    return current.directory / (manifest["session_id"] + ".manifest.json"), manifest


def checked(path):
    return capture.validate_journal(path, expected_run_id="synthetic_pack_capture")


def test_shared_packs_preserve_all_values_sources_and_local_offline_replay(fixture):
    first, one = finish(fixture)
    second, two = finish(fixture, "second")
    assert one["schema_version"] == two["schema_version"] == 3
    assert two["storage"]["stats"]["shared_objects_reused"] == 3
    assert not two["storage"]["stats"].get("new_objects")
    for member in one["storage"]["packs"]:
        assert (first.parent / member["file"]).samefile(second.parent / member["file"])
    raw = [json.loads(row) for row in gzip.decompress((second.parent / two["file"]).read_bytes()).splitlines()]
    assert all("packed" in row and "value" not in row for row in raw if row["record"] == "object")
    assert two["stored_uncompressed_bytes"] < two["uncompressed_bytes"] + 2048
    shutil.rmtree(first.parent)
    preview = packs.collect_unused_packs(fixture.pool, min_age_seconds=0, apply=True)
    assert not preview["removed"]
    shutil.rmtree(fixture.pool)
    assert checked(second)["complete"]
    rows = list(capture.iter_journal(second.parent / two["file"]))
    actual = capture.decode(next(row["value"] for row in rows if row["record"] == "object"))
    pd.testing.assert_frame_equal(actual, frame(), check_exact=True)
    assert actual.iloc[0]["raw"] is None and np.isnan(actual.iloc[1]["raw"])
    assert actual.iloc[2]["raw"] is pd.NA and np.signbit(actual.iloc[1]["float"])
    import base64

    assert {row["path"]: base64.b64decode(row["bytes_base64"]) for row in rows if row["record"] == "source"} == {
        path.name: path.read_bytes() for path in fixture.sources}


def test_multiple_sessions_in_one_directory_reuse_existing_local_link(fixture):
    first, one = finish(fixture)
    second, two = finish(fixture)
    assert first != second and one["storage"]["packs"] == two["storage"]["packs"]
    assert len(list(first.parent.glob("*.pack"))) == 1
    assert checked(first)["complete"] and checked(second)["complete"]


def test_worker_threads_share_serialized_catalogue_and_capture(fixture):
    current = writer(fixture)
    with ThreadPoolExecutor(max_workers=4) as workers:
        list(workers.map(lambda i: current.event("worker", frame().assign(worker=i)), range(12)))
    manifest = current.finish("returned")
    assert manifest["object_count"] == 12
    assert checked(current.directory / (manifest["session_id"] + ".manifest.json"))["complete"]


def test_shared_pool_budget_keeps_complete_local_inputs(fixture, monkeypatch):
    monkeypatch.setattr(packs, "MAX_POOL_BYTES", 1)
    path, manifest = finish(fixture)
    assert not list(fixture.pool.glob("*.pack"))
    assert manifest["storage"]["stats"]["local_packs_without_cache_budget"] == 1
    assert checked(path)["complete"]


def test_low_space_cannot_initialize_a_complete_capture(fixture, monkeypatch):
    monkeypatch.setattr(packs, "MIN_FREE_BYTES", 10**30)
    current = capture.CaptureWriter.__new__(capture.CaptureWriter)
    with pytest.raises(capture.CalculationCaptureError, match="disk_budget_exhausted"):
        current.__init__(fixture.root / "limited", "synthetic", {}, code_paths=fixture.sources)
    assert current.raw.closed
    assert not list(current.directory.glob("*.manifest.json"))


def test_packed_bytes_obey_directory_budget_and_keep_failed_partial(fixture, monkeypatch):
    current = writer(fixture)
    limit = sum(p.stat().st_size for p in current.directory.iterdir()) + 4096
    monkeypatch.setattr(capture, "MAX_DIRECTORY_BYTES", limit)
    with pytest.raises(capture.CalculationCaptureError, match="directory_budget_exhausted"):
        current.event("large_synthetic_bytes", pd.DataFrame({"bytes": [os.urandom(20000).hex()]}))
    with pytest.raises(capture.CalculationCaptureError):
        current.finish("returned")
    assert current.closed and current.raw.closed
    assert not list(current.directory.glob("*.manifest.json"))
    assert list(current.directory.glob("*.pack.partial"))
    assert sum(p.stat().st_size for p in current.directory.iterdir()) <= limit


def _sparse_pool_capture(fixture):
    current = writer(fixture)
    current.event("unrelated_synthetic_bytes", pd.DataFrame({"bytes": [os.urandom(60000).hex()]}))
    current.event("shared_value", frame())
    return current.finish("returned")


def test_sparse_reuse_fits_budget_without_unrelated_bytes_and_replays_offline(fixture, monkeypatch):
    original = _sparse_pool_capture(fixture)
    assert sum(row["bytes"] for row in original["storage"]["packs"]) > 40000
    monkeypatch.setattr(packs, "MIN_SPARSE_REUSE_PACK_BYTES", 1024)
    monkeypatch.setattr(capture, "MAX_DIRECTORY_BYTES", 20000)
    path, manifest = finish(fixture, "compact")
    assert manifest["storage"]["stats"]["sparse_shared_objects_compacted"] >= 1
    assert sum(p.stat().st_size for p in path.parent.iterdir()) <= 20000
    assert not ({row["file"] for row in original["storage"]["packs"]}
                & {row["file"] for row in manifest["storage"]["packs"]})
    shutil.rmtree(fixture.root / "first")
    shutil.rmtree(fixture.pool)
    assert checked(path)["complete"]
    rows = list(capture.iter_journal(path.parent / manifest["file"]))
    value = capture.decode(next(row["value"] for row in rows if row["record"] == "object"))
    pd.testing.assert_frame_equal(value, frame(), check_exact=True)


def test_compacted_operands_are_reused_by_later_captures(fixture, monkeypatch):
    original = _sparse_pool_capture(fixture)
    monkeypatch.setattr(packs, "MIN_SPARSE_REUSE_PACK_BYTES", 1024)
    compact_path, compact = finish(fixture, "compact_once")
    reused_path, reused = finish(fixture, "reuse_compact")
    assert compact["storage"]["stats"]["sparse_shared_objects_compacted"] >= 1
    assert reused["storage"]["stats"].get("new_objects", 0) == 0
    assert reused["storage"]["stats"].get("sparse_shared_objects_compacted", 0) == 0
    assert reused["storage"]["packs"] == compact["storage"]["packs"]
    assert reused["storage"]["packs"] != original["storage"]["packs"]
    shutil.rmtree(fixture.pool)
    assert checked(compact_path)["complete"] and checked(reused_path)["complete"]


def test_cache_full_eviction_preserves_local_replay_and_accepts_new_lookup(fixture, monkeypatch):
    original, manifest = finish(fixture)
    reserve = 1024**2
    with packs._pool_lock(fixture.pool):
        connection = packs._catalog(fixture.pool)
        monkeypatch.setattr(packs, "MAX_POOL_BYTES", packs._pool_size(fixture.pool) + reserve)
        result = packs._make_cache_room(connection, fixture.pool, 1, reserve)
        assert result["can_cache"] and result["cache_lookup_links_evicted"] == 1
        assert result["cache_object_mappings_evicted"] == 3
        assert connection.execute("SELECT COUNT(*) FROM packs").fetchone()[0] == 0
        connection.close()
    assert not list(fixture.pool.glob("*.pack"))
    assert checked(original)["complete"]
    replacement, _ = finish(fixture, "after_eviction")
    shutil.rmtree(fixture.pool)
    assert checked(original)["complete"] and checked(replacement)["complete"]


def test_catalogue_freelist_pages_are_reusable_without_more_eviction(fixture, monkeypatch):
    original, _ = finish(fixture)
    with packs._pool_lock(fixture.pool):
        connection = packs._catalog(fixture.pool)
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany("INSERT INTO objects VALUES (?,?,?,?,?,?,?)", [
            ("chunk", hashlib.sha256(str(i).encode()).digest(), "synthetic_unbound", 32, 1, 1, b"x" * 32)
            for i in range(15000)])
        connection.execute("DELETE FROM objects WHERE pack='synthetic_unbound'")
        connection.commit()
        physical = (fixture.pool / packs.CATALOG).stat().st_size
        assert physical > packs._catalog_used_bytes(connection) + 1024**2
        monkeypatch.setattr(packs, "MAX_CATALOG_BYTES", physical)
        result = packs._make_cache_room(connection, fixture.pool, 1, 1024**2)
        assert result["can_cache"] and not result["cache_lookup_links_evicted"]
        connection.close()
    assert checked(original)["complete"]


def test_cache_eviction_preserves_single_link_orphans_and_low_space(fixture, monkeypatch):
    original, manifest = finish(fixture)
    with packs._pool_lock(fixture.pool):
        connection = packs._catalog(fixture.pool)
        monkeypatch.setattr(packs, "MIN_FREE_BYTES", 10**30)
        low_space = packs._make_cache_room(connection, fixture.pool, 1, 1024**2)
        assert not low_space["can_cache"] and not low_space["cache_lookup_links_evicted"]
        monkeypatch.setattr(packs, "MIN_FREE_BYTES", 0)
        shutil.rmtree(original.parent)
        monkeypatch.setattr(packs, "MAX_POOL_BYTES", packs._pool_size(fixture.pool) + 1024**2)
        result = packs._make_cache_room(connection, fixture.pool, 1, 1024**2)
        assert not result["can_cache"] and not result["cache_lookup_links_evicted"]
        assert (fixture.pool / manifest["storage"]["packs"][0]["file"]).is_file()
        connection.close()


def test_cache_eviction_is_bounded_and_keeps_all_local_files(fixture, monkeypatch):
    first, one = finish(fixture)
    second, two = finish(fixture, "second", value=frame().assign(extra=1))
    with packs._pool_lock(fixture.pool):
        connection = packs._catalog(fixture.pool)
        monkeypatch.setattr(packs, "MAX_CACHE_EVICTIONS", 1)
        total = packs._pool_size(fixture.pool)
        largest = max(row["bytes"] for row in one["storage"]["packs"] + two["storage"]["packs"])
        monkeypatch.setattr(packs, "MAX_POOL_BYTES", total + 1024**2 - largest)
        result = packs._make_cache_room(connection, fixture.pool, 1, 1024**2)
        assert result["cache_lookup_links_evicted"] == 1 and not result["can_cache"]
        connection.close()
    shutil.rmtree(fixture.pool)
    assert checked(first)["complete"] and checked(second)["complete"]


def test_cache_eviction_commit_failure_recovers_from_unchanged_local_input(fixture, monkeypatch):
    first, _ = finish(fixture)
    original_catalog = packs._catalog

    class FailedCommit:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def commit(self):
            raise sqlite3.OperationalError("synthetic cache eviction commit refusal")

    with packs._pool_lock(fixture.pool):
        connection = original_catalog(fixture.pool)
        monkeypatch.setattr(packs, "MAX_POOL_BYTES", packs._pool_size(fixture.pool) + 1024**2)
        with pytest.raises(sqlite3.OperationalError, match="eviction commit refusal"):
            packs._make_cache_room(FailedCommit(connection), fixture.pool, 1, 1024**2)
        connection.close()
    recovered, manifest = finish(fixture, "recovered_eviction")
    assert manifest["storage"]["stats"]["missing_cached_packs"] == 1
    shutil.rmtree(fixture.pool)
    assert checked(first)["complete"] and checked(recovered)["complete"]


def test_sparse_reuse_does_not_hide_corrupt_shared_inputs(fixture, monkeypatch):
    original = _sparse_pool_capture(fixture)
    monkeypatch.setattr(packs, "MIN_SPARSE_REUSE_PACK_BYTES", 1024)
    member = original["storage"]["packs"][0]
    with (fixture.pool / member["file"]).open("r+b") as stream:
        stream.seek(len(packs.MAGIC))
        stream.write(b"corrupted")
    with pytest.raises(capture.CalculationCaptureError, match="pack_hash_mismatch"):
        writer(fixture, "corrupt")
    assert not list((fixture.root / "corrupt").glob("*.manifest.json"))


def test_sparse_reuse_still_fails_closed_when_compact_capture_exceeds_budget(fixture, monkeypatch):
    _sparse_pool_capture(fixture)
    monkeypatch.setattr(packs, "MIN_SPARSE_REUSE_PACK_BYTES", 1024)
    current = writer(fixture, "limited_compact")
    limit = sum(p.stat().st_size for p in current.directory.iterdir()) + 100
    with monkeypatch.context() as budget:
        budget.setattr(capture, "MAX_DIRECTORY_BYTES", limit)
        with pytest.raises(capture.CalculationCaptureError, match="budget_exhausted"):
            current.event("shared_value", frame())
            current.finish("returned")
        assert sum(p.stat().st_size for p in current.directory.iterdir()) <= limit
        assert not list(current.directory.glob("*.manifest.json"))
    if not current.closed:
        current._close(sync=False)
    assert not list(current.directory.glob("*.manifest.json"))


def test_pack_count_budget_cannot_advertise_complete_capture(fixture, monkeypatch):
    monkeypatch.setattr(packs, "TARGET_PACK_BYTES", 80)
    monkeypatch.setattr(packs, "MAX_PACK_FILES", 1)
    current = writer(fixture)
    with pytest.raises(capture.CalculationCaptureError):
        current.event("new_frame", frame())
        current.finish("returned")
    if not current.closed:
        current._close()
    assert not list(current.directory.glob("*.manifest.json"))


@pytest.mark.parametrize("mutation,reason", [
    ("offset", "out_of_bounds"), ("negative", "invalid_capture_pack_reference"),
    ("hash", "packed_object_hash_mismatch"), ("raw_size", "packed_object_decompression_failed"),
    ("path", "invalid_capture_pack_reference"), ("extra_field", "invalid_capture_pack_reference"),
])
def test_rehashed_journal_cannot_bypass_pack_bounds_and_content(fixture, mutation, reason):
    path, manifest = finish(fixture)
    journal = path.parent / manifest["file"]
    rows = [json.loads(row) for row in gzip.decompress(journal.read_bytes()).splitlines()]
    reference = next(row["packed"] for row in rows if row["record"] == "object")
    if mutation == "offset":
        reference["offset"] = manifest["storage"]["packs"][0]["bytes"]
    elif mutation == "negative":
        reference["compressed_bytes"] = -1
    elif mutation == "hash":
        reference["compressed_sha256"] = "0" * 64
    elif mutation == "raw_size":
        reference["raw_bytes"] = 1
    elif mutation == "path":
        reference["file"] = "../outside.pack"
    else:
        reference["unbound"] = True
    raw = b"".join(capture._json(row) + b"\n" for row in rows)
    journal.write_bytes(gzip.compress(raw, mtime=0))
    manifest.update(bytes=journal.stat().st_size, sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),
                    stored_uncompressed_bytes=len(raw))
    path.write_text(json.dumps(manifest))
    with pytest.raises(capture.CalculationCaptureError, match=reason):
        checked(path)


@pytest.mark.parametrize("mutation", ["missing", "content", "symlink", "unbound_manifest", "wrong_stored_size"])
def test_pack_and_manifest_corruption_is_rejected(fixture, mutation):
    path, manifest = finish(fixture)
    member = path.parent / manifest["storage"]["packs"][0]["file"]
    if mutation == "missing":
        member.unlink()
    elif mutation == "content":
        raw = member.read_bytes()
        member.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
    elif mutation == "symlink":
        member.unlink()
        member.symlink_to(fixture.pool / member.name)
    elif mutation == "unbound_manifest":
        manifest["storage"]["packs"][0]["file"] = "a" * 32 + ".pack"
        path.write_text(json.dumps(manifest))
    else:
        manifest["stored_uncompressed_bytes"] += 1
        path.write_text(json.dumps(manifest))
    with pytest.raises((capture.CalculationCaptureError, OSError)):
        checked(path)


def test_source_change_keeps_pack_partial_without_complete_manifest(fixture):
    current = writer(fixture)
    current.event("observed", frame())
    fixture.sources[0].write_text("# changed while capture was active\n")
    with pytest.raises(capture.CalculationCaptureError, match="source_changed"):
        current.finish("returned")
    assert not list(current.directory.glob("*.manifest.json"))
    assert list(current.directory.glob("*.pack.partial"))


def test_stale_catalogue_is_rebuilt_from_actual_new_input(fixture):
    original, one = finish(fixture)
    (fixture.pool / one["storage"]["packs"][0]["file"]).unlink()
    replacement, two = finish(fixture, "replacement")
    assert two["storage"]["stats"]["missing_cached_packs"] == 1
    assert checked(original)["complete"] and checked(replacement)["complete"]


def test_catalogue_tampering_cannot_substitute_different_capture_input(fixture):
    original, one = finish(fixture)
    with sqlite3.connect(fixture.pool / packs.CATALOG) as connection:
        connection.execute("UPDATE objects SET offset=offset+1")
    with pytest.raises(capture.CalculationCaptureError):
        writer(fixture, "rejected")
    assert checked(original)["complete"]
    assert not list((fixture.root / "rejected").glob("*.manifest.json"))


def test_gc_retains_references_recent_files_and_honors_batch_limit(fixture):
    first, one = finish(fixture)
    second, two = finish(fixture, "second", value=frame().assign(extra=1))
    assert not packs.collect_unused_packs(fixture.pool, min_age_seconds=0, apply=True)["removed"]
    shutil.rmtree(first.parent)
    assert not packs.collect_unused_packs(fixture.pool, min_age_seconds=0, apply=True)["removed"]
    shutil.rmtree(second.parent)
    recent = packs.collect_unused_packs(fixture.pool, min_age_seconds=86400, apply=True)
    assert not recent["removed"] and recent["retained"] == 2
    preview = packs.collect_unused_packs(fixture.pool, min_age_seconds=0, max_packs=1)
    assert len(preview["planned"]) == 1 and not preview["removed"]
    removed = packs.collect_unused_packs(fixture.pool, min_age_seconds=0, max_packs=1, apply=True)
    assert len(removed["removed"]) == 1 and removed["removed_allocated_bytes_estimate"] > 0
    assert len(list(fixture.pool.glob("*.pack"))) == 1
    with sqlite3.connect(fixture.pool / packs.CATALOG) as connection:
        assert connection.execute("SELECT COUNT(*) FROM objects o LEFT JOIN packs p ON o.pack=p.name WHERE p.name IS NULL").fetchone()[0] == 0


def test_gc_unlink_failure_retains_catalogue_and_reports_failure(fixture, monkeypatch):
    path, manifest = finish(fixture)
    shutil.rmtree(path.parent)
    target = fixture.pool / manifest["storage"]["packs"][0]["file"]
    original = Path.unlink

    def reject(path, *args, **kwargs):
        if path == target:
            raise PermissionError("synthetic unlink refusal")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", reject)
    result = packs.collect_unused_packs(fixture.pool, min_age_seconds=0, apply=True)
    assert not result["removed"] and result["errors"] and target.is_file()
    assert result["removed_allocated_bytes_estimate"] == 0 and not result["partial_removal_possible"]
    restored, reused = finish(fixture, "still_reusable")
    assert reused["storage"]["stats"]["shared_objects_reused"] == 3
    assert checked(restored)["complete"]


def test_gc_commit_failure_reports_partial_removal_and_stale_cache_recovers(fixture, monkeypatch):
    path, manifest = finish(fixture)
    shutil.rmtree(path.parent)
    original_catalog = packs._catalog

    class FailedCommit:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def commit(self):
            raise sqlite3.OperationalError("synthetic commit failure after unlink")

    with monkeypatch.context() as change:
        change.setattr(packs, "_catalog", lambda root: FailedCommit(original_catalog(root)))
        result = packs.collect_unused_packs(fixture.pool, min_age_seconds=0, apply=True)
    assert not result["removed"] and result["errors"] and result["partial_removal_possible"]
    assert result["removed_allocated_bytes_estimate"] is None
    restored, new = finish(fixture, "recovered")
    assert new["storage"]["stats"]["missing_cached_packs"] == 1
    assert checked(restored)["complete"]


def test_concurrent_writers_and_gc_preserve_complete_inputs(fixture):
    context = multiprocessing.get_context("fork")
    gate, results = context.Event(), context.Queue()

    def write(number):
        gate.wait(5)
        try:
            path, _ = finish(fixture, "parallel", value=frame().assign(worker=number))
            results.put(str(path))
        except BaseException as exc:
            results.put(f"ERROR:{type(exc).__name__}:{exc}")
            raise

    workers = [context.Process(target=write, args=(number,)) for number in range(3)]
    try:
        for worker in workers:
            worker.start()
        gate.set()
        for _ in range(10):
            packs.collect_unused_packs(fixture.pool, min_age_seconds=0, apply=True)
            time.sleep(0.02)
        for worker in workers:
            worker.join(15)
            assert worker.exitcode == 0
        paths = [Path(results.get(timeout=2)) for _ in workers]
        assert len(set(paths)) == 3 and all(checked(path)["complete"] for path in paths)
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(5)
        results.close()


def test_weekly_cleanup_collects_only_unreferenced_shared_inputs(fixture):
    pool = fixture.root / "reports/.calculation_input_pool"
    fixture.pool.mkdir()
    path, manifest = finish(fixture)
    pool.parent.mkdir()
    fixture.pool.rename(pool)
    # Existing local links still protect the pack at the scheduled cleanup path.
    settings = SimpleNamespace(project_root=fixture.root, core_root=fixture.root / "core",
                               reports_dir=pool.parent, archived_runs_dir=fixture.root / "archive",
                               runs_dir=fixture.root / "runs")
    now = datetime.fromtimestamp(time.time() + 2 * 86400)
    result = cleanup_disk_usage.run_cleanup(settings, apply=True, now=now).to_dict()
    assert not result["errors"] and not result["calculation_input_pool_gc"]["removed"]
    shutil.rmtree(path.parent)
    result = cleanup_disk_usage.run_cleanup(settings, apply=True, now=now).to_dict()
    assert not result["errors"] and len(result["calculation_input_pool_gc"]["removed"]) == 1


def test_legacy_inline_journals_still_validate(fixture, monkeypatch):
    monkeypatch.delenv(packs.ENV_SHARED_DIRECTORY)
    path, manifest = finish(fixture)
    assert manifest["schema_version"] == 1 and "storage" not in manifest
    assert checked(path)["complete"]
