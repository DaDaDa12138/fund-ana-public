"""Synthetic byte/type contracts; these fixtures are not fund business data."""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pytest

from core.utils import calculation_capture as capture
from core.utils import calculation_chunking as chunks
from core.utils import calculation_pack_storage as packs


def synthetic_bytes(size):
    return np.random.default_rng(709).bytes(size)


def scalar_chunks(raw):
    gear = [int.from_bytes(hashlib.sha256(b"fund-ana-capture-chunks-v1:" + bytes([value])).digest()[:8], "little")
            for value in range(256)]
    mask = ((1 << 12) - 1) << 52
    rolling = last = 0
    result = []
    for end, value in enumerate(raw, start=1):
        rolling = ((rolling << 1) + gear[value]) & 0xffffffffffffffff
        length = end - last
        if length == chunks.MAX_CHUNK_BYTES or length >= chunks.MIN_CHUNK_BYTES and rolling & mask == 0:
            result.append(raw[last:end])
            last = end
    if last < len(raw):
        result.append(raw[last:])
    return result


@pytest.mark.parametrize("size", [0, 1, 8192, 65537, 400000])
def test_vector_chunks_equal_independent_scalar_definition(size):
    raw = synthetic_bytes(size)
    actual = list(chunks.iter_content_chunks(raw))
    assert actual == scalar_chunks(raw)
    assert b"".join(actual) == raw
    assert all(chunks.MIN_CHUNK_BYTES <= len(value) <= chunks.MAX_CHUNK_BYTES for value in actual[:-1])
    assert not actual or 0 < len(actual[-1]) <= chunks.MAX_CHUNK_BYTES


@pytest.mark.parametrize("block_size", [197, 4096, 65536])
def test_chunk_boundaries_do_not_depend_on_scan_block(block_size, monkeypatch):
    raw = synthetic_bytes(300000)
    expected = list(chunks.iter_content_chunks(raw))
    monkeypatch.setattr(chunks, "_SCAN_BYTES", block_size)
    assert list(chunks.iter_content_chunks(raw)) == expected


def test_insertions_deletions_and_appends_reuse_unchanged_bytes():
    original = synthetic_bytes(1024 * 1024)
    baseline = {hashlib.sha256(value).digest() for value in chunks.iter_content_chunks(original)}
    changed = b"synthetic prefix" + original[:100000] + b"changed" * 20 + original[100023:] + b"synthetic tail"
    actual = list(chunks.iter_content_chunks(changed))
    assert b"".join(actual) == changed
    reused = sum(len(value) for value in actual if hashlib.sha256(value).digest() in baseline)
    assert reused > len(original) * 0.85


@pytest.fixture
def storage(tmp_path, monkeypatch):
    source = tmp_path / "synthetic_source.py"
    source.write_text("# Synthetic contract fixture, not fund data.\n")
    monkeypatch.setattr(capture, "PROJECT_ROOT", tmp_path)
    monkeypatch.setenv("FUND_ANA_WORKFLOW_RUN_ID", "synthetic_chunk_capture")
    monkeypatch.delenv(capture.ENV_CAPTURE_RUN_ID, raising=False)
    monkeypatch.delenv(capture.ENV_CAPTURE_SCOPE, raising=False)
    monkeypatch.setenv(packs.ENV_SHARED_DIRECTORY, str(tmp_path / "pool"))
    return tmp_path, source


def emit(storage, name, value):
    root, source = storage
    writer = capture.CaptureWriter(root / name, "synthetic", {}, code_paths=[source])
    writer.event("actual_synthetic_operand", value)
    manifest = writer.finish("returned")
    return writer.directory / (manifest["session_id"] + ".manifest.json"), manifest


def validate(path):
    return capture.validate_journal(path, expected_run_id="synthetic_chunk_capture")


def event_value(path, manifest):
    objects = {}
    for row in capture.iter_journal(path.parent / manifest["file"]):
        if row["record"] == "object":
            objects[row["sha256"]] = row["value"]
        if row["record"] == "event":
            return capture.decode(row["value"], objects.__getitem__)
    raise AssertionError("missing fixture event")


def test_arrays_and_long_scalar_sequences_keep_values_shape_and_missing_types(storage):
    array = np.arange(4000, dtype=np.float64).reshape(1000, 4)
    array[2] = [np.nan, np.inf, -np.inf, -0.0]
    codes = [f"synthetic_{number:06}" for number in range(500)]
    nullable = np.array([None, pd.NA, np.nan, "", 0.0, -0.0] * 200, dtype=object)
    value = {"array": array, "codes": codes, "tuple": tuple(codes), "nullable": nullable}
    path, manifest = emit(storage, "first", value)
    assert manifest["schema_version"] == 3 and manifest["object_count"] == 4
    assert validate(path)["typed_objects_roundtripped"] == 4
    restored = event_value(path, manifest)
    assert capture._json(capture._encode(restored)) == capture._json(capture._encode(value))
    assert restored["array"].shape == (1000, 4) and restored["array"].dtype == np.dtype("float64")
    assert np.signbit(restored["array"][2, 3])
    assert restored["nullable"][0] is None and restored["nullable"][1] is pd.NA
    assert isinstance(restored["codes"], list) and isinstance(restored["tuple"], tuple)


def test_large_changed_frames_reuse_chunks_and_replay_without_pool(storage):
    root, _ = storage
    frame = pd.DataFrame({"synthetic_index": np.arange(5000),
                          "synthetic_value": np.random.default_rng(709).normal(size=5000)})
    first, one = emit(storage, "first", frame)
    altered = frame.copy()
    altered.loc[2500, "synthetic_value"] += 1
    second, two = emit(storage, "second", altered)
    assert one["storage"]["stats"]["chunked_objects"] >= 1
    assert two["storage"]["stats"]["shared_raw_bytes_reused"] > 65536
    shutil.rmtree(first.parent)
    assert not packs.collect_unused_packs(root / "pool", min_age_seconds=0, apply=True)["errors"]
    shutil.rmtree(root / "pool")
    assert validate(second)["complete"]
    pd.testing.assert_frame_equal(event_value(second, two), altered, check_exact=True)


def rewrite_journal(path, manifest, rows):
    raw = b"".join(capture._json(row) + b"\n" for row in rows)
    journal = path.parent / manifest["file"]
    journal.write_bytes(gzip.compress(raw, mtime=0))
    manifest.update(bytes=journal.stat().st_size, sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),
                    stored_uncompressed_bytes=len(raw))
    path.write_text(json.dumps(manifest))


@pytest.mark.parametrize("mutation", ["empty", "order", "total", "chunk_hash", "nested", "boolean_size", "oversized", "schema_two"])
def test_chunk_corruption_cannot_pass_even_with_new_journal_hash(storage, mutation):
    value = pd.DataFrame({"synthetic": np.arange(5000), "payload": [synthetic_bytes(40).hex()] * 5000})
    path, manifest = emit(storage, "corrupt", value)
    rows = [json.loads(row) for row in gzip.decompress((path.parent / manifest["file"]).read_bytes()).splitlines()]
    reference = next(row["packed"] for row in rows if row["record"] == "object")
    assert len(reference["chunks"]) > 2
    if mutation == "empty":
        reference["chunks"] = []
    elif mutation == "order":
        reference["chunks"][0], reference["chunks"][1] = reference["chunks"][1], reference["chunks"][0]
    elif mutation == "total":
        reference["raw_bytes"] += 1
    elif mutation == "chunk_hash":
        reference["chunks"][0]["sha256"] = "0" * 64
    elif mutation == "nested":
        reference["chunks"][0]["packed"] = {"format": chunks.CHUNK_FORMAT, "raw_bytes": 4096, "chunks": []}
    elif mutation == "boolean_size":
        reference["chunks"][0]["packed"]["raw_bytes"] = True
    elif mutation == "oversized":
        reference["chunks"][0]["packed"]["raw_bytes"] = chunks.MAX_CHUNK_BYTES + 1
    else:
        rows[0]["schema_version"] = manifest["schema_version"] = 2
    rewrite_journal(path, manifest, rows)
    with pytest.raises(capture.CalculationCaptureError):
        validate(path)


def test_schema_two_direct_pack_journal_remains_readable(storage):
    path, manifest = emit(storage, "legacy_two", pd.DataFrame({"synthetic": [1, 2]}))
    rows = [json.loads(row) for row in gzip.decompress((path.parent / manifest["file"]).read_bytes()).splitlines()]
    assert all("chunks" not in row.get("packed", {}) for row in rows)
    # A small-frame v3 journal has the same direct-object representation as v2.
    # This synthetic fixture explicitly exercises the frozen v2 wire contract.
    rows[0]["schema_version"] = manifest["schema_version"] = 2
    rewrite_journal(path, manifest, rows)
    assert validate(path)["complete"]


def test_unshared_writer_keeps_legacy_inline_sequence_contract(storage, monkeypatch):
    monkeypatch.delenv(packs.ENV_SHARED_DIRECTORY)
    value = np.arange(2000, dtype=np.int64)
    path, manifest = emit(storage, "legacy_one", value)
    assert manifest["schema_version"] == 1 and manifest["object_count"] == 0
    assert validate(path)["complete"]
    np.testing.assert_array_equal(event_value(path, manifest), value)
