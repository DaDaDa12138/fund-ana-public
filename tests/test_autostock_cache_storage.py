"""Cache compression preserves a complete real API-validation observation."""
from __future__ import annotations

from datetime import datetime, timedelta
import gzip
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from bond_fund_screening.report_catalog import atomic_write_json_file
from bond_fund_screening.tasks import final_screening_autostock_helpers as helpers
from utils import autostock_cache_storage as storage

FIXTURE = Path(__file__).resolve().parent / "fixtures/autostock_cache_20260912.json.gz"
ORIGINAL = gzip.decompress(FIXTURE.read_bytes())
PAYLOAD = json.loads(ORIGINAL)
OBSERVED_AT = datetime.fromisoformat(PAYLOAD["generated_at"])
KEY = "54e03d225f227e16616628d8e9527f4e95c2445de6306d60b680e761bcc28e13"


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def put(tmp_path: Path, compressed: bool, original: bytes = ORIGINAL) -> Path:
    target = tmp_path / (KEY + (".json.gz" if compressed else ".json"))
    target.write_bytes(gzip.compress(original, mtime=0) if compressed else original)
    return target


def load(tmp_path: Path, *, at: datetime = OBSERVED_AT, schema: str = PAYLOAD["schema_version"]):
    return helpers.load_autostock_validation_cache(
        KEY, cache_dir=tmp_path, ttl_seconds=3600, required_schema_version=schema, now=at)


@pytest.mark.parametrize("compressed", [False, True])
def test_complete_real_cache_round_trip_preserves_all_values(tmp_path, compressed):
    path = put(tmp_path, compressed)
    assert storage.read_cache_bytes(path) == ORIGINAL
    assert canonical(load(tmp_path)) == canonical(PAYLOAD)
    recent = helpers.load_recent_autostock_validation_caches(
        cache_dir=tmp_path, ttl_seconds=3600, required_schema_version=PAYLOAD["schema_version"], now=OBSERVED_AT)
    assert canonical(recent) == canonical([PAYLOAD])


@pytest.mark.parametrize("compressed", [False, True])
@pytest.mark.parametrize("seconds,accepted", [(-1, False), (3599, True), (3600, True), (3601, False)])
def test_source_timestamp_still_controls_ttl(tmp_path, compressed, seconds, accepted):
    put(tmp_path, compressed)
    result = load(tmp_path, at=OBSERVED_AT + timedelta(seconds=seconds))
    assert (result is not None) == accepted
    if accepted:
        assert canonical(result) == canonical(PAYLOAD)


@pytest.mark.parametrize("compressed", [False, True])
def test_cache_schema_gate_remains_required(tmp_path, compressed):
    put(tmp_path, compressed)
    assert load(tmp_path, schema="incompatible-isolated-schema") is None


def test_identical_migration_pair_is_read_once(tmp_path):
    put(tmp_path, False)
    put(tmp_path, True)
    (tmp_path / ".uncommitted.json.gz").write_bytes(FIXTURE.read_bytes())
    (tmp_path / "other.json.tmp").write_bytes(ORIGINAL)
    assert storage.cache_keys(tmp_path) == [KEY]
    recent = helpers.load_recent_autostock_validation_caches(
        cache_dir=tmp_path, ttl_seconds=3600, required_schema_version=PAYLOAD["schema_version"], now=OBSERVED_AT)
    assert canonical(recent) == canonical([PAYLOAD])


def test_conflicting_representations_do_not_restore_an_old_result(tmp_path):
    put(tmp_path, False)
    changed = dict(PAYLOAD, generated_at=(OBSERVED_AT - timedelta(seconds=1)).isoformat())
    put(tmp_path, True, json.dumps(changed, ensure_ascii=False).encode())
    assert load(tmp_path) is None
    assert helpers.load_recent_autostock_validation_caches(
        cache_dir=tmp_path, ttl_seconds=3600, required_schema_version=PAYLOAD["schema_version"], now=OBSERVED_AT) == []


@pytest.mark.parametrize("content", [b"not gzip", gzip.compress(b"[]"), FIXTURE.read_bytes()[:-8]])
def test_invalid_gzip_is_a_cache_miss_without_legacy_fallback(tmp_path, content):
    put(tmp_path, False)
    (tmp_path / f"{KEY}.json.gz").write_bytes(content)
    assert load(tmp_path) is None


@pytest.mark.parametrize("compressed", [False, True])
def test_read_budget_limits_physical_and_expanded_bytes(tmp_path, compressed):
    path = put(tmp_path, compressed, b"x" * 8192)
    with pytest.raises(ValueError, match="exceeds_read_budget"):
        storage.read_cache_bytes(path, max_bytes=1024)


def test_writer_uses_original_serializer_and_removes_legacy_after_verification(tmp_path):
    put(tmp_path, False)
    writer = Mock(wraps=atomic_write_json_file)
    path = helpers.write_autostock_validation_cache(KEY, PAYLOAD, cache_dir=tmp_path, json_writer=writer)
    writer.assert_called_once()
    assert writer.call_args.args[1] is PAYLOAD
    assert path == tmp_path / f"{KEY}.json.gz"
    assert storage.read_cache_bytes(path) == ORIGINAL
    assert canonical(load(tmp_path)) == canonical(PAYLOAD)
    assert list(tmp_path.iterdir()) == [path]


def test_compression_failure_preserves_previous_complete_cache(tmp_path, monkeypatch):
    path = put(tmp_path, False)
    monkeypatch.setattr(storage.gzip, "compress", lambda *args, **kwargs: b"truncated")
    with pytest.raises(ValueError, match="gzip_invalid"):
        storage.write_cache(tmp_path, KEY, PAYLOAD, json_writer=atomic_write_json_file)
    assert path.read_bytes() == ORIGINAL
    assert list(tmp_path.iterdir()) == [path]
    assert canonical(load(tmp_path)) == canonical(PAYLOAD)


def test_failed_atomic_publication_preserves_previous_cache(tmp_path, monkeypatch):
    path = put(tmp_path, True)
    before = path.read_bytes()
    replace = storage.os.replace

    def fail_target(source, target):
        if Path(target) == path:
            raise OSError("isolated full disk during cache publication")
        return replace(source, target)

    monkeypatch.setattr(storage.os, "replace", fail_target)
    with pytest.raises(OSError, match="full disk"):
        storage.write_cache(tmp_path, KEY, PAYLOAD, json_writer=atomic_write_json_file)
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_missing_cache_and_symlink_do_not_produce_a_pass(tmp_path):
    assert load(tmp_path) is None
    original = tmp_path / "original"
    original.write_bytes(ORIGINAL)
    (tmp_path / f"{KEY}.json").symlink_to(original)
    assert load(tmp_path) is None


@pytest.mark.parametrize("key", ["", "../outside", ".temporary", "a/b", "a\\b"])
def test_cache_key_cannot_escape_its_directory(tmp_path, key):
    with pytest.raises(ValueError, match="cache_key"):
        storage.read_cache(tmp_path, key)
