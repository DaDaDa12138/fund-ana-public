"""Lossless conversion/retirement and failure recovery on synthetic databases."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

import pytest

from test_screening_archive_production import archive
from utils import screening_columnar_maintenance as maintenance
from utils.screening_history_archive_manifest import load_screening_archive_manifest


def convert(archive, **options):
    root, hot, active, _, binding = archive
    return maintenance.maintain_columnar_archive(
        project_root=root, active_archive_path=active, hot_db_path=hot,
        output_dir=root / 'columnar-audit', existing_copies=[binding], **options,
    )


def source_path(archive):
    _, _, active, _, _ = archive
    manifest = json.loads((active.parent / 'screening_history_archive_manifest.json').read_text())
    entry = manifest['immutable_shards'][0]
    return active.parent / (entry.get('sqlite_reference') or entry)['path']


def as_unconverted(archive):
    _, _, active, _, _ = archive
    path = active.parent / 'screening_history_archive_manifest.json'
    manifest = json.loads(path.read_text())
    manifest['immutable_shards'][0] = manifest['immutable_shards'][0]['sqlite_reference']
    path.write_text(json.dumps(manifest))
    return path


def test_preview_preserves_original_and_manifest(archive):
    manifest = as_unconverted(archive)
    before = manifest.read_bytes()
    source = source_path(archive)
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    result = convert(archive, mode='preview')
    assert result['status'] == 'preview' and result['converted_count'] == 0
    assert manifest.read_bytes() == before
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
    assert not source.with_suffix('.db.zst').exists()


def test_switch_retain_validate_then_retire_has_a_complete_recovery_copy(archive):
    manifest = as_unconverted(archive)
    source = source_path(archive)
    before = source.read_bytes()
    kept = convert(archive, mode='apply', keep_source=True)
    assert kept['status'] == 'converted_source_kept', kept
    assert source.read_bytes() == before
    assert len(kept['restoration_checks']) == 2  # This fixture has two sealed runs.
    assert kept['sample_basis'] == 'source_started_at_then_run_id_earliest_middle_latest'
    assert [row['run_id'] for row in kept['sample_runs']] == ['run-b', 'run-a']
    assert kept['sample_runs'][0]['started_at'] < kept['sample_runs'][1]['started_at']
    assert all(row['temporary_database_removed'] for row in kept['restoration_checks'])
    retired = convert(archive, mode='apply')
    assert retired['status'] == 'converted' and retired['source_removed'], retired
    assert not source.exists()
    root, _, active, _, _ = archive
    assert load_screening_archive_manifest(active, project_root=root)['status'] == 'ready'
    assert retired['recovery_copy']['decoded_sha256'] == hashlib.sha256(before).hexdigest()
    assert json.loads(manifest.read_text())['immutable_shards'][0]['recovery_copy']['path'].endswith('.db.zst')
    import subprocess

    recovered = subprocess.check_output(['zstd', '-q', '-d', '-c', str(source.with_suffix('.db.zst'))])
    assert recovered == before
    assert convert(archive, mode='apply')['status'] == 'skipped'


def test_future_rotation_creates_its_own_verified_parquet_copy(archive):
    as_unconverted(archive)
    root, hot, active, directory, _ = archive
    shutil.rmtree(directory)
    source = source_path(archive)
    result = maintenance.maintain_columnar_archive(
        project_root=root, active_archive_path=active, hot_db_path=hot,
        output_dir=root / 'columnar-audit', mode='apply',
    )
    assert result['status'] == 'converted', result
    assert result['export']['verification']['verified_table_count'] == 7
    assert result['converted_count'] == 1 and not source.exists()
    with closing(sqlite3.connect(hot)) as connection:
        assert connection.execute('SELECT COUNT(*) FROM screening_runs').fetchone()[0] == 3


def test_bad_recovery_copy_cannot_remove_original_or_advance_manifest(archive):
    as_unconverted(archive)
    assert convert(archive, mode='apply', keep_source=True)['status'] == 'converted_source_kept'
    root, _, active, _, _ = archive
    manifest = active.parent / 'screening_history_archive_manifest.json'
    before = manifest.read_bytes()
    source = source_path(archive)
    compressed = source.with_suffix('.db.zst')
    value = bytearray(compressed.read_bytes())
    value[10] ^= 1
    compressed.write_bytes(value)
    result = convert(archive, mode='apply')
    assert result['status'] == 'failed' and source.exists()
    assert manifest.read_bytes() == before
    assert load_screening_archive_manifest(active, project_root=root)['status'] == 'ready'


def test_manifest_commit_failure_preserves_original_and_recovery_copy(archive, monkeypatch):
    manifest = as_unconverted(archive)
    before = manifest.read_bytes()
    source = source_path(archive)

    def fail(*args, **kwargs):
        raise OSError('synthetic manifest persistence failure')

    monkeypatch.setattr(maintenance, 'write_screening_archive_manifest', fail)
    result = convert(archive, mode='apply')
    assert result['status'] == 'failed'
    assert manifest.read_bytes() == before and source.exists()
    assert source.with_suffix('.db.zst').exists()


def test_interrupted_retirement_can_resume_from_valid_parquet_manifest(archive, monkeypatch):
    as_unconverted(archive)
    source = source_path(archive)
    unlink = Path.unlink

    def fail_original(path, *args, **kwargs):
        if path == source:
            raise OSError('synthetic source unlink failure')
        return unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'unlink', fail_original)
        result = convert(archive, mode='apply')
    assert result['status'] == 'failed' and source.exists()
    root, _, active, _, _ = archive
    state = load_screening_archive_manifest(active, project_root=root)
    assert state['status'] == 'ready' and state['shards'][1]['format'] == 'parquet'
    result = convert(archive, mode='apply')
    assert result['status'] == 'converted' and not source.exists()


def test_low_disk_space_defers_before_export_without_deleting_data(archive, monkeypatch):
    manifest = as_unconverted(archive)
    root, hot, active, directory, _ = archive
    before = manifest.read_bytes()
    source = source_path(archive)
    shutil.rmtree(directory)
    monkeypatch.setattr(maintenance.shutil, 'disk_usage', lambda path: SimpleNamespace(free=1024))
    result = maintenance.maintain_columnar_archive(
        project_root=root, active_archive_path=active, hot_db_path=hot,
        output_dir=root / 'columnar-audit', mode='apply',
    )
    assert result['status'] == 'failed' and 'insufficient_space' in result['error']
    assert manifest.read_bytes() == before and source.exists()
    assert not source.with_suffix('.db.zst').exists()


def test_an_active_workflow_defers_conversion_without_changing_ownership(archive):
    from backend.storage import WorkflowStore

    manifest = as_unconverted(archive)
    before = manifest.read_bytes()
    root = archive[0]
    owner = WorkflowStore(root / 'backend/data/workflow_state.json')
    with owner._lock:
        owner._acquire_run_lock_locked('existing_business_workflow')
    try:
        result = convert(archive, mode='apply')
        assert result['status'] == 'deferred' and result['reason'] == 'workflow_lock_busy'
        assert owner.has_active_lock() and owner._active_run_id == 'existing_business_workflow'
        assert manifest.read_bytes() == before
        assert source_path(archive).exists()
    finally:
        with owner._lock:
            owner._release_run_lock_locked()
