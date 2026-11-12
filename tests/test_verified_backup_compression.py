from pathlib import Path
from contextlib import closing
import hashlib
import json
import shutil
import sqlite3
import subprocess

import pytest

from utils import verified_backup_compression as compression
from utils.database_backup_compression_execution import (
    REQUIRED_APPLY_CONFIRMATION, execute_database_backup_compression,
)
from utils.database_backup_compression_preview import build_database_backup_compression_preview_payload


@pytest.fixture
def backup(tmp_path):
    if not shutil.which('zstd'):
        pytest.skip('zstd is required for the restore contract')
    path = tmp_path / 'backups' / 'snapshot.db'
    path.parent.mkdir()
    with closing(sqlite3.connect(path)) as conn:
        conn.execute('CREATE TABLE observations(code TEXT, value REAL, note TEXT)')
        conn.executemany('INSERT INTO observations VALUES (?, ?, ?)', [
            ('000001', None, '缺失保留' * 100), ('000002', 0.0, '真实零' * 100),
            ('000003', 1.23456789, '完整精度' * 100),
        ] * 200)
        conn.commit()
    return path


def run_compression(path, **kwargs):
    return compression.compress_verified_backup(path, active_paths=[], remove_source=True, **kwargs)


def test_backup_full_restore_preserves_bytes_values_and_measures_net_space(backup, tmp_path):
    original = backup.read_bytes()
    proof = run_compression(backup)
    assert not backup.exists()
    # Unlinking an open SQLite inode delays the actual disk release until GC.
    open_files = Path('/proc/self/fd')
    if open_files.exists():
        links = []
        for descriptor in open_files.iterdir():
            try:
                links.append(str(descriptor.readlink()))
            except FileNotFoundError:
                continue
        assert str(backup) + ' (deleted)' not in links
    compressed = Path(proof['compressed_path'])
    restored = tmp_path / 'restored.db'
    subprocess.run(['zstd', '-q', '-d', str(compressed), '-o', str(restored)], check=True)
    assert restored.read_bytes() == original
    assert proof['source_sha256'] == proof['decoded_sha256'] == hashlib.sha256(original).hexdigest()
    assert proof['net_reclaimed_bytes'] == proof['allocated_bytes_before'] - proof['allocated_bytes_after'] > 0
    assert proof['net_reclaimed_bytes'] < proof['source_allocated_bytes']
    assert json.loads(Path(proof['verification_manifest_path']).read_text())['source_removed'] is True
    with sqlite3.connect(restored) as conn:
        assert conn.execute('PRAGMA quick_check').fetchall() == [('ok',)]
        assert conn.execute('SELECT code,value FROM observations LIMIT 3').fetchall() == [
            ('000001', None), ('000002', 0.0), ('000003', 1.23456789)]


@pytest.mark.parametrize('damage', ['wrong_valid_file', 'truncated_file'])
def test_existing_compressed_peer_must_match_original(backup, tmp_path, damage):
    original = backup.read_bytes()
    peer = backup.with_suffix('.db.zst')
    if damage == 'wrong_valid_file':
        wrong = tmp_path / 'wrong.db'
        wrong.write_bytes(original.replace('缺失保留'.encode(), '数据不同'.encode()))
        subprocess.run(['zstd', '-q', str(wrong), '-o', str(peer)], check=True)
    else:
        peer.write_bytes(b'not a complete zstd frame')
    with pytest.raises(ValueError):
        run_compression(backup)
    assert backup.read_bytes() == original
    assert not peer.with_suffix('.zst.manifest.json').exists()


def test_existing_equal_peer_is_verified_before_source_removal(backup):
    peer = backup.with_suffix('.db.zst')
    subprocess.run(['zstd', '-q', str(backup), '-o', str(peer)], check=True)
    proof = run_compression(backup)
    assert proof['existing_compressed'] is True and proof['source_removed'] is True
    assert proof['decoded_sha256'] == proof['source_sha256']
    assert proof['net_reclaimed_bytes'] > 0


def test_source_change_during_compression_keeps_original(backup, monkeypatch):
    decode = compression._decoded_digest

    def changing_decode(*args):
        result = decode(*args)
        with backup.open('ab') as stream:
            stream.write(b'concurrent change')
        return result

    monkeypatch.setattr(compression, '_decoded_digest', changing_decode)
    with pytest.raises(ValueError, match='source_changed'):
        run_compression(backup)
    assert backup.exists()
    assert not backup.with_suffix('.db.zst').exists()


def test_failed_recovery_manifest_write_keeps_source(backup, monkeypatch):
    original = backup.read_bytes()

    def fail_write(*args):
        raise OSError('simulated disk write failure')

    monkeypatch.setattr(compression, '_atomic_json', fail_write)
    with pytest.raises(OSError, match='disk write failure'):
        run_compression(backup)
    assert backup.read_bytes() == original


def test_source_change_after_manifest_write_still_prevents_removal(backup, monkeypatch):
    write = compression._atomic_json

    def changing_write(*args):
        write(*args)
        with backup.open('ab') as stream:
            stream.write(b'changed before delete')

    monkeypatch.setattr(compression, '_atomic_json', changing_write)
    with pytest.raises(ValueError, match='changed_before_source_removal'):
        run_compression(backup)
    assert backup.exists()


def test_active_database_or_hardlink_cannot_be_compressed(backup, tmp_path):
    with pytest.raises(ValueError, match='active_database'):
        compression.compress_verified_backup(backup, active_paths=[backup], remove_source=True)
    alias = tmp_path / 'active.db'
    alias.hardlink_to(backup)
    with pytest.raises(ValueError, match='active_database'):
        compression.compress_verified_backup(backup, active_paths=[alias], remove_source=True)
    assert backup.exists()


def test_uncheckpointed_wal_keeps_source_and_journal(backup):
    with sqlite3.connect(backup) as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute("INSERT INTO observations VALUES ('000004', 0, 'pending WAL')")
        conn.commit()
        with pytest.raises(ValueError, match='uncheckpointed_journal'):
            run_compression(backup)
        assert backup.exists()
        assert Path(str(backup) + '-wal').stat().st_size > 0
        assert conn.execute('SELECT COUNT(*) FROM observations').fetchone()[0] == 601


def test_keep_source_reports_new_allocated_space(backup):
    proof = compression.compress_verified_backup(backup, active_paths=[], remove_source=False)
    assert backup.exists() and proof['source_removed'] is False
    assert proof['removed_source_size_bytes'] == 0
    assert proof['net_reclaimed_bytes'] < 0


def test_invalid_sqlite_backup_is_not_removed(backup):
    backup.write_bytes(b'invalid SQLite input')
    with pytest.raises(sqlite3.DatabaseError):
        run_compression(backup)
    assert backup.exists()


def test_symlink_scope_is_rejected(backup, tmp_path):
    alias = backup.parent / 'alias.db'
    alias.symlink_to(backup.name)
    with pytest.raises(ValueError, match='symlinks'):
        execute_database_backup_compression(
            preview_payload={'execution_plan': {'target_dir': str(backup.parent), 'scope_files': [alias.name]}},
            output_dir=tmp_path / 'audit', mode='apply', confirm_apply=REQUIRED_APPLY_CONFIRMATION)
    assert backup.exists()


def test_missing_scope_is_not_reported_as_success(backup, tmp_path):
    preview = build_database_backup_compression_preview_payload(
        backup_dir=backup.parent, active_database_path=tmp_path / 'active.db', min_compress_size_mb=0)
    backup.unlink()
    _, _, payload = execute_database_backup_compression(preview_payload=preview,
        output_dir=tmp_path / 'audit', mode='apply', confirm_apply=REQUIRED_APPLY_CONFIRMATION)
    assert payload['status'] == 'failed'
    assert payload['summary']['missing_file_count'] == 1
    assert payload['summary']['source_removed_file_count'] == 0


def test_execution_rechecks_active_path_even_with_modified_scope(backup, tmp_path):
    preview = build_database_backup_compression_preview_payload(
        backup_dir=backup.parent, active_database_path=backup, min_compress_size_mb=0)
    preview['execution_plan']['scope_files'] = [backup.name]
    _, _, payload = execute_database_backup_compression(preview_payload=preview,
        output_dir=tmp_path / 'audit', mode='apply', confirm_apply=REQUIRED_APPLY_CONFIRMATION)
    assert payload['status'] == 'failed'
    assert 'active_database' in payload['files'][0]['error']
    assert backup.exists()
