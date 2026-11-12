"""Recovery and failure contracts using isolated SQLite fixtures, never fund data."""
from contextlib import closing
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from utils.nav_history_archive_backup import create_nav_history_archive_backup
from utils.rank_history_archive_backup import create_rank_history_archive_backup
from utils.screening_history_cleanup_backup import create_screening_history_cleanup_backup


CREATORS = [
    create_screening_history_cleanup_backup,
    create_nav_history_archive_backup,
    create_rank_history_archive_backup,
]


@pytest.fixture
def wal_database(tmp_path):
    path = tmp_path / 'source.db'
    with closing(sqlite3.connect(path)) as connection:
        connection.execute('PRAGMA journal_mode=WAL')
        connection.execute('PRAGMA wal_autocheckpoint=0')
        connection.execute('CREATE TABLE observations(id INTEGER PRIMARY KEY, value REAL, note TEXT)')
        connection.execute('INSERT INTO observations VALUES (1, NULL, ?)', ('缺失',))
        connection.commit()
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        connection.execute('INSERT INTO observations VALUES (2, 0, ?)', ('WAL 中真实零语义的测试值',))
        connection.execute('INSERT INTO observations VALUES (3, 1.23456789, ?)', ('测试精度',))
        connection.commit()
        assert Path(str(path) + '-wal').stat().st_size > 0
        yield path, connection


def create(creator, source, tmp_path, **kwargs):
    return creator(source_db_path=source, backup_dir=tmp_path / 'backups',
                   output_dir=tmp_path / 'audit', project_root=tmp_path,
                   timestamp='fixture', **kwargs)


def read_rows(path):
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)) as conn:
        assert conn.execute('PRAGMA quick_check').fetchall() == [('ok',)]
        return conn.execute('SELECT * FROM observations ORDER BY id').fetchall()


def restore(path, target):
    if path.suffix == '.zst':
        subprocess.run(['zstd', '-q', '-d', str(path), '-o', str(target)], check=True)
        return target
    return path


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
def test_automatic_backup_has_full_restore_manifest_and_committed_wal(wal_database, tmp_path, creator):
    source, conn = wal_database
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    _, _, payload = create(creator, source, tmp_path)
    assert payload['status'] == 'success'
    summary = payload['summary']
    artifact = Path(summary['backup_artifact_absolute_path'])
    restored = restore(artifact, tmp_path / 'restored.db')
    assert read_rows(restored) == expected
    manifest_path = artifact.with_suffix(artifact.suffix + '.manifest.json')
    assert manifest_path.is_file(), 'automatic backup lacks a complete recovery manifest'
    proof = json.loads(manifest_path.read_text())
    assert proof['source_sha256'] == proof['decoded_sha256'] == hashlib.sha256(restored.read_bytes()).hexdigest()
    assert proof['compressed_sha256'] == hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert proof['sqlite_quick_check'] == 'ok'
    assert proof['source_removed'] is True


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
def test_valid_but_wrong_compression_is_rejected_and_snapshot_kept(wal_database, tmp_path, monkeypatch, creator):
    source, conn = wal_database
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    wrong = tmp_path / 'wrong.db'
    with closing(sqlite3.connect(wrong)) as other:
        other.execute('CREATE TABLE observations(id INTEGER PRIMARY KEY, value REAL, note TEXT)')
        other.execute("INSERT INTO observations VALUES (1, 9, 'different fixture')")
        other.commit()
    real_run = subprocess.run

    def corrupt_compression(command, *args, **kwargs):
        if '-o' in command and '-d' not in command and Path(command[0]).name == 'zstd':
            command = list(command)
            command[command.index('-o') - 1] = str(wrong)
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, 'run', corrupt_compression)
    with pytest.raises((ValueError, RuntimeError), match='match|verification'):
        create(creator, source, tmp_path)
    snapshots = list((tmp_path / 'backups').glob('*.db'))
    assert len(snapshots) == 1
    assert read_rows(snapshots[0]) == expected
    assert not list((tmp_path / 'backups').glob('*.manifest.json'))
    assert conn.execute('SELECT * FROM observations ORDER BY id').fetchall() == expected


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
def test_automatic_backup_checks_disk_before_copy(wal_database, tmp_path, monkeypatch, creator):
    source, _ = wal_database
    monkeypatch.setattr(shutil, 'disk_usage', lambda _: SimpleNamespace(total=100000, used=99999, free=1))
    with pytest.raises((ValueError, RuntimeError), match='space'):
        create(creator, source, tmp_path)
    assert not list((tmp_path / 'backups').glob('*.db'))
    assert not list((tmp_path / 'backups').glob('*.zst'))


def test_monthly_backup_includes_committed_wal(wal_database, tmp_path):
    from schedulers.task_scheduler import TaskScheduler

    source, conn = wal_database
    scheduler = TaskScheduler.__new__(TaskScheduler)
    scheduler.db_manager = SimpleNamespace(db_path=source)
    scheduler.config = {}
    scheduler._execute_with_history = lambda name, logic, **kwargs: logic()
    result = scheduler._monthly_backup()
    assert result['count'] == result['processed'] == 1
    artifact = Path(result['metadata']['backup_file'])
    restored = restore(artifact, tmp_path / 'monthly_restored.db')
    assert read_rows(restored) == conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    assert artifact.with_suffix(artifact.suffix + '.manifest.json').is_file()


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
@pytest.mark.parametrize('compress,keep', [(False, False), (True, True), (True, False)])
def test_restore_modes_preserve_values_provenance_and_live_wal(wal_database, tmp_path, creator, compress, keep):
    from version import __version__

    source, conn = wal_database
    wal = Path(str(source) + '-wal')
    live_before = source.read_bytes(), wal.read_bytes()
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    _, _, payload = create(creator, source, tmp_path, compress=compress,
                           keep_uncompressed=keep, compute_sha256=False)
    summary = payload['summary']
    artifact = Path(summary['backup_artifact_absolute_path'])
    restored = restore(artifact, tmp_path / 'restored.db')
    assert read_rows(restored) == expected
    assert (source.read_bytes(), wal.read_bytes()) == live_before
    assert summary['backup_artifact_sha256'] == ''  # legacy option only hides the display field
    proof = json.loads(artifact.with_suffix(artifact.suffix + '.manifest.json').read_text())
    assert proof['source_sha256'] == summary['snapshot_sha256'] == hashlib.sha256(restored.read_bytes()).hexdigest()
    metadata = proof['snapshot']
    assert metadata['source_database_path'] == str(source)
    assert metadata['method'] == 'sqlite_online_backup'
    assert metadata['application_version'] == __version__
    assert metadata['purpose'] == creator.__name__.removeprefix('create_').removesuffix('_backup')
    assert metadata['backup_label'] == summary['backup_label']
    assert metadata['schema_sha256']
    assert metadata['source_wal_size_bytes'] > 0
    assert metadata['source_logical_size_bytes'] == metadata['page_count'] * metadata['page_size']
    assert metadata['creation_space_budget'] == summary['backup_space_budget']
    assert summary['backup_db_exists'] == (not compress or keep)
    assert proof['source_removed'] == (compress and not keep)
    assert (tmp_path / summary['verification_manifest_path']).is_file()
    assert not list((tmp_path / 'backups').glob('*-wal'))
    assert not list((tmp_path / 'backups').glob('*-shm'))


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
@pytest.mark.parametrize('missing', [False, True])
def test_backup_dry_run_and_missing_source_never_create_snapshot(wal_database, tmp_path, creator, missing):
    source, _ = wal_database
    if missing:
        source = tmp_path / 'does_not_exist.db'
    json_path, md_path, payload = create(creator, source, tmp_path, dry_run=not missing)
    assert payload['status'] == ('blocked' if missing else 'dry-run')
    assert payload['summary']['created'] is False
    assert json_path.is_file() and md_path.is_file()
    assert not (tmp_path / 'backups').exists()


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
@pytest.mark.parametrize('failure', [OSError, KeyboardInterrupt])
def test_recovery_manifest_failure_keeps_completed_snapshot(wal_database, tmp_path, monkeypatch, creator, failure):
    from utils import verified_backup_compression as verification

    source, conn = wal_database
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()

    def fail_manifest(*args):
        raise failure('injected recovery manifest failure')

    monkeypatch.setattr(verification, '_atomic_json', fail_manifest)
    with pytest.raises(failure, match='recovery manifest'):
        create(creator, source, tmp_path)
    snapshots = list((tmp_path / 'backups').glob('*.db'))
    assert len(snapshots) == 1 and read_rows(snapshots[0]) == expected
    compressed = snapshots[0].with_suffix('.db.zst')
    assert restore(compressed, tmp_path / 'restored.db').read_bytes() == snapshots[0].read_bytes()
    assert not compressed.with_suffix('.zst.manifest.json').exists()
    assert conn.execute('SELECT * FROM observations ORDER BY id').fetchall() == expected


@pytest.mark.parametrize('creator', CREATORS, ids=['screening', 'nav', 'rank'])
def test_space_loss_before_compression_keeps_snapshot(wal_database, tmp_path, monkeypatch, creator):
    source, conn = wal_database
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    disk_usage = shutil.disk_usage

    def lose_space(path):
        if any(not item.name.startswith('.') for item in (tmp_path / 'backups').glob('*.db')):
            return SimpleNamespace(total=1, used=1, free=0)
        return disk_usage(path)

    monkeypatch.setattr(shutil, 'disk_usage', lose_space)
    with pytest.raises(ValueError, match='space'):
        create(creator, source, tmp_path)
    snapshots = list((tmp_path / 'backups').glob('*.db'))
    assert len(snapshots) == 1 and read_rows(snapshots[0]) == expected
    assert not list((tmp_path / 'backups').glob('*.zst'))


@pytest.mark.parametrize('failure', ['disk', 'timeout'])
def test_interrupted_copy_removes_only_its_temporary_files(wal_database, tmp_path, monkeypatch, failure):
    from utils import sqlite_backup_snapshot as snapshots

    source, conn = wal_database
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    if failure == 'timeout':
        monkeypatch.setattr(snapshots, 'SNAPSHOT_TIMEOUT_SECONDS', -1)
    else:
        original = shutil.disk_usage
        calls = 0

        def lose_space(path):
            nonlocal calls
            calls += 1
            return original(path) if calls == 1 else SimpleNamespace(total=1, used=1, free=0)

        monkeypatch.setattr(shutil, 'disk_usage', lose_space)
    with pytest.raises((ValueError, TimeoutError), match='space|time_budget'):
        create(CREATORS[0], source, tmp_path)
    assert not list((tmp_path / 'backups').glob('*.db*'))
    assert conn.execute('SELECT * FROM observations ORDER BY id').fetchall() == expected


def test_readiness_and_creation_budget_use_logical_size_including_wal(wal_database, tmp_path, monkeypatch):
    from utils.screening_history_cleanup_backup import build_screening_history_cleanup_backup_readiness_payload
    from utils.verified_backup_compression import BACKUP_METADATA_RESERVE_BYTES

    source, conn = wal_database
    conn.execute('INSERT INTO observations VALUES (4, NULL, ?)', ('test payload' * 30000,))
    conn.commit()
    main_size = source.stat().st_size
    logical_size = conn.execute('PRAGMA page_count').fetchone()[0] * conn.execute('PRAGMA page_size').fetchone()[0]
    assert logical_size > main_size * 2
    free = BACKUP_METADATA_RESERVE_BYTES + main_size * 3
    monkeypatch.setattr(shutil, 'disk_usage', lambda _: SimpleNamespace(total=free * 2, used=free, free=free))
    readiness = build_screening_history_cleanup_backup_readiness_payload(
        source_db_path=source, backup_dir=tmp_path / 'backups', project_root=tmp_path)
    assert readiness['status'] == 'blocked'
    assert readiness['source_db_logical_size_bytes'] == logical_size
    assert readiness['required_free_bytes'] > free
    assert any(item['code'] == 'insufficient_backup_disk_space' for item in readiness['blockers'])
    assert not (tmp_path / 'backups').exists()
    with pytest.raises(ValueError, match='space'):
        create(CREATORS[0], source, tmp_path)
    assert not list((tmp_path / 'backups').glob('*.db'))


def test_backup_freezes_metadata_and_rows_before_later_commit(wal_database, tmp_path, monkeypatch):
    from utils import sqlite_backup_snapshot as snapshots

    source, conn = wal_database
    expected = conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
    metadata = snapshots.sqlite_snapshot_metadata

    def commit_after_snapshot(*args):
        result = metadata(*args)
        conn.execute("INSERT INTO observations VALUES (4, 4.0, 'later test commit')")
        conn.commit()
        return result

    monkeypatch.setattr(snapshots, 'sqlite_snapshot_metadata', commit_after_snapshot)
    _, _, payload = create(CREATORS[0], source, tmp_path)
    artifact = Path(payload['summary']['backup_artifact_absolute_path'])
    assert read_rows(restore(artifact, tmp_path / 'restored.db')) == expected
    assert conn.execute('SELECT COUNT(*) FROM observations').fetchone()[0] == 4


@pytest.mark.parametrize('peer_suffix', ['.db', '.db.zst', '.db.manifest.json', '.db.zst.manifest.json'])
def test_existing_recovery_files_are_not_overwritten(wal_database, tmp_path, peer_suffix):
    source, _ = wal_database
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir()
    peer = backup_dir / ('bond_funds_pre_screening_cleanup_fixture' + peer_suffix)
    peer.write_bytes(b'protected existing test evidence')
    with pytest.raises(FileExistsError):
        create(CREATORS[0], source, tmp_path)
    assert peer.read_bytes() == b'protected existing test evidence'


def test_concurrent_creator_is_rejected_before_snapshot(wal_database, tmp_path):
    source, _ = wal_database
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir()
    with (backup_dir / '.backup_creation.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            create(CREATORS[0], source, tmp_path)
    assert not list(backup_dir.glob('*.db'))


def test_monthly_backup_failure_does_not_prune_existing_points(wal_database, tmp_path, monkeypatch):
    from schedulers.task_scheduler import TaskScheduler
    from utils import verified_backup_compression as verification

    source, _ = wal_database
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir()
    old = backup_dir / 'bond_funds_backup_20200101_000000.db.zst'
    old.write_bytes(b'protected previous fixture')
    scheduler = TaskScheduler.__new__(TaskScheduler)
    scheduler.db_manager = SimpleNamespace(db_path=source)
    scheduler.config = {}
    scheduler._execute_with_history = lambda name, logic, **kwargs: logic()
    cleanup_calls = []
    scheduler._cleanup_old_backups = lambda *args, **kwargs: cleanup_calls.append(args)

    def fail_manifest(*args):
        raise OSError('injected manifest failure')

    monkeypatch.setattr(verification, '_atomic_json', fail_manifest)
    with pytest.raises(OSError, match='manifest'):
        scheduler._monthly_backup()
    assert old.read_bytes() == b'protected previous fixture'
    assert cleanup_calls == []


def test_monthly_retention_keeps_manifests_as_historical_evidence(wal_database, tmp_path):
    from schedulers.task_scheduler import TaskScheduler

    source, _ = wal_database
    backup_dir = tmp_path / 'backups'
    backup_dir.mkdir()
    old = backup_dir / 'bond_funds_backup_20200101_000000.db.zst'
    old.write_bytes(b'old fixture outside retention')
    manifest = old.with_suffix(old.suffix + '.manifest.json')
    manifest.write_text('{"fixture_evidence": true}')
    protected = backup_dir / 'bond_funds_pre_migration_20200101_000000.db.zst'
    protected.write_bytes(b'protected maintenance recovery point')
    scheduler = TaskScheduler.__new__(TaskScheduler)
    scheduler.db_manager = SimpleNamespace(db_path=source)
    scheduler.config = {'scheduler': {'cleanup': {'database_backups': {'scheduled_keep_count': 1}}}}
    scheduler._execute_with_history = lambda name, logic, **kwargs: logic()
    result = scheduler._monthly_backup()
    assert Path(result['metadata']['backup_file']).exists()
    assert not old.exists()
    assert json.loads(manifest.read_text()) == {'fixture_evidence': True}
    assert protected.read_bytes() == b'protected maintenance recovery point'


@pytest.mark.parametrize('kind', ['nav', 'rank'])
def test_backup_cli_reports_missing_source_as_blocked(tmp_path, kind):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'paths': {'database_path': str(tmp_path / 'missing.db')}}))
    script = Path(__file__).resolve().parents[1] / 'scripts' / f'prepare_{kind}_history_archive_backup.py'
    command = [sys.executable, str(script), '--config', str(config), '--create-backup',
               '--backup-dir', str(tmp_path / 'backups'), '--output-dir', str(tmp_path / 'audit')]
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    payload = json.loads(result.stdout)
    assert payload['backup_summary']['source_db_path'] == str(tmp_path / 'missing.db')
    assert result.returncode == 2, payload
    assert payload['status'] == 'blocked'
    assert payload['backup_summary']['created'] is False
    assert not (tmp_path / 'backups').exists()


@pytest.mark.parametrize('kind', ['nav', 'rank'])
@pytest.mark.parametrize('dry_run', [False, True])
def test_backup_cli_preserves_create_and_dry_run_semantics(wal_database, tmp_path, kind, dry_run):
    source, conn = wal_database
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'paths': {'database_path': str(source)}}))
    script = Path(__file__).resolve().parents[1] / 'scripts' / f'prepare_{kind}_history_archive_backup.py'
    command = [sys.executable, str(script), '--config', str(config), '--skip-sha256',
               '--backup-dir', str(tmp_path / 'backups'), '--output-dir', str(tmp_path / 'audit')]
    if not dry_run:
        command.append('--create-backup')
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload['status'] == ('dry-run' if dry_run else 'success')
    assert payload['backup_summary']['created'] is not dry_run
    if dry_run:
        assert not (tmp_path / 'backups').exists()
    else:
        artifact = Path(payload['backup_summary']['backup_artifact_absolute_path'])
        restored = restore(artifact, tmp_path / 'restored.db')
        assert read_rows(restored) == conn.execute('SELECT * FROM observations ORDER BY id').fetchall()
        assert artifact.with_suffix(artifact.suffix + '.manifest.json').exists()


def test_blocked_nav_backup_never_attempts_checklist_binding(tmp_path):
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'paths': {'database_path': str(tmp_path / 'missing.db')}}))
    script = Path(__file__).resolve().parents[1] / 'scripts/prepare_nav_history_archive_backup.py'
    result = subprocess.run(
        [sys.executable, str(script), '--config', str(config), '--create-backup', '--bind-checklist',
         '--backup-dir', str(tmp_path / 'backups'), '--output-dir', str(tmp_path / 'audit')],
        capture_output=True, text=True, timeout=30)
    payload = json.loads(result.stdout)
    assert result.returncode == 2 and payload['status'] == 'blocked'
    assert payload['bind_result'] is None
    assert not list((tmp_path / 'audit').glob('*checklist*'))
