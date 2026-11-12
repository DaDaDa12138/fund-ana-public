"""Create standalone SQLite recovery points from a consistent, WAL-aware read."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time

from utils import verified_backup_compression as verification
from version import __version__


SNAPSHOT_TIMEOUT_SECONDS = 1800


def sqlite_snapshot_metadata(connection: sqlite3.Connection, source: Path) -> dict:
    """Read the logical database size; the main file can lag committed WAL pages."""
    page_count = int(connection.execute('PRAGMA page_count').fetchone()[0])
    page_size = int(connection.execute('PRAGMA page_size').fetchone()[0])
    schema = connection.execute(
        'SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name,tbl_name'
    ).fetchall()
    wal = Path(str(source) + '-wal')
    return {
        'source_database_path': str(source),
        'source_main_size_bytes': source.stat().st_size,
        'source_logical_size_bytes': page_count * page_size,
        'source_wal_size_bytes': wal.stat().st_size if wal.exists() else 0,
        'page_count': page_count, 'page_size': page_size,
        'database_user_version': int(connection.execute('PRAGMA user_version').fetchone()[0]),
        'database_schema_version': int(connection.execute('PRAGMA schema_version').fetchone()[0]),
        'database_application_id': int(connection.execute('PRAGMA application_id').fetchone()[0]),
        'schema_sha256': hashlib.sha256(json.dumps(schema, ensure_ascii=False,
                                                  separators=(',', ':')).encode()).hexdigest(),
    }


def _check_sqlite(connection: sqlite3.Connection, label: str) -> str:
    result = connection.execute('PRAGMA quick_check').fetchall()
    if result != [('ok',)]:
        raise ValueError(f'{label}_sqlite_quick_check_failed:{result[:3]}')
    return 'ok'


def create_verified_sqlite_backup(
    source: Path, destination: Path, *, purpose: str, backup_label: str,
    compress: bool = True, keep_uncompressed: bool = False,
    check_source: bool = False,
) -> dict:
    """Serialize backup creators and preserve the snapshot if verification fails.

    Only the private destination changes journal mode. The online database and
    its WAL are read through SQLite, never copied independently or checkpointed.
    """
    source, destination = Path(source).resolve(), Path(destination).absolute()
    if not source.is_file():
        raise ValueError('backup_source_must_be_regular_file')
    if destination.resolve() == source:
        raise ValueError('backup_destination_is_source')
    if compress and not shutil.which('zstd'):
        raise RuntimeError('zstd executable is required when compress=True')
    destination.parent.mkdir(parents=True, exist_ok=True)
    compressed = destination.with_suffix(destination.suffix + '.zst')
    peers = (destination, compressed, destination.with_suffix(destination.suffix + '.manifest.json'),
             compressed.with_suffix(compressed.suffix + '.manifest.json'))
    started = time.monotonic()
    with (destination.parent / '.backup_creation.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if any(path.exists() or path.is_symlink() for path in peers):
            raise FileExistsError(f'backup artifact already exists for label: {backup_label}')
        with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True, timeout=120)) as reader:
            # Freeze metadata and backup to the same committed read transaction.
            reader.execute('BEGIN')
            metadata = sqlite_snapshot_metadata(reader, source)
            metadata.update({
                'schema_version': 1, 'purpose': purpose, 'backup_label': backup_label,
                'method': 'sqlite_online_backup', 'application_version': __version__,
                'sqlite_library_version': sqlite3.sqlite_version,
                'read_transaction_observed_at': datetime.now().astimezone().isoformat(),
            })
            size = max(metadata['source_main_size_bytes'], metadata['source_logical_size_bytes'])
            budget = verification.backup_space_budget(destination.parent, size,
                                                       compress=compress, create_snapshot=True)
            verification.require_backup_space(budget)
            metadata['creation_space_budget'] = budget
            source_check = _check_sqlite(reader, 'source') if check_source else ''
            descriptor, name = tempfile.mkstemp(prefix=f'.{destination.stem}.',
                                                suffix='.partial.db', dir=destination.parent)
            os.close(descriptor)
            temporary = Path(name)

            def progress(status: int, remaining: int, total: int) -> None:
                if time.monotonic() - started > SNAPSHOT_TIMEOUT_SECONDS:
                    raise TimeoutError('backup_snapshot_time_budget_exceeded')
                # Recheck during the copy in case another host process consumes space.
                required = remaining * metadata['page_size'] + verification.BACKUP_METADATA_RESERVE_BYTES
                if compress:
                    required += int(total * metadata['page_size'] * 1.1)
                if shutil.disk_usage(destination.parent).free < required:
                    raise ValueError('insufficient_backup_disk_space_during_snapshot')

            try:
                with closing(sqlite3.connect(temporary, timeout=120)) as writer:
                    reader.backup(writer, pages=1000, progress=progress)
                    mode = writer.execute('PRAGMA journal_mode=DELETE').fetchone()
                    if mode != ('delete',):
                        raise ValueError('backup_snapshot_cannot_be_standalone')
                verification._assert_offline_source(temporary, [source])
                with temporary.open('rb') as stream:
                    os.fsync(stream.fileno())
                # Never replace an existing recovery point, even if a peer appears late.
                os.link(temporary, destination)
                verification._fsync_directory(destination.parent)
            finally:
                temporary.unlink(missing_ok=True)
                # These belong to our private destination, never the live database.
                for suffix in ('-wal', '-shm', '-journal'):
                    Path(str(temporary) + suffix).unlink(missing_ok=True)

        compression_started = time.monotonic()
        if compress:
            proof = verification.compress_verified_backup(
                destination, active_paths=[source], remove_source=not keep_uncompressed,
                snapshot_metadata=metadata,
            )
            artifact = Path(proof['compressed_path'])
            manifest = Path(proof['verification_manifest_path'])
            digest = proof['compressed_sha256']
        else:
            identity = verification._identity(destination)
            with closing(sqlite3.connect(destination.as_uri() + '?mode=ro&immutable=1', uri=True)) as conn:
                _check_sqlite(conn, 'backup')
            digest = verification._sha256(destination)
            if verification._identity(destination) != identity:
                raise ValueError('backup_snapshot_changed_during_verification')
            artifact = destination
            manifest = destination.with_suffix(destination.suffix + '.manifest.json')
            proof = {
                'schema_version': 1, 'verified_at': datetime.now().astimezone().isoformat(),
                'source_path': str(destination), 'source_size_bytes': destination.stat().st_size,
                'source_sha256': digest, 'source_identity': identity, 'source_removed': False,
                'sqlite_quick_check': 'ok', 'verification': 'unchanged_standalone_SQLite_snapshot',
                'snapshot': metadata, 'space_budget': budget,
            }
            verification._atomic_json(manifest, proof)
        return {
            'artifact_path': artifact, 'artifact_sha256': digest,
            'verification_manifest_path': str(manifest),
            'verification_manifest_sha256': verification._sha256(manifest),
            'recovery_verification': proof['verification'],
            'snapshot_sha256': proof['source_sha256'],
            'decoded_sha256': proof.get('decoded_sha256'),
            'snapshot_metadata': metadata, 'backup_space_budget': budget,
            'source_sqlite_quick_check': source_check, 'sqlite_quick_check': proof['sqlite_quick_check'],
            'zstd_test_succeeded': bool(compress),
            'uncompressed_removed': proof['source_removed'],
            'compression_elapsed_seconds': round(time.monotonic() - compression_started, 3) if compress else 0.0,
            'elapsed_seconds': round(time.monotonic() - started, 3),
        }
