"""Losslessly replace an offline SQLite backup with a verified Zstandard file."""
from __future__ import annotations

import fcntl
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile
from datetime import datetime
from typing import Iterable


BACKUP_METADATA_RESERVE_BYTES = 16 * 1024 * 1024
DEFAULT_COMPRESSED_SPACE_MULTIPLIER = 2.1
DEFAULT_UNCOMPRESSED_SPACE_MULTIPLIER = 1.1


def backup_space_budget(
    directory: Path, size_bytes: int, *, compress: bool = True,
    create_snapshot: bool = False, space_multiplier: float | None = None,
) -> dict:
    """Budget additional files, including incompressible input and metadata.

    Online snapshot creation must allow both the SQLite copy and its compressed
    peer to coexist. Offline compression already has the SQLite copy on disk.
    """
    path = Path(directory)
    while not path.exists() and path.parent != path:
        path = path.parent
    minimum = (DEFAULT_COMPRESSED_SPACE_MULTIPLIER if compress and create_snapshot
               else DEFAULT_UNCOMPRESSED_SPACE_MULTIPLIER)
    requested = minimum if space_multiplier is None else float(space_multiplier)
    if not math.isfinite(requested) or requested < 0:
        raise ValueError('invalid_backup_space_multiplier')
    multiplier = max(minimum, requested)
    required = math.ceil(max(0, size_bytes) * multiplier) + BACKUP_METADATA_RESERVE_BYTES
    usage = shutil.disk_usage(path)
    return {
        'disk_check_path': str(path), 'disk_total_bytes': usage.total,
        'disk_free_bytes': usage.free, 'required_free_bytes': required,
        'estimated_snapshot_size_bytes': size_bytes, 'space_multiplier': multiplier,
        'metadata_reserve_bytes': BACKUP_METADATA_RESERVE_BYTES,
        'disk_space_margin_bytes': usage.free - required,
        'sufficient': usage.free >= required,
    }


def require_backup_space(budget: dict) -> None:
    if not budget['sufficient']:
        raise ValueError(
            f"insufficient_backup_disk_space:free={budget['disk_free_bytes']} "
            f"required={budget['required_free_bytes']}"
        )


def _identity(path: Path) -> tuple[int, int, int, int, int]:
    value = path.stat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _allocated(path: Path) -> int:
    return path.stat().st_blocks * 512 if path.exists() else 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: dict) -> None:
    descriptor, name = tempfile.mkstemp(prefix=f'.{path.name}.', suffix='.tmp', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _decoded_digest(binary: str, path: Path, expected_size: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with subprocess.Popen([binary, '-q', '-d', '-c', str(path)], stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE) as process:
        assert process.stdout is not None
        try:
            for block in iter(lambda: process.stdout.read(1024 * 1024), b''):
                size += len(block)
                if size > expected_size:
                    raise ValueError('backup_decoded_size_exceeds_source')
                digest.update(block)
            assert process.stderr is not None
            error = process.stderr.read().decode('utf-8', errors='replace')
            if process.wait() != 0:
                raise ValueError(f'backup_decompression_failed:{error[-1000:]}')
        finally:
            if process.poll() is None:
                process.kill()
    return size, digest.hexdigest()


def _assert_offline_source(source: Path, active_paths: Iterable[Path]) -> None:
    if source.is_symlink() or not source.is_file():
        raise ValueError('backup_source_must_be_regular_file')
    if source.suffix.lower() not in {'.db', '.sqlite', '.sqlite3'}:
        raise ValueError('backup_source_extension_not_supported')
    for active in active_paths:
        if source.resolve() == active.resolve() or (active.exists() and source.samefile(active)):
            raise ValueError('active_database_cannot_be_compressed')
    if source.stat().st_nlink != 1:
        raise ValueError('hardlinked_backup_requires_separate_capacity_review')
    for suffix in ('-wal', '-journal'):
        sidecar = Path(str(source) + suffix)
        if sidecar.exists() and sidecar.stat().st_size:
            raise ValueError('backup_has_uncheckpointed_journal')


def compress_verified_backup(
    source: Path, *, active_paths: Iterable[Path], remove_source: bool,
    snapshot_metadata: dict | None = None,
) -> dict:
    """Check full decoded bytes and SQLite integrity before removing any source.

    The manifest preserves the original byte identity and restoration command.
    This function never checkpoints a database or changes its business data.
    """
    source = Path(source)
    binary = shutil.which('zstd')
    if not binary:
        raise RuntimeError('zstd executable is required for database backup compression')
    compressed = source.with_suffix(source.suffix + '.zst')
    manifest = compressed.with_suffix(compressed.suffix + '.manifest.json')
    active_paths = tuple(Path(path) for path in active_paths)
    with (source.parent / '.backup_compression.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _assert_offline_source(source, active_paths)
        if compressed.is_symlink() or manifest.is_symlink():
            raise ValueError('backup_compressed_path_must_not_be_symlink')
        identity = _identity(source)
        size = identity[2]
        before_allocated = sum(_allocated(path) for path in (source, compressed, manifest))
        source_allocated = _allocated(source)
        existed = compressed.exists()
        budget = backup_space_budget(source.parent, 0 if existed else size)
        require_backup_space(budget)
        with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)) as connection:
            checked = connection.execute('PRAGMA quick_check').fetchall()
        if checked != [('ok',)]:
            raise ValueError(f'backup_sqlite_quick_check_failed:{checked[:3]}')
        source_sha = _sha256(source)
        if _identity(source) != identity:
            raise ValueError('backup_source_changed_during_verification')

        if not existed:
            # SQLite checks and hashing may take time; use current space again.
            budget = backup_space_budget(source.parent, size)
            require_backup_space(budget)
            descriptor, name = tempfile.mkstemp(prefix=f'.{compressed.name}.', suffix='.tmp', dir=source.parent)
            os.close(descriptor)
            temporary = Path(name)
            try:
                subprocess.run([binary, '-T2', '-3', '-q', '-f', str(source), '-o', str(temporary)],
                               check=True, capture_output=True)
                with temporary.open('rb') as stream:
                    os.fsync(stream.fileno())
                verified_compressed_identity = _identity(temporary)
                decoded_size, decoded_sha = _decoded_digest(binary, temporary, size)
                if decoded_size != size or decoded_sha != source_sha:
                    raise ValueError('backup_decoded_content_does_not_match_source')
                compressed_sha = _sha256(temporary)
                if _identity(temporary) != verified_compressed_identity:
                    raise ValueError('backup_compressed_file_changed_during_verification')
                if _identity(source) != identity:
                    raise ValueError('backup_source_changed_during_compression')
                # An unexpected peer must never be overwritten.
                os.link(temporary, compressed)
                _fsync_directory(source.parent)
            finally:
                temporary.unlink(missing_ok=True)

        compressed_identity = _identity(compressed)
        if existed:
            decoded_size, decoded_sha = _decoded_digest(binary, compressed, size)
            compressed_sha = _sha256(compressed)
        elif compressed_identity[:4] != verified_compressed_identity[:4]:
            # Linking/unlinking changes ctime, but must preserve inode, size and mtime.
            raise ValueError('backup_compressed_file_changed_during_publication')
        if decoded_size != size or decoded_sha != source_sha:
            raise ValueError('backup_decoded_content_does_not_match_source')
        _assert_offline_source(source, active_paths)
        if _identity(source) != identity or _identity(compressed) != compressed_identity:
            raise ValueError('backup_file_changed_during_compression')
        proof = {
            'schema_version': 1, 'verified_at': datetime.now().astimezone().isoformat(),
            'source_path': str(source), 'source_size_bytes': size, 'source_sha256': source_sha,
            'source_identity': identity, 'source_allocated_bytes': source_allocated,
            'compressed_path': str(compressed), 'compressed_size_bytes': compressed.stat().st_size,
            'compressed_sha256': compressed_sha, 'decoded_size_bytes': decoded_size,
            'decoded_sha256': decoded_sha, 'sqlite_quick_check': 'ok',
            'verification': 'full_decoded_bytes_equal_unchanged_offline_SQLite',
            'restore_command_argv': [binary, '-d', str(compressed), '-o', str(source)],
            'source_removed': False,
            'space_budget': budget,
        }
        if snapshot_metadata is not None:
            proof['snapshot'] = dict(snapshot_metadata)
        # Durable recovery evidence precedes source removal.
        _atomic_json(manifest, proof)
        if remove_source:
            _assert_offline_source(source, active_paths)
            if _identity(source) != identity or _identity(compressed) != compressed_identity:
                raise ValueError('backup_file_changed_before_source_removal')
            source.unlink()
            _fsync_directory(source.parent)
            proof['source_removed'] = True
            _atomic_json(manifest, proof)
        after_allocated = sum(_allocated(path) for path in (source, compressed, manifest))
        return {
            **proof, 'existing_compressed': existed, 'verification_manifest_path': str(manifest),
            'removed_source_size_bytes': size if remove_source else 0,
            'allocated_bytes_before': before_allocated, 'allocated_bytes_after': after_allocated,
            'net_reclaimed_bytes': before_allocated - after_allocated,
            'capacity_scope': 'source_plus_compressed_plus_verification_manifest_allocated_blocks',
        }
