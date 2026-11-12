"""Convert at most one sealed screening shard under the workflow owner lock.

The active SQLite file is never converted. Full export verification, three-run
restoration, and a byte-identical compressed SQLite recovery copy precede the
manifest switch and retirement of the redundant original.
"""
from __future__ import annotations

from contextlib import closing
from copy import deepcopy
from datetime import datetime
import fcntl
import json
from pathlib import Path
import resource
import shutil
import sqlite3
import time
from typing import Any, Callable
from uuid import uuid4

from utils.screening_archive_access import open_parquet_shard, probe_parquet_shard, verify_parquet_hot_values
from utils.screening_history_archive_manifest import load_screening_archive_manifest, write_screening_archive_manifest
from utils.verified_backup_compression import (
    _allocated, _assert_offline_source, _atomic_json, _decoded_digest,
    _fsync_directory, _identity, _sha256, compress_verified_backup,
)


def _readonly(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)


def _recovery_copy(source: Path, original: dict, active_paths: list[Path], log: Callable[[str], None]) -> dict:
    compressed = source.with_suffix(source.suffix + '.zst')
    manifest = compressed.with_suffix(compressed.suffix + '.manifest.json')
    if not (compressed.exists() and manifest.exists()):
        log('检查原SQLite并制作完整压缩恢复副本')
        result = compress_verified_backup(source, active_paths=active_paths, remove_source=False)
    else:
        # Resume from a previous completed compression without repeating a huge
        # SQLite integrity scan. Recheck EVERY decoded byte against the sealed
        # source SHA; the original must retain its previously checked identity.
        with (source.parent / '.backup_compression.lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _assert_offline_source(source, active_paths)
            if compressed.is_symlink() or manifest.is_symlink():
                raise ValueError('columnar_recovery_symlink_rejected')
            result = json.loads(manifest.read_text())
            identity = _identity(source)
            compressed_identity = _identity(compressed)
            if (result.get('schema_version') != 1 or result.get('sqlite_quick_check') != 'ok'
                    or result.get('source_identity') != list(identity)
                    or result.get('source_sha256') != original['sha256']
                    or result.get('verification') != 'full_decoded_bytes_equal_unchanged_offline_SQLite'):
                raise ValueError('columnar_recovery_previous_source_check_not_bound')
            log('逐字节复核已有压缩恢复副本')
            binary = shutil.which('zstd')
            if not binary:
                raise RuntimeError('zstd_required')
            size, digest = _decoded_digest(binary, compressed, identity[2])
            if (size != identity[2] or digest != original['sha256']
                    or result.get('decoded_size_bytes') != size or result.get('decoded_sha256') != digest
                    or result.get('compressed_sha256') != _sha256(compressed)
                    or result.get('compressed_size_bytes') != compressed.stat().st_size
                    or _identity(source) != identity or _identity(compressed) != compressed_identity):
                raise ValueError('columnar_recovery_decoded_bytes_mismatch')
            result = {**result, 'reverified_at': datetime.now().astimezone().isoformat(),
                      'verification_manifest_path': str(manifest)}
    if result['source_sha256'] != original['sha256']:
        raise ValueError('columnar_recovery_sealed_source_sha_mismatch')
    return result


def _maintain_columnar_archive_locked(
    *, project_root: Path, active_archive_path: Path, hot_db_path: Path,
    output_dir: Path, mode: str = 'preview', shard_id: str | None = None,
    existing_copies: list[dict] | None = None, keep_source: bool = False,
    log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Caller owns the workflow lock; this stable lock also excludes peer conversions."""
    if mode not in {'preview', 'apply'}:
        raise ValueError('columnar_mode_must_be_preview_or_apply')
    root = Path(project_root).resolve()
    active = Path(active_archive_path)
    active = (root / active).resolve() if not active.is_absolute() else active.resolve()
    state = load_screening_archive_manifest(active, project_root=root)
    if state['status'] != 'ready':
        raise ValueError('columnar_archive_manifest_not_ready')
    manifest_path = Path(state['manifest_path'])
    initial = manifest_path.read_bytes()
    payload = json.loads(initial)
    candidates = []
    for entry in payload['immutable_shards']:
        original = entry.get('sqlite_reference') if entry.get('format') == 'parquet' else entry
        if original and (not shard_id or entry['id'] == shard_id) and (manifest_path.parent / original['path']).exists():
            candidates.append((entry, original))
    if not candidates:
        return {'status': 'skipped', 'reason': 'no_unconverted_or_redundant_sqlite_shards', 'converted_count': 0}
    entry, original = candidates[0]
    source = manifest_path.parent / original['path']
    if source.is_symlink() or original.get('state') != 'immutable' or original.get('format', 'sqlite') != 'sqlite':
        raise ValueError('columnar_original_must_be_immutable_regular_sqlite')
    source = source.resolve()
    if not source.is_relative_to(active.parent) or source == active:
        raise ValueError('columnar_source_outside_archive_or_active')
    _assert_offline_source(source, [active, Path(hot_db_path)])
    source_identity = _identity(source)
    if (source_identity[2] != original['file_size_bytes'] or source_identity[3] != original['file_mtime_ns']
            or not original.get('sha256')):
        raise ValueError('columnar_source_no_longer_sealed')
    matches = [item for item in existing_copies or [] if item['shard_id'] == entry['id']]
    if len(matches) > 1:
        raise ValueError('columnar_duplicate_explicit_copy_binding')
    if entry.get('format') == 'parquet':
        directory = (manifest_path.parent / entry['path']).resolve().parent
    elif matches:
        directory = (root / matches[0]['directory']).resolve()
    else:
        directory = active.parent / 'analytics' / entry['id']
    if not directory.is_relative_to(active.parent) or directory == active.parent:
        raise ValueError('columnar_destination_outside_archive')
    result = {'status': 'preview', 'mode': mode, 'shard_id': entry['id'], 'source_path': str(source),
              'source_size_bytes': source_identity[2], 'source_sha256': original['sha256'],
              'directory': str(directory), 'keep_source': keep_source, 'converted_count': 0,
              'pending_sqlite_shard_count': len(candidates), 'manifest_generation_before': state['generation']}
    if mode == 'preview':
        return result
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    attempt = output_dir / ('conversion_' + datetime.now().strftime('%Y%m%d_%H%M%S_') + uuid4().hex[:8])
    attempt.mkdir()
    result['artifact_path'] = str(attempt / 'result.json')
    started = time.monotonic()
    before_free = shutil.disk_usage(active.parent).free
    log(f"历史列式转换开始: {entry['id']}; 原件 {source_identity[2]} bytes; 每次最多一个分片")
    lock_started = time.monotonic()
    with (active.parent / '.screening_columnar.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result['conversion_lock_acquire_seconds'] = time.monotonic() - lock_started
        try:
            if not directory.exists():
                # Budget for both the Parquet copy and the compressed recovery
                # copy. Preserve headroom and defer automatically if too low.
                if before_free < source_identity[2] * 2 + 2 * 1024**3:
                    raise ValueError('columnar_insufficient_space_for_verified_copies')
                import pyarrow as pa
                from scripts.export_screening_parquet import export_shard

                pa.set_cpu_count(1)
                pa.set_io_thread_count(1)
                log('导出并逐行复核七张历史表')
                result['export'] = export_shard(source, directory, original)
            parquet_manifest = directory / 'manifest.json'
            manifest_sha = _sha256(parquet_manifest)
            if matches and (matches[0]['manifest_sha256'] != manifest_sha
                            or matches[0]['source_sha256'] != original['sha256']):
                raise ValueError('columnar_explicit_copy_binding_mismatch')
            shard = {**entry, 'id': entry['id'], 'format': 'parquet', 'role': 'immutable',
                     'state': 'immutable', 'absolute_path': str(parquet_manifest),
                     'manifest_sha256': manifest_sha, 'source_sha256': original['sha256'],
                     'table_row_counts': original['table_row_counts']}
            reader = open_parquet_shard(shard)
            with closing(_readonly(source)) as connection:
                dated_runs = connection.execute(
                    'SELECT run_id,started_at FROM screening_runs ORDER BY started_at,run_id'
                ).fetchall()
            runs = [row[0] for row in dated_runs]
            if not runs or len(runs) != len(reader.run_ids) or set(runs) != reader.run_ids:
                raise ValueError('columnar_source_run_membership_mismatch')
            positions = sorted({0, len(runs) // 2, len(runs) - 1})
            selected = [runs[index] for index in positions]
            result['sample_basis'] = 'source_started_at_then_run_id_earliest_middle_latest'
            result['sample_runs'] = [{'run_id': dated_runs[index][0], 'started_at': dated_runs[index][1]}
                                     for index in positions]
            result['parquet_integrity'] = probe_parquet_shard(shard, selected)['check']
            with closing(_readonly(source)) as connection:
                result['source_value_comparison'] = verify_parquet_hot_values(shard, selected, connection)
            log('验证早、中、晚三个运行的独立SQLite恢复')
            from scripts.export_screening_parquet import TABLES, _rows_sha256, restore_run

            result['restoration_checks'] = []
            for index, run_id in enumerate(selected):
                restored = attempt / f'restored_{index}.db'
                restoration = restore_run(directory, run_id, restored)
                with closing(_readonly(source)) as original_db, closing(_readonly(restored)) as recovered:
                    hashes = {}
                    for table in TABLES:
                        from database.screening_decision_payloads import canonical_source

                        columns = [row[1] for row in original_db.execute(f'PRAGMA table_info("{table}")')]
                        projection, table_source = canonical_source(original_db, table, columns)
                        reference = _rows_sha256(original_db.execute(
                            f'SELECT h.rowid,{projection} FROM {table_source} WHERE h.run_id=? ORDER BY h.rowid',
                            (run_id,),
                        ).fetchall())
                        sql = f'SELECT rowid,* FROM "{table}" WHERE run_id=? ORDER BY rowid'
                        if _rows_sha256(recovered.execute(sql, (run_id,)).fetchall()) != reference:
                            raise ValueError(f'columnar_restored_values_differ:{table}:{run_id}')
                        hashes[table] = reference
                result['restoration_checks'].append({**restoration, 'table_sha256': hashes,
                    'database_sha256': _sha256(restored), 'temporary_database_removed': True})
                restored.unlink()
            recovery = _recovery_copy(source, original, [active, Path(hot_db_path)], log)
            result['recovery_copy'] = recovery
            compressed = source.with_suffix(source.suffix + '.zst')
            recovery_manifest = compressed.with_suffix(compressed.suffix + '.manifest.json')
            new_entry = {key: value for key, value in entry.items()
                         if key not in {'quick_check', 'quick_check_verified_at', 'quick_check_verified_size_bytes',
                                        'quick_check_verified_mtime_ns', 'absolute_path', 'role'}}
            new_entry.update(format='parquet', path=parquet_manifest.relative_to(active.parent).as_posix(),
                file_size_bytes=parquet_manifest.stat().st_size, file_mtime_ns=parquet_manifest.stat().st_mtime_ns,
                storage_size_bytes=result['parquet_integrity']['storage_size_bytes'], manifest_sha256=manifest_sha,
                source_sha256=original['sha256'], sqlite_reference=deepcopy(original),
                recovery_copy={'path': compressed.relative_to(active.parent).as_posix(),
                    'sha256': recovery['compressed_sha256'], 'manifest_path': recovery_manifest.relative_to(active.parent).as_posix(),
                    'manifest_sha256': _sha256(recovery_manifest)},
                converted_at=datetime.now().astimezone().isoformat())
            candidate = deepcopy(payload)
            candidate['immutable_shards'] = [new_entry if row['id'] == entry['id'] else row
                                              for row in candidate['immutable_shards']]
            candidate['generation'] += 1
            candidate['updated_at'] = datetime.now().isoformat()
            (attempt / 'previous_manifest.json').write_bytes(initial)
            _atomic_json(attempt / 'candidate_manifest.json', candidate)
            _atomic_json(attempt / 'precommit_checks.json', result)
            with (source.parent / '.backup_compression.lock').open('a+b') as backup_lock:
                fcntl.flock(backup_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                _assert_offline_source(source, [active, Path(hot_db_path)])
                if manifest_path.read_bytes() != initial or _identity(source) != source_identity:
                    raise ValueError('columnar_source_or_manifest_changed_before_commit')
                if _sha256(compressed) != recovery['compressed_sha256']:
                    raise ValueError('columnar_recovery_changed_before_commit')
                probe_parquet_shard(shard, selected)
                write_screening_archive_manifest(manifest_path, candidate)
                if load_screening_archive_manifest(active, project_root=root)['status'] != 'ready':
                    raise ValueError('columnar_committed_manifest_not_ready')
                if not keep_source:
                    source.unlink()
                    _fsync_directory(source.parent)
            result.update(status='converted_source_kept' if keep_source else 'converted', converted_count=1,
                          manifest_generation_after=candidate['generation'], source_removed=not source.exists(),
                          removed_source_allocated_bytes=0 if keep_source else recovery['source_allocated_bytes'],
                          final_storage_bytes=sum([_allocated(source), _allocated(compressed), _allocated(recovery_manifest),
                              _allocated(parquet_manifest), *[_allocated(reader._path(name)) for name in reader.files]]),
                          storage_scope='original_if_retained_plus_Parquet_manifest_data_and_compressed_recovery')
        except Exception as exc:
            result.update(status='failed', error=f'{type(exc).__name__}: {exc}', source_removed=not source.exists())
            log(f"历史列式转换失败，恢复材料保留: {result['error']}")
        finally:
            result.update(elapsed_seconds=round(time.monotonic() - started, 3),
                          available_bytes_before=before_free, available_bytes_after=shutil.disk_usage(active.parent).free,
                          peak_process_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          peak_child_rss_kib=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                          open_file_descriptor_count=len(list(Path('/proc/self/fd').iterdir())))
            _atomic_json(attempt / 'result.json', result)
    log(f"历史列式转换终态: {result['status']}; {result['elapsed_seconds']}秒; 原件已移除={result.get('source_removed')}")
    return result


def maintain_columnar_archive(
    *, project_root: Path, active_archive_path: Path, hot_db_path: Path,
    output_dir: Path, mode: str = 'preview', shard_id: str | None = None,
    existing_copies: list[dict] | None = None, keep_source: bool = False,
    workflow_state_file: Path | None = None, log: Callable[[str], None] = print,
) -> dict[str, Any]:
    """Both the weekly scheduler and operator CLI use the same process-wide lock."""
    from backend.storage import WorkflowLockBusyError, WorkflowStore

    arguments = dict(project_root=project_root, active_archive_path=active_archive_path,
        hot_db_path=hot_db_path, output_dir=output_dir, mode=mode, shard_id=shard_id,
        existing_copies=existing_copies, keep_source=keep_source, log=log)
    if mode == 'preview':
        return _maintain_columnar_archive_locked(**arguments)
    store = WorkflowStore(workflow_state_file or Path(project_root) / 'backend/data/workflow_state.json')
    started = time.monotonic()
    try:
        with store._lock:
            store._acquire_run_lock_locked('screening_columnar_maintenance')
    except WorkflowLockBusyError:
        log('历史列式转换延后：现有工作流仍持有维护锁')
        return {'status': 'deferred', 'reason': 'workflow_lock_busy', 'converted_count': 0}
    lock_seconds = time.monotonic() - started
    try:
        result = _maintain_columnar_archive_locked(**arguments)
        result['workflow_lock_acquire_seconds'] = lock_seconds
        if result.get('artifact_path'):
            _atomic_json(Path(result['artifact_path']), result)
        return result
    finally:
        with store._lock:
            store._release_run_lock_locked()
