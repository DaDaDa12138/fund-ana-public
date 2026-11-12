"""One shared, content-checked snapshot of the low-frequency fund directory."""
from __future__ import annotations
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import time
from typing import Any, Callable
from uuid import uuid4

MAX_CACHE_BYTES = 16 * 1024 * 1024


def _digest(data: Any) -> str:
    return hashlib.sha256(json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def get_directory_snapshot(
    path: Path, *, endpoint: str, ttl_seconds: float,
    fetch: Callable[[], tuple[list, dict]], clock: Callable[[], float] = time.time,
) -> tuple[list, dict]:
    """Merge simultaneous misses under one stable lock; never silently use stale data."""
    path = Path(path)
    if not math.isfinite(ttl_seconds) or ttl_seconds <= 0:
        raise ValueError('directory_cache_ttl_must_be_finite_and_positive')

    def metadata(payload: dict, *, status: str, age: float) -> dict:
        return {
            'cache_status': status, 'cache_age_seconds': age, 'cache_ttl_seconds': ttl_seconds,
            'cache_endpoint': endpoint, 'data_fetched_at_epoch': payload['fetched_at_epoch'],
            'data_sha256': payload['data_sha256'], 'origin_request_metadata': payload['origin_request_metadata'],
            'origin_request_metadata_sha256': payload['origin_request_metadata_sha256'],
            'cache_integrity_verified': status == 'hit',
        }

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_stream = path.with_name(path.name + '.lock').open('a+b')
    except OSError as exc:
        data, request_metadata = fetch()
        return data, {'cache_status': 'unavailable', 'cache_write_error': str(exc),
                      'origin_request_metadata': request_metadata, 'cache_integrity_verified': False}
    with lock_stream as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            rejection = None
            if path.is_file():
                try:
                    if path.stat().st_size > MAX_CACHE_BYTES:
                        raise ValueError('directory_cache_too_large')
                    payload = json.loads(path.read_text(encoding='utf-8'))
                    if (payload.get('schema_version') != 1 or payload.get('endpoint') != endpoint
                            or not isinstance(payload.get('data'), list)
                            or not isinstance(payload.get('origin_request_metadata'), dict)):
                        raise ValueError('directory_cache_identity_mismatch')
                    fetched_at = float(payload['fetched_at_epoch'])
                    age = clock() - fetched_at
                    if not math.isfinite(fetched_at) or not 0 <= age < ttl_seconds:
                        raise ValueError('directory_cache_expired_or_future_dated')
                    if _digest(payload['data']) != payload.get('data_sha256'):
                        raise ValueError('directory_cache_data_hash_mismatch')
                    if _digest(payload['origin_request_metadata']) != payload.get('origin_request_metadata_sha256'):
                        raise ValueError('directory_cache_origin_hash_mismatch')
                    return payload['data'], metadata(payload, status='hit', age=age)
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                    rejection = str(exc)
            data, request_metadata = fetch()
            if not isinstance(data, list) or not isinstance(request_metadata, dict):
                raise ValueError('directory_snapshot_requires_list_and_request_metadata')
            fetched_at = clock()
            signature = _digest(data)
            payload = {'schema_version': 1, 'endpoint': endpoint, 'fetched_at_epoch': fetched_at,
                       'data_sha256': signature, 'data': data, 'origin_request_metadata': request_metadata,
                       'origin_request_metadata_sha256': _digest(request_metadata)}
            encoded = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
            temporary = path.with_name(f'.{path.name}.{uuid4().hex}.tmp')
            write_error = None
            try:
                if len(encoded) > MAX_CACHE_BYTES:
                    raise ValueError('directory_cache_too_large')
                with temporary.open('xb') as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(path)
                descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except (OSError, ValueError) as exc:
                write_error = str(exc)
            finally:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError as exc:
                    write_error = write_error or str(exc)
            return data, {**metadata(payload, status='refreshed' if write_error is None else 'write_failed', age=0.0),
                          'cache_rejection_reason': rejection, 'cache_write_error': write_error,
                          }
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
