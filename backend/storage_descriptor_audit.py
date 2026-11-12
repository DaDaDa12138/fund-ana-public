"""Read-only host FD inspection protocol for a namespaced maintenance worker."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import stat
import struct
import time
import uuid

MAX_RESPONSE_BYTES = 8 * 1024 * 1024


def scan_open_inodes(proc_root: Path = Path('/proc')) -> set[tuple[int, int]]:
    result = set()
    try:
        # Path.glob suppresses some directory permission errors. A denied proc
        # root must never be reported as an empty, verified snapshot.
        processes = [path for path in proc_root.iterdir() if path.name.isdecimal()]
    except PermissionError as exc:
        raise ValueError('cannot_inspect_open_files') from exc
    if not processes:
        raise ValueError('cannot_inspect_open_files')
    for proc in processes:
        try:
            descriptors = list((proc / 'fd').iterdir())
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise ValueError('cannot_inspect_open_files') from exc
        for fd in descriptors:
            try:
                info = fd.stat()
                result.add((info.st_dev, info.st_ino))
            except FileNotFoundError:
                continue
    return result


def validate_snapshot(value: dict, nonce: str, started: float, finished: float) -> set[tuple[int, int]]:
    if (not isinstance(value, dict) or value.get('schema_version') != 1 or value.get('status') != 'verified'
            or value.get('nonce') != nonce or value.get('host_service') is not True):
        raise ValueError('host_descriptor_audit_invalid_response')
    observed = value.get('observed_at_epoch')
    if type(observed) not in (float, int) or not started - 1 <= observed <= finished + 1:
        raise ValueError('host_descriptor_audit_stale_response')
    if type(value.get('pid_namespace_inode')) is not int or value['pid_namespace_inode'] <= 0:
        raise ValueError('host_descriptor_audit_invalid_namespace')
    rows = value.get('open_inodes')
    if not isinstance(rows, list) or len(rows) > 200_000:
        raise ValueError('host_descriptor_audit_invalid_inodes')
    if any(not isinstance(row, list) or len(row) != 2
           or any(type(item) is not int or item < 0 for item in row) for row in rows):
        raise ValueError('host_descriptor_audit_invalid_inodes')
    return {tuple(row) for row in rows}


def request_host_inodes(path: Path) -> set[tuple[int, int]]:
    info = path.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != 0:
        raise ValueError('host_descriptor_audit_socket_invalid')
    nonce = uuid.uuid4().hex
    started = time.time()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(5)
        client.connect(str(path))
        _, uid, _ = struct.unpack('3i', client.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if uid != 0:
            raise ValueError('host_descriptor_audit_peer_invalid')
        client.sendall(json.dumps({'schema_version': 1, 'operation': 'snapshot', 'nonce': nonce}).encode() + b'\n')
        with client.makefile('rb') as stream:
            raw = stream.readline(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES or not raw.endswith(b'\n'):
            raise ValueError('host_descriptor_audit_response_too_large_or_incomplete')
    return validate_snapshot(json.loads(raw), nonce, started, time.time())


def configured_socket() -> Path | None:
    raw = os.environ.get('FUND_ANA_STORAGE_FD_AUDIT_SOCKET')
    return Path(raw) if raw else None
