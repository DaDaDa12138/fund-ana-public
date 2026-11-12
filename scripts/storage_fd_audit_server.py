#!/usr/bin/env python3
"""Host-only, socket-activated FD snapshots. No filesystem mutation API."""
from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import struct
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from backend.storage_descriptor_audit import scan_open_inodes


def snapshot(request: dict) -> dict:
    if (request.get('schema_version') != 1 or request.get('operation') != 'snapshot'
            or not isinstance(request.get('nonce'), str) or len(request['nonce']) != 32):
        raise ValueError('invalid_descriptor_audit_request')
    if Path('/.dockerenv').exists():
        raise ValueError('descriptor_audit_requires_host_process')
    result = scan_open_inodes()
    return {'schema_version': 1, 'status': 'verified', 'nonce': request['nonce'],
            'host_service': True, 'observed_at_epoch': time.time(),
            'pid_namespace_inode': Path('/proc/self/ns/pid').stat().st_ino,
            'open_inodes': sorted(result)}


def main() -> None:
    if int(os.environ.get('LISTEN_PID', '0')) != os.getpid() or os.environ.get('LISTEN_FDS') != '1':
        raise SystemExit('systemd_socket_activation_required')
    listener = socket.socket(fileno=3)
    while True:
        client, _ = listener.accept()
        with client:
            client.settimeout(5)
            try:
                _, uid, _ = struct.unpack('3i', client.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                if uid != 0:
                    raise ValueError('descriptor_audit_requires_root_peer')
                with client.makefile('rb') as stream:
                    raw = stream.readline(1025)
                if len(raw) > 1024 or not raw.endswith(b'\n'):
                    raise ValueError('invalid_descriptor_audit_request_size')
                response = snapshot(json.loads(raw))
            except Exception as exc:
                response = {'schema_version': 1, 'status': 'error', 'error': str(exc)}
            try:
                client.sendall(json.dumps(response, separators=(',', ':')).encode() + b'\n')
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass


if __name__ == '__main__':
    main()
