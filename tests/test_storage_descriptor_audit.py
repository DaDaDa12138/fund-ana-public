"""Host FD protocol tests use artificial proc trees and mocked sockets only."""
import io
import json
from pathlib import Path
import socket
import stat
import struct
from types import SimpleNamespace

import pytest

from backend import storage_descriptor_audit as audit
from backend import storage_lifecycle as lifecycle
from scripts import storage_fd_audit_server as server


def response(**changes):
    return {'schema_version': 1, 'status': 'verified', 'nonce': 'a' * 32,
            'host_service': True, 'observed_at_epoch': 101.,
            'pid_namespace_inode': 123, 'open_inodes': [[7, 10], [7, 10]], **changes}


def test_snapshot_deduplicates_and_keeps_device_identity():
    assert audit.validate_snapshot(response(open_inodes=[[7, 10], [8, 10], [7, 10]]),
                                   'a' * 32, 100., 102.) == {(7, 10), (8, 10)}


@pytest.mark.parametrize('changes', [
    {'status': 'error'}, {'nonce': 'b' * 32}, {'host_service': False},
    {'schema_version': 2}, {'observed_at_epoch': 98.}, {'observed_at_epoch': 104.},
    {'observed_at_epoch': True}, {'observed_at_epoch': float('nan')},
    {'pid_namespace_inode': 0}, {'pid_namespace_inode': True},
    {'open_inodes': None}, {'open_inodes': [[-1, 5]]},
    {'open_inodes': [[1, True]]}, {'open_inodes': [[1, 2, 3]]},
])
def test_invalid_or_stale_snapshot_rejected(changes):
    with pytest.raises(ValueError):
        audit.validate_snapshot(response(**changes), 'a' * 32, 100., 102.)


def test_non_object_response_rejected():
    with pytest.raises(ValueError):
        audit.validate_snapshot([], 'a' * 32, 100., 102.)


def test_proc_scan_follows_descriptors_and_tolerates_exited_process(tmp_path):
    data = tmp_path / 'open-file'; data.write_text('synthetic data')
    descriptors = tmp_path / 'proc/123/fd'; descriptors.mkdir(parents=True)
    (descriptors / '1').symlink_to(data)
    (descriptors / '2').symlink_to(tmp_path / 'no-longer-present')
    (tmp_path / 'proc/456').mkdir()
    info = data.stat()
    assert audit.scan_open_inodes(tmp_path / 'proc') == {(info.st_dev, info.st_ino)}


def test_proc_permission_failure_does_not_mean_no_open_files(tmp_path, monkeypatch):
    root = tmp_path / 'proc'; (root / '123/fd').mkdir(parents=True)
    original = Path.iterdir
    def restricted(path):
        if path.name == 'fd':
            raise PermissionError('restricted synthetic process')
        return original(path)
    monkeypatch.setattr(Path, 'iterdir', restricted)
    with pytest.raises(ValueError, match='cannot_inspect_open_files'):
        audit.scan_open_inodes(root)


def test_empty_proc_is_not_a_verified_empty_snapshot(tmp_path):
    with pytest.raises(ValueError, match='cannot_inspect_open_files'):
        audit.scan_open_inodes(tmp_path)


def test_proc_root_permission_denied_is_not_silently_ignored(tmp_path, monkeypatch):
    def denied(path): raise PermissionError('synthetic proc mount denied')
    monkeypatch.setattr(Path, 'iterdir', denied)
    with pytest.raises(ValueError, match='cannot_inspect_open_files'):
        audit.scan_open_inodes(tmp_path)


@pytest.mark.parametrize('mode,uid', [(stat.S_IFREG, 0), (stat.S_IFLNK, 0), (stat.S_IFSOCK, 1000)])
def test_socket_identity_rejected(monkeypatch, mode, uid):
    monkeypatch.setattr(Path, 'lstat', lambda _: SimpleNamespace(st_mode=mode, st_uid=uid))
    with pytest.raises(ValueError, match='socket_invalid'):
        audit.request_host_inodes(Path('/synthetic/socket'))


class FakeSocket:
    uid = 0
    raw = None
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def settimeout(self, value): assert value == 5
    def connect(self, path): assert path == '/synthetic/socket'
    def getsockopt(self, *args): return struct.pack('3i', 123, self.uid, 0)
    def sendall(self, raw):
        self.request = json.loads(raw)
    def makefile(self, mode):
        value = response(nonce=self.request['nonce'], observed_at_epoch=audit.time.time())
        return io.BytesIO(self.raw if self.raw is not None else json.dumps(value).encode() + b'\n')


def fake_client(monkeypatch, **changes):
    monkeypatch.setattr(Path, 'lstat', lambda _: SimpleNamespace(st_mode=stat.S_IFSOCK, st_uid=0))
    client = FakeSocket()
    for key, value in changes.items(): setattr(client, key, value)
    monkeypatch.setattr(socket, 'socket', lambda *args: client)


def test_client_checks_peer_and_nonce(monkeypatch):
    fake_client(monkeypatch)
    assert audit.request_host_inodes(Path('/synthetic/socket')) == {(7, 10)}


@pytest.mark.parametrize('raw', [b'{}', b'x' * (audit.MAX_RESPONSE_BYTES + 1)])
def test_partial_or_oversized_response_rejected(monkeypatch, raw):
    fake_client(monkeypatch, raw=raw)
    with pytest.raises(ValueError, match='too_large_or_incomplete'):
        audit.request_host_inodes(Path('/synthetic/socket'))


def test_non_root_peer_rejected(monkeypatch):
    fake_client(monkeypatch, uid=1000)
    with pytest.raises(ValueError, match='peer_invalid'):
        audit.request_host_inodes(Path('/synthetic/socket'))


def test_configured_service_failure_never_falls_back_to_local_proc(monkeypatch):
    monkeypatch.setenv('FUND_ANA_STORAGE_FD_AUDIT_SOCKET', '/synthetic/socket')
    def unavailable(path): raise OSError('service unavailable')
    monkeypatch.setattr(audit, 'request_host_inodes', unavailable)
    with pytest.raises(OSError, match='service unavailable'):
        lifecycle.open_inodes()


def test_host_server_refuses_container(monkeypatch):
    monkeypatch.setattr(Path, 'exists', lambda _: True)
    with pytest.raises(ValueError, match='requires_host_process'):
        server.snapshot({'schema_version': 1, 'operation': 'snapshot', 'nonce': 'a' * 32})


def test_server_rejects_other_operations():
    with pytest.raises(ValueError, match='invalid_descriptor_audit_request'):
        server.snapshot({'schema_version': 1, 'operation': 'delete', 'nonce': 'a' * 32})
