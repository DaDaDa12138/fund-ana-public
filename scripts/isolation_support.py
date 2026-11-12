"""Source-only pytest workspaces and inherited Linux I/O restrictions.

This is an accident-prevention boundary for repository tests, not a service for
running hostile code. Production audits deliberately do not use this helper.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import errno
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import sys
from typing import Any


SOURCE_DIRS = ("backend", "config", "core", "deploy", "frontend", "scripts", "tests")
SOURCE_FILES = ("requirements-runtime.txt", "requirements-analytics.txt")
EXCLUDED_DIRS = {"backend/data", "core/backend/data", "core/audit", "core/logs",
                 "core/bond_fund_screening/output", "deploy/systemd"}
CACHE_DIRS = {"__pycache__", ".pytest_cache", ".venv"}
NON_SOURCE_SUFFIXES = (".pyc", ".pyo", ".pyd", ".db", ".sqlite", ".sqlite3", "-wal", "-shm", ".bak")
CONTEXT_FILE = ".test_isolation.json"


class IsolationError(RuntimeError):
    pass


def source_manifest(root: Path) -> dict[str, str]:
    """Match the Docker COPY inputs; never follow runtime or source symlinks."""
    root = root.resolve()
    paths: list[Path] = []
    for name in SOURCE_DIRS:
        base = root / name
        if base.is_symlink() or not base.is_dir():
            raise IsolationError(f"test_source_directory_invalid: {name}")
        for walk, dirs, files in os.walk(base, followlinks=False):
            directory = Path(walk)
            kept = []
            for item in sorted(dirs):
                path = directory / item
                if item in CACHE_DIRS or path.relative_to(root).as_posix() in EXCLUDED_DIRS:
                    continue
                if path.is_symlink():
                    raise IsolationError(f"test_source_symlink: {path.relative_to(root)}")
                kept.append(item)
            dirs[:] = kept
            paths.extend(directory / item for item in sorted(files)
                         if not item.endswith(NON_SOURCE_SUFFIXES)
                         and (directory / item).relative_to(root).as_posix() not in EXCLUDED_DIRS)
    paths.extend(root / name for name in SOURCE_FILES)
    result = {}
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise IsolationError(f"test_source_file_invalid: {path.relative_to(root)}")
        result[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(result.items()))


def manifest_digest(manifest: dict[str, str]) -> str:
    return hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def copy_source_workspace(source: Path, workspace: Path, manifest: dict[str, str]) -> None:
    workspace.mkdir(exist_ok=False)
    for directory in SOURCE_DIRS:
        (workspace / directory).mkdir()
    for name in manifest:
        target = workspace / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target, follow_symlinks=False)
        shutil.copymode(source / name, target, follow_symlinks=False)
    if source_manifest(workspace) != manifest or source_manifest(source) != manifest:
        raise IsolationError("test_source_changed_during_snapshot")
    (workspace / ".tmp").mkdir()


def build_test_environment(workspace: Path) -> dict[str, str]:
    # Do not inherit PYTHONPATH, pytest plugins/options, API credentials, or the
    # Web-only embedded scheduler retirement override into unit-test stubs.
    env = {key: value for key, value in os.environ.items()
           if key in {"PATH", "LANG", "LC_ALL", "TZ"}}
    env.update({
        "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "TMPDIR": str(workspace / ".tmp"),
        "FUND_ANA_TEST_WORKSPACE": str(workspace),
        "FUND_ANA_STORAGE_ARCHIVE_ROOT": str(workspace / '.tmp/lifecycle_archives'),
        "FUND_ANA_TEST_ARTIFACT_ROOT": str(workspace / '.tmp/manual_tests'),
        "OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1",
    })
    return env


def validate_pytest_args(args: list[str]) -> None:
    forbidden = ("--basetemp", "--rootdir", "--confcutdir", "--noconftest", "--override-ini")
    for arg in args:
        if arg.split("=", 1)[0] in forbidden or arg in {"-c", "-o"}:
            raise IsolationError(f"test_runner_manages_pytest_option: {arg}")


def _restrict_network_and_legacy_truncate(landlock_abi: int) -> None:
    library = ctypes.util.find_library("seccomp")
    if not library:
        raise IsolationError("test_isolation_unavailable: libseccomp")
    lib = ctypes.CDLL(library, use_errno=True)
    lib.seccomp_init.argtypes = [ctypes.c_uint32]
    lib.seccomp_init.restype = ctypes.c_void_p
    lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    lib.seccomp_syscall_resolve_name.restype = ctypes.c_int
    lib.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
    lib.seccomp_load.argtypes = [ctypes.c_void_p]
    lib.seccomp_release.argtypes = [ctypes.c_void_p]
    context = lib.seccomp_init(0x7FFF0000)  # SCMP_ACT_ALLOW
    if not context:
        raise IsolationError("test_isolation_unavailable: seccomp_init")
    blocked = ["socket", "connect", "open_by_handle_at", "io_uring_setup", "ptrace",
               "process_vm_readv", "process_vm_writev"]
    # ABI 1/2 do not mediate truncate(path); ftruncate on an already permitted
    # writable descriptor remains available to SQLite and temporary files.
    if landlock_abi < 3:
        blocked.append("truncate")
    try:
        for name in blocked:
            number = lib.seccomp_syscall_resolve_name(name.encode())
            if number >= 0 and lib.seccomp_rule_add(context, 0x00050000 | errno.EPERM, number, 0) != 0:
                raise IsolationError(f"test_isolation_unavailable: seccomp_rule:{name}")
        if lib.seccomp_load(context) != 0:
            raise IsolationError("test_isolation_unavailable: seccomp_load")
    finally:
        lib.seccomp_release(context)


def restrict_test_io(workspace: Path) -> dict[str, Any]:
    """Restrictions survive exec/fork; missing kernel support fails closed."""
    if sys.platform != "linux" or platform.machine() not in {"x86_64", "aarch64"}:
        raise IsolationError("test_isolation_unavailable: supported Linux kernel required")
    libc = ctypes.CDLL(None, use_errno=True)
    abi = libc.syscall(444, 0, 0, 1)  # landlock_create_ruleset(..., VERSION)
    if abi < 1:
        raise IsolationError(f"test_isolation_unavailable: Landlock errno={ctypes.get_errno()}")
    handled = (1 << 13) - 1
    if abi >= 2:
        handled |= 1 << 13  # REFER
    if abi >= 3:
        handled |= 1 << 14  # TRUNCATE
    read_dir = (1 << 0) | (1 << 2) | (1 << 3)

    class Ruleset(ctypes.Structure):
        _fields_ = [("handled_access_fs", ctypes.c_uint64)]

    class PathRule(ctypes.Structure):
        _pack_ = 1
        _fields_ = [("allowed_access", ctypes.c_uint64), ("parent_fd", ctypes.c_int32)]

    ruleset = Ruleset(handled)
    descriptor = libc.syscall(444, ctypes.byref(ruleset), ctypes.sizeof(ruleset), 0)
    if descriptor < 0:
        raise IsolationError(f"test_isolation_unavailable: ruleset errno={ctypes.get_errno()}")

    def allow(path: Path, access: int) -> None:
        if not path.exists():
            return
        fd = os.open(path, os.O_PATH | os.O_CLOEXEC)
        try:
            rule = PathRule(access, fd)
            if libc.syscall(445, descriptor, 1, ctypes.byref(rule), 0) != 0:
                raise IsolationError(f"test_isolation_unavailable: path rule {path}")
        finally:
            os.close(fd)

    try:
        allow(workspace, handled)
        for name in ("/usr", "/lib", "/lib64", "/bin", "/sbin", "/proc"):
            allow(Path(name), read_dir)
        # DuckDB reads container memory/CPU limits while importing its types.
        # Denying cgroup metadata aborts the native library before tests start.
        # These kernel counters stay read-only; no business path is exposed.
        allow(Path("/sys/fs/cgroup"), read_dir)
        for name in ("/etc/ld.so.cache", "/etc/localtime", "/etc/timezone", "/etc/passwd",
                     "/etc/group", "/etc/nsswitch.conf", "/etc/os-release", "/.dockerenv",
                     "/dev/urandom", "/dev/random"):
            allow(Path(name), 1 << 2)
        allow(Path("/dev/null"), (1 << 1) | (1 << 2))
        # POSIX semaphore files are ephemeral IPC, never business persistence.
        allow(Path("/dev/shm"), (1 << 1) | (1 << 2) | (1 << 3) | (1 << 5) | (1 << 8))
        if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.syscall(446, descriptor, 0) != 0:
            raise IsolationError(f"test_isolation_unavailable: restrict errno={ctypes.get_errno()}")
    finally:
        os.close(descriptor)
    _restrict_network_and_legacy_truncate(abi)
    return {"landlock_abi": abi, "network_socket_creation": "denied",
            "business_paths_outside_workspace": "read_write_denied",
            "scope": "isolated_unit_regression"}


def assert_isolated_context(project_root: Path) -> dict[str, Any]:
    """Called by conftest before test modules can import backend.app."""
    workspace_text = os.environ.get("FUND_ANA_TEST_WORKSPACE", "")
    if not workspace_text or Path(workspace_text).resolve() != project_root.resolve():
        raise IsolationError("unsafe_pytest_entrypoint: use python3 scripts/run_isolated_tests.py -- <pytest args>")
    context_path = project_root / CONTEXT_FILE
    try:
        context = json.loads(context_path.read_text())
        guard = Path(context["guard_path"])
        if guard.is_relative_to(project_root):
            raise ValueError("guard_inside_workspace")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise IsolationError("test_isolation_context_invalid") from exc
    # An env flag alone is insufficient: prove an existing outside file is
    # actually unreadable, rather than absent, and sockets fail in the kernel.
    try:
        guard.read_bytes()
    except PermissionError:
        pass
    except OSError as exc:
        raise IsolationError("test_isolation_guard_missing") from exc
    else:
        raise IsolationError("test_isolation_guard_readable")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM):
            pass
    except PermissionError:
        pass
    else:
        raise IsolationError("test_isolation_network_not_blocked")
    return context


def check_default_runtime_paths(workspace: Path) -> dict[str, str]:
    from dataclasses import fields
    from backend.config import load_settings
    from core.utils.project_runtime_config import build_core_runtime_config

    settings = load_settings()
    for field in fields(settings):
        value = getattr(settings, field.name)
        if isinstance(value, Path) and not value.resolve().is_relative_to(workspace):
            raise IsolationError(f"test_runtime_path_outside_workspace: {field.name}")
    core_database = Path(build_core_runtime_config()["database"]["path"]).resolve()
    if not core_database.is_relative_to(workspace):
        raise IsolationError("test_core_database_outside_workspace")
    return {"backend_database": str(settings.database_path), "core_database": str(core_database)}
