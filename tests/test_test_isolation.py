from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import isolation_support as isolation
from scripts import run_regression_smoke as smoke


ROOT = Path(__file__).resolve().parent.parent


def test_backend_and_core_defaults_bootstrap_only_the_workspace_database():
    completed = subprocess.run([sys.executable, "-c", """
import json
from pathlib import Path
import backend.app as app
from core.utils.project_runtime_config import build_core_runtime_config
root = Path.cwd()
backend = app.settings.database_path.resolve()
core = Path(build_core_runtime_config()['database']['path']).resolve()
assert backend == core and backend.is_relative_to(root)
assert app._schema_bootstrap_result.ok
print(json.dumps({'database': str(backend), 'bootstrapped': True}))
"""], cwd=ROOT, capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout.splitlines()[-1])["bootstrapped"]


def test_exec_child_cannot_read_write_truncate_outside_or_open_network(tmp_path):
    context = isolation.assert_isolated_context(ROOT)
    completed = subprocess.run([sys.executable, "-c", """
import json, os, socket, sys
from pathlib import Path
outside = Path(sys.argv[1])
operations = {
    'read': lambda: outside.read_bytes(),
    'write': lambda: outside.write_text('must never reach the outside file'),
    'truncate': lambda: os.truncate(outside, 0),
    'socket': lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM),
}
denied = []
for name, action in operations.items():
    try:
        action()
    except PermissionError:
        denied.append(name)
    else:
        raise AssertionError(name + ' was allowed')
Path(sys.argv[2]).write_text('allowed test artifact')
print(json.dumps(denied))
""", context["guard_path"], str(tmp_path / "child.txt")], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == ["read", "write", "truncate", "socket"]
    assert (tmp_path / "child.txt").read_text() == "allowed test artifact"


def test_source_runtime_files_are_unreadable_even_with_explicit_original_path():
    context = isolation.assert_isolated_context(ROOT)
    with pytest.raises(PermissionError):
        (Path(context["source_root"]) / "config/project.yaml").read_text()


def test_duckdb_import_can_read_container_limits_without_access_to_business_files():
    context = isolation.assert_isolated_context(ROOT)
    completed = subprocess.run([sys.executable, "-c", """
import socket, sys
from pathlib import Path
import duckdb
with duckdb.connect(':memory:', config={'threads': 1, 'memory_limit': '192MB',
                                        'autoload_known_extensions': False,
                                        'autoinstall_known_extensions': False}) as connection:
    assert connection.execute('SELECT 1').fetchone() == (1,)
for action in [lambda: Path(sys.argv[1]).read_bytes(),
               lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM)]:
    try:
        action()
    except PermissionError:
        pass
    else:
        raise AssertionError('business/network boundary was relaxed')
print('ok')
""", context["guard_path"]], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "ok"


def test_an_environment_flag_does_not_authorize_an_unrestricted_workspace(tmp_path, monkeypatch):
    outside = tmp_path / "guard"
    outside.write_text("outside")
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / isolation.CONTEXT_FILE).write_text(json.dumps({"guard_path": str(outside)}))
    monkeypatch.setenv("FUND_ANA_TEST_WORKSPACE", str(workspace))
    with pytest.raises(isolation.IsolationError, match="guard_readable"):
        isolation.assert_isolated_context(workspace)


def test_missing_context_refuses_collection_before_application_import(tmp_path, monkeypatch):
    monkeypatch.delenv("FUND_ANA_TEST_WORKSPACE")
    with pytest.raises(isolation.IsolationError, match="unsafe_pytest_entrypoint"):
        isolation.assert_isolated_context(tmp_path)


@pytest.mark.parametrize("argument", ["--basetemp=/app/backend/data", "--rootdir=/app",
                                      "--confcutdir=/app", "--noconftest", "-c", "-o"])
def test_pytest_arguments_cannot_replace_the_managed_boundary(argument):
    with pytest.raises(isolation.IsolationError, match="manages_pytest_option"):
        isolation.validate_pytest_args([argument])


def _minimal_source(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    for name in isolation.SOURCE_DIRS:
        (root / name).mkdir()
    for name in isolation.SOURCE_FILES:
        (root / name).write_text("# fixture requirements\n")
    (root / "backend/module.py").write_text("VALUE = 1\n")
    return root


def test_source_snapshot_excludes_database_quota_reports_and_output_symlinks(tmp_path):
    root = _minimal_source(tmp_path)
    data = root / "backend/data"
    data.mkdir()
    (data / "autostock_rate_limit_state.json").write_text('{"fixture": true}')
    (data / "bond_funds.db").write_bytes(b"fixture, not business data")
    (root / "reports").mkdir()
    (root / "reports/latest.json").write_text("{}")
    (root / "backend/fund_analysis.db").write_bytes(b"legacy fixture")
    (root / "core/audit").mkdir()
    (root / "core/audit/legacy.json").write_text('{"fixture": true}')
    (root / "core/bond_fund_screening").mkdir()
    (root / "core/bond_fund_screening/output").symlink_to(root / "reports")
    manifest = isolation.source_manifest(root)
    workspace = tmp_path / "copy"
    isolation.copy_source_workspace(root, workspace, manifest)
    assert isolation.source_manifest(workspace) == manifest
    assert not (workspace / "backend/data").exists()
    assert not (workspace / "reports").exists()
    assert not (workspace / "backend/fund_analysis.db").exists()
    assert not (workspace / "core/audit").exists()
    assert not (workspace / "core/bond_fund_screening/output").exists()


def test_unknown_source_symlink_is_rejected(tmp_path):
    root = _minimal_source(tmp_path)
    (root / "backend/unexpected.py").symlink_to(root / "backend/module.py")
    with pytest.raises(isolation.IsolationError, match="source_file_invalid"):
        isolation.source_manifest(root)


def test_snapshot_detects_source_changes_instead_of_testing_stale_code(tmp_path):
    root = _minimal_source(tmp_path)
    manifest = isolation.source_manifest(root)
    (root / "backend/module.py").write_text("VALUE = 2\n")
    with pytest.raises(isolation.IsolationError, match="source_changed_during_snapshot"):
        isolation.copy_source_workspace(root, tmp_path / "copy", manifest)


def test_test_environment_does_not_inherit_web_overrides_or_pytest_plugins(tmp_path, monkeypatch):
    monkeypatch.setenv("FUND_ANA_DISABLE_EMBEDDED_SCHEDULER", "1")
    monkeypatch.setenv("PYTEST_ADDOPTS", "--noconftest")
    monkeypatch.setenv("PYTHONPATH", "/app")
    env = isolation.build_test_environment(tmp_path)
    assert "FUND_ANA_DISABLE_EMBEDDED_SCHEDULER" not in env
    assert "PYTEST_ADDOPTS" not in env
    assert "PYTHONPATH" not in env
    assert env["TMPDIR"] == str(tmp_path / ".tmp")
    assert env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"


def test_image_source_mismatch_refuses_to_create_a_workspace(tmp_path):
    output = tmp_path / "not_created"
    completed = subprocess.run([sys.executable, "scripts/run_isolated_tests.py", "--in-container",
                               "--artifact-dir", str(output), "--expected-source-sha256", "0" * 64,
                               "--", "tests/test_test_isolation.py"], cwd=ROOT,
                              capture_output=True, text=True)
    assert completed.returncode == 2
    assert "test_image_source_mismatch" in completed.stderr
    assert not output.exists()


def test_existing_artifact_directory_is_never_reused_or_cleaned(tmp_path):
    marker = tmp_path / "keep.txt"
    marker.write_text("keep")
    completed = subprocess.run([sys.executable, "scripts/run_isolated_tests.py", "--in-container",
                               "--artifact-dir", str(tmp_path), "--", "--collect-only"],
                              cwd=ROOT, capture_output=True, text=True)
    assert completed.returncode == 2
    assert marker.read_text() == "keep"
    assert not (tmp_path / "workspace").exists()


def test_all_smoke_node_ids_resolve_to_current_tests():
    parsed = {}
    for group in smoke._build_smoke_groups():
        for command in group.commands:
            if command.argv[1:3] != ["-m", "pytest"]:
                continue
            for node in command.argv[4:]:
                filename, function = node.split("::", 1)
                if filename not in parsed:
                    parsed[filename] = {item.name for item in ast.parse((ROOT / filename).read_text()).body
                                        if isinstance(item, ast.FunctionDef)}
                assert function in parsed[filename], node


def test_smoke_isolates_unit_tests_and_keeps_real_audits_in_runtime(tmp_path, monkeypatch):
    seen = []

    def run(argv, **kwargs):
        seen.append(argv)
        return SimpleNamespace(returncode=0, stdout="passed", stderr="")

    monkeypatch.setattr(smoke.subprocess, "run", run)
    group = smoke.GroupSpec("separation", "test scope", [
        smoke.CommandSpec("unit", [sys.executable, "-m", "pytest", "-q", "tests/test_manager_refresh.py"]),
        smoke.CommandSpec("audit", [sys.executable, "scripts/audit_risk_residual_warnings.py"]),
    ])
    result = smoke._run_group(group, tmp_path)
    assert "scripts/run_isolated_tests.py" in seen[0]
    assert seen[1] == group.commands[1].argv
    assert [item["execution_scope"] for item in result["commands"]] == [
        "isolated_unit_regression", "runtime_audit_or_syntax"]


def test_missing_isolation_support_is_an_environment_failure_not_a_pass():
    result = smoke._classify_command_issue(returncode=2, stdout_text="",
                                          stderr_text="[isolated-tests] isolation_error: Landlock unavailable")
    assert result["issue_classification_status"] == "environment_issue"


def test_missing_kernel_support_never_falls_back_to_unrestricted_execution(tmp_path, monkeypatch):
    monkeypatch.setattr(isolation.ctypes, "CDLL", lambda *args, **kwargs: SimpleNamespace(syscall=lambda *args: -1))
    with pytest.raises(isolation.IsolationError, match="test_isolation_unavailable: Landlock"):
        isolation.restrict_test_io(tmp_path)
