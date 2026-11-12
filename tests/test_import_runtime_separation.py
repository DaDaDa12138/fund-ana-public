"""Pure imports must work without writes; explicit runtimes must still log."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
IMPORT_MODULES = (
    "utils.logger", "utils.enhanced_logger", "database.connection",
    "database.task_history", "bond_fund_screening.config", "api.client",
    "bond_fund_screening.core.standard_risk_metrics",
    "bond_fund_screening.core.risk_calculator", "analyzers.risk_analyzer",
    "bond_fund_screening.analyzers.base_analyzer", "api.autostock_client",
    "validators.autostock_validator", "utils.config_manager",
    "schedulers.task_scheduler", "schedulers.task_timeout_monitor",
    "schedulers.run_scheduler", "backend.frozen_strategy_audit",
    "backend.calculation_input_audit", "backend.workflow", "backend.scheduler",
)


def run_child(source: str, tmp_path: Path, *arguments: str) -> dict:
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT / "core"))),
        "LOG_DIR_OVERRIDE": str(tmp_path / "runtime-logs"),
        "AUTOSTOCK_RATE_LIMIT_STATE_PATH": str(tmp_path / "quota.json"),
    }
    child = subprocess.run(
        [sys.executable, "-B", "-c", source, *arguments], cwd=tmp_path,
        env=environment, capture_output=True, text=True, timeout=40,
    )
    assert child.returncode == 0, child.stderr + child.stdout
    lines = [line for line in child.stdout.splitlines() if line.startswith("RESULT_JSON=")]
    assert len(lines) == 1, child.stdout
    return json.loads(lines[0].split("=", 1)[1])


@pytest.mark.parametrize("module", IMPORT_MODULES)
def test_pure_import_rejects_even_caught_filesystem_writes(module, tmp_path):
    result = run_child(r'''
import importlib, json, logging, os, sys
writes = []
write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
mutations = {"os.mkdir", "os.remove", "os.rmdir", "os.rename", "os.symlink",
             "os.link", "os.chmod", "os.chown", "os.truncate", "os.utime"}
def audit(event, args):
    writing = event in mutations
    if event == "open":
        mode, flags = args[1:3]
        writing = bool(isinstance(flags, int) and flags & write_flags)
        writing = writing or bool(isinstance(mode, str) and any(c in mode for c in "wax+"))
    if writing:
        writes.append([event, [str(value) for value in args]])
        raise PermissionError("pure import attempted a filesystem write")
sys.addaudithook(audit)
importlib.import_module(sys.argv[1])
handlers = [handler.baseFilename for logger in logging.Logger.manager.loggerDict.values()
            if isinstance(logger, logging.Logger) for handler in logger.handlers
            if isinstance(handler, logging.FileHandler)]
print("RESULT_JSON=" + json.dumps({"writes": writes, "file_handlers": handlers}))
''', tmp_path, module)
    assert result == {"writes": [], "file_handlers": []}
    assert not (tmp_path / "runtime-logs").exists()
    assert not (tmp_path / "quota.json").exists()


@pytest.mark.parametrize("configured", ["reports/custom-output", "/absolute/custom-output"])
def test_dangling_output_link_is_resolved_without_repair(monkeypatch, tmp_path, configured):
    from bond_fund_screening import config
    from utils import project_runtime_config

    directory = tmp_path / "core" / "bond_fund_screening"
    directory.mkdir(parents=True)
    legacy = directory / "output"
    legacy.symlink_to(tmp_path / "absent-host" / "reports")
    identity = legacy.lstat()
    monkeypatch.setattr(config, "__file__", str(directory / "config.py"))
    monkeypatch.setattr(config, "_PROJECT_ROOT", tmp_path / "core")
    monkeypatch.setattr(project_runtime_config, "load_project_config_payload",
                        lambda: {"paths": {"core_output_dir": configured}})
    expected = Path(configured) if Path(configured).is_absolute() else tmp_path / configured
    assert config._resolve_output_dir() == expected
    assert legacy.lstat() == identity
    assert not expected.exists()


@pytest.mark.parametrize("kind", ["absent", "directory", "working_symlink"])
def test_usable_legacy_output_path_is_preserved_without_creation(monkeypatch, tmp_path, kind):
    from bond_fund_screening import config

    directory = tmp_path / "core" / "bond_fund_screening"
    directory.mkdir(parents=True)
    legacy = directory / "output"
    if kind == "directory":
        legacy.mkdir()
    elif kind == "working_symlink":
        target = tmp_path / "configured-output"
        target.mkdir()
        legacy.symlink_to(target)
    monkeypatch.setattr(config, "__file__", str(directory / "config.py"))
    before = sorted(directory.iterdir())
    assert config._resolve_output_dir() == legacy
    assert sorted(directory.iterdir()) == before
    assert legacy.exists() == (kind != "absent")


def test_explicit_workflow_initialization_repairs_dangling_link(tmp_path):
    from backend.core_runner import ensure_core_output_link

    core = tmp_path / "core"
    (core / "bond_fund_screening").mkdir(parents=True)
    link = core / "bond_fund_screening" / "output"
    link.symlink_to(tmp_path / "absent-host-output")
    target = tmp_path / "configured" / "output"
    settings = SimpleNamespace(core_root=core, core_output_dir=target)
    assert ensure_core_output_link(settings) == link
    assert target.is_dir() and link.resolve() == target
    inode = link.lstat().st_ino
    assert ensure_core_output_link(settings) == link
    assert link.lstat().st_ino == inode


def test_concurrent_runtime_logger_setup_keeps_single_handlers_and_records(tmp_path):
    result = run_child(r'''
from concurrent.futures import ThreadPoolExecutor
import json, logging, os
from pathlib import Path
from utils.logger import setup_logger
def worker(number):
    logger = setup_logger("runtime-concurrent-probe", {"logging": {"emit_init_message": False}})
    logger.info("runtime-import-probe %d", number)
with ThreadPoolExecutor(max_workers=8) as pool:
    list(pool.map(worker, range(32)))
logger = logging.getLogger("runtime-concurrent-probe")
api_logger = logging.getLogger("api")
for handler in [*logger.handlers, *api_logger.handlers]:
    handler.flush()
root = Path(os.environ["LOG_DIR_OVERRIDE"])
collector = next(root.glob("collector_*.log")).read_text()
print("RESULT_JSON=" + json.dumps({
    "logger_handler_count": len(logger.handlers), "api_handler_count": len(api_logger.handlers),
    "records": [line.split("runtime-import-probe ")[-1]
                for line in collector.splitlines() if "runtime-import-probe " in line],
}))
''', tmp_path)
    assert result["logger_handler_count"] == 3
    assert result["api_handler_count"] == 1
    assert sorted(map(int, result["records"])) == list(range(32))


def test_explicit_runtime_initialization_preserves_task_database_and_api_logs(tmp_path):
    result = run_child(r'''
import json, logging, os
from pathlib import Path
from api.client import APIClient
from api.autostock_client import AutoStockClient
from database.connection import DatabaseManager
from database.models import TaskHistory
from database.task_history import TaskHistoryManager
from schedulers.task_scheduler import TaskScheduler
from sqlalchemy import text

root = Path.cwd()
db = DatabaseManager(str(root / "synthetic-runtime.db"))
TaskHistory.__table__.create(db.engine)
api = APIClient({"api": {"base_url": "http://127.0.0.1:1"}})
autostock = AutoStockClient(base_url="http://127.0.0.1:1", shared_state_path=str(root / "quota.json"))
scheduler = TaskScheduler(config={"scheduler": {"timezone": "Asia/Shanghai"}},
                          db_manager=db, api_client=api)
with db.get_session() as session:
    history = TaskHistoryManager(session)
    started = history.start_task("synthetic-runtime-probe")
    history.complete_task(started, success=False, records_processed=3, records_failed=1,
                          error_message="synthetic-runtime-probe-failure")
with db.get_session(readonly=True) as session:
    status = session.execute(text("SELECT status FROM task_history WHERE id=:id"),
                             {"id": started["id"]}).scalar_one()
db.engine.dispose()
for logger in logging.Logger.manager.loggerDict.values():
    if isinstance(logger, logging.Logger):
        for handler in logger.handlers:
            handler.flush()
logs = {path.name: path.read_text() for path in Path(os.environ["LOG_DIR_OVERRIDE"]).glob("*.log")}
print("RESULT_JSON=" + json.dumps({"status": status,
    "database_logged": any("ENGINE_CREATE" in value and "task_history" in value for value in logs.values()),
    "task_logged": any("START" in value and "COMPLETE" in value and "synthetic-runtime-probe" in value
                       for value in logs.values()),
    "autostock_logged": any("AutoStock API客户端初始化完成" in value for value in logs.values()),
    "api_log_exists": any(name.startswith("api_") for name in logs),
    "performance_log_exists": any(name.startswith("performance_") for name in logs),
    "quota_created": (root / "quota.json").exists(),
    "scheduler_running": scheduler.scheduler.running,
}, ensure_ascii=False))
''', tmp_path)
    assert result == {
        "status": "failed", "database_logged": True, "task_logged": True,
        "autostock_logged": True, "api_log_exists": True, "performance_log_exists": True,
        "quota_created": False, "scheduler_running": False,
    }
