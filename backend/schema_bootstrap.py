"""启动前置 schema 迁移（P0-01 / B1）。

目标：在任何业务线程写入 `fund_risk_metrics` 等表之前，先把 `core/database/migrations.py::run_all_migrations`
跑完；避免"先 INSERT 报 OperationalError 再 ALTER"的时序裂缝。

调用方：
- `backend/app.py`（Flask 启动）
- `scripts/run_scheduler.py`（独立调度进程）
- `backend/workflow.py::WorkflowManager.__init__`（覆盖 CLI 入口 `scripts/run_bond_workflow.py`）

本 helper 进程内缓存成功结果，重复调用幂等且极廉价。
"""

from __future__ import annotations

import logging
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.config import Settings


logger = logging.getLogger(__name__)

_BOOTSTRAP_LOCK = threading.Lock()
_BOOTSTRAP_CACHE: dict[str, "SchemaBootstrapResult"] = {}


@dataclass(slots=True)
class SchemaBootstrapResult:
    database_path: str
    ok: bool
    error: str | None
    duration_ms: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "database_path": self.database_path,
            "ok": bool(self.ok),
            "error": self.error,
            "duration_ms": round(float(self.duration_ms), 3),
        }


def _inject_core_path(settings: Settings) -> None:
    core_root = Path(getattr(settings, "core_root", None) or (settings.project_root / "core")).resolve()
    if str(core_root) not in sys.path:
        sys.path.insert(0, str(core_root))


def ensure_database_schema(settings: Settings, *, force: bool = False) -> SchemaBootstrapResult:
    """确保目标 SQLite schema 已迁移到最新。幂等。

    Args:
        settings: 已加载的 Settings。
        force: True 则跳过进程内缓存再跑一次（主要给测试用）。
    """

    db_path = str(Path(settings.database_path).resolve())

    with _BOOTSTRAP_LOCK:
        cached = _BOOTSTRAP_CACHE.get(db_path)
        if cached and cached.ok and not force:
            return cached

        import time as _time

        started = _time.monotonic()
        try:
            # V6: 核心迁移函数（`migrate_ensure_all_tables` 等）在 DB 文件不存在时会
            # 直接 return False 跳过；对冷启动场景并不友好。我们先 touch 空文件让
            # sqlalchemy `Base.metadata.create_all` 真正建表，避免 bootstrap 返回
            # ok=True 但实际没有任何表的误导状态。
            db_path_obj = Path(db_path)
            db_path_obj.parent.mkdir(parents=True, exist_ok=True)
            if not db_path_obj.exists():
                db_path_obj.touch()

            _inject_core_path(settings)
            from database.migrations import run_all_migrations  # type: ignore

            run_all_migrations(db_path=db_path)
            result = SchemaBootstrapResult(
                database_path=db_path,
                ok=True,
                error=None,
                duration_ms=(_time.monotonic() - started) * 1000,
            )
            _BOOTSTRAP_CACHE[db_path] = result
            logger.info("schema bootstrap ok: %s", result.to_dict())
            return result
        except Exception as exc:  # pragma: no cover - defensive
            result = SchemaBootstrapResult(
                database_path=db_path,
                ok=False,
                error=f"{type(exc).__name__}: {exc}",
                duration_ms=(_time.monotonic() - started) * 1000,
            )
            # 失败不缓存：允许下次调用重试
            logger.error("schema bootstrap failed: %s", result.to_dict())
            raise


def reset_cache_for_tests() -> None:
    """仅测试场景使用：清空进程内缓存。"""

    with _BOOTSTRAP_LOCK:
        _BOOTSTRAP_CACHE.clear()
