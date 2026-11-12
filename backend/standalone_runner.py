from __future__ import annotations

from typing import Any, Callable

from backend.config import Settings
from backend.core_runner import run_core_pipeline


def run_project_pipeline(
    settings: Settings,
    options: dict[str, Any],
    log: Callable[[str], None],
) -> dict[str, Any]:
    """Run the project-owned engine; snapshots cannot substitute for screening."""
    core_root = getattr(settings, "core_root", None)
    required_entries = ("main.py", "bond_fund_screening/tasks/daily_screening_master.py")
    if (
        core_root is None
        or not core_root.is_dir()
        or any(not (core_root / entry).is_file() for entry in required_entries)
    ):
        raise FileNotFoundError(
            "core_engine_unavailable: 项目内部 core 筛选引擎不可用或入口文件不完整，"
            f"请恢复完整核心目录：{core_root}"
        )

    log("使用项目内部 core 筛选引擎。")
    if (bool(options.get("run_screening", settings.run_screening_by_default))
            and not bool(options.get("run_collection", settings.run_collection_by_default))
            and not options.get("reuse_existing_outputs")
            and not options.get("skip_risk_calculation")):
        from backend.publication_dependencies import ensure_publication_dependency_configured
        ensure_publication_dependency_configured(settings)
    return run_core_pipeline(settings, options, log)
