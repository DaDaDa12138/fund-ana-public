"""Resolve frozen project artifacts across native and Docker workspaces.

The deployment's /app prefix is an explicit alias, not a basename search or a
fallback to another run. Resolve internal report aliases but reject traversal,
unknown absolute roots and symlinks that leave the configured project.
"""
from __future__ import annotations

from pathlib import Path


CONTAINER_PROJECT_ROOT = Path("/app")


def resolve_project_artifact_path(value: str | Path, *, project_root: Path | str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        raise ValueError("artifact_path_missing_or_invalid")
    path = Path(value)
    if not path.parts or ".." in path.parts or "\x00" in str(path):
        raise ValueError("artifact_path_traversal_or_invalid")
    configured_root = Path(project_root).absolute()
    try:
        root = configured_root.resolve()
        if not path.is_absolute():
            candidate = root / path
        elif path.is_relative_to(configured_root) or path.is_relative_to(root):
            candidate = path
        elif path.is_relative_to(CONTAINER_PROJECT_ROOT):
            candidate = root / path.relative_to(CONTAINER_PROJECT_ROOT)
        else:
            raise ValueError("artifact_path_unknown_absolute_root")
        resolved = candidate.resolve()
    except RuntimeError as error:
        raise ValueError("artifact_path_symlink_loop") from error
    if not resolved.is_relative_to(root):
        raise ValueError("artifact_path_outside_project")
    return resolved


def project_relative_artifact_path(value: str | Path, *, project_root: Path | str) -> str:
    path = resolve_project_artifact_path(value, project_root=project_root)
    return path.relative_to(Path(project_root).resolve()).as_posix()
