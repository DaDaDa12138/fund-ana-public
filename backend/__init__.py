from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CORE_ROOT = PROJECT_ROOT / "core"

# `backend` 会间接导入 `core/utils`, `core/api`, `core/collectors` 等模块。
# 生产容器里这些路径通常已由宿主环境注入；本地直接运行脚本时需要显式补齐。
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))
