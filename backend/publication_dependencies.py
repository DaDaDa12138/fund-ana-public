"""Known configuration blockers; this is not a provider health certificate."""
from __future__ import annotations

from typing import Any


def configured_publication_blocker(settings: Any) -> dict[str, Any] | None:
    provider = getattr(settings, "publication_validation_provider", "autostock")
    if provider == "eastmoney_cross_endpoint":
        urls = [getattr(settings, name, "") for name in (
            "tiantian_valuation_primary_url", "tiantian_valuation_fallback_url", "eastmoney_fund_info_url")]
        if all(str(url).startswith("https://") for url in urls):
            # Configured permits an actual comparison. It is neither a health
            # check nor a claim of supplier independence.
            return None
        return {"code": "validation_source_unconfigured", "source": provider,
                "configured_enabled": False, "source_complete": False,
                "replacement_independent_source": False,
                "message": "跨接口校验地址配置不完整；保留上一有效正式榜。"}
    if provider != "autostock":
        return {"code": "validation_source_unconfigured", "source": provider,
                "configured_enabled": False, "source_complete": False,
                "replacement_independent_source": False,
                "message": "正式数据校验来源配置无效；保留上一有效正式榜。"}
    if getattr(settings, "autostock_enabled", True) is not False:
        return None
    return {
        "code": "independent_source_unavailable",
        "source": "autostock",
        "configured_enabled": False,
        "source_complete": False,
        "replacement_independent_source": False,
        "message": "AutoStock 已停用，正式发布所需的独立来源校验不可用；保留上一有效正式榜。",
    }


def blocked_publication_result(blocker: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "blocked",
        "reason": blocker["code"],
        "blockers": [blocker],
        "run_id": None,
        "published": False,
    }


def ensure_publication_dependency_configured(settings: Any) -> None:
    blocker = configured_publication_blocker(settings)
    if blocker:
        raise RuntimeError(f"{blocker['code']}: {blocker['message']}")
