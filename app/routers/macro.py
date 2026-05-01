"""宏观指标 API。"""

from __future__ import annotations

import logging

from fastapi import APIRouter

from ..services import macro

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/macro", tags=["macro"])


@router.get("/risk-free-rate")
async def get_rate() -> dict:
    """返回当前缓存中的中国 10 年期国债收益率（小数形式）。"""
    rate = await macro.get_risk_free_rate()
    fetched_at = macro.get_fetched_at()
    return {
        "risk_free_rate": rate,
        "fetched_at": fetched_at.isoformat() if fetched_at else None,
    }


@router.post("/refresh")
async def refresh_rate() -> dict:
    """强制重新拉取一次国债收益率，跳过缓存。"""
    rate = await macro.get_risk_free_rate(force_refresh=True)
    fetched_at = macro.get_fetched_at()
    return {
        "ok": True,
        "risk_free_rate": rate,
        "fetched_at": fetched_at.isoformat() if fetched_at else None,
    }
