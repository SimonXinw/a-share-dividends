"""宏观指标服务：拉取中国 10 年期国债收益率作为「无风险利率」。

数据源：akshare.bond_zh_us_rate()
   返回中美 1/2/3/5/10/20/30 年期国债收益率历史数据。
   我们取最新一行的「中国国债收益率10年」列。

调用方拿到的是小数（例如 0.0185 = 1.85%），与 last_year_dividend_yield 同口径。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

logger = logging.getLogger(__name__)

_TTL = timedelta(hours=24)
_CACHE: dict = {
    "risk_free_rate": None,  # Decimal | None
    "fetched_at": None,      # datetime | None
}
_lock = asyncio.Lock()


async def get_risk_free_rate(force_refresh: bool = False) -> Optional[Decimal]:
    """获取中国 10 年期国债收益率（小数形式）。

    带 24 小时内存缓存。拉取失败时返回旧缓存值（如有），避免前端瞬时无阈值。
    """
    now = datetime.now(timezone.utc)

    if not force_refresh and _is_cache_valid(now):
        return _CACHE["risk_free_rate"]

    async with _lock:
        if not force_refresh and _is_cache_valid(now):
            return _CACHE["risk_free_rate"]

        rate = await _fetch_china_10y_yield()
        if rate is not None:
            _CACHE["risk_free_rate"] = rate
            _CACHE["fetched_at"] = now
            logger.info("已更新 10 年期国债收益率：%.4f%%", float(rate) * 100)

        return _CACHE.get("risk_free_rate")


def get_fetched_at() -> Optional[datetime]:
    """返回最近一次成功拉取的时间（UTC）。"""
    return _CACHE.get("fetched_at")


def _is_cache_valid(now: datetime) -> bool:
    fetched_at = _CACHE.get("fetched_at")
    rate = _CACHE.get("risk_free_rate")
    if rate is None or fetched_at is None:
        return False
    return now - fetched_at < _TTL


async def _fetch_china_10y_yield() -> Optional[Decimal]:
    """从 akshare 拉取最新的中国 10 年期国债收益率（百分数 → 小数）。"""
    import akshare as ak

    try:
        df = await asyncio.to_thread(ak.bond_zh_us_rate)
    except Exception as e:  # noqa: BLE001
        logger.warning("拉取国债收益率失败：%s", e)
        return None

    if df is None or df.empty:
        return None

    candidates = (
        "中国国债收益率10年",
        "10年期国债收益率",
        "China 10-Year Treasury Bond Yield",
    )
    target_col = None
    for c in candidates:
        if c in df.columns:
            target_col = c
            break

    if target_col is None:
        logger.warning("国债收益率表缺少 10 年期列，列名=%s", list(df.columns))
        return None

    series = df[target_col].dropna()
    if series.empty:
        return None

    latest = series.iloc[-1]
    try:
        rate_pct = Decimal(str(latest))
    except (InvalidOperation, ValueError):
        return None

    # 异常值保护：国债收益率几乎不可能 ≤ 0 或 > 30%
    if rate_pct <= 0 or rate_pct > 30:
        return None

    return rate_pct / Decimal("100")
