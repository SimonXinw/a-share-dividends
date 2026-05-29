"""易方达中证红利 ETF（515180）回测策略。"""

from __future__ import annotations

from decimal import Decimal

from ..base import build_ma_deviation_series

ETF_CODE = "515180"
ETF_NAME = "易方达中证红利ETF"
PROVIDER = "易方达"
TRACKING_INDEX = "中证红利"


def classify_zone(deviation_pct: Decimal | None) -> str:
    if deviation_pct is None:
        return "unknown"
    if deviation_pct <= Decimal("-8"):
        return "extreme-low"
    if deviation_pct <= Decimal("-5"):
        return "low"
    if deviation_pct < Decimal("8"):
        return "neutral"
    if deviation_pct < Decimal("18"):
        return "high"
    return "extreme-high"


def build_backtest_payload(history_rows: list[dict]) -> dict:
    """把通用日线数据转换为 515180 专用回测结果。"""
    items = build_ma_deviation_series(history_rows, ma_window=250)
    latest = items[-1] if items else None
    latest_dev = latest.get("nav_ma250_deviation_pct") if latest else None

    return {
        "etf": {
            "code": ETF_CODE,
            "name": ETF_NAME,
            "provider": PROVIDER,
            "tracking_index": TRACKING_INDEX,
        },
        "strategy": {
            "strategy_key": "yifangda_dividend_515180_ma250",
            "ma_window": 250,
            "latest_zone": classify_zone(latest_dev),
            "latest_deviation_pct": latest_dev,
        },
        "items": items,
        "total": len(items),
    }
