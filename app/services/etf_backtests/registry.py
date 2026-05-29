"""ETF 策略注册中心：按代码路由到对应业务文件。"""

from __future__ import annotations

from .base import build_ma_deviation_series
from .yifangda import dividend_515180


def build_default_payload(code: str, history_rows: list[dict]) -> dict:
    items = build_ma_deviation_series(history_rows, ma_window=250)
    latest = items[-1] if items else None
    return {
        "etf": {
            "code": code,
            "name": code,
            "provider": None,
            "tracking_index": None,
        },
        "strategy": {
            "strategy_key": "generic_ma250",
            "ma_window": 250,
            "latest_zone": "unknown",
            "latest_deviation_pct": latest.get("nav_ma250_deviation_pct") if latest else None,
        },
        "items": items,
        "total": len(items),
    }


def build_backtest_payload(code: str, history_rows: list[dict]) -> dict:
    normalized = code.strip()
    if normalized == dividend_515180.ETF_CODE:
        return dividend_515180.build_backtest_payload(history_rows)
    return build_default_payload(normalized, history_rows)
