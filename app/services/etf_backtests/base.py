"""ETF 回测基础工具（通用，可被不同票复用）。"""

from __future__ import annotations

from collections import deque
from datetime import date, datetime
from decimal import Decimal
import math


def build_ma_deviation_series(
    rows: list[dict],
    ma_window: int = 250,
) -> list[dict]:
    """根据日线收盘价生成 MA 与偏离度序列。

    入参 rows 要求按日期升序，且包含:
    - trade_date (str/date)
    - close_price (Decimal/float)
    """
    series: list[dict] = []
    rolling_values: deque[Decimal] = deque()
    rolling_sum = Decimal("0")

    for row in rows:
        close_price = Decimal(str(row["close_price"]))
        rolling_values.append(close_price)
        rolling_sum += close_price
        if len(rolling_values) > ma_window:
            popped = rolling_values.popleft()
            rolling_sum -= popped

        ma_value: Decimal | None = None
        deviation_pct: Decimal | None = None
        if len(rolling_values) == ma_window:
            ma_value = rolling_sum / Decimal(ma_window)
            if ma_value != 0:
                deviation_pct = (close_price / ma_value - Decimal("1")) * Decimal("100")

        series.append(
            {
                "date": str(row["trade_date"])[:10],
                "adj_net_price": close_price,
                "ma250": ma_value,
                "nav_ma250_deviation_pct": deviation_pct,
            }
        )

    return series


def to_date(value) -> date:
    text = str(value)[:10]
    return datetime.fromisoformat(text).date()


def build_price_series(rows: list[dict]) -> list[dict]:
    series: list[dict] = []
    for row in rows:
        price = Decimal(str(row["close_price"]))
        series.append(
            {
                "date": str(row["trade_date"])[:10],
                "trade_date": to_date(row["trade_date"]),
                "price": price,
            }
        )
    return series


def calc_annualized_return_pct(start_nav: Decimal, end_nav: Decimal, days: int) -> Decimal:
    if start_nav <= 0 or end_nav <= 0 or days <= 0:
        return Decimal("0")
    years = days / 365.25
    if years <= 0:
        return Decimal("0")
    annual_factor = float(end_nav / start_nav)
    annualized = math.pow(annual_factor, 1 / years) - 1
    return Decimal(str(annualized * 100))


def calc_max_drawdown_pct(nav_curve: list[Decimal]) -> Decimal:
    if not nav_curve:
        return Decimal("0")
    peak = nav_curve[0]
    max_drawdown = Decimal("0")
    for nav in nav_curve:
        if nav > peak:
            peak = nav
        if peak > 0:
            drawdown = (peak - nav) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
    return max_drawdown * Decimal("100")


def build_equity_curve_from_positions(
    price_series: list[dict],
    positions: list[Decimal],
) -> tuple[list[dict], dict]:
    if not price_series or not positions:
        return [], {}
    if len(price_series) != len(positions):
        raise ValueError("价格序列与仓位序列长度不一致")

    nav = Decimal("1")
    nav_curve: list[Decimal] = [nav]
    equity_curve = [
        {
            "date": price_series[0]["date"],
            "price": price_series[0]["price"],
            "nav": nav,
            "position": positions[0],
        }
    ]
    trade_count = 0
    for idx in range(1, len(price_series)):
        prev_price = price_series[idx - 1]["price"]
        curr_price = price_series[idx]["price"]
        if prev_price > 0:
            daily_ret = curr_price / prev_price - Decimal("1")
            nav = nav * (Decimal("1") + daily_ret * positions[idx - 1])
        nav_curve.append(nav)

        if positions[idx] != positions[idx - 1]:
            trade_count += 1

        equity_curve.append(
            {
                "date": price_series[idx]["date"],
                "price": curr_price,
                "nav": nav,
                "position": positions[idx],
            }
        )

    start_trade_date = price_series[0]["trade_date"]
    end_trade_date = price_series[-1]["trade_date"]
    span_days = max(1, (end_trade_date - start_trade_date).days)
    total_return_pct = (nav_curve[-1] - Decimal("1")) * Decimal("100")
    annualized_return_pct = calc_annualized_return_pct(Decimal("1"), nav_curve[-1], span_days)
    max_drawdown_pct = calc_max_drawdown_pct(nav_curve)

    summary = {
        "start_date": price_series[0]["date"],
        "end_date": price_series[-1]["date"],
        "span_days": span_days,
        "total_return_pct": total_return_pct,
        "annualized_return_pct": annualized_return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "trade_count": trade_count,
    }
    return equity_curve, summary
