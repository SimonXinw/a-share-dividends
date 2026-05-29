"""ETF 回测基础工具（通用，可被不同票复用）。"""

from __future__ import annotations

from collections import deque
from decimal import Decimal


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
