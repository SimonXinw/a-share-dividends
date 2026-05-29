"""ETF 策略注册中心：按代码路由到对应业务文件。"""

from __future__ import annotations

from decimal import Decimal

from .base import build_equity_curve_from_positions, build_ma_deviation_series, build_price_series
from .yifangda import dividend_515180

BASE_POSITION = Decimal("0.00")
DCA_POSITION = Decimal("0.70")
SWING_POSITION = Decimal("0.30")


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


def _build_buy_and_hold_result(context: dict) -> dict:
    price_series = context["price_series"]
    positions = [Decimal("1")] * len(price_series)
    equity_curve, summary = build_equity_curve_from_positions(price_series, positions)
    return {
        "strategy_key": "buy_and_hold",
        "strategy_name": "买入并持有",
        "description": "全程满仓持有，作为原始基准收益对照",
        "equity_curve": equity_curve,
        "summary": summary,
    }


def _build_ma250_zone_result(context: dict) -> dict:
    price_series = context["price_series"]
    ma_series = context["ma_series"]
    positions: list[Decimal] = []
    for row in ma_series:
        deviation_pct = row.get("nav_ma250_deviation_pct")
        if deviation_pct is None:
            positions.append(Decimal("0.5"))
            continue
        deviation = Decimal(str(deviation_pct))
        if deviation <= Decimal("-8"):
            positions.append(Decimal("1"))
        elif deviation <= Decimal("-5"):
            positions.append(Decimal("0.8"))
        elif deviation < Decimal("8"):
            positions.append(Decimal("0.5"))
        elif deviation < Decimal("18"):
            positions.append(Decimal("0.2"))
        else:
            positions.append(Decimal("0"))
    equity_curve, summary = build_equity_curve_from_positions(price_series, positions)
    return {
        "strategy_key": "ma250_zone_rotation",
        "strategy_name": "MA250 偏离分层仓位（当前）",
        "description": "当前已使用策略：按偏离区间分配仓位",
        "equity_curve": equity_curve,
        "summary": summary,
    }


def _build_buy_event_indices(price_series: list[dict], cadence: str) -> list[int]:
    indices: list[int] = []
    last_marker = None
    for idx, row in enumerate(price_series):
        trade_date = row["trade_date"]
        if cadence == "daily":
            indices.append(idx)
            continue
        if cadence == "weekly":
            marker = (trade_date.isocalendar().year, trade_date.isocalendar().week)
        elif cadence == "quarterly":
            marker = (trade_date.year, (trade_date.month - 1) // 3 + 1)
        elif cadence == "yearly":
            marker = trade_date.year
        else:
            marker = (trade_date.year, trade_date.month)
        if marker != last_marker:
            indices.append(idx)
            last_marker = marker
    return indices


def _build_progress_positions(price_series: list[dict], cadence: str, target: Decimal) -> list[Decimal]:
    indices = _build_buy_event_indices(price_series, cadence)
    if not indices:
        return [Decimal("0")] * len(price_series)
    step = target / Decimal(len(indices))
    index_set = set(indices)
    current = Decimal("0")
    positions: list[Decimal] = []
    for idx in range(len(price_series)):
        if idx in index_set:
            current = min(target, current + step)
        positions.append(current)
    return positions


def _build_periodic_buy_result(context: dict, cadence: str, strategy_key: str, strategy_name: str) -> dict:
    price_series = context["price_series"]
    positions = _build_progress_positions(price_series, cadence, Decimal("1"))
    equity_curve, summary = build_equity_curve_from_positions(price_series, positions)
    return {
        "strategy_key": strategy_key,
        "strategy_name": strategy_name,
        "description": "按固定周期等额投入资金，不做卖出",
        "equity_curve": equity_curve,
        "summary": summary,
    }


def _build_ma250_band_result(context: dict, band_pct: int) -> dict:
    price_series = context["price_series"]
    ma_series = context["ma_series"]
    band = Decimal(str(band_pct))
    dca_positions = _build_progress_positions(price_series, "monthly", DCA_POSITION)
    positions: list[Decimal] = []
    for idx, row in enumerate(ma_series):
        deviation_pct = row.get("nav_ma250_deviation_pct")
        if deviation_pct is None:
            swing_ratio = Decimal("0.5")
        else:
            deviation = Decimal(str(deviation_pct))
            if deviation <= -band:
                swing_ratio = Decimal("1")
            elif deviation >= band:
                swing_ratio = Decimal("0")
            else:
                swing_ratio = (band - deviation) / (band * Decimal("2"))
        swing_position = SWING_POSITION * swing_ratio
        total_position = BASE_POSITION + dca_positions[idx] + swing_position
        if total_position > Decimal("1"):
            total_position = Decimal("1")
        if total_position < Decimal("0"):
            total_position = Decimal("0")
        positions.append(total_position)

    equity_curve, summary = build_equity_curve_from_positions(price_series, positions)
    return {
        "strategy_key": f"ma250_band_{band_pct}pct",
        "strategy_name": f"MA250 ±{band_pct}% 分层加减仓",
        "description": "先建仓 30%，剩余 70% 拆分为定投仓与波段仓（按偏离阈值动态加减仓）",
        "equity_curve": equity_curve,
        "summary": summary,
    }


STRATEGY_DEFINITIONS = [
    {"strategy_key": "ma250_zone_rotation", "strategy_name": "MA250 偏离分层仓位（当前）", "description": "当前策略"},
    {"strategy_key": "periodic_buy_daily", "strategy_name": "每日买入", "description": "每日定投"},
    {"strategy_key": "periodic_buy_weekly", "strategy_name": "每周买入", "description": "每周定投"},
    {"strategy_key": "periodic_buy_monthly", "strategy_name": "每月买入", "description": "每月定投"},
    {"strategy_key": "periodic_buy_quarterly", "strategy_name": "每季度买入", "description": "每季度定投"},
    {"strategy_key": "periodic_buy_yearly", "strategy_name": "每年买入", "description": "每年定投"},
]
for _band in range(1, 11):
    STRATEGY_DEFINITIONS.append(
        {
            "strategy_key": f"ma250_band_{_band}pct",
            "strategy_name": f"MA250 ±{_band}% 分层加减仓",
            "description": "30% 底仓 + 定投 + 波段仓",
        }
    )


def get_available_strategies() -> list[dict]:
    return STRATEGY_DEFINITIONS


def _strategy_builders() -> dict:
    builders = {
        "ma250_zone_rotation": lambda context: _build_ma250_zone_result(context),
        "periodic_buy_daily": lambda context: _build_periodic_buy_result(
            context, "daily", "periodic_buy_daily", "每日买入"
        ),
        "periodic_buy_weekly": lambda context: _build_periodic_buy_result(
            context, "weekly", "periodic_buy_weekly", "每周买入"
        ),
        "periodic_buy_monthly": lambda context: _build_periodic_buy_result(
            context, "monthly", "periodic_buy_monthly", "每月买入"
        ),
        "periodic_buy_quarterly": lambda context: _build_periodic_buy_result(
            context, "quarterly", "periodic_buy_quarterly", "每季度买入"
        ),
        "periodic_buy_yearly": lambda context: _build_periodic_buy_result(
            context, "yearly", "periodic_buy_yearly", "每年买入"
        ),
    }
    for band in range(1, 11):
        builders[f"ma250_band_{band}pct"] = lambda context, b=band: _build_ma250_band_result(context, b)
    return builders


def resolve_strategy_keys(strategy_keys: list[str] | None) -> tuple[list[str], list[str]]:
    builders = _strategy_builders()
    if not strategy_keys:
        return list(builders.keys()), []
    selected: list[str] = []
    ignored: list[str] = []
    for item in strategy_keys:
        key = item.strip()
        if not key:
            continue
        if key in builders:
            selected.append(key)
        else:
            ignored.append(key)
    return (selected or list(builders.keys())), ignored


def build_compare_context(code: str, history_rows: list[dict]) -> dict:
    normalized_code = code.strip()
    price_series = build_price_series(history_rows)
    ma_series = build_ma_deviation_series(history_rows, ma_window=250)
    return {
        "etf": {
            "code": normalized_code,
            "name": dividend_515180.ETF_NAME if normalized_code == dividend_515180.ETF_CODE else normalized_code,
            "provider": dividend_515180.PROVIDER if normalized_code == dividend_515180.ETF_CODE else None,
            "tracking_index": dividend_515180.TRACKING_INDEX if normalized_code == dividend_515180.ETF_CODE else None,
        },
        "price_series": price_series,
        "ma_series": ma_series,
        "total": len(price_series),
    }


def build_benchmark_result(context: dict) -> dict:
    return _build_buy_and_hold_result(context)


def build_strategy_result(strategy_key: str, context: dict) -> dict:
    builder = _strategy_builders().get(strategy_key)
    if builder is None:
        raise KeyError(f"未知策略: {strategy_key}")
    return builder(context)


def build_compare_payload(
    code: str,
    history_rows: list[dict],
    strategy_keys: list[str] | None = None,
) -> dict:
    context = build_compare_context(code, history_rows)
    selected_keys, ignored_keys = resolve_strategy_keys(strategy_keys)
    benchmark = build_benchmark_result(context)
    strategy_results = [build_strategy_result(item, context) for item in selected_keys]
    return {
        "etf": context["etf"],
        "benchmark": benchmark,
        "strategies": strategy_results,
        "ignored_strategies": ignored_keys,
        "available_strategies": get_available_strategies(),
        "total": context["total"],
    }
