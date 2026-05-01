"""股息计算核心服务。

计算规则（与产品需求一致）：
1. 若用户在覆盖表 a_share_overrides 中手动指定了 this_year_estimated_profit，则直接使用。
2. 否则使用如下推导：
   - 取去年（last_year）的全年净利润 last_year_profit
   - 取去年各季度净利润 last_year_q_profit[1..4]
   - 取今年（last_year + 1）已发布的季度净利润 this_year_q_profit{q -> profit}
   - 今年预估净利润 = last_year_profit
                       - sum(last_year_q_profit[q] for q in this_year_q_profit)
                       + sum(this_year_q_profit.values())
3. 分红比例 payout_ratio = last_year_dividend_total / last_year_profit
   - 若数据库已存（来自数据源），优先使用
   - 否则用 last_year_dividend_per_share / (last_year_profit / 总股本) 推算（实现里我们使用每股口径，避免依赖股本）
   - 简化版本：直接对"每股"做等比例缩放：
        this_year_estimated_dividend_per_share =
            last_year_dividend_per_share * this_year_estimated_profit / last_year_profit
4. 今年预估股息率 = this_year_estimated_dividend_per_share / current_price

如果缺少必要数据（去年净利润缺失等），则只展示已有字段，预估字段为 None。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from .. import database


@dataclass
class CalculationContext:
    """单只股票计算所需的全部原始数据。"""

    code: str
    name: str
    industry: str | None
    price: Decimal | None
    price_date: str | None
    price_sync_date: str | None
    fundamental_sync_date: str | None
    sync_date: str | None
    current_market_cap: Decimal | None
    last_year_end_market_cap: Decimal | None
    last_year_end_price: Decimal | None
    last_year_end_date: str | None

    last_year: int | None
    last_year_dividend: Decimal | None
    last_year_net_profit: Decimal | None
    payout_ratio: Decimal | None  # 数据源给的分红比例，可能为 None

    last_year_quarter_profits: dict[int, Decimal]  # {1: xxx, 2: xxx, 3: xxx, 4: xxx}
    this_year_quarter_profits: dict[int, Decimal]

    override_this_year_profit: Decimal | None
    note: str | None


def _to_decimal(v) -> Decimal | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def estimate_this_year_profit(ctx: CalculationContext) -> Decimal | None:
    """推算今年预估全年净利润。"""
    if ctx.override_this_year_profit is not None:
        return ctx.override_this_year_profit

    if ctx.last_year_net_profit is None:
        return None

    last_year_profit = ctx.last_year_net_profit

    if not ctx.this_year_quarter_profits:
        # 没有今年季度数据，最朴素的预估就是延续去年
        return last_year_profit

    replaced_last_year_sum = Decimal("0")
    for q in ctx.this_year_quarter_profits.keys():
        last = ctx.last_year_quarter_profits.get(q)
        if last is None:
            # 去年同季度数据缺失，无法替换；此季度退回"按去年估算"
            # 等价于今年此季度=去年此季度，相当于不做替换
            replaced_last_year_sum += ctx.this_year_quarter_profits[q]
        else:
            replaced_last_year_sum += last

    this_year_published_sum = sum(ctx.this_year_quarter_profits.values(), Decimal("0"))

    return last_year_profit - replaced_last_year_sum + this_year_published_sum


def estimate_this_year_dividend_per_share(ctx: CalculationContext) -> Decimal | None:
    """推算今年预估每股分红。

    思路：保持「每股分红 / 每股净利润」（即分红比例）不变，按预估利润等比例放大/缩小。
    """
    estimated_profit = estimate_this_year_profit(ctx)

    if (
        estimated_profit is None
        or ctx.last_year_net_profit is None
        or ctx.last_year_net_profit == 0
        or ctx.last_year_dividend is None
    ):
        return None

    ratio = estimated_profit / ctx.last_year_net_profit
    return ctx.last_year_dividend * ratio


def calc_yield(dividend_per_share: Decimal | None, price: Decimal | None) -> Decimal | None:
    if dividend_per_share is None or price is None or price == 0:
        return None
    return dividend_per_share / price


# ============================================================================
# 从数据库一次性聚合所有 active 股票的计算上下文
# ============================================================================
async def load_all_contexts() -> list[CalculationContext]:
    """从数据库读取所有 active 股票的原始数据并组装成 CalculationContext。"""
    base_rows = await database.list_dashboard_rows()

    if not base_rows:
        return []

    codes = [r["code"] for r in base_rows]

    profit_rows = await database.list_quarterly_profits_by_codes(codes)

    last_year_map: dict[str, int | None] = {r["code"]: r["last_year"] for r in base_rows}

    last_year_q: dict[str, dict[int, Decimal]] = {c: {} for c in codes}
    this_year_q: dict[str, dict[int, Decimal]] = {c: {} for c in codes}

    for r in profit_rows:
        code = r["code"]
        ly = last_year_map.get(code)

        if ly is None:
            continue

        if r["year"] == ly:
            last_year_q[code][r["quarter"]] = _to_decimal(r["net_profit"])  # type: ignore[assignment]
        elif r["year"] == ly + 1:
            this_year_q[code][r["quarter"]] = _to_decimal(r["net_profit"])  # type: ignore[assignment]

    contexts: list[CalculationContext] = []
    for r in base_rows:
        code = r["code"]
        contexts.append(
            CalculationContext(
                code=code,
                name=r["name"],
                industry=r.get("industry"),
                price=_to_decimal(r.get("price")),
                price_date=r.get("price_date"),
                price_sync_date=r.get("price_sync_date"),
                fundamental_sync_date=r.get("fundamental_sync_date"),
                sync_date=r.get("sync_date"),
                current_market_cap=_to_decimal(r.get("current_market_cap")),
                last_year_end_market_cap=_to_decimal(r.get("last_year_end_market_cap")),
                last_year_end_price=_to_decimal(r.get("last_year_end_price")),
                last_year_end_date=r.get("last_year_end_date"),
                last_year=r.get("last_year"),
                last_year_dividend=_to_decimal(r.get("last_year_dividend")),
                last_year_net_profit=_to_decimal(r.get("last_year_net_profit")),
                payout_ratio=_to_decimal(r.get("payout_ratio")),
                last_year_quarter_profits=last_year_q.get(code, {}),
                this_year_quarter_profits=this_year_q.get(code, {}),
                override_this_year_profit=_to_decimal(r.get("override_this_year_profit")),
                note=r.get("note"),
            )
        )

    return contexts


def context_to_row(ctx: CalculationContext) -> dict:
    """把 CalculationContext 转成前端表格行需要的字段。"""
    # 去年股息率口径固定：去年每股分红 / 去年年末价格
    last_yield = calc_yield(ctx.last_year_dividend, ctx.last_year_end_price)
    estimated_profit = estimate_this_year_profit(ctx)
    estimated_div = estimate_this_year_dividend_per_share(ctx)
    estimated_yield = calc_yield(estimated_div, ctx.price)

    return {
        "code": ctx.code,
        "name": ctx.name,
        "industry": ctx.industry,
        "price": ctx.price,
        "price_date": ctx.price_date,
        "price_sync_date": ctx.price_sync_date,
        "fundamental_sync_date": ctx.fundamental_sync_date,
        "sync_date": ctx.sync_date,
        "current_market_cap": ctx.current_market_cap,
        "last_year_end_market_cap": ctx.last_year_end_market_cap,
        "last_year_end_price": ctx.last_year_end_price,
        "last_year_end_date": ctx.last_year_end_date,
        "last_year": ctx.last_year,
        "last_year_dividend": ctx.last_year_dividend,
        "last_year_dividend_yield": last_yield,
        "last_year_net_profit": ctx.last_year_net_profit,
        "payout_ratio": ctx.payout_ratio,
        "this_year_estimated_profit": estimated_profit,
        "this_year_estimated_dividend": estimated_div,
        "this_year_estimated_yield": estimated_yield,
        "note": ctx.note,
    }


def sort_rows_desc_by_estimated_yield(rows: Iterable[dict]) -> list[dict]:
    """按今年预估股息率降序排序，None 排到最后。"""
    def key(r: dict):
        v = r.get("this_year_estimated_yield")
        if v is None:
            return (1, Decimal("0"))
        return (0, -Decimal(str(v)))

    return sorted(rows, key=key)


def compute_industry_yield_means(contexts: list[CalculationContext]) -> dict[str, Decimal]:
    """计算每个行业的去年股息率均值（同口径：去年每股分红 / 去年年末价）。

    - 剔除：缺价格 / 价格非正 / 缺分红 / 分红非正 的样本
    - 行业为空字符串或 None 时，归入 "未分类"
    - 返回 {industry: mean_yield_decimal}，例如 {"白酒": Decimal("0.032")}
    """
    by_industry: dict[str, list[Decimal]] = {}

    for ctx in contexts:
        industry = (ctx.industry or "").strip() or "未分类"
        if (
            ctx.last_year_end_price is None
            or ctx.last_year_end_price <= 0
            or ctx.last_year_dividend is None
            or ctx.last_year_dividend <= 0
        ):
            continue

        ratio = ctx.last_year_dividend / ctx.last_year_end_price
        by_industry.setdefault(industry, []).append(ratio)

    return {
        industry: sum(ratios, Decimal("0")) / Decimal(len(ratios))
        for industry, ratios in by_industry.items()
        if ratios
    }


def industry_key(industry: str | None) -> str:
    """统一行业归一化逻辑（与 compute_industry_yield_means 保持一致）。"""
    return (industry or "").strip() or "未分类"
