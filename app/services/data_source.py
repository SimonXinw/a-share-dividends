"""数据源服务：使用 AKShare 从东方财富 / 新浪 / 腾讯等免费接口抓取 A 股数据。

AKShare 是开源的金融数据库，封装了多个免费数据源，无需注册账号。
官网：https://akshare.akfamily.xyz/

关键接口：
- ak.stock_zh_a_spot_em()                              全市场 A 股实时行情（含名称/最新价/行业）
- ak.stock_fhps_detail_em(symbol="600519")             单只股票分红送配明细（年度分红）
- ak.stock_profit_sheet_by_report_em(symbol="SH600519") 单只股票季度利润表
- ak.stock_individual_info_em(symbol="600519")         单只股票基本资料

注意：akshare 是同步阻塞调用，且某些接口偶尔会失败/返回空，
我们使用 asyncio.to_thread 包一层并加 try/except，失败的股票跳过。
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal, InvalidOperation
from typing import Optional

from .. import database

logger = logging.getLogger(__name__)


# ============================================================================
# 工具
# ============================================================================
def _to_decimal(v) -> Optional[Decimal]:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "-", "--", "nan", "NaN", "None"):
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _market_prefix(code: str) -> str:
    """根据股票代码返回 SH/SZ/BJ 前缀。"""
    if code.startswith(("60", "68", "9")):
        return "SH"
    if code.startswith(("00", "30", "20")):
        return "SZ"
    if code.startswith(("8", "4")):
        return "BJ"
    return "SH"


def _akshare_symbol(code: str) -> str:
    """akshare 部分接口需要 SH600519 / SZ000001 这种带前缀格式。"""
    return f"{_market_prefix(code)}{code}"


# ============================================================================
# 价格同步
# ============================================================================
async def sync_prices(codes: list[str] | None = None) -> int:
    """同步当前股价。一次性拉取全市场行情后筛出我们关心的股票，效率最高。"""
    import akshare as ak  # 延迟引入，避免在没装 akshare 的开发环境启动时报错
    import pandas as pd  # noqa: F401

    logger.info("开始同步股价...")

    df = await asyncio.to_thread(ak.stock_zh_a_spot_em)

    if df is None or df.empty:
        logger.warning("akshare 返回空行情数据")
        return 0

    code_col = "代码"
    name_col = "名称"
    price_col = "最新价"

    # 如果传了 codes 就过滤，否则同步所有数据库里 active 的股票
    if codes is None:
        rows = await database.fetch_all("select code from public.a_share_stocks where is_active = true")
        codes = [r["code"] for r in rows]

    code_set = set(codes)

    affected = 0

    async with database.acquire() as conn:
        async with conn.transaction():
            for _, r in df.iterrows():
                code = str(r[code_col]).zfill(6)
                if code not in code_set:
                    continue
                price = _to_decimal(r.get(price_col))
                name = str(r.get(name_col, "")).strip()

                if price is None:
                    continue

                if name:
                    await conn.execute(
                        "update public.a_share_stocks set name = $1 where code = $2 and (name is null or name = '' or name <> $1)",
                        name, code,
                    )

                await conn.execute(
                    """
                    insert into public.a_share_prices (code, price, price_date, updated_at)
                    values ($1, $2, current_date, now())
                    on conflict (code) do update
                       set price = excluded.price,
                           price_date = excluded.price_date,
                           updated_at = now()
                    """,
                    code, price,
                )
                affected += 1

    logger.info("股价同步完成，共 %d 条", affected)
    return affected


# ============================================================================
# 分红同步
# ============================================================================
async def _fetch_dividends_one(code: str) -> list[dict]:
    """抓取单只股票各年度分红总额（每股口径）。

    返回结构：[{"year": 2024, "dividend_per_share": Decimal("3.50")}, ...]
    """
    import akshare as ak

    try:
        df = await asyncio.to_thread(ak.stock_fhps_detail_em, symbol=code)
    except Exception as e:  # noqa: BLE001
        logger.warning("[%s] 拉取分红明细失败：%s", code, e)
        return []

    if df is None or df.empty:
        return []

    # akshare 列名（取决于版本）：
    #   "报告期" 或 "公告日期"
    #   "现金分红-现金分红比例" 或 "现金分红比例"  → 已经是「每股派现 × 10 股」 ?
    # 这里我们采用更稳的列：
    #   "送转股份-送转总比例" 与 "现金分红-现金分红比例" 都是按"每 10 股"
    # 因此每股分红 = 现金分红比例 / 10
    # 2024 年新版 akshare 列名：'现金分红-现金分红比例','现金分红-股息率'
    # 兼容多版本：
    period_col = None
    for c in ("报告期", "公告日期", "除权除息日", "Ex-dividend Date", "ex_dividend_date"):
        if c in df.columns:
            period_col = c
            break

    cash_col = None
    for c in (
        "现金分红-现金分红比例",
        "现金分红比例",
        "派息(税前)(每10股)",
        "派息(税前)(元/10股)",
    ):
        if c in df.columns:
            cash_col = c
            break

    if not period_col or not cash_col:
        logger.warning("[%s] 分红表缺少必要列，列名=%s", code, list(df.columns))
        return []

    by_year: dict[int, Decimal] = {}
    for _, row in df.iterrows():
        period = str(row[period_col])
        if len(period) < 4:
            continue
        try:
            year = int(period[:4])
        except ValueError:
            continue

        per10 = _to_decimal(row[cash_col])
        if per10 is None:
            continue

        per_share = per10 / Decimal("10")
        by_year[year] = by_year.get(year, Decimal("0")) + per_share

    return [{"year": y, "dividend_per_share": v} for y, v in sorted(by_year.items())]


async def _fetch_quarterly_profits_one(code: str) -> list[dict]:
    """抓取单只股票各年度各季度净利润。

    返回 [{"year": 2024, "quarter": 1, "net_profit": Decimal(...)}, ...]
    """
    import akshare as ak

    try:
        symbol = _akshare_symbol(code)
        df = await asyncio.to_thread(ak.stock_profit_sheet_by_report_em, symbol=symbol)
    except Exception as e:  # noqa: BLE001
        logger.warning("[%s] 拉取利润表失败：%s", code, e)
        return []

    if df is None or df.empty:
        return []

    # 关键列：报告期、归属于母公司股东的净利润
    period_col = None
    for c in ("REPORT_DATE", "报告期", "REPORT_DATE_NAME"):
        if c in df.columns:
            period_col = c
            break

    profit_col = None
    for c in (
        "PARENT_NETPROFIT",
        "归属于母公司股东的净利润",
        "归母净利润",
        "NETPROFIT",
    ):
        if c in df.columns:
            profit_col = c
            break

    if not period_col or not profit_col:
        logger.warning("[%s] 利润表缺少必要列，列名=%s", code, list(df.columns))
        return []

    # 这些数据是「累计值」（YTD）：Q1 / 半年 / 前三季 / 全年
    # 单季利润 = 当期累计 - 上期累计
    accumulated: dict[tuple[int, int], Decimal] = {}
    for _, row in df.iterrows():
        period = str(row[period_col])[:10]  # YYYY-MM-DD
        if len(period) < 10:
            continue
        try:
            year = int(period[:4])
            month = int(period[5:7])
        except ValueError:
            continue
        quarter = {3: 1, 6: 2, 9: 3, 12: 4}.get(month)
        if not quarter:
            continue
        v = _to_decimal(row[profit_col])
        if v is None:
            continue
        accumulated[(year, quarter)] = v

    result: list[dict] = []
    for (year, quarter), cum in sorted(accumulated.items()):
        if quarter == 1:
            single = cum
        else:
            prev = accumulated.get((year, quarter - 1))
            if prev is None:
                continue  # 没有上一季的累计，无法算单季
            single = cum - prev
        result.append({"year": year, "quarter": quarter, "net_profit": single})

    return result


async def sync_dividends_and_profits(codes: list[str] | None = None, concurrency: int = 5) -> int:
    """同步分红 + 季度利润。"""
    if codes is None:
        rows = await database.fetch_all("select code from public.a_share_stocks where is_active = true")
        codes = [r["code"] for r in rows]

    logger.info("开始同步分红/利润，共 %d 只股票", len(codes))

    sem = asyncio.Semaphore(concurrency)
    affected = 0
    lock = asyncio.Lock()

    async def handle(code: str) -> None:
        nonlocal affected
        async with sem:
            divs = await _fetch_dividends_one(code)
            profits = await _fetch_quarterly_profits_one(code)

        async with lock:
            async with database.acquire() as conn:
                async with conn.transaction():
                    # 写入年度分红
                    for d in divs:
                        # 如果同年有利润数据，可以推算 net_profit / payout_ratio
                        year = d["year"]
                        # 当年净利润 = 4 个季度之和（如果都有）
                        full_year_profit = None
                        q_in_year = [p for p in profits if p["year"] == year]
                        if len(q_in_year) == 4:
                            full_year_profit = sum((p["net_profit"] for p in q_in_year), Decimal("0"))

                        payout_ratio = None
                        if full_year_profit and full_year_profit != 0:
                            # 假设总股本未知，无法得到分红总额；这里用 payout_ratio 字段保存
                            # 「每股分红 / 每股净利润」也成立，因股本相同，约掉。
                            # 取近似：payout_ratio ~ dividend_per_share / (full_year_profit / shares)
                            # 由于没股本，留空，前端不强依赖此字段。
                            payout_ratio = None

                        await conn.execute(
                            """
                            insert into public.a_share_dividends
                                (code, year, dividend_per_share, net_profit, payout_ratio, source, updated_at)
                            values ($1, $2, $3, $4, $5, 'akshare', now())
                            on conflict (code, year) do update set
                                dividend_per_share = excluded.dividend_per_share,
                                net_profit = coalesce(excluded.net_profit, public.a_share_dividends.net_profit),
                                payout_ratio = coalesce(excluded.payout_ratio, public.a_share_dividends.payout_ratio),
                                source = 'akshare',
                                updated_at = now()
                            """,
                            code, year, d["dividend_per_share"], full_year_profit, payout_ratio,
                        )

                    # 单独再写一遍年度利润（即使无分红记录）
                    by_year: dict[int, Decimal] = {}
                    by_year_count: dict[int, int] = {}
                    for p in profits:
                        by_year[p["year"]] = by_year.get(p["year"], Decimal("0")) + p["net_profit"]
                        by_year_count[p["year"]] = by_year_count.get(p["year"], 0) + 1
                    for y, v in by_year.items():
                        if by_year_count.get(y) == 4:
                            await conn.execute(
                                """
                                insert into public.a_share_dividends
                                    (code, year, dividend_per_share, net_profit, source, updated_at)
                                values ($1, $2, 0, $3, 'akshare', now())
                                on conflict (code, year) do update set
                                    net_profit = excluded.net_profit,
                                    updated_at = now()
                                """,
                                code, y, v,
                            )

                    # 写入季度利润
                    for p in profits:
                        await conn.execute(
                            """
                            insert into public.a_share_quarterly_profits
                                (code, year, quarter, net_profit, is_published, source, updated_at)
                            values ($1, $2, $3, $4, true, 'akshare', now())
                            on conflict (code, year, quarter) do update set
                                net_profit = excluded.net_profit,
                                is_published = true,
                                source = 'akshare',
                                updated_at = now()
                            """,
                            code, p["year"], p["quarter"], p["net_profit"],
                        )

                    affected += 1
                    logger.info("[%s] 已同步：分红 %d 条，季度利润 %d 条", code, len(divs), len(profits))

    await asyncio.gather(*(handle(c) for c in codes), return_exceptions=False)

    logger.info("分红/利润同步完成，共 %d 只股票", affected)
    return affected


# ============================================================================
# 综合任务
# ============================================================================
async def sync_all(codes: list[str] | None = None, concurrency: int = 5) -> dict:
    price_count = await sync_prices(codes)
    fundamental_count = await sync_dividends_and_profits(codes, concurrency=concurrency)

    return {"price_count": price_count, "fundamental_count": fundamental_count}
