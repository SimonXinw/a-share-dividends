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
import random
from datetime import datetime, timezone, date
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable, Optional

from .. import database

logger = logging.getLogger(__name__)
ProgressCallback = Callable[[dict], Awaitable[None]]


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
async def sync_prices(codes: list[str] | None = None, progress_cb: ProgressCallback | None = None) -> int:
    """同步当前股价（逐股票拉取日线最新收盘价）。"""

    logger.info("开始同步股价...")

    # 如果传了 codes 就过滤，否则同步所有数据库里 active 的股票
    if codes is None:
        today = datetime.now(timezone.utc).date()
        codes = await database.list_price_sync_candidates(today)
        logger.info("价格同步待处理 %d 只（已跳过今日已同步且价格/市值完整的股票）", len(codes))

    return await _sync_prices_fallback_by_daily(codes, progress_cb=progress_cb)


async def _sync_prices_fallback_by_daily(
    codes: list[str],
    progress_cb: ProgressCallback | None = None,
) -> int:
    """降级路径：按股票逐个拉日线，取最新收盘价作为当前价。"""
    import akshare as ak
    import pandas as pd

    # py_mini_racer 在 Windows + Python 3.13 下并发调用存在稳定性问题，
    # 这里降为串行，优先保证任务可完成。
    sem = asyncio.Semaphore(1)
    total = len(codes)
    processed = 0
    affected = 0
    failed = 0
    lock = asyncio.Lock()

    async def handle(code: str) -> None:
        nonlocal affected, failed, processed
        symbol = f"{_market_prefix(code).lower()}{code}"
        success = False

        df = None
        for attempt in range(1, 4):
            async with sem:
                try:
                    df = await asyncio.to_thread(ak.stock_zh_a_daily, symbol=symbol)
                    break
                except Exception as e:  # noqa: BLE001
                    if attempt >= 3:
                        logger.warning("[%s] 降级拉取日线失败（第 %d/3 次）：%s", code, attempt, e)
                        async with lock:
                            processed += 1
                            failed += 1
                            if progress_cb is not None:
                                await progress_cb(
                                    {
                                        "stage": "price",
                                        "processed": processed,
                                        "total": total,
                                        "success": affected,
                                        "failed": failed,
                                        "code": code,
                                    }
                                )
                        return

                    wait_seconds = random.randint(1, 3)
                    logger.warning(
                        "[%s] 降级拉取日线失败（第 %d/3 次），%d 秒后重试：%s",
                        code,
                        attempt,
                        wait_seconds,
                        e,
                    )

            await asyncio.sleep(wait_seconds)

        if df is None or df.empty:
            async with lock:
                processed += 1
                failed += 1
                if progress_cb is not None:
                    await progress_cb(
                        {
                            "stage": "price",
                            "processed": processed,
                            "total": total,
                            "success": affected,
                            "failed": failed,
                            "code": code,
                        }
                    )
            return

        last_row = df.iloc[-1]
        price = _to_decimal(last_row.get("close"))
        if price is None:
            async with lock:
                processed += 1
                failed += 1
                if progress_cb is not None:
                    await progress_cb(
                        {
                            "stage": "price",
                            "processed": processed,
                            "total": total,
                            "success": affected,
                            "failed": failed,
                            "code": code,
                        }
                    )
            return

        latest_share_count = _to_decimal(last_row.get("outstanding_share"))
        current_market_cap = None
        if latest_share_count is not None:
            current_market_cap = price * latest_share_count

        last_year = datetime.now(timezone.utc).year - 1
        cutoff = date(last_year, 12, 31)
        year_rows = None
        if "date" in df.columns:
            parsed_dates = pd.to_datetime(df["date"], errors="coerce")
            year_rows = df[parsed_dates.dt.date <= cutoff]

        last_year_end_price = None
        last_year_end_market_cap = None
        last_year_end_date = None
        if year_rows is not None and not year_rows.empty:
            year_last_row = year_rows.iloc[-1]
            year_last_close = _to_decimal(year_last_row.get("close"))
            year_last_share = _to_decimal(year_last_row.get("outstanding_share"))
            if year_last_close is not None:
                last_year_end_price = year_last_close
            if year_last_close is not None and year_last_share is not None:
                last_year_end_market_cap = year_last_close * year_last_share
            year_last_date = year_last_row.get("date")
            if year_last_date is not None:
                last_year_end_date = str(year_last_date)[:10]

        await database.upsert_price_with_market_values(
            code=code,
            price=price,
            current_market_cap=current_market_cap,
            last_year_end_price=last_year_end_price,
            last_year_end_market_cap=last_year_end_market_cap,
            last_year_end_date=last_year_end_date,
        )
        success = True
        async with lock:
            processed += 1
            affected += 1
            if not success:
                failed += 1
            if progress_cb is not None:
                await progress_cb(
                    {
                        "stage": "price",
                        "processed": processed,
                        "total": total,
                        "success": affected,
                        "failed": failed,
                        "code": code,
                    }
                )

    await asyncio.gather(*(handle(code) for code in codes), return_exceptions=False)
    logger.info("股价同步（降级）完成，共 %d 条", affected)
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


async def sync_dividends_and_profits(
    codes: list[str] | None = None,
    concurrency: int = 5,
    progress_cb: ProgressCallback | None = None,
) -> int:
    """同步分红 + 季度利润。"""
    if codes is None:
        today = datetime.now(timezone.utc).date()
        codes = await database.list_fundamental_sync_candidates(today)
        logger.info("分红/利润待处理 %d 只（已跳过今日已同步且有基本面的股票）", len(codes))

    safe_concurrency = max(1, min(concurrency, 5))
    if safe_concurrency != concurrency:
        logger.info("分红/利润并发从 %d 调整为 %d（稳定性保护）", concurrency, safe_concurrency)

    logger.info("开始同步分红/利润，共 %d 只股票", len(codes))

    sem = asyncio.Semaphore(safe_concurrency)
    total = len(codes)
    processed = 0
    affected = 0
    failed = 0
    lock = asyncio.Lock()

    async def handle(code: str) -> None:
        nonlocal affected, failed, processed
        async with sem:
            divs = await _fetch_dividends_one(code)
            profits = await _fetch_quarterly_profits_one(code)

        async with lock:
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

                await database.upsert_dividend_row(
                    code=code,
                    year=year,
                    dividend_per_share=d["dividend_per_share"],
                    net_profit=full_year_profit,
                    payout_ratio=payout_ratio,
                )

            # 单独再写一遍年度利润（即使无分红记录）
            by_year: dict[int, Decimal] = {}
            by_year_count: dict[int, int] = {}
            for p in profits:
                by_year[p["year"]] = by_year.get(p["year"], Decimal("0")) + p["net_profit"]
                by_year_count[p["year"]] = by_year_count.get(p["year"], 0) + 1
            for y, v in by_year.items():
                if by_year_count.get(y) == 4:
                    await database.upsert_dividend_profit_only(code, y, v)

            # 写入季度利润
            for p in profits:
                await database.upsert_quarterly_profit(
                    code=code,
                    year=p["year"],
                    quarter=p["quarter"],
                    net_profit=p["net_profit"],
                )

            await database.mark_fundamental_synced(code)
            processed += 1
            affected += 1
            # fundamentals 允许部分数据为空，只要流程完成即视为成功
            if progress_cb is not None:
                await progress_cb(
                    {
                        "stage": "fundamental",
                        "processed": processed,
                        "total": total,
                        "success": affected,
                        "failed": failed,
                        "code": code,
                    }
                )
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
