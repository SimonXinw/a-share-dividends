"""ETF 数据源服务：通用抓取层（可复用到任意 ETF 代码）。"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Awaitable, Callable

from .. import database

logger = logging.getLogger(__name__)
ProgressCallback = Callable[[dict], Awaitable[None]]


def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None

    text = str(value).strip()
    if text in {"", "-", "--", "nan", "NaN", "None"}:
        return None

    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _pick_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


class AkshareEtfClient:
    """AKShare ETF 数据抓取客户端。"""

    async def fetch_spot_rows(self) -> list[dict]:
        import akshare as ak

        df = await asyncio.to_thread(ak.fund_etf_spot_em)
        if df is None or df.empty:
            return []
        return df.to_dict("records")

    async def fetch_daily_history_rows(self, code: str) -> list[dict]:
        import akshare as ak

        # 主通道：ETF 历史接口
        try:
            df = await asyncio.to_thread(
                ak.fund_etf_hist_em,
                symbol=code,
                period="daily",
                adjust="qfq",
            )
            if df is not None and not df.empty:
                return df.to_dict("records")
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] fund_etf_hist_em 抓取失败，准备走备用接口: %s", code, exc)

        # 备用通道 1：新浪 ETF 历史（AkShare 封装）
        market_prefixes = ["sh", "sz"]
        preferred_prefix = "sh" if code.startswith(("5", "6", "9")) else "sz"
        market_prefixes.sort(key=lambda prefix: 0 if prefix == preferred_prefix else 1)
        for prefix in market_prefixes:
            try:
                symbol = f"{prefix}{code}"
                df_sina = await asyncio.to_thread(ak.fund_etf_hist_sina, symbol=symbol)
                if df_sina is not None and not df_sina.empty:
                    return df_sina.to_dict("records")
            except Exception as exc:  # noqa: BLE001
                logger.warning("[%s] fund_etf_hist_sina(%s) 失败，继续尝试其他备用接口: %s", code, prefix, exc)

        # 备用通道 2：A 股历史接口（ETF 代码同样可用）
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        df_fallback = await asyncio.to_thread(
            ak.stock_zh_a_hist,
            symbol=code,
            period="daily",
            start_date="19900101",
            end_date=today,
            adjust="qfq",
        )
        if df_fallback is None or df_fallback.empty:
            return []
        return df_fallback.to_dict("records")


def _guess_market_by_code(code: str) -> str:
    if code.startswith(("0", "1", "2", "3")):
        return "SZ"
    return "SH"


async def resolve_etf_identity(code: str) -> dict:
    """按代码解析 ETF 基础信息（优先中文名称）。"""
    normalized_code = (code or "").strip()
    if not normalized_code:
        return {"code": "", "name": None, "market": "SH"}

    client = AkshareEtfClient()
    resolved_name: str | None = None

    try:
        spot_rows = await client.fetch_spot_rows()
        spot_map = _normalize_spot_rows(spot_rows)
        spot_item = spot_map.get(normalized_code) or {}
        name = str(spot_item.get("name") or "").strip()
        if name:
            resolved_name = name
    except Exception as exc:  # noqa: BLE001
        logger.warning("[%s] ETF 名称解析（spot）失败: %s", normalized_code, exc)

    if not resolved_name:
        try:
            import akshare as ak

            df = await asyncio.to_thread(ak.fund_name_em)
            if df is not None and not df.empty:
                columns = list(df.columns)
                code_col = _pick_column(columns, ("基金代码", "代码", "symbol"))
                name_col = _pick_column(columns, ("基金简称", "基金名称", "名称", "name"))
                if code_col and name_col:
                    matches = df[df[code_col].astype(str).str.strip() == normalized_code]
                    if not matches.empty:
                        raw_name = str(matches.iloc[0][name_col]).strip()
                        if raw_name and raw_name not in {"-", "--", "None", "nan", "NaN"}:
                            resolved_name = raw_name
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] ETF 名称解析（fund_name_em）失败: %s", normalized_code, exc)

    return {
        "code": normalized_code,
        "name": resolved_name,
        "market": _guess_market_by_code(normalized_code),
    }


async def _fetch_history_rows_with_retry(
    client: AkshareEtfClient,
    code: str,
    max_attempts: int = 6,
) -> list[dict]:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await client.fetch_daily_history_rows(code)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_attempts:
                break
            sleep_seconds = min(12.0, attempt * 1.5 + random.uniform(0.2, 1.0))
            logger.warning("[%s] ETF 历史抓取失败，第 %d/%d 次重试：%s", code, attempt, max_attempts, exc)
            await asyncio.sleep(sleep_seconds)

    raise RuntimeError(f"历史抓取重试耗尽: {last_error}") from last_error


def _normalize_spot_rows(rows: list[dict]) -> dict[str, dict]:
    if not rows:
        return {}

    columns = list(rows[0].keys())
    code_col = _pick_column(columns, ("代码", "基金代码", "symbol", "代码代码"))
    name_col = _pick_column(columns, ("名称", "基金简称", "name"))
    price_col = _pick_column(columns, ("最新价", "最新", "price", "最新行情"))
    if not code_col or not price_col:
        return {}

    normalized: dict[str, dict] = {}
    for row in rows:
        code = str(row.get(code_col, "")).strip()
        if not code:
            continue
        normalized[code] = {
            "code": code,
            "name": str(row.get(name_col, "")).strip() if name_col else "",
            "latest_price": _to_decimal(row.get(price_col)),
        }
    return normalized


def _normalize_history_rows(rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    columns = list(rows[0].keys())
    date_col = _pick_column(columns, ("日期", "净值日期", "trade_date", "date"))
    open_col = _pick_column(columns, ("开盘", "open", "open_price"))
    high_col = _pick_column(columns, ("最高", "high", "high_price"))
    low_col = _pick_column(columns, ("最低", "low", "low_price"))
    close_col = _pick_column(columns, ("收盘", "close", "close_price", "单位净值"))
    volume_col = _pick_column(columns, ("成交量", "volume"))
    amount_col = _pick_column(columns, ("成交额", "amount"))
    if not date_col or not close_col:
        return []

    normalized: list[dict] = []
    for row in rows:
        trade_date = str(row.get(date_col, "")).strip()[:10]
        close_price = _to_decimal(row.get(close_col))
        if not trade_date or close_price is None:
            continue

        normalized.append(
            {
                "trade_date": trade_date,
                "open_price": _to_decimal(row.get(open_col)) if open_col else None,
                "high_price": _to_decimal(row.get(high_col)) if high_col else None,
                "low_price": _to_decimal(row.get(low_col)) if low_col else None,
                "close_price": close_price,
                "volume": _to_decimal(row.get(volume_col)) if volume_col else None,
                "amount": _to_decimal(row.get(amount_col)) if amount_col else None,
            }
        )
    normalized.sort(key=lambda item: item["trade_date"])
    return normalized


async def sync_etf_prices(
    codes: list[str] | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    """同步 ETF 最新价。"""
    client = AkshareEtfClient()

    if codes is None:
        today = datetime.now(timezone.utc).date()
        codes = await database.list_etf_price_sync_candidates(today)
        logger.info("ETF 价格待处理 %d 只（今日已同步会跳过）", len(codes))

    target_codes = [code.strip() for code in (codes or []) if code and code.strip()]
    total = len(target_codes)
    if total == 0:
        return {"stage": "etf_price", "total": 0, "processed": 0, "success": 0, "failed": 0, "affected": 0, "failed_codes": []}

    spot_rows = await client.fetch_spot_rows()
    if not spot_rows:
        logger.warning("ETF 行情为空，本次同步结束")
        return {
            "stage": "etf_price",
            "total": total,
            "processed": 0,
            "success": 0,
            "failed": total,
            "affected": 0,
            "failed_codes": target_codes,
        }

    spot_map = _normalize_spot_rows(spot_rows)
    if not spot_map:
        logger.warning("ETF 行情缺少必要列，无法解析")
        return {
            "stage": "etf_price",
            "total": total,
            "processed": 0,
            "success": 0,
            "failed": total,
            "affected": 0,
            "failed_codes": target_codes,
        }

    affected = 0
    failed = 0
    processed = 0
    failed_codes: list[str] = []
    for code in target_codes:
        item = spot_map.get(code)
        if item is None:
            failed += 1
            processed += 1
            failed_codes.append(code)
            if progress_cb is not None:
                await progress_cb(
                    {
                        "stage": "etf_price",
                        "processed": processed,
                        "total": total,
                        "success": affected,
                        "failed": failed,
                        "code": code,
                    }
                )
            continue

        price = item.get("latest_price")
        if price is None:
            failed += 1
            processed += 1
            failed_codes.append(code)
            if progress_cb is not None:
                await progress_cb(
                    {
                        "stage": "etf_price",
                        "processed": processed,
                        "total": total,
                        "success": affected,
                        "failed": failed,
                        "code": code,
                    }
                )
            continue

        await database.upsert_etf_price(code=code, price=price)
        await database.mark_etf_price_synced(code)

        if item.get("name"):
            await database.update_etf_name_if_needed(code, item["name"])

        affected += 1
        processed += 1
        if progress_cb is not None:
            await progress_cb(
                {
                    "stage": "etf_price",
                    "processed": processed,
                    "total": total,
                    "success": affected,
                    "failed": failed,
                    "code": code,
                }
            )

    logger.info("ETF 价格同步完成：成功 %d，失败 %d", affected, failed)
    return {
        "stage": "etf_price",
        "total": total,
        "processed": processed,
        "success": affected,
        "failed": failed,
        "affected": affected,
        "failed_codes": failed_codes,
    }


async def sync_etf_histories(
    codes: list[str] | None = None,
    progress_cb: ProgressCallback | None = None,
) -> dict:
    """同步 ETF 日线历史。"""
    client = AkshareEtfClient()
    target_codes = [code.strip() for code in (codes or []) if code and code.strip()]
    if not target_codes:
        target_codes = await database.list_etf_active_codes()

    total = len(target_codes)
    if total == 0:
        return {"stage": "etf_history", "total": 0, "processed": 0, "success": 0, "failed": 0, "affected": 0, "failed_codes": []}

    processed = 0
    affected = 0
    failed = 0
    written_rows = 0
    failed_codes: list[str] = []
    for code in target_codes:
        try:
            raw_rows = await _fetch_history_rows_with_retry(client, code)
            rows = _normalize_history_rows(raw_rows)
            if not rows:
                raise ValueError("历史数据为空")

            latest_trade_date = await database.get_etf_history_latest_trade_date(code)
            if latest_trade_date:
                rows = [row for row in rows if row["trade_date"] > latest_trade_date]

            for row in rows:
                await database.upsert_etf_price_history_row(
                    code=code,
                    trade_date=row["trade_date"],
                    open_price=row["open_price"],
                    high_price=row["high_price"],
                    low_price=row["low_price"],
                    close_price=row["close_price"],
                    volume=row["volume"],
                    amount=row["amount"],
                )
            written_rows += len(rows)
            await database.mark_etf_history_synced(code)
            affected += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning("[%s] ETF 历史同步失败：%s", code, exc)
            failed += 1
            failed_codes.append(code)

        processed += 1
        if progress_cb is not None:
            await progress_cb(
                {
                    "stage": "etf_history",
                    "processed": processed,
                    "total": total,
                    "success": affected,
                    "failed": failed,
                    "code": code,
                }
            )

    logger.info("ETF 历史同步完成：成功 %d，失败 %d", affected, failed)
    return {
        "stage": "etf_history",
        "total": total,
        "processed": processed,
        "success": affected,
        "failed": failed,
        "affected": affected,
        "written_rows": written_rows,
        "failed_codes": failed_codes,
    }
