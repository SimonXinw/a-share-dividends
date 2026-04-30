"""数据库访问层：基于 Supabase HTTP API（PostgREST）。"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from supabase import Client, create_client

from .config import settings

logger = logging.getLogger(__name__)

_client: Client | None = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _run_supabase(callable_obj):
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            return await asyncio.to_thread(callable_obj)
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            is_transient = any(
                key in message
                for key in (
                    "UNEXPECTED_EOF_WHILE_READING",
                    "ConnectError",
                    "ReadError",
                    "Timeout",
                    "Connection reset",
                )
            )

            if (not is_transient) or attempt >= max_attempts:
                raise

            await asyncio.sleep(0.6 * attempt)


def _ensure_client() -> Client:
    if _client is None:
        raise RuntimeError("Supabase 客户端尚未初始化")
    return _client


async def init_pool() -> Client:
    global _client

    if _client is not None:
        return _client

    if not settings.supabase_url:
        raise RuntimeError("SUPABASE_URL 未配置，无法使用 Supabase HTTPS 数据访问")

    key = settings.supabase_service_role_key or settings.supabase_anon_key
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY / SUPABASE_ANON_KEY 至少配置一个")

    logger.info("正在初始化 Supabase HTTP 客户端...")
    _client = await _run_supabase(lambda: create_client(settings.supabase_url, key))
    logger.info("Supabase HTTP 客户端已就绪")
    return _client


async def close_pool() -> None:
    global _client

    _client = None


async def list_active_codes() -> list[str]:
    client = _ensure_client()

    response = await _run_supabase(
        lambda: client.table("a_share_stocks").select("code").eq("is_active", True).execute()
    )
    return [row["code"] for row in (response.data or [])]


async def list_dashboard_rows() -> list[dict]:
    client = _ensure_client()
    select_candidates = [
        (
            "code,name,industry,price,last_year,last_year_dividend,last_year_net_profit,"
            "payout_ratio,override_this_year_profit,note,price_date,price_sync_date,"
            "fundamental_sync_date,sync_date,current_market_cap,last_year_end_market_cap,"
            "last_year_end_price,last_year_end_date"
        ),
        (
            "code,name,industry,price,last_year,last_year_dividend,last_year_net_profit,"
            "payout_ratio,override_this_year_profit,note,price_date,price_sync_date,"
            "fundamental_sync_date,sync_date"
        ),
        (
            "code,name,industry,price,last_year,last_year_dividend,last_year_net_profit,"
            "payout_ratio,override_this_year_profit,note"
        ),
    ]

    for select_clause in select_candidates:
        try:
            response = await _run_supabase(
                lambda: client.table("a_share_dashboard_view")
                .select(select_clause)
                .order("code")
                .execute()
            )
            return response.data or []
        except Exception:
            continue

    raise RuntimeError("a_share_dashboard_view 查询失败，请检查视图定义和迁移脚本")


def _same_day(value: str | None, target: date) -> bool:
    if not value:
        return False
    try:
        return datetime.fromisoformat(value).date() == target
    except ValueError:
        try:
            return date.fromisoformat(value) == target
        except ValueError:
            return False


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


async def list_price_sync_candidates(today: date) -> list[str]:
    rows = await list_dashboard_rows()
    candidates: list[str] = []
    for row in rows:
        price = row.get("price")
        current_market_cap = row.get("current_market_cap")
        last_year_end_market_cap = row.get("last_year_end_market_cap")
        last_year_end_price = row.get("last_year_end_price")
        price_sync_date = row.get("price_sync_date") or row.get("price_date")
        has_price_bundle = (
            price is not None
            and current_market_cap is not None
            and last_year_end_market_cap is not None
            and last_year_end_price is not None
        )
        if has_price_bundle and _same_day(price_sync_date, today):
            continue
        candidates.append(row["code"])
    return candidates


async def list_fundamental_sync_candidates(today: date) -> list[str]:
    rows = await list_dashboard_rows()
    candidates: list[str] = []
    for row in rows:
        dividend_value = _to_float(row.get("last_year_dividend"))
        net_profit_value = _to_float(row.get("last_year_net_profit"))
        # 分红为 0 或空值都视为“未就绪”，避免错误数据被跳过。
        has_fundamental = (
            dividend_value is not None
            and dividend_value > 0
            and net_profit_value is not None
            and net_profit_value != 0
        )
        if has_fundamental and _same_day(row.get("fundamental_sync_date"), today):
            continue
        candidates.append(row["code"])
    return candidates


async def list_quarterly_profits_by_codes(codes: list[str]) -> list[dict]:
    if not codes:
        return []

    client = _ensure_client()
    response = await _run_supabase(
        lambda: client.table("a_share_quarterly_profits")
        .select("code,year,quarter,net_profit")
        .in_("code", codes)
        .execute()
    )
    return response.data or []


async def upsert_stock(code: str, name: str, industry: str | None, market: str) -> None:
    client = _ensure_client()

    existing_resp = await _run_supabase(
        lambda: client.table("a_share_stocks").select("name,industry").eq("code", code).limit(1).execute()
    )
    existing = (existing_resp.data or [None])[0]

    payload = {
        "code": code,
        "name": name or (existing or {}).get("name") or code,
        "industry": industry if industry is not None else (existing or {}).get("industry"),
        "market": market,
        "is_active": True,
    }

    await _run_supabase(
        lambda: client.table("a_share_stocks").upsert(payload, on_conflict="code").execute()
    )


async def deactivate_stock(code: str) -> None:
    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_stocks").update({"is_active": False}).eq("code", code).execute()
    )


async def stock_exists_active(code: str) -> bool:
    client = _ensure_client()
    response = await _run_supabase(
        lambda: client.table("a_share_stocks")
        .select("code")
        .eq("code", code)
        .eq("is_active", True)
        .limit(1)
        .execute()
    )
    return bool(response.data)


async def upsert_override(code: str, payload: dict[str, Any]) -> None:
    client = _ensure_client()
    data = {
        "code": code,
        "price": payload.get("price"),
        "last_year_dividend": payload.get("last_year_dividend"),
        "last_year_net_profit": payload.get("last_year_net_profit"),
        "this_year_estimated_profit": payload.get("this_year_estimated_profit"),
        "note": payload.get("note"),
        "updated_at": _utc_now_iso(),
    }
    await _run_supabase(
        lambda: client.table("a_share_overrides").upsert(data, on_conflict="code").execute()
    )


async def delete_override(code: str) -> None:
    client = _ensure_client()
    await _run_supabase(lambda: client.table("a_share_overrides").delete().eq("code", code).execute())


async def create_sync_log(job_type: str, started_at: datetime) -> int | None:
    client = _ensure_client()
    response = await _run_supabase(
        lambda: client.table("a_share_sync_logs")
        .insert(
            {
                "job_type": job_type,
                "status": "running",
                "started_at": started_at.isoformat(),
            }
        )
        .execute()
    )
    if response.data:
        return response.data[0].get("id")
    return None


async def mark_sync_log_success(log_id: int | None, affected_rows: int) -> None:
    if log_id is None:
        return

    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_sync_logs")
        .update(
            {
                "status": "success",
                "affected_rows": affected_rows,
                "finished_at": _utc_now_iso(),
            }
        )
        .eq("id", log_id)
        .execute()
    )


async def mark_sync_log_failed(log_id: int | None, message: str) -> None:
    if log_id is None:
        return

    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_sync_logs")
        .update({"status": "failed", "message": message, "finished_at": _utc_now_iso()})
        .eq("id", log_id)
        .execute()
    )


async def update_sync_log_progress(log_id: int | None, message: str, affected_rows: int | None = None) -> None:
    if log_id is None:
        return

    client = _ensure_client()
    payload: dict[str, Any] = {"message": message}
    if affected_rows is not None:
        payload["affected_rows"] = affected_rows

    await _run_supabase(
        lambda: client.table("a_share_sync_logs")
        .update(payload)
        .eq("id", log_id)
        .execute()
    )


async def list_sync_logs(limit: int = 20) -> list[dict]:
    client = _ensure_client()
    response = await _run_supabase(
        lambda: client.table("a_share_sync_logs")
        .select("id,job_type,status,affected_rows,message,started_at,finished_at")
        .order("started_at", desc=True)
        .limit(limit)
        .execute()
    )
    return response.data or []


async def update_stock_name_if_needed(code: str, name: str) -> None:
    if not name:
        return

    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_stocks")
        .update({"name": name})
        .eq("code", code)
        .execute()
    )


async def upsert_price(code: str, price: Decimal) -> None:
    client = _ensure_client()
    today_iso = datetime.now(timezone.utc).date().isoformat()
    await _run_supabase(
        lambda: client.table("a_share_prices")
        .upsert(
            {
                "code": code,
                "price": str(price),
                "price_date": today_iso,
                "updated_at": _utc_now_iso(),
            },
            on_conflict="code",
        )
        .execute()
    )
    try:
        await _run_supabase(
            lambda: client.table("a_share_stocks")
            .update({"price_sync_date": today_iso})
            .eq("code", code)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("price_sync_date 更新跳过（可能尚未执行迁移）: %s", exc)


async def upsert_price_with_market_values(
    code: str,
    price: Decimal,
    current_market_cap: Decimal | None,
    last_year_end_price: Decimal | None,
    last_year_end_market_cap: Decimal | None,
    last_year_end_date: str | None,
) -> None:
    client = _ensure_client()
    today_iso = datetime.now(timezone.utc).date().isoformat()
    try:
        await _run_supabase(
            lambda: client.table("a_share_prices")
            .upsert(
                {
                    "code": code,
                    "price": str(price),
                    "price_date": today_iso,
                    "current_market_cap": str(current_market_cap) if current_market_cap is not None else None,
                    "last_year_end_price": str(last_year_end_price) if last_year_end_price is not None else None,
                    "last_year_end_market_cap": str(last_year_end_market_cap) if last_year_end_market_cap is not None else None,
                    "last_year_end_date": last_year_end_date,
                    "updated_at": _utc_now_iso(),
                },
                on_conflict="code",
            )
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("市场值字段写入失败，回退到基础价格写入: %s", exc)
        await _run_supabase(
            lambda: client.table("a_share_prices")
            .upsert(
                {
                    "code": code,
                    "price": str(price),
                    "price_date": today_iso,
                    "updated_at": _utc_now_iso(),
                },
                on_conflict="code",
            )
            .execute()
        )
    try:
        await _run_supabase(
            lambda: client.table("a_share_stocks")
            .update({"price_sync_date": today_iso})
            .eq("code", code)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("price_sync_date 更新跳过（可能尚未执行迁移）: %s", exc)


async def mark_fundamental_synced(code: str) -> None:
    client = _ensure_client()
    today_iso = datetime.now(timezone.utc).date().isoformat()
    try:
        await _run_supabase(
            lambda: client.table("a_share_stocks")
            .update({"fundamental_sync_date": today_iso})
            .eq("code", code)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("fundamental_sync_date 更新跳过（可能尚未执行迁移）: %s", exc)


async def upsert_dividend_row(
    code: str,
    year: int,
    dividend_per_share: Decimal,
    net_profit: Decimal | None,
    payout_ratio: Decimal | None,
) -> None:
    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_dividends")
        .upsert(
            {
                "code": code,
                "year": year,
                "dividend_per_share": str(dividend_per_share),
                "net_profit": str(net_profit) if net_profit is not None else None,
                "payout_ratio": str(payout_ratio) if payout_ratio is not None else None,
                "source": "akshare",
                "updated_at": _utc_now_iso(),
            },
            on_conflict="code,year",
        )
        .execute()
    )


async def upsert_dividend_profit_only(code: str, year: int, net_profit: Decimal) -> None:
    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_dividends")
        .upsert(
            {
                "code": code,
                "year": year,
                "net_profit": str(net_profit),
                "source": "akshare",
                "updated_at": _utc_now_iso(),
            },
            on_conflict="code,year",
        )
        .execute()
    )


async def upsert_quarterly_profit(code: str, year: int, quarter: int, net_profit: Decimal) -> None:
    client = _ensure_client()
    await _run_supabase(
        lambda: client.table("a_share_quarterly_profits")
        .upsert(
            {
                "code": code,
                "year": year,
                "quarter": quarter,
                "net_profit": str(net_profit),
                "is_published": True,
                "source": "akshare",
                "updated_at": _utc_now_iso(),
            },
            on_conflict="code,year,quarter",
        )
        .execute()
    )
