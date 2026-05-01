"""股票相关 API。"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from .. import database
from ..schemas import StockAddPayload, StockOverridePayload
from ..services import calculator, macro

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stocks", tags=["stocks"])


def _attach_industry_avg(row: dict, industry: str | None, means: dict) -> dict:
    key = calculator.industry_key(industry)
    row["industry_avg_yield"] = means.get(key)
    return row


@router.get("")
async def list_stocks() -> dict:
    """返回表格数据，已按今年预估股息率降序排列。"""
    contexts = await calculator.load_all_contexts()
    industry_means = calculator.compute_industry_yield_means(contexts)
    risk_free_rate = await macro.get_risk_free_rate()

    rows = [
        _attach_industry_avg(calculator.context_to_row(c), c.industry, industry_means)
        for c in contexts
    ]
    rows = calculator.sort_rows_desc_by_estimated_yield(rows)

    return {
        "items": rows,
        "total": len(rows),
        "industry_yield_means": industry_means,
        "risk_free_rate": risk_free_rate,
    }


@router.post("")
async def add_stock(payload: StockAddPayload) -> dict:
    """新增一只股票到关注列表。如果已存在则启用它。"""
    code = payload.code.strip().zfill(6)
    name = payload.name or code
    market = payload.market or _guess_market(code)

    await database.upsert_stock(code, name, payload.industry, market)

    return {"ok": True, "code": code}


@router.delete("/{code}")
async def remove_stock(code: str) -> dict:
    """从关注列表移除（软删除：is_active=false）。"""
    await database.deactivate_stock(code)
    return {"ok": True, "code": code}


@router.put("/{code}/override")
async def upsert_override(code: str, payload: StockOverridePayload) -> dict:
    """保存用户在表格中编辑的字段。值为 null 表示清除该覆盖（恢复原始数据）。"""
    exists = await database.stock_exists_active(code)
    if not exists:
        raise HTTPException(404, f"股票 {code} 不存在或未启用")

    try:
        await database.upsert_override(
            code,
            {
                "price": payload.price,
                "last_year_dividend": payload.last_year_dividend,
                "last_year_net_profit": payload.last_year_net_profit,
                "this_year_estimated_profit": payload.this_year_estimated_profit,
                "note": payload.note,
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("[%s] upsert_override 写库失败，payload=%s", code, payload.model_dump())
        raise HTTPException(500, f"保存覆盖值失败：{exc}") from exc

    try:
        contexts = await calculator.load_all_contexts()
        matched = next((c for c in contexts if c.code == code), None)
        if not matched:
            raise HTTPException(404, "股票上下文加载失败")

        industry_means = calculator.compute_industry_yield_means(contexts)
        row = _attach_industry_avg(
            calculator.context_to_row(matched), matched.industry, industry_means
        )
        return {"ok": True, "row": row}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception("[%s] upsert_override 重算失败", code)
        raise HTTPException(500, f"覆盖已保存，但重算失败：{exc}") from exc


@router.delete("/{code}/override")
async def clear_override(code: str) -> dict:
    """清空所有覆盖字段。"""
    await database.delete_override(code)
    contexts = await calculator.load_all_contexts()
    matched = next((c for c in contexts if c.code == code), None)
    if not matched:
        return {"ok": True, "row": None}

    industry_means = calculator.compute_industry_yield_means(contexts)
    row = _attach_industry_avg(calculator.context_to_row(matched), matched.industry, industry_means)
    return {"ok": True, "row": row}


def _guess_market(code: str) -> str:
    if code.startswith(("60", "68", "9")):
        return "SH"
    if code.startswith(("00", "30", "20")):
        return "SZ"
    if code.startswith(("8", "4")):
        return "BJ"
    return "SH"
