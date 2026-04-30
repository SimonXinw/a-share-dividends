"""股票相关 API。"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from .. import database
from ..schemas import StockAddPayload, StockOverridePayload
from ..services import calculator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stocks", tags=["stocks"])


@router.get("")
async def list_stocks() -> dict:
    """返回表格数据，已按今年预估股息率降序排列。"""
    contexts = await calculator.load_all_contexts()
    rows = [calculator.context_to_row(c) for c in contexts]
    rows = calculator.sort_rows_desc_by_estimated_yield(rows)
    return {"items": rows, "total": len(rows)}


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

    contexts = await calculator.load_all_contexts()
    matched = next((c for c in contexts if c.code == code), None)
    if not matched:
        raise HTTPException(404, "股票上下文加载失败")

    return {"ok": True, "row": calculator.context_to_row(matched)}


@router.delete("/{code}/override")
async def clear_override(code: str) -> dict:
    """清空所有覆盖字段。"""
    await database.delete_override(code)
    contexts = await calculator.load_all_contexts()
    matched = next((c for c in contexts if c.code == code), None)
    return {"ok": True, "row": calculator.context_to_row(matched) if matched else None}


def _guess_market(code: str) -> str:
    if code.startswith(("60", "68", "9")):
        return "SH"
    if code.startswith(("00", "30", "20")):
        return "SZ"
    if code.startswith(("8", "4")):
        return "BJ"
    return "SH"
