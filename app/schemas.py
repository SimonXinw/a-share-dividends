"""Pydantic 模型：用于 API 入参/出参校验。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


# ============================================================================
# 表格行数据：前端表格展示和编辑的统一结构
# ============================================================================
class StockRow(BaseModel):
    code: str = Field(..., description="股票代码，如 600519")
    name: str = Field(..., description="股票名称")
    industry: Optional[str] = None

    price: Optional[Decimal] = Field(None, description="当前股价")

    last_year: Optional[int] = Field(None, description="去年的会计年度，如 2024")
    last_year_dividend: Optional[Decimal] = Field(None, description="去年每股分红（元/股）")
    last_year_dividend_yield: Optional[Decimal] = Field(None, description="去年股息率 = 去年分红 / 当前股价")

    last_year_net_profit: Optional[Decimal] = Field(None, description="去年归母净利润（元）")
    payout_ratio: Optional[Decimal] = Field(None, description="去年分红比例 = 去年分红总额 / 去年净利润")

    this_year_estimated_profit: Optional[Decimal] = Field(None, description="今年预估全年净利润（元）")
    this_year_estimated_dividend: Optional[Decimal] = Field(None, description="今年预估每股分红（元/股）")
    this_year_estimated_yield: Optional[Decimal] = Field(None, description="今年预估股息率")

    note: Optional[str] = None
    updated_at: Optional[datetime] = None


# ============================================================================
# 用户编辑：覆盖某只股票的几个字段
# ============================================================================
class StockOverridePayload(BaseModel):
    price: Optional[Decimal] = None
    last_year_dividend: Optional[Decimal] = None
    last_year_net_profit: Optional[Decimal] = None
    this_year_estimated_profit: Optional[Decimal] = None
    note: Optional[str] = None


# ============================================================================
# 同步任务
# ============================================================================
class SyncRequest(BaseModel):
    job_type: str = Field("all", description="price / dividend / profit / all")
    codes: Optional[list[str]] = Field(None, description="为空时同步所有 active 的股票")


class SyncResult(BaseModel):
    job_type: str
    status: str
    affected_rows: int
    message: Optional[str] = None
    started_at: datetime
    finished_at: Optional[datetime] = None


# ============================================================================
# 新增股票
# ============================================================================
class StockAddPayload(BaseModel):
    code: str
    name: Optional[str] = None
    industry: Optional[str] = None
    market: Optional[str] = None
