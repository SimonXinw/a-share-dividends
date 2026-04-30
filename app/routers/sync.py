"""同步任务 API。"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from time import monotonic

from fastapi import APIRouter, BackgroundTasks

from .. import database
from ..config import settings
from ..schemas import SyncRequest
from ..services import data_source

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sync", tags=["sync"])


async def _run_sync(req: SyncRequest) -> None:
    started_at = datetime.now(timezone.utc)
    log_id = await database.create_sync_log(req.job_type, started_at)
    progress_state = {"last_push": 0.0}

    async def push_progress(prefix: str, payload: dict, force: bool = False) -> None:
        now = monotonic()
        should_push = force or (now - progress_state["last_push"] >= 1.2)
        if not should_push:
            return

        processed = payload.get("processed", 0)
        total = payload.get("total", 0)
        failed = payload.get("failed", 0)
        success = payload.get("success", 0)
        message = f"{prefix}: {processed}/{total}，成功 {success}，失败 {failed}"
        await database.update_sync_log_progress(log_id, message=message, affected_rows=success)
        progress_state["last_push"] = now

    try:
        affected = 0
        if req.job_type == "price":
            affected = await data_source.sync_prices(
                req.codes,
                progress_cb=lambda p: push_progress("价格同步", p),
            )
        elif req.job_type in ("dividend", "profit", "fundamental"):
            affected = await data_source.sync_dividends_and_profits(
                req.codes,
                concurrency=settings.sync_concurrency,
                progress_cb=lambda p: push_progress("分红/利润同步", p),
            )
        else:
            price_count = await data_source.sync_prices(
                req.codes,
                progress_cb=lambda p: push_progress("一键同步-价格", p),
            )
            fundamental_count = await data_source.sync_dividends_and_profits(
                req.codes,
                concurrency=settings.sync_concurrency,
                progress_cb=lambda p: push_progress("一键同步-基本面", p),
            )
            affected = price_count + fundamental_count

        await database.update_sync_log_progress(log_id, message="同步阶段完成，正在收尾...", affected_rows=affected)
        await database.mark_sync_log_success(log_id, affected)
        logger.info("同步任务完成：%s, 影响 %d 条", req.job_type, affected)
    except Exception as e:  # noqa: BLE001
        logger.exception("同步任务失败")
        await database.mark_sync_log_failed(log_id, str(e))


@router.post("")
async def trigger_sync(req: SyncRequest, background: BackgroundTasks) -> dict:
    """触发同步任务（异步执行，立即返回）。"""
    background.add_task(_run_sync, req)
    return {"ok": True, "message": "同步任务已加入后台队列"}


@router.post("/blocking")
async def trigger_sync_blocking(req: SyncRequest) -> dict:
    """阻塞同步：用于命令行/容器初始化时一次性拉数据。"""
    await _run_sync(req)
    return {"ok": True}


@router.get("/logs")
async def list_logs(limit: int = 20) -> dict:
    rows = await database.list_sync_logs(limit=limit)
    return {"items": rows}
