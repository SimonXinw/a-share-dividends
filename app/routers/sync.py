"""同步任务 API。"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks

from .. import database
from ..config import settings
from ..schemas import SyncRequest
from ..services import data_source

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sync", tags=["sync"])


async def _run_sync(req: SyncRequest) -> None:
    started_at = datetime.now(timezone.utc)
    log_id_row = await database.fetch_one(
        """
        insert into public.a_share_sync_logs (job_type, status, started_at)
        values ($1, 'running', $2)
        returning id
        """,
        req.job_type, started_at,
    )
    log_id = log_id_row["id"] if log_id_row else None

    try:
        affected = 0
        if req.job_type == "price":
            affected = await data_source.sync_prices(req.codes)
        elif req.job_type in ("dividend", "profit", "fundamental"):
            affected = await data_source.sync_dividends_and_profits(
                req.codes, concurrency=settings.sync_concurrency
            )
        else:
            result = await data_source.sync_all(req.codes, concurrency=settings.sync_concurrency)
            affected = result["price_count"] + result["fundamental_count"]

        await database.execute(
            """
            update public.a_share_sync_logs
               set status = 'success', affected_rows = $2, finished_at = now()
             where id = $1
            """,
            log_id, affected,
        )
        logger.info("同步任务完成：%s, 影响 %d 条", req.job_type, affected)
    except Exception as e:  # noqa: BLE001
        logger.exception("同步任务失败")
        await database.execute(
            """
            update public.a_share_sync_logs
               set status = 'failed', message = $2, finished_at = now()
             where id = $1
            """,
            log_id, str(e),
        )


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
    rows = await database.fetch_all(
        """
        select id, job_type, status, affected_rows, message, started_at, finished_at
        from public.a_share_sync_logs
        order by started_at desc
        limit $1
        """,
        limit,
    )
    return {"items": rows}
