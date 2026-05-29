"""ETF 相关 API。"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from time import monotonic

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from .. import database
from ..schemas import EtfInstrumentPayload, EtfSyncRequest
from ..services import etf_backtests
from ..services import etf_data_source

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/etf", tags=["etf"])


def _build_sync_failure_message(result: dict) -> str:
    failed_codes = result.get("failed_codes") or []
    code_part = f"，失败代码: {','.join(failed_codes)}" if failed_codes else ""
    return (
        f"{result.get('stage')} 同步失败 {result.get('failed', 0)} 项 / 总计 {result.get('total', 0)} 项"
        f"{code_part}"
    )


async def _run_etf_sync(req: EtfSyncRequest, raise_on_error: bool = False) -> dict:
    started_at = datetime.now(timezone.utc)
    log_id = await database.create_etf_sync_log(req.job_type, started_at)
    progress_state = {"last_push": 0.0}

    async def push_progress(payload: dict, force: bool = False) -> None:
        now = monotonic()
        should_push = force or (now - progress_state["last_push"] >= 1.0)
        if not should_push:
            return

        processed = payload.get("processed", 0)
        total = payload.get("total", 0)
        success = payload.get("success", 0)
        failed = payload.get("failed", 0)
        stage = payload.get("stage") or "etf"
        stage_label = "ETF 历史同步" if stage == "etf_history" else "ETF 行情同步"
        message = f"{stage_label}: {processed}/{total}，成功 {success}，失败 {failed}"
        await database.update_etf_sync_log_progress(log_id, message=message, affected_rows=success)
        progress_state["last_push"] = now

    try:
        if req.job_type == "price":
            result = await etf_data_source.sync_etf_prices(
                req.codes,
                progress_cb=push_progress,
            )
        elif req.job_type == "history":
            result = await etf_data_source.sync_etf_histories(
                req.codes,
                progress_cb=push_progress,
            )
        elif req.job_type == "all":
            price_result = await etf_data_source.sync_etf_prices(
                req.codes,
                progress_cb=push_progress,
            )
            history_result = await etf_data_source.sync_etf_histories(
                req.codes,
                progress_cb=push_progress,
            )
            result = {
                "stage": "etf_all",
                "total": (price_result.get("total") or 0) + (history_result.get("total") or 0),
                "processed": (price_result.get("processed") or 0) + (history_result.get("processed") or 0),
                "success": (price_result.get("success") or 0) + (history_result.get("success") or 0),
                "failed": (price_result.get("failed") or 0) + (history_result.get("failed") or 0),
                "affected": (price_result.get("affected") or 0) + (history_result.get("affected") or 0),
                "failed_codes": (price_result.get("failed_codes") or []) + (history_result.get("failed_codes") or []),
                "price": price_result,
                "history": history_result,
            }
        else:
            raise ValueError("job_type 仅支持 price / history / all")

        affected = result.get("affected") or 0
        if (result.get("failed") or 0) > 0:
            raise RuntimeError(_build_sync_failure_message(result))

        await database.update_etf_sync_log_progress(log_id, message="ETF 同步完成，正在收尾...", affected_rows=affected)
        await database.mark_etf_sync_log_success(log_id, affected)
        return {"ok": True, **result}
    except Exception as exc:  # noqa: BLE001
        logger.exception("ETF 同步任务失败")
        await database.mark_etf_sync_log_failed(log_id, str(exc))
        if raise_on_error:
            raise
        return {"ok": False, "message": str(exc)}


@router.get("/instruments")
async def list_instruments() -> dict:
    rows = await database.list_etf_dashboard_rows()
    return {"items": rows, "total": len(rows)}


@router.get("/{code}/backtest")
async def get_backtest(
    code: str,
    limit: int = 6000,
    source: str = Query("auto", description="auto / realtime / snapshot"),
) -> dict:
    normalized_code = code.strip()
    if not normalized_code:
        raise HTTPException(400, "ETF 代码不能为空")

    normalized_source = source.strip().lower()
    if normalized_source not in {"auto", "realtime", "snapshot"}:
        raise HTTPException(400, "source 仅支持 auto / realtime / snapshot")

    snapshot_row = await database.get_etf_backtest_snapshot(normalized_code)
    snapshot_payload = (snapshot_row or {}).get("payload")
    if normalized_source == "snapshot":
        if isinstance(snapshot_payload, dict):
            return snapshot_payload
        raise HTTPException(404, f"ETF {normalized_code} 暂无回测快照")

    history_rows = await database.list_etf_price_history(normalized_code, limit=limit)
    if not history_rows:
        if normalized_source == "auto" and isinstance(snapshot_payload, dict):
            return snapshot_payload
        raise HTTPException(404, f"ETF {normalized_code} 暂无历史数据，请先执行 history 同步")

    payload = etf_backtests.build_backtest_payload(normalized_code, history_rows)
    await database.upsert_etf_backtest_snapshot(normalized_code, payload)
    return payload


@router.post("/instruments")
async def add_instrument(payload: EtfInstrumentPayload) -> dict:
    code = payload.code.strip()
    if not code:
        raise HTTPException(400, "ETF 代码不能为空")

    await database.upsert_etf_instrument(
        code=code,
        name=payload.name or code,
        provider=payload.provider or "易方达",
        tracking_index=payload.tracking_index,
        market=payload.market or "SH",
    )
    return {"ok": True, "code": code}


@router.delete("/instruments/{code}")
async def remove_instrument(code: str) -> dict:
    await database.deactivate_etf_instrument(code.strip())
    return {"ok": True, "code": code.strip()}


@router.post("/sync")
async def trigger_sync(req: EtfSyncRequest, background: BackgroundTasks) -> dict:
    background.add_task(_run_etf_sync, req)
    return {"ok": True, "message": "ETF 同步任务已加入后台队列"}


@router.post("/sync/blocking")
async def trigger_sync_blocking(req: EtfSyncRequest) -> dict:
    try:
        result = await _run_etf_sync(req, raise_on_error=True)
        return result
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(500, f"ETF 同步失败: {exc}") from exc


@router.get("/sync/logs")
async def list_logs(limit: int = 20) -> dict:
    rows = await database.list_etf_sync_logs(limit=limit)
    return {"items": rows}
