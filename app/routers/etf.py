"""ETF 相关 API。"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from time import monotonic

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import StreamingResponse

from .. import database
from ..schemas import EtfInstrumentPayload, EtfSyncRequest
from ..services import etf_backtests
from ..services import etf_data_source

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/etf", tags=["etf"])


def _stream_json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)} is not JSON serializable")


def _build_sync_failure_message(result: dict) -> str:
    failed_codes = result.get("failed_codes") or []
    code_part = f"，失败代码: {','.join(failed_codes)}" if failed_codes else ""
    return (
        f"{result.get('stage')} 同步失败 {result.get('failed', 0)} 项 / 总计 {result.get('total', 0)} 项"
        f"{code_part}"
    )


def _normalize_backtest_range(start_date: str | None, end_date: str | None) -> tuple[str | None, str | None]:
    normalized_start = (start_date or "").strip() or None
    normalized_end = (end_date or "").strip() or None

    if normalized_start is not None:
        try:
            datetime.strptime(normalized_start, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(400, "start_date 必须是 YYYY-MM-DD 格式") from exc

    if normalized_end is not None:
        try:
            datetime.strptime(normalized_end, "%Y-%m-%d")
        except ValueError as exc:
            raise HTTPException(400, "end_date 必须是 YYYY-MM-DD 格式") from exc

    if normalized_start and normalized_end and normalized_start > normalized_end:
        raise HTTPException(400, "start_date 不能晚于 end_date")

    return normalized_start, normalized_end


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
    started = monotonic()
    logger.info("[ETF] list_instruments start")
    rows = await database.list_etf_dashboard_rows()
    logger.info("[ETF] list_instruments done total=%d cost=%.3fs", len(rows), monotonic() - started)
    return {"items": rows, "total": len(rows)}


@router.get("/{code}/backtest")
async def get_backtest(
    code: str,
    limit: int = 6000,
    source: str = Query("auto", description="auto / realtime / snapshot"),
    start_date: str | None = Query(None, description="回测开始日期，格式 YYYY-MM-DD"),
    end_date: str | None = Query(None, description="回测结束日期，格式 YYYY-MM-DD"),
) -> dict:
    started = monotonic()
    normalized_code = code.strip()
    if not normalized_code:
        raise HTTPException(400, "ETF 代码不能为空")

    normalized_source = source.strip().lower()
    if normalized_source not in {"auto", "realtime", "snapshot"}:
        raise HTTPException(400, "source 仅支持 auto / realtime / snapshot")
    normalized_start, normalized_end = _normalize_backtest_range(start_date, end_date)
    logger.info(
        "[ETF] get_backtest start code=%s source=%s limit=%d start_date=%s end_date=%s",
        normalized_code,
        normalized_source,
        limit,
        normalized_start,
        normalized_end,
    )

    snapshot_row = await database.get_etf_backtest_snapshot(normalized_code)
    snapshot_payload = (snapshot_row or {}).get("payload")
    if normalized_source == "snapshot":
        if isinstance(snapshot_payload, dict):
            logger.info(
                "[ETF] get_backtest snapshot_hit code=%s cost=%.3fs",
                normalized_code,
                monotonic() - started,
            )
            return snapshot_payload
        raise HTTPException(404, f"ETF {normalized_code} 暂无回测快照")

    history_rows = await database.list_etf_price_history(
        normalized_code,
        limit=limit,
        start_date=normalized_start,
        end_date=normalized_end,
    )
    if not history_rows:
        if normalized_source == "auto" and (normalized_start is None and normalized_end is None) and isinstance(snapshot_payload, dict):
            logger.info(
                "[ETF] get_backtest fallback_snapshot code=%s cost=%.3fs",
                normalized_code,
                monotonic() - started,
            )
            return snapshot_payload
        if normalized_start or normalized_end:
            raise HTTPException(404, f"ETF {normalized_code} 在指定回测时间段内无历史数据")
        raise HTTPException(404, f"ETF {normalized_code} 暂无历史数据，请先执行 history 同步")

    payload = etf_backtests.build_backtest_payload(normalized_code, history_rows)
    await database.upsert_etf_backtest_snapshot(normalized_code, payload)
    logger.info(
        "[ETF] get_backtest done code=%s items=%d cost=%.3fs",
        normalized_code,
        payload.get("total", 0),
        monotonic() - started,
    )
    return payload


@router.get("/{code}/backtest/compare")
async def get_backtest_compare(
    code: str,
    limit: int = 6000,
    strategies: str | None = Query(None, description="逗号分隔策略 key"),
    start_date: str | None = Query(None, description="回测开始日期，格式 YYYY-MM-DD"),
    end_date: str | None = Query(None, description="回测结束日期，格式 YYYY-MM-DD"),
) -> dict:
    started = monotonic()
    normalized_code = code.strip()
    if not normalized_code:
        raise HTTPException(400, "ETF 代码不能为空")

    normalized_start, normalized_end = _normalize_backtest_range(start_date, end_date)
    history_rows = await database.list_etf_price_history(
        normalized_code,
        limit=limit,
        start_date=normalized_start,
        end_date=normalized_end,
    )
    if not history_rows:
        if normalized_start or normalized_end:
            raise HTTPException(404, f"ETF {normalized_code} 在指定回测时间段内无历史数据")
        raise HTTPException(404, f"ETF {normalized_code} 暂无历史数据，请先执行 history 同步")

    strategy_keys = None
    if strategies is not None:
        strategy_keys = [item.strip() for item in strategies.split(",")]
    logger.info(
        "[ETF] get_backtest_compare start code=%s limit=%d start_date=%s end_date=%s strategies=%s",
        normalized_code,
        limit,
        normalized_start,
        normalized_end,
        ",".join(strategy_keys or []),
    )

    payload = etf_backtests.build_compare_payload(
        normalized_code,
        history_rows,
        strategy_keys=strategy_keys,
    )
    logger.info(
        "[ETF] get_backtest_compare done code=%s strategies=%d cost=%.3fs",
        normalized_code,
        len(payload.get("strategies") or []),
        monotonic() - started,
    )
    return payload


@router.get("/{code}/backtest/compare/stream")
async def get_backtest_compare_stream(
    code: str,
    limit: int = 6000,
    strategies: str | None = Query(None, description="逗号分隔策略 key"),
    start_date: str | None = Query(None, description="回测开始日期，格式 YYYY-MM-DD"),
    end_date: str | None = Query(None, description="回测结束日期，格式 YYYY-MM-DD"),
) -> StreamingResponse:
    started = monotonic()
    normalized_code = code.strip()
    if not normalized_code:
        raise HTTPException(400, "ETF 代码不能为空")

    normalized_start, normalized_end = _normalize_backtest_range(start_date, end_date)
    history_rows = await database.list_etf_price_history(
        normalized_code,
        limit=limit,
        start_date=normalized_start,
        end_date=normalized_end,
    )
    if not history_rows:
        if normalized_start or normalized_end:
            raise HTTPException(404, f"ETF {normalized_code} 在指定回测时间段内无历史数据")
        raise HTTPException(404, f"ETF {normalized_code} 暂无历史数据，请先执行 history 同步")

    strategy_keys = None
    if strategies is not None:
        strategy_keys = [item.strip() for item in strategies.split(",")]
    logger.info(
        "[ETF] get_backtest_compare_stream start code=%s limit=%d start_date=%s end_date=%s strategies=%s",
        normalized_code,
        limit,
        normalized_start,
        normalized_end,
        ",".join(strategy_keys or []),
    )

    context = etf_backtests.build_compare_context(normalized_code, history_rows)
    selected_keys, ignored_keys = etf_backtests.resolve_strategy_keys(strategy_keys)

    async def stream():
        try:
            meta_payload = {
                "type": "meta",
                "etf": context["etf"],
                "total": context["total"],
                "available_strategies": etf_backtests.get_available_strategies(),
                "selected_strategies": selected_keys,
                "ignored_strategies": ignored_keys,
            }
            yield f"data: {json.dumps(meta_payload, ensure_ascii=False, default=_stream_json_default)}\n\n"

            benchmark = etf_backtests.build_benchmark_result(context)
            yield f"data: {json.dumps({'type': 'benchmark', 'data': benchmark}, ensure_ascii=False, default=_stream_json_default)}\n\n"

            for strategy_key in selected_keys:
                strategy_result = etf_backtests.build_strategy_result(strategy_key, context)
                yield f"data: {json.dumps({'type': 'strategy', 'data': strategy_result}, ensure_ascii=False, default=_stream_json_default)}\n\n"
                await asyncio.sleep(0)

            logger.info(
                "[ETF] get_backtest_compare_stream done code=%s strategies=%d cost=%.3fs",
                normalized_code,
                len(selected_keys),
                monotonic() - started,
            )
            yield "data: {\"type\":\"done\"}\n\n"
        except Exception as exc:  # noqa: BLE001
            logger.exception("[ETF] get_backtest_compare_stream failed code=%s", normalized_code)
            error_payload = {"type": "error", "message": str(exc)}
            yield f"data: {json.dumps(error_payload, ensure_ascii=False)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@router.post("/instruments")
async def add_instrument(payload: EtfInstrumentPayload) -> dict:
    code = payload.code.strip()
    if not code:
        raise HTTPException(400, "ETF 代码不能为空")

    resolved = await etf_data_source.resolve_etf_identity(code)
    resolved_name = (payload.name or "").strip()
    if (not resolved_name) or resolved_name == code:
        resolved_name = (resolved.get("name") or "").strip()

    resolved_provider = (payload.provider or "").strip() or "未知"
    resolved_market = (payload.market or "").strip() or str(resolved.get("market") or "SH")

    await database.upsert_etf_instrument(
        code=code,
        name=resolved_name or code,
        provider=resolved_provider,
        tracking_index=payload.tracking_index,
        market=resolved_market,
    )
    return {"ok": True, "code": code, "name": resolved_name or code, "market": resolved_market}


@router.delete("/instruments/{code}")
async def remove_instrument(code: str) -> dict:
    await database.deactivate_etf_instrument(code.strip())
    return {"ok": True, "code": code.strip()}


@router.post("/sync")
async def trigger_sync(req: EtfSyncRequest, background: BackgroundTasks) -> dict:
    logger.info("[ETF] trigger_sync enqueue job_type=%s codes=%s", req.job_type, req.codes)
    background.add_task(_run_etf_sync, req)
    return {"ok": True, "message": "ETF 同步任务已加入后台队列"}


@router.post("/sync/blocking")
async def trigger_sync_blocking(req: EtfSyncRequest) -> dict:
    started = monotonic()
    logger.info("[ETF] trigger_sync_blocking start job_type=%s codes=%s", req.job_type, req.codes)
    try:
        result = await _run_etf_sync(req, raise_on_error=True)
        logger.info(
            "[ETF] trigger_sync_blocking done ok=%s affected=%s cost=%.3fs",
            result.get("ok"),
            result.get("affected"),
            monotonic() - started,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("[ETF] trigger_sync_blocking failed job_type=%s codes=%s", req.job_type, req.codes)
        raise HTTPException(500, f"ETF 同步失败: {exc}") from exc


@router.get("/sync/logs")
async def list_logs(limit: int = 20) -> dict:
    started = monotonic()
    logger.info("[ETF] list_logs start limit=%d", limit)
    rows = await database.list_etf_sync_logs(limit=limit)
    logger.info("[ETF] list_logs done total=%d cost=%.3fs", len(rows), monotonic() - started)
    return {"items": rows}
