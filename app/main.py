"""FastAPI 主入口。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from . import database
from .config import settings
from .routers import stocks, sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_pool()
    try:
        yield
    finally:
        await database.close_pool()


app = FastAPI(
    title="A 股红利股息计算",
    description="A 股股票分红股息预估表，支持表格内编辑、实时计算、按预估股息率降序排序。",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# JSON 编码器：让 Decimal 序列化为 number 而不是字符串
# ============================================================================
class DecimalJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        import json
        from datetime import date, datetime

        def default(o):
            if isinstance(o, Decimal):
                # 转为 float，保留合理精度
                return float(o)
            if isinstance(o, (datetime, date)):
                return o.isoformat()
            raise TypeError(f"Object of type {type(o)} is not JSON serializable")

        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            default=default,
        ).encode("utf-8")


app.router.default_response_class = DecimalJSONResponse


# ============================================================================
# 路由
# ============================================================================
app.include_router(stocks.router)
app.include_router(sync.router)


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True}


# ============================================================================
# 静态文件 + 首页
# ============================================================================
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_debug,
    )
