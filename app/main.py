"""FastAPI 主入口。"""

from __future__ import annotations

import logging
import webbrowser
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from . import database
from .config import settings
from .routers import etf, macro, stocks, sync

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _build_page_view_url(host: str, port: int) -> str:
    """把 0.0.0.0 映射为本机可打开的 localhost。"""
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    return f"http://{host}:{port}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_pool()
    page_view_url = _build_page_view_url(settings.app_host, settings.app_port)
    logger.info("page view: on %s", page_view_url)

    if settings.app_auto_open_browser:
        try:
            webbrowser.open(page_view_url, new=2, autoraise=True)
            logger.info("已尝试自动打开页面")
        except Exception as exc:
            logger.warning("自动打开页面失败：%s", exc)
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
app.include_router(macro.router)
app.include_router(etf.router)


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True}


# ============================================================================
# 静态文件 + 首页
# ============================================================================
STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _static_version(filename: str) -> str:
    """用文件 mtime 作为版本号，文件改了 URL 自动变，浏览器自动拉新版。"""
    try:
        return str(int((STATIC_DIR / filename).stat().st_mtime))
    except OSError:
        return "0"


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    html = html.replace("__CSS_VERSION__", _static_version("style.css"))
    html = html.replace("__JS_VERSION__", _static_version("app.js"))
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@app.get("/dividend-strategy", response_class=HTMLResponse)
async def dividend_strategy_page() -> HTMLResponse:
    html = (STATIC_DIR / "dividend_strategy.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@app.get("/dividend-strategy/{code}", response_class=RedirectResponse)
async def dividend_strategy_page_with_code(code: str) -> RedirectResponse:
    return RedirectResponse(url=f"/dividend-strategy?code={code}")


@app.get("/dividend-strategy-compare", response_class=HTMLResponse)
async def dividend_strategy_compare_page() -> HTMLResponse:
    html = (STATIC_DIR / "dividend_strategy_compare.html").read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


@app.get("/dividend-strategy-compare/{code}", response_class=RedirectResponse)
async def dividend_strategy_compare_page_with_code(code: str) -> RedirectResponse:
    return RedirectResponse(url=f"/dividend-strategy-compare?code={code}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_debug,
    )
