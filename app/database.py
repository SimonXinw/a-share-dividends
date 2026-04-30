"""数据库连接：使用 asyncpg 直连 Supabase Postgres。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import asyncpg

from .config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool

    if _pool is not None:
        return _pool

    if not settings.database_url:
        raise RuntimeError("DATABASE_URL 未配置，请在 .env 中设置 Supabase Postgres 连接串")

    logger.info("正在创建数据库连接池...")
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=1,
        max_size=10,
        command_timeout=30,
        statement_cache_size=0,
    )
    logger.info("数据库连接池已就绪")
    return _pool


async def close_pool() -> None:
    global _pool

    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("数据库连接池尚未初始化")
    return _pool


@asynccontextmanager
async def acquire() -> AsyncIterator[asyncpg.Connection]:
    pool = get_pool()
    async with pool.acquire() as conn:
        yield conn


async def fetch_all(query: str, *args: Any) -> list[dict]:
    async with acquire() as conn:
        rows = await conn.fetch(query, *args)
        return [dict(r) for r in rows]


async def fetch_one(query: str, *args: Any) -> dict | None:
    async with acquire() as conn:
        row = await conn.fetchrow(query, *args)
        return dict(row) if row else None


async def execute(query: str, *args: Any) -> str:
    async with acquire() as conn:
        return await conn.execute(query, *args)
