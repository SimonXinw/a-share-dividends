"""应用配置：所有运行时参数都从环境变量读取。

.env 文件查找顺序（从优先到兜底，越后越优先覆盖）：
1. 当前工作目录的 .env
2. 项目根目录的 .env （a-share-dividends/.env）—— 推荐放这里

环境变量永远优先于 .env 文件（pydantic-settings 默认行为）。
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# app/config.py -> app -> 项目根
_APP_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _APP_DIR.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # 多路径加载：靠后的会覆盖靠前的，所以"项目根"放最后优先级最高
        env_file=(
            ".env",
            _PROJECT_ROOT / ".env",
        ),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------- Supabase / Postgres -------------------
    # Supabase 直接给的 Postgres 连接串，形如：
    # postgres://postgres:<password>@db.<project>.supabase.co:5432/postgres
    database_url: str = ""

    # 部分场景下 Supabase 还提供 REST API（这里我们直接走 Postgres，不用 REST）
    supabase_url: str | None = None
    supabase_anon_key: str | None = None

    # ------------------- App -------------------
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_debug: bool = False

    # 同步任务相关
    sync_concurrency: int = 5  # 并发抓取股票数据的协程数
    sync_request_timeout: int = 15  # 单次请求超时秒数

    # 跨域：前端如果独立部署可以配置允许的源
    cors_origins: str = "*"

    @property
    def cors_origin_list(self) -> list[str]:
        if not self.cors_origins or self.cors_origins == "*":
            return ["*"]
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


settings = get_settings()
