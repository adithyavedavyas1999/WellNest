"""
Application configuration — loaded from environment variables via pydantic-settings.

We keep everything in one Settings class so there's a single source of truth.
FastAPI pulls from this at startup; individual routers just import `get_settings`.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -- database --
    database_url: str = "postgresql://wellnest:changeme@localhost:5432/wellnest"
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_recycle: int = 1800  # seconds before a connection gets recycled

    # -- api server --
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = False
    api_log_level: str = "info"
    environment: str = "development"

    # -- auth --
    # simple header-based key for now; we can swap to JWT later
    api_key: Optional[str] = None
    api_key_header: str = "X-API-Key"

    # -- rate limiting (per IP) --
    rate_limit_requests: int = 100
    rate_limit_window: int = 60  # seconds

    # -- cors --
    api_cors_origins: str = "http://localhost:8501,http://localhost:3000"

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.api_cors_origins.split(",") if o.strip()]

    # -- pagination defaults --
    default_page_size: int = 50
    max_page_size: int = 200

    # -- openai --
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4o-mini"
    openai_embedding_model: str = "text-embedding-3-small"

    # -- pdf reports --
    reports_output_dir: str = "./reports/output"

    @field_validator("api_log_level")
    @classmethod
    def _normalize_log_level(cls, v: str) -> str:
        return v.lower()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton so we only parse env once per process."""
    return Settings()
