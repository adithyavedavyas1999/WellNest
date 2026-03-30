"""
Dagster resources for WellNest.

Centralizes all external service configuration so individual assets don't
need to know about connection strings, API keys, etc.  Everything reads
from env vars at startup via pydantic-settings.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ingestion.utils.http_client import WellNestHTTPClient

import structlog
from dagster import ConfigurableResource
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "transformation" / "dbt_project"
DBT_PROFILES_DIR = PROJECT_ROOT / "transformation"


# ---------------------------------------------------------------------------
# Global config — single source of truth for env vars
# ---------------------------------------------------------------------------


class WellNestConfig(BaseSettings):
    """Reads config from env vars / .env file.

    pydantic-settings handles the precedence: env var > .env > default.
    Nested prefixes would be nice but DAGSTER_ already claims that namespace
    so we just use flat names.
    """

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "wellnest"
    postgres_user: str = "wellnest"
    postgres_password: str = "changeme"
    database_url: str | None = None

    # dagster metadata db (separate from the app db)
    dagster_pg_host: str = "localhost"
    dagster_pg_port: int = 5432
    dagster_pg_db: str = "dagster"
    dagster_pg_user: str = "wellnest"
    dagster_pg_password: str = "changeme"

    # dbt
    dbt_profiles_dir: str = str(DBT_PROFILES_DIR)
    dbt_target: str = "dev"

    # api keys
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"
    openai_embedding_model: str = "text-embedding-3-small"
    census_api_key: str = ""
    airnow_api_key: str = ""
    socrata_app_token: str = ""

    # mlflow
    mlflow_tracking_uri: str = "http://localhost:5000"
    mlflow_experiment_name: str = "wellnest-default"

    # paths
    data_dir: str = str(PROJECT_ROOT / "data")
    reports_output_dir: str = str(PROJECT_ROOT / "reports" / "output")

    # misc
    environment: str = "development"
    log_level: str = "INFO"

    @property
    def pg_url(self) -> str:
        if self.database_url:
            return self.database_url
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


_config_singleton: WellNestConfig | None = None


def get_config() -> WellNestConfig:
    """Lazy singleton so we only parse env vars once per process."""
    global _config_singleton
    if _config_singleton is None:
        _config_singleton = WellNestConfig()
    return _config_singleton


# ---------------------------------------------------------------------------
# Postgres resource
# ---------------------------------------------------------------------------


class PostgresResource(ConfigurableResource):
    """Connection pool for the WellNest application database.

    We keep a single engine per resource instance to avoid the connection
    leak issue we hit during the initial prototype (see ingestion/utils/__init__.py
    docstring for the war story).
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "wellnest"
    user: str = "wellnest"
    password: str = "changeme"
    pool_size: int = 5
    max_overflow: int = 10

    _engine: Engine | None = None

    class Config:
        arbitrary_types_allowed = True

    @property
    def connection_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self.connection_url,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_pre_ping=True,
            )
        return self._engine

    def execute(self, query: str, params: dict[str, Any] | None = None) -> Any:
        engine = self.get_engine()
        with engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            return result

    def ensure_schema(self, schema_name: str) -> None:
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def get_row_count(self, table: str) -> int:
        engine = self.get_engine()
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT count(*) FROM {table}")).fetchone()
            return row[0] if row else 0

    def teardown_for_execution(self, context: Any) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


# ---------------------------------------------------------------------------
# dbt resource
# ---------------------------------------------------------------------------


class DbtResource(ConfigurableResource):
    """Wraps dbt CLI invocations.

    We shell out to dbt rather than using the Python API because the Python
    API doesn't support all commands and the CLI output is more debuggable.
    dagster-dbt has a DbtCliResource but we wrap it here so we can inject
    our project paths and keep the asset code clean.
    """

    project_dir: str = str(DBT_PROJECT_DIR)
    profiles_dir: str = str(DBT_PROFILES_DIR)
    target: str = "dev"

    def _base_cmd(self) -> list[str]:
        return [
            "dbt",
            "--project-dir",
            self.project_dir,
            "--profiles-dir",
            self.profiles_dir,
            "--target",
            self.target,
        ]

    def run(
        self,
        select: str | None = None,
        exclude: str | None = None,
        full_refresh: bool = False,
        vars_dict: dict[str, Any] | None = None,
    ) -> DbtResult:
        import json
        import subprocess

        cmd = [*self._base_cmd(), "run"]
        if select:
            cmd += ["--select", select]
        if exclude:
            cmd += ["--exclude", exclude]
        if full_refresh:
            cmd.append("--full-refresh")
        if vars_dict:
            cmd += ["--vars", json.dumps(vars_dict)]

        logger.info("dbt_run", cmd=" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_dir)

        return DbtResult(
            success=result.returncode == 0,
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.returncode,
        )

    def test(self, select: str | None = None) -> DbtResult:
        import subprocess

        cmd = [*self._base_cmd(), "test"]
        if select:
            cmd += ["--select", select]

        logger.info("dbt_test", cmd=" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_dir)

        return DbtResult(
            success=result.returncode == 0,
            stdout=result.stdout,
            stderr=result.stderr,
            return_code=result.returncode,
        )

    def ls(self, select: str | None = None, resource_type: str = "model") -> list[str]:
        """List dbt resources matching a selector."""
        import subprocess

        cmd = [*self._base_cmd(), "ls", "--resource-type", resource_type, "--output", "name"]
        if select:
            cmd += ["--select", select]

        result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.project_dir)
        if result.returncode != 0:
            logger.warning("dbt_ls_failed", stderr=result.stderr)
            return []

        return [line.strip() for line in result.stdout.strip().split("\n") if line.strip()]


class DbtResult:
    """Thin wrapper around subprocess results from dbt."""

    def __init__(self, success: bool, stdout: str, stderr: str, return_code: int):
        self.success: bool = success
        self.stdout: str = stdout
        self.stderr: str = stderr
        self.return_code: int = return_code

    @property
    def model_count(self) -> int:
        """Parse the number of models from dbt stdout.

        Looks for lines like "Completed successfully. 12 models, 0 errors."
        This is fragile but beats parsing the JSON manifest just for a count.
        """
        import re

        for line in self.stdout.split("\n"):
            match = re.search(r"(\d+)\s+(?:of\s+\d+\s+)?(?:model|test)", line, re.IGNORECASE)
            if match:
                return int(match.group(1))
        return 0

    def raise_on_failure(self) -> None:
        if not self.success:
            raise RuntimeError(
                f"dbt failed (exit {self.return_code}):\n{self.stderr}\n{self.stdout}"
            )


# ---------------------------------------------------------------------------
# HTTP client resource
# ---------------------------------------------------------------------------


class HttpClientResource(ConfigurableResource):
    """Dagster-managed wrapper around our HTTP client.

    Exists mostly so the rate limiter and session lifecycle are tied to the
    dagster resource lifecycle instead of floating around as module globals.
    """

    rate_limit: float = 2.0
    timeout: int = 45
    max_retries: int = 3

    def get_client(self) -> WellNestHTTPClient:
        from ingestion.utils.http_client import WellNestHTTPClient

        return WellNestHTTPClient(
            rate_limit=self.rate_limit,
            timeout=self.timeout,
            max_retries=self.max_retries,
        )


# ---------------------------------------------------------------------------
# OpenAI resource
# ---------------------------------------------------------------------------


class OpenAIResource(ConfigurableResource):
    """OpenAI client for RAG, briefs, and LLM-based quality checks.

    We lazy-import openai so the rest of the pipeline doesn't blow up when
    the openai package isn't installed (it's only needed for AI features).
    """

    api_key: str = ""
    model: str = "gpt-4o"
    embedding_model: str = "text-embedding-3-small"
    max_retries: int = 3
    timeout: float = 60.0

    def get_client(self) -> Any:
        from openai import OpenAI

        return OpenAI(
            api_key=self.api_key,
            max_retries=self.max_retries,
            timeout=self.timeout,
        )

    def get_embeddings(self, texts: list[str]) -> list[list[float]]:
        client = self.get_client()
        response = client.embeddings.create(
            model=self.embedding_model,
            input=texts,
        )
        return [item.embedding for item in response.data]

    def chat(self, messages: list[dict[str, str]], **kwargs: Any) -> str:
        client = self.get_client()
        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            **kwargs,
        )
        return response.choices[0].message.content or ""


# ---------------------------------------------------------------------------
# Resource definitions (wired up in definitions.py)
# ---------------------------------------------------------------------------


def build_resources() -> dict[str, Any]:
    """Construct all dagster resources from environment config.

    Called once in definitions.py.  Each resource reads its config from
    env vars via WellNestConfig.
    """
    cfg = get_config()

    return {
        "config": cfg,
        "postgres": PostgresResource(
            host=cfg.postgres_host,
            port=cfg.postgres_port,
            database=cfg.postgres_db,
            user=cfg.postgres_user,
            password=cfg.postgres_password,
        ),
        "dbt": DbtResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=cfg.dbt_profiles_dir,
            target=cfg.dbt_target,
        ),
        "http_client": HttpClientResource(
            rate_limit=2.0,
            timeout=45,
            max_retries=3,
        ),
        "openai": OpenAIResource(
            api_key=cfg.openai_api_key,
            model=cfg.openai_model,
            embedding_model=cfg.openai_embedding_model,
        ),
    }
