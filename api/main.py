"""
WellNest API — FastAPI application entry point.

Run locally:
    uvicorn api.main:app --reload --port 8000

Or via the convenience wrapper:
    wellnest-api  (defined in pyproject.toml [project.scripts])
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.config import get_settings
from api.dependencies import close_db_pool, get_db, verify_api_key
from api.middleware.rate_limiter import RateLimitMiddleware
from api.routers import (
    ask_router,
    counties_router,
    health_router,
    predictions_router,
    reports_router,
    schools_router,
    search_router,
)

logger = logging.getLogger("wellnest.api")


# ---------------------------------------------------------------------------
# Lifespan — startup/shutdown hooks
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.api_log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger.info(
        "Starting WellNest API (env=%s, port=%d)",
        settings.environment,
        settings.api_port,
    )
    yield
    close_db_pool()
    logger.info("WellNest API shutdown complete")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

app = FastAPI(
    title="WellNest API",
    description=(
        "REST API serving child wellbeing data for 130K+ US public schools. "
        "Composite scores, pillar breakdowns, rankings, predictions, resource "
        "gaps, and AI-generated community briefs."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Middleware stack (order matters — outermost runs first)
# ---------------------------------------------------------------------------

settings = get_settings()

# CORS — let the dashboard and PWA talk to us
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# rate limiting
app.add_middleware(
    RateLimitMiddleware,
    max_requests=settings.rate_limit_requests,
    window_seconds=settings.rate_limit_window,
)


# request logging — lightweight, just method + path + duration
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000

    # skip noisy health checks in the logs
    if request.url.path != "/api/health":
        logger.info(
            "%s %s %d (%.1fms)",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )

    response.headers["X-Response-Time"] = f"{elapsed_ms:.1f}ms"
    return response


# ---------------------------------------------------------------------------
# Global exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "detail": "Not found",
            "path": str(request.url.path),
            "timestamp": datetime.now(UTC).isoformat(),
        },
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "timestamp": datetime.now(UTC).isoformat(),
        },
    )


# ---------------------------------------------------------------------------
# Include routers
# ---------------------------------------------------------------------------

# health doesn't need auth — monitoring tools hit this
app.include_router(health_router, prefix="/api")

# everything else goes through API key auth (if configured)
protected = [
    schools_router,
    counties_router,
    search_router,
    predictions_router,
    ask_router,
    reports_router,
]
for router in protected:
    app.include_router(router, prefix="/api", dependencies=[Depends(verify_api_key)])


# ---------------------------------------------------------------------------
# Stats endpoint — quick aggregate numbers for the dashboard header
# ---------------------------------------------------------------------------


@app.get("/api/stats", tags=["meta"])
def get_stats(db: Session = Depends(get_db)) -> dict:
    """
    Quick stats for the dashboard hero section: total schools, counties,
    and the national average composite score.
    """
    row = (
        db.execute(
            text("""
        SELECT
            count(*) AS total_schools,
            count(DISTINCT county_fips) AS total_counties,
            round(avg(composite_score)::numeric, 1) AS avg_score,
            round(min(composite_score)::numeric, 1) AS min_score,
            round(max(composite_score)::numeric, 1) AS max_score
        FROM gold.child_wellbeing_score
    """)
        )
        .mappings()
        .first()
    )

    if not row or row["total_schools"] == 0:
        return {
            "total_schools": 0,
            "total_counties": 0,
            "avg_score": None,
            "min_score": None,
            "max_score": None,
        }

    return {
        "total_schools": row["total_schools"],
        "total_counties": row["total_counties"],
        "avg_score": float(row["avg_score"]),
        "min_score": float(row["min_score"]),
        "max_score": float(row["max_score"]),
    }


# ---------------------------------------------------------------------------
# CLI runner (used by wellnest-api console_script)
# ---------------------------------------------------------------------------


def run() -> None:
    """Entry point for `wellnest-api` CLI command."""
    import uvicorn

    s = get_settings()
    uvicorn.run(
        "api.main:app",
        host=s.api_host,
        port=s.api_port,
        reload=s.api_reload,
        log_level=s.api_log_level,
    )


if __name__ == "__main__":
    run()
