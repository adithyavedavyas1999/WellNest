"""
Health check endpoint.

This is the first thing our monitoring hits every 30s. If the DB is down
we still return 200 but flag database as "unreachable" — lets the load
balancer know we're alive but degraded.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.config import Settings, get_settings
from api.dependencies import get_db
from api.models.common import HealthResponse

router = APIRouter(tags=["health"])

APP_VERSION = "0.1.0"


@router.get("/health", response_model=HealthResponse)
def health_check(
    settings: Settings = Depends(get_settings),
    db: Session = Depends(get_db),
) -> HealthResponse:
    db_status = "unknown"
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "unreachable"

    return HealthResponse(
        status="ok",
        version=APP_VERSION,
        environment=settings.environment,
        database=db_status,
        timestamp=datetime.now(timezone.utc),
    )
