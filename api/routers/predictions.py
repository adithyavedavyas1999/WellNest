"""
Predictions endpoint — surfaces the XGBoost model output.

We store predictions in gold.school_predictions (written by the ml pipeline).
This endpoint is mostly used by the dashboard's "Trends" page to show
which schools the model thinks are headed for trouble.
"""

from __future__ import annotations

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.dependencies import PaginationParams, get_db
from api.models.common import PaginatedResponse
from api.models.school import SchoolPrediction

logger = logging.getLogger(__name__)
router = APIRouter(tags=["predictions"])


@router.get("/predictions", response_model=PaginatedResponse[SchoolPrediction])
def list_predictions(
    db: Session = Depends(get_db),
    pagination: PaginationParams = Depends(),
    state: Annotated[Optional[str], Query(max_length=2)] = None,
    risk_only: Annotated[bool, Query(description="Only show schools flagged as at-risk")] = False,
) -> PaginatedResponse[SchoolPrediction]:
    clauses: list[str] = []
    params: dict = {}

    if state:
        clauses.append("p.state = :state")
        params["state"] = state.upper()
    if risk_only:
        clauses.append("p.risk_flag = true")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    total = db.execute(
        text(f"SELECT count(*) FROM gold.school_predictions p {where}"), params
    ).scalar() or 0

    query = f"""
        SELECT
            p.nces_id,
            p.predicted_score_change,
            p.confidence_interval_low,
            p.confidence_interval_high,
            p.risk_flag,
            p.top_contributing_factors,
            p.model_version,
            p.predicted_at
        FROM gold.school_predictions p
        {where}
        ORDER BY p.predicted_score_change ASC
        LIMIT :limit OFFSET :offset
    """
    params["limit"] = pagination.per_page
    params["offset"] = pagination.offset

    rows = db.execute(text(query), params).mappings().all()

    items = []
    for r in rows:
        factors = r.get("top_contributing_factors") or []
        if isinstance(factors, str):
            factors = [f.strip() for f in factors.split(",")]

        items.append(SchoolPrediction(
            nces_id=r["nces_id"],
            predicted_score_change=r["predicted_score_change"],
            confidence_interval_low=r["confidence_interval_low"],
            confidence_interval_high=r["confidence_interval_high"],
            risk_flag=r["risk_flag"],
            top_contributing_factors=factors,
            model_version=r.get("model_version"),
            predicted_at=r.get("predicted_at"),
        ))

    return PaginatedResponse.build(items, total, pagination.page, pagination.per_page)
