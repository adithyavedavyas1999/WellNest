"""
Gold layer assets — final analytical models and the wellbeing score.

Gold models are the last stop before ML / reporting / dashboards.  They
represent fully denormalized, business-ready tables:

  - child_wellbeing_score: composite score per school (the centerpiece)
  - pillar scores: individual scores for each domain (education, health, etc.)
  - county/tract summaries: aggregated views for the community dashboard
  - ranking tables: relative positioning for comparison features

All of these are dbt models in the gold/ directory.  The wellbeing score
computation itself lives in dbt SQL because it's essentially a weighted
average of the pillar scores, and keeping it in SQL means the logic is
auditable through dbt docs without needing to read Python.
"""

from __future__ import annotations

import structlog
from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)

from orchestration.resources import DbtResource, PostgresResource

logger = structlog.get_logger(__name__)

GOLD_GROUP = "gold"
GOLD_TAGS = {"layer": "gold", "pipeline": "transformation"}


# ---------------------------------------------------------------------------
# Pillar scores
# ---------------------------------------------------------------------------


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["silver_school_profile", "silver_tract_indicators"],
    description=(
        "Education pillar score — academic proficiency, graduation rates, "
        "Title I status, and student-teacher ratios rolled into a 0-100 score."
    ),
)
def gold_education_pillar(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.pillar_education")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.pillar_education")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["silver_school_profile", "silver_tract_indicators"],
    description=(
        "Health pillar score — insurance coverage, HPSA designation, CDC PLACES "
        "health outcomes, and environmental health indicators."
    ),
)
def gold_health_pillar(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.pillar_health")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.pillar_health")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["silver_tract_indicators", "silver_county_safety"],
    description=(
        "Environment pillar score — air quality (EPA AQI), FEMA risk index, "
        "and environmental health tracking metrics."
    ),
)
def gold_environment_pillar(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.pillar_environment")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.pillar_environment")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["silver_county_safety"],
    description=(
        "Safety pillar score — crime rates (FBI UCR), FEMA hazard risk, "
        "and severe weather frequency."
    ),
)
def gold_safety_pillar(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.pillar_safety")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.pillar_safety")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["silver_tract_indicators"],
    description=(
        "Economic pillar score — poverty rate, median income, food access, "
        "and economic opportunity indicators."
    ),
)
def gold_economic_pillar(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.pillar_economic")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.pillar_economic")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


# ---------------------------------------------------------------------------
# Composite wellbeing score
# ---------------------------------------------------------------------------


@asset(
    group_name=GOLD_GROUP,
    tags={**GOLD_TAGS, "critical": "true"},
    deps=[
        "gold_education_pillar",
        "gold_health_pillar",
        "gold_environment_pillar",
        "gold_safety_pillar",
        "gold_economic_pillar",
    ],
    description=(
        "THE score.  Weighted composite of all five pillar scores, normalized "
        "to 0-100.  One row per school.  This drives the main dashboard view "
        "and the community ranking features."
    ),
)
def gold_child_wellbeing_score(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.child_wellbeing_score")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.child_wellbeing_score")
    context.log.info(f"Wellbeing scores computed for {row_count} schools")

    stats = _score_distribution(postgres)

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
            "mean_score": MetadataValue.float(stats.get("mean", 0.0)),
            "median_score": MetadataValue.float(stats.get("median", 0.0)),
            "min_score": MetadataValue.float(stats.get("min", 0.0)),
            "max_score": MetadataValue.float(stats.get("max", 0.0)),
        },
    )


# ---------------------------------------------------------------------------
# Summary / ranking tables
# ---------------------------------------------------------------------------


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["gold_child_wellbeing_score"],
    description=(
        "County-level summary: avg/median/p10/p90 wellbeing scores, school counts, "
        "and pillar breakdowns.  Powers the county comparison feature."
    ),
)
def gold_county_summary(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.county_summary")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.county_summary")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["gold_child_wellbeing_score"],
    description=(
        "State-level rankings — states ordered by average wellbeing score.  "
        "Used in the national overview page and the PDF report generator."
    ),
)
def gold_state_ranking(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.state_ranking")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.state_ranking")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=GOLD_GROUP,
    tags=GOLD_TAGS,
    deps=["gold_child_wellbeing_score"],
    description=(
        "Tract-level aggregated wellbeing indicators for the map layer.  "
        "Includes H3 hex IDs for spatial aggregation on the dashboard."
    ),
)
def gold_tract_summary(
    context,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="gold.tract_summary")
    result.raise_on_failure()

    row_count = _try_count(postgres, "gold.tract_summary")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _try_count(pg: PostgresResource, table: str) -> int:
    try:
        return pg.get_row_count(table)
    except Exception:
        return -1


def _score_distribution(pg: PostgresResource) -> dict[str, float]:
    """Pull basic stats on the wellbeing score distribution.

    Useful for quick sanity checks — if the mean suddenly drops by 20 points
    we probably broke something upstream.
    """
    try:
        engine = pg.get_engine()
        import sqlalchemy

        with engine.connect() as conn:
            row = conn.execute(
                sqlalchemy.text("""
                    SELECT
                        avg(wellbeing_score)::float AS mean,
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY wellbeing_score)::float AS median,
                        min(wellbeing_score)::float AS min,
                        max(wellbeing_score)::float AS max
                    FROM gold.child_wellbeing_score
                    WHERE wellbeing_score IS NOT NULL
                """)
            ).fetchone()

        if row:
            return {
                "mean": round(row[0] or 0.0, 2),
                "median": round(row[1] or 0.0, 2),
                "min": round(row[2] or 0.0, 2),
                "max": round(row[3] or 0.0, 2),
            }
    except Exception as e:
        logger.warning("score_distribution_failed", error=str(e))

    return {}


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_GOLD_ASSETS: list = [
    gold_education_pillar,
    gold_health_pillar,
    gold_environment_pillar,
    gold_safety_pillar,
    gold_economic_pillar,
    gold_child_wellbeing_score,
    gold_county_summary,
    gold_state_ranking,
    gold_tract_summary,
]
