"""
Silver layer assets — dbt staging and silver models.

These depend on the bronze assets and run dbt selectors against the
staging/ and silver/ model directories.  The split between staging and
silver is a convention from the dbt style guide:

  staging = 1:1 with source, renamed + retyped, minimal logic
  silver  = joined/enriched, business-level entities

We trigger dbt via CLI (see resources.DbtResource) rather than using
dagster-dbt's @dbt_assets because our dbt project isn't set up with
the manifest approach yet.  TODO: migrate to @dbt_assets once we
stabilize the dbt project structure.
"""

from __future__ import annotations

from typing import Any

import structlog
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MaterializeResult,
    MetadataValue,
    asset,
)

from orchestration.resources import DbtResource, PostgresResource

logger = structlog.get_logger(__name__)

SILVER_GROUP = "silver"
SILVER_TAGS = {"layer": "silver", "pipeline": "transformation"}


# ---------------------------------------------------------------------------
# Staging models — 1:1 renames of raw tables
# ---------------------------------------------------------------------------

@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=[
        "bronze_nces_ccd",
        "bronze_nces_edge",
    ],
    description=(
        "dbt staging models for education sources (NCES CCD + EDGE).  "
        "Renames columns, casts types, deduplicates."
    ),
)
def stg_education(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_nces_ccd staging.stg_nces_edge")
    result.raise_on_failure()

    row_count = _try_count(postgres, "staging.stg_nces_ccd")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "stg_nces_ccd_rows": row_count,
            "dbt_stdout": MetadataValue.text(result.stdout[-2000:]),
        },
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=[
        "bronze_cdc_places",
        "bronze_cdc_env_health",
    ],
    description="dbt staging models for CDC health indicator sources.",
)
def stg_health(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_cdc_places staging.stg_cdc_env_health")
    result.raise_on_failure()

    row_count = _try_count(postgres, "staging.stg_cdc_places")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "stg_cdc_places_rows": row_count,
        },
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["bronze_census_acs"],
    description="dbt staging model for Census ACS tract-level demographics.",
)
def stg_demographics(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_census_acs")
    result.raise_on_failure()

    row_count = _try_count(postgres, "staging.stg_census_acs")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "stg_census_acs_rows": row_count,
        },
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["bronze_epa_airnow", "bronze_fema_nri"],
    description="dbt staging models for environmental data (EPA air quality + FEMA NRI).",
)
def stg_environment(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_epa_aqi staging.stg_fema_nri")
    result.raise_on_failure()

    return MaterializeResult(
        metadata={"dbt_models_run": result.model_count},
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["bronze_hrsa_hpsa", "bronze_hrsa_mua"],
    description="dbt staging models for HRSA provider shortage designations.",
)
def stg_healthcare_access(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_hrsa_hpsa staging.stg_hrsa_mua")
    result.raise_on_failure()

    return MaterializeResult(
        metadata={"dbt_models_run": result.model_count},
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["bronze_usda_food_access"],
    description="dbt staging model for USDA food access / food desert indicators.",
)
def stg_food_access(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_usda_food_access")
    result.raise_on_failure()

    row_count = _try_count(postgres, "staging.stg_usda_food_access")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "stg_usda_food_access_rows": row_count,
        },
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["bronze_noaa_nws_alerts"],
    description="dbt staging model for NOAA weather alerts.",
)
def stg_weather_alerts(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_noaa_alerts")
    result.raise_on_failure()

    return MaterializeResult(
        metadata={"dbt_models_run": result.model_count},
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["bronze_fbi_ucr"],
    description="dbt staging model for FBI UCR crime statistics.",
)
def stg_crime(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="staging.stg_fbi_ucr")
    result.raise_on_failure()

    return MaterializeResult(
        metadata={"dbt_models_run": result.model_count},
    )


# ---------------------------------------------------------------------------
# Silver models — joined + enriched entities
# ---------------------------------------------------------------------------

@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=[
        "stg_education",
        "stg_health",
        "stg_demographics",
        "stg_environment",
        "stg_healthcare_access",
        "stg_food_access",
        "stg_crime",
    ],
    description=(
        "Silver layer school profile: CCD + EDGE joined, enriched with "
        "tract-level demographics and health indicators.  This is the main "
        "entity table for downstream scoring."
    ),
)
def silver_school_profile(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="silver.school_profile")
    result.raise_on_failure()

    row_count = _try_count(postgres, "silver.school_profile")
    context.log.info(f"silver_school_profile materialized with {row_count} rows")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["stg_demographics", "stg_health", "stg_environment", "stg_food_access"],
    description=(
        "Silver tract-level community indicators — demographics, health, "
        "environment, and food access merged by census tract FIPS."
    ),
)
def silver_tract_indicators(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="silver.tract_indicators")
    result.raise_on_failure()

    row_count = _try_count(postgres, "silver.tract_indicators")

    return MaterializeResult(
        metadata={
            "dbt_models_run": result.model_count,
            "row_count": row_count,
        },
    )


@asset(
    group_name=SILVER_GROUP,
    tags=SILVER_TAGS,
    deps=["stg_environment", "stg_crime", "stg_healthcare_access"],
    description="Silver county-level safety and access indicators.",
)
def silver_county_safety(
    context: AssetExecutionContext,
    dbt: DbtResource,
    postgres: PostgresResource,
) -> MaterializeResult:
    result = dbt.run(select="silver.county_safety")
    result.raise_on_failure()

    row_count = _try_count(postgres, "silver.county_safety")

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
    """Try to get a row count; return -1 if the table doesn't exist yet.

    During the initial run before dbt has created any tables, these counts
    will fail.  That's fine — the metadata is informational, not critical.
    """
    try:
        return pg.get_row_count(table)
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_SILVER_ASSETS: list = [
    stg_education,
    stg_health,
    stg_demographics,
    stg_environment,
    stg_healthcare_access,
    stg_food_access,
    stg_weather_alerts,
    stg_crime,
    silver_school_profile,
    silver_tract_indicators,
    silver_county_safety,
]
