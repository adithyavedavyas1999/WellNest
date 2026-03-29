"""
Bronze (raw) layer assets — one per data source.

Each asset wraps an ingestion connector and writes raw data to the `raw`
schema in Postgres.  The connectors handle the actual download/parse/validate
cycle; we're just providing the dagster scheduling envelope and metadata.

Partitioning strategy:
  - Year-partitioned: CCD, Census ACS, EPA AirNow, FBI UCR (these have
    annual releases and we want to backfill historical years)
  - Unpartitioned: HRSA, USDA, FEMA, CDC, NCES EDGE (single-snapshot datasets
    that get replaced each run)
  - Daily-granularity: NOAA NWS alerts (ephemeral, appended each run)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    StaticPartitionsDefinition,
    asset,
)

from orchestration.resources import HttpClientResource, PostgresResource, WellNestConfig

logger = structlog.get_logger(__name__)

# year partitions for sources that publish annual releases
SURVEY_YEAR_PARTITIONS = StaticPartitionsDefinition(
    ["2020-21", "2021-22", "2022-23"]
)
CALENDAR_YEAR_PARTITIONS = StaticPartitionsDefinition(
    ["2020", "2021", "2022", "2023"]
)

BRONZE_GROUP = "bronze"
BRONZE_TAGS = {"layer": "bronze", "pipeline": "ingestion"}


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bronze_metadata(row_count: int, source: str, **extra: Any) -> dict[str, Any]:
    """Standard metadata attached to every bronze materialization."""
    md: dict[str, Any] = {
        "row_count": row_count,
        "source": source,
        "ingested_at": MetadataValue.text(_run_timestamp()),
    }
    md.update(extra)
    return md


# ---------------------------------------------------------------------------
# NCES Common Core of Data (school directory)
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    partitions_def=SURVEY_YEAR_PARTITIONS,
    description=(
        "School-level directory from NCES CCD.  ~100K rows per year.  "
        "Partitioned by survey year (e.g. 2022-23)."
    ),
    metadata={"source_url": "https://nces.ed.gov/ccd/files.asp"},
)
def bronze_nces_ccd(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.nces_ccd import NCESCCDConnector

    year = context.partition_key
    context.log.info(f"Ingesting NCES CCD for survey year {year}")

    connector = NCESCCDConnector(survey_year=year)
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "nces_ccd", survey_year=year),
    )


# ---------------------------------------------------------------------------
# NCES EDGE (geocoded school locations)
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "Geocoded school coordinates from NCES EDGE.  Joined to CCD by "
        "NCESSCH downstream in the silver layer."
    ),
)
def bronze_nces_edge(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.nces_edge import NCESEdgeConnector

    connector = NCESEdgeConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "nces_edge"),
    )


# ---------------------------------------------------------------------------
# CDC PLACES (health indicators by county/tract)
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "CDC PLACES health measure estimates at county and tract level.  "
        "~800K tract rows + ~90K county rows.  Socrata-based API, no auth."
    ),
)
def bronze_cdc_places(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.cdc_places import CDCPlacesConnector

    connector = CDCPlacesConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "cdc_places"),
    )


# ---------------------------------------------------------------------------
# CDC Environmental Health Tracking
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "CDC Environmental Health Tracking Network — childhood lead levels, "
        "asthma ED visits, environmental exposures by county."
    ),
)
def bronze_cdc_env_health(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.cdc_env_health import CDCEnvHealthConnector

    connector = CDCEnvHealthConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "cdc_env_health"),
    )


# ---------------------------------------------------------------------------
# Census ACS 5-year estimates
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    partitions_def=CALENDAR_YEAR_PARTITIONS,
    description=(
        "Census ACS 5-year tract-level estimates — poverty, insurance, "
        "education, income, demographics.  Partitioned by end year."
    ),
    metadata={"source_url": "https://api.census.gov/data.html"},
)
def bronze_census_acs(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.census_acs import CensusACSConnector

    year = int(context.partition_key)
    context.log.info(f"Ingesting Census ACS 5-year estimates for {year}")

    connector = CensusACSConnector(year=year)
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "census_acs", year=year),
    )


# ---------------------------------------------------------------------------
# EPA AirNow / AQS air quality
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "EPA AQS annual county-level AQI summaries.  Multi-year pull "
        "(2020-2022 by default).  Used for Environment pillar scoring."
    ),
)
def bronze_epa_airnow(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.epa_airnow import EPAAirNowConnector

    connector = EPAAirNowConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "epa_airnow"),
    )


# ---------------------------------------------------------------------------
# HRSA Health Professional Shortage Areas
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "HRSA HPSA designations — primary care, dental, and mental health "
        "shortage areas.  HPSA score is a key input for the Health & Resources pillar."
    ),
)
def bronze_hrsa_hpsa(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.hrsa_hpsa import HRSAHPSAConnector

    connector = HRSAHPSAConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "hrsa_hpsa"),
    )


# ---------------------------------------------------------------------------
# HRSA Medically Underserved Areas
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "HRSA MUA/MUP designations.  IMU score <= 62 qualifies as medically "
        "underserved.  Joined to tracts in silver layer."
    ),
)
def bronze_hrsa_mua(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.hrsa_mua import HRSAMUAConnector

    connector = HRSAMUAConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "hrsa_mua"),
    )


# ---------------------------------------------------------------------------
# USDA Food Access Research Atlas
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "USDA Food Access Research Atlas — tract-level food desert indicators.  "
        "~73K rows.  Key input for the Nutrition & Food Security pillar."
    ),
)
def bronze_usda_food_access(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.usda_food_access import USDAFoodAccessConnector

    connector = USDAFoodAccessConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "usda_food_access"),
    )


# ---------------------------------------------------------------------------
# FEMA National Risk Index
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    description=(
        "FEMA NRI county-level hazard risk scores.  ~3200 rows covering all "
        "US counties.  Used in Environment and Safety pillars."
    ),
)
def bronze_fema_nri(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.fema_nri import FEMANRIConnector

    connector = FEMANRIConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "fema_nri"),
    )


# ---------------------------------------------------------------------------
# NOAA NWS weather alerts
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags={**BRONZE_TAGS, "refresh": "daily"},
    description=(
        "Active weather alerts from NWS.  Ephemeral data — alerts expire, so we "
        "snapshot and append.  Run daily (or more often during severe weather season)."
    ),
)
def bronze_noaa_nws_alerts(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.noaa_nws_alerts import NOAANWSAlertsConnector

    connector = NOAANWSAlertsConnector()
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(
            row_count,
            "noaa_nws_alerts",
            note="appended (not replaced) — alerts are time-series",
        ),
    )


# ---------------------------------------------------------------------------
# FBI UCR / Crime Data Explorer
# ---------------------------------------------------------------------------

@asset(
    group_name=BRONZE_GROUP,
    tags=BRONZE_TAGS,
    partitions_def=CALENDAR_YEAR_PARTITIONS,
    description=(
        "FBI UCR county-level crime statistics.  Partitioned by year.  "
        "Note: NIBRS transition in 2021 means coverage drops significantly."
    ),
)
def bronze_fbi_ucr(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    from ingestion.sources.fbi_ucr import FBIUCRConnector

    year = int(context.partition_key)
    context.log.info(f"Ingesting FBI UCR data for {year}")

    connector = FBIUCRConnector(year=year)
    row_count = connector.run()

    return MaterializeResult(
        metadata=_bronze_metadata(row_count, "fbi_ucr", year=year),
    )


# ---------------------------------------------------------------------------
# convenience list for the Definitions object
# ---------------------------------------------------------------------------

ALL_BRONZE_ASSETS: list = [
    bronze_nces_ccd,
    bronze_nces_edge,
    bronze_cdc_places,
    bronze_cdc_env_health,
    bronze_census_acs,
    bronze_epa_airnow,
    bronze_hrsa_hpsa,
    bronze_hrsa_mua,
    bronze_usda_food_access,
    bronze_fema_nri,
    bronze_noaa_nws_alerts,
    bronze_fbi_ucr,
]
