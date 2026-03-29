"""
Census Bureau American Community Survey (ACS) 5-year estimates connector.

Pulls tract-level demographic data from the Census API.  We use the 5-year
estimates because the 1-year product doesn't cover tracts (too small a sample).

API docs: https://api.census.gov/data/{year}/acs/acs5

Quirks that wasted real debugging time:
  - The API returns -666666666 for missing/suppressed values.  This is
    documented exactly nowhere in the official API docs -- found it in a
    Census Bureau forum post from 2014.
  - There's a hard limit of 50 variables per request.  We need ~25 so
    we're fine for now, but if someone adds more variables this'll break
    silently (the API just drops extras without an error).
  - You must request geography in a specific format:
      for=tract:*&in=state:17&in=county:031
    Putting county in the "for" clause instead of "in" returns 400.
  - The first row of the JSON response is the header row.  Every other
    Census API returns actual data on the first row.  Why.
  - Requesting too many states at once sometimes causes a timeout.
    We iterate state by state.
  - API key is free but required.  Without it you get 500 requests/day
    with basically no error message when you hit the limit.
"""

from __future__ import annotations

import os
import time

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import (
    WellNestHTTPClient,
    ensure_schema,
    format_fips,
    get_pg_url,
    retry_on_http_error,
)

logger = structlog.get_logger(__name__)

ACS_BASE = "https://api.census.gov/data"
CENSUS_MISSING_VALUE = -666666666  # the magic sentinel

# tables we pull and their variable codes
# naming comes from the Census variable naming scheme:
#   B17001_002E = estimate, B17001_002M = margin of error
ACS_VARIABLES = {
    # poverty
    "B17001_001E": "poverty_universe",
    "B17001_002E": "poverty_below_total",
    # health insurance (under 19)
    "B27001_001E": "insurance_universe",
    "B27001_005E": "uninsured_male_under6",
    "B27001_008E": "uninsured_male_6to18",
    "B27001_033E": "uninsured_female_under6",
    "B27001_036E": "uninsured_female_6to18",
    # education attainment (25+)
    "B15003_001E": "edu_universe",
    "B15003_017E": "edu_hs_diploma",
    "B15003_022E": "edu_bachelors",
    "B15003_023E": "edu_masters",
    "B15003_024E": "edu_professional",
    "B15003_025E": "edu_doctorate",
    # median household income
    "B19013_001E": "median_hh_income",
    # total population
    "B01003_001E": "total_population",
    # race/ethnicity (for demographic context)
    "B03002_001E": "race_universe",
    "B03002_003E": "race_white_alone",
    "B03002_004E": "race_black_alone",
    "B03002_012E": "race_hispanic",
}

# all 50 states + DC + PR (we skip island territories -- tiny sample sizes)
ALL_STATE_FIPS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56", "72",
]


class ACSRecord(BaseModel):
    """Validated ACS tract-level record."""
    state_fips: str = Field(..., min_length=2, max_length=2)
    county_fips: str = Field(..., min_length=3, max_length=3)
    tract_fips: str = Field(..., min_length=6, max_length=6)
    full_fips: str = Field(..., min_length=11, max_length=11)
    total_population: int | None = None
    poverty_rate: float | None = None
    median_hh_income: int | None = None
    uninsured_children: int | None = None
    pct_bachelors_plus: float | None = None

    @field_validator("poverty_rate", "pct_bachelors_plus")
    @classmethod
    def rate_bounds(cls, v):
        if v is not None and (v < 0 or v > 100):
            return None
        return v


class CensusACSConnector:
    """Pulls ACS 5-year tract-level data from the Census Bureau API."""

    def __init__(
        self,
        api_key: str | None = None,
        year: int = 2022,
        state_fips_list: list[str] | None = None,
    ):
        self.api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
        if not self.api_key:
            logger.warning(
                "census_no_api_key",
                msg="Running without API key -- limited to 500 requests/day",
            )
        self.year = year
        self.states = state_fips_list or ALL_STATE_FIPS
        # census bureau starts throttling hard above ~4 req/s
        self.http = WellNestHTTPClient(rate_limit=3.0, timeout=90)

    def _api_url(self) -> str:
        return f"{ACS_BASE}/{self.year}/acs/acs5"

    def _variable_list(self) -> str:
        return ",".join(ACS_VARIABLES.keys())

    @retry_on_http_error(max_attempts=3, min_wait=5)
    def _fetch_state_tracts(self, state_fips: str) -> list[list[str]]:
        """Pull all tract-level data for a single state.

        Returns the raw Census response which is a list of lists where
        the first element is the header row.
        """
        params = {
            "get": self._variable_list(),
            "for": "tract:*",
            "in": f"state:{state_fips}",
            "key": self.api_key,
        }

        data = self.http.get_json(self._api_url(), params=params)

        if not data or len(data) < 2:
            logger.warning("census_empty_response", state=state_fips)
            return []

        return data

    def extract(self) -> pl.DataFrame:
        """Iterate through states and build the full tract-level dataset.

        We go state by state because requesting all states at once causes
        the Census API to timeout roughly 40% of the time.
        """
        all_rows: list[dict] = []
        header: list[str] | None = None

        for i, st in enumerate(self.states):
            logger.info("census_fetching_state", state=st, progress=f"{i+1}/{len(self.states)}")

            try:
                result = self._fetch_state_tracts(st)
            except Exception:
                logger.error("census_state_failed", state=st)
                continue

            if not result:
                continue

            if header is None:
                header = result[0]

            for row in result[1:]:
                record = dict(zip(header, row))
                all_rows.append(record)

            # brief pause between states to be a good API citizen
            if i % 10 == 9:
                time.sleep(1)

        logger.info("census_extract_done", total_rows=len(all_rows))
        if not all_rows:
            return pl.DataFrame()

        return pl.DataFrame(all_rows)

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean Census data and compute derived metrics."""
        if df.is_empty():
            return df

        # rename variables to human-readable names
        renames = {k: v for k, v in ACS_VARIABLES.items() if k in df.columns}
        df = df.rename(renames)

        # the Census API returns everything as strings
        numeric_cols = [c for c in df.columns if c not in ("state", "county", "tract", "NAME")]
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        # replace the infamous -666666666 with null
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col) == CENSUS_MISSING_VALUE)
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )

        # build FIPS columns
        if "state" in df.columns and "county" in df.columns and "tract" in df.columns:
            df = df.with_columns([
                pl.col("state").cast(pl.Utf8).str.zfill(2).alias("state_fips"),
                pl.col("county").cast(pl.Utf8).str.zfill(3).alias("county_fips"),
                pl.col("tract").cast(pl.Utf8).str.zfill(6).alias("tract_fips"),
            ])
            df = df.with_columns(
                (pl.col("state_fips") + pl.col("county_fips") + pl.col("tract_fips")).alias("full_fips")
            )

        # compute derived measures
        if "poverty_universe" in df.columns and "poverty_below_total" in df.columns:
            df = df.with_columns(
                (pl.col("poverty_below_total") / pl.col("poverty_universe") * 100)
                .round(2)
                .alias("poverty_rate")
            )

        # uninsured children = sum of all the under-19 uninsured buckets
        uninsr_cols = [c for c in [
            "uninsured_male_under6", "uninsured_male_6to18",
            "uninsured_female_under6", "uninsured_female_6to18",
        ] if c in df.columns]
        if uninsr_cols:
            df = df.with_columns(
                pl.sum_horizontal(uninsr_cols).cast(pl.Int64).alias("uninsured_children")
            )

        # pct with bachelor's or higher
        edu_higher = [c for c in [
            "edu_bachelors", "edu_masters", "edu_professional", "edu_doctorate",
        ] if c in df.columns]
        if edu_higher and "edu_universe" in df.columns:
            df = df.with_columns(
                (pl.sum_horizontal(edu_higher) / pl.col("edu_universe") * 100)
                .round(2)
                .alias("pct_bachelors_plus")
            )

        # cast income to int
        if "median_hh_income" in df.columns:
            df = df.with_columns(pl.col("median_hh_income").cast(pl.Int64, strict=False))
        if "total_population" in df.columns:
            df = df.with_columns(pl.col("total_population").cast(pl.Int64, strict=False))

        # drop raw Census column names (state, county, tract are now redundant)
        drop = [c for c in ["state", "county", "tract", "NAME"] if c in df.columns]
        df = df.drop(drop)

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        required = {"state_fips", "county_fips", "tract_fips", "full_fips"}
        missing = required - set(df.columns)
        if missing:
            logger.error("census_missing_columns", missing=missing)
            return df.clear()

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                ACSRecord(**row)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("census_validation_dropped", count=bad)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.census_acs_tract"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("census_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("census_pipeline_done", rows=count)
        return count
