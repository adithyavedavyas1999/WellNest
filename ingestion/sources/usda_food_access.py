"""
USDA Food Access Research Atlas connector.

The Food Access Research Atlas provides census-tract-level indicators of
food access, including low-income / low-access flags at different distance
thresholds (1 mile urban, 10 miles rural).

The dataset is published as an Excel workbook with multiple sheets.  We
only need the main "Food Access Research Atlas" sheet which has one row
per census tract (~73K rows).

Quirks:
  - The Excel file is ~15MB.  Polars can read it but it takes a while.
  - Column names have changed between releases.  The 2019 version uses
    "LILATracts_1And10" but older ones used "lalowi1_10".  We handle both.
  - The "Urban" column is 1/0 but stored as float in some releases.
  - Some tracts appear multiple times due to an ERS processing bug that
    was fixed in the 2019 release -- deduplicate on CensusTract.
  - The tract FIPS in "CensusTract" is a 11-digit string but sometimes
    gets read as float64 by Excel readers, which truncates it to something
    like 1.7031e+10.  We have to be careful about type casting.
  - "PovertyRate" can be >100 in a few tracts where the denominator (total
    population) was revised after the poverty estimate was calculated.
    We cap at 100.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

# the USDA publishes the atlas as an Excel file
FOOD_ACCESS_URL = (
    "https://www.ers.usda.gov/webdocs/DataFiles/80591/FoodAccessResearchAtlasData2019.xlsx"
)

# TODO: check if the 2022 update is published yet -- as of mid-2024 the
# 2019 data is still the latest available


class FoodAccessRecord(BaseModel):
    """One census tract from the food access atlas."""

    census_tract: str = Field(..., min_length=11, max_length=11)
    state: str | None = None
    county: str | None = None
    urban: bool | None = None
    poverty_rate: float | None = None
    median_family_income: float | None = None
    low_access_1: bool = False  # low access at 1 mile (urban) / 10 mile (rural)
    low_access_half: bool = False  # low access at 0.5 mile (urban) / 10 mile (rural)
    low_income: bool = False
    lila_1_and_10: bool = False  # low income AND low access (1/10)
    lila_half_and_10: bool = False
    total_pop: int | None = None
    kids_pop: int | None = None
    seniors_pop: int | None = None

    @field_validator("poverty_rate")
    @classmethod
    def cap_poverty(cls, v):
        if v is not None and v > 100:
            return 100.0
        return v


class USDAFoodAccessConnector:
    """Ingests the USDA Food Access Research Atlas."""

    def __init__(self, data_dir: str | None = None, url: str | None = None):
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_food_"))
        self.url = url or FOOD_ACCESS_URL
        self.http = WellNestHTTPClient(rate_limit=1.0, timeout=180)

    def extract(self) -> pl.DataFrame:
        dest = self.data_dir / "food_access_atlas.xlsx"
        logger.info("food_access_downloading", url=self.url)

        self.http.download_file(self.url, dest)

        # the main data is on the first sheet
        df = pl.read_excel(
            dest,
            sheet_name="Food Access Research Atlas",
        )
        logger.info("food_access_raw", rows=len(df), cols=len(df.columns))
        return df

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        # normalize column names
        col_map = {}
        for col in df.columns:
            col_map[col] = col.strip()
        df = df.rename(col_map)

        # the CensusTract column sometimes gets read as float
        # if it looks like 1.7031e+10, convert properly
        if "CensusTract" in df.columns:
            df = df.with_columns(
                pl.col("CensusTract")
                .cast(pl.Float64, strict=False)
                .cast(pl.Int64, strict=False)
                .cast(pl.Utf8)
                .str.zfill(11)
                .alias("census_tract")
            )
        elif "censusTract" in df.columns:
            df = df.rename({"censusTract": "CensusTract"})
            df = df.with_columns(
                pl.col("CensusTract").cast(pl.Utf8).str.zfill(11).alias("census_tract")
            )

        # map known columns
        renames = {
            "State": "state",
            "County": "county",
            "Urban": "urban_raw",
            "PovertyRate": "poverty_rate",
            "MedianFamilyIncome": "median_family_income",
            "LA1and10": "low_access_1",
            "LAhalfand10": "low_access_half",
            "LowIncomeTracts": "low_income",
            "LILATracts_1And10": "lila_1_and_10",
            "LILATracts_halfAnd10": "lila_half_and_10",
            "Pop2010": "total_pop",
            "TractKids": "kids_pop",
            "TractSeniors": "seniors_pop",
            # older column names
            "lalowi1_10": "lila_1_and_10",
            "lalowihalfshare": "lila_half_and_10",
        }
        existing_renames = {k: v for k, v in renames.items() if k in df.columns}
        if existing_renames:
            df = df.rename(existing_renames)

        # select columns we need
        keep = [
            c
            for c in [
                "census_tract",
                "state",
                "county",
                "urban_raw",
                "poverty_rate",
                "median_family_income",
                "low_access_1",
                "low_access_half",
                "low_income",
                "lila_1_and_10",
                "lila_half_and_10",
                "total_pop",
                "kids_pop",
                "seniors_pop",
            ]
            if c in df.columns
        ]
        df = df.select(keep)

        # convert urban flag (stored as 1.0/0.0 float in some releases)
        if "urban_raw" in df.columns:
            df = df.with_columns(
                pl.col("urban_raw").cast(pl.Float64, strict=False).eq(1.0).alias("urban")
            ).drop("urban_raw")

        # convert flag columns to boolean
        flag_cols = [
            "low_access_1",
            "low_access_half",
            "low_income",
            "lila_1_and_10",
            "lila_half_and_10",
        ]
        for col in flag_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False).eq(1.0).alias(col))

        # cast numeric columns
        if "poverty_rate" in df.columns:
            df = df.with_columns(pl.col("poverty_rate").cast(pl.Float64, strict=False))
            # cap poverty rate at 100 (see module docstring)
            df = df.with_columns(
                pl.when(pl.col("poverty_rate") > 100)
                .then(100.0)
                .otherwise(pl.col("poverty_rate"))
                .alias("poverty_rate")
            )
        if "median_family_income" in df.columns:
            df = df.with_columns(pl.col("median_family_income").cast(pl.Float64, strict=False))

        for col in ["total_pop", "kids_pop", "seniors_pop"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

        # deduplicate by tract
        if "census_tract" in df.columns:
            before = len(df)
            df = df.unique(subset=["census_tract"], keep="first")
            dupes = before - len(df)
            if dupes > 0:
                logger.info("food_access_deduped", removed=dupes)

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                FoodAccessRecord(**row)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("food_access_validation_dropped", count=bad)

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.usda_food_access"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("food_access_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("food_access_pipeline_done", rows=count)
        return count
