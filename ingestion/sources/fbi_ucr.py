"""
FBI Uniform Crime Reporting (UCR) / Crime Data Explorer connector.

Downloads county-level crime statistics from the FBI's Crime Data Explorer.
The UCR program transitioned from the legacy Summary Reporting System to
NIBRS (National Incident-Based Reporting System) starting in 2021, which
means the data format changed significantly.

We use the pre-built annual county-level CSV files from the Crime Data
Explorer's bulk download page.

Quirks:
  - The transition to NIBRS in 2021 caused a massive drop in reporting
    coverage -- many agencies hadn't converted yet.  2019 and 2020 data
    is more complete for national analysis.
  - County-level data has "estimated" totals that the FBI imputes for
    agencies that didn't report.  These estimates are flagged but the
    flag column name varies by year.
  - Some counties report zero crimes, which is different from not reporting
    at all.  A missing row means no data, a row with 0 means they reported
    zero incidents (which for violent crime is plausible in very small
    counties).
  - FIPS codes in the FBI data are called "FIPS_STATE" and "FIPS_COUNTY"
    and stored as integers.
  - "Violent crime" in FBI terms = murder + rape + robbery + aggravated
    assault.  "Property crime" = burglary + larceny + motor vehicle theft.
  - Population figures in the FBI data come from the Census Bureau but
    can differ from ACS estimates because they use different base years.
  - The CSV encoding is Windows-1252, not UTF-8, because government.
"""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import WellNestHTTPClient, ensure_schema, get_pg_url

logger = structlog.get_logger(__name__)

# FBI Crime Data Explorer bulk downloads
# the "estimated" file has imputed data for non-reporting agencies
CDE_DOWNLOAD_BASE = "https://cde.ucr.cjis.gov/LATEST/webapp/api/bulk-downloads"

# they also publish direct CSV links that are more stable
COUNTY_CRIME_URL_TEMPLATE = (
    "https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/"
    "estimated_crimes_{year}.csv"
)


class CountyCrimeRecord(BaseModel):
    """One county's crime data for a year."""
    state_fips: str = Field(..., min_length=2, max_length=2)
    county_fips: str = Field(..., min_length=5, max_length=5)
    state_name: str | None = None
    county_name: str | None = None
    year: int
    population: int | None = None
    violent_crime: int | None = None
    murder: int | None = None
    rape: int | None = None
    robbery: int | None = None
    aggravated_assault: int | None = None
    property_crime: int | None = None
    burglary: int | None = None
    larceny: int | None = None
    motor_vehicle_theft: int | None = None
    violent_crime_rate: float | None = None
    property_crime_rate: float | None = None

    @field_validator("state_fips")
    @classmethod
    def pad_state(cls, v: str) -> str:
        return v.zfill(2)

    @field_validator("county_fips")
    @classmethod
    def pad_county(cls, v: str) -> str:
        return v.zfill(5)


class FBIUCRConnector:
    """Ingests county-level crime data from the FBI Crime Data Explorer."""

    def __init__(
        self,
        years: list[int] | None = None,
        data_dir: str | None = None,
    ):
        # 2019 and 2020 have the best coverage pre-NIBRS transition
        self.years = years or [2019, 2020]
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_ucr_"))
        self.http = WellNestHTTPClient(rate_limit=1.0, timeout=120)

    def _build_url(self, year: int) -> str:
        return COUNTY_CRIME_URL_TEMPLATE.format(year=year)

    def extract(self) -> pl.DataFrame:
        """Download and concatenate crime data across years."""
        frames = []

        for yr in self.years:
            url = self._build_url(yr)
            dest = self.data_dir / f"estimated_crimes_{yr}.csv"
            logger.info("ucr_downloading", year=yr, url=url)

            try:
                self.http.download_file(url, dest)
            except Exception:
                # try alternate zip format
                logger.warning("ucr_csv_failed_trying_zip", year=yr)
                try:
                    zip_url = url.replace(".csv", ".zip")
                    zip_dest = self.data_dir / f"estimated_crimes_{yr}.zip"
                    self.http.download_file(zip_url, zip_dest)
                    with zipfile.ZipFile(zip_dest, "r") as zf:
                        csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
                        zf.extract(csv_name, self.data_dir)
                        dest = self.data_dir / csv_name
                except Exception:
                    logger.error("ucr_download_failed", year=yr)
                    continue

            # FBI CSVs are sometimes Windows-1252 encoded
            try:
                df = pl.read_csv(dest, infer_schema_length=5000, ignore_errors=True)
            except Exception:
                df = pl.read_csv(
                    dest,
                    infer_schema_length=5000,
                    ignore_errors=True,
                    encoding="utf8-lossy",
                )

            df = df.with_columns(pl.lit(yr).alias("year"))
            frames.append(df)
            logger.info("ucr_year_loaded", year=yr, rows=len(df))

        if not frames:
            return pl.DataFrame()

        return pl.concat(frames, how="diagonal")

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        # normalize column names
        col_map = {}
        for col in df.columns:
            col_map[col] = (
                col.strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )
        df = df.rename(col_map)

        # known column mappings (the FBI has used different names over the years)
        known = {
            "fips_state": "state_fips_raw",
            "state_fips": "state_fips_raw",
            "fips_county": "county_fips_raw",
            "county_fips": "county_fips_raw",
            "state_name": "state_name",
            "county_name": "county_name",
            "population": "population",
            "violent_crime": "violent_crime",
            "violent_crime_total": "violent_crime",
            "murder_and_nonnegligent_manslaughter": "murder",
            "murder": "murder",
            "rape_(revised_definition)": "rape",
            "rape": "rape",
            "robbery": "robbery",
            "aggravated_assault": "aggravated_assault",
            "property_crime": "property_crime",
            "property_crime_total": "property_crime",
            "burglary": "burglary",
            "larceny_theft": "larceny",
            "larceny": "larceny",
            "motor_vehicle_theft": "motor_vehicle_theft",
        }
        renames = {k: v for k, v in known.items() if k in df.columns and k != v}
        df = df.rename(renames)

        # build proper FIPS codes
        if "state_fips_raw" in df.columns:
            df = df.with_columns(
                pl.col("state_fips_raw").cast(pl.Utf8).str.zfill(2).alias("state_fips")
            )
        if "county_fips_raw" in df.columns and "state_fips" in df.columns:
            # FBI county FIPS is 3 digits relative to state
            df = df.with_columns(
                pl.col("county_fips_raw").cast(pl.Utf8).str.zfill(3).alias("county_code_3")
            )
            df = df.with_columns(
                (pl.col("state_fips") + pl.col("county_code_3")).alias("county_fips")
            )

        # cast crime counts to int
        crime_cols = [
            "violent_crime", "murder", "rape", "robbery",
            "aggravated_assault", "property_crime", "burglary",
            "larceny", "motor_vehicle_theft",
        ]
        for col in crime_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False))

        if "population" in df.columns:
            df = df.with_columns(pl.col("population").cast(pl.Int64, strict=False))

        # calculate per-capita rates (per 100,000 population)
        if "violent_crime" in df.columns and "population" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("population") > 0)
                .then((pl.col("violent_crime") / pl.col("population") * 100_000).round(1))
                .otherwise(None)
                .alias("violent_crime_rate")
            )

        if "property_crime" in df.columns and "population" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("population") > 0)
                .then((pl.col("property_crime") / pl.col("population") * 100_000).round(1))
                .otherwise(None)
                .alias("property_crime_rate")
            )

        if "year" in df.columns:
            df = df.with_columns(pl.col("year").cast(pl.Int32))

        # strip whitespace from names
        for col in ["state_name", "county_name"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).str.strip_chars())

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df

        good = []
        bad = 0
        for row in df.iter_rows(named=True):
            try:
                CountyCrimeRecord(**row)
                good.append(row)
            except Exception:
                bad += 1

        if bad:
            logger.warning("ucr_validation_dropped", count=bad, pct=round(bad / max(len(df), 1) * 100, 1))

        return pl.DataFrame(good, schema=df.schema) if good else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        if df.is_empty():
            return 0

        ensure_schema("raw")
        table = "raw.fbi_ucr_county"
        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )
        logger.info("ucr_loaded", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        self.http.close()
        logger.info("ucr_pipeline_done", years=self.years, rows=count)
        return count
