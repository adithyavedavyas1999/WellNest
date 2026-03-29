"""
NCES Common Core of Data (CCD) connector.

Downloads the annual school directory CSV from the National Center for
Education Statistics.  One row per public school in the US (~100K rows).

Quirks we've hit:
  - Column names change between release years.  The 2021-22 file uses
    "SCH_NAME" but 2019-20 used "SCHNAM".  We remap in _normalize_columns().
  - Free/reduced lunch counts switched from "TOTFRL" to "FRL" around 2020.
  - Files come as zipped CSVs.  Older ones are sometimes double-zipped.
  - Some state-level records sneak in (school type 4/5) -- we filter to
    regular, special-ed, vocational, and alternative schools.
  - FIPS codes show up as integers, stripping the leading zero for states
    like Connecticut (09).
"""

from __future__ import annotations

import os
import tempfile
import zipfile
from pathlib import Path

import polars as pl
import structlog
from pydantic import BaseModel, Field, field_validator

from ingestion.utils import (
    WellNestHTTPClient,
    ensure_schema,
    get_pg_url,
    normalize_fips_column,
)

logger = structlog.get_logger(__name__)

# NCES publishes the flat-file directory downloads here.  The URL includes
# the survey year and a version suffix that changes with each revision.
# We pin to the "1a" (first final) release.
CCD_BASE_URL = "https://nces.ed.gov/ccd/data/zip"

# mapping of survey year -> filename (without .zip)
# these have to be updated manually each year, which is annoying
CCD_FILES = {
    "2022-23": "ccd_sch_029_2223_w_1a_071824",
    "2021-22": "ccd_sch_029_2122_w_1a_080621",
    "2020-21": "ccd_sch_029_2021_w_1a_080321",
}

DEFAULT_YEAR = "2022-23"

# we only keep schools of these types
KEEP_SCHOOL_TYPES = {1, 2, 3, 4}  # regular, special-ed, vocational, alternative


# ------------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------------

class CCDSchoolRecord(BaseModel):
    """Validated representation of a single CCD school row."""
    ncessch: str = Field(..., min_length=12, max_length=12)
    school_name: str
    lea_name: str | None = None
    state_abbr: str = Field(..., min_length=2, max_length=2)
    state_fips: str
    county_fips: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    school_type: int | None = None
    enrollment: int | None = None
    free_reduced_lunch: int | None = None
    title_i_status: str | None = None
    school_level: str | None = None

    @field_validator("state_fips")
    @classmethod
    def pad_state_fips(cls, v: str) -> str:
        return v.zfill(2)

    @field_validator("county_fips")
    @classmethod
    def pad_county_fips(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return v.zfill(5)


# ------------------------------------------------------------------
# Column name mapping
# ------------------------------------------------------------------

# maps the messy CCD headers to our internal names
_COLUMN_MAP = {
    "NCESSCH": "ncessch",
    "SCH_NAME": "school_name",
    "SCHNAM": "school_name",  # older years
    "LEA_NAME": "lea_name",
    "LEANM": "lea_name",
    "ST": "state_abbr",
    "STABR": "state_abbr",
    "FIPST": "state_fips",
    "STFIP": "state_fips",
    "CNTY": "county_fips",
    "CONUM": "county_fips",
    "LAT": "latitude",
    "LATCOD": "latitude",
    "LON": "longitude",
    "LONCOD": "longitude",
    "SCH_TYPE": "school_type",
    "SCHOOL_TYPE": "school_type",
    "TOTAL": "enrollment",
    "MEMBER": "enrollment",
    "TOTFRL": "free_reduced_lunch",
    "FRL": "free_reduced_lunch",
    "TITLEI": "title_i_status",
    "TITLEI_STATUS": "title_i_status",
    "LEVEL": "school_level",
    "SCH_TYPE_TEXT": "school_type_text",
    "SCHOOL_LEVEL": "school_level",
}


class NCESCCDConnector:
    """Extract-validate-load pipeline for NCES CCD directory files."""

    def __init__(
        self,
        survey_year: str = DEFAULT_YEAR,
        data_dir: str | None = None,
    ):
        self.survey_year = survey_year
        self.data_dir = Path(data_dir or tempfile.mkdtemp(prefix="wellnest_ccd_"))
        self.client = WellNestHTTPClient(rate_limit=1.0, timeout=120)

    def _build_url(self) -> str:
        filename = CCD_FILES.get(self.survey_year)
        if not filename:
            raise ValueError(
                f"No CCD file mapping for year {self.survey_year}. "
                f"Known years: {list(CCD_FILES.keys())}"
            )
        return f"{CCD_BASE_URL}/{filename}.zip"

    def extract(self) -> pl.DataFrame:
        """Download and parse the CCD directory CSV."""
        url = self._build_url()
        zip_path = self.data_dir / f"ccd_{self.survey_year}.zip"

        logger.info("ccd_extract_start", year=self.survey_year, url=url)
        self.client.download_file(url, zip_path)

        csv_path = self._unzip(zip_path)
        df = pl.read_csv(
            csv_path,
            infer_schema_length=5000,
            ignore_errors=True,
            encoding="utf8-lossy",
        )
        logger.info("ccd_raw_rows", count=len(df), columns=df.columns[:10])
        return df

    def _unzip(self, zip_path: Path) -> Path:
        """Extract the CSV from the zip archive.

        NCES zips always contain exactly one CSV, but the name isn't always
        predictable so we just grab the first .csv we find.
        """
        with zipfile.ZipFile(zip_path, "r") as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise FileNotFoundError(f"No CSV found in {zip_path}")
            target = csv_names[0]
            zf.extract(target, self.data_dir)
            return self.data_dir / target

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Rename columns to our standard names using the mapping table."""
        upper_cols = {c: c.upper().strip() for c in df.columns}
        df = df.rename({orig: upper_cols[orig] for orig in df.columns})

        rename_map = {}
        for col in df.columns:
            if col in _COLUMN_MAP:
                rename_map[col] = _COLUMN_MAP[col]
        return df.rename(rename_map)

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean and reshape the raw CCD data."""
        df = self._normalize_columns(df)

        # keep only the columns we care about
        keep = [c for c in [
            "ncessch", "school_name", "lea_name", "state_abbr", "state_fips",
            "county_fips", "latitude", "longitude", "school_type", "enrollment",
            "free_reduced_lunch", "title_i_status", "school_level",
        ] if c in df.columns]
        df = df.select(keep)

        # cast types -- CCD loves mixing int/string in the same column
        if "ncessch" in df.columns:
            df = df.with_columns(pl.col("ncessch").cast(pl.Utf8).str.zfill(12))
        if "state_fips" in df.columns:
            df = normalize_fips_column(df, "state_fips", width=2)
        if "county_fips" in df.columns:
            df = normalize_fips_column(df, "county_fips", width=5)
        if "latitude" in df.columns:
            df = df.with_columns(pl.col("latitude").cast(pl.Float64, strict=False))
        if "longitude" in df.columns:
            df = df.with_columns(pl.col("longitude").cast(pl.Float64, strict=False))
        if "enrollment" in df.columns:
            df = df.with_columns(pl.col("enrollment").cast(pl.Int64, strict=False))
        if "free_reduced_lunch" in df.columns:
            df = df.with_columns(pl.col("free_reduced_lunch").cast(pl.Int64, strict=False))

        # filter to actual schools (not state/territory summary rows)
        if "school_type" in df.columns:
            df = df.with_columns(pl.col("school_type").cast(pl.Int32, strict=False))
            df = df.filter(pl.col("school_type").is_in(list(KEEP_SCHOOL_TYPES)))

        # CCD uses negative sentinel values for suppressed/missing data
        # -1 = "not applicable", -2 = "not available", -9 = "missing"
        for col in ["enrollment", "free_reduced_lunch"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(col) < 0).then(None).otherwise(pl.col(col)).alias(col)
                )

        return df

    def validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate rows against the Pydantic model, dropping bad records."""
        valid_rows = []
        bad_count = 0

        for row in df.iter_rows(named=True):
            try:
                CCDSchoolRecord(**row)
                valid_rows.append(row)
            except Exception:
                bad_count += 1

        if bad_count:
            logger.warning("ccd_validation_dropped", bad_rows=bad_count, kept=len(valid_rows))

        return pl.DataFrame(valid_rows, schema=df.schema) if valid_rows else df.clear()

    def load(self, df: pl.DataFrame) -> int:
        """Write to raw.nces_ccd_directory in Postgres."""
        ensure_schema("raw")
        table = "raw.nces_ccd_directory"
        logger.info("ccd_load_start", table=table, rows=len(df))

        df.write_database(
            table_name=table,
            connection=get_pg_url(),
            if_table_exists="replace",
            engine="sqlalchemy",
        )

        logger.info("ccd_load_complete", table=table, rows=len(df))
        return len(df)

    def run(self) -> int:
        """Full EtVL pipeline."""
        raw = self.extract()
        cleaned = self.transform(raw)
        validated = self.validate(cleaned)
        count = self.load(validated)
        logger.info("ccd_pipeline_done", year=self.survey_year, final_rows=count)
        self.client.close()
        return count
