"""
Dagster sensors for WellNest.

Three sensor types:
  1. File arrival — watches a drop directory for manually uploaded data files
     (CSV/Excel).  Some of our partner orgs can't do API integrations and
     just email us files, which an admin drops into the watched folder.
  2. Quality failure — checks the quality.check_results table for recent
     failures and creates alert notifications.
  3. Stale data — monitors bronze table timestamps and triggers re-ingestion
     if data is older than expected.

Sensor evaluation intervals are set conservatively.  The file sensor checks
every 30 seconds; the quality and staleness sensors check every 5 minutes.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    RunConfig,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    sensor,
)

logger = structlog.get_logger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# where manually-uploaded data files get dropped
# configurable via env var so it works in both local dev and deployed envs
DATA_DROP_DIR = Path(os.environ.get("DATA_DROP_DIR", str(PROJECT_ROOT / "data" / "incoming")))

# file extensions we'll pick up
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".parquet"}

# maps filename patterns to the bronze asset they should trigger
# the admin names files like "nces_ccd_2023.csv" or "fema_nri_update.xlsx"
SOURCE_FILE_PATTERNS: dict[str, str] = {
    "nces_ccd": "bronze_nces_ccd",
    "nces_edge": "bronze_nces_edge",
    "cdc_places": "bronze_cdc_places",
    "cdc_env": "bronze_cdc_env_health",
    "census_acs": "bronze_census_acs",
    "epa_air": "bronze_epa_airnow",
    "hrsa_hpsa": "bronze_hrsa_hpsa",
    "hrsa_mua": "bronze_hrsa_mua",
    "usda_food": "bronze_usda_food_access",
    "fema_nri": "bronze_fema_nri",
    "noaa": "bronze_noaa_nws_alerts",
    "fbi_ucr": "bronze_fbi_ucr",
}


# ---------------------------------------------------------------------------
# File arrival sensor
# ---------------------------------------------------------------------------

file_ingest_job = define_asset_job(
    name="file_arrival_ingest",
    selection=AssetSelection.groups("bronze"),
    description="Triggered by the file arrival sensor when new data files appear.",
    tags={"triggered_by": "file_sensor"},
)


@sensor(
    name="file_arrival_sensor",
    job=file_ingest_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Watches the data/incoming/ directory for new files.  When a file "
        "appears, maps it to the appropriate bronze asset and triggers ingestion.  "
        "Processed files get moved to data/incoming/processed/."
    ),
)
def file_arrival_sensor(context: SensorEvaluationContext) -> Any:
    if not DATA_DROP_DIR.exists():
        DATA_DROP_DIR.mkdir(parents=True, exist_ok=True)
        return SkipReason("Created data drop directory, no files yet")

    new_files = [
        f for f in DATA_DROP_DIR.iterdir()
        if f.is_file()
        and f.suffix.lower() in SUPPORTED_EXTENSIONS
        and not f.name.startswith(".")
    ]

    if not new_files:
        return SkipReason("No new files in drop directory")

    run_requests: list[RunRequest] = []

    for filepath in new_files:
        asset_name = _match_file_to_asset(filepath.name)
        if not asset_name:
            context.log.warning(
                f"Unrecognized file pattern: {filepath.name} — skipping.  "
                f"Expected patterns: {list(SOURCE_FILE_PATTERNS.keys())}"
            )
            continue

        context.log.info(f"New file detected: {filepath.name} -> {asset_name}")

        run_requests.append(
            RunRequest(
                run_key=f"file_{filepath.name}_{filepath.stat().st_mtime_ns}",
                tags={
                    "source_file": filepath.name,
                    "triggered_by": "file_arrival_sensor",
                },
            )
        )

        _move_to_processed(filepath)

    if not run_requests:
        return SkipReason("Files found but none matched known source patterns")

    return run_requests


# ---------------------------------------------------------------------------
# Quality failure alerting sensor
# ---------------------------------------------------------------------------

quality_remediation_job = define_asset_job(
    name="quality_remediation",
    selection=AssetSelection.groups("quality"),
    description="Re-runs quality checks when failures are detected.",
    tags={"triggered_by": "quality_sensor"},
)


@sensor(
    name="quality_failure_sensor",
    job=quality_remediation_job,
    minimum_interval_seconds=300,  # 5 minutes
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Monitors quality.check_results for recent failures.  If new failures "
        "are detected since the last check, logs a warning and optionally "
        "triggers a quality re-run."
    ),
)
def quality_failure_sensor(context: SensorEvaluationContext) -> Any:
    from orchestration.resources import get_config

    cfg = get_config()

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(cfg.pg_url)

        # use cursor to track what we've already seen
        last_checked = context.cursor or "1970-01-01T00:00:00"

        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT count(*) as fail_count,
                           max(checked_at) as latest
                    FROM quality.check_results
                    WHERE failed > 0
                      AND checked_at > :last_checked
                """),
                {"last_checked": last_checked},
            ).fetchone()

        engine.dispose()

        if result is None or result[0] == 0:
            return SkipReason("No new quality failures")

        fail_count = result[0]
        latest = str(result[1]) if result[1] else last_checked

        context.update_cursor(latest)

        context.log.warning(
            f"Quality alert: {fail_count} new check failures since {last_checked}"
        )

        # TODO: hook up actual alerting (Slack webhook, PagerDuty, email)
        # for now we just log and trigger a quality re-run
        return RunRequest(
            run_key=f"quality_alert_{latest}",
            tags={"triggered_by": "quality_failure_sensor", "failures": str(fail_count)},
        )

    except Exception as e:
        # don't fail the sensor if the quality table doesn't exist yet
        if "does not exist" in str(e).lower() or "relation" in str(e).lower():
            return SkipReason("Quality results table not yet created")
        context.log.error(f"Quality sensor error: {e}")
        return SkipReason(f"Error checking quality: {e}")


# ---------------------------------------------------------------------------
# Stale data sensor
# ---------------------------------------------------------------------------

stale_data_refresh_job = define_asset_job(
    name="stale_data_refresh",
    selection=AssetSelection.groups("bronze"),
    description="Re-ingests bronze sources that are older than their freshness SLA.",
    tags={"triggered_by": "stale_data_sensor"},
)

# freshness SLAs in days — how old is too old for each source
FRESHNESS_SLAS: dict[str, int] = {
    "raw.noaa_nws_alerts": 1,
    "raw.nces_ccd_directory": 60,
    "raw.census_acs_tract": 60,
    "raw.cdc_places_county": 45,
    "raw.epa_aqi_annual": 90,
    "raw.hrsa_hpsa": 30,
    "raw.hrsa_mua": 30,
    "raw.usda_food_access": 90,
    "raw.fema_nri": 90,
    "raw.fbi_ucr": 90,
}


@sensor(
    name="stale_data_sensor",
    job=stale_data_refresh_job,
    minimum_interval_seconds=3600,  # hourly
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Hourly check for stale bronze tables.  Compares table timestamps "
        "against freshness SLAs.  If any table is past its SLA, triggers "
        "a re-ingestion of all bronze sources."
    ),
)
def stale_data_sensor(context: SensorEvaluationContext) -> Any:
    from orchestration.resources import get_config

    cfg = get_config()

    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(cfg.pg_url)
        stale_tables: list[str] = []

        for table, sla_days in FRESHNESS_SLAS.items():
            try:
                with engine.connect() as conn:
                    # check if table exists first
                    schema, tbl = table.split(".")
                    exists = conn.execute(
                        text("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.tables
                                WHERE table_schema = :schema AND table_name = :table
                            )
                        """),
                        {"schema": schema, "table": tbl},
                    ).scalar()

                    if not exists:
                        stale_tables.append(table)
                        continue

                    # look for a timestamp column to check age
                    cols_result = conn.execute(
                        text("""
                            SELECT column_name FROM information_schema.columns
                            WHERE table_schema = :schema AND table_name = :table
                            AND data_type IN ('timestamp without time zone',
                                              'timestamp with time zone', 'date')
                        """),
                        {"schema": schema, "table": tbl},
                    ).fetchall()

                    ts_cols = [r[0] for r in cols_result]
                    if not ts_cols:
                        continue

                    ts_col = ts_cols[0]
                    age_result = conn.execute(
                        text(f"""
                            SELECT EXTRACT(EPOCH FROM (now() - max({ts_col}))) / 86400
                            FROM {table}
                        """),
                    ).scalar()

                    if age_result is not None and age_result > sla_days:
                        stale_tables.append(table)
                        context.log.info(
                            f"{table} is {age_result:.1f} days old (SLA: {sla_days}d)"
                        )

            except Exception as e:
                context.log.debug(f"Could not check {table}: {e}")
                continue

        engine.dispose()

        if not stale_tables:
            return SkipReason("All tables within freshness SLAs")

        context.log.warning(f"Stale tables detected: {stale_tables}")

        return RunRequest(
            run_key=f"stale_refresh_{datetime.now(timezone.utc).strftime('%Y%m%d%H')}",
            tags={
                "triggered_by": "stale_data_sensor",
                "stale_tables": json.dumps(stale_tables),
            },
        )

    except Exception as e:
        context.log.error(f"Stale data sensor error: {e}")
        return SkipReason(f"Error: {e}")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _match_file_to_asset(filename: str) -> str | None:
    """Match a dropped filename to a bronze asset name.

    We do a simple substring match — admin naming convention is
    "{source}_{optional_suffix}.{ext}".
    """
    lower = filename.lower()
    for pattern, asset_name in SOURCE_FILE_PATTERNS.items():
        if pattern in lower:
            return asset_name
    return None


def _move_to_processed(filepath: Path) -> None:
    """Move a processed file to the processed/ subdirectory.

    We keep processed files around for debugging — they get cleaned up
    by a separate cron job after 30 days.
    """
    processed_dir = filepath.parent / "processed"
    processed_dir.mkdir(exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest = processed_dir / f"{timestamp}_{filepath.name}"

    try:
        filepath.rename(dest)
        logger.info("file_moved_to_processed", src=filepath.name, dest=str(dest))
    except OSError as e:
        logger.warning("file_move_failed", src=filepath.name, error=str(e))


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_SENSORS: list = [
    file_arrival_sensor,
    quality_failure_sensor,
    stale_data_sensor,
]

SENSOR_JOBS: list = [
    file_ingest_job,
    quality_remediation_job,
    stale_data_refresh_job,
]
