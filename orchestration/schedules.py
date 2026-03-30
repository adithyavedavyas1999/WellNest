"""
Dagster schedules for WellNest pipelines.

Schedule strategy:
  - Weekly (Sunday 2 AM UTC): full pipeline refresh — all bronze through gold,
    then ML retraining and quality checks.  Most federal data sources update
    weekly or less frequently, so daily would just re-download identical files.
  - Daily (6 AM UTC): weather alerts only — these expire and need to be
    captured frequently.  Could bump to every 6h during severe weather season.
  - Monthly (1st of month, 4 AM UTC): AI brief regeneration.  These are
    expensive (OpenAI API costs) and the underlying data doesn't change fast
    enough to justify more frequent runs.
  - Post-pipeline: quality checks run as a downstream dependency of the main
    pipeline, not a separate schedule.  They show up here as part of the
    weekly schedule's asset selection.

Times are in UTC.  The team is in Chicago (UTC-6) so 2 AM UTC = 8 PM CT,
which means the pipeline finishes before anyone's morning standup.
"""

from __future__ import annotations

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# ---------------------------------------------------------------------------
# Jobs (named collections of assets that schedules trigger)
# ---------------------------------------------------------------------------

weekly_full_pipeline_job = define_asset_job(
    name="weekly_full_pipeline",
    selection=AssetSelection.groups("bronze", "silver", "gold", "ml", "quality"),
    description=(
        "Full pipeline: ingest all sources, run dbt transformations, "
        "train ML models, and run quality checks."
    ),
    tags={"pipeline": "weekly", "priority": "high"},
)

daily_weather_alerts_job = define_asset_job(
    name="daily_weather_alerts",
    selection=AssetSelection.assets("bronze_noaa_nws_alerts", "stg_weather_alerts"),
    description="Refresh NWS active weather alerts and staging model.",
    tags={"pipeline": "daily"},
)

monthly_ai_briefs_job = define_asset_job(
    name="monthly_ai_briefs",
    selection=AssetSelection.groups("ai"),
    description=(
        "Regenerate RAG index and community briefs.  Runs monthly because "
        "OpenAI API costs add up — ~$16 per full run."
    ),
    tags={"pipeline": "monthly"},
)

quality_checks_job = define_asset_job(
    name="quality_checks",
    selection=AssetSelection.groups("quality"),
    description="Run all data quality checks across silver and gold layers.",
    tags={"pipeline": "quality"},
)


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

weekly_pipeline_schedule = ScheduleDefinition(
    job=weekly_full_pipeline_job,
    cron_schedule="0 2 * * 0",  # Sunday 2 AM UTC
    name="weekly_pipeline_refresh",
    description=(
        "Weekly full pipeline refresh.  Runs Sunday at 2 AM UTC (8 PM CT Saturday).  "
        "Ingests all 12 data sources, runs dbt staging + silver + gold, "
        "retrains ML models, and runs quality checks."
    ),
    default_status=DefaultScheduleStatus.STOPPED,
    # don't start automatically — require explicit activation in the UI
    # so nobody accidentally kicks off a full pipeline on first deploy
    execution_timezone="UTC",
)

daily_alerts_schedule = ScheduleDefinition(
    job=daily_weather_alerts_job,
    cron_schedule="0 6 * * *",  # daily 6 AM UTC
    name="daily_weather_alerts_refresh",
    description=(
        "Daily weather alert snapshot.  Captures active NWS alerts and "
        "processes them through staging.  Runs at 6 AM UTC."
    ),
    default_status=DefaultScheduleStatus.STOPPED,
    execution_timezone="UTC",
)

monthly_ai_schedule = ScheduleDefinition(
    job=monthly_ai_briefs_job,
    cron_schedule="0 4 1 * *",  # 1st of month 4 AM UTC
    name="monthly_ai_regeneration",
    description=(
        "Monthly AI pipeline: rebuild RAG index, regenerate community briefs, "
        "run LLM quality validation.  Runs 1st of each month at 4 AM UTC."
    ),
    default_status=DefaultScheduleStatus.STOPPED,
    execution_timezone="UTC",
)

quality_after_pipeline_schedule = ScheduleDefinition(
    job=quality_checks_job,
    cron_schedule="0 8 * * 0",  # Sunday 8 AM UTC (after weekly pipeline finishes)
    name="quality_checks_post_pipeline",
    description=(
        "Quality checks that run after the weekly pipeline.  Scheduled 6 hours "
        "after the main pipeline to give it time to finish.  "
        "TODO: replace this with a sensor that triggers on pipeline completion."
    ),
    default_status=DefaultScheduleStatus.STOPPED,
    execution_timezone="UTC",
)


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_SCHEDULES: list = [
    weekly_pipeline_schedule,
    daily_alerts_schedule,
    monthly_ai_schedule,
    quality_after_pipeline_schedule,
]

ALL_JOBS: list = [
    weekly_full_pipeline_job,
    daily_weather_alerts_job,
    monthly_ai_briefs_job,
    quality_checks_job,
]
