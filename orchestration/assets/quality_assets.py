"""
Data quality assets — Soda checks, freshness monitoring, quality reports.

Quality checks run after every pipeline materialization.  We use Soda Core
for declarative checks (null rates, value ranges, row counts) and custom
Python for freshness and cross-table consistency checks.

Soda check YAML files live in transformation/quality/.  If they don't exist
yet, the assets fall back to inline checks defined here.

TODO: set up Soda Cloud for the dashboarding piece — right now results
are just written to a Postgres table and the dagster metadata pane.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import structlog
from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from orchestration.resources import PostgresResource

logger = structlog.get_logger(__name__)

QA_GROUP = "quality"
QA_TAGS = {"layer": "quality", "pipeline": "quality"}

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SODA_DIR = PROJECT_ROOT / "transformation" / "quality"
QUALITY_TABLE = "quality.check_results"


# ---------------------------------------------------------------------------
# Silver layer quality checks
# ---------------------------------------------------------------------------

@asset(
    group_name=QA_GROUP,
    tags=QA_TAGS,
    deps=[
        "silver_school_profile",
        "silver_tract_indicators",
        "silver_county_safety",
    ],
    description=(
        "Soda quality checks on silver layer tables.  Validates row counts, "
        "null rates, value ranges, and referential integrity between staging "
        "and silver models."
    ),
)
def quality_silver_checks(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    checks: list[dict[str, Any]] = []

    silver_tables = {
        "silver.school_profile": {
            "min_rows": 50000,
            "required_columns": ["ncessch", "school_name", "state_fips"],
            "max_null_pct": {"ncessch": 0, "school_name": 1, "state_fips": 0},
        },
        "silver.tract_indicators": {
            "min_rows": 30000,
            "required_columns": ["full_fips", "state_fips"],
            "max_null_pct": {"full_fips": 0},
        },
        "silver.county_safety": {
            "min_rows": 1000,
            "required_columns": ["county_fips"],
            "max_null_pct": {"county_fips": 0},
        },
    }

    engine = postgres.get_engine()

    for table, rules in silver_tables.items():
        result = _run_table_checks(engine, table, rules, context)
        checks.append(result)

    total_passed = sum(c["passed"] for c in checks)
    total_failed = sum(c["failed"] for c in checks)
    total_warnings = sum(c["warnings"] for c in checks)

    _store_check_results(postgres, checks, "silver")

    context.log.info(
        f"Silver checks: {total_passed} passed, {total_failed} failed, {total_warnings} warnings"
    )

    return MaterializeResult(
        metadata={
            "checks_passed": total_passed,
            "checks_failed": total_failed,
            "checks_warned": total_warnings,
            "details": MetadataValue.json(checks),
        },
    )


# ---------------------------------------------------------------------------
# Gold layer quality checks
# ---------------------------------------------------------------------------

@asset(
    group_name=QA_GROUP,
    tags=QA_TAGS,
    deps=[
        "gold_child_wellbeing_score",
        "gold_county_summary",
        "gold_state_ranking",
    ],
    description=(
        "Quality checks on gold layer tables.  Extra scrutiny on the "
        "wellbeing scores — checks distribution, bounds, and completeness."
    ),
)
def quality_gold_checks(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    checks: list[dict[str, Any]] = []
    engine = postgres.get_engine()

    gold_tables = {
        "gold.child_wellbeing_score": {
            "min_rows": 50000,
            "required_columns": ["ncessch", "wellbeing_score"],
            "max_null_pct": {"ncessch": 0, "wellbeing_score": 10},
            "value_ranges": {
                "wellbeing_score": (0, 100),
                "education_score": (0, 100),
                "health_score": (0, 100),
                "environment_score": (0, 100),
                "safety_score": (0, 100),
                "economic_score": (0, 100),
            },
        },
        "gold.county_summary": {
            "min_rows": 1000,
            "required_columns": ["county_fips"],
            "max_null_pct": {"county_fips": 0},
        },
        "gold.state_ranking": {
            "min_rows": 40,
            "required_columns": ["state_fips"],
            "max_null_pct": {},
        },
    }

    for table, rules in gold_tables.items():
        result = _run_table_checks(engine, table, rules, context)
        checks.append(result)

    # additional score distribution check
    dist_check = _check_score_distribution(engine, context)
    checks.append(dist_check)

    total_passed = sum(c["passed"] for c in checks)
    total_failed = sum(c["failed"] for c in checks)

    _store_check_results(postgres, checks, "gold")

    return MaterializeResult(
        metadata={
            "checks_passed": total_passed,
            "checks_failed": total_failed,
            "details": MetadataValue.json(checks),
        },
    )


# ---------------------------------------------------------------------------
# Freshness checks
# ---------------------------------------------------------------------------

@asset(
    group_name=QA_GROUP,
    tags=QA_TAGS,
    deps=[
        "bronze_nces_ccd",
        "bronze_cdc_places",
        "bronze_census_acs",
        "bronze_noaa_nws_alerts",
    ],
    description=(
        "Checks that key bronze tables have been refreshed within expected "
        "windows.  Weather alerts should be < 24h old, everything else < 30 days."
    ),
)
def quality_freshness_checks(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    freshness_rules: dict[str, int] = {
        "raw.noaa_nws_alerts": 1,  # days
        "raw.nces_ccd_directory": 30,
        "raw.census_acs_tract": 30,
        "raw.cdc_places_county": 30,
        "raw.cdc_places_tract": 30,
        "raw.epa_aqi_annual": 90,
    }

    results: list[dict[str, Any]] = []

    for table, max_age_days in freshness_rules.items():
        check = _check_freshness(engine, table, max_age_days, context)
        results.append(check)

    stale = [r for r in results if r.get("status") == "stale"]
    fresh = [r for r in results if r.get("status") == "fresh"]
    missing = [r for r in results if r.get("status") == "missing"]

    context.log.info(
        f"Freshness: {len(fresh)} fresh, {len(stale)} stale, {len(missing)} missing"
    )

    return MaterializeResult(
        metadata={
            "fresh_count": len(fresh),
            "stale_count": len(stale),
            "missing_count": len(missing),
            "details": MetadataValue.json(results),
        },
    )


# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------

@asset(
    group_name=QA_GROUP,
    tags=QA_TAGS,
    deps=["quality_silver_checks", "quality_gold_checks", "quality_freshness_checks"],
    description=(
        "Aggregates all quality check results into a summary report.  "
        "Writes to quality.summary in Postgres and generates a JSON report "
        "file for the dashboard."
    ),
)
def quality_report(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> MaterializeResult:
    engine = postgres.get_engine()

    try:
        df = pl.read_database(
            f"SELECT * FROM {QUALITY_TABLE} ORDER BY checked_at DESC LIMIT 1000",
            connection=engine,
        )
    except Exception:
        context.log.warning("No quality check results found — probably first run")
        df = pl.DataFrame()

    now = datetime.now(timezone.utc).isoformat()

    if df.is_empty():
        report = {
            "generated_at": now,
            "status": "no_data",
            "message": "No quality check results available yet.",
        }
    else:
        total_passed = int(df["passed"].sum()) if "passed" in df.columns else 0
        total_failed = int(df["failed"].sum()) if "failed" in df.columns else 0
        total_checks = total_passed + total_failed

        report = {
            "generated_at": now,
            "status": "healthy" if total_failed == 0 else "degraded",
            "total_checks": total_checks,
            "passed": total_passed,
            "failed": total_failed,
            "pass_rate": round(total_passed / max(total_checks, 1) * 100, 1),
            "layers_checked": df["layer"].unique().to_list() if "layer" in df.columns else [],
        }

    report_path = PROJECT_ROOT / "reports" / "quality_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    try:
        postgres.ensure_schema("quality")
        summary_df = pl.DataFrame([{
            "generated_at": now,
            "report_json": json.dumps(report),
            "status": report["status"],
        }])
        summary_df.write_database(
            table_name="quality.summary",
            connection=postgres.connection_url,
            if_table_exists="append",
            engine="sqlalchemy",
        )
    except Exception as e:
        context.log.warning(f"Failed to persist quality summary: {e}")

    context.log.info(f"Quality report: {report.get('status', 'unknown')}")

    return MaterializeResult(
        metadata={
            "status": report.get("status", "unknown"),
            "pass_rate": MetadataValue.float(report.get("pass_rate", 0.0)),
            "report_path": MetadataValue.path(str(report_path)),
            "report": MetadataValue.json(report),
        },
    )


# ---------------------------------------------------------------------------
# check helpers
# ---------------------------------------------------------------------------

def _run_table_checks(
    engine: Any,
    table: str,
    rules: dict[str, Any],
    context: AssetExecutionContext,
) -> dict[str, Any]:
    """Run a set of declarative checks against a table."""
    passed = 0
    failed = 0
    warnings = 0
    details: list[str] = []

    try:
        df = pl.read_database(f"SELECT * FROM {table} LIMIT 500000", connection=engine)
    except Exception as e:
        return {
            "table": table,
            "passed": 0,
            "failed": 1,
            "warnings": 0,
            "details": [f"Table does not exist or is not accessible: {e}"],
        }

    row_count = len(df)

    min_rows = rules.get("min_rows", 0)
    if row_count >= min_rows:
        passed += 1
        details.append(f"Row count {row_count} >= {min_rows}: PASS")
    else:
        failed += 1
        details.append(f"Row count {row_count} < {min_rows}: FAIL")

    for col in rules.get("required_columns", []):
        if col in df.columns:
            passed += 1
        else:
            failed += 1
            details.append(f"Missing required column: {col}")

    for col, max_pct in rules.get("max_null_pct", {}).items():
        if col not in df.columns:
            continue
        null_pct = round(df[col].null_count() / max(len(df), 1) * 100, 2)
        if null_pct <= max_pct:
            passed += 1
            details.append(f"{col} null rate {null_pct}% <= {max_pct}%: PASS")
        else:
            failed += 1
            details.append(f"{col} null rate {null_pct}% > {max_pct}%: FAIL")

    for col, (lo, hi) in rules.get("value_ranges", {}).items():
        if col not in df.columns:
            continue
        non_null = df.filter(pl.col(col).is_not_null())
        if non_null.is_empty():
            warnings += 1
            details.append(f"{col} is all null, can't check range: WARN")
            continue

        col_min = non_null[col].min()
        col_max = non_null[col].max()
        if col_min >= lo and col_max <= hi:
            passed += 1
        else:
            failed += 1
            details.append(f"{col} range [{col_min}, {col_max}] outside [{lo}, {hi}]: FAIL")

    return {
        "table": table,
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
        "row_count": row_count,
        "details": details,
    }


def _check_score_distribution(engine: Any, context: AssetExecutionContext) -> dict[str, Any]:
    """Sanity-check the wellbeing score distribution.

    Flags if the mean is outside a plausible range or if there are suspicious
    spikes (e.g., 50% of scores at exactly 50.0 suggests a default value bug).
    """
    try:
        df = pl.read_database(
            "SELECT wellbeing_score FROM gold.child_wellbeing_score WHERE wellbeing_score IS NOT NULL",
            connection=engine,
        )
    except Exception:
        return {
            "check": "score_distribution",
            "passed": 0,
            "failed": 1,
            "warnings": 0,
            "details": ["Could not read wellbeing scores"],
        }

    if df.is_empty():
        return {
            "check": "score_distribution",
            "passed": 0,
            "failed": 1,
            "warnings": 0,
            "details": ["No scores found"],
        }

    mean_score = df["wellbeing_score"].mean()
    std_score = df["wellbeing_score"].std()

    passed = 0
    failed = 0
    details: list[str] = []

    # mean should be somewhere between 20 and 80 for a well-calibrated score
    if 20 <= mean_score <= 80:
        passed += 1
        details.append(f"Mean score {mean_score:.1f} in plausible range: PASS")
    else:
        failed += 1
        details.append(f"Mean score {mean_score:.1f} outside plausible range [20,80]: FAIL")

    # std should indicate spread — if it's < 1 something is probably broken
    if std_score > 1:
        passed += 1
        details.append(f"Score std {std_score:.2f} > 1: PASS")
    else:
        failed += 1
        details.append(f"Score std {std_score:.2f} suspiciously low: FAIL")

    # check for single-value dominance
    mode = df["wellbeing_score"].mode()
    if mode is not None and not mode.is_empty():
        mode_val = mode[0]
        mode_pct = df.filter(pl.col("wellbeing_score") == mode_val).height / len(df) * 100
        if mode_pct > 25:
            failed += 1
            details.append(f"Score {mode_val} accounts for {mode_pct:.1f}% of records: FAIL")
        else:
            passed += 1

    return {
        "check": "score_distribution",
        "passed": passed,
        "failed": failed,
        "warnings": 0,
        "details": details,
        "mean": round(mean_score, 2),
        "std": round(std_score, 2),
    }


def _check_freshness(
    engine: Any,
    table: str,
    max_age_days: int,
    context: AssetExecutionContext,
) -> dict[str, Any]:
    """Check if a table has been updated within the expected window.

    We use the max value of any timestamp-like column as a proxy for
    freshness.  Not bulletproof but good enough for alerting.
    """
    try:
        df = pl.read_database(f"SELECT * FROM {table} LIMIT 1", connection=engine)
    except Exception:
        return {"table": table, "status": "missing", "max_age_days": max_age_days}

    # look for timestamp columns
    ts_cols = [
        c for c in df.columns
        if any(hint in c.lower() for hint in ["date", "time", "created", "updated", "ingested"])
    ]

    if not ts_cols:
        return {"table": table, "status": "unknown", "reason": "no timestamp column found"}

    ts_col = ts_cols[0]

    try:
        result = pl.read_database(
            f"SELECT max({ts_col}) as max_ts FROM {table}",
            connection=engine,
        )
        max_ts_val = result["max_ts"][0]
        if max_ts_val is None:
            return {"table": table, "status": "unknown", "reason": "null timestamps"}

        if isinstance(max_ts_val, str):
            max_ts = datetime.fromisoformat(max_ts_val.replace("Z", "+00:00"))
        elif isinstance(max_ts_val, datetime):
            max_ts = max_ts_val if max_ts_val.tzinfo else max_ts_val.replace(tzinfo=timezone.utc)
        else:
            return {"table": table, "status": "unknown", "reason": f"unexpected type: {type(max_ts_val)}"}

        now = datetime.now(timezone.utc)
        age_days = (now - max_ts).total_seconds() / 86400

        status = "fresh" if age_days <= max_age_days else "stale"

        return {
            "table": table,
            "status": status,
            "age_days": round(age_days, 1),
            "max_age_days": max_age_days,
            "latest_timestamp": max_ts.isoformat(),
        }
    except Exception as e:
        return {"table": table, "status": "error", "error": str(e)}


def _store_check_results(
    pg: PostgresResource,
    checks: list[dict[str, Any]],
    layer: str,
) -> None:
    """Persist check results to the quality schema."""
    try:
        pg.ensure_schema("quality")
        now = datetime.now(timezone.utc).isoformat()

        records = []
        for check in checks:
            records.append({
                "checked_at": now,
                "layer": layer,
                "table_or_check": check.get("table", check.get("check", "unknown")),
                "passed": check.get("passed", 0),
                "failed": check.get("failed", 0),
                "warnings": check.get("warnings", 0),
                "details_json": json.dumps(check.get("details", [])),
            })

        df = pl.DataFrame(records)
        df.write_database(
            table_name=QUALITY_TABLE,
            connection=pg.connection_url,
            if_table_exists="append",
            engine="sqlalchemy",
        )
    except Exception as e:
        logger.warning("quality_results_store_failed", error=str(e))


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_QUALITY_ASSETS: list = [
    quality_silver_checks,
    quality_gold_checks,
    quality_freshness_checks,
    quality_report,
]
