"""
Generate and insert realistic sample data for local WellNest development.

Creates ~100 schools across 5 states with wellbeing scores, resource gaps,
predictions, and county summaries with AI briefs.  Uses Polars for the heavy
lifting and writes directly to the PostgreSQL gold schema via SQLAlchemy.

Usage:
    python scripts/seed_sample_data.py                     # defaults
    python scripts/seed_sample_data.py --schools 200       # more schools
    python scripts/seed_sample_data.py --drop-existing     # wipe first
    python scripts/seed_sample_data.py --dry-run           # just print, don't write
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import random
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wellnest.seed")

# ── reference data ───────────────────────────────────────────────────────────

STATES: list[dict[str, Any]] = [
    {"abbr": "IL", "fips_prefix": "17", "name": "Illinois"},
    {"abbr": "CA", "fips_prefix": "06", "name": "California"},
    {"abbr": "TX", "fips_prefix": "48", "name": "Texas"},
    {"abbr": "NY", "fips_prefix": "36", "name": "New York"},
    {"abbr": "FL", "fips_prefix": "12", "name": "Florida"},
]

# realistic county data — 4 counties per state
COUNTIES: list[dict[str, str]] = [
    # Illinois
    {"fips": "17031", "name": "Cook", "state": "IL"},
    {"fips": "17043", "name": "DuPage", "state": "IL"},
    {"fips": "17089", "name": "Kane", "state": "IL"},
    {"fips": "17197", "name": "Will", "state": "IL"},
    # California
    {"fips": "06037", "name": "Los Angeles", "state": "CA"},
    {"fips": "06073", "name": "San Diego", "state": "CA"},
    {"fips": "06075", "name": "San Francisco", "state": "CA"},
    {"fips": "06001", "name": "Alameda", "state": "CA"},
    # Texas
    {"fips": "48201", "name": "Harris", "state": "TX"},
    {"fips": "48113", "name": "Dallas", "state": "TX"},
    {"fips": "48029", "name": "Bexar", "state": "TX"},
    {"fips": "48453", "name": "Travis", "state": "TX"},
    # New York
    {"fips": "36061", "name": "New York", "state": "NY"},
    {"fips": "36047", "name": "Kings", "state": "NY"},
    {"fips": "36081", "name": "Queens", "state": "NY"},
    {"fips": "36005", "name": "Bronx", "state": "NY"},
    # Florida
    {"fips": "12086", "name": "Miami-Dade", "state": "FL"},
    {"fips": "12011", "name": "Broward", "state": "FL"},
    {"fips": "12095", "name": "Orange", "state": "FL"},
    {"fips": "12057", "name": "Hillsborough", "state": "FL"},
]

SCHOOL_TYPES = ["Regular", "Magnet", "Charter", "Title I", "Special Education"]
GRADE_RANGES = ["PK-5", "PK-8", "K-5", "K-8", "6-8", "9-12", "PK-12"]

# name parts for generating realistic school names
SCHOOL_PREFIXES = [
    "Lincoln",
    "Washington",
    "Jefferson",
    "Roosevelt",
    "King",
    "Kennedy",
    "Adams",
    "Madison",
    "Monroe",
    "Jackson",
    "Hamilton",
    "Franklin",
    "Oakwood",
    "Riverside",
    "Lakeview",
    "Hillcrest",
    "Greenfield",
    "Fairview",
    "Brookside",
    "Cedarwood",
    "Maplewood",
    "Sunnyside",
    "Heritage",
    "Northside",
    "Southgate",
    "Westwood",
    "Eastview",
    "Summit",
    "Valley",
    "Ridge",
    "Meadow",
    "Prairie",
]
SCHOOL_SUFFIXES = [
    "Elementary School",
    "Middle School",
    "High School",
    "Academy",
    "Preparatory",
    "School",
    "Learning Center",
    "Community School",
    "STEM Academy",
    "Arts Academy",
]

# bounding boxes (lat/lon) for each state — keeps school locations plausible
STATE_BOUNDS: dict[str, tuple[float, float, float, float]] = {
    "IL": (37.0, -91.5, 42.5, -87.5),
    "CA": (32.5, -124.4, 42.0, -114.1),
    "TX": (25.8, -106.6, 36.5, -93.5),
    "NY": (40.5, -79.8, 45.0, -71.9),
    "FL": (24.5, -87.6, 31.0, -80.0),
}

AI_BRIEF_TEMPLATES = [
    (
        "{county} County, {state} serves {pop:,} residents across {n_schools} assessed schools. "
        "The county's average Child Wellbeing Index score of {score:.1f}/100 places it in the "
        "{category} category. {strength_pillar} ({strength_score:.1f}) shows relative strength, "
        "while {weak_pillar} ({weak_score:.1f}) remains the greatest area for improvement. "
        "Approximately {gap_pct:.0f}% of schools have at least one pillar in the bottom quartile, "
        "suggesting targeted interventions could yield meaningful gains."
    ),
    (
        "With an average wellbeing score of {score:.1f}/100, {county} County ({state}) falls "
        "in the {category} tier nationally. Across {n_schools} schools serving {pop:,} residents, "
        "the data reveals a {spread:.1f}-point spread between the strongest pillar "
        "({strength_pillar}: {strength_score:.1f}) and the weakest ({weak_pillar}: {weak_score:.1f}). "
        "This disparity points to uneven resource allocation. {gap_count} schools have been "
        "flagged for critical resource gaps."
    ),
    (
        "Analysis of {n_schools} schools in {county} County, {state} (pop. {pop:,}) shows "
        "a composite wellbeing score of {score:.1f}/100 ({category}). The county ranks "
        "#{rank:,} nationally. {strength_pillar} performance ({strength_score:.1f}) is a "
        "relative bright spot, but {weak_pillar} ({weak_score:.1f}) pulls the overall index "
        "down. Of the schools assessed, {gap_count} show resource deficiencies in at least "
        "one domain — {critical_count} of those are classified as Critical Need."
    ),
]


# ── data generation ──────────────────────────────────────────────────────────


def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, val))


def _generate_school_name(rng: random.Random) -> str:
    prefix = rng.choice(SCHOOL_PREFIXES)
    suffix = rng.choice(SCHOOL_SUFFIXES)
    return f"{prefix} {suffix}"


def _generate_score(base: float, spread: float, rng: random.Random) -> float:
    """Generate a score centered around `base` with gaussian noise."""
    return _clamp(rng.gauss(base, spread))


def generate_schools(n_schools: int, seed: int = 42) -> pl.DataFrame:
    """Generate n_schools rows of realistic wellbeing data."""
    rng = random.Random(seed)
    rows: list[dict[str, Any]] = []

    schools_per_county = max(1, n_schools // len(COUNTIES))
    extra = n_schools - schools_per_county * len(COUNTIES)

    school_id_counter = 100000000000

    for county in COUNTIES:
        count = schools_per_county + (1 if extra > 0 else 0)
        extra -= 1 if extra > 0 else 0

        state = county["state"]
        lat_lo, lon_lo, lat_hi, lon_hi = STATE_BOUNDS[state]

        # each county gets a "baseline" quality level to make the data feel clustered
        county_base = rng.gauss(55.0, 15.0)

        for _ in range(count):
            school_id_counter += rng.randint(1, 50)
            nces_id = str(school_id_counter)

            edu = _generate_score(county_base + rng.gauss(0, 5), 12, rng)
            health = _generate_score(county_base + rng.gauss(0, 5), 10, rng)
            env = _generate_score(county_base + rng.gauss(3, 4), 8, rng)
            safety = _generate_score(county_base + rng.gauss(-2, 6), 14, rng)

            # weighted composite: edu 30%, health 30%, env 20%, safety 20%
            composite = _clamp(edu * 0.3 + health * 0.3 + env * 0.2 + safety * 0.2)

            if composite <= 25:
                category = "Critical"
            elif composite <= 50:
                category = "At Risk"
            elif composite <= 75:
                category = "Moderate"
            else:
                category = "Thriving"

            enrollment = max(50, int(rng.gauss(450, 250)))
            poverty_rate = _clamp(rng.gauss(22, 15), 2, 65)
            absenteeism = _clamp(rng.gauss(18, 10), 3, 55)

            rows.append(
                {
                    "nces_school_id": nces_id,
                    "school_name": _generate_school_name(rng),
                    "school_type": rng.choice(SCHOOL_TYPES),
                    "grade_range": rng.choice(GRADE_RANGES),
                    "county_fips": county["fips"],
                    "county_name": county["name"],
                    "state_abbr": state,
                    "latitude": round(rng.uniform(lat_lo, lat_hi), 6),
                    "longitude": round(rng.uniform(lon_lo, lon_hi), 6),
                    "total_enrollment": enrollment,
                    "title_i": rng.random() < (poverty_rate / 100 + 0.3),
                    "wellbeing_score": round(composite, 2),
                    "education_score": round(edu, 2),
                    "health_score": round(health, 2),
                    "environment_score": round(env, 2),
                    "safety_score": round(safety, 2),
                    "wellbeing_category": category,
                    "pillars_with_data": 4,
                    "math_proficiency": round(_clamp(rng.gauss(edu * 0.6, 10), 5, 95), 1),
                    "reading_proficiency": round(_clamp(rng.gauss(edu * 0.65, 8), 5, 95), 1),
                    "chronic_absenteeism_rate": round(absenteeism, 1),
                    "student_teacher_ratio": round(_clamp(rng.gauss(17, 4), 8, 35), 1),
                    "poverty_rate": round(poverty_rate, 1),
                    "uninsured_children_rate": round(_clamp(rng.gauss(6, 3), 1, 20), 1),
                    "food_desert": rng.random() < 0.15,
                    "hpsa_score": round(_clamp(rng.gauss(12, 6), 0, 25), 1)
                    if rng.random() > 0.3
                    else None,
                    "aqi_avg": round(_clamp(rng.gauss(45, 20), 10, 200), 1),
                    "violent_crime_rate": round(_clamp(rng.gauss(350, 200), 20, 1500), 1),
                    "social_vulnerability": round(_clamp(rng.gauss(0.5, 0.2), 0, 1), 3),
                    "score_change_1y": round(rng.gauss(0, 4), 1),
                    "updated_at": datetime.now(UTC),
                }
            )

    log.info("Generated %d schools across %d counties", len(rows), len(COUNTIES))
    return pl.DataFrame(rows)


def generate_resource_gaps(schools_df: pl.DataFrame, seed: int = 42) -> pl.DataFrame:
    """Flag schools with at least one pillar in the bottom quartile."""
    random.Random(seed)
    pillars = ["education_score", "health_score", "environment_score", "safety_score"]
    pillar_labels = ["Education", "Health", "Environment", "Safety"]

    # bottom quartile thresholds (p25)
    thresholds = {col: schools_df[col].quantile(0.25) for col in pillars}

    rows: list[dict[str, Any]] = []
    for school in schools_df.to_dicts():
        gaps = []
        for col, label in zip(pillars, pillar_labels, strict=False):
            if school[col] is not None and school[col] < thresholds[col]:
                gaps.append((label, school[col]))

        if not gaps:
            continue

        gaps.sort(key=lambda x: x[1])
        weakest = gaps[0][0]

        scores = [school[c] for c in pillars if school[c] is not None]
        spread = max(scores) - min(scores) if scores else 0

        if len(gaps) >= 3:
            priority = "Critical Need"
        elif len(gaps) == 2 or spread > 30:
            priority = "High Priority"
        else:
            priority = "Moderate Need"

        rows.append(
            {
                "nces_school_id": school["nces_school_id"],
                "school_name": school["school_name"],
                "county_fips": school["county_fips"],
                "wellbeing_score": school["wellbeing_score"],
                "education_score": school["education_score"],
                "health_score": school["health_score"],
                "environment_score": school["environment_score"],
                "safety_score": school["safety_score"],
                "gap_count": len(gaps),
                "weakest_pillar": weakest,
                "pillar_spread": round(spread, 2),
                "intervention_priority": priority,
                "has_strength": any(school[c] is not None and school[c] > 50 for c in pillars),
            }
        )

    log.info("Identified %d schools with resource gaps", len(rows))
    return pl.DataFrame(rows)


def generate_county_summaries(schools_df: pl.DataFrame, seed: int = 42) -> pl.DataFrame:
    """Aggregate school-level data into county summaries with AI briefs."""
    rng = random.Random(seed)

    county_groups = schools_df.group_by("county_fips").agg(
        [
            pl.col("county_name").first(),
            pl.col("state_abbr").first(),
            pl.col("wellbeing_score").mean().alias("avg_wellbeing_score"),
            pl.col("education_score").mean().alias("avg_education_score"),
            pl.col("health_score").mean().alias("avg_health_score"),
            pl.col("environment_score").mean().alias("avg_environment_score"),
            pl.col("safety_score").mean().alias("avg_safety_score"),
            pl.col("nces_school_id").count().alias("scored_school_count"),
            pl.col("poverty_rate").mean().alias("avg_poverty_rate"),
            pl.col("chronic_absenteeism_rate").mean().alias("avg_chronic_absenteeism"),
            (pl.col("wellbeing_category") == "Thriving").sum().alias("thriving_count"),
            (pl.col("wellbeing_category") == "Moderate").sum().alias("moderate_count"),
            (pl.col("wellbeing_category") == "At Risk").sum().alias("at_risk_count"),
            (pl.col("wellbeing_category") == "Critical").sum().alias("critical_count"),
        ]
    )

    rows = county_groups.to_dicts()

    # sort by score descending for ranking
    rows.sort(key=lambda r: r["avg_wellbeing_score"], reverse=True)
    total_counties = len(rows)

    for rank, row in enumerate(rows, 1):
        row["national_rank"] = rank
        row["total_counties"] = total_counties
        row["total_population"] = rng.randint(200_000, 10_000_000)

        score = row["avg_wellbeing_score"]
        if score <= 25:
            row["county_category"] = "Critical"
        elif score <= 50:
            row["county_category"] = "At Risk"
        elif score <= 75:
            row["county_category"] = "Moderate"
        else:
            row["county_category"] = "Thriving"

        # figure out strongest / weakest pillar for the brief
        pillar_scores = {
            "Education": row["avg_education_score"],
            "Health": row["avg_health_score"],
            "Environment": row["avg_environment_score"],
            "Safety": row["avg_safety_score"],
        }
        strength = max(pillar_scores, key=pillar_scores.get)
        weakness = min(pillar_scores, key=pillar_scores.get)

        # count schools with gaps in this county (rough estimate)
        gap_count = row["at_risk_count"] + row["critical_count"]
        gap_pct = (
            (gap_count / row["scored_school_count"] * 100) if row["scored_school_count"] else 0
        )
        critical_count = max(1, row["critical_count"])

        template = rng.choice(AI_BRIEF_TEMPLATES)
        row["ai_brief"] = template.format(
            county=row["county_name"],
            state=row["state_abbr"],
            pop=row["total_population"],
            n_schools=row["scored_school_count"],
            score=row["avg_wellbeing_score"],
            category=row["county_category"],
            strength_pillar=strength,
            strength_score=pillar_scores[strength],
            weak_pillar=weakness,
            weak_score=pillar_scores[weakness],
            spread=abs(pillar_scores[strength] - pillar_scores[weakness]),
            gap_pct=gap_pct,
            gap_count=gap_count,
            critical_count=critical_count,
            rank=rank,
        )

        # count gaps for this county
        row["schools_with_gaps"] = gap_count

        # round floats
        for k in [
            "avg_wellbeing_score",
            "avg_education_score",
            "avg_health_score",
            "avg_environment_score",
            "avg_safety_score",
            "avg_poverty_rate",
            "avg_chronic_absenteeism",
        ]:
            if row[k] is not None:
                row[k] = round(row[k], 2)

    log.info("Built %d county summaries", len(rows))
    return pl.DataFrame(rows)


# ── database operations ──────────────────────────────────────────────────────


def _get_db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "wellnest")
    user = os.environ.get("POSTGRES_USER", "wellnest")
    pw = os.environ.get("POSTGRES_PASSWORD", "changeme")
    return f"postgresql://{user}:{pw}@{host}:{port}/{db}"


def _ensure_gold_tables(engine) -> None:
    """Create gold schema tables if they don't already exist.

    In a real setup dbt handles this, but for seed data we need
    somewhere to land the rows.
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS gold.child_wellbeing_score (
                nces_school_id       TEXT PRIMARY KEY,
                school_name          TEXT,
                school_type          TEXT,
                grade_range          TEXT,
                county_fips          TEXT,
                county_name          TEXT,
                state_abbr           TEXT,
                latitude             DOUBLE PRECISION,
                longitude            DOUBLE PRECISION,
                total_enrollment     INTEGER,
                title_i              BOOLEAN,
                wellbeing_score      DOUBLE PRECISION,
                education_score      DOUBLE PRECISION,
                health_score         DOUBLE PRECISION,
                environment_score    DOUBLE PRECISION,
                safety_score         DOUBLE PRECISION,
                wellbeing_category   TEXT,
                pillars_with_data    INTEGER,
                math_proficiency     DOUBLE PRECISION,
                reading_proficiency  DOUBLE PRECISION,
                chronic_absenteeism_rate DOUBLE PRECISION,
                student_teacher_ratio    DOUBLE PRECISION,
                poverty_rate         DOUBLE PRECISION,
                uninsured_children_rate  DOUBLE PRECISION,
                food_desert          BOOLEAN,
                hpsa_score           DOUBLE PRECISION,
                aqi_avg              DOUBLE PRECISION,
                violent_crime_rate   DOUBLE PRECISION,
                social_vulnerability DOUBLE PRECISION,
                score_change_1y      DOUBLE PRECISION,
                updated_at           TIMESTAMPTZ DEFAULT now()
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS gold.resource_gaps (
                nces_school_id       TEXT PRIMARY KEY,
                school_name          TEXT,
                county_fips          TEXT,
                wellbeing_score      DOUBLE PRECISION,
                education_score      DOUBLE PRECISION,
                health_score         DOUBLE PRECISION,
                environment_score    DOUBLE PRECISION,
                safety_score         DOUBLE PRECISION,
                gap_count            INTEGER,
                weakest_pillar       TEXT,
                pillar_spread        DOUBLE PRECISION,
                intervention_priority TEXT,
                has_strength         BOOLEAN
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS gold.county_summary (
                county_fips          TEXT PRIMARY KEY,
                county_name          TEXT,
                state_abbr           TEXT,
                avg_wellbeing_score  DOUBLE PRECISION,
                avg_education_score  DOUBLE PRECISION,
                avg_health_score     DOUBLE PRECISION,
                avg_environment_score DOUBLE PRECISION,
                avg_safety_score     DOUBLE PRECISION,
                scored_school_count  INTEGER,
                total_population     INTEGER,
                avg_poverty_rate     DOUBLE PRECISION,
                avg_chronic_absenteeism DOUBLE PRECISION,
                thriving_count       INTEGER,
                moderate_count       INTEGER,
                at_risk_count        INTEGER,
                critical_count       INTEGER,
                schools_with_gaps    INTEGER,
                national_rank        INTEGER,
                total_counties       INTEGER,
                county_category      TEXT,
                ai_brief             TEXT
            )
        """)
        )

        conn.execute(
            text("""
            CREATE TABLE IF NOT EXISTS gold.county_ai_briefs (
                county_fips          TEXT,
                brief_text           TEXT,
                generated_at         TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (county_fips, generated_at)
            )
        """)
        )


def write_to_postgres(
    schools_df: pl.DataFrame,
    gaps_df: pl.DataFrame,
    summaries_df: pl.DataFrame,
    db_url: str,
    drop_existing: bool = False,
) -> None:
    """Write all generated data to PostgreSQL gold schema."""
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        _ensure_gold_tables(engine)

        with engine.begin() as conn:
            if drop_existing:
                log.warning("Dropping existing seed data...")
                conn.execute(text("TRUNCATE gold.child_wellbeing_score CASCADE"))
                conn.execute(text("TRUNCATE gold.resource_gaps CASCADE"))
                conn.execute(text("TRUNCATE gold.county_summary CASCADE"))
                conn.execute(text("TRUNCATE gold.county_ai_briefs CASCADE"))

            # upsert schools
            log.info("Writing %d schools to gold.child_wellbeing_score...", len(schools_df))
            _upsert_df(conn, schools_df, "gold.child_wellbeing_score", "nces_school_id")

            # upsert resource gaps
            log.info("Writing %d resource gaps to gold.resource_gaps...", len(gaps_df))
            _upsert_df(conn, gaps_df, "gold.resource_gaps", "nces_school_id")

            # upsert county summaries
            log.info("Writing %d county summaries to gold.county_summary...", len(summaries_df))
            _upsert_df(conn, summaries_df, "gold.county_summary", "county_fips")

            # also write AI briefs to the separate briefs table the report generator reads from
            briefs_rows = summaries_df.select(
                [
                    "county_fips",
                    pl.col("ai_brief").alias("brief_text"),
                ]
            ).to_dicts()

            for row in briefs_rows:
                conn.execute(
                    text("""
                        INSERT INTO gold.county_ai_briefs (county_fips, brief_text, generated_at)
                        VALUES (:county_fips, :brief_text, now())
                        ON CONFLICT (county_fips, generated_at) DO NOTHING
                    """),
                    row,
                )

            log.info("Wrote %d AI briefs to gold.county_ai_briefs", len(briefs_rows))

    finally:
        engine.dispose()


def _upsert_df(conn, df: pl.DataFrame, table: str, pk: str) -> None:
    """Simple upsert: INSERT ... ON CONFLICT DO UPDATE for every non-pk column."""
    if df.is_empty():
        return

    columns = df.columns
    non_pk = [c for c in columns if c != pk]
    col_list = ", ".join(columns)
    val_list = ", ".join(f":{c}" for c in columns)
    update_list = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_pk)

    sql = text(f"""
        INSERT INTO {table} ({col_list})
        VALUES ({val_list})
        ON CONFLICT ({pk}) DO UPDATE SET {update_list}
    """)

    rows = df.to_dicts()
    for row in rows:
        # clean up any polars-specific types that psycopg2 can't handle
        for k, v in row.items():
            if isinstance(v, date) and not isinstance(v, datetime):
                row[k] = datetime(v.year, v.month, v.day, tzinfo=UTC)
            elif v is not None and isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
        conn.execute(sql, row)


# ── CLI ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed WellNest gold schema with realistic sample data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--schools",
        type=int,
        default=100,
        help="Number of sample schools to generate (default: 100)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Truncate gold tables before inserting",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate data and print stats but don't write to DB",
    )
    parser.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="PostgreSQL connection URL (default: from DATABASE_URL env or .env)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    log.info("Generating sample data: %d schools, seed=%d", args.schools, args.seed)

    schools = generate_schools(n_schools=args.schools, seed=args.seed)
    gaps = generate_resource_gaps(schools, seed=args.seed)
    summaries = generate_county_summaries(schools, seed=args.seed)

    # quick sanity checks
    log.info(
        "Schools: %d | Resource gaps: %d | County summaries: %d",
        len(schools),
        len(gaps),
        len(summaries),
    )
    log.info(
        "Score distribution — mean: %.1f, std: %.1f, min: %.1f, max: %.1f",
        schools["wellbeing_score"].mean(),
        schools["wellbeing_score"].std(),
        schools["wellbeing_score"].min(),
        schools["wellbeing_score"].max(),
    )
    log.info(
        "Categories — Thriving: %d, Moderate: %d, At Risk: %d, Critical: %d",
        schools.filter(pl.col("wellbeing_category") == "Thriving").height,
        schools.filter(pl.col("wellbeing_category") == "Moderate").height,
        schools.filter(pl.col("wellbeing_category") == "At Risk").height,
        schools.filter(pl.col("wellbeing_category") == "Critical").height,
    )

    if args.dry_run:
        log.info("Dry run — skipping database write")
        log.info("Sample school row:\n%s", schools.head(1))
        log.info("Sample county summary:\n%s", summaries.head(1))
        return

    db_url = args.db_url or _get_db_url()
    log.info("Writing to database...")
    write_to_postgres(schools, gaps, summaries, db_url, drop_existing=args.drop_existing)

    log.info(
        "Done! Seeded %d schools across %d counties in %d states.",
        len(schools),
        len(COUNTIES),
        len(STATES),
    )


if __name__ == "__main__":
    main()
