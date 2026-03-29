"""
Feature engineering pipeline for the WellNest ML layer.

Reads from silver/gold tables and produces a wide feature matrix suitable for
XGBoost / LightGBM.  One row per school-year with ~50 features covering all
four pillars plus interaction terms and lag features.

Design decisions worth knowing about:
  - We build lag features (t-1, t-2, t-3) from the trend_metrics table.
    This is the biggest source of nulls because many schools only have 1-2
    years of data.  The imputation strategy matters a lot here — we use
    median fill within the same state to avoid cross-state information leakage.
  - Interaction features (poverty*food_desert, hpsa*uninsured) capture the
    compounding effects that linear models miss.  XGBoost can learn these
    on its own but feeding them explicitly still helps convergence speed.
  - We do NOT include the target variable (next-year proficiency change) in
    the feature matrix.  That gets joined at training time to keep the
    feature pipeline target-agnostic.
  - Categorical encoding is minimal since tree models handle categoricals
    natively.  We just ordinal-encode school_type and wellbeing_category.

Known issues:
  - AQI data has a seasonal bias (collected more in summer months) that
    we don't correct for.  Mentioned in the data quality report.
  - HPSA designation is binary and fairly coarse — it doesn't capture the
    severity gradient well.  HRSA publishes a numeric score but we only
    get the designation flag from the bulk download.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)

FEATURE_TABLE: str = "ml.feature_matrix"

# ordered so tree models can split on them sensibly
CATEGORY_MAP: dict[str, int] = {
    "Critical": 0,
    "At Risk": 1,
    "Moderate": 2,
    "Thriving": 3,
}

SCHOOL_TYPE_MAP: dict[str, int] = {
    "Regular": 0,
    "Special Education": 1,
    "Career/Technical": 2,
    "Alternative": 3,
    "Charter": 4,
    "Magnet": 5,
}

LAG_YEARS: list[int] = [1, 2, 3]

# features we compute lags for (these have the most meaningful year-over-year signal)
LAG_COLUMNS: list[str] = [
    "chronic_absenteeism_pct",
    "math_proficiency_pct",
    "reading_proficiency_pct",
    "median_aqi",
    "poverty_rate",
    "violent_crime_rate",
]

# anything below this coverage fraction is more noise than signal
MIN_COVERAGE: float = 0.30


class FeatureBuilder:
    """Assembles the wide feature matrix from silver/gold PostgreSQL tables.

    Typical usage::

        fb = FeatureBuilder(engine, connection_url="postgresql://...")
        df = fb.build(target_year=2022)
        fb.write_to_db(df)

    The builder keeps its own reference to the DB engine so callers don't
    have to thread it through every method.  It's also nice for testing —
    you can mock the engine once in the constructor.
    """

    def __init__(
        self,
        engine: Any,
        connection_url: str | None = None,
        *,
        min_coverage: float = MIN_COVERAGE,
    ) -> None:
        self._engine: Any = engine
        self._connection_url: str | None = connection_url
        self._min_coverage: float = min_coverage

    def build(self, *, target_year: int | None = None) -> pl.DataFrame:
        """Build the full feature matrix from silver/gold tables.

        If target_year is provided, only includes rows for that year.
        Otherwise grabs everything available.
        """
        base_df: pl.DataFrame = self._load_base_features(target_year=target_year)

        if base_df.is_empty():
            logger.warning("feature_matrix_empty", reason="no base data from gold tables")
            return base_df

        logger.info("feature_base_loaded", rows=len(base_df), cols=len(base_df.columns))

        lag_df: pl.DataFrame = self._lag_features()

        if not lag_df.is_empty():
            base_df = base_df.join(lag_df, on="nces_school_id", how="left")
            logger.info("lag_features_joined", new_cols=len(base_df.columns))

        base_df = self._interactions(base_df)
        base_df = self._encode_categoricals(base_df)
        base_df = self._impute(base_df)
        base_df = self._drop_low_coverage(base_df)

        logger.info(
            "feature_matrix_built",
            rows=len(base_df),
            cols=len(base_df.columns),
            null_pct=round(
                base_df.null_count().sum_horizontal().item()
                / (len(base_df) * len(base_df.columns))
                * 100,
                2,
            ),
        )
        return base_df

    # ------------------------------------------------------------------
    # lag features
    # ------------------------------------------------------------------

    def _lag_features(self) -> pl.DataFrame:
        """Build lag features from the trend_metrics table.

        The trend table has year-over-year changes already computed by dbt.
        We pivot those into lag columns: feature_lag1, feature_lag2, etc.

        TODO: this assumes trend_metrics has a school_year column.  If that
        column doesn't exist yet, the query will fail gracefully and we just
        skip lag features entirely.
        """
        query: str = """
            SELECT
                nces_school_id,
                math_proficiency_change,
                reading_proficiency_change,
                absenteeism_change,
                enrollment_change,
                combined_education_change,
                education_change_zscore
            FROM gold.trend_metrics
        """

        try:
            df: pl.DataFrame = pl.read_database(query, connection=self._engine)
        except Exception as exc:
            logger.warning("lag_features_unavailable", error=str(exc))
            return pl.DataFrame()

        if df.is_empty():
            return df

        renames: dict[str, str] = {
            "math_proficiency_change": "math_prof_yoy_change",
            "reading_proficiency_change": "reading_prof_yoy_change",
            "absenteeism_change": "absenteeism_yoy_change",
            "enrollment_change": "enrollment_yoy_change",
            "combined_education_change": "education_combined_yoy",
            "education_change_zscore": "education_zscore",
        }
        existing_renames: dict[str, str] = {k: v for k, v in renames.items() if k in df.columns}
        df = df.rename(existing_renames)
        return df

    # ------------------------------------------------------------------
    # interaction features
    # ------------------------------------------------------------------

    def _interactions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add interaction terms that capture compounding disadvantage.

        These come from the public health literature — poverty + food desert
        is worse than the sum of its parts, and HPSA + high uninsured creates
        a particularly bad access-to-care situation.

        We multiply and normalize instead of just multiplying raw values.
        Still not perfect but the trees handle the rescaling gracefully.
        """
        interactions: list[pl.Expr] = []

        if "child_poverty_rate" in df.columns and "pct_tracts_food_desert" in df.columns:
            interactions.append(
                (pl.col("child_poverty_rate") * pl.col("pct_tracts_food_desert") / 100).alias(
                    "poverty_x_food_desert"
                )
            )

        if "is_hpsa_designated" in df.columns and "uninsured_children_rate" in df.columns:
            interactions.append(
                (
                    pl.col("is_hpsa_designated").cast(pl.Float64)
                    * pl.col("uninsured_children_rate")
                ).alias("hpsa_x_uninsured")
            )

        if "violent_crime_rate" in df.columns and "child_poverty_rate" in df.columns:
            interactions.append(
                (pl.col("violent_crime_rate") * pl.col("child_poverty_rate") / 100).alias(
                    "crime_x_poverty"
                )
            )

        if "median_aqi" in df.columns and "asthma_pct" in df.columns:
            interactions.append(
                (pl.col("median_aqi") * pl.col("asthma_pct") / 100).alias("aqi_x_asthma")
            )

        if "chronic_absenteeism_pct" in df.columns and "child_poverty_rate" in df.columns:
            interactions.append(
                (pl.col("chronic_absenteeism_pct") * pl.col("child_poverty_rate") / 100).alias(
                    "absenteeism_x_poverty"
                )
            )

        # enrollment density as a rough proxy for school size effects
        if "total_enrollment" in df.columns and "student_teacher_ratio" in df.columns:
            interactions.append(
                (pl.col("total_enrollment") / pl.col("student_teacher_ratio").clip(1, None)).alias(
                    "implied_teacher_count"
                )
            )

        if interactions:
            df = df.with_columns(interactions)
            logger.info("interaction_features_added", count=len(interactions))

        return df

    # ------------------------------------------------------------------
    # imputation
    # ------------------------------------------------------------------

    def _impute(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fill missing values using a state-level median strategy.

        Strategy:
          - Numeric: median within the same state (avoids cross-state leakage).
            Falls back to global median if state-level median is also null.
          - Categorical: mode (most frequent value).
          - Binary flags: assume False (no designation) if missing.

        WARNING: this is the step most likely to introduce subtle data leakage.
        During training we should really be fitting the imputer on the train set
        only.  But for the stored feature matrix (used for prediction and
        dashboards) we impute on the full population.  The training code
        re-imputes properly with a train-only fit.
        """
        numeric_cols: list[str] = [
            c
            for c in df.columns
            if df[c].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
            and c not in ("nces_school_id",)
        ]

        # state-level median fill — this is the main defense against
        # cross-state information leakage during imputation
        if "state_abbr" in df.columns:
            for col in numeric_cols:
                state_medians: pl.DataFrame = df.group_by("state_abbr").agg(
                    pl.col(col).median().alias("_state_median")
                )
                df = df.join(state_medians, on="state_abbr", how="left")
                df = df.with_columns(
                    pl.col(col).fill_null(pl.col("_state_median")).alias(col)
                )
                df = df.drop("_state_median")

        # global median for anything still null after state-level fill
        for col in numeric_cols:
            median_val: float | None = df[col].median()
            if median_val is not None:
                df = df.with_columns(pl.col(col).fill_null(median_val))

        # boolean/flag columns: fill with False (0)
        flag_cols: list[str] = [
            c
            for c in df.columns
            if c.endswith("_int") or c.startswith("is_") or c.startswith("has_")
        ]
        for col in flag_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).fill_null(0))

        return df

    # ------------------------------------------------------------------
    # write to database
    # ------------------------------------------------------------------

    def write_to_db(self, df: pl.DataFrame, *, schema: str = "ml") -> int:
        """Write the feature matrix to Postgres.

        Uses replace mode because we rebuild the whole matrix each run.
        Incremental would be nice but the interaction features make it
        tricky to update individual rows without recomputing everything.
        """
        if self._connection_url is None:
            raise ValueError("connection_url is required to write to DB — pass it in the constructor")

        if df.is_empty():
            logger.warning("feature_matrix_save_skipped", reason="empty dataframe")
            return 0

        self._ensure_schema(schema)

        df.write_database(
            table_name=FEATURE_TABLE,
            connection=self._connection_url,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

        logger.info("feature_matrix_saved", table=FEATURE_TABLE, rows=len(df))
        return len(df)

    # ------------------------------------------------------------------
    # feature selection
    # ------------------------------------------------------------------

    @staticmethod
    def select_by_importance(
        df: pl.DataFrame,
        importances: dict[str, float],
        threshold: float = 0.01,
    ) -> list[str]:
        """Filter features using a pre-computed importance dict.

        Threshold of 0.01 is pretty aggressive — keeps about 60-70% of features
        in practice.  Bump to 0.005 if you're losing useful signal.

        NOTE: importance values from XGBoost gain vs cover vs weight can give
        very different rankings.  We default to gain.  Worth re-evaluating if
        feature stability becomes a problem (it probably will once we add more
        data sources).
        """
        keep: list[str] = [
            col for col, imp in importances.items() if imp >= threshold and col in df.columns
        ]

        dropped: list[str] = [col for col, imp in importances.items() if imp < threshold]
        if dropped:
            logger.info(
                "features_dropped_by_importance", count=len(dropped), threshold=threshold
            )

        return keep

    @staticmethod
    def get_feature_names(df: pl.DataFrame) -> list[str]:
        """Return all numeric feature column names (excludes identifiers and target)."""
        exclude: set[str] = {
            "nces_school_id",
            "school_name",
            "state_abbr",
            "county_fips",
            "county_name",
            "wellbeing_category",
            "school_type",
        }
        return [
            c
            for c in df.columns
            if c not in exclude and df[c].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        ]

    # ------------------------------------------------------------------
    # private helpers
    # ------------------------------------------------------------------

    def _load_base_features(self, target_year: int | None = None) -> pl.DataFrame:
        """Pull the core feature set from gold.child_wellbeing_score + silver tables.

        This is the widest query in the pipeline — we grab everything and trim later.
        """
        year_filter: str = f"AND sp.school_year = {target_year}" if target_year else ""

        query: str = f"""
            SELECT
                w.nces_school_id,
                sp.school_name,
                sp.state_abbr,
                sp.county_fips,
                sp.county_name,
                sp.school_type,
                sp.total_enrollment,
                sp.student_teacher_ratio,
                sp.is_title_i,
                sp.chronic_absenteeism_pct,
                sp.math_proficiency_pct,
                sp.reading_proficiency_pct,

                -- pillar scores (target-adjacent but useful for anomaly detection)
                w.wellbeing_score,
                w.education_score,
                w.health_score,
                w.environment_score,
                w.safety_score,
                w.wellbeing_category,

                -- health context
                hc.child_poverty_rate,
                hc.uninsured_children_rate,
                hc.asthma_pct,
                hc.poor_mental_health_pct,

                -- environment
                se.median_aqi,
                se.expected_annual_loss,
                se.aqi_days_unhealthy,

                -- safety
                ss.violent_crime_rate,
                ss.property_crime_rate,
                ss.social_vulnerability_score,

                -- resources
                sr.pct_tracts_food_desert,
                sr.is_hpsa_designated,
                sr.has_primary_care_shortage,
                sr.hpsa_primary_care_score

            FROM gold.child_wellbeing_score w
            JOIN silver.school_profiles sp ON w.nces_school_id = sp.nces_school_id
            LEFT JOIN silver.school_health_context hc ON w.nces_school_id = hc.nces_school_id
            LEFT JOIN silver.school_environment se ON w.nces_school_id = se.nces_school_id
            LEFT JOIN silver.school_safety ss ON w.nces_school_id = ss.nces_school_id
            LEFT JOIN silver.school_resources sr ON w.nces_school_id = sr.nces_school_id
            WHERE 1=1 {year_filter}
        """

        try:
            return pl.read_database(query, connection=self._engine)
        except Exception as exc:
            logger.error("feature_base_query_failed", error=str(exc))
            return pl.DataFrame()

    def _encode_categoricals(self, df: pl.DataFrame) -> pl.DataFrame:
        """Ordinal-encode the handful of categorical columns.

        Tree models can handle these natively but encoding them upfront means
        we get consistent ordering and can use them with linear baselines too.
        """
        if "wellbeing_category" in df.columns:
            df = df.with_columns(
                pl.col("wellbeing_category")
                .replace_strict(CATEGORY_MAP, default=None)
                .cast(pl.Int32)
                .alias("wellbeing_category_encoded")
            )

        if "school_type" in df.columns:
            df = df.with_columns(
                pl.col("school_type")
                .replace_strict(SCHOOL_TYPE_MAP, default=None)
                .cast(pl.Int32)
                .alias("school_type_encoded")
            )

        # boolean -> int for tree models that don't handle bools
        bool_cols: list[str] = [c for c in df.columns if df[c].dtype == pl.Boolean]
        for col in bool_cols:
            df = df.with_columns(pl.col(col).cast(pl.Int32).alias(f"{col}_int"))

        return df

    def _drop_low_coverage(self, df: pl.DataFrame) -> pl.DataFrame:
        """Drop columns where too many values are null even after imputation.

        This catches features that are only available for a subset of states
        (like the CDC environmental health data which only covers ~30 states).
        """
        n_rows: int = len(df)
        if n_rows == 0:
            return df

        protected: set[str] = {
            "nces_school_id",
            "school_name",
            "state_abbr",
            "county_fips",
            "county_name",
        }

        drop_cols: list[str] = []
        for col in df.columns:
            if col in protected:
                continue
            coverage: float = 1.0 - (df[col].null_count() / n_rows)
            if coverage < self._min_coverage:
                drop_cols.append(col)

        if drop_cols:
            logger.info(
                "low_coverage_features_dropped",
                count=len(drop_cols),
                cols=drop_cols[:10],
                threshold=self._min_coverage,
            )
            df = df.drop(drop_cols)

        return df

    def _ensure_schema(self, schema: str) -> None:
        """Create the target schema if it doesn't exist yet."""
        from sqlalchemy import create_engine, text

        eng = create_engine(self._connection_url)
        with eng.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        eng.dispose()
