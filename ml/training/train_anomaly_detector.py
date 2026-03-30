"""
Anomaly detection on school-level score vectors.

Two complementary approaches:
  1. Isolation Forest: catches schools whose *overall profile* is weird
     (e.g., high education score but rock-bottom everything else).
  2. Z-score on year-over-year changes: catches schools whose scores
     changed dramatically between years (possible data issues or real events
     like a school closure / new funding).

We flag schools with |z| > 2.5 on any pillar change, which in practice
catches about 3-5% of schools.  The 2.5 threshold was chosen after looking
at a few dozen flagged examples — 2.0 produced too many false positives
from normal score volatility, 3.0 missed some genuine anomalies in the
rural school data.

Each anomaly gets a narrative explanation generated from the data (not LLM).
The narrative says things like "Math proficiency dropped 18 points year-over-year
(2.8 std devs below mean change), while enrollment decreased 12%."
This helps analysts triage without digging into the raw data.

TODO: add a feedback loop so analysts can mark false positives and we can
adjust the contamination parameter accordingly.  We've been talking about
this for two sprints now, should really just do it.
"""

from __future__ import annotations

import pickle
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import structlog
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = structlog.get_logger(__name__)

MODEL_DIR: Path = Path(__file__).resolve().parent.parent / "artifacts"
ANOMALIES_TABLE: str = "ml.anomalies"

Z_THRESHOLD: float = 2.5

PILLAR_COLS: list[str] = [
    "education_score",
    "health_score",
    "environment_score",
    "safety_score",
]

CHANGE_COLS: list[str] = [
    "math_prof_yoy_change",
    "reading_prof_yoy_change",
    "absenteeism_yoy_change",
    "enrollment_yoy_change",
]

# human-readable labels for narrative generation
CHANGE_LABELS: dict[str, str] = {
    "math_prof_yoy_change": "math proficiency",
    "reading_prof_yoy_change": "reading proficiency",
    "absenteeism_yoy_change": "chronic absenteeism",
    "enrollment_yoy_change": "enrollment",
}


@dataclass
class AnomalyResult:
    """Holds everything the anomaly pipeline produces in a single run."""

    anomaly_df: pl.DataFrame
    total_schools: int
    n_anomalies_iforest: int
    n_anomalies_zscore: int
    n_anomalies_combined: int
    model_path: Path | None = None


class AnomalyDetector:
    """Detects anomalous schools using Isolation Forest and z-score methods.

    The two methods are complementary:
      - Isolation Forest catches multi-dimensional outliers in the pillar
        score space.  Good for schools with a bizarre *profile* (e.g.,
        safety=95 but health=12).
      - Z-score flags catch univariate outliers in year-over-year changes.
        Good for sudden swings that might indicate data problems or real
        events (funding cut, new program, school merger).

    Typical usage::

        detector = AnomalyDetector(feature_df, contamination=0.05)
        result = detector.run()
        detector.save_to_db(result, connection_url="postgresql://...")
    """

    def __init__(
        self,
        feature_df: pl.DataFrame,
        *,
        contamination: float = 0.05,
        z_threshold: float = Z_THRESHOLD,
        random_state: int = 42,
    ) -> None:
        self._feature_df: pl.DataFrame = feature_df
        self._contamination: float = contamination
        self._z_threshold: float = z_threshold
        self._random_state: int = random_state

        self._available_pillars: list[str] = [c for c in PILLAR_COLS if c in feature_df.columns]
        self._available_changes: list[str] = [c for c in CHANGE_COLS if c in feature_df.columns]

    def run(self) -> AnomalyResult:
        """Run both anomaly detection methods and merge results."""
        if not self._available_pillars:
            logger.warning("anomaly_detection_skipped", reason="no pillar score columns found")
            return self._empty_result()

        # need at least school ID and pillar scores with no nulls for IForest
        iforest_df: pl.DataFrame = self._feature_df.select(
            ["nces_school_id", *self._available_pillars]
        ).drop_nulls()

        if len(iforest_df) < 50:
            logger.warning(
                "anomaly_detection_skipped",
                reason="too few complete rows",
                rows=len(iforest_df),
            )
            return self._empty_result()

        iforest_flags: pl.DataFrame = self.fit_isolation_forest(iforest_df)

        zscore_flags: pl.DataFrame = pl.DataFrame()
        if self._available_changes:
            change_df: pl.DataFrame = self._feature_df.select(
                ["nces_school_id", *self._available_changes]
            ).drop_nulls()
            if len(change_df) >= 30:
                zscore_flags = self.compute_zscore_flags(change_df)

        combined: pl.DataFrame = self._merge_flags(iforest_flags, zscore_flags)

        if combined.is_empty():
            return AnomalyResult(
                anomaly_df=combined,
                total_schools=len(self._feature_df),
                n_anomalies_iforest=len(iforest_flags),
                n_anomalies_zscore=len(zscore_flags),
                n_anomalies_combined=0,
            )

        # join back school metadata for the narratives
        meta_cols: list[str] = [
            c
            for c in ["nces_school_id", "school_name", "state_abbr", "county_name"]
            if c in self._feature_df.columns
        ]
        meta: pl.DataFrame = self._feature_df.select(meta_cols).unique(subset=["nces_school_id"])
        combined = combined.join(meta, on="nces_school_id", how="left")

        combined = self.explain_anomalies(combined)

        combined = combined.with_columns(pl.lit(datetime.now(UTC).isoformat()).alias("detected_at"))

        logger.info(
            "anomaly_detection_complete",
            total=len(self._feature_df),
            iforest=len(iforest_flags),
            zscore=len(zscore_flags),
            combined=len(combined),
        )

        return AnomalyResult(
            anomaly_df=combined,
            total_schools=len(self._feature_df),
            n_anomalies_iforest=len(iforest_flags),
            n_anomalies_zscore=len(zscore_flags),
            n_anomalies_combined=len(combined),
        )

    # ------------------------------------------------------------------
    # Isolation Forest
    # ------------------------------------------------------------------

    def fit_isolation_forest(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fit Isolation Forest on pillar scores, return flagged school IDs.

        Contamination parameter is tricky — too low and you miss real anomalies,
        too high and analysts get alert fatigue.  We default to 0.05 and let
        the z-score method catch the rest.

        We scale before fitting because IForest uses random splits and the
        pillar scores technically have the same 0-100 range, but in practice
        the distributions are quite different (safety is left-skewed, health
        is roughly normal).
        """
        X: np.ndarray = df.select(self._available_pillars).to_numpy()

        scaler = StandardScaler()
        X_scaled: np.ndarray = scaler.fit_transform(X)

        iso = IsolationForest(
            contamination=self._contamination,
            random_state=self._random_state,
            n_estimators=150,
            max_samples="auto",
            n_jobs=-1,
        )
        labels: np.ndarray = iso.fit_predict(X_scaled)
        raw_scores: np.ndarray = iso.decision_function(X_scaled)

        anomaly_mask: np.ndarray = labels == -1

        self._save_anomaly_model(iso, scaler)

        flagged_ids: pl.DataFrame = df.filter(pl.Series(anomaly_mask)).select("nces_school_id")
        flagged_scores: np.ndarray = raw_scores[anomaly_mask]

        return flagged_ids.with_columns(
            pl.Series("iforest_score", flagged_scores.tolist()),
            pl.lit("isolation_forest").alias("detection_method"),
        )

    # ------------------------------------------------------------------
    # Z-score detection
    # ------------------------------------------------------------------

    def compute_zscore_flags(self, df: pl.DataFrame) -> pl.DataFrame:
        """Flag schools where any year-over-year change exceeds the z-threshold.

        Returns school IDs with the worst z-score and which column triggered it.

        The z-score approach catches different things than IForest — mainly
        schools that were normal last year and suddenly jumped/dropped.  This is
        where you find data entry errors, school mergers, and the occasional
        district that changed their testing vendor.
        """
        school_ids: pl.Series = df["nces_school_id"]
        X: np.ndarray = df.select(self._available_changes).to_numpy()

        means: np.ndarray = np.nanmean(X, axis=0)
        stds: np.ndarray = np.nanstd(X, axis=0)
        stds = np.where(stds == 0, 1.0, stds)

        z_scores: np.ndarray = (X - means) / stds

        max_abs_z: np.ndarray = np.nanmax(np.abs(z_scores), axis=1)
        flagged_mask: np.ndarray = max_abs_z > self._z_threshold

        if not np.any(flagged_mask):
            return pl.DataFrame()

        worst_col_idx: np.ndarray = np.nanargmax(np.abs(z_scores[flagged_mask]), axis=1)
        worst_col_names: list[str] = [self._available_changes[i] for i in worst_col_idx]
        worst_z_values: list[float] = [
            float(z_scores[flagged_mask][i, worst_col_idx[i]]) for i in range(len(worst_col_idx))
        ]

        return pl.DataFrame(
            {
                "nces_school_id": school_ids.filter(pl.Series(flagged_mask)).to_list(),
                "zscore_worst": worst_z_values,
                "zscore_trigger_col": worst_col_names,
                "detection_method": ["zscore"] * int(flagged_mask.sum()),
            }
        )

    # ------------------------------------------------------------------
    # narrative generation
    # ------------------------------------------------------------------

    def explain_anomalies(self, anomaly_df: pl.DataFrame) -> pl.DataFrame:
        """Generate a human-readable explanation for each anomaly.

        Not using an LLM here — just template-based narratives from the data.
        Keeps it fast and deterministic.  The LLM narrative generation is in
        ai/briefs/ and runs separately for the final reports.
        """
        if anomaly_df.is_empty():
            return anomaly_df

        join_cols: list[str] = (
            ["nces_school_id"]
            + self._available_pillars
            + [c for c in self._available_changes if c in self._feature_df.columns]
        )
        join_cols = [c for c in join_cols if c in self._feature_df.columns]
        scores: pl.DataFrame = self._feature_df.select(join_cols).unique(subset=["nces_school_id"])

        merged: pl.DataFrame = anomaly_df.join(scores, on="nces_school_id", how="left")

        narratives: list[str] = []
        for row in merged.iter_rows(named=True):
            narratives.append(self._build_narrative(row))

        drop_cols: list[str] = [
            c for c in self._available_pillars + self._available_changes if c in merged.columns
        ]
        merged = merged.drop(drop_cols)
        merged = merged.with_columns(pl.Series("narrative", narratives))

        return merged

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------

    def save_to_db(self, result: AnomalyResult, connection_url: str) -> int:
        """Persist anomaly results to Postgres."""
        if result.anomaly_df.is_empty():
            logger.info("no_anomalies_to_save")
            return 0

        from sqlalchemy import create_engine, text

        engine = create_engine(connection_url)
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS ml"))
        engine.dispose()

        result.anomaly_df.write_database(
            table_name=ANOMALIES_TABLE,
            connection=connection_url,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

        logger.info("anomalies_saved", table=ANOMALIES_TABLE, rows=len(result.anomaly_df))
        return len(result.anomaly_df)

    # ------------------------------------------------------------------
    # private helpers
    # ------------------------------------------------------------------

    def _empty_result(self) -> AnomalyResult:
        return AnomalyResult(
            anomaly_df=pl.DataFrame(),
            total_schools=len(self._feature_df),
            n_anomalies_iforest=0,
            n_anomalies_zscore=0,
            n_anomalies_combined=0,
        )

    def _merge_flags(
        self,
        iforest_flags: pl.DataFrame,
        zscore_flags: pl.DataFrame,
    ) -> pl.DataFrame:
        """Combine both detection methods, deduplicating on school ID.

        Schools flagged by both methods get a higher severity rating — in practice
        these are the ones that analysts should look at first.
        """
        if iforest_flags.is_empty() and zscore_flags.is_empty():
            return pl.DataFrame()

        if iforest_flags.is_empty():
            return zscore_flags.with_columns(pl.lit("zscore_only").alias("severity"))

        if zscore_flags.is_empty():
            return iforest_flags.with_columns(pl.lit("iforest_only").alias("severity"))

        both: pl.DataFrame = iforest_flags.select("nces_school_id").join(
            zscore_flags.select("nces_school_id"),
            on="nces_school_id",
            how="inner",
        )

        combined: pl.DataFrame = pl.concat(
            [
                iforest_flags.select("nces_school_id", "iforest_score", "detection_method"),
                zscore_flags.select(
                    "nces_school_id",
                    pl.col("zscore_worst").alias("iforest_score"),
                    "detection_method",
                ),
            ]
        ).unique(subset=["nces_school_id"], keep="first")

        both_ids: set[str] = set(both["nces_school_id"].to_list())
        combined = combined.with_columns(
            pl.when(pl.col("nces_school_id").is_in(list(both_ids)))
            .then(pl.lit("both_methods"))
            .otherwise(pl.col("detection_method"))
            .alias("severity")
        )

        return combined

    def _build_narrative(self, row: dict[str, Any]) -> str:
        """Build a single narrative string for one anomalous school.

        The narrative is intentionally simple.  We tried making it more verbose
        early on and analysts said they preferred terse summaries they could
        scan quickly.  The full data is always available via the API.
        """
        school: str = str(row.get("school_name", row.get("nces_school_id", "Unknown")))
        parts: list[str] = [f"{school}:"]

        pillar_scores: dict[str, float] = {
            c: row[c] for c in self._available_pillars if row.get(c) is not None
        }
        if pillar_scores:
            worst: str = min(pillar_scores, key=pillar_scores.get)  # type: ignore[arg-type]
            best: str = max(pillar_scores, key=pillar_scores.get)  # type: ignore[arg-type]
            gap: float = pillar_scores[best] - pillar_scores[worst]

            worst_name: str = worst.replace("_score", "").replace("_", " ").title()
            best_name: str = best.replace("_score", "").replace("_", " ").title()

            if gap > 30:
                parts.append(
                    f"Large pillar gap — {best_name} ({pillar_scores[best]:.0f}) "
                    f"vs {worst_name} ({pillar_scores[worst]:.0f}), "
                    f"a {gap:.0f}-point spread."
                )

        changes: dict[str, float] = {
            c: row[c] for c in self._available_changes if row.get(c) is not None
        }
        if changes:
            biggest: str = max(changes, key=lambda k: abs(changes[k]))
            val: float = changes[biggest]
            label: str = CHANGE_LABELS.get(biggest, biggest)
            direction: str = "increased" if val > 0 else "decreased"

            # for absenteeism, increase is bad; for proficiency, decrease is bad
            if "absenteeism" in biggest:
                direction = "worsened" if val > 0 else "improved"

            parts.append(f"{label.title()} {direction} by {abs(val):.1f} points YoY.")

        severity: str = str(row.get("severity", "unknown"))
        if severity == "both_methods":
            parts.append("Flagged by both Isolation Forest and z-score — high confidence anomaly.")

        if len(parts) <= 1:
            return f"{school}: Unusual score profile detected."
        return " ".join(parts)

    def _save_anomaly_model(
        self,
        model: IsolationForest,
        scaler: StandardScaler,
    ) -> Path:
        """Persist the fitted IForest model for reuse in prediction serving."""
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        path: Path = MODEL_DIR / "anomaly_detector.pkl"

        with open(path, "wb") as f:
            pickle.dump(
                {
                    "model": model,
                    "scaler": scaler,
                    "features": self._available_pillars,
                },
                f,
            )

        return path
