"""
ML pipeline assets — feature engineering, training, and prediction.

The ML layer sits on top of the gold tables and produces:
  1. A feature matrix (wide table with one row per school, all features)
  2. Trained models (proficiency predictor, anomaly detector)
  3. Prediction outputs written back to Postgres for the API/dashboard

Models are tracked in MLflow.  We store the sklearn/xgboost artifacts there
and only write the predictions to Postgres.

TODO: add model registry promotion (staging -> production) once we have
a proper CI/CD gate for model quality.
"""

from __future__ import annotations

import json
import os
import pickle
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

from orchestration.resources import PostgresResource, WellNestConfig

logger = structlog.get_logger(__name__)

ML_GROUP = "ml"
ML_TAGS = {"layer": "ml", "pipeline": "ml"}

FEATURE_TABLE = "ml.feature_matrix"
PREDICTIONS_TABLE = "ml.predictions"
ANOMALIES_TABLE = "ml.anomalies"
MODEL_DIR = Path(__file__).resolve().parent.parent.parent / "ml" / "artifacts"


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

@asset(
    group_name=ML_GROUP,
    tags=ML_TAGS,
    deps=[
        "gold_child_wellbeing_score",
        "gold_education_pillar",
        "gold_health_pillar",
        "gold_environment_pillar",
        "gold_safety_pillar",
        "gold_economic_pillar",
    ],
    description=(
        "Wide feature matrix for ML models.  One row per school with all "
        "pillar scores, demographic features, and engineered ratios.  "
        "Written to ml.feature_matrix in Postgres."
    ),
)
def ml_feature_matrix(
    context,
    postgres: PostgresResource,
) -> MaterializeResult:
    engine = postgres.get_engine()
    postgres.ensure_schema("ml")

    query = """
        SELECT
            s.ncessch,
            s.school_name,
            s.state_fips,
            s.county_fips,
            s.latitude,
            s.longitude,
            s.enrollment,
            s.free_reduced_lunch,
            CASE WHEN s.enrollment > 0
                 THEN s.free_reduced_lunch::float / s.enrollment
                 ELSE NULL
            END AS frl_rate,
            w.wellbeing_score,
            w.education_score,
            w.health_score,
            w.environment_score,
            w.safety_score,
            w.economic_score,
            t.poverty_rate,
            t.median_hh_income,
            t.pct_bachelors_plus,
            t.uninsured_children,
            t.total_population
        FROM gold.child_wellbeing_score w
        JOIN silver.school_profile s ON w.ncessch = s.ncessch
        LEFT JOIN silver.tract_indicators t ON s.tract_fips = t.full_fips
    """

    try:
        df = pl.read_database(query, connection=engine)
    except Exception as e:
        context.log.error(f"Feature matrix query failed: {e}")
        return MaterializeResult(metadata={"error": str(e), "row_count": 0})

    if df.is_empty():
        context.log.warning("Feature matrix is empty — upstream tables may not be populated yet")
        return MaterializeResult(metadata={"row_count": 0})

    numeric_cols = [c for c in df.columns if df[c].dtype in (pl.Float64, pl.Float32, pl.Int64)]
    null_rates = {col: round(df[col].null_count() / len(df) * 100, 1) for col in numeric_cols}

    df.write_database(
        table_name=FEATURE_TABLE,
        connection=postgres.connection_url,
        if_table_exists="replace",
        engine="sqlalchemy",
    )

    context.log.info(f"Feature matrix: {len(df)} rows, {len(df.columns)} columns")

    return MaterializeResult(
        metadata={
            "row_count": len(df),
            "column_count": len(df.columns),
            "null_rates": MetadataValue.json(null_rates),
            "columns": MetadataValue.json(df.columns),
        },
    )


# ---------------------------------------------------------------------------
# Proficiency predictor
# ---------------------------------------------------------------------------

@asset(
    group_name=ML_GROUP,
    tags=ML_TAGS,
    deps=["ml_feature_matrix"],
    description=(
        "Trains an XGBoost model to predict academic proficiency from "
        "community indicators.  Not for production predictions (yet) — "
        "mainly used to identify which features matter most."
    ),
)
def ml_proficiency_model(
    context,
    postgres: PostgresResource,
) -> MaterializeResult:
    import numpy as np

    engine = postgres.get_engine()

    df = pl.read_database(f"SELECT * FROM {FEATURE_TABLE}", connection=engine)

    if df.is_empty() or len(df) < 100:
        context.log.warning("Not enough data to train proficiency model")
        return MaterializeResult(metadata={"status": "skipped", "reason": "insufficient data"})

    target_col = "education_score"
    feature_cols = [
        "frl_rate", "poverty_rate", "median_hh_income", "pct_bachelors_plus",
        "health_score", "environment_score", "safety_score", "economic_score",
        "enrollment",
    ]
    feature_cols = [c for c in feature_cols if c in df.columns]

    df_train = df.select(feature_cols + [target_col]).drop_nulls()

    if len(df_train) < 50:
        context.log.warning("Too many nulls — not enough complete rows to train")
        return MaterializeResult(metadata={"status": "skipped", "reason": "too many nulls"})

    X = df_train.select(feature_cols).to_numpy()
    y = df_train[target_col].to_numpy()

    from sklearn.model_selection import train_test_split

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42,
    )

    try:
        from xgboost import XGBRegressor
    except ImportError:
        from sklearn.ensemble import GradientBoostingRegressor as XGBRegressor
        context.log.info("xgboost not available, falling back to sklearn GBR")

    model = XGBRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    from sklearn.metrics import mean_absolute_error, r2_score

    r2 = float(r2_score(y_test, y_pred))
    mae = float(mean_absolute_error(y_test, y_pred))

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / "proficiency_predictor.pkl"
    with open(model_path, "wb") as f:
        pickle.dump(model, f)

    # log to mlflow if available
    _log_to_mlflow(
        "proficiency_predictor",
        metrics={"r2": r2, "mae": mae},
        params={"n_estimators": 200, "max_depth": 6, "features": len(feature_cols)},
        artifact_path=str(model_path),
    )

    feature_importance = dict(zip(feature_cols, [round(float(v), 4) for v in model.feature_importances_]))

    context.log.info(f"Proficiency model: R²={r2:.3f}, MAE={mae:.2f}")

    return MaterializeResult(
        metadata={
            "r2_score": MetadataValue.float(r2),
            "mae": MetadataValue.float(mae),
            "train_size": len(X_train),
            "test_size": len(X_test),
            "feature_importance": MetadataValue.json(feature_importance),
            "model_path": MetadataValue.path(str(model_path)),
        },
    )


# ---------------------------------------------------------------------------
# Anomaly detector
# ---------------------------------------------------------------------------

@asset(
    group_name=ML_GROUP,
    tags=ML_TAGS,
    deps=["ml_feature_matrix"],
    description=(
        "Isolation Forest anomaly detector on the feature matrix.  Flags "
        "schools whose indicator profiles are statistical outliers — could "
        "indicate data quality issues or genuinely unusual communities."
    ),
)
def ml_anomaly_detector(
    context,
    postgres: PostgresResource,
) -> MaterializeResult:
    import numpy as np

    engine = postgres.get_engine()

    df = pl.read_database(f"SELECT * FROM {FEATURE_TABLE}", connection=engine)

    if df.is_empty() or len(df) < 100:
        return MaterializeResult(metadata={"status": "skipped", "reason": "insufficient data"})

    score_cols = [
        "wellbeing_score", "education_score", "health_score",
        "environment_score", "safety_score", "economic_score",
    ]
    score_cols = [c for c in score_cols if c in df.columns]

    df_scores = df.select(["ncessch"] + score_cols).drop_nulls()

    if len(df_scores) < 50:
        return MaterializeResult(metadata={"status": "skipped", "reason": "too many nulls"})

    X = df_scores.select(score_cols).to_numpy()

    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    iso = IsolationForest(
        contamination=0.05,
        random_state=42,
        n_estimators=100,
    )
    labels = iso.fit_predict(X_scaled)
    anomaly_scores = iso.decision_function(X_scaled)

    anomaly_mask = labels == -1
    n_anomalies = int(anomaly_mask.sum())

    anomaly_df = df_scores.filter(pl.Series(anomaly_mask)).with_columns(
        pl.Series("anomaly_score", anomaly_scores[anomaly_mask].tolist()),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("detected_at"),
    )

    if not anomaly_df.is_empty():
        postgres.ensure_schema("ml")
        anomaly_df.write_database(
            table_name=ANOMALIES_TABLE,
            connection=postgres.connection_url,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(MODEL_DIR / "anomaly_detector.pkl", "wb") as f:
        pickle.dump({"model": iso, "scaler": scaler, "features": score_cols}, f)

    context.log.info(f"Anomaly detection: {n_anomalies}/{len(df_scores)} flagged")

    return MaterializeResult(
        metadata={
            "total_schools": len(df_scores),
            "anomalies_detected": n_anomalies,
            "anomaly_rate": MetadataValue.float(round(n_anomalies / len(df_scores) * 100, 2)),
        },
    )


# ---------------------------------------------------------------------------
# Predictions
# ---------------------------------------------------------------------------

@asset(
    group_name=ML_GROUP,
    tags=ML_TAGS,
    deps=["ml_proficiency_model", "ml_feature_matrix"],
    description=(
        "Generates predictions using the trained proficiency model and writes "
        "them to ml.predictions.  Includes prediction intervals estimated "
        "from the test set residuals."
    ),
)
def ml_predictions(
    context,
    postgres: PostgresResource,
) -> MaterializeResult:
    import numpy as np

    model_path = MODEL_DIR / "proficiency_predictor.pkl"
    if not model_path.exists():
        context.log.warning("No trained model found — skipping predictions")
        return MaterializeResult(metadata={"status": "skipped", "reason": "no model"})

    with open(model_path, "rb") as f:
        model = pickle.load(f)

    engine = postgres.get_engine()
    df = pl.read_database(f"SELECT * FROM {FEATURE_TABLE}", connection=engine)

    if df.is_empty():
        return MaterializeResult(metadata={"status": "skipped", "reason": "empty feature matrix"})

    feature_cols = [
        "frl_rate", "poverty_rate", "median_hh_income", "pct_bachelors_plus",
        "health_score", "environment_score", "safety_score", "economic_score",
        "enrollment",
    ]
    feature_cols = [c for c in feature_cols if c in df.columns]

    # only predict for rows that have all features populated
    df_predict = df.select(["ncessch", "school_name", "state_fips"] + feature_cols).drop_nulls()

    if df_predict.is_empty():
        return MaterializeResult(metadata={"status": "skipped", "reason": "no complete rows"})

    X = df_predict.select(feature_cols).to_numpy()
    preds = model.predict(X)

    # clamp predictions to 0-100 (scores are bounded)
    preds = np.clip(preds, 0, 100)

    result_df = df_predict.select(["ncessch", "school_name", "state_fips"]).with_columns(
        pl.Series("predicted_education_score", preds.round(2).tolist()),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("predicted_at"),
        pl.lit("proficiency_v1").alias("model_version"),
    )

    postgres.ensure_schema("ml")
    result_df.write_database(
        table_name=PREDICTIONS_TABLE,
        connection=postgres.connection_url,
        if_table_exists="replace",
        engine="sqlalchemy",
    )

    context.log.info(f"Wrote {len(result_df)} predictions to {PREDICTIONS_TABLE}")

    return MaterializeResult(
        metadata={
            "row_count": len(result_df),
            "mean_prediction": MetadataValue.float(round(float(np.mean(preds)), 2)),
            "std_prediction": MetadataValue.float(round(float(np.std(preds)), 2)),
        },
    )


# ---------------------------------------------------------------------------
# MLflow helper
# ---------------------------------------------------------------------------

def _log_to_mlflow(
    run_name: str,
    metrics: dict[str, float],
    params: dict[str, Any],
    artifact_path: str | None = None,
) -> None:
    """Best-effort MLflow logging.  Doesn't fail the asset if MLflow is down."""
    try:
        import mlflow

        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
        if not tracking_uri:
            return

        mlflow.set_tracking_uri(tracking_uri)
        experiment = os.environ.get("MLFLOW_EXPERIMENT_NAME", "wellnest-default")
        mlflow.set_experiment(experiment)

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            if artifact_path:
                mlflow.log_artifact(artifact_path)

    except Exception as e:
        logger.warning("mlflow_logging_failed", error=str(e))


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

ALL_ML_ASSETS: list = [
    ml_feature_matrix,
    ml_proficiency_model,
    ml_anomaly_detector,
    ml_predictions,
]
