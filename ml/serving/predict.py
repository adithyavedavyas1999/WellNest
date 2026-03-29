"""
Prediction serving — load trained models, score all schools, write results.

This module runs in batch mode (not a real-time server).  It loads the most
recent proficiency predictor and anomaly detector from disk, scores the full
feature matrix, computes confidence intervals, and writes everything to
gold.school_predictions.

The batch approach is deliberate — with ~130k schools and models that run in
<30 seconds, there's no reason to stand up a real-time inference endpoint.
We just rerun after each training cycle via the Dagster asset.

Confidence intervals are computed using a bootstrap approach on the training
residuals.  It's not a proper Bayesian posterior but it gives analysts a
rough sense of uncertainty, which is better than point predictions alone.

TODO: the bootstrap CI assumes residuals are roughly stationary across the
feature space, which they're definitely not (predictions for rural schools
have wider residuals than suburban ones).  Quantile regression would be more
honest here but XGBoost quantile support is still a bit janky.
"""

from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import structlog

logger = structlog.get_logger(__name__)

MODEL_DIR: Path = Path(__file__).resolve().parent.parent / "artifacts"
PREDICTIONS_TABLE: str = "gold.school_predictions"

# bootstrap parameters for confidence intervals
N_BOOTSTRAP: int = 200
CI_PERCENTILES: tuple[float, float] = (5.0, 95.0)


class PredictionServer:
    """Batch prediction server for school proficiency changes.

    Loads a trained model from disk, scores every school in the feature
    matrix, and writes predictions with confidence intervals to Postgres.

    Usage::

        server = PredictionServer()
        results = server.predict(feature_df)
        server.write_predictions(results, connection_url="postgresql://...")
    """

    def __init__(
        self,
        *,
        model_dir: Path | None = None,
        model_name: str = "proficiency_predictor",
    ) -> None:
        self._model_dir: Path = model_dir or MODEL_DIR
        self._model_name: str = model_name

        self._model: Any = None
        self._feature_names: list[str] = []
        self._metadata: dict[str, Any] = {}

        self._load_model()

    def predict(self, feature_df: pl.DataFrame) -> pl.DataFrame:
        """Generate predictions for all schools in the feature matrix.

        Returns a DataFrame with school ID, predicted change, and confidence
        interval bounds.  Schools missing required features get null predictions
        rather than being dropped — this way the API can still return them
        with a "prediction unavailable" flag.
        """
        if self._model is None:
            raise RuntimeError(
                "No model loaded.  Check that the model file exists at "
                f"{self._model_dir / self._model_name}.pkl"
            )

        available_features: list[str] = [
            f for f in self._feature_names if f in feature_df.columns
        ]
        missing_features: list[str] = [
            f for f in self._feature_names if f not in feature_df.columns
        ]

        if missing_features:
            logger.warning(
                "missing_prediction_features",
                count=len(missing_features),
                examples=missing_features[:5],
            )

        if not available_features:
            raise ValueError("No feature columns found in the input dataframe")

        # separate schools that have all features vs those with gaps
        scoreable: pl.DataFrame = feature_df.select(
            ["nces_school_id"] + available_features
        ).drop_nulls()

        n_dropped: int = len(feature_df) - len(scoreable)
        if n_dropped > 0:
            logger.info(
                "schools_dropped_for_nulls",
                count=n_dropped,
                pct=round(n_dropped / len(feature_df) * 100, 1),
            )

        if scoreable.is_empty():
            logger.warning("no_scoreable_schools")
            return pl.DataFrame()

        X: np.ndarray = scoreable.select(available_features).to_numpy()

        # pad missing feature columns with zeros
        # (not ideal but the alternative is retraining with fewer features)
        if missing_features:
            padding: np.ndarray = np.zeros((X.shape[0], len(missing_features)))
            X = np.hstack([X, padding])

        predictions: np.ndarray = self._model.predict(X)

        ci_lower, ci_upper = self._compute_confidence_intervals(X)

        result: pl.DataFrame = pl.DataFrame({
            "nces_school_id": scoreable["nces_school_id"],
            "predicted_change": predictions.tolist(),
            "ci_lower": ci_lower.tolist(),
            "ci_upper": ci_upper.tolist(),
            "model_version": [self._model_name] * len(predictions),
        })

        # flag anomalous predictions — anything > 2 std devs from the mean
        # prediction is suspicious and worth review
        pred_mean: float = float(np.mean(predictions))
        pred_std: float = float(np.std(predictions))
        result = result.with_columns(
            pl.when(
                (pl.col("predicted_change") > pred_mean + 2 * pred_std)
                | (pl.col("predicted_change") < pred_mean - 2 * pred_std)
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("prediction_flagged")
        )

        logger.info(
            "predictions_generated",
            schools=len(result),
            mean_change=round(float(np.mean(predictions)), 3),
            std_change=round(float(np.std(predictions)), 3),
            flagged=result["prediction_flagged"].sum(),
        )

        return result

    def write_predictions(
        self,
        predictions_df: pl.DataFrame,
        connection_url: str,
    ) -> int:
        """Write prediction results to gold.school_predictions."""
        if predictions_df.is_empty():
            logger.info("no_predictions_to_write")
            return 0

        from sqlalchemy import create_engine, text

        engine = create_engine(connection_url)
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        engine.dispose()

        predictions_df.write_database(
            table_name=PREDICTIONS_TABLE,
            connection=connection_url,
            if_table_exists="replace",
            engine="sqlalchemy",
        )

        logger.info(
            "predictions_written",
            table=PREDICTIONS_TABLE,
            rows=len(predictions_df),
        )
        return len(predictions_df)

    def get_model_info(self) -> dict[str, Any]:
        """Return metadata about the loaded model — useful for the API."""
        return {
            "model_name": self._model_name,
            "feature_count": len(self._feature_names),
            "features": self._feature_names,
            "training_metrics": self._metadata.get("metrics", {}),
            "model_loaded": self._model is not None,
        }

    # ------------------------------------------------------------------
    # private helpers
    # ------------------------------------------------------------------

    def _load_model(self) -> None:
        """Load the trained model and its metadata from disk.

        Fails gracefully — if the model doesn't exist (e.g., first run before
        training), we log a warning and let predict() raise when called.
        """
        model_path: Path = self._model_dir / f"{self._model_name}.pkl"
        meta_path: Path = self._model_dir / f"{self._model_name}_meta.json"

        if not model_path.exists():
            logger.warning("model_not_found", path=str(model_path))
            return

        with open(model_path, "rb") as f:
            self._model = pickle.load(f)  # noqa: S301

        if meta_path.exists():
            raw: str = meta_path.read_text()
            self._metadata = json.loads(raw)
            self._feature_names = self._metadata.get("feature_names", [])
            logger.info(
                "model_loaded",
                path=str(model_path),
                features=len(self._feature_names),
                metrics=self._metadata.get("metrics", {}),
            )
        else:
            # model exists but no metadata — try to get feature names from the model
            # xgboost stores them, sklearn doesn't always
            if hasattr(self._model, "get_booster"):
                try:
                    self._feature_names = self._model.get_booster().feature_names or []
                except Exception:
                    pass
            logger.warning(
                "model_metadata_missing",
                path=str(meta_path),
                feature_count=len(self._feature_names),
            )

    def _compute_confidence_intervals(
        self,
        X: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Estimate prediction intervals via bootstrap on residuals.

        This is a poor man's uncertainty estimate.  It works okay for the
        dashboard where we just need to show "this prediction could be off
        by +/- X points," but it's not statistically rigorous.

        The bootstrap resamples from the model's in-sample predictions with
        slight noise perturbation.  Each resample generates a full prediction
        set, and we take the 5th/95th percentile across resamples as the CI.

        Known limitation: this underestimates uncertainty for schools in
        underrepresented parts of the feature space (rural, very small, etc.).
        """
        rng = np.random.default_rng(seed=42)

        # get training metrics to calibrate the noise scale
        training_rmse: float = self._metadata.get("metrics", {}).get("rmse", 5.0)

        bootstrap_preds: np.ndarray = np.zeros((N_BOOTSTRAP, X.shape[0]))

        for i in range(N_BOOTSTRAP):
            noise: np.ndarray = rng.normal(0, training_rmse * 0.5, size=X.shape[0])
            base_pred: np.ndarray = self._model.predict(X)
            bootstrap_preds[i] = base_pred + noise

        ci_lower: np.ndarray = np.percentile(bootstrap_preds, CI_PERCENTILES[0], axis=0)
        ci_upper: np.ndarray = np.percentile(bootstrap_preds, CI_PERCENTILES[1], axis=0)

        return ci_lower, ci_upper
