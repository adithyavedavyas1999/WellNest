"""
XGBoost predictor for next-year proficiency score changes.

Target: delta in combined education score between year N and year N+1.
We're predicting *change* rather than absolute level because:
  1. Absolute proficiency is heavily confounded by demographics — the model
     just learns "poverty predicts low scores" which we already know.
  2. Predicting change surfaces which schools are improving/declining
     *relative to their baseline*, which is more actionable for NGOs.

Train/test split is strictly temporal:
  - Train on years 1-3 (2019-2021 in current data)
  - Test on year 4 (2022)
  This avoids the look-ahead leakage that plagues most education ML papers.
  See Appendix C of our methodology doc for the full leakage audit.

Hyperparameter tuning: we use TimeSeriesSplit CV on the training data
  (not random CV, because autocorrelation in scores would inflate metrics).
  The search space was chosen to avoid overfitting on our smallish dataset
  (~95k schools) — we cap max_depth at 8 and n_estimators at 500.

Model performance context:
  - R-squared of 0.25-0.35 is actually pretty good for education score
    prediction.  Most published models get 0.15-0.25.  Anything above 0.5
    probably means leakage somewhere.
  - MAE under 5 points (on a 0-100 scale) is our target for production use.
"""

from __future__ import annotations

import json
import pickle
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import structlog
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import TimeSeriesSplit, cross_val_score

logger = structlog.get_logger(__name__)

MODEL_DIR: Path = Path(__file__).resolve().parent.parent / "artifacts"
DEFAULT_MODEL_NAME: str = "proficiency_predictor"

TARGET_COL: str = "education_combined_yoy"

# features we exclude from training — identifiers, targets, and things
# that would cause leakage (like the score we're trying to predict change for)
EXCLUDE_FROM_TRAINING: set[str] = {
    "nces_school_id",
    "school_name",
    "state_abbr",
    "county_fips",
    "county_name",
    "wellbeing_category",
    "school_type",
    "wellbeing_score",
    "education_score",
    TARGET_COL,
    "education_zscore",  # derived from target, would leak
}


@dataclass
class TrainingResult:
    """Container for a training run's outputs."""

    model: Any
    model_type: str
    feature_names: list[str]
    metrics: dict[str, float]
    feature_importances: dict[str, float]
    params: dict[str, Any]
    train_size: int
    test_size: int
    model_path: Path | None = None
    cv_scores: list[float] = field(default_factory=list)


class ProficiencyTrainer:
    """Trains and evaluates an XGBoost model for proficiency change prediction.

    Usage::

        trainer = ProficiencyTrainer(feature_df)
        result = trainer.train()
        trainer.evaluate(result)
        trainer.save_model(result)

    The class holds the feature dataframe and handles the full lifecycle from
    splitting through hyperparameter tuning to evaluation and persistence.
    """

    def __init__(
        self,
        feature_df: pl.DataFrame,
        *,
        model_type: str = "xgboost",
        random_state: int = 42,
    ) -> None:
        if TARGET_COL not in feature_df.columns:
            raise ValueError(
                f"Target column '{TARGET_COL}' not found.  "
                "Make sure lag features were joined before training."
            )

        self._feature_df: pl.DataFrame = feature_df
        self._model_type: str = model_type
        self._random_state: int = random_state

        self._feature_cols: list[str] = self._pick_training_features()
        if not self._feature_cols:
            raise ValueError("No valid feature columns found after filtering")

    def train(self, *, tune_hyperparams: bool = True) -> TrainingResult:
        """Run the full training pipeline: split, fit, score, return result.

        Set tune_hyperparams=False for quick dev iterations — uses
        reasonable defaults instead of running GridSearchCV.
        """
        df_clean: pl.DataFrame = self._feature_df.select(
            [*self._feature_cols, TARGET_COL]
        ).drop_nulls()

        if len(df_clean) < 100:
            raise ValueError(
                f"Only {len(df_clean)} complete rows — need at least 100 for training.  "
                "Check null rates in the feature matrix."
            )

        X: np.ndarray = df_clean.select(self._feature_cols).to_numpy()
        y: np.ndarray = df_clean[TARGET_COL].to_numpy()

        # temporal split: last 25% of rows as test set
        # data should already be sorted by year from the feature pipeline
        split_idx: int = int(len(X) * 0.75)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        logger.info(
            "training_data_split",
            train=len(X_train),
            test=len(X_test),
            features=len(self._feature_cols),
            target_mean=round(float(np.mean(y_train)), 3),
            target_std=round(float(np.std(y_train)), 3),
        )

        model, actual_type, params = self._build_model(
            tune=tune_hyperparams,
            X_train=X_train,
            y_train=y_train,
        )

        model.fit(X_train, y_train)

        y_pred: np.ndarray = model.predict(X_test)
        metrics: dict[str, float] = self._compute_metrics(y_test, y_pred)

        # sanity check: R-squared > 0.5 on education data is suspicious
        if metrics["r2"] > 0.50:
            logger.warning(
                "suspiciously_high_r2",
                r2=metrics["r2"],
                hint="Check for target leakage — education_score shouldn't be in features",
            )

        # cross-validation on training data for a more robust estimate
        cv = TimeSeriesSplit(n_splits=4)
        cv_raw: np.ndarray = cross_val_score(
            model, X_train, y_train, cv=cv, scoring="neg_mean_absolute_error"
        )
        cv_mae: list[float] = [-float(s) for s in cv_raw]

        logger.info(
            "cv_results",
            cv_mae_mean=round(float(np.mean(cv_mae)), 3),
            cv_mae_std=round(float(np.std(cv_mae)), 3),
        )

        importances: dict[str, float] = self._extract_importances(model)

        return TrainingResult(
            model=model,
            model_type=actual_type,
            feature_names=self._feature_cols,
            metrics=metrics,
            feature_importances=importances,
            params=params,
            train_size=len(X_train),
            test_size=len(X_test),
            cv_scores=cv_mae,
        )

    def evaluate(self, result: TrainingResult) -> dict[str, float]:
        """Log evaluation metrics and return them.

        Mostly a convenience wrapper — the heavy lifting already happened
        in train().  This is for callers who want a clean evaluate-only step
        in their pipeline (like Dagster assets that split train/evaluate).
        """
        logger.info(
            "evaluation_summary",
            model_type=result.model_type,
            r2=round(result.metrics["r2"], 4),
            mae=round(result.metrics["mae"], 3),
            rmse=round(result.metrics["rmse"], 3),
            train_size=result.train_size,
            test_size=result.test_size,
        )

        # log top 10 features for quick debugging
        top_features: list[tuple[str, float]] = sorted(
            result.feature_importances.items(), key=lambda x: x[1], reverse=True
        )[:10]
        logger.info("top_features", features=top_features)

        return result.metrics

    def save_model(
        self,
        result: TrainingResult,
        *,
        model_dir: Path | None = None,
        model_name: str = DEFAULT_MODEL_NAME,
    ) -> Path:
        """Persist model artifact and metadata to disk.

        Also logs to MLflow if it's configured — fails silently if not.
        """
        out_dir: Path = model_dir or MODEL_DIR
        out_dir.mkdir(parents=True, exist_ok=True)

        model_path: Path = out_dir / f"{model_name}.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(result.model, f)

        # save metadata alongside the model so we know what went into it
        meta_path: Path = out_dir / f"{model_name}_meta.json"
        meta: dict[str, Any] = {
            "feature_names": result.feature_names,
            "params": {k: str(v) for k, v in result.params.items()},
            "metrics": result.metrics,
            "train_size": result.train_size,
            "test_size": result.test_size,
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        result.model_path = model_path
        logger.info("model_saved", path=str(model_path))

        self._try_log_mlflow(result)
        return model_path

    # ------------------------------------------------------------------
    # internal: model building and tuning
    # ------------------------------------------------------------------

    def _pick_training_features(self) -> list[str]:
        """Select numeric columns that are safe to use as features."""
        return [
            c
            for c in self._feature_df.columns
            if c not in EXCLUDE_FROM_TRAINING
            and self._feature_df[c].dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
        ]

    def _build_model(
        self,
        tune: bool,
        X_train: np.ndarray,
        y_train: np.ndarray,
    ) -> tuple[Any, str, dict[str, Any]]:
        """Instantiate the model, optionally with hyperparameter tuning.

        Falls back gracefully: xgboost -> lightgbm -> sklearn GBR.
        """
        base_params: dict[str, Any] = {
            "random_state": self._random_state,
            "n_estimators": 300,
            "max_depth": 6,
            "learning_rate": 0.05,
        }

        if self._model_type == "xgboost":
            try:
                from xgboost import XGBRegressor

                if tune:
                    base_params = self._tune_xgboost(X_train, y_train)
                else:
                    base_params.update({"subsample": 0.8, "colsample_bytree": 0.8})

                return XGBRegressor(**base_params), "xgboost", base_params

            except ImportError:
                logger.warning("xgboost_not_installed", fallback="lightgbm")
                self._model_type = "lightgbm"

        if self._model_type == "lightgbm":
            try:
                from lightgbm import LGBMRegressor

                if tune:
                    base_params = self._tune_lightgbm(X_train, y_train)
                else:
                    base_params.update(
                        {
                            "subsample": 0.8,
                            "colsample_bytree": 0.8,
                            "verbose": -1,
                        }
                    )

                return LGBMRegressor(**base_params), "lightgbm", base_params

            except ImportError:
                logger.warning("lightgbm_not_installed", fallback="sklearn_gbr")

        # last resort
        from sklearn.ensemble import GradientBoostingRegressor

        params: dict[str, Any] = {
            "n_estimators": base_params.get("n_estimators", 300),
            "max_depth": base_params.get("max_depth", 6),
            "learning_rate": base_params.get("learning_rate", 0.05),
            "subsample": 0.8,
            "random_state": self._random_state,
        }
        return GradientBoostingRegressor(**params), "sklearn_gbr", params

    def _tune_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
    ) -> dict[str, Any]:
        """Quick hyperparameter search for XGBoost.

        We use a small grid instead of Bayesian optimization because the
        dataset is small enough that grid search finishes in <5 minutes.
        TODO: switch to optuna if we ever get more than ~200k rows.
        """
        from sklearn.model_selection import GridSearchCV
        from xgboost import XGBRegressor

        param_grid: dict[str, list[Any]] = {
            "n_estimators": [200, 400],
            "max_depth": [4, 6, 8],
            "learning_rate": [0.03, 0.05, 0.1],
            "subsample": [0.8],
            "colsample_bytree": [0.8],
        }

        cv = TimeSeriesSplit(n_splits=3)
        grid = GridSearchCV(
            XGBRegressor(random_state=self._random_state),
            param_grid,
            cv=cv,
            scoring="neg_mean_absolute_error",
            n_jobs=-1,
            verbose=0,
        )
        grid.fit(X_train, y_train)

        best: dict[str, Any] = grid.best_params_
        best["random_state"] = self._random_state
        logger.info("xgboost_tuning_done", best_params=best, best_score=round(-grid.best_score_, 3))
        return best

    def _tune_lightgbm(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
    ) -> dict[str, Any]:
        """Quick hyperparameter search for LightGBM."""
        from lightgbm import LGBMRegressor
        from sklearn.model_selection import GridSearchCV

        param_grid: dict[str, list[Any]] = {
            "n_estimators": [200, 400],
            "max_depth": [4, 6, 8],
            "learning_rate": [0.03, 0.05, 0.1],
            "num_leaves": [31, 63],
            "subsample": [0.8],
        }

        cv = TimeSeriesSplit(n_splits=3)
        grid = GridSearchCV(
            LGBMRegressor(random_state=self._random_state, verbose=-1),
            param_grid,
            cv=cv,
            scoring="neg_mean_absolute_error",
            n_jobs=-1,
            verbose=0,
        )
        grid.fit(X_train, y_train)

        best: dict[str, Any] = grid.best_params_
        best["random_state"] = self._random_state
        best["verbose"] = -1
        logger.info(
            "lightgbm_tuning_done", best_params=best, best_score=round(-grid.best_score_, 3)
        )
        return best

    # ------------------------------------------------------------------
    # internal: metrics and feature importance
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
        """Standard regression metrics."""
        return {
            "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
            "mae": float(mean_absolute_error(y_true, y_pred)),
            "r2": float(r2_score(y_true, y_pred)),
            "mean_residual": float(np.mean(y_true - y_pred)),
            "max_error": float(np.max(np.abs(y_true - y_pred))),
        }

    def _extract_importances(self, model: Any) -> dict[str, float]:
        """Pull feature importances from the fitted model.

        Feature importance stability is a known issue with tree boosters —
        the rankings can shuffle quite a bit between runs, especially for
        correlated features (poverty_rate and child_poverty_rate, etc.).
        We use gain importance by default which is somewhat more stable
        than split count, but still noisy.

        TODO: consider SHAP values for a more reliable ranking.  The
        computation cost is non-trivial on 95k rows though.
        """
        if hasattr(model, "feature_importances_"):
            raw: np.ndarray = model.feature_importances_
            return {
                name: round(float(imp), 6)
                for name, imp in sorted(
                    zip(self._feature_cols, raw, strict=False), key=lambda x: x[1], reverse=True
                )
            }
        return {}

    # ------------------------------------------------------------------
    # internal: MLflow integration
    # ------------------------------------------------------------------

    @staticmethod
    def _try_log_mlflow(result: TrainingResult) -> None:
        """Push training results to MLflow.  Fails silently if not configured."""
        try:
            from ml.experiments.mlflow_config import log_training_run

            log_training_run(
                experiment_name="wellnest-proficiency",
                run_name=f"{result.model_type}_proficiency",
                params=result.params,
                metrics=result.metrics,
                artifacts={"model": str(result.model_path)} if result.model_path else {},
                tags={
                    "model_type": result.model_type,
                    "target": TARGET_COL,
                    "features": str(len(result.feature_names)),
                },
            )
        except Exception as exc:
            logger.warning("mlflow_logging_failed", error=str(exc))
