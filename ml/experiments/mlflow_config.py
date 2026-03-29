"""
MLflow configuration and experiment management helpers.

We use MLflow's local file backend (no server) to keep things simple.  The
tracking URI points to a directory under the project root — everything is
just files on disk.  This makes it easy to inspect runs, copy artifacts,
and avoid running yet another service in dev.

If you need a remote tracking server in prod, set MLFLOW_TRACKING_URI in
the environment and this module will respect it.  But honestly for a team
of 2-3 people the file backend has been fine.

Experiment naming convention: "wellnest-{model_type}" e.g.,
  - wellnest-proficiency
  - wellnest-anomaly

TODO: figure out model registry.  MLflow's built-in registry requires the
tracking server, and I'm not sure it's worth the operational overhead for
our use case.  For now we just version by run_id.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# default to a local directory relative to the project root
_DEFAULT_TRACKING_DIR: Path = Path(__file__).resolve().parent.parent.parent / "mlruns"
_DEFAULT_ARTIFACT_DIR: Path = Path(__file__).resolve().parent.parent / "artifacts"


def get_tracking_uri() -> str:
    """Resolve the MLflow tracking URI.

    Checks MLFLOW_TRACKING_URI env var first, falls back to a local file
    path.  The env var is how you'd switch to a remote server in prod
    without touching code.
    """
    env_uri: str | None = os.environ.get("MLFLOW_TRACKING_URI")
    if env_uri:
        return env_uri

    _DEFAULT_TRACKING_DIR.mkdir(parents=True, exist_ok=True)
    return str(_DEFAULT_TRACKING_DIR)


def get_or_create_experiment(experiment_name: str) -> str:
    """Get an experiment by name, creating it if it doesn't exist.

    Returns the experiment ID as a string (MLflow's convention).
    """
    import mlflow

    mlflow.set_tracking_uri(get_tracking_uri())

    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is not None:
        return experiment.experiment_id

    experiment_id: str = mlflow.create_experiment(
        experiment_name,
        artifact_location=str(_DEFAULT_ARTIFACT_DIR / experiment_name),
    )
    logger.info("mlflow_experiment_created", name=experiment_name, id=experiment_id)
    return experiment_id


def log_training_run(
    *,
    experiment_name: str,
    run_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    artifacts: dict[str, str] | None = None,
    tags: dict[str, str] | None = None,
) -> str:
    """Log a complete training run to MLflow.

    This is the main entry point for the training code.  It creates a run,
    logs params/metrics/artifacts, and returns the run ID.

    We stringify params because MLflow doesn't handle non-primitive types
    well (lists, numpy types, etc.).  This is annoying but not worth
    fighting with MLflow's type system over.
    """
    import mlflow

    mlflow.set_tracking_uri(get_tracking_uri())
    experiment_id: str = get_or_create_experiment(experiment_name)

    with mlflow.start_run(
        experiment_id=experiment_id,
        run_name=run_name,
    ) as run:

        safe_params: dict[str, str] = {k: str(v) for k, v in params.items()}
        mlflow.log_params(safe_params)

        mlflow.log_metrics(metrics)

        if tags:
            mlflow.set_tags(tags)

        if artifacts:
            for name, path in artifacts.items():
                artifact_path: Path = Path(path)
                if artifact_path.exists():
                    mlflow.log_artifact(str(artifact_path), artifact_path=name)
                else:
                    logger.warning(
                        "mlflow_artifact_missing",
                        name=name,
                        path=path,
                    )

        run_id: str = run.info.run_id

    logger.info(
        "mlflow_run_logged",
        experiment=experiment_name,
        run_id=run_id,
        metrics=metrics,
    )

    return run_id


def log_artifact_file(
    experiment_name: str,
    run_id: str,
    file_path: str | Path,
    artifact_subdir: str = "model",
) -> None:
    """Log a single artifact file to an existing MLflow run.

    Useful for adding artifacts after the initial training run — e.g.,
    logging a SHAP summary plot or the feature importance CSV.
    """
    import mlflow

    mlflow.set_tracking_uri(get_tracking_uri())

    path: Path = Path(file_path)
    if not path.exists():
        logger.warning("artifact_file_not_found", path=str(path))
        return

    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(str(path), artifact_path=artifact_subdir)

    logger.info("mlflow_artifact_added", run_id=run_id, file=str(path))


def list_runs(
    experiment_name: str,
    *,
    max_results: int = 20,
) -> list[dict[str, Any]]:
    """List recent runs for an experiment, sorted by start time.

    Returns a simplified dict for each run — useful for the dashboard
    and API when showing model training history.
    """
    import mlflow
    from mlflow.entities import ViewType

    mlflow.set_tracking_uri(get_tracking_uri())

    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        return []

    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        max_results=max_results,
        run_view_type=ViewType.ACTIVE_ONLY,
        order_by=["start_time DESC"],
    )

    if runs.empty:
        return []

    results: list[dict[str, Any]] = []
    for _, row in runs.iterrows():
        results.append({
            "run_id": row.get("run_id"),
            "run_name": row.get("tags.mlflow.runName"),
            "status": row.get("status"),
            "start_time": str(row.get("start_time")),
            "metrics": {
                k.replace("metrics.", ""): v
                for k, v in row.items()
                if str(k).startswith("metrics.") and v is not None
            },
            "params": {
                k.replace("params.", ""): v
                for k, v in row.items()
                if str(k).startswith("params.") and v is not None
            },
        })

    return results


def get_best_run(
    experiment_name: str,
    metric: str = "mae",
    *,
    lower_is_better: bool = True,
) -> dict[str, Any] | None:
    """Find the best run for an experiment based on a metric.

    Used by the serving module to figure out which model to load.
    """
    runs: list[dict[str, Any]] = list_runs(experiment_name)
    if not runs:
        return None

    valid_runs: list[dict[str, Any]] = [
        r for r in runs if metric in r.get("metrics", {})
    ]
    if not valid_runs:
        return None

    key_fn = lambda r: r["metrics"][metric]
    best: dict[str, Any] = min(valid_runs, key=key_fn) if lower_is_better else max(valid_runs, key=key_fn)

    logger.info(
        "best_run_found",
        experiment=experiment_name,
        run_id=best.get("run_id"),
        metric_value=best["metrics"][metric],
    )
    return best
