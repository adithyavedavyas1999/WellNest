"""
WellNest ML layer — predictive models and anomaly detection for child wellbeing.

Three main subsystems:
  features/   Build the wide feature matrix from silver/gold Postgres tables.
  training/   XGBoost proficiency predictor + Isolation Forest anomaly detector.
  serving/    Load trained artifacts, batch-score all schools, write predictions.
  experiments/ MLflow plumbing for experiment tracking.

The pipeline is designed around a strict temporal split: we never let future
data leak into training.  This sounds obvious but it's surprisingly easy to
break when you have lag features and imputation happening in the same pipeline.
"""
