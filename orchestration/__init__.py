"""
WellNest orchestration layer — Dagster pipelines for the full data stack.

Modules:
    definitions  — Dagster Definitions object (entry point)
    resources    — database, dbt, HTTP, OpenAI resource configs
    assets/      — asset definitions organized by layer (bronze/silver/gold/ml/ai/quality)
    schedules    — cron-based pipeline triggers
    sensors      — event-driven pipeline triggers

Run locally:
    dagster dev -m orchestration.definitions

Deploy (Dagster Cloud / K8s):
    Set DAGSTER_MODULE_NAME=orchestration.definitions
"""
