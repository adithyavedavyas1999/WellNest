"""
Main Dagster Definitions — the single entry point that dagster loads.

Point dagster at this file:
    dagster dev -m orchestration.definitions

Or set DAGSTER_MODULE_NAME=orchestration.definitions in your env.

Everything gets wired together here: assets, resources, schedules, sensors.
The actual logic lives in the submodules — this file is just plumbing.
"""

from __future__ import annotations

from dagster import Definitions

from orchestration.assets import ALL_ASSETS
from orchestration.resources import build_resources
from orchestration.schedules import ALL_JOBS, ALL_SCHEDULES
from orchestration.sensors import ALL_SENSORS, SENSOR_JOBS

defs = Definitions(
    assets=ALL_ASSETS,
    resources=build_resources(),
    schedules=ALL_SCHEDULES,
    sensors=ALL_SENSORS,
    jobs=ALL_JOBS + SENSOR_JOBS,
)
