"""Dashboard utility modules — DB access and caching."""

from dashboard.utils.cache import TTLCache, check_staleness, format_freshness
from dashboard.utils.db import (
    check_db_health,
    get_data_freshness,
    get_engine,
    get_school_detail,
    get_states,
    run_query,
)

__all__ = [
    "TTLCache",
    "check_db_health",
    "check_staleness",
    "format_freshness",
    "get_data_freshness",
    "get_engine",
    "get_school_detail",
    "get_states",
    "run_query",
]
