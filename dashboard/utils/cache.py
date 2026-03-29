"""
Caching utilities beyond what st.cache_data gives us.

The main addition here is a simple TTL wrapper for API responses (used when
the dashboard hits the FastAPI backend directly instead of going through SQL)
and a freshness checker that shows the user how stale the displayed data is.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

import streamlit as st

logger = logging.getLogger("wellnest.dashboard.cache")


class TTLCache:
    """
    Dead-simple in-memory TTL cache. We store it on st.session_state so it
    survives across reruns within a single user session but gets cleared when
    the session ends.

    Not meant for large datasets — use st.cache_data for that. This is for
    small API responses (stats, AI briefs, etc.) where we want explicit
    control over invalidation.
    """

    def __init__(self, namespace: str = "default", ttl_seconds: int = 300):
        self.namespace = namespace
        self.ttl = ttl_seconds
        self._state_key = f"_ttl_cache_{namespace}"

    @property
    def _store(self) -> dict:
        if self._state_key not in st.session_state:
            st.session_state[self._state_key] = {}
        return st.session_state[self._state_key]

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None:
            return None
        if time.time() - entry["ts"] > self.ttl:
            del self._store[key]
            return None
        return entry["value"]

    def set(self, key: str, value: Any) -> None:
        self._store[key] = {"value": value, "ts": time.time()}

    def get_or_fetch(self, key: str, fetch_fn: Callable[[], Any]) -> Any:
        """Return cached value or call fetch_fn, cache the result, and return it."""
        cached = self.get(key)
        if cached is not None:
            return cached
        value = fetch_fn()
        self.set(key, value)
        return value

    def invalidate(self, key: Optional[str] = None) -> None:
        if key is None:
            st.session_state[self._state_key] = {}
        elif key in self._store:
            del self._store[key]


def format_freshness(timestamp_str: Optional[str]) -> str:
    """
    Turn a timestamp into a human-friendly "X ago" string.

    Returns "Unknown" if we can't parse it. The dashboard sidebar uses this
    to show when data was last refreshed.
    """
    if not timestamp_str:
        return "Unknown"

    try:
        ts = datetime.fromisoformat(str(timestamp_str).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        delta = now - ts

        seconds = int(delta.total_seconds())
        if seconds < 60:
            return "Just now"
        elif seconds < 3600:
            mins = seconds // 60
            return f"{mins}m ago"
        elif seconds < 86400:
            hours = seconds // 3600
            return f"{hours}h ago"
        else:
            days = seconds // 86400
            return f"{days}d ago"
    except (ValueError, TypeError):
        return "Unknown"


def check_staleness(timestamp_str: Optional[str], threshold_hours: int = 48) -> bool:
    """
    Returns True if data is older than threshold_hours.
    Used to show a warning banner when the pipeline hasn't run recently.
    """
    if not timestamp_str:
        return True
    try:
        ts = datetime.fromisoformat(str(timestamp_str).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
        return age_hours > threshold_hours
    except (ValueError, TypeError):
        return True
