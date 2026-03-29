"""
Router subpackage — each module defines an APIRouter that gets included in main.py.
"""

from api.routers.ask import router as ask_router
from api.routers.counties import router as counties_router
from api.routers.health import router as health_router
from api.routers.predictions import router as predictions_router
from api.routers.reports import router as reports_router
from api.routers.schools import router as schools_router
from api.routers.search import router as search_router

__all__ = [
    "ask_router",
    "counties_router",
    "health_router",
    "predictions_router",
    "reports_router",
    "schools_router",
    "search_router",
]
