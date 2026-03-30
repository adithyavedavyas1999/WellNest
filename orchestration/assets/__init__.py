"""
Asset registry — collects all asset groups for the Definitions object.

Import paths are explicit here so it's obvious which modules contribute
assets.  If you add a new asset file, add its ALL_*_ASSETS list here.
"""

from orchestration.assets.ai_assets import ALL_AI_ASSETS
from orchestration.assets.bronze import ALL_BRONZE_ASSETS
from orchestration.assets.gold import ALL_GOLD_ASSETS
from orchestration.assets.ml_assets import ALL_ML_ASSETS
from orchestration.assets.quality_assets import ALL_QUALITY_ASSETS
from orchestration.assets.silver import ALL_SILVER_ASSETS

ALL_ASSETS: list = (
    ALL_BRONZE_ASSETS
    + ALL_SILVER_ASSETS
    + ALL_GOLD_ASSETS
    + ALL_ML_ASSETS
    + ALL_AI_ASSETS
    + ALL_QUALITY_ASSETS
)

__all__ = [
    "ALL_AI_ASSETS",
    "ALL_ASSETS",
    "ALL_BRONZE_ASSETS",
    "ALL_GOLD_ASSETS",
    "ALL_ML_ASSETS",
    "ALL_QUALITY_ASSETS",
    "ALL_SILVER_ASSETS",
]
