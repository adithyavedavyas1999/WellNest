"""
Source connectors for all WellNest data pipelines.

Each connector follows a rough extract-validate-load pattern, though the
specifics vary by data source (some are CSV downloads, others are APIs
with pagination, etc.).  Call connector.run() for the full pipeline.

Usage::

    from ingestion.sources import NCESCCDConnector, CDCPlacesConnector

    ccd = NCESCCDConnector(survey_year="2022-23")
    rows = ccd.run()
"""

from ingestion.sources.cdc_env_health import CDCEnvHealthConnector
from ingestion.sources.cdc_places import CDCPlacesConnector
from ingestion.sources.census_acs import CensusACSConnector
from ingestion.sources.epa_airnow import EPAAirNowConnector
from ingestion.sources.fbi_ucr import FBIUCRConnector
from ingestion.sources.fema_nri import FEMANRIConnector
from ingestion.sources.hrsa_hpsa import HRSAHPSAConnector
from ingestion.sources.hrsa_mua import HRSAMUAConnector
from ingestion.sources.nces_ccd import NCESCCDConnector
from ingestion.sources.nces_edge import NCESEdgeConnector
from ingestion.sources.noaa_nws_alerts import NOAANWSAlertsConnector
from ingestion.sources.usda_food_access import USDAFoodAccessConnector

__all__ = [
    "CDCEnvHealthConnector",
    "CDCPlacesConnector",
    "CensusACSConnector",
    "EPAAirNowConnector",
    "FBIUCRConnector",
    "FEMANRIConnector",
    "HRSAHPSAConnector",
    "HRSAMUAConnector",
    "NCESCCDConnector",
    "NCESEdgeConnector",
    "NOAANWSAlertsConnector",
    "USDAFoodAccessConnector",
]

# maps a short name to the connector class -- used by the dagster asset factory
CONNECTOR_REGISTRY: dict[str, type] = {
    "nces_ccd": NCESCCDConnector,
    "nces_edge": NCESEdgeConnector,
    "cdc_places": CDCPlacesConnector,
    "cdc_env_health": CDCEnvHealthConnector,
    "census_acs": CensusACSConnector,
    "epa_airnow": EPAAirNowConnector,
    "hrsa_hpsa": HRSAHPSAConnector,
    "hrsa_mua": HRSAMUAConnector,
    "usda_food_access": USDAFoodAccessConnector,
    "fema_nri": FEMANRIConnector,
    "noaa_nws_alerts": NOAANWSAlertsConnector,
    "fbi_ucr": FBIUCRConnector,
}
