"""
WellNest ingestion layer.

Responsible for pulling data from 12 federal sources into the raw Postgres
schema.  Each source gets its own connector in ingestion/sources/ and its
own JSON schema contract in ingestion/schemas/.

The main entry point for the full pipeline is the Dagster asset definitions
in orchestration/, but each connector can be run standalone for testing::

    from ingestion.sources import NCESCCDConnector
    NCESCCDConnector().run()
"""
