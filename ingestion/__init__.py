from dagster import Definitions, load_assets_from_modules

from .assets import ingest_to_landing
from .resources import (
    databricks_client_resource,
    dbx_landing_resource,
    dbx_landing_resource_asset_check,
)
from .jobs import ingestion_job
from .schedules import daily_update_job

all_assets = load_assets_from_modules([ingest_to_landing])

defs = Definitions(
    assets=all_assets,
    asset_checks=[ingest_to_landing.target_has_no_nulls],
    resources={
        "dbx_client": databricks_client_resource,
        "landing": dbx_landing_resource,
        "landing_asset_check": dbx_landing_resource_asset_check,
    },
    jobs=[ingestion_job],
    schedules=[daily_update_job],
)
