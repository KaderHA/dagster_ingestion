from dagster import Definitions, load_assets_from_modules

from .assets import ingest_to_landing
from .resources import (
    databricks_client_resource,
    dbx_landing_resource,
)
from .jobs import ingestion_job
from .schedules import daily_update_job

all_assets = load_assets_from_modules([ingest_to_landing])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbx_client": databricks_client_resource,
        "landing": dbx_landing_resource,
    },
    jobs=[ingestion_job],
    schedules=[daily_update_job],
)
