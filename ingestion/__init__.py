from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import (
    databricks_client_resource,
    dbx_landing_resource,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "dbx_client": databricks_client_resource,
        "landing": dbx_landing_resource,
    },
)
