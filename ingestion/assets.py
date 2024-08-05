from typing import Dict
import sys

from dagster import AssetExecutionContext, ResourceParam, asset, open_pipes_session

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from dagster_databricks.pipes import (
    PipesDbfsContextInjector,
    PipesDbfsLogReader,
    PipesDbfsMessageReader,
)

from ingestion.resources import DatabricksResource


@asset
def lei_records_landing(
    context: AssetExecutionContext,
    dbx_client: ResourceParam[WorkspaceClient],
    landing: DatabricksResource,
):
    """Ingests lei records"""
    with open_pipes_session(
        context=context,
        extras={"foo": "bar"},
        context_injector=PipesDbfsContextInjector(client=dbx_client),
        message_reader=PipesDbfsMessageReader(
            client=dbx_client,
            log_readers=[
                PipesDbfsLogReader(
                    client=dbx_client,
                    remote_log_name="stdout",
                    target_stream=sys.stdout,
                ),
                PipesDbfsLogReader(
                    client=dbx_client,
                    remote_log_name="stderr",
                    target_stream=sys.stderr,
                ),
            ],
        ),
    ) as pipes_session:
        env_vars = pipes_session.get_bootstrap_env_vars()
        landing.launch_databricks_notebook(env_vars)

    yield from pipes_session.get_results()
