import sys

from dagster import (
    AssetCheckExecutionContext,
    AssetExecutionContext,
    Config,
    ResourceParam,
    asset,
    asset_check,
    open_pipes_session,
)

from pydantic import Field

from databricks.sdk import WorkspaceClient

from dagster_databricks.pipes import (
    PipesDbfsContextInjector,
    PipesDbfsLogReader,
    PipesDbfsMessageReader,
)

from ingestion.resources import DatabricksResource
<<<<<<< Updated upstream


class NotebookConfig(Config):
    source: str = Field(description=("Path to source data"))
    dest: str = Field(description=("Path to destination data"))
    notebook_path: str = Field(description=("Path to notebook on Databricks"))
    cluster_id: str = Field(description=("Databricks cluster id"))

=======
>>>>>>> Stashed changes

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

<<<<<<< Updated upstream

@asset_check(asset=lei_records_landing, blocking=False)
def target_has_no_nulls(
    context: AssetCheckExecutionContext,
    dbx_client: ResourceParam[WorkspaceClient],
    asset_check_landing: DatabricksResource,
=======
@asset_check(asset=lei_records_landing)
def target_has_no_nulls(
    context: AssetCheckExecutionContext,
    dbx_client: ResourceParam[WorkspaceClient],
    landing_asset_check: DatabricksResource,
>>>>>>> Stashed changes
):
    """Ingests lei records"""
    with open_pipes_session(
        context=context._op_execution_context,
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
<<<<<<< Updated upstream
        asset_check_landing.launch_databricks_notebook(env_vars)

    yield from pipes_session.get_results()
=======
        landing_asset_check.launch_databricks_notebook(env_vars)

    yield from pipes_session.get_results()

>>>>>>> Stashed changes
