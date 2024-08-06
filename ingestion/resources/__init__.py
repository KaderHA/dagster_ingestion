from typing import Dict
from dagster import ConfigurableResource, RunConfig
from pydantic import Field

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

databricks_client_resource = WorkspaceClient()


class DatabricksResource(ConfigurableResource):
    source: str = Field(description=("Path to source data"))
    dest: str = Field(description=("Path to destination data"))
    notebook_path: str = Field(description=("Path to notebook on Databricks"))
    cluster_id: str = Field(description=("Databricks cluster id"))

    def launch_databricks_notebook(self, params: Dict[str, str]):
        params["src"] = self.source
        params["dest"] = self.dest
        task = jobs.SubmitTask(
            task_key=self.notebook_path.split("/").pop(),
            existing_cluster_id=self.cluster_id,
            notebook_task=jobs.NotebookTask(
                notebook_path=self.notebook_path, base_parameters=params
            ),
        )

        databricks_client_resource.jobs.submit(
            run_name="dagster_pipes_job", tasks=[task]
        ).result()


dbx_landing_resource = DatabricksResource(
    source="https://goldencopy.gleif.org/api/v2/golden-copies/publishes",
    dest="abfss://demo@saintern.dfs.core.windows.net",
    notebook_path="/Users/wm1372b@norges-bank.no/.bundle/ingestion/dev/files/src/ingestion/gleif/gleif-lei-records_daily",
    cluster_id="0801-080028-op8xsvb6",
)

dbx_landing_resource_asset_check = DatabricksResource(
    source="https://goldencopy.gleif.org/api/v2/golden-copies/publishes",
    dest="abfss://demo@saintern.dfs.core.windows.net",
    notebook_path="/Users/wm1372b@norges-bank.no/.bundle/ingestion/dev/files/src/ingestion/gleif/gleif-lei-records_daily_asset_check",
    cluster_id="0801-080028-op8xsvb6",
)