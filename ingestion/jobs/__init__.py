from dagster import AssetSelection, define_asset_job

lei_records_landing = AssetSelection.assets("lei_records_landing")

ingestion_job = define_asset_job(name="ingestion_job", selection=lei_records_landing)
