from dagster import ScheduleDefinition
from ingestion.jobs import ingestion_job


daily_update_job = ScheduleDefinition(job=ingestion_job, cron_schedule="0 0,8,16 * * *")
