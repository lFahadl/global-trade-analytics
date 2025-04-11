from dagster import EnvVar
from dagster_gcp import BigQueryResource


bigquery_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT_ID"),
)
