from dagster import EnvVar
from dagster_gcp import BigQueryResource

# Define BigQuery resource with project ID and credentials path
bigquery_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT_ID"),
    gcp_credentials=EnvVar("GOOGLE_APPLICATION_CREDENTIALS"),
)