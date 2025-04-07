from dagster import Definitions

from . import assets
from .resources import bigquery_resource

# Load specific assets from the assets module
ingestion_assets = [
    assets.load_gcs_to_bigquery_tables,
    assets.find_source_tables,
    assets.combined_trade_data,
    assets.country_year_metrics,
    assets.optimize_combined_table
]

# Define the Dagster definitions for the ingestion module
defs = Definitions(
    assets=ingestion_assets,
    resources={
        "bq_resource": bigquery_resource,
    },
)
