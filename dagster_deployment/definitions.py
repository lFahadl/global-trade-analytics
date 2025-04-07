from dagster import Definitions

# Import specific assets from ingestion module
from ingestion.assets import load_gcs_to_bigquery_tables, find_source_tables
from ingestion.assets import combined_trade_data, country_year_metrics, optimize_combined_table

# Import specific assets from analytics module
from analytics.views import v_global_yearly_metrics_view

# Import resources
from ingestion.resources import bigquery_resource

# Combine all the requested assets
all_assets = [
    # Ingestion assets
    load_gcs_to_bigquery_tables,
    find_source_tables,
    combined_trade_data,
    country_year_metrics,
    optimize_combined_table,
    
    # Analytics assets
    v_global_yearly_metrics_view
]

# Define the Dagster definitions that will be loaded by the Dagster instance
defs = Definitions(
    assets=all_assets,
    resources={
        "bq_resource": bigquery_resource,
    },
)
