from dagster import Definitions, define_asset_job, AssetSelection

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

# Define jobs
# Job 1: Data ingestion job that loads data from GCS to BigQuery
data_ingestion_job = define_asset_job(
    name="data_ingestion_job",
    selection=AssetSelection.assets(load_gcs_to_bigquery_tables),
    description="Loads data from Google Cloud Storage to BigQuery tables",
)

# Job 2: Combined table job that finds source tables, creates combined table, and optimizes it
combined_table_job = define_asset_job(
    name="combined_table_job",
    selection=AssetSelection.assets(find_source_tables, combined_trade_data, optimize_combined_table),
    description="Creates and optimizes the combined trade data table",
)

# Job 3: Analytics job that creates country year metrics and global yearly metrics view
analytics_job = define_asset_job(
    name="analytics_job",
    selection=AssetSelection.assets(country_year_metrics, v_global_yearly_metrics_view),
    description="Creates analytics views for dashboards",
)

# Define the Dagster definitions that will be loaded by the Dagster instance
defs = Definitions(
    assets=all_assets,
    resources={
        "bq_resource": bigquery_resource,
    },
    jobs=[
        data_ingestion_job,
        combined_table_job,
        analytics_job,
    ],
)
