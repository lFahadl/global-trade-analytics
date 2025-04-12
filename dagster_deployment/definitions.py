from dagster import Definitions, define_asset_job, AssetSelection

# Import specific assets from ingestion module
from ingestion.assets import load_gcs_to_bigquery_tables, find_source_tables
from ingestion.assets import combined_trade_data, country_year_metrics, optimize_combined_table

# Import specific assets from analytics module
from analytics.views import v_global_yearly_metrics_view, create_eci_balance_trends_view, create_export_portfolio_view
from analytics.views import create_diversification_complexity_view, create_coi_predictive_power_view, create_portfolio_evolution_view
from analytics.assets import calculate_product_year_metrics, calculate_bilateral_flows, calculate_export_specialization, calculate_complexity_dynamics
from analytics.assets2 import calculate_product_complexity, calculate_export_portfolio, calculate_partner_diversification, calculate_coi_growth_analysis

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
    
    # Analytics assets from views.py
    v_global_yearly_metrics_view,
    create_eci_balance_trends_view,
    create_export_portfolio_view,
    create_diversification_complexity_view,
    create_coi_predictive_power_view,
    create_portfolio_evolution_view,
    
    # Analytics assets from assets.py
    calculate_product_year_metrics,
    calculate_bilateral_flows,
    calculate_export_specialization,
    calculate_complexity_dynamics,
    
    # Analytics assets from assets2.py
    calculate_product_complexity,
    calculate_export_portfolio,
    calculate_partner_diversification,
    calculate_coi_growth_analysis
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
