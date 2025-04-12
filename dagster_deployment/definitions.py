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

# Job 3: Basic analytics job that creates country year metrics and global yearly metrics view
basic_analytics_job = define_asset_job(
    name="basic_analytics_job",
    selection=AssetSelection.assets(country_year_metrics, v_global_yearly_metrics_view),
    description="Creates basic analytics views for dashboards",
)

# Job 4: Product metrics job that calculates product-related metrics
product_metrics_job = define_asset_job(
    name="product_metrics_job",
    selection=AssetSelection.assets(
        calculate_product_year_metrics,
        calculate_product_complexity,
        create_export_portfolio_view
    ),
    description="Calculates product-related metrics and views",
)

# Job 5: Trade relationship job that calculates bilateral flows and partner diversification
trade_relationship_job = define_asset_job(
    name="trade_relationship_job",
    selection=AssetSelection.assets(
        calculate_bilateral_flows,
        calculate_partner_diversification,
        create_diversification_complexity_view
    ),
    description="Analyzes trade relationships between countries",
)

# Job 6: Economic complexity job that calculates complexity dynamics and export specialization
economic_complexity_job = define_asset_job(
    name="economic_complexity_job",
    selection=AssetSelection.assets(
        calculate_complexity_dynamics,
        calculate_export_specialization,
        create_eci_balance_trends_view
    ),
    description="Analyzes economic complexity indicators",
)

# Job 7: Advanced analytics job that calculates export portfolio and COI analysis
advanced_analytics_job = define_asset_job(
    name="advanced_analytics_job",
    selection=AssetSelection.assets(
        calculate_export_portfolio,
        calculate_coi_growth_analysis,
        create_coi_predictive_power_view,
        create_portfolio_evolution_view
    ),
    description="Performs advanced analytics on trade data",
)

# Job 8: Full analytics job that runs all analytics assets
full_analytics_job = define_asset_job(
    name="full_analytics_job",
    selection=AssetSelection.assets(
        country_year_metrics,
        v_global_yearly_metrics_view,
        calculate_product_year_metrics,
        calculate_bilateral_flows,
        calculate_export_specialization,
        calculate_complexity_dynamics,
        calculate_product_complexity,
        calculate_export_portfolio,
        calculate_partner_diversification,
        calculate_coi_growth_analysis,
        create_eci_balance_trends_view,
        create_export_portfolio_view,
        create_diversification_complexity_view,
        create_coi_predictive_power_view,
        create_portfolio_evolution_view
    ),
    description="Creates all analytics assets for comprehensive analysis",
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
        basic_analytics_job,
        product_metrics_job,
        trade_relationship_job,
        economic_complexity_job,
        advanced_analytics_job,
        full_analytics_job,
    ],
)
