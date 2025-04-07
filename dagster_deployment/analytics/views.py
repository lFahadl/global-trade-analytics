from typing import Dict, List, Tuple, Any, Optional

from dagster import asset, AssetExecutionContext, EnvVar, Output
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

# Import dependencies
from analytics.assets import country_year_metrics, calculate_complexity_dynamics
from analytics.assets_part2 import calculate_export_portfolio, calculate_partner_diversification
from analytics.assets_part2 import calculate_product_complexity, calculate_coi_growth_analysis


@asset(
    description="Creates a global yearly metrics view for dashboard performance",
    deps=[country_year_metrics]
)
def v_global_yearly_metrics_view(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:

    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    analytics_dataset: str = EnvVar("ANALYTICS_DATASET").get_value()
    view_name: str = "v_global_yearly_metrics"
    
    view_ref: str = f"{project_id}.{analytics_dataset}.{view_name}"

    query: str = f"""
    SELECT
      year,
      SUM(total_exports) as global_exports,
      SUM(total_imports) as global_imports,
      SUM(total_exports) as global_trade_volume, -- Use global exports as the measure
      AVG(eci) as avg_eci
    FROM `{processed_dataset}.country_year_metrics`
    GROUP BY year;
    """

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=bq.QueryJobConfig(
            destination=view_ref,
            write_disposition="WRITE_TRUNCATE",
        ))
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
    return view_ref


@asset(
    description="Creates an ECI vs. trade balance trends view",
    deps=[calculate_complexity_dynamics]
)
def create_eci_balance_trends_view(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Creates a view for analyzing economic complexity vs. trade balance trends.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    analytics_dataset: str = EnvVar("ANALYTICS_DATASET").get_value()
    dynamics_table_name: str = "complexity_dynamics"
    view_name: str = "v_eci_balance_trends"
    
    dynamics_table_ref: str = f"{project_id}.{processed_dataset}.{dynamics_table_name}"
    view_ref: str = f"{project_id}.{analytics_dataset}.{view_name}"

    context.log.info(f"Creating ECI balance trends view {view_ref}...")

    query: str = f"""
    CREATE OR REPLACE VIEW `{view_ref}` AS
    SELECT
      country_id,
      year,
      eci,
      eci_change,
      trade_balance,
      balance_change,
      trend_category
    FROM `{processed_dataset}.{dynamics_table_name}`
    ORDER BY year DESC, country_id;
    """

    context.log.info(f"Executing query: \n{query}")

    with bq_resource.get_client() as client:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated view: {view_ref}")
        return view_ref


@asset(
    description="Creates an export complexity portfolio view",
    deps=[calculate_export_portfolio, calculate_complexity_dynamics]
)
def create_export_portfolio_view(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Creates a view for analyzing export complexity portfolio.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    analytics_dataset: str = EnvVar("ANALYTICS_DATASET").get_value()
    portfolio_table_name: str = "export_portfolio"
    dynamics_table_name: str = "complexity_dynamics"
    view_name: str = "v_export_complexity_portfolio"
    
    portfolio_table_ref: str = f"{project_id}.{processed_dataset}.{portfolio_table_name}"
    dynamics_table_ref: str = f"{project_id}.{processed_dataset}.{dynamics_table_name}"
    view_ref: str = f"{project_id}.{analytics_dataset}.{view_name}"

    context.log.info(f"Creating export complexity portfolio view {view_ref}...")

    query: str = f"""
    CREATE OR REPLACE VIEW `{view_ref}` AS
    SELECT
      ep.country_id,
      ep.year,
      ep.high_complexity_share,
      ep.medium_high_share,
      ep.medium_low_share,
      ep.low_complexity_share,
      ep.weighted_complexity,
      cd.eci
    FROM `{processed_dataset}.{portfolio_table_name}` ep
    JOIN `{processed_dataset}.{dynamics_table_name}` cd
      ON ep.country_id = cd.country_id AND ep.year = cd.year
    ORDER BY cd.eci DESC, ep.country_id;
    """

    context.log.info(f"Executing query: \n{query}")

    with bq_resource.get_client() as client:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated view: {view_ref}")
        return view_ref


@asset(
    description="Creates a partner diversification vs. economic complexity view",
    deps=[calculate_partner_diversification, calculate_complexity_dynamics]
)
def create_diversification_complexity_view(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Creates a view for analyzing partner diversification vs. economic complexity.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    analytics_dataset: str = EnvVar("ANALYTICS_DATASET").get_value()
    diversification_table_name: str = "partner_diversification"
    dynamics_table_name: str = "complexity_dynamics"
    view_name: str = "v_diversification_complexity"
    
    diversification_table_ref: str = f"{project_id}.{processed_dataset}.{diversification_table_name}"
    dynamics_table_ref: str = f"{project_id}.{processed_dataset}.{dynamics_table_name}"
    view_ref: str = f"{project_id}.{analytics_dataset}.{view_name}"

    context.log.info(f"Creating diversification complexity view {view_ref}...")

    query: str = f"""
    CREATE OR REPLACE VIEW `{view_ref}` AS
    SELECT
      pd.country_id,
      pd.year,
      pd.partner_count,
      pd.partner_hhi,
      pd.concentration_category,
      cd.eci,
      cd.eci_change
    FROM `{processed_dataset}.{diversification_table_name}` pd
    JOIN `{processed_dataset}.{dynamics_table_name}` cd
      ON pd.country_id = cd.country_id AND pd.year = cd.year
    ORDER BY pd.year, pd.country_id;
    """

    context.log.info(f"Executing query: \n{query}")

    with bq_resource.get_client() as client:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated view: {view_ref}")
        return view_ref


@asset(
    description="Creates a complexity outlook index predictive power view",
    deps=[calculate_coi_growth_analysis]
)
def create_coi_predictive_power_view(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Creates a view for analyzing complexity outlook index predictive power.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    analytics_dataset: str = EnvVar("ANALYTICS_DATASET").get_value()
    growth_table_name: str = "coi_growth_analysis"
    view_name: str = "v_coi_predictive_power"
    
    growth_table_ref: str = f"{project_id}.{processed_dataset}.{growth_table_name}"
    view_ref: str = f"{project_id}.{analytics_dataset}.{view_name}"

    context.log.info(f"Creating COI predictive power view {view_ref}...")

    query: str = f"""
    CREATE OR REPLACE VIEW `{view_ref}` AS
    SELECT
      coi_quartile,
      base_year,
      AVG(high_complexity_growth) as avg_high_complexity_growth,
      AVG(medium_high_growth) as avg_medium_high_growth,
      STDDEV(high_complexity_growth) as std_high_complexity_growth,
      STDDEV(medium_high_growth) as std_medium_high_growth,
      COUNT(*) as country_count
    FROM `{processed_dataset}.{growth_table_name}`
    GROUP BY coi_quartile, base_year
    ORDER BY base_year, coi_quartile;
    """

    context.log.info(f"Executing query: \n{query}")

    with bq_resource.get_client() as client:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated view: {view_ref}")
        return view_ref


@asset(
    description="Creates an export portfolio evolution view",
    deps=[calculate_export_portfolio, calculate_complexity_dynamics]
)
def create_portfolio_evolution_view(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Creates a view for analyzing export portfolio evolution.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    analytics_dataset: str = EnvVar("ANALYTICS_DATASET").get_value()
    portfolio_table_name: str = "export_portfolio"
    dynamics_table_name: str = "complexity_dynamics"
    view_name: str = "v_portfolio_evolution"
    
    portfolio_table_ref: str = f"{project_id}.{processed_dataset}.{portfolio_table_name}"
    dynamics_table_ref: str = f"{project_id}.{processed_dataset}.{dynamics_table_name}"
    view_ref: str = f"{project_id}.{analytics_dataset}.{view_name}"

    context.log.info(f"Creating portfolio evolution view {view_ref}...")

    query: str = f"""
    CREATE OR REPLACE VIEW `{view_ref}` AS
    SELECT
      ep.country_id,
      ep.year,
      ep.high_complexity_share,
      ep.medium_high_share,
      ep.medium_low_share,
      ep.low_complexity_share,
      cd.eci,
      RANK() OVER (PARTITION BY ep.year ORDER BY cd.eci DESC) as eci_rank
    FROM `{processed_dataset}.{portfolio_table_name}` ep
    JOIN `{processed_dataset}.{dynamics_table_name}` cd
      ON ep.country_id = cd.country_id AND ep.year = cd.year
    ORDER BY ep.year, eci_rank;
    """

    context.log.info(f"Executing query: \n{query}")

    with bq_resource.get_client() as client:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated view: {view_ref}")
        return view_ref
