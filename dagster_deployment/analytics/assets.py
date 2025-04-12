from typing import Dict, List, Tuple, Any, Optional

from dagster import asset, AssetExecutionContext, EnvVar, Output
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound

# Import the combined_trade_data asset to establish dependencies
from ingestion.assets import combined_trade_data, country_year_metrics


@asset(
    description="Calculates product-year metrics from the combined trade table",
    deps=[combined_trade_data]
)
def calculate_product_year_metrics(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates various metrics aggregated by product and year from the combined trade data.
    Stores the results in a new table `product_year_metrics` in the processed dataset.
    """
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    metrics_table_name: str = "product_year_metrics"
    combined_table_name: str = "combined_trade_data"
    
    combined_table_ref: str = f"{project_id}.{processed_dataset}.{combined_table_name}"
    metrics_table_ref: str = f"{project_id}.{processed_dataset}.{metrics_table_name}"

    context.log.info(f"Calculating product-year metrics from {combined_table_ref} into {metrics_table_ref}...")

    query: str = f"""
    SELECT
      product_id,
      year,
      AVG(pci) as avg_pci,
      SUM(export_value) as global_export_value,
      COUNT(DISTINCT country_id) as exporting_countries
    FROM `{processed_dataset}.{combined_table_name}`
    GROUP BY product_id, year;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=metrics_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated product-year metrics table: {metrics_table_ref}")
        return metrics_table_ref


@asset(
    description="Calculates bilateral trade flows from the combined trade table",
    deps=[combined_trade_data]
)
def calculate_bilateral_flows(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Extracts bilateral trade flows from the combined trade data.
    Stores the results in a new table `bilateral_flows` in the processed dataset.
    """
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    combined_table_name: str = "combined_trade_data"
    flows_table_name: str = "bilateral_flows"
    
    combined_table_ref: str = f"{project_id}.{processed_dataset}.{combined_table_name}"
    flows_table_ref: str = f"{project_id}.{processed_dataset}.{flows_table_name}"

    context.log.info(f"Calculating bilateral flows from {combined_table_ref} into {flows_table_ref}...")

    query: str = f"""
    SELECT
      country_id as exporter_id,
      partner_country_id as importer_id,
      product_id,
      year,
      export_value,
      pci
    FROM `{processed_dataset}.{combined_table_name}`
    WHERE partner_country_id IS NOT NULL
    AND export_value > 0;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=flows_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated bilateral flows table: {flows_table_ref}")
        return flows_table_ref


@asset(
    description="Calculates export specialization metrics from the combined trade table",
    deps=[combined_trade_data]
)
def calculate_export_specialization(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates export specialization metrics from the combined trade data.
    Stores the results in a new table `export_specialization` in the processed dataset.
    """
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    combined_table_name: str = "combined_trade_data"
    specialization_table_name: str = "export_specialization"
    
    combined_table_ref: str = f"{project_id}.{processed_dataset}.{combined_table_name}"
    specialization_table_ref: str = f"{project_id}.{processed_dataset}.{specialization_table_name}"

    context.log.info(f"Calculating export specialization from {combined_table_ref} into {specialization_table_ref}...")

    query: str = f"""
    SELECT
      t.country_id,
      t.product_id,
      t.year,
      t.export_value,
      t.pci,
      t.export_value / SUM(t.export_value) OVER (PARTITION BY t.country_id, t.year) as export_share,
      t.export_value / SUM(t.export_value) OVER (PARTITION BY t.product_id, t.year) as global_market_share
    FROM `{processed_dataset}.{combined_table_name}` t
    WHERE t.export_value > 0;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=specialization_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated export specialization table: {specialization_table_ref}")
        return specialization_table_ref


@asset(
    description="Calculates complexity dynamics from country-year metrics",
    deps=[country_year_metrics]
)
def calculate_complexity_dynamics(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates economic complexity dynamics from country-year metrics.
    Stores the results in a new table `complexity_dynamics` in the processed dataset.
    """
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    metrics_table_name: str = "country_year_metrics"
    dynamics_table_name: str = "complexity_dynamics"
    
    metrics_table_ref: str = f"{project_id}.{processed_dataset}.{metrics_table_name}"
    dynamics_table_ref: str = f"{project_id}.{processed_dataset}.{dynamics_table_name}"

    context.log.info(f"Calculating complexity dynamics from {metrics_table_ref} into {dynamics_table_ref}...")

    query: str = f"""
    WITH yearly_changes AS (
      SELECT
        country_id,
        year,
        eci,
        coi,
        LAG(eci) OVER (PARTITION BY country_id ORDER BY year) as prev_eci,
        LAG(coi) OVER (PARTITION BY country_id ORDER BY year) as prev_coi,
        trade_balance,
        LAG(trade_balance) OVER (PARTITION BY country_id ORDER BY year) as prev_balance
      FROM `{processed_dataset}.{metrics_table_name}`
    )
    -- Multi-year data with actual changes
    SELECT
      country_id,
      year,
      eci,
      coi,
      eci - prev_eci as eci_change,
      coi - prev_coi as coi_change,
      trade_balance,
      trade_balance - prev_balance as balance_change,
      CASE 
        WHEN (eci - prev_eci) > 0 AND (trade_balance - prev_balance) > 0 THEN 'Both improving'
        WHEN (eci - prev_eci) > 0 AND (trade_balance - prev_balance) < 0 THEN 'ECI improving only'
        WHEN (eci - prev_eci) < 0 AND (trade_balance - prev_balance) > 0 THEN 'Balance improving only'
        ELSE 'Both worsening'
      END as trend_category
    FROM yearly_changes
    WHERE prev_eci IS NOT NULL AND prev_balance IS NOT NULL
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=dynamics_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated complexity dynamics table: {dynamics_table_ref}")
        return dynamics_table_ref
