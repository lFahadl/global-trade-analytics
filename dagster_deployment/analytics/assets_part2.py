from typing import Dict, List, Tuple, Any, Optional

from dagster import asset, AssetExecutionContext, EnvVar, Output
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq

# Import dependencies
from analytics.assets import calculate_product_year_metrics, calculate_complexity_dynamics
from analytics.assets import calculate_export_specialization, calculate_bilateral_flows


@asset(
    description="Calculates product complexity segmentation",
    deps=[calculate_product_year_metrics]
)
def calculate_product_complexity(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates product complexity segmentation from product-year metrics.
    Stores the results in a new table `product_complexity` in the processed dataset.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    metrics_table_name: str = "product_year_metrics"
    complexity_table_name: str = "product_complexity"
    
    metrics_table_ref: str = f"{project_id}.{processed_dataset}.{metrics_table_name}"
    complexity_table_ref: str = f"{project_id}.{processed_dataset}.{complexity_table_name}"

    context.log.info(f"Calculating product complexity from {metrics_table_ref} into {complexity_table_ref}...")

    query: str = f"""
    WITH complexity_ranking AS (
      SELECT
        product_id,
        year,
        avg_pci,
        NTILE(4) OVER (PARTITION BY year ORDER BY avg_pci) as complexity_quartile,
        global_export_value,
        RANK() OVER (PARTITION BY year ORDER BY global_export_value DESC) as export_rank
      FROM `{processed_dataset}.{metrics_table_name}`
    )
    SELECT
      product_id,
      year,
      avg_pci,
      complexity_quartile,
      CASE
        WHEN complexity_quartile = 4 THEN 'High Complexity'
        WHEN complexity_quartile = 3 THEN 'Medium-High Complexity'
        WHEN complexity_quartile = 2 THEN 'Medium-Low Complexity'
        ELSE 'Low Complexity'
      END as complexity_tier,
      global_export_value,
      export_rank
    FROM complexity_ranking;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=complexity_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated product complexity table: {complexity_table_ref}")
        return complexity_table_ref


@asset(
    description="Calculates country export portfolio analysis",
    deps=[calculate_export_specialization, calculate_product_complexity]
)
def calculate_export_portfolio(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates country export portfolio analysis from export specialization and product complexity.
    Stores the results in a new table `export_portfolio` in the processed dataset.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    specialization_table_name: str = "export_specialization"
    complexity_table_name: str = "product_complexity"
    portfolio_table_name: str = "export_portfolio"
    
    specialization_table_ref: str = f"{project_id}.{processed_dataset}.{specialization_table_name}"
    complexity_table_ref: str = f"{project_id}.{processed_dataset}.{complexity_table_name}"
    portfolio_table_ref: str = f"{project_id}.{processed_dataset}.{portfolio_table_name}"

    context.log.info(f"Calculating export portfolio from {specialization_table_ref} and {complexity_table_ref} into {portfolio_table_ref}...")

    query: str = f"""
    WITH country_product_metrics AS (
      SELECT
        e.country_id,
        e.year,
        e.export_share,
        e.product_id,
        p.avg_pci,
        p.complexity_tier,
        e.export_value,
        ROW_NUMBER() OVER (PARTITION BY e.country_id, e.year ORDER BY e.export_value DESC) as product_rank
      FROM `{processed_dataset}.{specialization_table_name}` e
      JOIN `{processed_dataset}.{complexity_table_name}` p ON e.product_id = p.product_id AND e.year = p.year
    )
    SELECT
      country_id,
      year,
      SUM(CASE WHEN complexity_tier = 'High Complexity' THEN export_value ELSE 0 END) / SUM(export_value) as high_complexity_share,
      SUM(CASE WHEN complexity_tier = 'Medium-High Complexity' THEN export_value ELSE 0 END) / SUM(export_value) as medium_high_share,
      SUM(CASE WHEN complexity_tier = 'Medium-Low Complexity' THEN export_value ELSE 0 END) / SUM(export_value) as medium_low_share,
      SUM(CASE WHEN complexity_tier = 'Low Complexity' THEN export_value ELSE 0 END) / SUM(export_value) as low_complexity_share,
      SUM(export_share * avg_pci) as weighted_complexity,
      COUNT(DISTINCT product_id) as product_diversity,
      SUM(CASE WHEN product_rank <= 10 THEN export_value ELSE 0 END) / SUM(export_value) as top10_concentration
    FROM country_product_metrics
    GROUP BY country_id, year;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=portfolio_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated export portfolio table: {portfolio_table_ref}")
        return portfolio_table_ref


@asset(
    description="Calculates trade partner diversification",
    deps=[calculate_bilateral_flows]
)
def calculate_partner_diversification(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates trade partner diversification from bilateral flows.
    Stores the results in a new table `partner_diversification` in the processed dataset.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    flows_table_name: str = "bilateral_flows"
    diversification_table_name: str = "partner_diversification"
    
    flows_table_ref: str = f"{project_id}.{processed_dataset}.{flows_table_name}"
    diversification_table_ref: str = f"{project_id}.{processed_dataset}.{diversification_table_name}"

    context.log.info(f"Calculating partner diversification from {flows_table_ref} into {diversification_table_ref}...")

    query: str = f"""
    WITH partner_shares AS (
      SELECT
        exporter_id as country_id,
        year,
        importer_id as partner_id,
        SUM(export_value) as bilateral_export,
        SUM(export_value) / SUM(SUM(export_value)) OVER (PARTITION BY exporter_id, year) as partner_share,
        POWER(SUM(export_value) / SUM(SUM(export_value)) OVER (PARTITION BY exporter_id, year), 2) as squared_share
      FROM `{processed_dataset}.{flows_table_name}`
      GROUP BY exporter_id, year, importer_id
    )
    SELECT
      country_id,
      year,
      COUNT(DISTINCT partner_id) as partner_count,
      SUM(squared_share) as partner_hhi,
      CASE 
        WHEN SUM(squared_share) < 0.15 THEN 'Highly Diversified'
        WHEN SUM(squared_share) < 0.25 THEN 'Moderately Diversified'
        WHEN SUM(squared_share) < 0.45 THEN 'Moderately Concentrated'
        ELSE 'Highly Concentrated'
      END as concentration_category
    FROM partner_shares
    GROUP BY country_id, year;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=diversification_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated partner diversification table: {diversification_table_ref}")
        return diversification_table_ref


@asset(
    description="Calculates complexity outlook and export growth analysis",
    deps=[calculate_complexity_dynamics, calculate_export_specialization, calculate_product_complexity]
)
def calculate_coi_growth_analysis(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:
    """
    Calculates complexity outlook and export growth analysis.
    Stores the results in a new table `coi_growth_analysis` in the processed dataset.
    """
    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    metrics_table_name: str = "country_year_metrics"
    specialization_table_name: str = "export_specialization"
    complexity_table_name: str = "product_complexity"
    growth_table_name: str = "coi_growth_analysis"
    
    metrics_table_ref: str = f"{project_id}.{processed_dataset}.{metrics_table_name}"
    specialization_table_ref: str = f"{project_id}.{processed_dataset}.{specialization_table_name}"
    complexity_table_ref: str = f"{project_id}.{processed_dataset}.{complexity_table_name}"
    growth_table_ref: str = f"{project_id}.{processed_dataset}.{growth_table_name}"

    context.log.info(f"Calculating COI growth analysis into {growth_table_ref}...")

    query: str = f"""
    WITH country_complex_exports AS (
      SELECT
        r.country_id,
        r.year,
        r.coi,
        SUM(CASE WHEN p.complexity_tier = 'High Complexity' THEN e.export_value ELSE 0 END) as high_complexity_exports,
        SUM(CASE WHEN p.complexity_tier = 'Medium-High Complexity' THEN e.export_value ELSE 0 END) as medium_high_exports
      FROM `{processed_dataset}.{metrics_table_name}` r
      JOIN `{processed_dataset}.{specialization_table_name}` e ON r.country_id = e.country_id AND r.year = e.year
      JOIN `{processed_dataset}.{complexity_table_name}` p ON e.product_id = p.product_id AND e.year = p.year
      GROUP BY r.country_id, r.year, r.coi
    ),
    growth_metrics AS (
      SELECT
        c1.country_id,
        c1.year as base_year,
        c1.coi,
        c2.year as target_year,
        (c2.high_complexity_exports - c1.high_complexity_exports) / NULLIF(c1.high_complexity_exports, 0) as high_complexity_growth,
        (c2.medium_high_exports - c1.medium_high_exports) / NULLIF(c1.medium_high_exports, 0) as medium_high_growth
      FROM country_complex_exports c1
      JOIN country_complex_exports c2 
        ON c1.country_id = c2.country_id 
        AND c2.year = c1.year + 3 -- 3-year growth window
    )
    SELECT
      country_id,
      base_year,
      coi,
      NTILE(4) OVER (PARTITION BY base_year ORDER BY coi) as coi_quartile,
      high_complexity_growth,
      medium_high_growth,
      target_year
    FROM growth_metrics
    WHERE high_complexity_growth IS NOT NULL;
    """

    context.log.info(f"Executing query: \n{query}")
    job_config = bq.QueryJobConfig(
        destination=growth_table_ref,
        write_disposition="WRITE_TRUNCATE"
    )

    with bq_resource.get_client() as client:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        # Calculate and log the cost
        cost_dollars = (query_job.total_bytes_processed / (1024 ** 4)) * 5  # Cost per TB
        context.log.info(f"Query completed. Processed {query_job.total_bytes_processed / (1024 ** 3):.2f} GB. Estimated cost: ${cost_dollars:.4f}")
    
        context.log.info(f"Successfully created/updated COI growth analysis table: {growth_table_ref}")
        return growth_table_ref
