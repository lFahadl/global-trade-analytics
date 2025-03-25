#!/usr/bin/env python

import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
import argparse

# Load environment variables
load_dotenv()

# Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
RAW_DATASET = "raw_trade_data"  # The BigQuery dataset where raw data is stored
PROCESSED_DATASET = "processed_trade_data"  # The BigQuery dataset for processed data
ANALYTICS_DATASET = "trade_analytics"  # The BigQuery dataset for analytics views

def execute_query(query):
    """Execute a BigQuery SQL query.
    
    Args:
        query: The SQL query to execute
        
    Returns:
        The query job result
    """
    client = bigquery.Client()
    print(f"Executing query:\n{query[:200]}...")
    
    # Start the query job
    query_job = client.query(query)
    
    # Wait for the job to complete
    result = query_job.result()
    
    print(f"Query completed successfully. Affected rows: {query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 'N/A'}")
    return result


def create_materialized_views():
    """Create materialized views for efficient querying."""
    print("Creating materialized views for efficient querying...")
    
    # Country Annual Trade Materialized View
    country_annual_query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_country_annual_trade` 
    AS
    SELECT
      country_id,
      year,
      SUM(export_value) AS total_exports,
      SUM(import_value) AS total_imports,
      SUM(export_value + import_value) AS total_trade_volume,
      AVG(eci) AS economic_complexity_index,
      AVG(coi) AS complexity_outlook_index
    FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.hs12_country_country_product_year_4_2022`
    GROUP BY country_id, year;
    """
    execute_query(country_annual_query)
    
    # Country Pairs Annual Trade Materialized View
    country_pairs_query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_country_pairs_annual_trade` 
    AS
    SELECT
      country_id,
      partner_country_id,
      year,
      SUM(export_value) AS exports_to_partner,
      SUM(import_value) AS imports_from_partner,
      SUM(export_value + import_value) AS total_trade_with_partner
    FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.hs12_country_country_product_year_4_2022`
    GROUP BY country_id, partner_country_id, year;
    """
    execute_query(country_pairs_query)
    
    # Product Annual Trade Materialized View
    product_annual_query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_product_annual_trade` 
    AS
    SELECT
      product_id,
      year,
      SUM(export_value) AS global_export_value,
      SUM(import_value) AS global_import_value,
      SUM(export_value + import_value) AS global_trade_volume,
      AVG(pci) AS product_complexity_index
    FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.hs12_country_country_product_year_4_2022`
    GROUP BY product_id, year;
    """
    execute_query(product_annual_query)
    
    print("Materialized views created successfully!")


def create_metric_views():
    """Create views for the required metrics."""
    print("Creating views for required metrics...")
    
    # Global Trade Metrics View
    global_metrics_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_global_trade_metrics` 
    AS
    WITH annual_metrics AS (
      SELECT
        year,
        SUM(total_trade_volume) AS global_trade_volume,
        AVG(economic_complexity_index) AS avg_economic_complexity
      FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_country_annual_trade`
      GROUP BY year
    ),
    with_previous_year AS (
      SELECT
        a.year,
        a.global_trade_volume,
        a.avg_economic_complexity,
        b.global_trade_volume AS prev_global_trade_volume,
        b.avg_economic_complexity AS prev_avg_economic_complexity
      FROM annual_metrics a
      LEFT JOIN annual_metrics b
        ON a.year = b.year + 1
    )
    SELECT
      year,
      global_trade_volume,
      avg_economic_complexity,
      global_trade_volume - IFNULL(prev_global_trade_volume, 0) AS trade_volume_change,
      CASE 
        WHEN IFNULL(prev_global_trade_volume, 0) = 0 THEN NULL
        ELSE (global_trade_volume - prev_global_trade_volume) / prev_global_trade_volume * 100 
      END AS trade_volume_pct_change,
      avg_economic_complexity - IFNULL(prev_avg_economic_complexity, 0) AS eci_change,
      CASE 
        WHEN IFNULL(prev_avg_economic_complexity, 0) = 0 THEN NULL
        ELSE (avg_economic_complexity - prev_avg_economic_complexity) / prev_avg_economic_complexity * 100 
      END AS eci_pct_change
    FROM with_previous_year
    ORDER BY year;
    """
    execute_query(global_metrics_query)
    
    # Top Traded Products View
    top_products_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_top_traded_products` 
    AS
    WITH ranked_products AS (
      SELECT
        year,
        product_id,
        global_trade_volume,
        product_complexity_index,
        ROW_NUMBER() OVER(PARTITION BY year ORDER BY global_trade_volume DESC) AS rank
      FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_product_annual_trade`
    ),
    top_products AS (
      SELECT * FROM ranked_products WHERE rank <= 20  -- Get top 20 for flexibility
    ),
    with_previous_year AS (
      SELECT
        a.year,
        a.product_id,
        a.rank,
        a.global_trade_volume,
        a.product_complexity_index,
        b.global_trade_volume AS prev_global_trade_volume
      FROM top_products a
      LEFT JOIN `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_product_annual_trade` b
        ON a.product_id = b.product_id AND a.year = b.year + 1
    )
    SELECT
      year,
      product_id,
      rank,
      global_trade_volume,
      product_complexity_index,
      global_trade_volume - IFNULL(prev_global_trade_volume, 0) AS volume_change,
      CASE 
        WHEN IFNULL(prev_global_trade_volume, 0) = 0 THEN NULL
        ELSE (global_trade_volume - prev_global_trade_volume) / prev_global_trade_volume * 100 
      END AS volume_pct_change
    FROM with_previous_year
    ORDER BY year, rank;
    """
    execute_query(top_products_query)
    
    # Top Trading Partners View
    top_partners_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_top_trading_partners` 
    AS
    WITH ranked_partners AS (
      SELECT
        country_id,
        partner_country_id,
        year,
        exports_to_partner,
        imports_from_partner,
        total_trade_with_partner,
        ROW_NUMBER() OVER(
          PARTITION BY country_id, year 
          ORDER BY total_trade_with_partner DESC
        ) AS rank
      FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.mv_country_pairs_annual_trade`
      WHERE partner_country_id != country_id  -- Exclude self-trade if present
    )
    SELECT
      country_id,
      partner_country_id,
      year,
      exports_to_partner,
      imports_from_partner,
      total_trade_with_partner,
      rank
    FROM ranked_partners
    WHERE rank <= 10  -- Get top 10 partners for flexibility
    ORDER BY country_id, year, rank;
    """
    execute_query(top_partners_query)
    
    print("Metric views created successfully!")


def main():
    """Main function to execute BigQuery transformations."""
    parser = argparse.ArgumentParser(description='Transform data in BigQuery for analytics')
    parser.add_argument('--skip-materialized-views', action='store_true', 
                        help='Skip creation of materialized views (useful if they already exist)')
    args = parser.parse_args()
    
    if not args.skip_materialized_views:
        create_materialized_views()
    
    create_metric_views()
    
    print("\nData transformation complete!")
    print("The following views and materialized views are now available:")
    print("  - mv_country_annual_trade: Aggregated country-level trade data by year")
    print("  - mv_country_pairs_annual_trade: Bilateral trade relationships")
    print("  - mv_product_annual_trade: Product-level trade data by year")
    print("  - v_global_trade_metrics: Global trade volume and economic complexity with YoY changes")
    print("  - v_top_traded_products: Top traded products with YoY changes")
    print("  - v_top_trading_partners: Top trading partners for each country")


if __name__ == "__main__":
    main()
