#!/usr/bin/env python

import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
import argparse

# Load environment variables
load_dotenv()

# Set environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = os.getenv("RAW_DATASET")
COMBINED_DATASET = os.getenv("COMBINED_DATASET")
PROCESSED_DATASET = os.getenv("PROCESSED_DATASET")
ANALYTICS_DATASET = os.getenv("ANALYTICS_DATASET")

# Get the location of datasets
def get_dataset_location(dataset_id):
    """Get the location of a dataset."""
    client = bigquery.Client()
    try:
        dataset = client.get_dataset(f"{GCP_PROJECT_ID}.{dataset_id}")
        return dataset.location
    except Exception as e:
        print(f"Warning: Could not get location for dataset {dataset_id}: {e}")
        return None

# Get the location of the raw dataset at module load time
RAW_DATASET_LOCATION = get_dataset_location(RAW_DATASET)
if RAW_DATASET_LOCATION:
    print(f"Using dataset location: {RAW_DATASET_LOCATION} for all operations")
else:
    RAW_DATASET_LOCATION = "US"  # Default to US if we can't determine the location
    print(f"Warning: Could not determine dataset location. Using default: {RAW_DATASET_LOCATION}")

def execute_query(query):
    """Execute a BigQuery SQL query.
    
    Args:
        query: The SQL query to execute
        
    Returns:
        The query job result
    """
    client = bigquery.Client()
    print(f"Executing query:\n{query[:200]}...")
    
    # Use the raw dataset location for all queries
    print(f"Using location: {RAW_DATASET_LOCATION} for query execution")
    query_job = client.query(query, location=RAW_DATASET_LOCATION)
    
    # Wait for the job to complete
    result = query_job.result()
    
    print(f"Query completed successfully. Affected rows: {query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 'N/A'}")
    return result

def list_tables(dataset_id):
    """List tables in a dataset."""
    client = bigquery.Client()
    dataset_ref = f"{GCP_PROJECT_ID}.{dataset_id}"
    try:
        tables = list(client.list_tables(dataset_ref))
        
        print(f"Tables in {dataset_ref}:")
        for table in tables:
            print(f"  - {table.table_id}")
        
        return [table.table_id for table in tables]
    except Exception as e:
        print(f"Error listing tables in {dataset_ref}: {e}")
        return []

def delete_existing_views(dataset=None, exclude_tables=[]):
    """Delete existing views and materialized views to start fresh."""
    print("Deleting existing views and materialized views...")
    
    client = bigquery.Client()
    
    # Get list of tables/views in each dataset
    if dataset:
        datasets = [dataset]
    else:
        datasets = [RAW_DATASET, PROCESSED_DATASET, ANALYTICS_DATASET]
    
    for dataset in datasets:
        dataset_ref = f"{GCP_PROJECT_ID}.{dataset}"
        try:
            tables = list(client.list_tables(dataset_ref))
            for table in tables:
                if table.table_id not in exclude_tables:
                    table_id = f"{dataset_ref}.{table.table_id}"
                    print(f"Deleting {table_id}...")
                    client.delete_table(table_id, not_found_ok=True)
                else:
                    print(f"Skipping deletion of {dataset_ref}.{table.table_id} (in exclude list)")
        except Exception as e:
            print(f"Error listing tables in {dataset_ref}: {e}")
    
    print("Deletion complete!")

def check_combined_dataset_exists():
    """Check if the combined dataset exists."""
    client = bigquery.Client()
    dataset_id = f"{GCP_PROJECT_ID}.{COMBINED_DATASET}"
    
    try:
        # Get the dataset reference
        dataset = client.get_dataset(dataset_id)
        print(f"Found dataset {dataset_id} in location {dataset.location}")
        
        # Check if location matches the raw dataset location
        if dataset.location != RAW_DATASET_LOCATION:
            print(f"WARNING: Dataset location {dataset.location} does not match raw dataset location {RAW_DATASET_LOCATION}!")
            print("This will cause location errors when running queries.")
            print("Please recreate the combined dataset in the same location as the raw dataset.")
            return False
            
        return True
    except Exception as e:
        print(f"Error: Dataset {dataset_id} does not exist! {e}")
        print("Please run create_combined_dataset.py first to create the dataset.")
        return False

# delete this once transformation layer is completed.
def create_combined_table(source_tables):
    """Create a combined table from all source tables with partitioning and clustering.
    
    Args:
        source_tables: List of source table names in the raw dataset
        
    Returns:
        The name of the combined table
    """
    print(f"Creating combined table from {len(source_tables)} source tables...")
    
    # Check if the combined dataset exists
    if not check_combined_dataset_exists():
        return None
    
    # Create a combined table with partitioning and clustering
    combined_table_name = "combined_trade_data"
    table_ref = f"{GCP_PROJECT_ID}.{COMBINED_DATASET}.{combined_table_name}"
    client = bigquery.Client()
    
    # Check if the table exists
    try:
        table = client.get_table(table_ref)
        print(f"Table {table_ref} already exists.")
        
        # For each source table, use MERGE to add any new data
        for source_table in source_tables:
            source_table_ref = f"{GCP_PROJECT_ID}.{RAW_DATASET}.{source_table}"
            print(f"Merging data from {source_table_ref} into {table_ref}...")
            
            # Use MERGE to add new data without duplicates
            # The merge key is the combination of country_id, partner_country_id, product_id, and year
            merge_query = f"""
            MERGE `{table_ref}` T
            USING (
              SELECT DISTINCT
                country_id,
                partner_country_id,
                product_id,
                year,
                COALESCE(export_value, 0) as export_value,
                COALESCE(import_value, 0) as import_value,
                eci,
                coi,
                pci
              FROM `{source_table_ref}`
              WHERE year >= 2012 -- As per the dataset description
              AND export_value >= 0 -- Ensure no negative values
              AND import_value >= 0
            ) S
            ON T.country_id = S.country_id 
               AND T.partner_country_id = S.partner_country_id
               AND T.product_id = S.product_id
               AND T.year = S.year
            WHEN NOT MATCHED THEN
              INSERT (country_id, partner_country_id, product_id, year, export_value, import_value, eci, coi, pci)
              VALUES (S.country_id, S.partner_country_id, S.product_id, S.year, S.export_value, S.import_value, S.eci, S.coi, S.pci)
            """
            execute_query(merge_query)
        
        return combined_table_name
        
    except Exception:
        # Table doesn't exist, create it
        print(f"Table {table_ref} does not exist. Creating it...")
        
        # Build a UNION ALL query to combine all source tables
        # Use DISTINCT to ensure idempotency by removing any potential duplicates
        union_queries = []
        for table in source_tables:
            union_queries.append(f"""
            SELECT DISTINCT
              country_id,
              partner_country_id,
              product_id,
              year,
              COALESCE(export_value, 0) as export_value,
              COALESCE(import_value, 0) as import_value,
              eci,
              coi,
              pci
            FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.{table}`
            WHERE year >= 2012 -- As per the dataset description
            AND export_value >= 0 -- Ensure no negative values
            AND import_value >= 0
            """)
        
        union_query = " UNION ALL ".join(union_queries)
        
        # Create the combined table with range partitioning for the year column and clustering
        combined_table_query = f"""
        CREATE OR REPLACE TABLE `{table_ref}`
        PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2012, 2030, 1))
        CLUSTER BY country_id, product_id
        AS (
        SELECT DISTINCT * FROM (
        {union_query}
        ) combined_data
        );
        """
        
        execute_query(combined_table_query)
        print(f"Combined table {combined_table_name} created successfully with range partitioning by year and clustering by country_id, product_id!")
    
    return combined_table_name

def create_raw_data_views(combined_table):
    """Create raw data layer views using the combined table."""
    print(f"Creating raw data layer views using combined table: {combined_table}...")
    
    # 1. Clean base trade data table - this is now just a view on the combined table
    raw_trade_data_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_trade_data` AS
    SELECT *
    FROM `{GCP_PROJECT_ID}.{COMBINED_DATASET}.{combined_table}`;
    """
    execute_query(raw_trade_data_query)
    
    # 2. Country yearly aggregates
    country_year_metrics_query = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_country_year_metrics` AS
    SELECT
      country_id,
      year,
      MAX(eci) as eci,
      MAX(coi) as coi,
      SUM(export_value) as total_exports,
      SUM(import_value) as total_imports,
      SUM(export_value - import_value) as trade_balance,
      COUNT(DISTINCT product_id) as product_count,
      COUNT(DISTINCT partner_country_id) as partner_count
    FROM `{GCP_PROJECT_ID}.{COMBINED_DATASET}.{combined_table}`
    GROUP BY country_id, year;
    """
    execute_query(country_year_metrics_query)
    
    # 3. Product yearly metrics
    product_year_metrics_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_product_year_metrics` AS
    SELECT
      product_id,
      year,
      AVG(pci) as avg_pci,
      SUM(export_value) as global_export_value,
      COUNT(DISTINCT country_id) as exporting_countries
    FROM `{GCP_PROJECT_ID}.{COMBINED_DATASET}.{combined_table}`
    GROUP BY product_id, year;
    """
    execute_query(product_year_metrics_query)
    
    # 4. Bilateral trade flows
    bilateral_flows_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_bilateral_flows` AS
    SELECT
      country_id as exporter_id,
      partner_country_id as importer_id,
      product_id,
      year,
      export_value,
      pci
    FROM `{GCP_PROJECT_ID}.{COMBINED_DATASET}.{combined_table}`
    WHERE partner_country_id IS NOT NULL
    AND export_value > 0;
    """
    execute_query(bilateral_flows_query)
    
    # 5. Export specialization metrics
    export_specialization_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_export_specialization` AS
    SELECT
      t.country_id,
      t.product_id,
      t.year,
      t.export_value,
      t.pci,
      t.export_value / SUM(t.export_value) OVER (PARTITION BY t.country_id, t.year) as export_share,
      t.export_value / SUM(t.export_value) OVER (PARTITION BY t.product_id, t.year) as global_market_share
    FROM `{GCP_PROJECT_ID}.{COMBINED_DATASET}.{combined_table}` t
    WHERE t.export_value > 0;
    """
    execute_query(export_specialization_query)
    
    print("Raw data layer views created successfully!")

def create_transformation_views():
    """Create transformation layer views."""
    print("Creating transformation layer views...")
    
    # 1. Economic Complexity Dynamics
    complexity_dynamics_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_complexity_dynamics` AS
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
      FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_country_year_metrics`
    ),
    -- Add a fallback for single-year data
    single_year_data AS (
      SELECT
        country_id,
        year,
        eci,
        coi,
        0 as eci_change,  -- Default to 0 change for single-year data
        0 as coi_change,  -- Default to 0 change for single-year data
        trade_balance,
        0 as balance_change,  -- Default to 0 change for single-year data
        'No change data' as trend_category
      FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_country_year_metrics`
    ),
    -- Multi-year data with actual changes
    multi_year_data AS (
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
    )
    -- Combine both datasets, preferring multi-year data when available
    SELECT * FROM multi_year_data
    UNION ALL
    SELECT s.* 
    FROM single_year_data s
    LEFT JOIN multi_year_data m ON s.country_id = m.country_id AND s.year = m.year
    WHERE m.country_id IS NULL
    """
    execute_query(complexity_dynamics_query)
    
    # 2. Product Complexity Segmentation
    product_complexity_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_product_complexity` AS
    WITH complexity_ranking AS (
      SELECT
        product_id,
        year,
        avg_pci,
        NTILE(4) OVER (PARTITION BY year ORDER BY avg_pci) as complexity_quartile,
        global_export_value,
        RANK() OVER (PARTITION BY year ORDER BY global_export_value DESC) as export_rank
      FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_product_year_metrics`
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
    execute_query(product_complexity_query)
    
    # 3. Country Export Portfolio Analysis
    export_portfolio_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_export_portfolio` AS
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
      FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_export_specialization` e
      JOIN `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_product_complexity` p ON e.product_id = p.product_id AND e.year = p.year
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
    execute_query(export_portfolio_query)
    
    # 4. Trade Partner Diversification
    partner_diversification_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_partner_diversification` AS
    WITH partner_shares AS (
      SELECT
        exporter_id as country_id,
        year,
        importer_id as partner_id,
        SUM(export_value) as bilateral_export,
        SUM(export_value) / SUM(SUM(export_value)) OVER (PARTITION BY exporter_id, year) as partner_share,
        POWER(SUM(export_value) / SUM(SUM(export_value)) OVER (PARTITION BY exporter_id, year), 2) as squared_share
      FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_bilateral_flows`
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
    execute_query(partner_diversification_query)
    
    # 5. Complexity Outlook and Export Growth Analysis
    coi_growth_analysis_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_coi_growth_analysis` AS
    WITH country_complex_exports AS (
      SELECT
        r.country_id,
        r.year,
        r.coi,
        SUM(CASE WHEN p.complexity_tier = 'High Complexity' THEN e.export_value ELSE 0 END) as high_complexity_exports,
        SUM(CASE WHEN p.complexity_tier = 'Medium-High Complexity' THEN e.export_value ELSE 0 END) as medium_high_exports
      FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_country_year_metrics` r
      JOIN `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_export_specialization` e ON r.country_id = e.country_id AND r.year = e.year
      JOIN `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_product_complexity` p ON e.product_id = p.product_id AND e.year = p.year
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
    execute_query(coi_growth_analysis_query)
    
    print("Transformation layer views created successfully!")

def create_presentation_views():
    """Create presentation layer views for the dashboard."""
    print("Creating presentation layer views...")
    
    # 0. Global yearly metrics view (for dashboard performance)
    global_metrics_mv_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_global_yearly_metrics` AS
    SELECT
      year,
      SUM(total_exports) as global_exports,
      SUM(total_imports) as global_imports,
      SUM(total_exports) as global_trade_volume, -- Use global exports as the measure
      AVG(eci) as avg_eci
    FROM `{GCP_PROJECT_ID}.{RAW_DATASET}.raw_country_year_metrics`
    GROUP BY year;
    """
    execute_query(global_metrics_mv_query)
    
    # 1. Economic Complexity vs. Trade Balance Trends
    eci_balance_trends_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_eci_balance_trends` AS
    SELECT
      country_id,
      year,
      eci,
      eci_change,
      trade_balance,
      balance_change,
      trend_category
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_complexity_dynamics`
    ORDER BY year DESC, country_id;
    """
    execute_query(eci_balance_trends_query)
    
    # 2. Export Complexity Portfolio
    export_portfolio_view_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_export_complexity_portfolio` AS
    SELECT
      ep.country_id,
      ep.year,
      ep.high_complexity_share,
      ep.medium_high_share,
      ep.medium_low_share,
      ep.low_complexity_share,
      ep.weighted_complexity,
      cd.eci
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_export_portfolio` ep
    JOIN `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_complexity_dynamics` cd
      ON ep.country_id = cd.country_id AND ep.year = cd.year
    ORDER BY cd.eci DESC, ep.country_id;
    """
    execute_query(export_portfolio_view_query)
    
    # 3. Partner Diversification vs. Economic Complexity
    diversification_complexity_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_diversification_complexity` AS
    SELECT
      pd.country_id,
      pd.year,
      pd.partner_count,
      pd.partner_hhi,
      pd.concentration_category,
      cd.eci,
      cd.eci_change
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_partner_diversification` pd
    JOIN `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_complexity_dynamics` cd
      ON pd.country_id = cd.country_id AND pd.year = cd.year
    ORDER BY pd.year, pd.country_id;
    """
    execute_query(diversification_complexity_query)
    
    # 4. Complexity Outlook Index Predictive Power
    coi_predictive_power_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_coi_predictive_power` AS
    SELECT
      coi_quartile,
      base_year,
      AVG(high_complexity_growth) as avg_high_complexity_growth,
      AVG(medium_high_growth) as avg_medium_high_growth,
      STDDEV(high_complexity_growth) as std_high_complexity_growth,
      STDDEV(medium_high_growth) as std_medium_high_growth,
      COUNT(*) as country_count
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_coi_growth_analysis`
    GROUP BY coi_quartile, base_year
    ORDER BY base_year, coi_quartile;
    """
    execute_query(coi_predictive_power_query)
    
    # 5. Export Portfolio Evolution
    portfolio_evolution_query = f"""
    CREATE OR REPLACE VIEW `{GCP_PROJECT_ID}.{ANALYTICS_DATASET}.v_portfolio_evolution` AS
    SELECT
      ep.country_id,
      ep.year,
      ep.high_complexity_share,
      ep.medium_high_share,
      ep.medium_low_share,
      ep.low_complexity_share,
      cd.eci,
      RANK() OVER (PARTITION BY ep.year ORDER BY cd.eci DESC) as eci_rank
    FROM `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_export_portfolio` ep
    JOIN `{GCP_PROJECT_ID}.{PROCESSED_DATASET}.transformed_complexity_dynamics` cd
      ON ep.country_id = cd.country_id AND ep.year = cd.year
    ORDER BY ep.year, eci_rank;
    """
    execute_query(portfolio_evolution_query)
    
    print("Presentation layer views created successfully!")

def main():
    """Main function to execute BigQuery transformations."""
    parser = argparse.ArgumentParser(description='Transform data in BigQuery for analytics')
    parser.add_argument('--skip-delete', action='store_true', 
                        help='Skip deletion of existing views')
    parser.add_argument('--only-raw', action='store_true',
                        help='Only create the raw data layer views')
    parser.add_argument('--only-transform', action='store_true',
                        help='Only create the transformation layer views')
    parser.add_argument('--only-presentation', action='store_true',
                        help='Only create the presentation layer views')
    args = parser.parse_args()
    
    # First, list tables in the raw dataset to find our source tables
    print("Checking for source tables...")
    raw_tables = list_tables(RAW_DATASET)
    
    # Find all source tables that contain the trade data
    source_tables = []
    for table in raw_tables:
        if "country_country_product_year" in table:
            source_tables.append(table)
            print(f"Found source table: {table}")
    
    if not source_tables:
        print("Error: Could not find any source tables in raw_trade_data dataset!")
        print("Please ensure the trade data tables are loaded into BigQuery.")
        return
    
    # Skip deleting the source tables and modify deletion behavior for specific flags
    if not args.skip_delete:
        if args.only_transform:
            # For --only-transform, only delete transformation layer views
            delete_existing_views(dataset=PROCESSED_DATASET, exclude_tables=source_tables)
        elif args.only_presentation:
            # For --only-presentation, only delete presentation layer views
            delete_existing_views(dataset=ANALYTICS_DATASET, exclude_tables=source_tables)
        else:
            # For full run or --only-raw, delete all views except source tables
            delete_existing_views(exclude_tables=source_tables)
    
    # Create a combined table from all source tables with partitioning and clustering
    combined_table = create_combined_table(source_tables)
    
    if not combined_table:
        print("Error: Could not create or access the combined table.")
        return
    
    if args.only_raw:
        create_raw_data_views(combined_table)
    elif args.only_transform:
        create_transformation_views()
    elif args.only_presentation:
        create_presentation_views()
    else:
        create_raw_data_views(combined_table)
        create_transformation_views()
        create_presentation_views()
    
    print("\nData transformation complete!")
    print("The following views are now available:")
    print("\nRaw Data Layer:")
    print("  - raw_trade_data: Clean base trade data")
    print("  - raw_country_year_metrics: Country yearly aggregates")
    print("  - raw_product_year_metrics: Product yearly metrics")
    print("  - raw_bilateral_flows: Bilateral trade flows")
    print("  - raw_export_specialization: Export specialization metrics")
    print("\nTransformation Layer:")
    print("  - transformed_complexity_dynamics: Economic complexity dynamics")
    print("  - transformed_product_complexity: Product complexity segmentation")
    print("  - transformed_export_portfolio: Country export portfolio analysis")
    print("  - transformed_partner_diversification: Trade partner diversification")
    print("  - transformed_coi_growth_analysis: Complexity outlook and export growth analysis")
    print("\nPresentation Layer:")
    print("  - v_eci_balance_trends: Economic complexity vs. trade balance trends")
    print("  - v_export_complexity_portfolio: Export complexity portfolio")
    print("  - v_diversification_complexity: Partner diversification vs. economic complexity")
    print("  - v_coi_predictive_power: Complexity outlook index predictive power")
    print("  - v_portfolio_evolution: Export portfolio evolution")

if __name__ == "__main__":
    main()
