from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dagster import asset, AssetExecutionContext, EnvVar, Output
from dagster_gcp import BigQueryResource
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound


def check_if_bigquery_table_exists_and_has_data(client: bq.Client, table_ref: str) -> Tuple[bool, bool]:

    table_exists = False
    table_has_data = False
    
    try:
        client.get_table(table_ref)
        table_exists = True
        
        # Check if the table has data
        query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
        query_job = client.query(query)
        row_count = list(query_job.result())[0]["count"]
        
        if row_count > 0:
            table_has_data = True
            
    except NotFound:
        pass
    
    return table_exists, table_has_data


@asset
def load_gcs_to_bigquery_tables(context: AssetExecutionContext, 
                             bq_resource: BigQueryResource) -> Dict[str, int]:
    
    # Get environment variables using Dagster's EnvVar
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    bucket_name: str = EnvVar("GCS_BUCKET_NAME").get_value()  # Using the standard bucket name variable
    dataset_id: str = EnvVar("BQ_DATASET").get_value()
    
    context.log.info(f"Using public bucket: {bucket_name}")
    
    # Initialize statistics
    stats: Dict[str, int] = {"total": 0, "loaded": 0, "skipped": 0, "error": 0}
    
    with bq_resource.get_client() as client:
        # First verify the dataset exists
        dataset_ref: str = f"{project_id}.{dataset_id}"
        try:
            client.get_dataset(dataset_ref)
            context.log.info(f"Using dataset {dataset_ref}")
        except NotFound:
            error_msg: str = f"Dataset {dataset_ref} does not exist. Please create it using Terraform before running this pipeline."
            context.log.error(error_msg)
            raise Exception(error_msg)
            
        # Initialize GCS client to list files in the bucket
        from google.cloud import storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # List all blobs in the 'raw/' prefix
        blobs = list(bucket.list_blobs(prefix="raw/"))
        context.log.info(f"Found {len(blobs)} files in gs://{bucket_name}/raw/")
        
        # Process each file in the bucket
        for blob in blobs:
            # Skip if not a gzipped file or if it's just the directory marker
            if not blob.name.endswith(".gz") or blob.name == "raw/":
                continue
                
            stats["total"] += 1
            # Extract table name from the blob name (remove 'raw/' prefix and '.gz' suffix)
            table_name: str = blob.name.replace("raw/", "").replace(".gz", "")
            table_ref: str = f"{project_id}.{dataset_id}.{table_name}"
            
            # URI for the GCS file
            uri: str = f"gs://{bucket_name}/{blob.name}"
            context.log.info(f"Loading data from {uri} to {table_ref}...")
            
            try:
                # Configure the load job
                job_config = bq.LoadJobConfig(
                    source_format=bq.SourceFormat.CSV,
                    skip_leading_rows=1,  # Skip the header row
                    autodetect=True,  # Auto-detect schema
                    allow_quoted_newlines=True,
                    allow_jagged_rows=True,
                )
                
                # Start the load job
                load_job = client.load_table_from_uri(
                    uri, table_ref, job_config=job_config
                )
                
                # Wait for the job to complete
                load_job.result()
                context.log.info(f"Loaded {table_name} successfully!")
                stats["loaded"] += 1
                
            except NotFound as e:
                context.log.error(f"Error loading {table_name}: {str(e)}")
                stats["error"] += 1
    
    context.log.info(f"Ingestion complete. Stats: {stats}")
    return stats


@asset
def find_source_tables(context: AssetExecutionContext, bq_resource: BigQueryResource) -> List[str]:
    
    # Get environment variables
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    raw_dataset: str = EnvVar("RAW_DATASET").get_value()
    
    with bq_resource.get_client() as client:
        # List tables in the raw dataset to find our source tables
        context.log.info(f"Listing tables in {raw_dataset} dataset...")
        tables = list(client.list_tables(f"{project_id}.{raw_dataset}"))
        
        # Find all source tables that contain the trade data
        source_tables: List[str] = []
        for table in tables:
            if "country_country_product_year" in table.table_id:
                source_tables.append(table.table_id)
                context.log.info(f"Found source table: {table.table_id}")
        
        if not source_tables:
            error_msg: str = f"Could not find any source tables in {raw_dataset} dataset!"
            context.log.error(error_msg)
            raise Exception(error_msg)
            
        return source_tables


@asset(deps=[find_source_tables])
def combined_trade_data(context: AssetExecutionContext, bq_resource: BigQueryResource, 
                         find_source_tables: List[str]) -> Tuple[str, str, str]:
    
    # Get environment variables
    project_id: str = EnvVar("GCP_PROJECT_ID").get_value()
    raw_dataset: str = EnvVar("RAW_DATASET").get_value()
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    combined_table_name: str = "combined_trade_data"
    
    # Create the full table reference
    table_ref: str = f"{project_id}.{processed_dataset}.{combined_table_name}"
    
    # Get source tables from find_source_tables asset
    source_tables = find_source_tables
    
    context.log.info(f"Creating combined table from {len(source_tables)} source tables in {raw_dataset}...")
    
    with bq_resource.get_client() as client:
        # Check if the table exists and has data
        table_exists, table_has_data = check_if_bigquery_table_exists_and_has_data(client, table_ref)
        
        # If table exists and has data, use MERGE to add any new data
        if table_exists and table_has_data:
            # For each source table, use MERGE to add any new data
            for source_table in source_tables:
                source_table_ref: str = f"{project_id}.{raw_dataset}.{source_table}"
                context.log.info(f"Merging data from {source_table_ref} into {table_ref}...")
                
                # Use MERGE to add new data without duplicates
                merge_query: str = f"""
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
                
                # Execute the merge query
                query_job = client.query(merge_query)
                query_job.result()  # Wait for the query to complete
                context.log.info(f"Merged data from {source_table_ref} into {table_ref}")
            
            # Return empty union_query since we don't need it for existing tables with data
            return combined_table_name, table_ref, ""
        else:
            # Table doesn't exist or has no data, prepare the UNION ALL query for optimization
            context.log.info(f"Table {table_ref} does not exist or has no data. Preparing query for optimization...")
            
            # Build a UNION ALL query to combine all source tables
            union_queries: List[str] = []
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
                FROM `{project_id}.{raw_dataset}.{table}`
                WHERE year >= 2012 -- As per the dataset description
                AND export_value >= 0 -- Ensure no negative values
                AND import_value >= 0
                """)
            
            union_query: str = " UNION ALL ".join(union_queries)
            
            # Return the table info and union query for the optimize_combined_table asset
            return combined_table_name, table_ref, union_query


@asset(deps=[combined_trade_data])
def optimize_combined_table(context: AssetExecutionContext, bq_resource: BigQueryResource, 
                          combined_trade_data: Tuple[str, str, str]) -> str:
    
    # Unpack the tuple from combined_trade_data
    combined_table_name, table_ref, union_query = combined_trade_data
    
    # If union_query is empty, the table already exists and has been updated with MERGE
    if not union_query:
        context.log.info(f"Table {combined_table_name} already exists with data and has been updated.")
        return combined_table_name
    
    # Table doesn't exist or is empty, create it with the optimization query
    context.log.info(f"Creating optimized table {combined_table_name}...")
    
    with bq_resource.get_client() as client:
        # Create the optimized table query
        optimized_table_query = f"""
        CREATE OR REPLACE TABLE `{table_ref}`
        PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2012, 2030, 1))
        CLUSTER BY country_id, product_id
        AS (
        SELECT DISTINCT * FROM (
        {union_query}
        ) combined_data
        );
        """
        
        # Execute the query
        query_job = client.query(optimized_table_query)
        query_job.result()  # Wait for the query to complete
        context.log.info(f"Combined table {combined_table_name} created successfully with optimization!")
    
    return combined_table_name



@asset(
    description="Calculates aggregated metrics per country and year from the combined trade table.",
    deps=[combined_trade_data]
)
def country_year_metrics(context: AssetExecutionContext, bq_resource: BigQueryResource) -> str:

    project_id: str = bq_resource.project
    processed_dataset: str = EnvVar("PROCESSED_DATASET").get_value()
    combined_table_name: str = "combined_trade_data"
    metrics_table_name: str = "country_year_metrics"  # Define the name for the output table
    combined_table_ref: str = f"{project_id}.{processed_dataset}.{combined_table_name}"
    metrics_table_ref: str = f"{project_id}.{processed_dataset}.{metrics_table_name}"

    context.log.info(f"Calculating country-year metrics from {combined_table_ref} into {metrics_table_ref}...")

    query: str = f"""
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
    FROM `{processed_dataset}.{combined_table_name}`
    GROUP BY country_id, year;
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
    
        context.log.info(f"Successfully created/updated metrics table: {metrics_table_ref}")
        return metrics_table_ref
