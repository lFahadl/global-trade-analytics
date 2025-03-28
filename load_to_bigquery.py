import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
import argparse

# Load environment variables
load_dotenv()


# Check if a table exists and has data in BigQuery
def table_exists_with_data(dataset_id, table_id):
    """Check if a table exists and has data in BigQuery.
    
    Args:
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        
    Returns:
        Tuple of (exists, row_count): Boolean indicating if table exists and row count if it does
    """
    client = bigquery.Client()
    table_ref = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"
    
    try:
        table = client.get_table(table_ref)
        return True, table.num_rows
    except Exception as e:
        # Table doesn't exist
        return False, 0

# Load data from GCS to BigQuery
def load_gcs_to_bigquery(bucket_name, source_blob_name, dataset_id, table_id, force_reload=False):
    """Load a gzipped CSV file from Google Cloud Storage to BigQuery.
    
    Args:
        bucket_name: Name of the GCS bucket
        source_blob_name: Path to the file in the bucket
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        force_reload: If True, reload the data even if the table exists and has data
        
    Returns:
        String indicating the result: "loaded", "skipped", or "error"
    """
    client = bigquery.Client()
    
    # Check if table already exists and has data
    if not force_reload:
        exists, row_count = table_exists_with_data(dataset_id, table_id)
        if exists and row_count > 0:
            print(f"Table {dataset_id}.{table_id} already exists with {row_count} rows, skipping...")
            return "skipped"
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row
        autodetect=True,      # Auto-detect schema
    )
    
    # URI for the GCS file
    # For gzipped files, BigQuery automatically detects and handles the compression
    # based on the .gz file extension
    uri = f"gs://{bucket_name}/{source_blob_name}"
    
    print(f"Loading data from {uri} to {GCP_PROJECT_ID}.{dataset_id}.{table_id}...")
    
    try:
        # Start the load job
        load_job = client.load_table_from_uri(
            uri, f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}", job_config=job_config
        )
        
        # Wait for the job to complete
        load_job.result()
        
        # Get the table to check row count
        table = client.get_table(f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}")
        print(f"Loaded {table.num_rows} rows to {dataset_id}.{table_id}")
        return "loaded"
    except Exception as e:
        print(f"Error loading data to {dataset_id}.{table_id}: {e}")
        return "error"

# Main function to load all CSV files from GCS to BigQuery
def load_all_to_bigquery(use_test_data=False, force_reload=False):
    """Load all gzipped CSV files from GCS raw folder to BigQuery.
    
    Args:
        use_test_data: If True, use test_data directory instead of data directory
        force_reload: If True, reload the data even if the table exists and has data
    """
    # Ensure the dataset exists
    client = bigquery.Client()
    dataset_ref = client.dataset(BQ_DATASET)
    
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {GCP_PROJECT_ID}.{BQ_DATASET} already exists")
    except Exception:
        # Dataset does not exist, create it
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Specify the location
        dataset = client.create_dataset(dataset)
        print(f"Created dataset {GCP_PROJECT_ID}.{BQ_DATASET}")
    
    # Select the appropriate data directory to determine which files to process
    data_directory = TEST_DATA_DIR if use_test_data else DATA_DIR
    print(f"Using file list from: {data_directory}")
    
    # Track statistics
    total_files = 0
    loaded_files = 0
    skipped_files = 0
    error_files = 0
    
    # For each CSV file in the data directory, load from GCS to BigQuery
    for csv_file in data_directory.glob("*.csv"):
        total_files += 1
        table_name = csv_file.stem
        # Note: In GCS, the files are stored with .gz extension
        source_blob_name = f"raw/{table_name}.gz"
        
        # Load the data
        result = load_gcs_to_bigquery(
            bucket_name=GCS_BUCKET_NAME,
            source_blob_name=source_blob_name,
            dataset_id=BQ_DATASET,
            table_id=table_name,
            force_reload=force_reload
        )
        
        # Update statistics
        if result == "loaded":
            loaded_files += 1
        elif result == "skipped":
            skipped_files += 1
        else:  # error
            error_files += 1
    
    # Print summary
    print(f"\nProcessing complete!")
    print(f"Total files: {total_files}")
    print(f"Successfully loaded: {loaded_files}")
    print(f"Skipped (already exists): {skipped_files}")
    print(f"Failed: {error_files}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load data from Google Cloud Storage to BigQuery')
    parser.add_argument('--test', action='store_true', help='Use test data instead of full data')
    parser.add_argument('--force', action='store_true', help='Force reload of data even if table exists')
    args = parser.parse_args()
    
    load_all_to_bigquery(use_test_data=args.test, force_reload=args.force)
