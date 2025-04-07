import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Set environment variables
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_DATASET", "raw_trade_data")
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))


def load_gcs_to_bigquery(bucket_name, dataset_id, table_id, source_blob_name):
    """
    Load a gzipped CSV file from Google Cloud Storage to BigQuery.
    
    Args:
        bucket_name: Name of the GCS bucket
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        source_blob_name: Path to the file in the bucket
        
    Returns:
        String indicating the result: "loaded", "skipped", or "error"
    """
    client = bigquery.Client()
    table_ref = f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}"
    
    # Verify dataset exists
    dataset_ref = f"{GCP_PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        print(f"Error: Dataset {dataset_ref} does not exist. Please create it first.")
        return "error"
    

    # Configure and run the load job
    uri = f"gs://{bucket_name}/{source_blob_name}"
    print(f"Loading data from {uri} to {table_ref}...")
    
    try:
        # Start the load job with auto-detection
        load_job = client.load_table_from_uri(
            uri, 
            table_ref, 
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True
            )
        )
        
        # Wait for the job to complete and get row count
        load_job.result()
        table = client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows to {table_ref}")
        return "loaded"
    except Exception as e:
        print(f"Error loading data to {table_ref}: {e}")
        return "error"


def load_all_to_bigquery():
 
    # Process files from the main data directory
    print(f"Using file list from: {DATA_DIR}")
    
    # Track statistics
    stats = {"total": 0, "loaded": 0, "skipped": 0, "error": 0}
    
    # Process each CSV file
    for csv_file in DATA_DIR.glob("*.csv"):
        stats["total"] += 1
        table_name = csv_file.stem
        source_blob_name = f"raw/{table_name}.gz"
        
        # Load the data and update statistics
        result = load_gcs_to_bigquery(
            bucket_name=GCS_BUCKET_NAME,
            dataset_id=BQ_DATASET,
            table_id=table_name,
            source_blob_name=source_blob_name,
        )
        stats[result] += 1
    
    # Print summary
    print(f"\nProcessing complete! Total: {stats['total']}, Loaded: {stats['loaded']}, "
          f"Skipped: {stats['skipped']}, Failed: {stats['error']}")
    
    return stats 


if __name__ == "__main__":
    load_all_to_bigquery()
