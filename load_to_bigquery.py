import os
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import bigquery
import argparse

# Load environment variables
load_dotenv()

# Configuration
DATA_DIR = Path("data")
TEST_DATA_DIR = Path("test_data")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BQ_DATASET = "raw_trade_data"  # The BigQuery dataset to load data into

# Load data from GCS to BigQuery
def load_gcs_to_bigquery(bucket_name, source_blob_name, dataset_id, table_id):
    """Load a gzipped CSV file from Google Cloud Storage to BigQuery.
    
    Args:
        bucket_name: Name of the GCS bucket
        source_blob_name: Path to the file in the bucket
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
    """
    client = bigquery.Client()
    
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
    
    # Start the load job
    load_job = client.load_table_from_uri(
        uri, f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}", job_config=job_config
    )
    
    # Wait for the job to complete
    load_job.result()
    
    # Get the table to check row count
    table = client.get_table(f"{GCP_PROJECT_ID}.{dataset_id}.{table_id}")
    print(f"Loaded {table.num_rows} rows to {dataset_id}.{table_id}")

# Main function to load all CSV files from GCS to BigQuery
def load_all_to_bigquery(use_test_data=False):
    """Load all gzipped CSV files from GCS raw folder to BigQuery.
    
    Args:
        use_test_data: If True, use test_data directory instead of data directory
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
    
    # For each CSV file in the data directory, load from GCS to BigQuery
    for csv_file in data_directory.glob("*.csv"):
        table_name = csv_file.stem
        # Note: In GCS, the files are stored with .gz extension
        source_blob_name = f"raw/{table_name}.gz"
        
        # Load the data
        load_gcs_to_bigquery(
            bucket_name=GCS_BUCKET,
            source_blob_name=source_blob_name,
            dataset_id=BQ_DATASET,
            table_id=table_name
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Load data from Google Cloud Storage to BigQuery')
    parser.add_argument('--test', action='store_true', help='Use test data instead of full data')
    args = parser.parse_args()
    
    load_all_to_bigquery(use_test_data=args.test)
