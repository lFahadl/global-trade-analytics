import os
from pathlib import Path
from dotenv import load_dotenv
import gzip
import shutil
from google.cloud import storage
import concurrent.futures
import time

# Load environment variables
load_dotenv()

# Initialize GCS client
storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

# Define a function to compress a file with gzip
def compress_file(file_path):
    """Compress a file using gzip.
    
    Args:
        file_path: Path to the file to compress
        
    Returns:
        Path to the compressed file
    """
    compressed_file_path = f"{file_path}.gz"
    
    print(f"Compressing {file_path} to {compressed_file_path}...")
    start_time = time.time()
    
    with open(file_path, 'rb') as f_in:
        with gzip.open(compressed_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    # Get original and compressed file sizes for comparison
    original_size = os.path.getsize(file_path)
    compressed_size = os.path.getsize(compressed_file_path)
    compression_ratio = original_size / compressed_size
    elapsed_time = time.time() - start_time
    
    print(f"Compression complete for {file_path}. "
          f"Original: {original_size/1024/1024:.2f} MB, "
          f"Compressed: {compressed_size/1024/1024:.2f} MB, "
          f"Ratio: {compression_ratio:.2f}x, "
          f"Time: {elapsed_time:.2f} seconds")
    
    return compressed_file_path

# Define a function to upload a file to GCS
def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Uploads a file to the bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        source_file_path: Path to the local file
        destination_blob_name: Path to the file in the bucket
        
    Returns:
        True if upload was successful, False otherwise
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    print(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}...")
    start_time = time.time()
    
    try:
        # Set a longer timeout for the upload (5 minutes)
        blob.upload_from_filename(source_file_path, timeout=300)
        elapsed_time = time.time() - start_time
        print(f"File {source_file_path} uploaded to gs://{bucket_name}/{destination_blob_name} "
              f"in {elapsed_time:.2f} seconds")
        return True
    except Exception as e:
        print(f"Error uploading {source_file_path}: {e}")
        return False

# Define a function to check if a file exists in GCS
def file_exists_in_gcs(bucket_name, blob_name):
    """Check if a file exists in the GCS bucket.
    
    Args:
        bucket_name: Name of the GCS bucket
        blob_name: Path to the file in the bucket
        
    Returns:
        True if file exists, False otherwise
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()

# Define a function to process a single file (compress and upload)
def process_file(csv_file):
    """Process a single CSV file: compress and upload to GCS.
    
    Args:
        csv_file: Path to the CSV file to process
        
    Returns:
        Tuple of (csv_file, success, status)
    """
    try:
        print(f"Processing {csv_file}...")
        
        # Define destination blob name
        destination_blob_name = f"raw/{csv_file.stem}.gz"
        
        # Check if file already exists in GCS
        if file_exists_in_gcs(GCS_BUCKET_NAME, destination_blob_name):
            print(f"File {destination_blob_name} already exists in bucket {GCS_BUCKET_NAME}, skipping...")
            return csv_file, True, "already_exists"
        
        # Compress the file
        compressed_file_path = compress_file(str(csv_file))
        
        # Upload the compressed file with .gz extension
        success = upload_to_gcs(
            bucket_name=GCS_BUCKET_NAME,
            source_file_path=compressed_file_path,
            destination_blob_name=destination_blob_name
        )
        
        # Remove the compressed file after successful upload
        if success:
            os.remove(compressed_file_path)
            print(f"Temporary compressed file {compressed_file_path} removed")
        
        return csv_file, success, "uploaded"
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")
        return csv_file, False, "error"

# Main function to load all CSV files to GCS
def load_to_gcs(use_test_data=False):
    """Load all CSV files from the data directory to GCS using parallel processing.
    
    Args:
        use_test_data: If True, use test_data directory instead of data directory
    """
    start_time = time.time()
    
    # Select the appropriate data directory
    data_directory = TEST_DATA_DIR if use_test_data else DATA_DIR
    print(f"Using data from: {data_directory}")
    
    # Create the bucket if it doesn't exist
    try:
        bucket = storage_client.get_bucket(GCS_BUCKET_NAME)
        print(f"Bucket {GCS_BUCKET_NAME} already exists")
    except Exception:
        bucket = storage_client.create_bucket(GCS_BUCKET_NAME)
        print(f"Bucket {GCS_BUCKET_NAME} created")
    
    # Get list of CSV files
    csv_files = list(data_directory.glob("*.csv"))
    total_files = len(csv_files)
    print(f"Found {total_files} CSV files to process")
    
    if total_files == 0:
        print(f"No CSV files found in {data_directory}")
        return
    
    # Process files in parallel
    successful_files = 0
    skipped_files = 0
    failed_files = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all files for processing
        future_to_file = {executor.submit(process_file, csv_file): csv_file for csv_file in csv_files}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_file):
            csv_file = future_to_file[future]
            try:
                _, success, status = future.result()
                if success:
                    if status == "already_exists":
                        skipped_files += 1
                    else:
                        successful_files += 1
                else:
                    failed_files += 1
            except Exception as e:
                print(f"Exception processing {csv_file}: {e}")
                failed_files += 1
    
    # Print summary
    elapsed_time = time.time() - start_time
    print(f"\nProcessing complete!")
    print(f"Total files: {total_files}")
    print(f"Successfully uploaded: {successful_files}")
    print(f"Skipped (already exists): {skipped_files}")
    print(f"Failed: {failed_files}")
    print(f"Total time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload CSV files to Google Cloud Storage with gzip compression')
    parser.add_argument('--test', action='store_true', help='Use test data instead of full data')
    args = parser.parse_args()
    
    load_to_gcs(use_test_data=args.test)
