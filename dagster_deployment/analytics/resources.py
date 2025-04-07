import os
import tempfile
import base64
from dagster import EnvVar
from dagster_gcp import BigQueryResource

# Setup GCP credentials function
def setup_gcp_credentials():
    """Set up GCP credentials from base64-encoded credentials in environment variable."""
    # Get the base64-encoded credentials from the environment variable
    encoded_creds = EnvVar("GCP_CREDS").get_value()
    
    # Decode the credentials
    creds_json = base64.b64decode(encoded_creds).decode('utf-8')
    
    # Create a temporary file to store the credentials
    fd, temp_credentials_path = tempfile.mkstemp(suffix='.json')
    with os.fdopen(fd, 'w') as f:
        f.write(creds_json)
    
    # Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to point to the temporary file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_credentials_path
    
    return temp_credentials_path

# Setup GCP credentials
temp_credentials_path = setup_gcp_credentials()

# Define BigQuery resource with project ID
# Authentication will be handled by the GOOGLE_APPLICATION_CREDENTIALS environment variable
bigquery_resource = BigQueryResource(
    project=EnvVar("GCP_PROJECT").get_value(),
)
