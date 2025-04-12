# Global Trade Analytics

## Problem Description

This project addresses the challenge of analyzing global trade patterns using the Harmonized System (HS) 2012 dataset from Harvard Dataverse. The dataset contains detailed international trade flows that provide valuable insights into global economic relationships.

The dataset itself contains trade flows classified via the Harmonized System (HS) 2012, which offers a contemporary and detailed classification of approximately 5,200 goods. While the HS data provides granular categorization at 1-, 2-, 4-, and 6-digit detail levels, it only covers data from 2012 onward, and reporting reliability can vary at the most detailed (6-digit) level.

Our solution implements a complete data pipeline that:
1. Compresses and transfers the raw data to Google Cloud Storage
2. Loads the data into BigQuery with appropriate schema definitions
3. Transforms the raw data into analytics-ready views through a multi-step orchestrated process
4. Optimizes query performance through strategic partitioning, clustering, and materialized views
5. Provides a foundation for visualization and analysis of global trade patterns

The end result is a robust analytics platform that enables exploration of international trade relationships, economic complexity indicators, and year-over-year trend analysis.

## Project Overview

This project implements a data pipeline for loading large CSV files (>1GB) from local storage to Google Cloud Storage and then to BigQuery. The pipeline follows a three-step approach:

1. **Step 1**: Load CSV files from local storage to Google Cloud Storage (using gzip compression)
2. **Step 2**: Load data from GCS to BigQuery using direct GCP APIs
3. **Step 3**: Transform raw data in BigQuery into analytics-ready views and materialized views

## Pipeline Characteristics

### Pipeline Type and Architecture
- **Batch Processing**: The pipeline operates in batch mode, processing complete datasets during each run
- **Data Warehouse**: Uses BigQuery as the primary data warehouse for storage and analytics
- **Execution Frequency**: Monthly updates to incorporate new trade data
- **Trigger Mechanism**: Event-based triggers initiate the pipeline execution
- **Data Volume**: Processes 4GB+ of trade data in each pipeline run

### Data Processing Strategy
- **Idempotency Guarantees**: Implements a write-truncate pattern to ensure idempotent operations
- **Partitioning Strategy**: The combined table is partitioned by year (RANGE_BUCKET) for efficient time-based queries
- **Clustering Strategy**: Data is clustered by country_id and product_id to optimize common query patterns
- **Table Creation Pattern**: For existing tables, uses MERGE operations to add new data without duplicates

## Public Data Access

For convenience, we've created a public Google Cloud Storage bucket with the pre-loaded trade data files:

- **Public Bucket URL**: https://storage.googleapis.com/global-trade-analytics-public-trade-data
- **GCS Path**: `gs://global-trade-analytics-public-trade-data/raw/`

This public bucket contains the compressed trade data files and can be used directly with BigQuery for loading data without having to run the initial data upload steps.

## Key Features

- **Efficient Compression**: Uses gzip to compress CSV files before uploading to GCS (typically 5-10x reduction in size)
- **Parallel Processing**: Compresses and uploads multiple files simultaneously for improved performance
- **Data Lake Storage**: Maintains compressed files in GCS for future processing
- **Analytics-Ready Data**: Transforms raw data into optimized structures for efficient querying
- **Year-over-Year Analysis**: Built-in calculations for tracking changes in key metrics over time
- **Infrastructure as Code**: Uses Terraform for GCP infrastructure management
- **Environment Management**: Automatically updates environment variables with latest bucket names
- **Idempotent Operations**: All pipeline steps check for existing data to prevent duplicates
- **Optimized Combined Table**: Uses partitioning by year and clustering by country_id and product_id

## Technologies

- **Python**: Core programming language for all data processing scripts
- **Google Cloud Platform (GCP)**: Primary cloud infrastructure provider
- **BigQuery**: Data warehouse for storage and analytics processing
- **Google Cloud Storage (GCS)**: Object storage for the data lake
- **Terraform**: Infrastructure as Code (IaC) for managing cloud resources
- **Dagster**: Data orchestration and pipeline management
- **Docker**: Containerization for consistent deployment environments
- **pandas**: Data manipulation and analysis library
- **pyarrow**: High-performance data processing and conversion
- **Streamlit**: Dashboard creation and data visualization

## File Structure

- `data/` - Directory containing large CSV trade data files
- `test_data/` - Directory containing smaller CSV files for testing purposes
- `load_to_gcs.py` - Script to compress and upload CSV files to Google Cloud Storage
- `load_to_bigquery.py` - Script to load data from GCS to BigQuery
- `new_transform_data.py` - Script to create combined table and analytics views
- `create_combined_dataset.py` - Script to create the combined_trade_data dataset
- `bigquery_transformations.sql` - SQL transformations for analytics
- `sample_queries.sql` - Example queries for accessing the transformed data
- `terraform/` - Terraform configuration for GCP infrastructure
- `update_env.sh` - Script to update environment variables
- `tests/` - Unit tests for the data pipeline
- `create_public_bucket.py` - Script to create a public GCS bucket for sharing data

## Setup and Usage

### Prerequisites

- Python 3.7+
- Terraform
- Google Cloud Platform account with appropriate permissions
- Service account credentials (`creds.json`)
- Enabled Google Cloud APIs:
  - Identity and Access Management (IAM) API
  - Cloud Resource Manager API
  - BigQuery API
  - Cloud Storage API

### Installation

1. Clone the repository
2. Set up a virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Configure environment files:
   - Create a root `.env` file by copying `.env.example` to `.env` and updating the values
   - Place your service account credentials in the root directory and encode them in base64:
     ```bash
     # Convert your credentials file to base64 format
     cat creds.json | base64 > creds_base64.txt
     
     # Set the environment variable for GCP credentials
     export GCP_CREDS=$(cat creds_base64.txt)
     ```
   - Set the environment variables for file paths:
     ```bash
     # Path to your .env file
     export ENV_FILE_PATH=/path/to/your/.env
     ```
5. Create a `terraform.tfvars` file in the terraform directory with your GCP configuration:
   ```
   project_id       = "your-project-id"
   credentials_file = "path-to-credsfile"
   region           = "us-central1"
   ```

### Running the Pipeline

#### 1. Set Up Infrastructure with Terraform

1. Navigate to the terraform directory:
   ```bash
   cd terraform
   ```

2. Initialize Terraform:
   ```bash
   terraform init
   ```

3. Apply the Terraform configuration to create the required GCP resources:
   ```bash
   terraform apply
   ```

#### 2. Start Dagster with Docker

1. Navigate to the dagster_deployment directory:
   ```bash
   cd dagster_deployment
   ```

2. Build and start the Docker containers:
   ```bash
   docker-compose up -d
   ```

3. Access the Dagster UI at http://localhost:3000

#### 3. Run the Data Pipeline

Note: You can skip running `load_to_gcs.py` as the data is already uploaded to a cloud bucket to save upload time. If you want to run it yourself, you can download the data from [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YAVJDF).

Use the Dagster UI or CLI to run the following jobs in order:

1. **data_ingestion_job**: Loads data from Google Cloud Storage to BigQuery tables
2. **combined_table_job**: Creates and optimizes the combined trade data table
3. **basic_analytics_job**: Creates basic analytics views for dashboards

Additional specialized analytics jobs are available for more detailed analysis:

4. **product_metrics_job**: Calculates product-related metrics and views
5. **trade_relationship_job**: Analyzes trade relationships between countries
6. **economic_complexity_job**: Analyzes economic complexity indicators
7. **advanced_analytics_job**: Performs advanced analytics on trade data
8. **full_analytics_job**: Creates all analytics assets for comprehensive analysis

To run each job using the CLI:
```bash
# Run the data ingestion job
docker exec -it docker_example_user_code dagster job execute -j data_ingestion_job

# Run the combined table job
docker exec -it docker_example_user_code dagster job execute -j combined_table_job

# Run the basic analytics job
docker exec -it docker_example_user_code dagster job execute -j basic_analytics_job

# Run the product metrics job
docker exec -it docker_example_user_code dagster job execute -j product_metrics_job

# Run the trade relationship job
docker exec -it docker_example_user_code dagster job execute -j trade_relationship_job

# Run the economic complexity job
docker exec -it docker_example_user_code dagster job execute -j economic_complexity_job

# Run the advanced analytics job
docker exec -it docker_example_user_code dagster job execute -j advanced_analytics_job

# Run the full analytics job
docker exec -it docker_example_user_code dagster job execute -j full_analytics_job
```

#### 4. Shutting Down

1. Stop the Dagster containers:
   ```bash
   cd dagster_deployment
   docker-compose down
   ```

2. To destroy the GCP infrastructure (optional, for cleanup):
   ```bash
   cd terraform
   terraform destroy
   ```

## Data Organization

The project uses a three-tier dataset organization in BigQuery:

1. **Raw Data** (`raw_trade_data`): Original, unmodified data loaded from source files
2. **Processed Data** (`processed_trade_data`): Intermediate, cleaned data with materialized views and combined tables
3. **Analytics** (`trade_analytics`): Final analytics-ready views with metrics and KPIs

## Dashboard

The project includes a Streamlit-based dashboard (`trade_dashboard.py`) for visualizing global trade analytics data. The dashboard provides interactive visualizations and insights derived from the processed data in BigQuery.

### Running the Dashboard

1. Ensure you have the required dependencies installed:
   ```bash
   pip install streamlit plotly pandas google-cloud-bigquery python-dotenv
   ```

2. Set up your environment variables in the `.env` file with your GCP credentials and project information.

3. Start the Streamlit dashboard:
   ```bash
   streamlit run trade_dashboard.py
   ```

4. Access the dashboard in your browser at http://localhost:8501

### Dashboard Features

- **Interactive Filters**: Select specific years, countries, and metrics for customized analysis
- **Global Trade Overview**: Visualize global trade volume trends and year-over-year changes
- **Country Comparisons**: Compare trade metrics between countries with interactive charts
- **Product Analysis**: Explore product complexity and export specialization patterns
- **Economic Complexity**: Analyze economic complexity indices and their correlation with GDP
- **Data Tables**: View and download detailed trade data for further analysis