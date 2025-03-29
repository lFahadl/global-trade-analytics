# Global Trade Analytics

A data pipeline for processing large trade data CSV files to Google Cloud Storage and BigQuery with analytics transformations.

## Project Overview

This project implements a data pipeline for loading large CSV files (>1GB) from local storage to Google Cloud Storage and then to BigQuery. The pipeline follows a three-step approach:

1. **Step 1**: Load CSV files from local storage to Google Cloud Storage (using gzip compression)
2. **Step 2**: Load data from GCS to BigQuery using direct GCP APIs
3. **Step 3**: Transform raw data in BigQuery into analytics-ready views and materialized views

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

## Setup and Usage

### Prerequisites

- Python 3.7+
- Google Cloud Platform account with appropriate permissions
- Service account credentials (stored in `Global-Trade-Analytics.json`)

### Installation

1. Clone the repository
2. Set up a virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Configure environment variables in `.env`

### Running the Pipeline

1. **Upload to GCS**: `python load_to_gcs.py` or `python load_to_gcs.py --test` for test data
2. **Load to BigQuery**: `python load_to_bigquery.py` or `python load_to_bigquery.py --test` for test data
3. **Create Combined Dataset**: `python create_combined_dataset.py`
4. **Transform Data**: `python new_transform_data.py`

## Data Organization

The project uses a four-tier dataset organization in BigQuery:

1. **Raw Data** (`raw_trade_data`): Original, unmodified data loaded from source files
2. **Combined Data** (`combined_trade_data`): Optimized table combining all source tables
3. **Processed Data** (`processed_trade_data`): Intermediate, cleaned data with materialized views
4. **Analytics** (`trade_analytics`): Final analytics-ready views with metrics and KPIs

## Performance Optimizations

- **Idempotent Operations**: All scripts check for existing data before processing
- **Partitioning by Year**: The combined table is partitioned by year for efficient time-based queries
- **Clustering by Country and Product**: The combined table is clustered by country_id and product_id
- **MERGE Operations**: Uses BigQuery MERGE for idempotent data updates
- **Gzip Compression**: CSV files are compressed before uploading, reducing file sizes by ~7x
- **Parallel Processing**: Multiple files are processed concurrently

## Infrastructure Management

The project uses Terraform to manage Google Cloud infrastructure:

- **BigQuery Datasets**: Three datasets are created for the data pipeline
- **Google Cloud Storage**: A data lake bucket with a raw folder for storing compressed CSV files
- **Service Account**: A dedicated service account for the data pipeline with appropriate permissions

### Development vs. Production

For development and testing environments, the Terraform configuration includes `delete_contents_on_destroy = true` for BigQuery datasets. This ensures that all tables and views are deleted when running `terraform destroy`, making it easier to clean up resources during testing.

**Note**: For production environments, you may want to remove this setting to prevent accidental data loss.

## Data Transformations

The project includes several BigQuery transformations to prepare the data for analytics:

1. **Materialized Views**: Pre-aggregated data for efficient querying
   - `mv_country_annual_trade`: Country-level trade metrics by year
   - `mv_country_pairs_annual_trade`: Bilateral trade relationships
   - `mv_product_annual_trade`: Product-level trade data

2. **Analytics Views**: Ready-to-use views for specific metrics
   - `v_global_trade_metrics`: Global trade volume and economic complexity with YoY changes
   - `v_top_traded_products`: Top traded products with YoY changes
   - `v_top_trading_partners`: Top trading partners for each country

3. **Fallback Mechanism for Single-Year Data**:
   - The transformation layer includes a fallback mechanism for countries with only one year of data
   - For countries with multiple years, actual year-over-year changes in ECI and trade balance are calculated
   - For countries with only one year, default values are assigned (0 for changes, "No change data" for trend category)
   - This ensures all countries appear in visualizations, even those without historical data
   - The dashboard UI clearly indicates countries with single-year data

## Performance Considerations

The data pipeline uses several techniques to optimize performance:

1. **Gzip Compression**: CSV files are compressed before uploading, reducing file sizes by ~7x
2. **Parallel Processing**: Multiple files are processed concurrently
3. **Direct GCP APIs**: Uses direct Google Cloud APIs for optimal performance
4. **Materialized Views**: Pre-aggregated data reduces query costs and improves performance

For very large files or high-performance requirements, consider using `gsutil` for even faster uploads.

## Testing

The project includes a `test_data/` directory containing a smaller subset of the data for testing purposes. The full dataset includes multiple files larger than 1GB each, which can be time-consuming to upload to GCS. For quicker testing and demonstration, use the test data option:

```bash
python load_to_gcs.py --test
python load_to_bigquery.py --test
```

This will process only the files in the `test_data/` directory, which includes the smallest complete CSV file from the dataset (approximately 350MB). This provides a realistic test while being much faster than processing the full multi-gigabyte files.

Unit tests are available in the `tests/` directory and can be run with:

```bash
python run_tests.py
```

## Changes

- **Dashboard Performance Optimization**: Implemented a two-step approach using regular tables and materialized views to reduce query data scan from 2.22 GB to a few KB
- **Dashboard Simplification**: Temporarily commented out additional dashboard pages that require improved data models for optimal performance (to be implemented in future iterations)
- **Data Accuracy Fix (Global Metrics)**: Corrected logic in `v_global_yearly_metrics` view; removed incorrect division by 2 as summing country-level totals directly provides global aggregates.
- **BigQuery Materialized View Workaround**: Created regular tables for complex aggregations (with COUNT DISTINCT) to overcome BigQuery materialized view limitations
- **Optimized Data Storage**: Created a combined table with range partitioning by year and clustering by country_id and product_id
- **Location Consistency**: Ensured all datasets use the same location (us-central1) to prevent cross-location query errors
- **Idempotent Operations**: Implemented MERGE operations and DISTINCT selects to ensure data pipeline idempotency
- **Temporary Dataset Solution**: Added temporary dataset approach to handle location constraints