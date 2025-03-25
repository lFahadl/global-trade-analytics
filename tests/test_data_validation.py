import unittest
import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TestDataValidation(unittest.TestCase):
    
    def setUp(self):
        self.client = bigquery.Client()
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.raw_dataset = "raw_trade_data"
        self.processed_dataset = "processed_trade_data"
        self.analytics_dataset = "trade_analytics"
    
    def test_country_id_year_uniqueness(self):
        """Test that country_id for each year is unique in mv_country_annual_trade."""
        query = f"""
        WITH duplicates AS (
            SELECT 
                country_id, 
                year, 
                COUNT(*) as count
            FROM `{self.project_id}.{self.processed_dataset}.mv_country_annual_trade`
            GROUP BY country_id, year
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) as duplicate_count FROM duplicates
        """
        
        query_job = self.client.query(query)
        results = query_job.result()
        row = list(results)[0]
        
        self.assertEqual(row.duplicate_count, 0, "Found duplicate country_id and year combinations")

if __name__ == '__main__':
    unittest.main()
