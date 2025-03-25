import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Add the parent directory to the path so we can import the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock the google.cloud module before importing the module under test
sys.modules['google'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.bigquery'] = MagicMock()

# Now import the module under test
from load_to_bigquery import load_gcs_to_bigquery


class TestLoadToBigQuery(unittest.TestCase):
    def setUp(self):
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'GCP_PROJECT_ID': 'test-project',
            'GCS_BUCKET_NAME': 'test-bucket',
            'GOOGLE_APPLICATION_CREDENTIALS': 'test-credentials.json'
        })
        self.env_patcher.start()
        
        # Reset module variables that depend on environment variables
        import load_to_bigquery
        load_to_bigquery.GCP_PROJECT_ID = 'test-project'
        load_to_bigquery.GCS_BUCKET = 'test-bucket'
        load_to_bigquery.GCP_CREDENTIALS_PATH = 'test-credentials.json'
    
    def tearDown(self):
        self.env_patcher.stop()
    
    @patch('load_to_bigquery.bigquery.Client')
    def test_load_gcs_to_bigquery(self, mock_client_class):
        # Setup mock objects
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        mock_load_job = MagicMock()
        mock_table = MagicMock(num_rows=100)
        
        # Configure the mock client
        mock_client.load_table_from_uri.return_value = mock_load_job
        mock_client.get_table.return_value = mock_table
        
        # Call the function
        load_gcs_to_bigquery(
            bucket_name='test-bucket',
            source_blob_name='raw/test_file.gz',
            dataset_id='test_dataset',
            table_id='test_table'
        )
        
        # Verify the client was initialized
        mock_client_class.assert_called_once()
        
        # Verify load_table_from_uri was called with the correct parameters
        mock_client.load_table_from_uri.assert_called_once()
        args, kwargs = mock_client.load_table_from_uri.call_args
        self.assertEqual(args[0], 'gs://test-bucket/raw/test_file.gz')
        
        # Verify the job result was awaited
        mock_load_job.result.assert_called_once()
        
        # Verify the table was retrieved to get row count
        mock_client.get_table.assert_called_once()

    @patch('load_to_bigquery.bigquery.Client')
    def test_load_gcs_to_bigquery_error_handling(self, mock_client_class):
        # Setup mock to raise an exception
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.load_table_from_uri.side_effect = Exception('Test error')
        
        # Call the function and verify it handles the exception
        with self.assertRaises(Exception):
            load_gcs_to_bigquery(
                bucket_name='test-bucket',
                source_blob_name='raw/test_file.gz',
                dataset_id='test_dataset',
                table_id='test_table'
            )


if __name__ == '__main__':
    unittest.main()
