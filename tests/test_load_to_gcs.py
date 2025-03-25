import unittest
from unittest.mock import patch, MagicMock
import os
import sys

# Add the parent directory to the path so we can import the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock the google.cloud module before importing the module under test
sys.modules['google'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()

# Now import the module under test
from load_to_gcs import compress_file, upload_to_gcs


class TestLoadToGCS(unittest.TestCase):
    def setUp(self):
        # Mock environment variables
        self.env_patcher = patch.dict('os.environ', {
            'GCP_PROJECT_ID': 'test-project',
            'GCS_BUCKET_NAME': 'test-bucket',
            'GOOGLE_APPLICATION_CREDENTIALS': 'test-credentials.json'
        })
        self.env_patcher.start()
        
        # Mock time.time to return a consistent value
        self.time_patcher = patch('load_to_gcs.time.time', return_value=1234567890.0)
        self.time_patcher.start()
    
    def tearDown(self):
        self.env_patcher.stop()
        self.time_patcher.stop()
    
    @patch('load_to_gcs.shutil.copyfileobj')
    @patch('load_to_gcs.gzip.open')
    @patch('load_to_gcs.open')
    @patch('load_to_gcs.os.path.getsize')
    def test_compress_file(self, mock_getsize, mock_open, mock_gzip_open, mock_copyfileobj):
        # Setup mocks
        mock_getsize.side_effect = [1000, 200]  # Original size, compressed size
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file
        mock_gzip_file = MagicMock()
        mock_gzip_open.return_value.__enter__.return_value = mock_gzip_file
        
        # Call the function
        result = compress_file('test_file.csv')
        
        # Verify the result
        self.assertEqual(result, 'test_file.csv.gz')
        
        # Verify the file was opened correctly
        mock_open.assert_called_once_with('test_file.csv', 'rb')
        mock_gzip_open.assert_called_once_with('test_file.csv.gz', 'wb')
        
        # Verify copyfileobj was called
        mock_copyfileobj.assert_called_once()
        
        # Verify the file sizes were checked
        self.assertEqual(mock_getsize.call_count, 2)

    @patch('load_to_gcs.storage_client')
    def test_upload_to_gcs(self, mock_storage_client):
        # Setup mocks
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Call the function
        result = upload_to_gcs(
            bucket_name='test-bucket',
            source_file_path='test_file.csv.gz',
            destination_blob_name='raw/test_file.gz'
        )
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify the bucket and blob were created correctly
        mock_storage_client.bucket.assert_called_once_with('test-bucket')
        mock_bucket.blob.assert_called_once_with('raw/test_file.gz')
        
        # Verify the file was uploaded
        mock_blob.upload_from_filename.assert_called_once_with('test_file.csv.gz', timeout=300)


if __name__ == '__main__':
    unittest.main()
