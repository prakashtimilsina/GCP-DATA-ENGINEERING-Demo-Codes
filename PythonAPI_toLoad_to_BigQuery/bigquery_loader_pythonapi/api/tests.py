# api/tests.py

from django.test import TestCase
from .utils import BigQueryConfig
from google.cloud import bigquery
from unittest.mock import MagicMock

class BigQueryConfigTest(TestCase):
    def setUp(self):
        # Mock BigQuery client
        self.client = MagicMock(spec=bigquery.Client)

    def test_get_table_config_default(self):
        config = BigQueryConfig.get_table_config()
        self.assertIn('dataset_id', config)
        self.assertIn('table_id', config)
        self.assertIn('schema', config)

    def test_get_table_config_specific_table(self):
        config = BigQueryConfig.get_table_config('users')
        self.assertEqual(config['table_id'], 'users_table')
        self.assertEqual(config['dataset_id'], 'users_dataset')
        self.assertIn
