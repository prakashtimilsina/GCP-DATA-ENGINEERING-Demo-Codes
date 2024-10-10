import os
from django.conf import settings
import yaml 
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from typing import List, Dict, Any
from google.cloud.bigquery import SchemaField


class BigQueryConfig:
    _config = None

    @classmethod
    def load_config(cls):
        if cls._config is None:
            config_path = os.path.join(settings.BASE_DIR, 'config', 'bigquery_config.yaml')
            print(f"config yaml path: {config_path}")
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Bigquery yaml configuration file not found at {config_path}")
            with open(config_path, 'r') as file:
                cls._config = yaml.safe_load(file)
            print(f"safe load yaml output: {cls._config}")
        return cls._config
    
    @classmethod
    def get_table_config(cls, table_name: str=None) -> Dict[str, Any]:
        config = cls.load_config()
        if table_name and table_name in config.get('table', {}):
            return config['tables'][table_name]
        else:
            table_config = config.get('default', {})
        
        if 'table_id' not in table_config:
            raise KeyError(f"'table_id' is missing in the YAML configuration file for table '{table_name or 'default'}'")
        
        print(f"Fetching config for table: {table_name}")
        print(f"Retrieved table config: {table_config}")

        return config.get('default', {})
    
    @classmethod
    def table_exists(cls, client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
        table_ref = client.dataset(dataset_id).table(table_id)
        try:
            client.get_table(table_ref)
        except NotFound:
            return False
    
    @classmethod
    def create_table(cls, client: bigquery.Client, dataset_id: str, table_id: str, schema: List[Dict[str, Any]]):
        table_ref = client.dataset(dataset_id).table(table_id)

        # Convert schema dict to SchemaField objects
        schema_fields = [SchemaField(**field) for field in schema]
        table = bigquery.Table(table_ref, schema=schema_fields)
        client.create_table(table)
        print(f"Table {dataset_id}.{table_id} is created at BigQuery!")