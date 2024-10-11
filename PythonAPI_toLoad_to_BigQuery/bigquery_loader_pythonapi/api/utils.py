import os
from django.conf import settings
import yaml 
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from typing import List, Dict, Any
from google.cloud.bigquery import SchemaField
import logging

logger = logging.getLogger(__name__)


class BigQueryConfig:
    _config = None

    @classmethod
    def load_config(cls):
        if cls._config is None:
            config_path = os.path.join(settings.BASE_DIR, 'config', 'bigquery_config.yaml')
            logger.info(f"config yaml path: {config_path}")
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Bigquery yaml configuration file not found at {config_path}")
            with open(config_path, 'r') as file:
                cls._config = yaml.safe_load(file)
                logger.debug(f"safe load yaml output:- {cls._config}")
        return cls._config
    
    @classmethod
    def get_table_config(cls, table_name: str=None) -> Dict[str, Any]:
        config = cls.load_config()
        logger.debug(f"Resolving table config: {config}")
        if table_name and table_name in config.get('tables ', {}):
            table_config = config['tables'][table_name]
            logger.debug(f"Retrieved table config for '{table_name}': {table_config}")
            return table_config 
        table_config = config.get('default', {})
        logger.debug(f"Retrieved default table config: {table_config} ")
        return table_config
    
    @classmethod
    def table_exists(cls, client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
        table_ref = client.dataset(dataset_id).table(table_id)
        try:
            client.get_table(table_ref)
            logger.debug(f"Table '{dataset_id}.{table_id}' exists.")
            return True
        except NotFound:
            logger.debug(f"Table '{dataset_id}.{table_id}' does not exist.")
            return False
    
    @classmethod
    def create_table(cls, client: bigquery.Client, dataset_id: str, table_id: str, schema: List[Dict[str, Any]]):
        table_ref = client.dataset(dataset_id).table(table_id)
        # map 'type' to 'field_type'
        # mapped_schema = []
        # for field in schema:
        #     mapped_field = field.copy()
        #     if 'type' in mapped_field:
        #         mapped_field['field_type'] = mapped_field.pop('type')
        #     mapped_field.append(mapped_field)

        # Convert schema dict to SchemaField objects
        try:
            schema_fields = [SchemaField(**field) for field in schema]
            logger.debug(f"Converted schema fields: {schema_fields}")
        except TypeError as e:
            logger.error(f"Error converting schema fields: {str(e)}")
            raise

        table = bigquery.Table(table_ref, schema=schema_fields)
        client.create_table(table)
        logger.info(f"Table {dataset_id}.{table_id} is created at BigQuery!")