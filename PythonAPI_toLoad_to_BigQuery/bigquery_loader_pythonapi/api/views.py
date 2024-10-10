#api/views.py
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import DataBatchSerializer

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import os
from typing import Dict, List, Any
from google.cloud.bigquery import SchemaField
from .utils import BigQueryConfig
import logging

logger = logging.getLogger(__name__)

# (Optional) if not setting GOOGLE_APPLICATION_CREDENTIALS via environment variables in settings.py

# Initializing Bigquery Client
#client = bigquery.Client()

# Configuration : Replace with your actual dataset and table IDs
# DATASET_ID = 'test_dataset' # 'your_dataset_id'
# TABLE_ID =  'test_table1' #your_table_id
# TABLE_REF = f"{client.project}.{DATASET_ID}.{TABLE_ID}"
# gcp-dataeng-demos-432703.test_dataset.test_table1

def home(request):
    return render(request, 'api/home.html')

def load_json(request):
    return render(request, 'api/load_json.html')

class LoadJsonView(APIView):
    def post(self, request, format=None):
        serializer = DataBatchSerializer(data=request.data)
        logger.info(f"serializer: {serializer}")
        if serializer.is_valid():
            records = serializer.validated_data.get('records', [])
            table_name = serializer.validated_data.get('table_name')
            logger.info(f"records {records} \ntable name {table_name}")
            rows_to_insert = [record for record in records]
            logger.info(f" rows to insert: {rows_to_insert}")

            # Retrieve table configuration from YAML
            try:
                table_config = BigQueryConfig.get_table_config(table_name)
            except KeyError as e:
                return Response(
                    {"error": str(e)},
                    status=status.HTTP_400_BAD_REQUEST
                )
            
            dataset_id = table_config.get('dataset_id')
            table_id = table_config.get('table_id')
            schema_config = table_config.get('schema', [])

            # Debugging Statements
            logger.debug(f"Table Name: {table_name}")
            logger.debug(f"Dataset ID: {dataset_id}")
            logger.debug(f"Table ID: {table_id}")
            logger.debug(f"Schema Config: {schema_config}")

            # Check if 'dataset_id' and 'table_id' are present.
            if not dataset_id or not table_id:
                return Response(
                    {"error": "'dataset_id' or 'table_id' is missing in the configuration."},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            # Initializing BigQuery Client
            client = bigquery.Client()
            print(f"!!CLIENT!! {client}")
            # Check if the table exists
            if not BigQueryConfig.table_exists(client, dataset_id, table_id):
                # convert schema from YAML to BigQuery SchemaFields objects
                schema = [SchemaField(name=field['name'], field_type=field['field_type'], mode=field.get('mode', 'NULLABLE')) for field in schema_config]
                try: 
                    BigQueryConfig.create_table(client, dataset_id, table_id, schema)
                
                except GoogleAPIError as e:
                    logger.error(f"Failed to create table: {e.message}")
                    return Response(
                        {"error": f"Failed to create table: {e.message}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                except Exception as e:
                    logger.error(f"Unexpected error during table creation: {str(e)}")
                    return Response(
                        {"error": f"Unexpected error during table creation: {str(e)}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
            table_ref = client.dataset(dataset_id).table(table_id)
            print(f"table_ref : {table_ref}")

            try:
                    # Retrieve the table (now guaranteed to exist)
                    table = client.get_table(table_ref) # API Request

                    # Insert data
                    errors = client.insert_rows_json(table, rows_to_insert) # API Request

                    if errors:
                        # Format errors for better readability
                        error_messages = [str(error) for error in errors]
                        logger.error(f"Errors inserting rows: {error_messages}")
                        return Response(
                            {"errors": error_messages},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                    logger.info(f"Data loaded successfully into {dataset_id}.{table_id}")
                    return Response(
                        {"message": "Data loaded successfully!"}, 
                        status=status.HTTP_201_CREATED
                        )
                
            except GoogleAPIError as e:
                logger.error(f"BigQuery API error: {e.message}")
                return Response(
                        {"error": f"BigQuery API error: {e.message}"},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
                
            except Exception as e:
                logger.error(f"Unexpected error during data insertion: {str(e)}")
                return Response(
                        {"error": str(e)},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )
            
        else:
            logger.warning(f"Serializer errors: {serializer.errors}")
            return Response(
                serializer.errors,
                status=status.HTTP_400_BAD_REQUEST
            )           