#api/serializers.py
from rest_framework import serializers
from .utils import BigQueryConfig
import logging

logger = logging.getLogger(__name__)

class DynamicDataItemSerializer(serializers.Serializer):
    def __init__(self, *args, **kwargs):
        table_name = kwargs.pop('table_name', None)
        super(DynamicDataItemSerializer, self).__init__(*args, **kwargs)

        table_config = BigQueryConfig.get_table_config(table_name)
        schema = table_config.get('schema', [])

        for field in schema:
            field_name = field['name']
            field_type = field['field_type']
            field_mode = field.get('mode', 'NULLABLE')

            # Map BigQuery types to DRF serializer fields
            if field_type == 'STRING':
                self.fields[field_name] = serializers.CharField(max_length=255, required=(field_mode != 'NULLABLE'))
            elif field_type == 'INTEGER':
                self.fields[field_name] = serializers.IntegerField(required=(field_mode != 'NULLABLE'))
            elif field_type == 'FLOAT':
                self.fields[field_name] = serializers.FloatField(required=(field_mode != 'NULLABLE'))
            elif field_type == 'BOOLEAN':
                self.fields[field_name] = serializers.BooleanField(required=(field_mode != 'NULLABLE'))
            elif field_type == 'TIMESTAMP':
                self.fields[field_name] = serializers.DateTimeField(required=(field_mode != 'NULLABLE'))
            else: 
                # Handled unsupported types or default to CharField
                self.fields[field_name] = serializers.CharField(required=(field_mode != 'NULLABLE'))
            # Add more as needed.


class DataBatchSerializer(serializers.Serializer):
    table_name = serializers.CharField(max_length=100, required=False)
    records = serializers.ListField()

    def to_internal_value(self, data):
        table_name = data.get('table_name', None)
        records = data.get('records', [])
        if not isinstance(records, list):
            raise serializers.ValidationError({"records": "This field must be a list."})
        
        # Initialize the DynamicDataItemSerializer with the table_name
        child_serializer = DynamicDataItemSerializer(table_name=table_name, many=True)
        validated_records = child_serializer.run_validation(records)
        logger.debug(f"Table Name from DataBatchSerializer function:- {table_name}")
        logger.debug(f"Records from DataBatchSerializer function:- {validated_records}")

        return {
            'table_name' : table_name,
            'records': validated_records
        }
    
    

'''
Static Single serializer 
class DataItemSerializer(serializers.Serializer):
    # Define fields according to BigQuery table schema
    # Adjust or make dynamic if handling multiple schemas
    name = serializers.CharField(max_length=100)
    age = serializers.IntegerField()
    email = serializers.EmailField()
    # Add more fields as necessary based on your YAML Schema

class DataBatchSerializer(serializers.Serializer):
    table_name = serializers.CharField(max_length=100, required=False)
    records = DataItemSerializer(many=True)

'''

