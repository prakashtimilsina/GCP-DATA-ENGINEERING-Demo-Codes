#api/serializers.py
from rest_framework import serializers
from .utils import BigQueryConfig

class DynamicDataItemSerializer(serializers.Serializer):
    def __iint__(self, *args, **kwargs):
        table_name = kwargs.pop('table_name', None)
        super(DynamicDataItemSerializer, self).__init__(*args, **kwargs)

        table_config = BigQueryConfig.get_table_config(table_name)
        schema = table_config.get('schema', [])

        for field in schema:
            field_name = field['name']
            field_type = field['type']
            field_mode = field['mode']

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
            # Add more as needed.


class DataBatchSerializer(serializers.Serializer):
    table_name = serializers.CharField(max_length=100, required=False)
    records = serializers.ListField(
        child=DynamicDataItemSerializer()
    )
    def __init__(self, *args, **kwargs):
        table_name = kwargs['data'].get('table_name') if 'data' in kwargs and kwargs['data'] else None
        super(DataBatchSerializer, self).__init__(*args, **kwargs)
        self.fields['records'].child = DynamicDataItemSerializer(table_name=table_name)

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

