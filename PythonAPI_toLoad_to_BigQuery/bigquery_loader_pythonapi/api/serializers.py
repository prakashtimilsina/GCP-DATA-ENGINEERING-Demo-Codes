#api/serializers.py
from rest_framework import serializers

class DataItemSerializer(serializers.Serializer):
    # Define fields according to BigQuery table schema
    name = serializers.CharField(max_length=100)
    age = serializers.IntegerField()
    email = serializers.EmailField()
    # Add more fields as necessary

class DataBatchSerializer(serializers.Serializer):
    records = DataItemSerializer(many=True)