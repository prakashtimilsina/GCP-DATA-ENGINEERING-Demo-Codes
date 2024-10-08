#api/views.py
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .serializers import DataBatchSerializer

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import os

# (Optional) if not setting GOOGLE_APPLICATION_CREDENTIALS via environment variables in settings.py

# Initializing Bigquery Client
client = bigquery.Client()

# Configuration : Replace with your actual dataset and table IDs
DATASET_ID = 'test_dataset' # 'your_dataset_id'
TABLE_ID =  'test_table1' #your_table_id
TABLE_REF = f"{client.project}.{DATASET_ID}.{TABLE_ID}"
# gcp-dataeng-demos-432703.test_dataset.test_table1

def home(request):
    return render(request, 'api/home.html')

def load_json(request):
    return render(request, 'api/load_json.html')

class LoadJsonView(APIView):
    def post(self, request, format=None):
        serializer = DataBatchSerializer(data=request.data)
        print(f"serializer: {serializer}")
        if serializer.is_valid():
            records = serializer.validated_data.get('records', [])
            print(f"records {records}")
            rows_to_insert = [record for record in records]
            print(f" rows to insert: {rows_to_insert}")

            try:
                # Retrieve the table
                table = client.get_table(TABLE_REF) # API Request

                # Insert data
                errors = client.insert_rows_json(table, rows_to_insert) # API Request

                if errors:
                    # Format errors for better readability
                    error_messages = [str(error) for error in errors]
                    return Response(
                        {"errors": error_messages},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                return Response({"message": "Data loaded successfully!"}, status=status.HTTP_201_CREATED)
            except GoogleAPIError as e:
                return Response(
                    {"error": f"BigQuery API error: {e.message}"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            except Exception as e:
                return Response(
                    {"error": str(e)},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
            
        else:
            return Response(
                serializer.errors,
                status=status.HTTP_400_BAD_REQUEST
            )           