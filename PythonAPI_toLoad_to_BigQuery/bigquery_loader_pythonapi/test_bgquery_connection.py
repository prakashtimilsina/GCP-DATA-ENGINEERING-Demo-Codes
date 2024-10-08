# test_bgquery_connection.py

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
import os

def test_bigquery_connection():
    #path to your service account JSON Key
    json_key_path = '/Users/prakashtimilsina/Documents/Learning/GCP/GCP-DATA-ENGINEERING-Demo-Codes/PythonAPI_toLoad_to_BigQuery/service_account.json'

    # Ensure the JSON Key file exists
    if not os.path.exists(json_key_path):
        print(f"Service Account key file not found at : {json_key_path}")
        return

    #Set the environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_key_path

    try:
        # Initialize bigquery client
        client = bigquery.Client()

        # Perform a simple operation: List datasets in the project
        datasets = list(client.list_datasets()) # Make an API request:

        if datasets:
            print("Datasets in Project:")
            for dataset in datasets:
                print(f"\t{dataset.dataset_id}")
        else: 
            print("No datasets found in the project.")
        # Alternatively, run a simple query
        query = '''
            SELECT 
                name,
                sum(number) as total
            FROM
                `bigquery-public-data.usa_names.usa_1910_2013`
            WHERE
                state = "TX"
            GROUP BY
                name
            ORDER BY 
                total DESC
            LIMIT 20
        ''' 
        query_job = client.query(query) # Make an API Request
        print(f"\nTop 20 Most common names in Texas:")
        for row in query_job:
            print(f"\t{row.name}: \t{row.total}")
    except GoogleAPIError as e:
        print(f"Google API Error: {e.message}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    test_bigquery_connection()


## Run this python script. python3 test_bgquery_connection.py