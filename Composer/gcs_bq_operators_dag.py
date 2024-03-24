# Import dependencies/statements - libraries or module
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorgeToBigQueryOperator

# Python logic to derive yesterday's date
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments - dictionary
default_arg = {
    'start_date' : yesterday,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}


# DAG Definitions
with DAG(dag_id='GCS_to_BQ_and_AGG',
         catchup=False,
         schedule_interval = timedelta(days=1),
         default_args=default_arg
        ) as dag:
    # Dummy start task
    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    #GCS to BigQuery Data load Operator and task
    gcs_to_bq_load=GoogleCloudStorgeToBigQueryOperator(
        task_id='gcs_to_bq_load',
        bucket='data_eng_demos',
        source_objects=['sample_file.csv'],
        destination_project_dataset_table='data_eng_demo18.gcp_dataeng_demos.gcs_to_bq_table',
        schema_fields=[
            {'name':'year', 'type':'STRING', 'mode':'NULLABLE'},
            {'name':'name', 'type':'STRING', 'mode':'NULLABLE'}
        ],
        skip_heading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag
    )

    # BigQuery Task, Operator
    create_aggr_bg_table=BigQueryOperator(
        task_id='create_aggr_bg_table',
        use_legacy_sql=False,
        allow_large_results=True,
        sql="CREATE OR REPLACE TABLE gcp_dataeng_demos.bg_table_aggr AS SELECT year, name, sum(data_value) as sum_value FROM data_eng_demo18.gcp_dataeng_demos.gcs_to_bq_table GROUP BY year, name",
        dag=dag
    )

    # Dummy end task
    end=DummyOperator(
        task_id='end',
        dag=dag
    )

    # Setting up task dependency
    start >> gcs_to_bq_load >> create_aggr_bg_table >> end
