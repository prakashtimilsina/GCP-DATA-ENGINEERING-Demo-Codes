# Import dependencies - libraries or module
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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

#Python custom logic/Function for python operator
def print_hello():
    print('Hey, I am Python Operator')


# DAG Definition -- This name (dag_id will be displayed in AirFlow UI)
with DAG( dag_id='bash_python_operator_demo',
         catchup=False,
         schedule_interval=timedelta(days=1),   # crontab schedule format is also applicable.
         default_arg=default_arg
        ) as dag:
    
    # Tasks stars here

    # Dummy Start Task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Bash Operator , task
    bask_task = BashOperator(
        task_id='bash_task',
        bash_command="date; echo 'Hey I am a bash operator'",
    )

    #Python Operator, task
    python_task = PythonOperator(
        task_id='python_task',
        python_callable = print_hello,
        dag=dag
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    # Setting up Task Dependencies using AirFlow standard notations
    start >> bask_task >> python_task >> end