from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Add path to your scripts folder
sys.path.append("/home/zainab/university_dwh_project/scripts")

from extract_data import run as extract_run
from clean_data import run as clean_run
from transform_data import transform_data  # Import your transform function
from load_data import main as load_main  # Import load function from load_data.py

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG("university_dwh_pipeline",
         default_args=default_args,
         schedule_interval=None,  # Only run manually for now
         catchup=False,
         tags=["university", "etl"]) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_run
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_run
    )
    
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )
    
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_main
    )

    extract_task >> clean_task >> transform_task >> load_task
