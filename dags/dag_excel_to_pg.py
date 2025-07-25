from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')
from hospital_data_pg import extractor_loader

default_args = {
    'start_date': datetime(2025, 7, 10),
    'email': ['hapisbaihaqi@gmail.com'],              
    'email_on_failure': True,  
}

with DAG(
    'excel_to_postgres',
    default_args=default_args,
    schedule_interval='@hourly',
    description='Extract all sheets from Hospital Management file to Postgres',
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='extractor_loader',
        python_callable=extractor_loader
    )