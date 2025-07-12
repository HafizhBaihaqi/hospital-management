from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')
from hospital_data_pg import extractor_loader

default_args = {
    'start_date': datetime(2025, 7, 10),
    'catchup': False
}

with DAG(
    'excel_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    description='Extract all sheets from Hospital Management file to Postgres',
) as dag:

    load_task = PythonOperator(
        task_id='extractor_loader',
        python_callable=extractor_loader
    )