from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')
from hospital_data_pg_to_bq import load_pg_to_bq

default_args = {
    'start_date': datetime(2025, 7, 11)
}

# buat alert

with DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    schedule_interval='@daily',
    description='Extract all tables from Postgres to BigQuery',
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_pg_to_bq',
        python_callable=load_pg_to_bq
    )