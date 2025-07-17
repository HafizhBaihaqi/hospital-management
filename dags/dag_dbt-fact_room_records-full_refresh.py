from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 14),
}

with DAG(
    dag_id='dbt-fact_room_records-full_refresh',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False,
    tags=['dbt'],
    description='dbt run full refresh fact_room_records',
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command = 'cd /opt/airflow/dbt_hospital && dbt run -m fact_room_records --full-refresh',
    )

    dbt_run