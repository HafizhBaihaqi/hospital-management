from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 14),
}

with DAG(
    dag_id='dbt-stg_appointments-full_refresh',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False,
    tags=['dbt'],
    description='dbt run full refresh stg_appointments',
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command = 'cd /opt/airflow/dbt_hospital && dbt run -m stg_appointments --full-refresh',
    )

    dbt_run