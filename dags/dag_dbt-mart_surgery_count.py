from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 14),
    'email': ['hapisbaihaqi@gmail.com'],              
    'email_on_failure': True,  
}

with DAG(
    dag_id='dbt-mart_surgery_count',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
    tags=['dbt'],
    description='dbt run mart_surgery_count',
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command = 'cd /opt/airflow/dbt_hospital && dbt run -m mart_surgery_count',
    )

    dbt_run