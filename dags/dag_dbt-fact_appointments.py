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
    dag_id='dbt-fact_appointments',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False,
    tags=['dbt'],
    description='dbt run fact_appointments',
) as dag:
    
    wait_for_postgres_to_bq = ExternalTaskSensor(
        task_id='wait_for_postgres_to_bq',
        external_dag_id='postgres_to_bigquery',
        external_task_id=None,
        allowed_states=['success'],
        mode='reschedule',
        timeout=300
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command = 'cd /opt/airflow/dbt_hospital && dbt run -m fact_appointments',
    )

    wait_for_postgres_to_bq >> dbt_run