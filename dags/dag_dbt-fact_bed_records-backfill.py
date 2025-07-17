from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 14),
}

with DAG(
    dag_id='dbt-fact_bed_records-backfill',
    default_args=default_args,
    schedule_interval='@once', 
    catchup=False,
    tags=['dbt'],
    description='dbt run backfill fact_bed_records',
    params={
        'date_start': '',
        'date_until': ''
        }
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            "cd /opt/airflow/dbt_hospital && "
            "dbt run -m fact_bed_records "
            "--vars '{\"backfill\": true, "
            "\"date_start\": \"{{ params.date_start }}\", "
            "\"date_until\": \"{{ params.date_until }}\"}'"
        ),
    )

    dbt_run