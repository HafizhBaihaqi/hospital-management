from google.cloud import bigquery
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta

# Configs
gcs_bucket = 'jdeol003-bucket'
gcs_folder = 'capstone3_hafizh'
project_id = 'purwadika'
dataset_id = 'jcdeol3_final_project_hafizh'

# SA
credentials_path = '/opt/airflow/keys/purwadika-5a3c4e2b7b24.json'

# Postgres engine
engine = create_engine('postgresql+psycopg2://hospital-db:hafizh@postgres-data:5432/hospital-db')

# Full Table for non incremental
full_table = [
    'beds',
    'departments',
    'doctors',
    'helpers',
    'nurses',
    'patients',
    'rooms',
    'staff_shifts',
    'wards'
]

# Increment Table
increment_table = {
    'appointments': 'appointment_Date',
    'bed_records': 'admission_Date',
    'medical_records': 'visit_Date',
    'room_records': 'admission_Date',
    'surgery_records': 'surgery_Date'
}

def load_pg_to_bq():
    # Set client
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

    # For loop to iterate full table
    for table in full_table:
        print(f'Loading {table} from PG')

        # Save the table as a dataframe
        df_full = pd.read_sql_table(table, con=engine)

        print(f'Saving {table}')

        # Set table ID
        table_id = f"{project_id}.{dataset_id}.stg_{table}"
        # Set job configuration
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE" )

        # Load the dataframe to as BigqQuery table
        job = client.load_table_from_dataframe(df_full, table_id, job_config=job_config)
        job.result()

        print(f"Table stg_{table} is loaded into BQ {project_id}.{dataset_id}")

    # For loop to iterate incremental table
    for table, date in increment_table.items():
        print(f'Loading {table} from PG')

        # Save the table as a dataframe
        df_increment = pd.read_sql_table(table, con=engine)
        # Only to save data D-1
        df_increment = df_increment[df_increment[date].dt.date <= (datetime.now() - timedelta(days=1)).date()] 

        print(f'Saving {table}')
        
        # Set table ID
        table_id = f"{project_id}.{dataset_id}.stg_{table}"
        # Set job configuration
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE" )

        # Load the dataframe to as BigqQuery table
        job = client.load_table_from_dataframe(df_increment, table_id, job_config=job_config)
        job.result()

        print(f"Table stg_{table} is loaded into BQ {project_id}.{dataset_id}")