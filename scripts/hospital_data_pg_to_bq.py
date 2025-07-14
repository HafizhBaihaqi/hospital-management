from google.cloud import bigquery
from sqlalchemy import create_engine
import pandas as pd

gcs_bucket = 'jdeol003-bucket'
gcs_folder = 'capstone3_hafizh'
project_id = 'purwadika'
dataset_id = 'jcdeol3_final_project_hafizh'
credentials_path = '/opt/airflow/keys/purwadika-5a3c4e2b7b24.json'
engine = create_engine('postgresql+psycopg2://hospital-db:hafizh@postgres-data:5432/hospital-db')

full_table = [
    'beds',
    'bed_records',
    'departments',
    'doctors',
    'helpers',
    'medical_records',
    'nurses',
    'patients',
    'rooms',
    'room_records',
    'staff_shifts',
    'surgery_records',
    'wards'
]

increment_table = [
    'appointments',
]

def load_pg_to_bq():
    client = bigquery.Client.from_service_account_json(credentials_path, project=project_id)

    for i in full_table:
        print(f'Loading {i} from PG')
        df = pd.read_sql_table(i, con=engine)

        print(f'Saving {i}')

        table_id = f"{project_id}.{dataset_id}.src_{i}"

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE" )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"Table {i} is loaded into BQ {project_id}.{dataset_id}")