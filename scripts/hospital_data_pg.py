import pandas as pd
from sqlalchemy import create_engine
import importlib.util
import os

# Path for the data
file_path = '/opt/airflow/data/Hospital Management System.xlsx'
# Directory for schema
schema_dir = '/opt/airflow/scripts/hospital_schema'
# SQL engine
engine = create_engine('postgresql+psycopg2://hospital-db:hafizh@postgres-data:5432/hospital-db')

# Dict for sheet name
sheet_name = {
    # SourceName : output_name 
    'Appointment':'appointments',
    'Bed':'beds',
    'BedRecords':'bed_records',
    'Department':'departments',
    'Doctor':'doctors',
    'Helpers':'helpers',
    'MedicalRecord':'medical_records',
    'Nurse':'nurses',
    'Patients':'patients',
    'Room':'rooms',
    'RoomRecords':'room_records',
    'StaffShift':'staff_shifts',
    'SurgeryRecord':'surgery_records',
    'Ward':'wards'
}

def extractor_loader():
    # For loop to iterate each sheet in the Excel file
    for source, output in sheet_name.items():
        print(f'Reading the file for {source}')
        # Save the data as a dataframe
        df = pd.read_excel(file_path, sheet_name=source)
        print(f'Saving {source} as {output}')

        # Set the schema path
        schema_path = os.path.join(schema_dir, f"{output}_schema.py")
        # Set the schema module
        spec = importlib.util.spec_from_file_location(f"{output}_schema", schema_path)
        schema_module = importlib.util.module_from_spec(spec)
        # Load the schema
        spec.loader.exec_module(schema_module)
        print(f'Load {output} schema')

        # Load the dataframe as a Postgres table
        df.to_sql(
            name = output,
            con = engine,
            if_exists = 'replace', # this is equal to full refresh
            index = False,
            dtype = schema_module.schema
        )

        print(f'Table {output} is loaded into PG')
