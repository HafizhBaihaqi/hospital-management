import pandas as pd
from sqlalchemy import create_engine
import importlib.util
import os

# Path for the data
file_path = '/opt/airflow/data/Hospital Management System.xlsx'

# Dict for sheet name
sheet_name = {
    # SourceName : output_name 
    'Appointment':'appointments'
    # 'Bed':'beds',
    # 'BedRecords':'bed_records',
    # 'Department':'departments',
    # 'Doctor':'doctors',
    # 'Helpers':'helpers',
    # 'MedicalRecords':'medical_records',
    # 'Nurse':'nurses',
    # 'Patients':'patients',
    # 'Room':'rooms',
    # 'RoomRecords':'room_records',
    # 'StaffShift':'staff_shifts',
    # 'SurgeryRecord':'surgery_records',
    # 'Ward':'wards'
}

# Directory for schema
schema_dir = '/opt/airflow/scripts/hospital_schema'

# SQL engine
engine = create_engine('postgresql+psycopg2://hospital-db:hafizh@postgres-data:5432/hospital-db')

def extractor_loader():
    for source, output in sheet_name.items():
        df = pd.read_excel(file_path, sheet_name=source)

        # Dynamically import schema module inline
        schema_path = os.path.join(schema_dir, f"{output}_schema.py")
        spec = importlib.util.spec_from_file_location(f"{output}_schema", schema_path)
        schema_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(schema_module)

        df.to_sql(
            name = output,
            con = engine,
            if_exists = 'replace', # this is equal to full refresh
            index = False,
            dtype = schema_module.schema
        )

        print(f'Table {output} is loaded into PG')
