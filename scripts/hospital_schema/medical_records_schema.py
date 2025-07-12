from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'record_Id': Integer(),
    'doct_Id': Integer(),
    'patient_Id': String(),
    'visit_Date': Date(),
    'curr_Weight': Float(),
    'curr_height': Float(),
    'curr_Blood_Pressure': String(),
    'curr_Temp_F': Float(),
    'diagnosis': String(),
    'treatment': String(),
    'next_Visit': Date()
}