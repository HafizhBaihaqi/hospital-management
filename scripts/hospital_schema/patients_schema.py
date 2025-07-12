from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'patient_Id': Integer(),
    'FName': String(),
    'LName': String(),
    'Gender': String(),
    'Date_Of_Birth': Date(),
    'conatct_No': String(),
    'pt_Address': String()
}