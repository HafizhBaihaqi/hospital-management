from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'doct_Id': Integer(),
    'dept_Id': Integer(),
    'FName': String(),
    'LName': String(),
    'Gender': String(),
    'contact_No': String(),
    'surgeon_Type': String(),
    'office_No': Integer()
}