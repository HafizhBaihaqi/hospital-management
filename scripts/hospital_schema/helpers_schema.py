from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'helper_Id': Integer(),
    'dept_Id': Integer(),
    'FName': String(),
    'LName': String(),
    'Gender': String(),
    'contact_No': String()
}