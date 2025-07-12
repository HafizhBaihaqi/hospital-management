from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'nurse_Id': Integer(),
    'dept_Id': Integer(),
    'FName': String(),
    'LName': String(),
    'Gender': String(),
    'conatct_No': String()
}