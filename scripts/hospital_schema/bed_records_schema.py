from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'admission_Id': Integer(),
    'bed_No': Integer(),
    'patient_Id': Integer(),
    'nurse_Id': Integer(),
    'helper_Id': Integer(),
    'admission_Date': Date(),
    'discharge_Date': Date(),
    'amount': Integer(),
    'mode_of_payment': String()
}