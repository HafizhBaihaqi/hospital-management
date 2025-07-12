from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'admisson_ID': Integer(),
    'room_no': Integer(),
    'patient_Id': String(),
    'nurse_Id': String(),
    'helper_Id': String(),
    'admission_Date': Date(),
    'discharge_Date': Date(),
    'amount': Integer(),
    'mode_of_payment': String()
}