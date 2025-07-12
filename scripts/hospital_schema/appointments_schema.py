from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date

schema = {
    'appoIntment_Id': Integer(),
    'patient_Id': Integer(),
    'doct_Id': Integer(),
    'reason': String(),
    'appointment_Date': Date(),
    'payment_amount': Integer(),
    'mode_of_payment': String(),
    'mode_of_appointment': String(),
    'appointment_status': String()
}