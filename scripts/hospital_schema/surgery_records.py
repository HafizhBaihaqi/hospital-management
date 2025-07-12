from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date, Time

schema = {
    'surgery_Id': Integer(),
    'patient_Id': Integer(),
    'surgeon_Id': Integer(),
    'surgery_Type': String(),
    'surgery_Date': Date(),
    'start_Time': Time(),
    'end_Time': Time(),
    'room_no': Integer(),
    'notes': String(),
    'nurse_Id': Integer(),
    'helper_Id': Integer()
}