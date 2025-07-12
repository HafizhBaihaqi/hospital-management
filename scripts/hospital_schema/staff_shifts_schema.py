from sqlalchemy import Integer, String, Float, Boolean, DateTime, Date, Time

schema = {
    'shift_Id': Integer(),
    'doct_Id': Integer(),
    'nurse_Id': String(),
    'helper_Id': String(),
    'shift_Date': String(),
    'shift_Start': Time(),
    'shift_End': Time()
}