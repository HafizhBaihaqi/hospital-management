{{
    config(
        materialized='table'
    )
}}

with staff_shifts as (
    select
        shift_Id as shift_id,
        doct_Id as doctor_id,
        nurse_Id as nurse_id,
        helper_Id as helper_id,
        date(shift_Date) as shift_date,
        shift_Start as shift_start,
        shift_End as shift_end
    from
        {{ source('hospital', 'src_staff_shifts') }}
)
select
    *
from
    staff_shifts