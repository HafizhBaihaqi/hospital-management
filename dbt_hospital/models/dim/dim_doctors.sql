{{
    config(
        materialized='table'
    )
}}

with doctors as (
    select
        doct_Id as doctor_id,
        dept_Id as dept_id,
        FName as first_name,
        LName as last_name,
        Gender as gender,
        contact_No as contact_no,
        surgeon_Type as surgeon_type,
        cast(office_No as int) as office_id
    from
        {{ source('hospital', 'stg_doctors') }}
)
select
    *
from
    doctors