{{
    config(
        materialized='table'
    )
}}

with patients as (
    select
        patient_Id as patient_id,
        FName as first_name,
        LName as last_name,
        Gender as gender,
        date(Date_Of_Birth) as date_of_birth,
        contact_No as contact_no,
        pt_Address as address
    from
        {{ source('hospital', 'src_patients') }}
)
select
    *
from
    patients