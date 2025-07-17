{{
    config(
        materialized='table'
    )
}}

with nurses as (
    select
        nurse_Id as nurse_id,
        dept_Id as dept_id,
        FName as first_name,
        LName as last_name,
        Gender as gender,
        conatct_No as contact_no
    from
        {{ source('hospital', 'stg_nurses') }}
)
select
    *
from
    nurses