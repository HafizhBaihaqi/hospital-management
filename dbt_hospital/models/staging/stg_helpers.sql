{{
    config(
        materialized='table'
    )
}}

with helpers as (
    select
        helper_Id as helper_id,
        dept_Id as dept_id,
        FName as first_name,
        LName as last_name,
        Gender as gender,
        contact_No as contact_no
    from
        {{ source('hospital', 'src_helpers') }}
)
select
    *
from
    helpers