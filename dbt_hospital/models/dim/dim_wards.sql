{{
    config(
        materialized='table'
    )
}}

with wards as (
    select
        ward_No as ward_id,
        ward_Name as ward_name,
        dept_Id as dept_id
    from
        {{ source('hospital', 'stg_wards') }}
)
select
    *
from
    wards