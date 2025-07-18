{{
    config(
        materialized='table'
    )
}}

with dept as (
    select
        dept_Id as dept_id,
        dept_Name as dept_name
    from
        {{ source('hospital', 'stg_departments') }}
)
select
    *
from
    dept