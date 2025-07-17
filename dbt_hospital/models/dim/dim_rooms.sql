{{
    config(
        materialized='table'
    )
}}

with rooms as (
    select
        room_No as room_id,
        dept_Id as dept_id,
        room_Type as room_type
    from
        {{ source('hospital', 'stg_rooms') }}
)
select
    *
from
    rooms