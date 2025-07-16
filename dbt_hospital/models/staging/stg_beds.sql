{{
    config(
        materialized='table'
    )
}}

with beds as (
    select
        bed_No as bed_id,
        ward_No as ward_id
    from
        {{ source('hospital', 'src_beds') }}
)
select
    *
from
    beds