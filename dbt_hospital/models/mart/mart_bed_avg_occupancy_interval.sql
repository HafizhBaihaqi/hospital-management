{{
    config(
        materialized='table'
    )
}}

with bed_records as (
    select
        a.admission_id,
        a.bed_id,
        a.admission_date,
        a.discharge_date,
        date_diff(discharge_date, admission_date, day) as occupancy_interval,
        b.ward_id,
        c.ward_name
    from
        {{ ref('fact_bed_records') }}  a
    join
        {{ ref('dim_beds') }} b
    on
        a.bed_id = b.bed_id
    join
        {{ ref('dim_wards') }} c 
    on
        b.ward_id =  c.ward_id
)
select
    ward_name,
    avg(occupancy_interval) as avg_occupancy_interval
from
    bed_records
