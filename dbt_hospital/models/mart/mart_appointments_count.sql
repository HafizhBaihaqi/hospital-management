{{
    config(
        materialized='table'
    )
}}

select
    appointment_date,
    reason,
    count(distinct(patient_id)) as num_patient_completed
from
    {{ ref('fact_appointments') }}  
where
    lower(appointment_status) = 'completed'
group by
    1, 2