{{
    config(
        materialized='table'
    )
}}

select
    appointment_date,
    count(distinct(
        case
            when lower(appointment_status) = 'completed' then patient_id end
    )) as num_patient_completed,
    count(distinct(
        case
            when lower(appointment_status) = 'cancelled' then patient_id end
    )) as num_patient_cancelled,
    count(distinct(
        case
            when lower(appointment_status) = 'no-show' then patient_id end
    )) as num_patient_noshow,
    count(distinct(
        case
            when lower(appointment_status) = 'scheduled' then patient_id end
    )) as num_patient_scheduled
from
    {{ ref('fact_appointments') }}  
group by
    1