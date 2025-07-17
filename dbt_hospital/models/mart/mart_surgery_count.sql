{{
    config(
        materialized='table'
    )
}}
  
select
    surgery_type,
    notes,
    count(distinct(patient_id)) as num_patient_surgeried
from
    {{ ref('fact_surgery_records') }}
group by
    1, 2