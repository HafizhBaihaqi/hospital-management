{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'lpep_pickup_datetime',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        unique_key='id',
        incremental_strategy = 'insert_overwrite'
    )
}}

with appointments as (
    select
        appoIntment_Id as appointment_id,
        patient_Id as patient_id,
        doct_Id as doctor_id,
        reason,
        date(appointment_Date) as appointment_date,
        payment_amount,
        mode_of_payment,
        mode_of_appointment,
        appointment_status
    from
        {{ source('hospital', 'src_appointments') }}
    where
        true
        -- incremental filter
        {% if is_incremental() %}
            {%- call statement('latest', fetch_result=True) -%}
                select max(appointment_date) from {{ this }}
            {%- endcall -%}
            {%- set latest = load_result('latest') -%}
            and date(appointment_date) >= date('{{ latest["data"][0][0] }}')
        {% endif %}
)
select
    *
from
    appointments