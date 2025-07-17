{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'appointment_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        unique_key='appointment_id',
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
        {{ source('hospital', 'stg_appointments') }}
    where
        true
        -- incremental filter and backfill
        {% if is_incremental() -%}
            {%- set backfill = var("backfill", "") %}
            {%- set date_start = var("date_start", "") %}
            {%- set date_until = var("date_until", "") %}
            {% if backfill %}
                and date(appointment_date) >= '{{ date_start }}'
                and date(appointment_date) <= '{{ date_until }}'
            {% else -%}
                {% if not latest -%}
                    {%- call statement('latest', fetch_result=True) -%}
                        select max(appointment_date) from {{ this }}
                    {%- endcall -%}
                {% endif %}
                {%- set latest = load_result('latest') -%}
                and  date(appointment_date) >= date('{{ latest["data"][0][0] }}')
            {% endif %}
        {% endif %}
)
select
    *
from
    appointments