{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'visit_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        unique_key='record_id',
        incremental_strategy = 'insert_overwrite'
    )
}}

with medical_records as (
    select
        record_Id as record_id,
        doct_Id as doctor_id,
        patient_Id as patient_id,
        date(visit_Date) as visit_date,
        curr_Weight as curr_weight,
        curr_height,
        curr_Blood_Pressure as curr_blood_pressure,
        curr_Temp_F as current_temp_f,
        diagnosis,
        treatment,
        date(next_Visit) as next_visit
    from
        {{ source('hospital', 'src_medical_records') }}
    where
        true
        -- incremental filter and backfill
        {% if is_incremental() -%}
            {%- set backfill = var("backfill", "") %}
            {%- set date_start = var("date_start", "") %}
            {%- set date_until = var("date_until", "") %}
            {% if backfill %}
                and date(visit_date) >= '{{ date_start }}'
                and date(visit_date) <= '{{ date_until }}'
            {% else -%}
                {% if not latest -%}
                    {%- call statement('latest', fetch_result=True) -%}
                        select max(visit_date) from {{ this }}
                    {%- endcall -%}
                {% endif %}
                {%- set latest = load_result('latest') -%}
                and  date(visit_date) >= date('{{ latest["data"][0][0] }}')
            {% endif %}
        {% endif %}
)
select
    *
from
    medical_records