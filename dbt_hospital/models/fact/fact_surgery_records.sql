{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'surgery_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        unique_key='surgery_id',
        incremental_strategy = 'insert_overwrite'
    )
}}

with surgery_records as (
    select
        surgery_Id as surgery_id,
        patient_Id as patient_id,
        surgeon_Id as surgeon_id,
        surgery_Type as surgery_type,
        date(surgery_Date) as surgery_date,
        start_Time as start_time,
        end_Time as end_time,
        room_no as room_id,
        notes,
        nurse_Id as nurse_id,
        helper_Id as helper_id
    from
        {{ source('hospital', 'stg_surgery_records') }}
    where
        true
        -- incremental filter and backfill
        {% if is_incremental() -%}
            {%- set backfill = var("backfill", "") %}
            {%- set date_start = var("date_start", "") %}
            {%- set date_until = var("date_until", "") %}
            {% if backfill %}
                and date(surgery_date) >= '{{ date_start }}'
                and date(surgery_date) <= '{{ date_until }}'
            {% else -%}
                {% if not latest -%}
                    {%- call statement('latest', fetch_result=True) -%}
                        select max(surgery_date) from {{ this }}
                    {%- endcall -%}
                {% endif %}
                {%- set latest = load_result('latest') -%}
                and  date(surgery_date) >= date('{{ latest["data"][0][0] }}')
            {% endif %}
        {% endif %}
)
select
    *
from
    surgery_records