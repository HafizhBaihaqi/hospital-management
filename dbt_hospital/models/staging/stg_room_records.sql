{{
    config(
        materialized='incremental',
        partition_by={
            'field': 'admission_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        unique_key='admission_id',
        incremental_strategy = 'insert_overwrite'
    )
}}

with room_records as (
    select
        admisson_ID as admission_id,
        room_no as room_id,
        patient_Id as patient_id,
        nurse_Id as nurse_id,
        helper_Id as helper_id,
        date(admission_Date) as admission_date,
        date(discharge_Date) as discharge_date,
        amount,
        mode_of_payment
    from
        {{ source('hospital', 'src_room_records') }}
    where
        true
        -- incremental filter and backfill
        {% if is_incremental() -%}
            {%- set backfill = var("backfill", "") %}
            {%- set date_start = var("date_start", "") %}
            {%- set date_until = var("date_until", "") %}
            {% if backfill %}
                and date(admission_date) >= '{{ date_start }}'
                and date(admission_date) <= '{{ date_until }}'
            {% else -%}
                {% if not latest -%}
                    {%- call statement('latest', fetch_result=True) -%}
                        select max(admission_date) from {{ this }}
                    {%- endcall -%}
                {% endif %}
                {%- set latest = load_result('latest') -%}
                and  date(admission_date) >= date('{{ latest["data"][0][0] }}')
            {% endif %}
        {% endif %}
)
select
    *
from
    room_records