{{
    config(
        materialized = 'incremental',
        alias='fact_leads_funnel',
		unique_key='room_id',
        schema = 'analytics',
    )
}}

    with valid_rooms as (
    select distinct 
        (raw.message_data->>'room_id') as room_id
    from raw_data.chat_logs raw
    join pth8tkl5puks7pg.keyword_leads noco 
        on (raw.message_data->>'message_text') ilike '%' || noco.keyword || '%'
),

room_metrics as (
    select 
        (message_data->>'room_id') as room_id,
        min((message_data->>'timestamp')::timestamp) as leads_date,
        max(message_data->'metadata'->>'channel') as channel,
        min(message_data->>'sender_phone') as phone_number,
        min(case 
            when message_data->'metadata'->'booking_info'->>'booking_id' is not null 
            then (message_data->>'timestamp')::timestamp 
        end) as first_booking_date,
        max(case 
            when message_data->'metadata'->'booking_info'->>'booking_id' is not null 
            then (message_data->>'timestamp')::timestamp 
        end) as last_booking_date,
        min(case 
            when message_data->'metadata'->'payment_info'->>'status' in ('PAID', 'SUCCESS') 
            then (message_data->>'timestamp')::timestamp 
        end) as first_transaction_date,
        max(case 
            when message_data->'metadata'->'payment_info'->>'status' in ('PAID', 'SUCCESS') 
            then (message_data->>'timestamp')::timestamp 
        end) as last_transaction_date,
        count(distinct case 
            when message_data->'metadata'->'booking_info'->>'booking_id' is not null and 
            	message_data->'metadata'->'booking_info'->>'status' = 'confirmed'
            then message_data->'metadata'->'booking_info'->>'booking_id'
        end) total_booked,
        sum(coalesce((message_data->'metadata'->'payment_info'->>'amount')::decimal, 0)) as transaction_value
    from raw_data.chat_logs
    
    where (message_data->>'room_id') in (select room_id from valid_rooms)
    group by 1
)

select 
    room_id,
    leads_date,
    channel,
    phone_number,
    first_booking_date,
    last_booking_date,
    total_booked,
    first_transaction_date,
    last_transaction_date,
    transaction_value
from room_metrics



