with t1 as (
    select payment_type as payment_code, max(load_timestamp) as load_timestamp from {{ref("green_taxi")}} group by payment_code
    union
    select payment_type as payment_code, max(load_timestamp) as load_timestamp from {{ref("yellow_taxi")}} group by payment_code
),
t2 as(
    select 
        payment_code,
        case 
            when payment_code=1 then 'Credit card'
            when payment_code=2 then 'Cash'
            when payment_code=3 then 'No charge'
            when payment_code=4 then 'Dispute'
            when payment_code=5 then 'Unknown'
            when payment_code=6 then 'Voided trip'
            else 'Not Available'
        end as payment_type,
        max(load_timestamp) as load_timestamp 
    from t1 
    where payment_code not in (select payment_code from {{this}} group by payment_code) 
    group by payment_code 
    order by payment_code
)
select 
    nyc_taxi.consumption.payment_sk_seq.nextval as payment_sk, 
    payment_code,
    payment_type,
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}