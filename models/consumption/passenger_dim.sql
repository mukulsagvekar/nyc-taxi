with t1 as (
    select passenger_count, max(load_timestamp) as load_timestamp from {{ref("green_taxi")}} group by passenger_count
    union
    select passenger_count, max(load_timestamp) as load_timestamp from {{ref("yellow_taxi")}} group by passenger_count
),
t2 as(
    select 
        passenger_count, 
        max(load_timestamp) as load_timestamp 
    from t1 
    where passenger_count not in (select passenger_count from {{this}} group by passenger_count) 
    group by passenger_count 
    order by passenger_count
)
select 
    nyc_taxi.consumption.passengers_sk_seq.nextval as passengers_sk, 
    passenger_count, 
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}