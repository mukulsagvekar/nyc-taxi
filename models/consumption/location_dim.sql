with t1 as (
    select pickup_location_id as location_id, max(load_timestamp) as load_timestamp from {{ref('green_taxi')}} group by location_id
    union
    select dropoff_location_id as location_id, max(load_timestamp) as load_timestamp from {{ref('green_taxi')}} group by location_id
    union
    select pickup_location_id as location_id, max(load_timestamp) load_timestamp from {{ref('yellow_taxi')}} group by location_id
    union
    select dropoff_location_id as location_id, max(load_timestamp) load_timestamp from {{ref('yellow_taxi')}} group by location_id
),
t2 as(
    select
        location_id,
        max(load_timestamp) as load_timestamp 
    from t1 
    where location_id not in (select location_id from {{this}} group by location_id) 
    group by location_id 
    order by location_id
)
select 
    nyc_taxi.consumption.location_sk_seq.nextval as location_sk, 
    location_id,
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}