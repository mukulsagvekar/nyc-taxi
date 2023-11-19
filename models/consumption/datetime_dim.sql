with t1 as (
    select pickup_datetime as datetime, max(load_timestamp) as load_timestamp from {{ref('green_taxi')}} group by datetime 
    union
    select dropoff_datetime as datetime, max(load_timestamp) as load_timestamp from {{ref('green_taxi')}} group by datetime
    union
    select pickup_datetime as datetime, max(load_timestamp) load_timestamp from {{ref("yellow_taxi")}} group by datetime
    union
    select dropoff_datetime as datetime, max(load_timestamp) load_timestamp from {{ref("yellow_taxi")}} group by datetime
),
t2 as(
    select 
        datetime,
        max(load_timestamp) as load_timestamp 
    from t1 
    where datetime not in (select datetime from {{this}} group by datetime) 
    group by datetime 
    order by datetime
)
select 
    nyc_taxi.consumption.datetime_sk_seq.nextval as datetime_sk, 
    datetime,
    year(datetime) as year,
    quarter(datetime) as quarter,
    month(datetime) as month,
    day(datetime) as day_of_month,
    dayofweek(datetime) as day_of_week,
    hour(datetime) as hour,
    minute(datetime) as minute,
    second(datetime) as second,
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}