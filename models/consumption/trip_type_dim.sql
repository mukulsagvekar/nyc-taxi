with t1 as (
    select trip_type as trip_type_code, max(load_timestamp) as load_timestamp from {{ref('green_taxi')}} group by trip_type_code
    union
    select null as trip_type_code, max(load_timestamp) as load_timestamp from {{ref("yellow_taxi")}} group by trip_type_code
),
t2 as(
    select 
        trip_type_code,
        case 
            when trip_type_code=1 then 'Street-hail'
            when trip_type_code=2 then 'Dispatch'
            else 'Not Available'
        end as trip_type,
        max(load_timestamp) as load_timestamp 
    from t1 
    where trip_type_code not in (select trip_type_code from {{this}} group by trip_type_code) 
    group by trip_type_code 
    order by trip_type_code
)
select 
    nyc_taxi.consumption.trip_type_sk_seq.nextval as trip_type_sk, 
    trip_type_code,
    trip_type,
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}