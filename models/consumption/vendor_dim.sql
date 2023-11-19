with t1 as(
    select vendor_id, 'Green' as taxi_type,max(load_timestamp) as load_timestamp from {{ref("green_taxi")}} group by vendor_id
    union
    select vendor_id, 'Yellow' as taxi_type,max(load_timestamp) as load_timestamp from {{ref("yellow_taxi")}} group by vendor_id
),
t2 as(
    select  
        vendor_id, 
        case 
            when vendor_id=1 then 'Creative Mobile Technologies' 
            when vendor_id=2 then 'VeriFone Inc.'
            else 'Not Available'
        end as vendor_name,
        taxi_type,
        max(load_timestamp) as load_timestamp
    from t1
    where vendor_id not in (select vendor_id from {{this}} group by vendor_id)
    group by vendor_id, vendor_name, taxi_type
)
select 
    nyc_taxi.consumption.vendor_sk_seq.nextval as vendor_sk,
    vendor_id,
    vendor_name,
    taxi_type,
    load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}