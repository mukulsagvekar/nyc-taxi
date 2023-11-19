with t1 as (
    select rate_code_id, max(load_timestamp) as load_timestamp from {{ref("green_taxi")}} group by rate_code_id
    union
    select rate_code_id, max(load_timestamp) as load_timestamp from {{ref("yellow_taxi")}} group by rate_code_id
),
t2 as(
    select 
        rate_code_id,
        case 
            when rate_code_id=1 then 'Standard rate'
            when rate_code_id=2 then 'JFK'
            when rate_code_id=3 then 'Newark'
            when rate_code_id=4 then 'Nassau or Westchester'
            when rate_code_id=5 then 'Negotiated fare'
            when rate_code_id=6 then 'Group ride'
            else 'Not Available'
        end as rate_code,
        max(load_timestamp) as load_timestamp 
    from t1 
    where rate_code_id not in (select rate_code_id from {{this}} group by rate_code_id) 
    group by rate_code_id 
    order by rate_code_id
)
select 
    nyc_taxi.consumption.rate_code_sk_seq.nextval as rate_code_sk, 
    rate_code_id, 
    rate_code,
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}