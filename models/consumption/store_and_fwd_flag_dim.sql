with t1 as (
    select store_and_fwd_flag, max(load_timestamp) as load_timestamp from {{ref("green_taxi")}} group by store_and_fwd_flag
    union
    select store_and_fwd_flag, max(load_timestamp) as load_timestamp from {{ref("yellow_taxi")}} group by store_and_fwd_flag
),
t2 as(
    select 
        store_and_fwd_flag,
        case 
            when store_and_fwd_flag=true then 'store and forward trip'
            when store_and_fwd_flag=false then 'not a store and forward trip'
            else 'Not Available'
        end as snf_desc,
        max(load_timestamp) as load_timestamp 
    from t1 
    where store_and_fwd_flag not in (select store_and_fwd_flag from {{this}} group by store_and_fwd_flag) 
    group by store_and_fwd_flag 
    order by store_and_fwd_flag
)
select 
    nyc_taxi.consumption.snf_sk_seq.nextval as snf_sk, 
    store_and_fwd_flag,
    snf_desc,
    load_timestamp, 
    to_timestamp_ntz(current_timestamp) as update_timestamp
from t2

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}