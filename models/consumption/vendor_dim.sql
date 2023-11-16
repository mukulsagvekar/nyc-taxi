with t as(
    select vendor_id, 'Green' as taxi_type from {{ ref("green_taxi") }} group by vendor_id
    union
    select vendor_id, 'Yellow' from {{ ref("yellow_taxi") }} group by vendor_id
)
select nyc_taxi.consumption.vendor_sk_seq.nextval as vendor_sk, 
    vendor_id, 
    case when vendor_id=1 then 'Creative Mobile Technologies' 
         when vendor_id=2 then 'VeriFone Inc.'
    end as vendor_name,
    taxi_type 
from t
{% if is_incremental() %}
  -- If this is an incremental model, include the primary key in the unique key
  WHERE vendor_sk > (SELECT MAX(vendor_sk) FROM {{ this }}) OR vendor_sk IS NULL
{% endif %}