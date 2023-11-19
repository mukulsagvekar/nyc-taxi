with t1 as (

    select 
        data:VendorID::number as vendor_id,
        data:tpep_pickup_datetime::varchar::timestamp_ntz as pickup_datetime,
        data:tpep_dropoff_datetime::varchar::timestamp_ntz as dropoff_datetime,
        data:passenger_count::number as passenger_count,
        data:trip_distance::number(20,2) as trip_distance,
        data:RatecodeID::number as rate_code_id,
        data:store_and_fwd_flag::boolean as store_and_fwd_flag,
        data:PULocationID::number as pickup_location_id,
        data:DOLocationID::number as dropoff_location_id,
        data:payment_type::number as payment_type,
        data:fare_amount::number(20,2) as fare_amount,
        data:extra::number(20,2) as extra,
        data:mta_tax::number(20,2) as mta_tax,
        data:tip_amount::number(20,2) as trip_amount,
        data:tolls_amount::number(20,2) as tolls_amount,
        data:improvement_surcharge::number(20,2) as improvement_surcharge,
        data:total_amount::number(20,2) as total_amount,
        data:congestion_surcharge::number(20,2) as congestion_surcharge,
        data:airport_fee::number(20,2) as airport_fee,
        load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from {{ source("raw","stg_yellow_taxi") }}
    where total_amount > 0 and rate_code_id is not null and rate_code_id != 99 and payment_type != 0 
    and payment_type is not null and passenger_count != 0 and passenger_count is not null

)

select * from t1 

{% if is_incremental() %}
  -- If this is an incremental model, include the primary key in the unique key
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
{% endif %}