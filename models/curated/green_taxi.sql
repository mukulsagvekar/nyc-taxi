with t1 as(
    
    select
        data:VendorID::number as vendor_id,
        data:lpep_pickup_datetime::varchar::timestamp_ntz as pickup_datetime,
        data:lpep_dropoff_datetime::varchar::timestamp_ntz as dropoff_datetime,
        data:store_and_fwd_flag::boolean as store_and_fwd_flag,
        data:RatecodeID::number as rate_code_id,
        data:PULocationID::number as pickup_location_id,
        data:DOLocationID::number as dropoff_location_id,
        data:passenger_count::number as passenger_count,
        data:trip_distance::number(20,2) as trip_distance,
        data:fare_amount::number(20,2) as fare_amount,
        data:extra::number(20,2) as extra,
        data:mta_tax::number(20,2) as mta_tax,
        data:tip_amount::number(20,2) as trip_amount,
        data:tolls_amount::number(20,2) as tolls_amount,
        data:ehail_fee::varchar as ehail_fee,
        data:improvement_surcharge::number(20,2) as improvement_surcharge,
        data:total_amount::number(20,2) as total_amount,
        data:payment_type::number as payment_type,
        data:trip_type::number as trip_type,
        data:congestion_surcharge::number(20,2) as congestion_surcharge,
        load_timestamp,
        current_timestamp as dbt_update_timestamp
    from {{ source("raw","stg_green_taxi") }}

)

select * from t1

{% if is_incremental() %}
  -- If this is an incremental model, include the primary key in the unique key
  WHERE load_timestamp > (SELECT MAX(dbt_update_timestamp) FROM {{ this }})
{% endif %}