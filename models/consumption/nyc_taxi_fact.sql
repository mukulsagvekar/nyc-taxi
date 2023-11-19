with t as (
    select vendor_id, 'Green' as taxi_type, pickup_datetime, dropoff_datetime, store_and_fwd_flag, rate_code_id, 
    pickup_location_id, dropoff_location_id, passenger_count, trip_distance, fare_amount, extra, mta_tax, trip_amount, 
    tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge, null as airport_fee, 
    load_timestamp from {{ref("green_taxi")}}
    union
    select vendor_id, 'Yellow' as taxi_type, pickup_datetime, dropoff_datetime, store_and_fwd_flag, rate_code_id, 
    pickup_location_id, dropoff_location_id, passenger_count, trip_distance, fare_amount, extra, mta_tax, trip_amount, 
    tolls_amount, null as ehail_fee, improvement_surcharge, total_amount, payment_type, null as trip_type, 
    congestion_surcharge, airport_fee, load_timestamp from {{ref("yellow_taxi")}}
),
fact_tbl as (
    select 
        v.vendor_sk, d1.datetime_sk as pickup_datetime_sk, d2.datetime_sk as dropoff_datetime_sk, s.snf_sk, r.rate_code_sk, 
        l1.location_id as pickup_location_sk, l1.location_id as dropoff_location_sk, ps.passengers_sk, py.payment_sk, 
        tt.trip_type_sk, trip_distance, fare_amount, extra, mta_tax, trip_amount, tolls_amount, ehail_fee, improvement_surcharge, 
        total_amount, congestion_surcharge, airport_fee, t.load_timestamp, current_timestamp as update_timestamp
    from 
        t
    inner join 
        {{ref("datetime_dim")}} d1 on t.pickup_datetime=d1.datetime
    inner join 
        {{ref("datetime_dim")}} d2 on t.dropoff_datetime=d2.datetime
    inner join 
        {{ref("vendor_dim")}} v on t.vendor_id=v.vendor_id and lower(t.taxi_type)=lower(v.taxi_type)
    inner join
        {{ref("store_and_fwd_flag_dim")}} s on t.store_and_fwd_flag=s.store_and_fwd_flag
    inner join
        {{ref("rate_code_dim")}} r on t.rate_code_id=r.rate_code_id
    inner join
        {{ref("location_dim")}} l1 on t.pickup_location_id=l1.location_id
    inner join
        {{ref("location_dim")}} l2 on t.dropoff_location_id=l2.location_id
    inner join
        {{ref("passenger_dim")}} ps on t.passenger_count=ps.passenger_count
    inner join
        {{ref("payment_dim")}} py on t.payment_type=py.payment_code
    inner join
        {{ref("trip_type_dim")}} tt on equal_null(t.trip_type,tt.trip_type_code)
)
select * from fact_tbl

{% if is_incremental() %}
  WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{this}})
{% endif %}