{{ config(
    materialized='incremental',
    partition_by={
        "field": "pickup_datetime",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=['pickup_location_id', 'dropoff_location_id']
) }}

select
    -- 운행 식별 정보
    
        concat(
        vendor_id, '_',
        cast(pickup_datetime as string), '_',
        cast(pickup_location_id as string)
    ) as trip_id,
    vendor_id,
    
    -- 시간 관련 측정값
    pickup_datetime,
    dropoff_datetime,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) as duration_minutes,    
    -- 위치 관련 측정값
    pickup_location_id,
    dropoff_location_id,
    trip_distance,
    
    -- 승객 정보
    passenger_count,
    trip_type,
    rate_code,
    store_and_fwd_flag,
    
    -- 요금 관련 측정값
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    airport_fee,
    imp_surcharge,
    total_amount,
    payment_type,
    
    -- 성과 지표 계산
    case 
        when fare_amount > 0 then tip_amount / fare_amount 
        else 0 
    end as tip_ratio,
    
    case 
        when TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) > 0 
        then total_amount / TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)
        else 0 
    end as revenue_per_minute,
    
    -- 파일 정보
    data_file_year,
    data_file_month

from {{ source('ny_taxi', 'tlc_green_trips_2022') }}

{% if is_incremental() %}
where pickup_datetime > (select max(pickup_datetime) from {{ this }})
{% endif %}
