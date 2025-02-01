{{
    config(
        materialized='incremental',
        partition_by={
            "field": "data_file_month",
            "data_type": "int64",
            "granularity": "month",
            "range": {
              "start": 1,
              "end": 12,
              "interval": 1
            }
        },
        cluster_by="location_id,borough_type"
    )
}}

select
    f.data_file_year,
    f.data_file_month,
    date_trunc(f.pickup_datetime, day) as pickup_date, 
    l.location_id,
    l.borough_type,
    l.service_area_type,
    count(distinct f.trip_id) as total_trips,  -- 고유 trip_id 개수
    avg(f.fare_amount) as avg_fare,
    avg(case when f.rate_code = '2.0' then 1 else 0 end) as airport_trip_ratio
from {{ ref('fact_trips') }} f
join {{ ref('dim_locations') }} l
    on f.pickup_location_id = l.location_id
{% if is_incremental() %}
  where f.data_file_month >= (select max(data_file_month) from {{ this }})
{% endif %}
group by
    f.data_file_year,
    f.data_file_month,
    pickup_date,
    l.location_id,
    l.borough_type,
    l.service_area_type
