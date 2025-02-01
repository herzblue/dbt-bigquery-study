{{ config( materialized='table', cluster_by=['location_id'] ) }}

SELECT DISTINCT
    pickup_location_id AS location_id,
    -- 위치의 고유한 특성을 정의
    CASE 
        WHEN pickup_location_id IN ("1", "2") THEN 'airport'  -- 공항 위치 ID
        WHEN pickup_location_id BETWEEN "100" AND "200" THEN 'manhattan'
        ELSE 'outer_borough'
    END AS borough_type,
    case
        WHEN pickup_location_id IN ("1", "2") THEN 'restricted'
        ELSE 'general'
    END AS service_area_type
from {{ source('ny_taxi', 'tlc_green_trips_2022') }}
