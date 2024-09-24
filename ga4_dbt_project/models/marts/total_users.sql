{{ config(
    materialized='table'
) }}

WITH user_data AS (
    -- Aggregate user-level metrics
    SELECT
        date(event_date) AS event_date,  -- Date of session start
        device_category,  -- Device category
        geo_country,  -- Country
        geo_region,  -- Region
        geo_city,  -- City
        params_key AS traffic_source,  -- Traffic source
        count(DISTINCT user_pseudo_id) AS total_user_ct
    FROM {{ ref('stg__flattened_ga4_events') }}  -- Source from staging model
    WHERE params_key IN ('campaign', 'source', 'medium')
    GROUP BY 1, 2, 3, 4, 5, 6  -- Group by date, device, country, region, city
)

-- Final selection of total users by traffic source
SELECT
    event_date,
    device_category,
    geo_country,
    geo_region,
    geo_city,
    traffic_source,
    total_user_ct
FROM user_data
