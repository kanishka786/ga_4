{{ config(
    materialized='table'
) }}

WITH new_user_data AS (
    -- Filter events for new users (first-time visitors)
    SELECT
        device_category,  -- Device category
        geo_country,  -- Country
        geo_region,  -- Region
        geo_city,  -- City
        params_key AS traffic_source,  -- Traffic source
        DATE(event_ts) AS event_date,   -- Date of the new user session
        COUNT(DISTINCT user_pseudo_id) AS new_users_ct

    FROM {{ ref('stg__flattened_ga4_events') }}  -- Source from staging model
    WHERE user_first_touch_ts = event_ts
    GROUP BY
        event_date,
        device_category,
        geo_country,
        geo_region,
        geo_city,
        traffic_source
)


SELECT *
FROM new_user_data
