{{ config(
    materialized='table'
) }}

WITH page_view_data AS (
    -- Filter events for page views
    SELECT
        device_category,  -- Category of the device used for the session
        geo_country,      -- Country of the session
        geo_region,       -- Region of the session
        geo_city,         -- City of the session
        params_key AS traffic_source,  -- Source of the traffic
        DATE(event_ts) AS event_date,  -- Date of the page view
        params_key, -- Traffic source
        COUNTIF(event_name = 'page_view') AS page_views_ct  -- Count total page views
    FROM {{ ref('stg__flattened_ga4_events') }}  -- Reference your staging model for GA4 events
    WHERE event_name = 'page_view'
    GROUP BY
        event_date,
        device_category,
        geo_country,
        geo_region,
        geo_city,
        traffic_source,
        params_key
)

-- Final selection of total page views by traffic source and other dimensions
SELECT
    event_date,
    device_category,
    geo_country,
    geo_region,
    geo_city,
    traffic_source,
    params_key,
    page_views_ct
FROM page_view_data
