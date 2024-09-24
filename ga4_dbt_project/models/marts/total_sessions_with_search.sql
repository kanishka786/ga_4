{{ config(
    materialized='table'
) }}

WITH sessions_with_search AS (
    -- Identify sessions where a search event occurred
    SELECT
        device_category,          -- Category of the device used for the session
        geo_country,              -- Country where the session originated
        geo_region,               -- Region where the session originated
        geo_city,                 -- City where the session originated
        params_key AS traffic_source,  -- Traffic source (campaign, source, medium)
        DATE(event_ts) AS event_date,  -- Date of the session
        ga_session_id,            -- Unique session identifier
        COUNTIF(event_name = 'view_search_results') > 0 AS has_search_event  -- Identify if the session includes a search event
    FROM {{ ref('stg__flattened_ga4_events') }}  -- Reference your staging model for GA4 events
    WHERE ga_session_id IS NOT NULL  -- Ensure we are counting valid sessions
    GROUP BY
        event_date,
        device_category,
        geo_country,
        geo_region,
        geo_city,
        traffic_source,
        ga_session_id
)

-- Final aggregation: Count sessions with search events
SELECT
    event_date,
    device_category,
    geo_country,
    geo_region,
    geo_city,
    traffic_source,
    COUNT(DISTINCT ga_session_id) AS total_sessions_with_search_ct
FROM sessions_with_search
WHERE has_search_event = TRUE  -- Only include sessions where a search event occurred
GROUP BY
    event_date,
    device_category,
    geo_country,
    geo_region,
    geo_city,
    traffic_source