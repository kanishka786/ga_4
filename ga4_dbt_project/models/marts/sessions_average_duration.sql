{{ config(
    materialized='table'
) }}

WITH session_data AS (
    -- Calculate session duration for each session
    SELECT
        ga_session_id,                           -- Session ID
        DATE(event_ts) AS event_date,            -- Date of the session
        device_category,                         -- Device category used during the session
        geo_country,                             -- Country where the session originated
        geo_region,                              -- Region where the session originated
        geo_city,                                -- City where the session originated
        MIN(event_ts) AS session_start,          -- Session start timestamp
        MAX(event_ts) AS session_end,            -- Session end timestamp
        -- Calculate session duration in seconds
        TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds
    FROM {{ ref('stg__flattened_ga4_events') }}  -- Source from staging model for GA4 events
    WHERE ga_session_id IS NOT NULL  -- Ensure we are calculating valid sessions
    GROUP BY
        ga_session_id, event_date, device_category, geo_country, geo_region, geo_city
),

average_session_duration AS (
    -- Calculate the average session duration across all sessions
    SELECT
        event_date,
        device_category,
        geo_country,
        geo_region,
        geo_city,
        -- Calculate average session duration (total duration / total sessions)
        AVG(session_duration_seconds) AS avg_session_duration_seconds
    FROM session_data
    GROUP BY event_date, device_category, geo_country, geo_region, geo_city
)

-- Final selection of average session duration
SELECT *
FROM average_session_duration
