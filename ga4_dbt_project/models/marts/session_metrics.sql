{{ config(
    materialized='incremental',
    unique_key='ga_session_id'
) }}

WITH base_data AS (
    -- Basic event data for relevant sessions
    SELECT
        ga_session_id,
        ga_session_number,
        device_category,
        geo_country,
        geo_region,
        geo_city,
        user_pseudo_id,
        event_name,
        event_value_in_usd,
        event_ts,
        params_key
    FROM {{ ref('stg__flattened_ga4_events') }}
    WHERE ga_session_id IS NOT NULL
        AND params_key IN ('campaign', 'source', 'medium')

    -- Incremental logic to only select new data
    {% if is_incremental() %}
        AND event_ts > (SELECT MAX(session_end) FROM {{ this }})
    {% endif %}
),
session_data AS (
    -- Aggregate session-level metrics
    SELECT
        ga_session_id,
        ga_session_number,
        device_category,
        geo_country,
        geo_region,
        geo_city,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        COUNT(*) AS total_events,
        COUNTIF(event_name = 'purchase') AS total_purchases,
        COALESCE(SUM(event_value_in_usd), 0) AS total_revenue,
        MIN(event_ts) AS session_start,
        MAX(event_ts) AS session_end,
        TIMESTAMP_DIFF(MAX(event_ts), MIN(event_ts), SECOND) AS session_duration_seconds
    FROM base_data
    GROUP BY
        ga_session_id,
        ga_session_number,
        device_category,
        geo_country,
        geo_region,
        geo_city
),
invalid_sessions AS (
    -- Identify sessions with multiple devices
    SELECT
        ga_session_id
    FROM session_data
    GROUP BY ga_session_id
    HAVING COUNT(*) > 1  -- Multiple devices or duplicate records in the same session
)

-- Final session-level metrics excluding invalid sessions
SELECT
    ga_session_id,
    ga_session_number,
    device_category,
    geo_country,
    geo_region,
    geo_city,
    total_users,
    total_events,
    total_purchases,
    total_revenue,
    session_start,
    session_end,
    session_duration_seconds,
    DATE(session_start) AS date
FROM session_data
WHERE ga_session_id NOT IN (SELECT ga_session_id FROM invalid_sessions)
