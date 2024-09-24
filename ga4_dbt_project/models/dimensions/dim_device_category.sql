{{ config(
    materialized='table'
) }}

WITH device_categories AS (
  SELECT
    device_category,
    COUNT(1) AS total_events  -- Count of events by device category
  FROM {{ ref('stg__flattened_ga4_events') }}
  GROUP BY device_category
)

SELECT
  device_category,
  CASE
    WHEN device_category = 'mobile' THEN 'Mobile Phone'
    WHEN device_category = 'tablet' THEN 'Tablet'
    WHEN device_category = 'desktop' THEN 'Desktop Computer'
    ELSE 'Other'
  END AS device_description
FROM device_categories
