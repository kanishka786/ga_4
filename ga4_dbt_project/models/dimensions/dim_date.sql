{{ config(
    materialized='table'
) }}


WITH calendar AS (
  SELECT
    CAST(event_date AS DATE) AS date,
    EXTRACT(YEAR FROM event_date) AS year,
    EXTRACT(MONTH FROM event_date) AS month,
    EXTRACT(DAY FROM event_date) AS day,
    EXTRACT(DAYOFWEEK FROM event_date) AS day_of_week,
    EXTRACT(QUARTER FROM event_date) AS quarter,
    EXTRACT(WEEK FROM event_date) AS week_of_year,
    FORMAT_DATE('%Y-%m', event_date) AS year_month
  FROM {{ ref('stg__flattened_ga4_events') }}  -- Source of event data
  GROUP BY event_date
)

SELECT * FROM calendar
