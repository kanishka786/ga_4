{{ config(
    materialized='table'
) }}


WITH locations AS (
  SELECT
    geo_continent AS continent_code,
    geo_country AS country_code,
    geo_region AS region_name,
    geo_city AS city_name,
    COUNT(1) AS total_events  -- Count of events by location
  FROM {{ ref('stg__flattened_ga4_events') }}
  GROUP BY geo_continent, geo_country, geo_region, geo_city
)

SELECT
  continent_code,
  country_code,
  region_name,
  city_name
FROM locations