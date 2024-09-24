-- models/ga4_events.sql

{{ config(materialized='view') }}  -- Creates a view instead of a table

SELECT
  *
FROM `{{ var('public_project') }}.{{ var('public_dataset') }}.events_*`
