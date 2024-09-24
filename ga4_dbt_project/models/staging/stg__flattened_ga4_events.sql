WITH params AS (
  -- Filter the event_params only for the keys we need
  SELECT
    events.event_date,
    events.event_timestamp,
    events.user_pseudo_id,

    -- Extract ga_session_id and ga_session_number directly from params
    MAX(CASE WHEN params.key = "ga_session_id" THEN params.value.int_value ELSE NULL END) AS ga_session_id,
    MAX(CASE WHEN params.key = "ga_session_number" THEN params.value.int_value ELSE NULL END) AS ga_session_number
  FROM {{ ref('stg__ga4_events') }} AS events
  LEFT JOIN UNNEST(events.event_params) AS params
  WHERE params.key IN ('ga_session_id', 'ga_session_number')  -- Filter for relevant keys
  GROUP BY events.event_date, events.event_timestamp, events.user_pseudo_id
),

events AS (
  -- Join the parameters table to extract ga_session_id and ga_session_number
  SELECT
    PARSE_DATE('%Y%m%d', events.event_date) AS event_date,
    TIMESTAMP_MICROS(events.event_timestamp) AS event_ts,

    -- Join from params CTE to avoid repeated partitioning
    p.ga_session_id,
    p.ga_session_number,

    events.event_bundle_sequence_id,
    events.user_pseudo_id,
    events.user_id,
    TIMESTAMP_MICROS(events.user_first_touch_timestamp) AS user_first_touch_ts,
    events.event_name,
    events.event_previous_timestamp,
    events.event_value_in_usd,
    events.event_server_timestamp_offset,

    -- Directly unnest params for other fields as needed
    params.key AS params_key,
    COALESCE(params.value.string_value, CAST(params.value.int_value AS STRING),
             CAST(params.value.float_value AS STRING), CAST(params.value.double_value AS STRING)) AS event_param_value,

    -- Privacy info struct
    events.privacy_info.analytics_storage AS privacy_analytics_storage,
    events.privacy_info.ads_storage AS privacy_ads_storage,
    events.privacy_info.uses_transient_token AS privacy_uses_transient_token,

    -- User LTV struct
    events.user_ltv.revenue AS user_ltv_revenue,
    events.user_ltv.currency AS user_ltv_currency,

    -- Device struct
    events.device.category AS device_category,
    events.device.mobile_brand_name AS device_mobile_brand,
    events.device.mobile_model_name AS device_mobile_model,
    events.device.mobile_marketing_name AS device_mobile_marketing,
    events.device.mobile_os_hardware_model AS device_mobile_os_hardware_model,
    events.device.operating_system AS device_operating_system,
    events.device.operating_system_version AS device_os_version,
    events.device.vendor_id AS device_vendor_id,
    events.device.advertising_id AS device_advertising_id,
    events.device.language AS device_language,
    events.device.is_limited_ad_tracking AS device_limited_ad_tracking,
    events.device.time_zone_offset_seconds AS device_time_zone_offset_seconds,
    events.device.web_info.browser AS device_web_browser,
    events.device.web_info.browser_version AS device_web_browser_version,

    -- Geo struct
    events.geo.continent AS geo_continent,
    events.geo.sub_continent AS geo_sub_continent,
    events.geo.country AS geo_country,
    events.geo.region AS geo_region,
    events.geo.city AS geo_city,
    events.geo.metro AS geo_metro,

    -- App Info struct
    events.app_info.id AS app_id,
    events.app_info.version AS app_version,
    events.app_info.install_store AS app_install_store,
    events.app_info.firebase_app_id AS app_firebase_id,
    events.app_info.install_source AS app_install_source,

    -- Traffic Source struct
    events.traffic_source.medium AS traffic_medium,
    events.traffic_source.name AS traffic_name,
    events.traffic_source.source AS traffic_channel,

    -- Other fields
    events.stream_id AS stream_id,
    events.platform AS platform
  FROM {{ ref('stg__ga4_events') }} AS events
  LEFT JOIN UNNEST(events.event_params) AS params
  LEFT JOIN params p ON p.event_timestamp = events.event_timestamp
    AND p.user_pseudo_id = events.user_pseudo_id
)

SELECT *
FROM events
