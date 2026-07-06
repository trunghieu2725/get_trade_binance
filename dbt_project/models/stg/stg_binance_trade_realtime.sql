{{
    config(
        materialized = 'view'
    )
}}

WITH
  payload AS (
    SELECT payload, ingestion_time, trade_time  
    FROM {{ source('binance_raw', 'raw_binance_trade_realtime_persist') }}
    WHERE 
      trade_time >= now() - INTERVAL 25 HOUR
  ),

  payload_fixed AS (
    SELECT
      -- normalize payload into a valid JSON text in `data_json`
      CASE
        WHEN startsWith(payload, '{') THEN payload
        WHEN startsWith(payload, '\"{') AND endsWith(payload, '}\"')
          THEN replaceRegexpAll(substring(payload, 2, length(payload) - 2), '\\\\"', '\"')
        ELSE payload
      END AS data_json,
      ingestion_time,trade_time
    FROM payload
  )
SELECT
    JSONExtractString(data_json, 't') AS trade_id
    ,toFloat64(
    nullIf(
      replaceRegexpAll(coalesce(JSONExtractString(data_json,'p'), ''), '[^0-9eE+\-\.]', ''),
      '')) AS price
    ,toFloat64(
    nullIf(
      replaceRegexpAll(coalesce(JSONExtractString(data_json,'q'), ''), '[^0-9eE+\-\.]', ''),
      '')) AS qty
    ,toTimeZone(trade_time, 'UTC') as trade_time_us
    ,toTimeZone(trade_time, 'Asia/Ho_Chi_Minh') AS trade_time_vn
    ,JSONExtractBool(data_json, 'm') AS is_buyer_maker
    ,JSONExtractBool(data_json, 'M') AS is_best_match
    ,JSONExtractString(data_json, 's') AS symbol
    ,ingestion_time 
FROM payload_fixed
