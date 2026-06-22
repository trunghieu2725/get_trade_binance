{{
    config(
        materialized = 'view'
    )
}}

WITH
  payload AS (
    SELECT payload, ingestion_time
    FROM {{ source('binance_raw', 'raw_binance_trade_realtime_persist') }}
    WHERE toDate(
    toTimeZone(
      toDateTime64(
        coalesce(
          nullIf(JSONExtractUInt(payload,'trade_time_us')/1000000.0,0),
          nullIf(JSONExtractUInt(payload,'T')/1000.0,0)
        ),
        6,'UTC'
      ),
      'Asia/Ho_Chi_Minh'
    )
  ) = today()
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
      ingestion_time
    FROM payload
  )
SELECT
  coalesce(JSONExtractUInt(data_json, 't'), JSONExtractUInt(data_json, 'trade_id')) AS trade_id,

  toFloat64(
    nullIf(
      replaceRegexpAll(coalesce(JSONExtractString(data_json,'p'), JSONExtractString(data_json,'price'), ''), '[^0-9eE+\-\.]', ''),
      ''
    )
  ) AS price,

  toFloat64(
    nullIf(
      replaceRegexpAll(coalesce(JSONExtractString(data_json,'q'), JSONExtractString(data_json,'qty'), ''), '[^0-9eE+\-\.]', ''),
      ''
    )
  ) AS qty,

toTimeZone(
    toDateTime64(
      coalesce(
        nullIf(JSONExtractUInt(data_json, 'trade_time_us') / 1000000.0, 0),
        nullIf(JSONExtractUInt(data_json, 'T') / 1000.0, 0)
      ),
      6,
      'UTC'
    ),
    'Asia/Ho_Chi_Minh'
  ) AS trade_time,
  coalesce(JSONExtractInt(data_json, 'm'), JSONExtractInt(data_json, 'is_buyer_maker')) AS is_buyer_maker,
  coalesce(JSONExtractInt(data_json, 'M'), JSONExtractInt(data_json, 'is_best_match')) AS is_best_match,
  coalesce(JSONExtractString(data_json, 's'), JSONExtractString(data_json, 'symbol')) AS symbol,
  ingestion_time
FROM payload_fixed
WHERE length(data_json) > 0