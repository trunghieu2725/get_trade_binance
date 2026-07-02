{% macro create_mv_raw_binance_trade_realtime_to_persist_kafka_view() %}
CREATE MATERIALIZED VIEW IF NOT EXISTS raw.mv_raw_binance_trade_realtime_to_persist TO raw.raw_binance_trade_realtime_persist
(
    payload String,
    ingestion_time DateTime64(6, 'Asia/Ho_Chi_Minh')
)
AS SELECT
    payload,
    toTimeZone(now64(6), 'Asia/Ho_Chi_Minh') AS ingestion_time
FROM raw.raw_binance_trade_realtime
{% endmacro %}
