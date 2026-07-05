{% macro create_raw_binance_trade_realtime_persist_kafka_table() %}

CREATE TABLE IF NOT EXISTS raw.raw_binance_trade_realtime_persist
(
    payload String
    ,ingestion_time DateTime64(6, 'UTC')
    ,trade_time DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY trade_time 
PARTITION BY toDate(trade_time)
SETTINGS index_granularity = 8192
{% endmacro %}
