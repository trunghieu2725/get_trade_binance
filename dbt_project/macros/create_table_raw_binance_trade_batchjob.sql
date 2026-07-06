{% macro create_raw_binance_trade_batchjob_table() %}

CREATE TABLE IF NOT EXISTS raw.raw_binance_trade_batchjob
(
    `trade_id` UInt64,
    `price` Float64,
    `qty` Float64,
    `quote_qty` Float64,
    `trade_time` Int64,
    `is_buyer_maker` UInt8,
    `is_best_match` UInt8,
    `symbol` String,
    `ingestion_time` String
)
ENGINE = MergeTree
ORDER BY (trade_time, symbol)
PARTITION BY date(toTimeZone(fromUnixTimestamp64Micro(trade_time),'UTC'))
SETTINGS index_granularity = 8192
{% endmacro %}