SELECT
    dateDiff(
        'second',
        max(ingestion_time),
        now('UTC')
    ) AS delay_seconds
FROM raw.raw_binance_trade_realtime_persist
HAVING delay_seconds > 60