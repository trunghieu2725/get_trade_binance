
SELECT
    count(*) AS failed_rows
FROM raw.raw_binance_trade_realtime_persist
WHERE isValidJSON(payload) = 0