SELECT
    count(*) AS failed_rows,
    groupArray(
        tuple(
            symbol,
            trade_id,
            price
        )
    ) AS failed_records
FROM
(
    SELECT
        symbol,
        trade_id,
        price
    FROM stg.stg_binance_trade_realtime
    WHERE price <= 0
    LIMIT 100
)