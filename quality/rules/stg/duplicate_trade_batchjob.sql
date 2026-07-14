SELECT
    count(*) AS failed_rows,
    groupArray(
        tuple(
            symbol,
            trade_id,
            cnt
        )
    ) AS failed_records
FROM
(
    SELECT
        symbol,
        trade_id,
        count(*) AS cnt
    FROM stg.stg_binance_trade_batchjob
    GROUP BY
        symbol,
        trade_id
    HAVING cnt > 1
    LIMIT 100
)