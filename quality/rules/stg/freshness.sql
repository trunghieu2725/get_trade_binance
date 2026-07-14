SELECT *
FROM stg.stg_binance_trade_realtime
WHERE 
    now() - trade_time > 60