Drop table IF EXISTS raw.binance_trade_batchjob;
CREATE TABLE IF NOT EXISTS raw.binance_trade_batchjob (
      trade_id UInt64,
      price Float64,
      qty Float64,
      quote_qty Float64,
      trade_time bigint,
      is_buyer_maker UInt8,
      is_best_match UInt8,
      symbol String,
      ingestion_time String
    ) ENGINE = MergeTree()
    ORDER BY (trade_time, symbol)

