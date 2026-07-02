select
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    trade_time,
    is_buyer_maker,
    is_best_match,
    ingestion_time
from {{ ref('stg_binance_trade_batchjob') }}

union all

select
    trade_id,
    symbol,
    price,
    qty as quantity,
    price * qty as quote_quantity,
    trade_time,
    is_buyer_maker,
    is_best_match,
    ingestion_time
from {{ ref('stg_binance_trade_realtime') }}
