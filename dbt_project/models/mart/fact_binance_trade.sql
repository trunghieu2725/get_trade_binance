{{
    config(
        materialized= 'view',
    )
}}


with total_trade as (
select
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    trade_time_us,
    trade_time_vn,
    is_buyer_maker,
    is_best_match,
    ingestion_time,
    ingestion_time_vn

from {{ ref('stg_binance_trade_batchjob') }}

union all

select
    trade_id,
    symbol,
    price,
    qty as quantity,
    price * qty as quote_quantity,
    trade_time_us,
    trade_time_vn,
    is_buyer_maker,
    is_best_match,
    ingestion_time,
    toTimeZone(ingestion_time, 'Asia/Ho_Chi_Minh') as ingestion_time_vn

from {{ ref('stg_binance_trade_realtime') }}
)

select
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    trade_time,
    trade_time_vn,
    is_buyer_maker,
    is_best_match,
    ingestion_time,
    ingestion_time_vn
from total_trade
