{{
    config(
        materialized= 'incremental',
        unique_key=['symbol','trade_id']
    )
}}

with source_data as (
    select
        cast(trade_id as UInt64) as trade_id,
        cast(symbol as String) as symbol,
        cast(price as Float64) as price,
        cast(qty as Float64) as quantity,
        cast(quote_qty as Float64) as quote_quantity,
        toInt64(trade_time) as trade_time_us,
        cast(ingestion_time as DateTime) as ingestion_time,
        cast(is_buyer_maker as UInt8) as is_buyer_maker,
        cast(is_best_match as UInt8) as is_best_match
    from {{ source('binance_raw', 'raw_binance_trade_batchjob') }}
)

select
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    fromUnixTimestamp64Micro(trade_time_us) as trade_time,
    ingestion_time,
    is_buyer_maker,
    is_best_match
from source_data

{% if is_incremental() %}
  where fromUnixTimestamp64Micro(trade_time_us) > (select max(trade_time) from {{ this }})
{% endif %}
