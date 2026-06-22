{{
    config(
        materialized= 'incremental',
        unique_key= 'trade_id'
    )
}}

select
    cast(trade_id as UInt64) as trade_id,
    cast(symbol as String) as symbol,
    cast(price as Float64) as price,
    cast(qty as Float64) as quantity,
    cast(quote_qty as Float64) as quote_quantity,
    fromUnixTimestamp64Micro(trade_time) AS trade_time,
    cast(ingestion_time as DateTime) as ingestion_time,
    cast(is_buyer_maker as UInt8) as is_buyer_maker,
    cast(is_best_match as UInt8) as is_best_match
from {{ source('binance_raw', 'raw_binance_trade_batchjob') }}

{% if is_incremental() %}
  where fromUnixTimestamp64Micro(trade_time) > (select max(trade_time) from {{ this }})
{% endif %}