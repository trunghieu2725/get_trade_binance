{{
    config(
        materialized= 'incremental',
        engine='MergeTree()',
        partition_by=['toDate(trade_time_us)'],
        order_by=['symbol', 'trade_time_us'],
        unique_key=['symbol','trade_id']
    )
}}

with source_data as (
    select
        cast(trade_id as String) as trade_id,
        cast(symbol as String) as symbol,
        cast(price as Float64) as price,
        cast(qty as Float64) as quantity,
        cast(quote_qty as Float64) as quote_quantity,
        toTimeZone(fromUnixTimestamp64Micro(trade_time),'UTC') as trade_time_us,
        cast(ingestion_time as DateTime) as ingestion_time,
        cast(is_buyer_maker as UInt8) as is_buyer_maker,
        cast(is_best_match as UInt8) as is_best_match,
        ingestion_time
        
    from {{ source('binance_raw', 'raw_binance_trade_batchjob') }}
)

select
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    trade_time_us,
    toTimeZone(trade_time_us,'Asia/Ho_Chi_Minh') as trade_time_vn,
    ingestion_time,
    toTimeZone(ingestion_time,'Asia/Ho_Chi_Minh') as ingestion_time_vn,
    is_buyer_maker,
    is_best_match
from source_data

{% if is_incremental() %}
    where toDate(trade_time_us) = Date('{{ var("run_date") }}')
{% endif %}
