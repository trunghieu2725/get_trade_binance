

select
    -- Ép kiểu ID và Chuỗi ký tự tự động
    cast(trade_id as UInt64) as trade_id,
    cast(symbol as String) as symbol,
    
    -- Chuyển đổi định dạng số (ClickHouse tối ưu rất tốt Float64/Decimal)
    cast(price as Float64) as price,
    cast(qty as Float64) as quantity,
    cast(quote_qty as Float64) as quote_quantity,
    
    -- Xử lý thời gian (Ép sang DateTime của ClickHouse)
    cast(trade_time as DateTime) as trade_time,
    cast(ingestion_time as DateTime) as ingestion_time,
    
    -- Biến Boolean
    cast(is_buyer_maker as UInt8) as is_buyer_maker,
    cast(is_best_match as UInt8) as is_best_match

from  {{ source('binance_raw', 'binance_trade_batchjob') }}