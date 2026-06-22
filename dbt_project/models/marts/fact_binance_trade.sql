select trade_id
    ,symbol
    ,price
    ,quantity
    ,quote_quantity
    ,trade_time
    ,is_buyer_maker
    ,is_best_match
    ,ingestion_time
from {{source('stg','stg_binance_trade_batchjob')}}
union all 
select trade_id
    ,symbol
    ,price
    ,qty
    ,price*qty
    ,trade_time
    ,is_buyer_maker
    ,is_best_match
    ,ingestion_time
from {{source('stg','stg_binance_trade_realtime')}}