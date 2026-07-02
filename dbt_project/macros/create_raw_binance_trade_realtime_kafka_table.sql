{% macro create_raw_binance_trade_realtime_kafka_table() %}
CREATE TABLE IF NOT EXISTS raw.raw_binance_trade_realtime
(
    `key` String,
    `payload` String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'binance-trade',
    kafka_group_name = 'clickhouse-consumer-2',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1
{% endmacro %}
