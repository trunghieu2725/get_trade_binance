from config import *
import json
from kafka import KafkaConsumer
from datetime import datetime
import os
consumer = KafkaConsumer(

    "binance-trade",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    group_id="trade-group",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("consumer started, waiting messages...")
for msg in consumer:
    print(msg.value)