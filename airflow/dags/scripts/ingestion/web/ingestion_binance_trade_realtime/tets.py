# receive_test.py
from kafka import KafkaConsumer
import json
c = KafkaConsumer(
  'test-topic-clean',
  bootstrap_servers='localhost:9092',
  group_id='test-clean-group',
  auto_offset_reset='earliest',
  value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
for msg in c:
  print(msg.value)
  break