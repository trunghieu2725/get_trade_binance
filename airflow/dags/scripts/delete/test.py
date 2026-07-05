import pandas as pd
import pyarrow.parquet as pq
import glob
import clickhouse_connect
import os
from dotenv import load_dotenv
from datetime import datetime
from datetime import datetime, timedelta
import argparse
from airflow.exceptions import AirflowException
import time

CLICKHOUSE_DB = "raw"
CLICKHOUSE_TABLE = "raw_binance_trade_realtime_persist"

# =========================
# ENV + CLICKHOUSE CONNECT
# =========================
load_dotenv()

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port = os.getenv("CLICKHOUSE_PORT", 8123),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", "")
)

result = client.query("SELECT count(*) FROM raw.raw_binance_trade_batchjob")

print(result)
print(type(result))
print(result.first_item)
print(type(result.first_item))
print(result.result_rows)
print(result.column_names)