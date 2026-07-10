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
# =========================
# CONFIG
# =========================

CLICKHOUSE_DB_RAW =  "raw"
CLICKHOUSE_TABLE_RAW = "raw_binance_trade_batchjob"


CLICKHOUSE_DB_STG = "stg"
CLICKHOUSE_TABLE_STG = "stg_binance_trade_batchjob"

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

parser = argparse.ArgumentParser(description="Load Binance trade parquet to ClickHouse")

parser.add_argument(
    "--date",
    type=str,
    help="Target date (YYYY-MM-DD). If omitted, defaults to yesterday."
)

args = parser.parse_args()

def check_and_delete_batchjob(run_date):  ## kiểm tra thử batchjob đã chạy xong chưa, nếu chạy xong thì xóa realtime partition


        client.command(f"""
            delete from {CLICKHOUSE_DB_RAW}.{CLICKHOUSE_TABLE_RAW}
            where date(toTimeZone(fromUnixTimestamp64Micro(trade_time),'UTC')) <= '{run_date}'
        """)

        print("Drop Raw success")

        client.command(f"""
            delete from {CLICKHOUSE_DB_STG}.{CLICKHOUSE_TABLE_STG}
            where date(trade_time_us) <= '{run_date}'
        """)

        print("Drop STG success")

        return
 



if __name__ == "__main__":
    
    if args.date:
        target_date = args.date
    else:
        # Không truyền thì mặc định lấy hôm qua
        target_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

    check_and_delete_batchjob(target_date)

    print("Clear batch data date:", target_date)