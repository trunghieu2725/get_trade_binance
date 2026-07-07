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

parser = argparse.ArgumentParser(description="Load Binance trade parquet to ClickHouse")

parser.add_argument(
    "--date",
    type=str,
    help="Target date (YYYY-MM-DD). If omitted, defaults to yesterday."
)

args = parser.parse_args()

def check_and_delete_realtime(run_date):  ## kiểm tra thử batchjob đã chạy xong chưa, nếu chạy xong thì xóa realtime partition

    timeout = 120          # 2 phút
    interval = 10          # check mỗi 10 giây

    start = time.time()

    while time.time() - start < timeout:

        count = client.query(f"""
            SELECT count(*)
            FROM stg.stg_binance_trade_batchjob
            WHERE Date(trade_time_us) = '{run_date}'
        """).first_item["count()"]

        print(f"Batch rows = {count}")

        if count > 0:

            client.command(f"""
                ALTER TABLE raw.raw_binance_trade_realtime_persist
                DROP PARTITION '{run_date}'
            """)

            print("Drop partition success")
            return

        time.sleep(interval)

    print(f"WARNING: Batch data for {run_date} not found after 2 minutes.")
    return


if __name__ == "__main__":
    
    if args.date:
        target_date = args.date
    else:
        # Không truyền thì mặc định lấy hôm qua
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    check_and_delete_realtime(target_date)

    print("Clear batch data date:", target_date)