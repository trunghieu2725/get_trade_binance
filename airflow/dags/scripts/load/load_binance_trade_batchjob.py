import pandas as pd
import pyarrow.parquet as pq
import glob
import clickhouse_connect
import os
from dotenv import load_dotenv
from datetime import datetime
from datetime import datetime, timedelta
import argparse
# =========================
# CONFIG
# =========================
PARQUET_FOLDER = "/opt/airflow/data/spot_trade_daily"
CLICKHOUSE_DB = "raw"
CLICKHOUSE_TABLE = "raw_binance_trade_batchjob"

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

# =========================
# SCHEMA
# =========================
COLUMNS = [
    "is_best_match",
    "ingestion_time",
    "trade_time",
    "is_buyer_maker",
    "quote_qty",
    "qty",
    "price",
    "trade_id",
    "symbol"
]
parser = argparse.ArgumentParser(description="Load Binance trade parquet to ClickHouse")

parser.add_argument(
    "--date",
    type=str,
    help="Target date (YYYY-MM-DD). If omitted, defaults to yesterday."
)

args = parser.parse_args()
# =========================
# LOAD PARQUET
# =========================
def load_parquet_files(target_date: str):
    base_folder = os.path.join(PARQUET_FOLDER, f"{target_date}")
    pattern = os.path.join(base_folder, "*", f"{target_date}_*.parquet")
    files = glob.glob(pattern)

    if not files:
        print(f"No files found: {pattern}")
        return None

    dfs = []

    for f in files:
        df = pq.read_table(f).to_pandas()
        dfs.append(df)

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True)

# =========================
# ADD INGESTION TIME
# =========================
def add_ingestion_time(df):
    if df is None or df.empty:
        print("No data to insert")
        return
    df["ingestion_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return df

# =========================
# =========================
# INSERT CLICKHOUSE
# =========================
def insert_to_clickhouse(df):
    if df is None or df.empty:
        print("No data to insert")
        return

    client.insert_df(
        database=CLICKHOUSE_DB,
        table=CLICKHOUSE_TABLE,
        df=df
    )

    print(f"Inserted {len(df)} rows into {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    
    if args.date:
        target_date = args.date
    else:
        # Không truyền thì mặc định lấy hôm qua
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # target_date = '2026-06-17'

    df = load_parquet_files(target_date)

    df = add_ingestion_time(df)

    print("Columns after clean:", df.columns.tolist())

    insert_to_clickhouse(df)