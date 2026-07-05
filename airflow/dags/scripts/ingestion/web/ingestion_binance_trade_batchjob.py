import os
from datetime import datetime
from io import BytesIO
import pandas as pd
import requests
from datetime import datetime, timedelta
import argparse

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "TONUSDT",
    "DOTUSDT", "LINKUSDT", "POLUSDT", "LTCUSDT", "BCHUSDT",
    "NEARUSDT", "ATOMUSDT", "APTUSDT", "ARBUSDT", "OPUSDT"
]
parser = argparse.ArgumentParser(
    description="Download Binance daily trade data"
)

parser.add_argument(
    "--date",
    type=str,
    help="Target date (YYYY-MM-DD). Default: yesterday"
)

args = parser.parse_args()


# RUN_DATE = '2026-06-17'

OUTPUT_DIR = "data/spot_trade_daily"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# =========================
# DOWNLOAD
# =========================
def download_daily_trades(symbol: str, trade_date: str) -> pd.DataFrame:
    url = (
        f"https://data.binance.vision/data/spot/daily/trades/"
        f"{symbol}/{symbol}-trades-{trade_date}.zip"
    )

    r = requests.get(url, timeout=300)
    r.raise_for_status()

    df = pd.read_csv(
        BytesIO(r.content),
        compression="zip",
        header=None,
        names=[
            "trade_id",
            "price",
            "qty",
            "quote_qty",
            "trade_time",
            "is_buyer_maker",
            "is_best_match",
        ],
    )

    # add symbol
    df["symbol"] = symbol
    return df


# =========================
# SAVE PARQUET
# =========================
def save_parquet(df: pd.DataFrame, symbol: str, trade_date: str):
    folder_path = os.path.join(
    OUTPUT_DIR,
    f"{trade_date}",
    f"{symbol}"
    )
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, f"{trade_date}_{symbol}.parquet")
    df.to_parquet(
        file_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    print(f"Saved: {file_path} ({len(df):,} rows)")


# =========================
# MAIN
# =========================
def main():
    if args.date:
        run_date = args.date
    else:
        run_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    for symbol in SYMBOLS:
        try:
            print(f"Downloading {symbol} ...")

            df = download_daily_trades(symbol, run_date)


            save_parquet(df, symbol, run_date)

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")


if __name__ == "__main__":

    main()