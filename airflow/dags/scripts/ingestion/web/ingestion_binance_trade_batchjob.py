import os
from datetime import datetime
from io import BytesIO
import pandas as pd
import requests
from datetime import datetime, timedelta


SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "TONUSDT",
    "DOTUSDT", "LINKUSDT", "POLUSDT", "LTCUSDT", "BCHUSDT",
    "NEARUSDT", "ATOMUSDT", "APTUSDT", "ARBUSDT", "OPUSDT"
]

RUN_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

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
# CLEAN + NORMALIZE
# =========================
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    
    df["trade_time"] = df["trade_time"].astype(str)

    return df




# =========================
# SAVE PARQUET
# =========================
def save_parquet(df: pd.DataFrame, symbol: str, trade_date: str):
    file_path = os.path.join(
        OUTPUT_DIR,
        f"{symbol}_{trade_date}.parquet"
    )

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
    for symbol in SYMBOLS:
        try:
            print(f"Downloading {symbol} ...")

            df = download_daily_trades(symbol, RUN_DATE)

            df = normalize_df(df)

            save_parquet(df, symbol, RUN_DATE)

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")


if __name__ == "__main__":

    main()