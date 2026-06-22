KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "binance-trades"
symbols = [
    "btcusdt"
    # , "ethusdt", "bnbusdt", "xrpusdt", "solusdt",
    # "dogeusdt", "adausdt", "maticusdt", "ltcusdt", "dotusdt",
    # "shibusdt", "avaxusdt", "uniusdt", "linkusdt", "atomusdt",
    # "etcusdt", "filusdt", "nearusdt", "aptusdt"
    , "arbusdt"
]
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"