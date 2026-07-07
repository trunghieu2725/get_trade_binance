
from kafka import KafkaProducer
import json
import time
import argparse
import os
import websocket
import threading

def now_us():
    return int(time.time() * 1_000_000)


def send_trade(producer, topic, trade):
    # trade is a dict (the Binance "data" object)
    # wrap as {"payload": "<json string>"} so ClickHouse JSONEachRow can map to payload column
    wrapped = {"payload": json.dumps(trade, separators=(',', ':'))}
    producer.send(topic, value=wrapped)

def build_stream_url(symbols):
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"

def run_ws(producer, topic, symbols, bootstrap):
    url = build_stream_url(symbols)
    def on_message(ws, message):
        try:
            payload = json.loads(message)
            data = payload.get("data")
            if data:
                send_trade(producer, topic, data)
        except Exception as e:
            print("ws msg parse error:", e)

    def on_error(ws, err):
        print("ws error:", err)

    def on_close(ws, code, reason):
        print("ws closed:", code, reason)

    def on_open(ws):
        print("ws opened to", url)

    while True:
        ws = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_open=on_open)
        ws.run_forever()
        print("ws exited, reconnecting in 5s")
        time.sleep(5)

def main():
    symbols_get  = [
    "BTCUSDT"
    # , "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    # "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "TRXUSDT", "TONUSDT"
]
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP","localhost:9092"))
    parser.add_argument("--topic", default="binance-trade")
    
    args = parser.parse_args()

    symbols = [s.upper() for s in symbols_get]
    producer = KafkaProducer(

        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5
    )

    t = threading.Thread(target=run_ws, args=(producer, args.topic, symbols, args.bootstrap), daemon=True)
    t.start()

    try:
        while True:
            time.sleep(1)
            producer.flush()
    except KeyboardInterrupt:
        print("stopping producer")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()