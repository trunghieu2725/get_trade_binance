# ...existing code...
from kafka import KafkaProducer
import json
import time
import argparse
import os
import websocket
import threading

def now_us():
    return int(time.time() * 1_000_000)

# def send_trade(producer, topic, trade):
#     # send full Binance trade object, but add normalized timestamp fields for convenience
#     msg = dict(trade)  # copy original payload
#     # add microsecond timestamps (if T present in ms)
#     try:
#         msg["trade_time_us"] = int(trade.get("T")) * 1000
#     except Exception:
#         msg["trade_time_us"] = None
#     msg["ingestion_time_us"] = now_us()
#     # send raw msg (JSON serializable)
#     print(f"sending {msg.get('s')} price={msg.get('p')} qty={msg.get('q')}")
#     producer.send(topic, value=msg, key=(trade.get("s") or "").encode("utf-8"))
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP","localhost:9092"))
    parser.add_argument("--topic", default="binance-trade")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,ADAUSDT")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
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