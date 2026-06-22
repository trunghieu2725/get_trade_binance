import json
import websocket

def on_message(ws, message):
    data = json.loads(message)

    print(
        f"Time: {data['T']} | "
        f"Symbol: {data['s']} | "
        f"Price: {data['p']} | "
        f"Quantity: {data['q']}"
    )

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to Binance WebSocket")

symbol = "btcusdt"
socket_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"

ws = websocket.WebSocketApp(
    socket_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()