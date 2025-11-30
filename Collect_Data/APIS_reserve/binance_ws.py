import websocket
import json
import pandas as pd
from datetime import datetime

# --- 1️⃣ Liste des cryptos à suivre en temps réel ---
symbols = ["btcusdt", "ethusdt", "bnbusdt"]  # tu peux en ajouter jusqu'à 50 environ
            
interval = "1m"  # intervalle de bougies temps réel (1m, 5m, 1h...)

# --- 2️⃣ Construction de l'URL WebSocket ---
streams = "/".join([f"{s}@kline_{interval}" for s in symbols])
socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"

print(f"🔗 Connexion à {socket_url}\n")

# --- 3️⃣ Fonction callback exécutée à chaque nouvelle donnée ---
def on_message(ws, message):
    data = json.loads(message)
    k = data['data']['k']  # partie "kline" du message
    symbol = k['s']
    open_time = datetime.fromtimestamp(k['t']/1000)
    close_time = datetime.fromtimestamp(k['T']/1000)
    interval = k['i']
    open_price = float(k['o'])
    close_price = float(k['c'])
    high = float(k['h'])
    low = float(k['l'])
    volume = float(k['v'])
    is_closed = k['x']

    print(f"⏰ {open_time} | {symbol} | {interval} | "
          f"Open: {open_price:.2f} | Close: {close_price:.2f} | "
          f"High: {high:.2f} | Low: {low:.2f} | Vol: {volume:.3f} | "
          f"{'✔️ Clôturée' if is_closed else '🟢 En cours'}")

# --- 4️⃣ Gestion des événements WebSocket ---
def on_error(ws, error):
    print("❌ Erreur WebSocket :", error)

def on_close(ws, close_status_code, close_msg):
    print("🔌 Connexion fermée :", close_msg)

def on_open(ws):
    print("✅ Connexion WebSocket ouverte - flux en cours...\n")

# --- 5️⃣ Lancement du WebSocket ---
ws = websocket.WebSocketApp(socket_url,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

ws.run_forever()
