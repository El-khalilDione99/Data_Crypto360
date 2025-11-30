# APIS_binance/binance_kafka.py
import websocket
import json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import time
import signal
import threading

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
KAFKA_TOPIC = 'binance-realtime'

# Liste des cryptos
symbols = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT", "USDCUSDT",
    "DOGEUSDT", "ADAUSDT", "TRXUSDT", "AVAXUSDT", "DOTUSDT", "LTCUSDT",
    "SHIBUSDT", "TONUSDT", "CROUSDT", "XMRUSDT", "ZECUSDT", "LINKUSDT",
    "MNTUSDT", "TAOUSDT", "XLMUSDT", "BCHUSDT"
]

interval = "1m"
streams = "/".join([f"{s.lower()}@kline_{interval}" for s in symbols])
socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"

stop_event = threading.Event()
ws_app = None
producer = None


def wait_for_kafka(max_retries=20, delay=3):
    """Attend que Kafka soit prêt"""
    for i in range(max_retries):
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(" Kafka prêt.")
            return prod
        except Exception as e:
            print(f" Tentative {i+1}/{max_retries} : Kafka non disponible ({e})")
            time.sleep(delay)
    raise ConnectionError(" Kafka ne répond pas.")


def on_message(ws, message):
    """Envoie les données vers Kafka"""
    global producer
    
    try:
        data = json.loads(message)
        k = data["data"]["k"]
        
        # Préparer les données
        crypto_data = {
            "symbol": k["s"],
            "timestamp": datetime.fromtimestamp(k["t"]/1000).isoformat(),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "num_trades": int(k["n"]),
            "is_closed": k["x"]
        }
        
        # Afficher prix en temps réel
        if not k["x"]:
            print(f" {crypto_data['symbol']} → {crypto_data['close']:.2f}")
        
        # Envoyer vers Kafka
        if producer:
            producer.send(KAFKA_TOPIC, value=crypto_data)
            if k["x"]:  # Bougie fermée
                print(f" Bougie {crypto_data['symbol']} envoyée à Kafka")
                
    except Exception as e:
        print(f" Erreur : {e}")


def on_error(ws, error):
    print(f" Erreur WebSocket : {error}")


def on_close(ws, close_status_code, close_msg):
    print(f" Connexion fermée : {close_msg}")


def on_open(ws):
    print(" WebSocket ouvert - Envoi vers Kafka...\n")


def signal_handler(sig, frame):
    print("\n Arrêt en cours...")
    stop_event.set()
    if ws_app:
        ws_app.close()
    if producer:
        producer.flush()
        producer.close()


def main():
    global ws_app, producer
    
    print(f" Connexion à : {socket_url}")
    
    # Initialiser Kafka
    try:
        producer = wait_for_kafka()
    except Exception as e:
        print(f" Erreur Kafka : {e}")
        return False
    
    # Signaux
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Boucle WebSocket
    while not stop_event.is_set():
        try:
            ws_app = websocket.WebSocketApp(
                socket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws_app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            if stop_event.is_set():
                break
            print(f" Erreur : {e}")
        
        if not stop_event.is_set():
            print(" Reconnexion dans 5s...")
            time.sleep(5)
    
    print(" Arrêt propre.")
    return True


if __name__ == "__main__":
    main()