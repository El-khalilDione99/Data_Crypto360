#!/usr/bin/env python3
"""
Binance WebSocket → Kafka avec monitoring Prometheus
"""

import websocket
import json
from datetime import datetime
from kafka import KafkaProducer
import time
import signal
import threading
import sys

# Import du monitoring
sys.path.append('/app')
from utils.monitoring import (
    start_metrics_server,
    track_execution_time,
    ServiceHealthTracker,
    crypto_messages_received,
    crypto_current_price,
    websocket_connections_active,
    websocket_reconnections_total,
    websocket_message_latency,
    kafka_messages_sent_total,
    kafka_processing_duration,
    update_process_metrics
)

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
health_tracker = ServiceHealthTracker('binance_kafka')

# Compteurs
message_count = 0
candle_count = 0
error_count = 0


@track_execution_time('binance_kafka', 'wait_for_kafka')
def wait_for_kafka(max_retries=20, delay=3):
    """Attend que Kafka soit prêt avec monitoring"""
    for i in range(max_retries):
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(" Kafka prêt.")
            health_tracker.mark_healthy()
            return prod
        except Exception as e:
            print(f"Tentative {i+1}/{max_retries} : Kafka non disponible ({e})")
            health_tracker.record_error('KafkaConnectionError')
            time.sleep(delay)
    
    health_tracker.mark_unhealthy()
    raise ConnectionError(" Kafka ne répond pas.")


def on_message(ws, message):
    """Traite et envoie les données vers Kafka avec métriques"""
    global producer, message_count, candle_count
    
    start_time = time.time()
    
    try:
        data = json.loads(message)
        k = data["data"]["k"]
        
        # Calculer la latence
        message_timestamp = k["t"] / 1000  # Timestamp en secondes
        latency = time.time() - message_timestamp
        
        symbol = k["s"]
        close_price = float(k["c"])
        
        # Enregistrer la latence WebSocket
        websocket_message_latency.labels(
            exchange='binance',
            symbol=symbol
        ).observe(latency)
        
        # Compter le message reçu
        crypto_messages_received.labels(
            source='binance',
            symbol=symbol,
            message_type='kline'
        ).inc()
        
        message_count += 1
        
        # Mettre à jour le prix actuel
        crypto_current_price.labels(
            symbol=symbol,
            source='binance'
        ).set(close_price)
        
        # Préparer les données
        crypto_data = {
            "symbol": symbol,
            "timestamp": datetime.fromtimestamp(message_timestamp).isoformat(),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": close_price,
            "volume": float(k["v"]),
            "num_trades": int(k["n"]),
            "is_closed": k["x"],
            "latency_ms": latency * 1000
        }
        
        # Afficher prix en temps réel (seulement pour certaines cryptos)
        if not k["x"] and symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
            print(f" {symbol:10} → ${close_price:10,.2f} | "
                  f"Latence: {latency*1000:.0f}ms | Total: {message_count}")
        
        # Envoyer vers Kafka
        if producer:
            try:
                producer.send(KAFKA_TOPIC, value=crypto_data)
                
                # Métriques Kafka
                kafka_messages_sent_total.labels(
                    topic=KAFKA_TOPIC,
                    status='success'
                ).inc()
                
                if k["x"]:  # Bougie fermée
                    candle_count += 1
                    print(f" Bougie fermée: {symbol} envoyée à Kafka (#{candle_count})")
                
                # Enregistrer le temps de traitement
                processing_time = time.time() - start_time
                kafka_processing_duration.labels(
                    topic=KAFKA_TOPIC
                ).observe(processing_time)
                
                # Marquer comme sain
                health_tracker.mark_healthy()
                
            except Exception as e:
                print(f" Erreur envoi Kafka: {e}")
                kafka_messages_sent_total.labels(
                    topic=KAFKA_TOPIC,
                    status='error'
                ).inc()
                health_tracker.record_error('KafkaSendError')
                
    except json.JSONDecodeError as e:
        print(f" Erreur JSON: {e}")
        health_tracker.record_error('JSONDecodeError')
    except Exception as e:
        print(f" Erreur traitement message: {e}")
        health_tracker.record_error('MessageProcessingError')


def on_error(ws, error):
    """Gère les erreurs WebSocket"""
    global error_count
    error_count += 1
    print(f" Erreur WebSocket #{error_count}: {error}")
    health_tracker.record_error('WebSocketError')


def on_close(ws, close_status_code, close_msg):
    """Gère la fermeture de la connexion"""
    print(f" Connexion fermée: {close_msg} (code: {close_status_code})")
    websocket_connections_active.labels(exchange='binance').dec()
    
    if not stop_event.is_set():
        websocket_reconnections_total.labels(
            exchange='binance',
            reason='closed'
        ).inc()


def on_open(ws):
    """Gère l'ouverture de la connexion"""
    print(f" WebSocket ouvert - {len(symbols)} cryptos surveillées")
    print(f" Envoi vers Kafka topic: {KAFKA_TOPIC}\n")
    
    websocket_connections_active.labels(exchange='binance').inc()
    health_tracker.mark_healthy()


def signal_handler(sig, frame):
    """Gère l'arrêt propre"""
    print("\n Arrêt en cours...")
    stop_event.set()
    
    if ws_app:
        ws_app.close()
    
    if producer:
        print(" Flush des messages Kafka...")
        producer.flush()
        producer.close()
    
    # Statistiques finales
    print(f"\n Statistiques finales:")
    print(f"   Messages reçus: {message_count}")
    print(f"   Bougies fermées: {candle_count}")
    print(f"   Erreurs: {error_count}")
    
    websocket_connections_active.labels(exchange='binance').set(0)


def metrics_updater():
    """Thread pour mettre à jour les métriques système"""
    while not stop_event.is_set():
        try:
            update_process_metrics('binance_kafka')
            time.sleep(30)  # Toutes les 30 secondes
        except:
            pass


def main():
    """Point d'entrée principal"""
    global ws_app, producer
    
    print("="*60)
    print(" BINANCE WEBSOCKET → KAFKA AVEC MONITORING")
    print("="*60)
    print(f" WebSocket: {socket_url}")
    print(f" Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f" Topic: {KAFKA_TOPIC}")
    print(f" Cryptos: {len(symbols)}")
    print("="*60 + "\n")
    
    # Démarrer le serveur de métriques
    start_metrics_server(port=8000, service_name='binance_kafka')
    # Démarrer le thread de mise à jour des métriques
    metrics_thread = threading.Thread(target=metrics_updater, daemon=True)
    metrics_thread.start()
    
    # Initialiser Kafka
    try:
        producer = wait_for_kafka()
    except Exception as e:
        print(f" Erreur Kafka : {e}")
        return False
    
    # Configurer les signaux
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Boucle WebSocket avec reconnexion
    reconnect_count = 0
    while not stop_event.is_set():
        try:
            print(f" Connexion WebSocket (tentative #{reconnect_count + 1})...")
            
            ws_app = websocket.WebSocketApp(
                socket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            ws_app.run_forever(
                ping_interval=20,
                ping_timeout=10
            )
            
        except Exception as e:
            if stop_event.is_set():
                break
            
            reconnect_count += 1
            print(f" Erreur WebSocket: {e}")
            health_tracker.record_error('WebSocketConnectionError')
        
        if not stop_event.is_set():
            reconnect_delay = min(5 * reconnect_count, 30)  # Max 30s
            print(f" Reconnexion dans {reconnect_delay}s...")
            time.sleep(reconnect_delay)
    
    print(" Arrêt propre du service Binance → Kafka")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)