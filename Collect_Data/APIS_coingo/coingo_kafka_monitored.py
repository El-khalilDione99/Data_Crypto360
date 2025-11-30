#!/usr/bin/env python3
"""
CoinGecko API → Kafka avec monitoring Prometheus
"""

import requests
from datetime import datetime
from kafka import KafkaProducer
import json
import time
import signal
import sys

# Import du monitoring
sys.path.append('/app')
from utils.monitoring import (
    start_metrics_server,
    track_execution_time,
    ServiceHealthTracker,
    APICallTracker,
    crypto_messages_received,
    crypto_current_price,
    crypto_volume_24h,
    kafka_messages_sent_total,
    kafka_processing_duration,
    api_requests_total,
    update_process_metrics
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
KAFKA_TOPIC = 'coingecko-data'
FETCH_INTERVAL = 300  # 5 minutes

# Mapping Binance ↔ CoinGecko
mapping = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "BNBUSDT": "binancecoin",
    "XRPUSDT": "ripple",
    "SOLUSDT": "solana",
    "USDCUSDT": "usd-coin",
    "DOGEUSDT": "dogecoin",
    "ADAUSDT": "cardano",
    "TRXUSDT": "tron",
    "AVAXUSDT": "avalanche-2",
    "DOTUSDT": "polkadot",
    "LTCUSDT": "litecoin",
    "SHIBUSDT": "shiba-inu",
    "TONUSDT": "the-open-network",
    "CROUSDT": "cronos",
    "XMRUSDT": "monero",
    "ZECUSDT": "zcash",
    "LINKUSDT": "chainlink",
    "MNTUSDT": "mantle",
    "TAOUSDT": "bittensor",
    "XLMUSDT": "stellar",
    "BCHUSDT": "bitcoin-cash"
}

# Variables globales
stop_event = False
producer = None
health_tracker = ServiceHealthTracker('coingecko_kafka')

# Statistiques
total_fetches = 0
total_cryptos_sent = 0
total_errors = 0


@track_execution_time('coingecko_kafka', 'wait_for_kafka')
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
            print("✅ Kafka prêt.")
            health_tracker.mark_healthy()
            return prod
        except Exception as e:
            print(f"⏳ Tentative {i+1}/{max_retries} : Kafka non disponible ({e})")
            health_tracker.record_error('KafkaConnectionError')
            time.sleep(delay)
    
    health_tracker.mark_unhealthy()
    raise ConnectionError("❌ Kafka ne répond pas.")


@track_execution_time('coingecko_kafka', 'fetch_coingecko_data')
def fetch_coingecko_data():
    """Récupère les données de CoinGecko avec métriques"""
    global total_fetches, total_cryptos_sent, total_errors
    
    ids = ",".join(mapping.values())
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": ids}
    
    # Utiliser le context manager pour tracker l'appel API
    with APICallTracker('coingecko', '/coins/markets') as tracker:
        try:
            print(f"📡 Requête CoinGecko (fetch #{total_fetches + 1})...")
            
            response = requests.get(url, params=params, timeout=30)
            tracker.status_code = response.status_code
            response.raise_for_status()
            
            data = response.json()
            timestamp = datetime.utcnow().isoformat()
            
            print(f"✅ {len(data)} cryptos reçues de CoinGecko")
            
            # Envoyer chaque crypto vers Kafka
            sent_count = 0
            for coin in data:
                try:
                    # Extraire les données
                    symbol_binance = next((k for k, v in mapping.items() if v == coin["id"]), None)
                    symbol = coin["symbol"].upper()
                    price = coin.get("current_price", 0)
                    volume_24h = coin.get("total_volume", 0)
                    market_cap = coin.get("market_cap", 0)
                    
                    # Compter le message
                    crypto_messages_received.labels(
                        source='coingecko',
                        symbol=symbol_binance or symbol,
                        message_type='market_data'
                    ).inc()
                    
                    # Mettre à jour les métriques de prix
                    if price > 0:
                        crypto_current_price.labels(
                            symbol=symbol_binance or symbol,
                            source='coingecko'
                        ).set(price)
                    
                    if volume_24h > 0:
                        crypto_volume_24h.labels(
                            symbol=symbol_binance or symbol,
                            source='coingecko'
                        ).set(volume_24h)
                    
                    # Préparer les données complètes
                    crypto_data = {
                        "symbol_binance": symbol_binance,
                        "id_coingecko": coin["id"],
                        "name": coin["name"],
                        "symbol": symbol,
                        "image_url": coin["image"],
                        "current_price_usd": price,
                        "market_cap": market_cap,
                        "market_cap_rank": coin.get("market_cap_rank"),
                        "total_volume": volume_24h,
                        "price_change_24h": coin.get("price_change_24h"),
                        "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                        "high_24h": coin.get("high_24h"),
                        "low_24h": coin.get("low_24h"),
                        "circulating_supply": coin.get("circulating_supply"),
                        "total_supply": coin.get("total_supply"),
                        "max_supply": coin.get("max_supply"),
                        "ath": coin.get("ath"),
                        "ath_date": coin.get("ath_date"),
                        "timestamp": timestamp
                    }
                    
                    # Envoyer vers Kafka
                    send_start = time.time()
                    producer.send(KAFKA_TOPIC, value=crypto_data)
                    
                    # Métriques Kafka
                    kafka_messages_sent_total.labels(
                        topic=KAFKA_TOPIC,
                        status='success'
                    ).inc()
                    
                    send_duration = time.time() - send_start
                    kafka_processing_duration.labels(
                        topic=KAFKA_TOPIC
                    ).observe(send_duration)
                    
                    sent_count += 1
                    
                except Exception as e:
                    print(f"❌ Erreur traitement {coin.get('id', 'unknown')}: {e}")
                    kafka_messages_sent_total.labels(
                        topic=KAFKA_TOPIC,
                        status='error'
                    ).inc()
                    health_tracker.record_error('DataProcessingError')
            
            # Flush Kafka
            producer.flush()
            
            total_fetches += 1
            total_cryptos_sent += sent_count
            
            print(f"✅ {sent_count} cryptos envoyées à Kafka")
            print(f"📊 Statistiques: {total_cryptos_sent} cryptos | {total_fetches} fetches | {total_errors} erreurs\n")
            
            # Afficher top 5
            top_5 = sorted(data, key=lambda x: x.get('market_cap_rank', 999))[:5]
            print("🏆 Top 5 Market Cap:")
            for coin in top_5:
                rank = coin.get('market_cap_rank', '?')
                name = coin.get('name', 'Unknown')
                price = coin.get('current_price', 0)
                change_24h = coin.get('price_change_percentage_24h', 0)
                print(f"   #{rank:2} {name:15} ${price:10,.2f} ({change_24h:+6.2f}%)")
            print()
            
            health_tracker.mark_healthy()
            return True
            
        except requests.exceptions.Timeout:
            tracker.status_code = 408
            print("❌ Timeout de la requête CoinGecko")
            health_tracker.record_error('APITimeout')
            total_errors += 1
            return False
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur API CoinGecko: {e}")
            health_tracker.record_error('APIRequestError')
            total_errors += 1
            return False
            
        except Exception as e:
            tracker.status_code = 500
            print(f"❌ Erreur générale: {e}")
            import traceback
            traceback.print_exc()
            health_tracker.record_error('GeneralError')
            total_errors += 1
            return False


def signal_handler(sig, frame):
    """Gestionnaire d'arrêt propre"""
    global stop_event
    print("\n🛑 Arrêt demandé...")
    stop_event = True
    
    if producer:
        print("💾 Flush des messages Kafka...")
        producer.flush()
        producer.close()
    
    # Statistiques finales
    print(f"\n📊 Statistiques finales:")
    print(f"   Fetches totaux: {total_fetches}")
    print(f"   Cryptos envoyées: {total_cryptos_sent}")
    print(f"   Erreurs: {total_errors}")
    
    sys.exit(0)


def metrics_updater():
    """Met à jour les métriques système périodiquement"""
    while not stop_event:
        try:
            update_process_metrics('coingecko_kafka')
            time.sleep(30)
        except:
            pass


def main():
    """Point d'entrée principal"""
    global stop_event, producer
    
    print("="*60)
    print("🚀 COINGECKO API → KAFKA AVEC MONITORING")
    print("="*60)
    print(f"📨 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📊 Topic: {KAFKA_TOPIC}")
    print(f"💰 Cryptos: {len(mapping)}")
    print(f"⏱️  Intervalle: {FETCH_INTERVAL}s ({FETCH_INTERVAL//60} minutes)")
    print("="*60 + "\n")
    
    # Démarrer le serveur de métriques
    start_metrics_server(port=8000, service_name='coingecko_kafka')
    
    # Démarrer le thread de mise à jour des métriques
    import threading
    metrics_thread = threading.Thread(target=metrics_updater, daemon=True)
    metrics_thread.start()
    
    # Initialiser Kafka
    try:
        producer = wait_for_kafka()
    except Exception as e:
        print(f"❌ Erreur Kafka : {e}")
        return False
    
    # Configurer les signaux
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Boucle principale
    while not stop_event:
        try:
            # Fetch les données
            success = fetch_coingecko_data()
            
            if not success:
                print("⚠️  Échec du fetch, nouvelle tentative dans 60s...")
                time.sleep(60)
                continue
            
            # Attendre avant le prochain fetch
            print(f"⏳ Prochaine exécution dans {FETCH_INTERVAL}s...")
            
            # Attente interruptible
            for i in range(FETCH_INTERVAL):
                if stop_event:
                    break
                time.sleep(1)
                
                # Afficher un point toutes les 30 secondes
                if (i + 1) % 30 == 0:
                    remaining = FETCH_INTERVAL - i - 1
                    print(f"⏳ Attente... {remaining}s restantes")
            
        except Exception as e:
            print(f"❌ Erreur dans la boucle principale: {e}")
            import traceback
            traceback.print_exc()
            health_tracker.record_error('MainLoopError')
            time.sleep(60)
    
    # Fermeture propre
    if producer:
        producer.flush()
        producer.close()
    
    print("✅ Arrêt propre du service CoinGecko → Kafka")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)