# APIS_coingo/coingo_kafka.py
import requests
from datetime import datetime
from kafka import KafkaProducer
import json
import time
import signal
import sys

KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
KAFKA_TOPIC = 'coingecko-data'

# Variable globale pour arrêt propre
stop_event = False

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


def signal_handler(sig, frame):
    """Gestionnaire d'arrêt propre"""
    global stop_event
    print("\n Arrêt demandé...")
    stop_event = True
    sys.exit(0)


def main():
    """Point d'entrée principal"""
    global stop_event
    
    print(" Démarrage CoinGecko → Kafka...")
    
    # Initialiser Kafka
    try:
        producer = wait_for_kafka()
    except Exception as e:
        print(f" Erreur Kafka : {e}")
        return False
    
    # Configurer signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Boucle principale
    while not stop_event:
        try:
            # Appel API CoinGecko
            ids = ",".join(mapping.values())
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {"vs_currency": "usd", "ids": ids}
            
            print(" Requête CoinGecko...")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            timestamp = datetime.utcnow().isoformat()
            
            # Envoyer chaque crypto vers Kafka
            for coin in data:
                crypto_data = {
                    "symbol_binance": next((k for k, v in mapping.items() if v == coin["id"]), None),
                    "id_coingecko": coin["id"],
                    "name": coin["name"],
                    "symbol": coin["symbol"].upper(),
                    "image_url": coin["image"],
                    "current_price_usd": coin.get("current_price"),
                    "market_cap": coin.get("market_cap"),
                    "market_cap_rank": coin.get("market_cap_rank"),
                    "total_volume": coin.get("total_volume"),
                    "price_change_24h": coin.get("price_change_24h"),
                    "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                    "timestamp": timestamp
                }
                
                producer.send(KAFKA_TOPIC, value=crypto_data)
            
            print(f" {len(data)} cryptos envoyées à Kafka")
            producer.flush()
            
            # Attendre 5 minutes
            print(" Prochaine exécution dans 5 minutes...")
            for _ in range(300):  # 300 secondes = 5 minutes
                if stop_event:
                    break
                time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f" Erreur API CoinGecko : {e}")
            time.sleep(60)
        except Exception as e:
            print(f" Erreur générale : {e}")
            import traceback
            traceback.print_exc()
            time.sleep(60)
    
    # Fermeture propre
    producer.flush()
    producer.close()
    print(" Arrêt propre du service CoinGecko.")
    return True


if __name__ == "__main__":
    main()

