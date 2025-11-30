from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import os
import time

# Configuration depuis variables d'environnement
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "my-bucket")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC_BINANCE = "binance-realtime"
KAFKA_TOPIC_COINGECKO = "coingecko-data"

print("🚀 Kafka → InfluxDB consumer démarré...")
print(f"📡 Kafka: {KAFKA_BROKER}")
print(f"💾 InfluxDB: {INFLUX_URL}")
print(f"📊 Bucket: {INFLUX_BUCKET}")

# Connexion InfluxDB
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Connexion Kafka (écoute les 2 topics)
consumer = KafkaConsumer(
    KAFKA_TOPIC_BINANCE,
    KAFKA_TOPIC_COINGECKO,
    bootstrap_servers=KAFKA_BROKER,
    group_id='crypto-influx-consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',  # Commence aux nouveaux messages
    enable_auto_commit=True
)

print(f"✅ En écoute des topics : {KAFKA_TOPIC_BINANCE}, {KAFKA_TOPIC_COINGECKO}\n")

# Compteurs pour monitoring
count_binance = 0
count_coingecko = 0

try:
    for message in consumer:
        try:
            data = message.value
            topic = message.topic
            
            # ===== TRAITEMENT BINANCE =====
            if topic == KAFKA_TOPIC_BINANCE:
                # Données Binance : {"symbol": "BTCUSDT", "close": 106450, "volume": 123.45, ...}
                
                # Créer un point InfluxDB
                point = Point("binance_prices") \
                    .tag("symbol", data.get("symbol", "UNKNOWN")) \
                    .tag("interval", data.get("interval", "1m")) \
                    .field("open", float(data.get("open", 0))) \
                    .field("high", float(data.get("high", 0))) \
                    .field("low", float(data.get("low", 0))) \
                    .field("close", float(data.get("close", 0))) \
                    .field("volume", float(data.get("volume", 0))) \
                    .field("num_trades", int(data.get("num_trades", 0)))
                
                # Ajouter timestamp si présent
                if "timestamp" in data:
                    point = point.time(data["timestamp"])
                
                # Écrire dans InfluxDB
                write_api.write(bucket=INFLUX_BUCKET, record=point)
                
                count_binance += 1
                if count_binance % 10 == 0:  # Log toutes les 10 écritures
                    print(f"📊 Binance: {count_binance} points écrits | Dernier: {data['symbol']} = ${data['close']:.2f}")
            
            # ===== TRAITEMENT COINGECKO =====
            elif topic == KAFKA_TOPIC_COINGECKO:
                # Données CoinGecko : {"symbol_binance": "BTCUSDT", "name": "Bitcoin", "current_price_usd": 106450, ...}
                
                point = Point("coingecko_prices") \
                    .tag("symbol", data.get("symbol_binance", "UNKNOWN")) \
                    .tag("coin_id", data.get("id_coingecko", "unknown")) \
                    .tag("name", data.get("name", "Unknown")) \
                    .field("logo_url", data.get("image_url", ""))\
                    .field("price_usd", float(data.get("current_price_usd", 0))) \
                    .field("market_cap", float(data.get("market_cap", 0))) \
                    .field("market_cap_rank", int(data.get("market_cap_rank", 0))) \
                    .field("volume_24h", float(data.get("total_volume", 0))) \
                    .field("price_change_24h", float(data.get("price_change_24h", 0))) \
                    .field("price_change_pct_24h", float(data.get("price_change_percentage_24h", 0)))
                
                if "timestamp" in data:
                    point = point.time(data["timestamp"])
                
                write_api.write(bucket=INFLUX_BUCKET, record=point)
                
                count_coingecko += 1
                if count_coingecko % 5 == 0:
                    print(f"🪙 CoinGecko: {count_coingecko} points écrits | Dernier: {data['name']} = ${data['current_price_usd']:.2f}")
        
        except Exception as e:
            print(f"❌ Erreur traitement message : {e}")
            print(f"   Message: {message.value}")
            continue

except KeyboardInterrupt:
    print("\n🛑 Arrêt du consumer...")
finally:
    consumer.close()
    client.close()
    print(f"\n📊 Stats finales:")
    print(f"   Binance: {count_binance} points")
    print(f"   CoinGecko: {count_coingecko} points")
    print("✅ Consumer arrêté proprement")