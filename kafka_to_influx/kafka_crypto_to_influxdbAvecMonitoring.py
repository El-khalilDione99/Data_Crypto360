#!/usr/bin/env python3
"""
Kafka → InfluxDB Consumer avec monitoring Prometheus
Version adaptée au code existant
"""

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
import os
import time
import sys

# Import du monitoring
sys.path.append('/app')
from utils.monitoring import (
    start_metrics_server,
    track_execution_time,
    ServiceHealthTracker,
    kafka_messages_consumed_total,
    kafka_consumer_lag,
    kafka_processing_duration,
    influxdb_points_written_total,
    influxdb_write_duration,
    update_process_metrics
)

# Configuration depuis variables d'environnement
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "my-bucket")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC_BINANCE = "binance-realtime"
KAFKA_TOPIC_COINGECKO = "coingecko-data"

# Health tracker
health_tracker = ServiceHealthTracker('kafka_influxdb')

print("="*60)
print(" KAFKA → INFLUXDB AVEC MONITORING")
print("="*60)
print(f" Kafka: {KAFKA_BROKER}")
print(f" InfluxDB: {INFLUX_URL}")
print(f" Bucket: {INFLUX_BUCKET}")
print(f" Métriques: http://0.0.0.0:8000/metrics")
print("="*60 + "\n")

# Démarrer le serveur de métriques Prometheus
start_metrics_server(port=8000, service_name='kafka_influxdb')

# Connexion InfluxDB
@track_execution_time('kafka_influxdb', 'init_influxdb')
def init_influxdb():
    """Initialise la connexion InfluxDB"""
    try:
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        
        # Test de connexion
        health = client.health()
        if health.status == "pass":
            print(" InfluxDB connecté et opérationnel")
            health_tracker.mark_healthy()
        else:
            print(f" InfluxDB santé: {health.status}")
        
        return client, write_api
    except Exception as e:
        print(f" Erreur connexion InfluxDB: {e}")
        health_tracker.record_error('InfluxDBConnectionError')
        raise

client, write_api = init_influxdb()

# Connexion Kafka (écoute les 2 topics)
@track_execution_time('kafka_influxdb', 'init_kafka')
def init_kafka():
    """Initialise le consumer Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_BINANCE,
            KAFKA_TOPIC_COINGECKO,
            bootstrap_servers=KAFKA_BROKER,
            group_id='crypto-influx-consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        print(f" En écoute des topics : {KAFKA_TOPIC_BINANCE}, {KAFKA_TOPIC_COINGECKO}\n")
        health_tracker.mark_healthy()
        return consumer
    except Exception as e:
        print(f" Erreur connexion Kafka: {e}")
        health_tracker.record_error('KafkaConnectionError')
        raise

consumer = init_kafka()

# Compteurs pour monitoring
count_binance = 0
count_coingecko = 0
count_errors = 0
last_metrics_update = time.time()

def process_binance_message(data):
    """Traite un message Binance et retourne un Point InfluxDB"""
    global count_binance
    
    start_time = time.time()
    
    try:
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
        write_start = time.time()
        write_api.write(bucket=INFLUX_BUCKET, record=point)
        write_duration = time.time() - write_start
        
        # Métriques
        kafka_messages_consumed_total.labels(
            topic=KAFKA_TOPIC_BINANCE,
            consumer_group='crypto-influx-consumer'
        ).inc()
        
        influxdb_points_written_total.labels(
            measurement='binance_prices',
            status='success'
        ).inc()
        
        influxdb_write_duration.labels(
            measurement='binance_prices'
        ).observe(write_duration)
        
        processing_time = time.time() - start_time
        kafka_processing_duration.labels(
            topic=KAFKA_TOPIC_BINANCE
        ).observe(processing_time)
        
        count_binance += 1
        
        # Log toutes les 10 écritures
        if count_binance % 10 == 0:
            print(f" Binance: {count_binance} points écrits | "
                  f"Dernier: {data['symbol']} = ${data['close']:.2f} | "
                  f"Latence: {write_duration*1000:.1f}ms")
        
        health_tracker.mark_healthy()
        return True
        
    except Exception as e:
        print(f"Erreur traitement Binance: {e}")
        influxdb_points_written_total.labels(
            measurement='binance_prices',
            status='error'
        ).inc()
        health_tracker.record_error('BinanceProcessingError')
        return False


def process_coingecko_message(data):
    """Traite un message CoinGecko et retourne un Point InfluxDB"""
    global count_coingecko
    
    start_time = time.time()
    
    try:
        point = Point("coingecko_prices") \
            .tag("symbol", data.get("symbol_binance", "UNKNOWN")) \
            .tag("coin_id", data.get("id_coingecko", "unknown")) \
            .tag("name", data.get("name", "Unknown")) \
            .field("logo_url", data.get("image_url", "")) \
            .field("price_usd", float(data.get("current_price_usd", 0))) \
            .field("market_cap", float(data.get("market_cap", 0))) \
            .field("market_cap_rank", int(data.get("market_cap_rank", 0))) \
            .field("volume_24h", float(data.get("total_volume", 0))) \
            .field("price_change_24h", float(data.get("price_change_24h", 0))) \
            .field("price_change_pct_24h", float(data.get("price_change_percentage_24h", 0)))
        
        if "timestamp" in data:
            point = point.time(data["timestamp"])
        
        # Écrire dans InfluxDB
        write_start = time.time()
        write_api.write(bucket=INFLUX_BUCKET, record=point)
        write_duration = time.time() - write_start
        
        # Métriques
        kafka_messages_consumed_total.labels(
            topic=KAFKA_TOPIC_COINGECKO,
            consumer_group='crypto-influx-consumer'
        ).inc()
        
        influxdb_points_written_total.labels(
            measurement='coingecko_prices',
            status='success'
        ).inc()
        
        influxdb_write_duration.labels(
            measurement='coingecko_prices'
        ).observe(write_duration)
        
        processing_time = time.time() - start_time
        kafka_processing_duration.labels(
            topic=KAFKA_TOPIC_COINGECKO
        ).observe(processing_time)
        
        count_coingecko += 1
        
        # Log toutes les 5 écritures
        if count_coingecko % 5 == 0:
            print(f" CoinGecko: {count_coingecko} points écrits | "
                  f"Dernier: {data['name']} = ${data['current_price_usd']:.2f} | "
                  f"Latence: {write_duration*1000:.1f}ms")
        
        health_tracker.mark_healthy()
        return True
        
    except Exception as e:
        print(f" Erreur traitement CoinGecko: {e}")
        influxdb_points_written_total.labels(
            measurement='coingecko_prices',
            status='error'
        ).inc()
        health_tracker.record_error('CoinGeckoProcessingError')
        return False


def update_consumer_lag():
    """Met à jour les métriques de lag du consumer"""
    try:
        partitions = consumer.assignment()
        for partition in partitions:
            current_offset = consumer.position(partition)
            end_offsets = consumer.end_offsets([partition])
            end_offset = end_offsets[partition]
            lag = end_offset - current_offset
            
            kafka_consumer_lag.labels(
                topic=partition.topic,
                partition=str(partition.partition),
                consumer_group='crypto-influx-consumer'
            ).set(lag)
    except Exception as e:
        pass  # Ignore les erreurs de lag


try:
    print(" Consommation des messages Kafka en cours...\n")
    
    for message in consumer:
        try:
            data = message.value
            topic = message.topic
            
            # ===== TRAITEMENT BINANCE =====
            if topic == KAFKA_TOPIC_BINANCE:
                success = process_binance_message(data)
                if not success:
                    count_errors += 1
            
            # ===== TRAITEMENT COINGECKO =====
            elif topic == KAFKA_TOPIC_COINGECKO:
                success = process_coingecko_message(data)
                if not success:
                    count_errors += 1
            
            # Mettre à jour les métriques système périodiquement
            if time.time() - last_metrics_update > 30:
                update_process_metrics('kafka_influxdb')
                update_consumer_lag()
                last_metrics_update = time.time()
                
                # Afficher un résumé
                total = count_binance + count_coingecko
                print(f"\n📊 Résumé: {total} points écrits "
                      f"(Binance: {count_binance}, CoinGecko: {count_coingecko}, "
                      f"Erreurs: {count_errors})\n")
        
        except Exception as e:
            print(f"❌ Erreur traitement message : {e}")
            print(f"   Message: {message.value}")
            count_errors += 1
            health_tracker.record_error('MessageProcessingError')
            continue

except KeyboardInterrupt:
    print("\n Arrêt du consumer...")
finally:
    consumer.close()
    client.close()
    
    print(f"\n{'='*60}")
    print(" STATISTIQUES FINALES")
    print(f"{'='*60}")
    print(f"   Binance points: {count_binance}")
    print(f"   CoinGecko points: {count_coingecko}")
    print(f"   Total: {count_binance + count_coingecko}")
    print(f"   Erreurs: {count_errors}")
    print(f"{'='*60}")
    print(" Consumer arrêté proprement")