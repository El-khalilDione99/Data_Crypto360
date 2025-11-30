#!/usr/bin/env python3
"""
Module de monitoring centralisé pour DataFlow360
Fournit des métriques Prometheus pour tous les services

Usage:
    from utils.monitoring import start_metrics_server, crypto_messages_received
    
    start_metrics_server(port=8000)
    crypto_messages_received.labels(source='binance', symbol='BTC').inc()
"""

import logging
import time
import os
from functools import wraps
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    start_http_server
)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==================== MÉTRIQUES PROMETHEUS ====================

# --- MESSAGES CRYPTO ---
crypto_messages_received = Counter(
    'dataflow_crypto_messages_received_total',
    'Nombre total de messages crypto reçus',
    ['source', 'symbol', 'message_type']
)

# --- PRIX DES CRYPTOS ---
crypto_current_price = Gauge(
    'dataflow_crypto_current_price_usd',
    'Prix actuel de la crypto en USD',
    ['symbol', 'source']
)

crypto_volume_24h = Gauge(
    'dataflow_crypto_volume_24h_usd',
    'Volume de trading 24h en USD',
    ['symbol', 'source']
)

# --- API EXTERNES ---
api_requests_total = Counter(
    'dataflow_api_requests_total',
    'Nombre total de requêtes API',
    ['api_name', 'endpoint', 'status_code']
)

api_request_duration = Histogram(
    'dataflow_api_request_duration_seconds',
    'Durée des requêtes API en secondes',
    ['api_name', 'endpoint'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

# --- WEBSOCKETS ---
websocket_connections_active = Gauge(
    'dataflow_websocket_connections_active',
    'Nombre de connexions WebSocket actives',
    ['exchange']
)

websocket_reconnections_total = Counter(
    'dataflow_websocket_reconnections_total',
    'Nombre de reconnexions WebSocket',
    ['exchange', 'reason']
)

websocket_message_latency = Histogram(
    'dataflow_websocket_message_latency_seconds',
    'Latence des messages WebSocket',
    ['exchange', 'symbol'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
)

# --- KAFKA ---
kafka_messages_sent_total = Counter(
    'dataflow_kafka_messages_sent_total',
    'Messages envoyés à Kafka',
    ['topic', 'status']
)

kafka_messages_consumed_total = Counter(
    'dataflow_kafka_messages_consumed_total',
    'Messages consommés depuis Kafka',
    ['topic', 'consumer_group']
)

kafka_consumer_lag = Gauge(
    'dataflow_kafka_consumer_lag_messages',
    'Nombre de messages en retard dans Kafka',
    ['topic', 'partition', 'consumer_group']
)

kafka_processing_duration = Histogram(
    'dataflow_kafka_processing_duration_seconds',
    'Durée de traitement d\'un message Kafka',
    ['topic'],
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
)

# --- INFLUXDB ---
influxdb_points_written_total = Counter(
    'dataflow_influxdb_points_written_total',
    'Points de données écrits dans InfluxDB',
    ['measurement', 'status']
)

influxdb_write_duration = Histogram(
    'dataflow_influxdb_write_duration_seconds',
    'Durée d\'écriture dans InfluxDB',
    ['measurement'],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
)

influxdb_batch_size = Gauge(
    'dataflow_influxdb_batch_size_points',
    'Taille du batch actuel pour InfluxDB',
    ['measurement']
)

# --- HDFS ---
hdfs_files_written_total = Counter(
    'dataflow_hdfs_files_written_total',
    'Fichiers écrits sur HDFS',
    ['path', 'status']
)

hdfs_file_size_bytes = Gauge(
    'dataflow_hdfs_file_size_bytes',
    'Taille des fichiers sur HDFS en bytes',
    ['path']
)

hdfs_write_duration = Histogram(
    'dataflow_hdfs_write_duration_seconds',
    'Durée d\'écriture sur HDFS',
    ['operation'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]
)

# --- SANTÉ DES SERVICES ---
service_errors_total = Counter(
    'dataflow_service_errors_total',
    'Nombre total d\'erreurs par service',
    ['service', 'error_type']
)

service_health_status = Gauge(
    'dataflow_service_health_status',
    'Statut de santé du service (1=up, 0=down)',
    ['service']
)

service_last_success_timestamp = Gauge(
    'dataflow_service_last_success_timestamp_seconds',
    'Timestamp Unix de la dernière opération réussie',
    ['service']
)

# --- PERFORMANCE ---
operation_summary = Summary(
    'dataflow_operation_summary_seconds',
    'Résumé des opérations (quantiles)',
    ['service', 'operation']
)

process_memory_bytes = Gauge(
    'dataflow_process_memory_bytes',
    'Utilisation mémoire du processus en bytes',
    ['service']
)

# ==================== DÉCORATEURS ====================

def track_execution_time(service_name, operation_name=None):
    """
    Décorateur pour mesurer le temps d'exécution d'une fonction
    
    Usage:
        @track_execution_time('binance', 'fetch_data')
        def fetch_binance_data():
            ...
    """
    def decorator(func):
        op_name = operation_name or func.__name__
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Enregistrer la durée
                operation_summary.labels(
                    service=service_name,
                    operation=op_name
                ).observe(duration)
                
                # Mettre à jour le timestamp de succès
                service_last_success_timestamp.labels(
                    service=service_name
                ).set(time.time())
                
                logger.debug(f"✅ [{service_name}] {op_name} completed in {duration:.3f}s")
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                
                # Compter l'erreur
                service_errors_total.labels(
                    service=service_name,
                    error_type=type(e).__name__
                ).inc()
                
                logger.error(f"❌ [{service_name}] {op_name} failed after {duration:.3f}s: {e}")
                raise
                
        return wrapper
    return decorator


def count_operation(metric_counter, **label_values):
    """
    Décorateur pour compter les opérations
    
    Usage:
        @count_operation(crypto_messages_received, source='binance', symbol='BTC', message_type='trade')
        def process_btc_message():
            ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            metric_counter.labels(**label_values).inc()
            return result
        return wrapper
    return decorator


# ==================== CLASSES HELPERS ====================

class ServiceHealthTracker:
    """Classe pour tracker la santé d'un service"""
    
    def __init__(self, service_name):
        self.service_name = service_name
        service_health_status.labels(service=service_name).set(1)
        logger.info(f"✅ Health tracker initialisé pour '{service_name}'")
    
    def mark_healthy(self):
        """Marquer le service comme sain"""
        service_health_status.labels(service=self.service_name).set(1)
        service_last_success_timestamp.labels(service=self.service_name).set(time.time())
    
    def mark_unhealthy(self):
        """Marquer le service comme en panne"""
        service_health_status.labels(service=self.service_name).set(0)
        logger.warning(f"⚠️  Service '{self.service_name}' marqué comme unhealthy")
    
    def record_error(self, error_type):
        """Enregistrer une erreur"""
        service_errors_total.labels(
            service=self.service_name,
            error_type=error_type
        ).inc()
        logger.error(f"❌ [{self.service_name}] Erreur enregistrée: {error_type}")


class APICallTracker:
    """Context manager pour tracker les appels API"""
    
    def __init__(self, api_name, endpoint):
        self.api_name = api_name
        self.endpoint = endpoint
        self.start_time = None
        self.status_code = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        
        # Déterminer le code de statut
        if exc_type is None:
            self.status_code = self.status_code or 200
        else:
            self.status_code = self.status_code or 500
        
        # Enregistrer la requête
        api_requests_total.labels(
            api_name=self.api_name,
            endpoint=self.endpoint,
            status_code=str(self.status_code)
        ).inc()
        
        # Enregistrer la durée
        api_request_duration.labels(
            api_name=self.api_name,
            endpoint=self.endpoint
        ).observe(duration)
        
        logger.debug(f"📡 API {self.api_name}{self.endpoint} → {self.status_code} ({duration:.3f}s)")
        
        return False  # Ne pas supprimer l'exception


# ==================== FONCTIONS UTILITAIRES ====================

def start_metrics_server(port=8000, service_name=None):
    """
    Démarre le serveur HTTP Prometheus
    
    Args:
        port: Port d'écoute (défaut: 8000)
        service_name: Nom du service (pour les logs)
    """
    try:
        start_http_server(port, addr="0.0.0.0")
        logger.info(f"✅ Serveur de métriques Prometheus démarré sur le port {port}")
        logger.info(f"📊 Métriques disponibles sur http://0.0.0.0:{port}/metrics")
        
        if service_name:
            service_health_status.labels(service=service_name).set(1)
            logger.info(f"✅ Service '{service_name}' marqué comme actif")
            
    except OSError as e:
        if "Address already in use" in str(e):
            logger.warning(f"⚠️  Port {port} déjà utilisé, le serveur de métriques est peut-être déjà démarré")
        else:
            logger.error(f"❌ Impossible de démarrer le serveur de métriques: {e}")


def record_crypto_price(symbol, price, source='unknown', volume_24h=None):
    """
    Enregistre le prix d'une crypto
    
    Args:
        symbol: Symbole de la crypto (BTC, ETH, etc.)
        price: Prix en USD
        source: Source des données (binance, coingecko, etc.)
        volume_24h: Volume 24h optionnel
    """
    crypto_current_price.labels(symbol=symbol, source=source).set(price)
    
    if volume_24h is not None:
        crypto_volume_24h.labels(symbol=symbol, source=source).set(volume_24h)
    
    logger.debug(f"💹 Prix mis à jour: {symbol} = ${price:.2f} ({source})")


def get_memory_usage():
    """Retourne l'utilisation mémoire du processus actuel"""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss
    except ImportError:
        logger.debug("psutil non installé, impossible de mesurer la mémoire")
        return 0
    except Exception as e:
        logger.debug(f"Erreur lors de la mesure mémoire: {e}")
        return 0


def update_process_metrics(service_name):
    """Met à jour les métriques du processus"""
    memory_bytes = get_memory_usage()
    if memory_bytes > 0:
        process_memory_bytes.labels(service=service_name).set(memory_bytes)
        logger.debug(f"💾 Mémoire {service_name}: {memory_bytes/1024/1024:.1f} MB")


# ==================== EXPORT ====================

__all__ = [
    # Métriques
    'crypto_messages_received',
    'crypto_current_price',
    'crypto_volume_24h',
    'api_requests_total',
    'api_request_duration',
    'websocket_connections_active',
    'websocket_reconnections_total',
    'websocket_message_latency',
    'kafka_messages_sent_total',
    'kafka_messages_consumed_total',
    'kafka_consumer_lag',
    'kafka_processing_duration',
    'influxdb_points_written_total',
    'influxdb_write_duration',
    'influxdb_batch_size',
    'hdfs_files_written_total',
    'hdfs_file_size_bytes',
    'hdfs_write_duration',
    'service_errors_total',
    'service_health_status',
    'service_last_success_timestamp',
    'operation_summary',
    'process_memory_bytes',
    
    # Décorateurs
    'track_execution_time',
    'count_operation',
    
    # Classes
    'ServiceHealthTracker',
    'APICallTracker',
    
    # Fonctions
    'start_metrics_server',
    'record_crypto_price',
    'update_process_metrics',
    'get_memory_usage',
]


# ==================== EXEMPLE D'UTILISATION ====================

if __name__ == '__main__':
    """
    Exemple d'utilisation du module
    """
    print("="*60)
    print("🧪 TEST DU MODULE MONITORING")
    print("="*60)
    
    # Démarrer le serveur de métriques
    start_metrics_server(8000, 'test_service')
    
    # Créer un health tracker
    health = ServiceHealthTracker('test_service')
    
    # Simuler des opérations
    @track_execution_time('test_service', 'test_operation')
    def test_function():
        time.sleep(0.1)
        return "OK"
    
    # Exécuter plusieurs fois
    for i in range(5):
        print(f"\n🔄 Test #{i+1}")
        
        # Appeler la fonction
        result = test_function()
        
        # Simuler réception de messages
        crypto_messages_received.labels(
            source='binance',
            symbol='BTCUSDT',
            message_type='test'
        ).inc()
        
        # Mettre à jour un prix
        record_crypto_price('BTCUSDT', 50000 + i*100, 'binance')
        
        # Marquer comme sain
        health.mark_healthy()
        
        time.sleep(1)
    
    # Afficher le lien vers les métriques
    print(f"\n{'='*60}")
    print("✅ Test terminé !")
    print(f"📊 Métriques disponibles: http://localhost:8000/metrics")
    print(f"{'='*60}")
    print("\nAppuyez sur Ctrl+C pour arrêter...")
    
    try:
        while True:
            update_process_metrics('test_service')
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n👋 Arrêt du test")