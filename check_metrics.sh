#!/bin/bash
echo "🔍 Diagnostic des métriques Prometheus"
echo ""

for service in binance_kafka coingo_kafka kafka_to_influx crypto_batch; do
    echo "=== $service ==="
    
    # Test 1 : Le conteneur tourne ?
    if docker ps | grep -q $service; then
        echo "✅ Conteneur actif"
    else
        echo "❌ Conteneur non actif"
        continue
    fi
    
    # Test 2 : Le fichier monitoring.py existe ?
    if docker exec $service test -f /app/utils/monitoring.py 2>/dev/null; then
        echo "✅ monitoring.py existe"
    else
        echo "❌ monitoring.py manquant"
    fi
    
    # Test 3 : Le port 8000 écoute ?
    if docker exec $service netstat -tuln 2>/dev/null | grep -q ":8000"; then
        echo "✅ Port 8000 en écoute"
    else
        echo "❌ Port 8000 ne répond pas"
    fi
    
    # Test 4 : Peut-on accéder aux métriques ?
    if docker exec $service curl -s http://localhost:8000/metrics >/dev/null 2>&1; then
        echo "✅ Métriques accessibles"
    else
        echo "❌ Métriques inaccessibles"
    fi
    
    echo ""
done