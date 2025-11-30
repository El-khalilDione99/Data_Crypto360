#!/bin/bash

echo "🚀 Configuration du système de monitoring DataFlow360..."

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Créer la structure de répertoires
echo -e "${YELLOW}📁 Création de la structure de répertoires...${NC}"
mkdir -p monitoring/{grafana/{data,provisioning/{datasources,dashboards},dashboards},loki,promtail,prometheus/{data}}

# Donner les permissions
chmod -R 777 monitoring/

echo -e "${GREEN}✅ Structure créée${NC}"

# Créer les fichiers de configuration
echo -e "${YELLOW}📝 Création des fichiers de configuration...${NC}"

# Loki config
cat > monitoring/loki/config.yml << 'EOF'
auth_enabled: false
server:
  http_listen_port: 3100
  grpc_listen_port: 9096
common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    instance_addr: 127.0.0.1
    kvstore:
      store: inmemory
schema_config:
  configs:
    - from: 2020-10-24
      store: boltdb-shipper
      object_store: filesystem
      schema: v11
      index:
        prefix: index_
        period: 24h
limits_config:
  retention_period: 720h
  ingestion_rate_mb: 16
  ingestion_burst_size_mb: 32
EOF

# Promtail config
cat > monitoring/promtail/config.yml << 'EOF'
server:
  http_listen_port: 9080
  grpc_listen_port: 0
positions:
  filename: /tmp/positions.yaml
clients:
  - url: http://loki:3100/loki/api/v1/push
scrape_configs:
  - job_name: docker
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
    relabel_configs:
      - source_labels: ['__meta_docker_container_name']
        target_label: 'container'
        regex: '/(.*)'
      - source_labels: ['__meta_docker_container_label_com_docker_compose_service']
        target_label: 'service'
      - source_labels: ['__meta_docker_container_name']
        regex: '.*(ingestion-binance|coingecko-fetch|kafka|influxdb|namenode|datanode).*'
        action: keep
EOF

# Prometheus config
cat > monitoring/prometheus/config.yml << 'EOF'
global:
  scrape_interval: 15s
  evaluation_interval: 15s
scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']
  - job_name: 'python-services'
    static_configs:
      - targets:
          - 'ingestion-binance:8000'
          - 'coingecko-fetch:8000'
EOF

# Grafana datasources
cat > monitoring/grafana/provisioning/datasources/datasources.yml << 'EOF'
apiVersion: 1
datasources:
  - name: Loki
    type: loki
    access: proxy
    url: http://loki:3100
    isDefault: true
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
EOF

# Grafana dashboards provisioning
cat > monitoring/grafana/provisioning/dashboards/dashboards.yml << 'EOF'
apiVersion: 1
providers:
  - name: 'DataFlow360'
    orgId: 1
    folder: 'DataFlow360'
    type: file
    options:
      path: /var/lib/grafana/dashboards
EOF

echo -e "${GREEN}✅ Fichiers de configuration créés${NC}"

# Ajouter prometheus_client aux requirements si absent
if ! grep -q "prometheus_client" requirements.txt 2>/dev/null; then
    echo -e "${YELLOW}📦 Ajout de prometheus_client aux dépendances...${NC}"
    echo "prometheus_client==0.19.0" >> requirements.txt
fi

# Démarrer la stack de monitoring
echo -e "${YELLOW}🚀 Démarrage de la stack de monitoring...${NC}"
docker compose -f docker-compose-monitoring.yml up -d

# Attendre que les services soient prêts
echo -e "${YELLOW}⏳ Attente du démarrage des services...${NC}"
sleep 20

# Vérifier l'état
echo -e "\n${GREEN}📊 État des services de monitoring:${NC}"
docker compose -f docker-compose-monitoring.yml ps

echo -e "\n${GREEN}✅ Setup terminé!${NC}"
echo -e "\n${YELLOW}🎯 Accès aux interfaces:${NC}"
echo -e "  • Grafana:    http://localhost:3000 (admin/admin123)"
echo -e "  • Prometheus: http://localhost:9090"
echo -e "  • Loki:       http://localhost:3100"
echo -e "  • cAdvisor:   http://localhost:8080"
echo -e "\n${YELLOW}📝 Prochaines étapes:${NC}"
echo -e "  1. Connectez-vous à Grafana"
echo -e "  2. Importez le dashboard depuis monitoring/grafana/dashboards/"
echo -e "  3. Ajoutez les métriques à vos scripts Python"
echo -e "  4. Lancez vos services avec 'docker compose up -d'"