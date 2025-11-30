FROM python:3.10-slim

WORKDIR /app

# Installer les dépendances
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    influxdb-client==1.38.0

# Copier le script
COPY kafka_crypto_to_influx.py /app/kafka_to_influx.py

CMD ["python", "-u", "kafka_to_influx.py"]

