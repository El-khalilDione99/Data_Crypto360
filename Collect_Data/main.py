import os
import sys
import time

mode = os.getenv("SERVICE_MODE")

print(f" Démarrage du service : {mode}")

try:
    if mode == "crypto_batch":
        print(" Traitement batch CSV → HDFS...")
        import fichierVersHdfsAvecMetrics  #  SANS Collect_Data.
        fichierVersHdfsAvecMetrics.main()
        sys.exit(0)
        
    elif mode == "binance_kafka":
        print(" Binance WebSocket → Kafka...")
        from APIS_binance import binance_kafka_monitored  # SANS Collect_Data.
        binance_kafka_monitored.main()
        
    elif mode == "coingo_kafka":
        print(" CoinGecko API → Kafka...")
        from APIS_coingo import coingo_kafka_monitored  # SANS Collect_Data.
        coingo_kafka_monitored.main()
        
    else:
        print(f" SERVICE_MODE '{mode}' invalide.")
        sys.exit(1)

except Exception as e:
    print(f" Erreur : {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
