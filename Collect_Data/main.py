import os
import sys
import time

mode = os.getenv("SERVICE_MODE")

print(f"🚀 Démarrage du service : {mode}")

try:
    if mode == "crypto_batch":
        print("📦 Traitement batch CSV → HDFS...")
        import fichierVersHdfsAvecMetrics  # ✅ SANS Collect_Data.
        fichierVersHdfsAvecMetrics.main()
        sys.exit(0)
        
    elif mode == "binance_kafka":
        print("📊 Binance WebSocket → Kafka...")
        from APIS_binance import binance_kafka_monitored  # ✅ SANS Collect_Data.
        binance_kafka_monitored.main()
        
    elif mode == "coingo_kafka":
        print("🪙 CoinGecko API → Kafka...")
        from APIS_coingo import coingo_kafka_monitored  # ✅ SANS Collect_Data.
        coingo_kafka_monitored.main()
        
    else:
        print(f"❌ SERVICE_MODE '{mode}' invalide.")
        sys.exit(1)

except Exception as e:
    print(f"❌ Erreur : {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ---------------------------------

# import os
# import sys
# import time

# mode = os.getenv("SERVICE_MODE")

# print(f" Démarrage du service : {mode}")

# try:
#     if mode == "binance_realtime":
#         print(" Démarrage Binance WebSocket (temps réel)...")
#         from APIS_binance import binance_ws_realtime
#         binance_ws_realtime.main()
        
#     elif mode == "crypto_batch":
#         print(" Démarrage du traitement batch (upload CSV vers HDFS)...")
#         import fichier_vesrHdfs
#         fichier_vesrHdfs.main()
#         print(" Traitement batch terminé avec succès.")
#         sys.exit(0)  #  Terminer après l'exécution batch
        
#     elif mode == "coingo_api":
#         print(" Démarrage CoinGecko API...")
#         from APIS_coingo import api_coingo  #  Corrigé APIS_coin → APIS_coingo
        
#         # Exécution périodique toutes les 5 minutes
#         while True:
#             try:
#                 success = api_coingo.main()
#                 if success:
#                     print(" Prochaine exécution dans 5 minutes...")
#                     time.sleep(300)  # 5 minutes
#                 else:
#                     print(" Erreur détectée. Nouvelle tentative dans 1 minute...")
#                     time.sleep(60)
#             except KeyboardInterrupt:
#                 print("\n Arrêt demandé")
#                 sys.exit(0)
#             except Exception as e:
#                 print(f" Erreur : {e}. Nouvelle tentative dans 1 minute...")
#                 time.sleep(60)
        
#     else:
#         print(f" SERVICE_MODE '{mode}' invalide ou non défini.")
#         print("Valeurs acceptées : binance_realtime, crypto_batch, coingo_api")
#         sys.exit(1)

#     # Boucle d'attente UNIQUEMENT pour binance_realtime (déjà géré dans sa fonction main())
#     # Note : binance_ws_realtime.main() contient déjà une boucle infinie
#     print(f" {mode} en cours d'exécution...")
#     while True:
#         time.sleep(60)

# except KeyboardInterrupt:
#     print("\n Arrêt demandé par l'utilisateur")
#     sys.exit(0)
# except Exception as e:
#     print(f" Erreur fatale : {e}")
#     import traceback
#     traceback.print_exc()
#     sys.exit(1)