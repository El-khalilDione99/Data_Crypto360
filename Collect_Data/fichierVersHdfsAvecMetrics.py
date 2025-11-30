#!/usr/bin/env python3
"""
Upload CSV vers HDFS avec monitoring Prometheus
"""

import pandas as pd
import glob
from hdfs import InsecureClient
from io import StringIO
import time
import os
import sys

# Import du monitoring
sys.path.append('/app')
from utils.monitoring import (
    start_metrics_server,
    track_execution_time,
    ServiceHealthTracker,
    hdfs_files_written_total,
    hdfs_file_size_bytes,
    hdfs_write_duration,
    update_process_metrics
)

# ===========================
# Configuration HDFS
# ===========================
HDFS_URL = os.getenv("HDFS_URL", "http://namenode:50070")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_DESTINATION = "/user/hadoop/data/crypto/csv_files"

# ===========================
# Chemin local des CSV
# ===========================
LOCAL_CSV_PATH = os.getenv("LOCAL_CSV_PATH", "./Fichiers/*.csv")

# Health tracker
health_tracker = ServiceHealthTracker('csv_hdfs_uploader')

# Statistiques
total_files_found = 0
total_files_uploaded = 0
total_bytes_uploaded = 0
total_errors = 0

# ===========================
# Fonctions
# ===========================

@track_execution_time('csv_hdfs', 'wait_for_hdfs')
def wait_for_hdfs(max_retries=20, delay=3):
    """
    Attend que le NameNode soit prêt à répondre avant de poursuivre.
    """
    print(f" Tentative de connexion à HDFS: {HDFS_URL}")
    
    for i in range(max_retries):
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.status("/", strict=False)
            print("HDFS prêt.")
            health_tracker.mark_healthy()
            return client
        except Exception as e:
            print(f" Tentative {i+1}/{max_retries} : HDFS non disponible ({e})")
            health_tracker.record_error('HDFSConnectionError')
            time.sleep(delay)
    
    health_tracker.mark_unhealthy()
    raise ConnectionError(" Le NameNode ne répond pas après plusieurs tentatives.")


@track_execution_time('csv_hdfs', 'upload_single_file')
def upload_file_to_hdfs(client, csv_file, hdfs_destination):
    """
    Upload un fichier CSV vers HDFS avec métriques
    """
    global total_files_uploaded, total_bytes_uploaded, total_errors
    
    try:
        file_name = os.path.basename(csv_file)
        hdfs_path = f"{hdfs_destination}/{file_name}"
        
        print(f" Upload de {file_name}...", end=" ")
        
        # Lire le CSV
        start_read = time.time()
        df = pd.read_csv(csv_file)
        read_duration = time.time() - start_read
        
        # Convertir en CSV string
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        data_size = len(csv_data.encode('utf-8'))
        
        # Écrire sur HDFS
        start_write = time.time()
        client.write(hdfs_path, data=csv_data, overwrite=True, encoding="utf-8")
        write_duration = time.time() - start_write
        
        # Métriques
        hdfs_files_written_total.labels(
            path=hdfs_destination,
            status='success'
        ).inc()
        
        hdfs_file_size_bytes.labels(
            path=hdfs_path
        ).set(data_size)
        
        hdfs_write_duration.labels(
            operation='write_csv'
        ).observe(write_duration)
        
        total_files_uploaded += 1
        total_bytes_uploaded += data_size
        
        print(f" ({data_size/1024:.1f} KB, {len(df)} lignes, "
              f"read: {read_duration:.2f}s, write: {write_duration:.2f}s)")
        
        health_tracker.mark_healthy()
        return True
        
    except Exception as e:
        print(f" Erreur : {e}")
        hdfs_files_written_total.labels(
            path=hdfs_destination,
            status='error'
        ).inc()
        health_tracker.record_error('FileUploadError')
        total_errors += 1
        return False


@track_execution_time('csv_hdfs', 'upload_all_csv')
def upload_csv_to_hdfs():
    """
    Parcourt tous les fichiers CSV locaux et les envoie dans HDFS
    """
    global total_files_found
    
    print("="*60)
    print(" UPLOAD CSV → HDFS AVEC MONITORING")
    print("="*60)
    print(f" Chemin local: {LOCAL_CSV_PATH}")
    print(f" HDFS: {HDFS_URL}")
    print(f" Destination: {HDFS_DESTINATION}")
    print(f" Métriques: http://0.0.0.0:8000/metrics")
    print("="*60 + "\n")
    
    # Connexion HDFS
    client = wait_for_hdfs()
    
    # Recherche des fichiers CSV
    csv_files = glob.glob(LOCAL_CSV_PATH)
    total_files_found = len(csv_files)
    
    if not csv_files:
        print(f" Aucun fichier CSV trouvé dans {LOCAL_CSV_PATH}")
        
        # Debug : afficher le contenu du dossier
        print(f"\n Debug - Contenu du répertoire actuel :")
        try:
            print(f"   Répertoire: {os.getcwd()}")
            print(f"   Fichiers: {os.listdir('.')}")
        except:
            pass
        
        if os.path.exists("./Fichiers/"):
            print(f"\n Contenu de ./Fichiers/ :")
            try:
                files = os.listdir('./Fichiers/')
                for f in files:
                    print(f"   - {f}")
            except:
                pass
        else:
            print(" Le dossier ./Fichiers/ n'existe pas !")
        
        health_tracker.record_error('NoFilesFound')
        return
    
    print(f" {len(csv_files)} fichiers CSV trouvés")
    
    # Créer le dossier HDFS
    try:
        print(f" Création du dossier HDFS: {HDFS_DESTINATION}")
        client.makedirs(HDFS_DESTINATION)
        print(f" Dossier HDFS créé")
    except Exception as e:
        print(f"  Dossier HDFS déjà existant ou erreur : {e}")
    
    print(f"\n Démarrage de l'upload...\n")
    
    # Upload des fichiers
    start_time = time.time()
    
    for i, csv_file in enumerate(csv_files, 1):
        print(f"[{i}/{len(csv_files)}] ", end="")
        upload_file_to_hdfs(client, csv_file, HDFS_DESTINATION)
        
        # Mettre à jour les métriques système tous les 5 fichiers
        if i % 5 == 0:
            update_process_metrics('csv_hdfs')
    
    total_duration = time.time() - start_time
    
    # Résumé final
    print(f"\n{'='*60}")
    print(" RÉSUMÉ DE L'UPLOAD")
    print(f"{'='*60}")
    print(f"   Fichiers trouvés: {total_files_found}")
    print(f"   Fichiers uploadés: {total_files_uploaded}")
    print(f"   Erreurs: {total_errors}")
    print(f"   Taux de succès: {(total_files_uploaded/total_files_found*100):.1f}%")
    print(f"   Données transférées: {total_bytes_uploaded/1024/1024:.2f} MB")
    print(f"   Durée totale: {total_duration:.2f}s")
    if total_files_uploaded > 0:
        print(f"   Vitesse moyenne: {total_bytes_uploaded/1024/total_duration:.1f} KB/s")
    print(f"{'='*60}")
    
    if total_files_uploaded == total_files_found:
        print(" Tous les fichiers ont été transférés avec succès !")
    else:
        print(f"  {total_errors} fichier(s) ont échoué")


def main():
    """
    Point d'entrée principal - appelé par main.py
    """
    # Démarrer le serveur de métriques
    start_metrics_server(port=8000, service_name='csv_hdfs_uploader')
    
    try:
        upload_csv_to_hdfs()
        
        # Garder le serveur de métriques actif pendant 60s
        # pour permettre à Prometheus de scraper
        print("\n Métriques disponibles pendant 60 secondes...")
        print("   URL: http://0.0.0.0:8000/metrics")
        time.sleep(60)
        
    except KeyboardInterrupt:
        print("\n Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"\n Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        health_tracker.record_error('FatalError')
    finally:
        print("\n Programme terminé")


# ===========================
# Lancement
# ===========================
if __name__ == "__main__":
    main()