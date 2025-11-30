import pandas as pd
import glob
from hdfs import InsecureClient
from io import StringIO
import time
import os

# ===========================
# Configuration HDFS
# ===========================
HDFS_URL = "http://namenode:50070"
HDFS_USER = "hadoop"
HDFS_DESTINATION = "/user/hadoop/data/crypto/csv_files"

# ===========================
# Chemin local des CSV
# ===========================
LOCAL_CSV_PATH = "./Fichiers/*.csv"

# ===========================
# Fonctions
# ===========================

def wait_for_hdfs(max_retries=20, delay=3):
    """
    Attend que le NameNode soit prêt à répondre avant de poursuivre.
    """
    for i in range(max_retries):
        try:
            client = InsecureClient(HDFS_URL, user=HDFS_USER)
            client.status("/", strict=False)
            print(" HDFS prêt.")
            return client
        except Exception as e:
            print(f" Tentative {i+1}/{max_retries} : HDFS non disponible ({e})")
            time.sleep(delay)
    raise ConnectionError(" Le NameNode ne répond pas après plusieurs tentatives.")


def upload_csv_to_hdfs():
    """
    Parcourt tous les fichiers CSV locaux et les envoie dans HDFS
    """
    client = wait_for_hdfs()

    csv_files = glob.glob(LOCAL_CSV_PATH)
    if not csv_files:
        print(f" Aucun fichier CSV trouvé dans {LOCAL_CSV_PATH}")
        # Debug : afficher le contenu du dossier
        print(f" Contenu du répertoire actuel : {os.listdir('.')}")
        if os.path.exists("./Fichiers/"):
            print(f" Contenu de ./Fichiers/ : {os.listdir('./Fichiers/')}")
        else:
            print(" Le dossier ./Fichiers/ n'existe pas !")
        return

    print(f"{len(csv_files)} fichiers trouvés, création du dossier HDFS si nécessaire...")

    # Créer le dossier HDFS
    try:
        client.makedirs(HDFS_DESTINATION)
        print(f" Dossier HDFS créé : {HDFS_DESTINATION}")
    except Exception as e:
        print(f"ℹ Dossier HDFS déjà existant ou erreur : {e}")

    # Upload des fichiers
    success_count = 0
    for csv_file in csv_files:
        try:
            file_name = os.path.basename(csv_file)
            hdfs_path = f"{HDFS_DESTINATION}/{file_name}"

            print(f" Upload de {file_name}...", end=" ")
            df = pd.read_csv(csv_file)

            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            client.write(hdfs_path, data=csv_buffer.getvalue(),
                         overwrite=True, encoding="utf-8")

            print("")
            success_count += 1

        except Exception as e:
            print(f" Erreur : {e}")

    print(f"\n {success_count}/{len(csv_files)} fichiers transférés vers HDFS avec succès !")


def main():
    """
    Point d'entrée principal - appelé par main.py
    """
    upload_csv_to_hdfs()


# ===========================
# Lancement
# ===========================
if __name__ == "__main__":
    main()