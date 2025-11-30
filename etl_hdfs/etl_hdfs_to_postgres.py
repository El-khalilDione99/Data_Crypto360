import pandas as pd
from hdfs import InsecureClient
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import sys

# ======================
# 🔹 Connexion à HDFS
# ======================
hdfs_client = InsecureClient('http://namenode:50070', user='hdfs')

# Path HDFS
HDFS_PATH = "/user/hadoop/data/crypto/csv_files/"

# ======================
# 🔹 Connexion PostgreSQL
# ======================
conn = psycopg2.connect(
    host="postgres_dwh",
    dbname="crypto_warehouse",
    user="user123",
    password="admin1234"
)
cur = conn.cursor()

# ======================
# 🔹 Caches (accélération)
# ======================
date_cache = {}
crypto_cache = {}


# -------------------------------------------------
# 🔹 Parser intelligent de date
# -------------------------------------------------
def parse_date(date_str):
    try:
        # Format CSV standard : 2020-05-03
        return datetime.strptime(date_str, "%Y-%m-%d")
    except:
        try:
            # Format avec heure
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except:
            print(f"[WARN] Format date inconnu: {date_str}")
            return None


# -------------------------------------------------
# 🔹 Gestion dim_date AUTOMATIQUE
# -------------------------------------------------
def get_or_create_dim_date(date_str):
    global date_cache

    date_obj = parse_date(date_str)
    if date_obj is None:
        return None

    date_key = date_obj.date()

    # Cache pour éviter SELECT plusieurs fois
    if date_key in date_cache:
        return date_cache[date_key]

    cur.execute("SELECT date_id FROM dim_date WHERE date = %s", (date_key,))
    row = cur.fetchone()

    if row:
        date_cache[date_key] = row[0]
        return row[0]

    cur.execute("""
        INSERT INTO dim_date (date, year, month, day, weekday, week_of_year, quarter)
        VALUES (%s, %s, %s, %s, %s, EXTRACT(WEEK FROM %s), EXTRACT(QUARTER FROM %s))
        RETURNING date_id
    """, (date_key, date_obj.year, date_obj.month, date_obj.day, date_obj.weekday(),
          date_key, date_key))

    new_id = cur.fetchone()[0]
    date_cache[date_key] = new_id
    conn.commit()
    return new_id


# -------------------------------------------------
# 🔹 Gestion dim_crypto AUTOMATIQUE
# -------------------------------------------------
def get_or_create_dim_crypto(name, symbol):
    global crypto_cache

    if symbol in crypto_cache:
        return crypto_cache[symbol]

    cur.execute("SELECT crypto_id FROM dim_crypto WHERE symbol = %s", (symbol,))
    row = cur.fetchone()

    if row:
        crypto_cache[symbol] = row[0]
        return row[0]

    cur.execute("""
        INSERT INTO dim_crypto (name, symbol, first_seen_date)
        VALUES (%s, %s, CURRENT_DATE)
        RETURNING crypto_id
    """, (name, symbol))

    new_id = cur.fetchone()[0]
    crypto_cache[symbol] = new_id
    conn.commit()
    return new_id


# -------------------------------------------------
# 🔹 Lecture de TOUS les fichiers HDFS
# -------------------------------------------------
files = hdfs_client.list(HDFS_PATH)

for file in files:
    print(f"\nProcessing: {file}")

    try:
        # Lecture CSV
        with hdfs_client.read(HDFS_PATH + file, encoding="utf-8") as reader:
            df = pd.read_csv(reader)

        if df.empty:
            print(" Fichier vide → ignoré")
            continue

        rows_to_insert = []

        for _, row in df.iterrows():
            crypto_id = get_or_create_dim_crypto(row["Name"], row["Symbol"])
            date_id = get_or_create_dim_date(row["Date"])

            if date_id is None:
                continue

            high = float(row["High"])
            low = float(row["Low"])
            open_p = float(row["Open"])
            close_p = float(row["Close"])

            volatility = (high - low)
            price_range = (close_p - open_p)
            change_percent = ((close_p - open_p) / open_p * 100) if open_p != 0 else 0

            rows_to_insert.append((
                crypto_id, date_id,
                high, low, open_p, close_p,
                float(row["Volume"]), float(row["Marketcap"]),
                change_percent, volatility, price_range
            ))

        # Insertion par batch
        if rows_to_insert:
            execute_values(cur, """
                INSERT INTO fact_market_data (
                    crypto_id, date_id,
                    high, low, open, close,
                    volume, marketcap,
                    change_percent, volatility, price_range
                ) VALUES %s
            """, rows_to_insert)

            conn.commit()
            print(f"   ✔ {len(rows_to_insert)} lignes insérées")

    except Exception as e:
        print(f"   Erreur: {e}")
        continue

print("\n🎉 ETL TERMINÉ – Données chargées dans le DataWarehouse !")

cur.close()
conn.close()
