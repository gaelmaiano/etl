import os
import pandas as pd
import sqlite3
import mysql.connector
import logging
import subprocess
from datetime import datetime

# === CONFIGURATION ===
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
EXPORT_DIR = "./exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

MYSQL_CONFIG = {
    "host": MYSQL_HOST,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DB,
    "port": MYSQL_PORT
}

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === FONCTIONS ===

def extraire_donnees_sqlite():
    logging.info("Connexion SQLite : %s...", SQLITE_DB_PATH)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    data = {
        "region": pd.read_sql("SELECT * FROM region", conn),
        "revendeur": pd.read_sql("SELECT * FROM revendeur", conn),
        "produit": pd.read_sql("SELECT * FROM produit", conn),
        "production": pd.read_sql("SELECT * FROM production", conn)
    }
    conn.close()
    for table, df in data.items():
        logging.info("Table '%s': %d lignes", table, len(df))
    return data

def extraire_commandes_csv():
    logging.info("Extraction des commandes depuis : %s", CSV_PATH)
    if not os.path.exists(CSV_PATH):
        logging.warning("Fichier CSV non trouvé : %s", CSV_PATH)
        return pd.DataFrame()
    df = pd.read_csv(CSV_PATH)
    logging.info("%d lignes extraites du fichier CSV", len(df))
    return df

def connexion_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

def charger_table_mysql(df, table_name, conn):
    if df.empty:
        logging.info("Aucune donnée à insérer dans '%s'", table_name)
        return
    cursor = conn.cursor()
    colonnes = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT IGNORE INTO {table_name} ({colonnes}) VALUES ({placeholders})"
    data = [tuple(row) for row in df.to_numpy()]
    cursor.executemany(insert_sql, data)
    conn.commit()
    logging.info("Chargement dans MySQL (anti-doublons) : '%s' → %d lignes", table_name, cursor.rowcount)

def transformer_et_charger(donnees_sqlite, commandes_csv):
    conn = connexion_mysql()

    # Chargement des tables fixes
    charger_table_mysql(donnees_sqlite["region"], "Regions", conn)
    charger_table_mysql(donnees_sqlite["revendeur"], "Revendeurs", conn)
    charger_table_mysql(donnees_sqlite["produit"], "Produits", conn)

    # Enregistrement des réceptions dans StockEvenement
    production = donnees_sqlite["production"].copy()
    production["type"] = "reception"
    production.rename(columns={"date_production": "date", "quantite": "quantité"}, inplace=True)
    charger_table_mysql(production[["date", "produit_id", "quantité", "type"]], "StockEvenement", conn)

    # Traitement des commandes
    if not commandes_csv.empty:
        commandes = commandes_csv[["commande_id", "revendeur_id", "date_commande"]].drop_duplicates()
        commandes.rename(columns={"date_commande": "date"}, inplace=True)
        charger_table_mysql(commandes, "Commandes", conn)

        lignes = commandes_csv[["commande_id", "produit_id", "quantite", "prix_unitaire"]].copy()
        lignes.loc[:, 'ligne_id'] = range(1, len(lignes) + 1)
        lignes.rename(columns={"quantite": "quantité"}, inplace=True)
        charger_table_mysql(lignes[["ligne_id", "commande_id", "produit_id", "quantité", "prix_unitaire"]], "LignesCommande", conn)

        # Enregistrement des commandes dans StockEvenement
        events = commandes_csv[["date_commande", "produit_id", "quantite"]].copy()
        events["type"] = "commande"
        events.rename(columns={"date_commande": "date", "quantite": "quantité"}, inplace=True)
        charger_table_mysql(events[["date", "produit_id", "quantité", "type"]], "StockEvenement", conn)

    conn.close()

def export_sql_complet():
    logging.info("Export SQL de la base complète...")
    date_str = datetime.now().strftime("%Y%m%d")
    output_file = f"{EXPORT_DIR}/distributech_export_{date_str}.sql"
    try:
        cmd = [
            "mysqldump",
            f"-u{MYSQL_USER}",
            f"-p{MYSQL_PASSWORD}",
            f"-h{MYSQL_HOST}",
            f"-P{MYSQL_PORT}",
            MYSQL_DB
        ]
        with open(output_file, "w") as f:
            subprocess.run(cmd, stdout=f, check=True)
        logging.info("✅ Export SQL terminé : %s", output_file)
    except Exception as e:
        logging.error("❌ Erreur export SQL : %s", e)

def export_csv_stock():
    logging.info("Export CSV de l'état des stocks...")
    try:
        conn = connexion_mysql()
        query = """
        SELECT
            p.produit_id,
            p.nom AS nom_produit,
            SUM(CASE WHEN s.type = 'reception' THEN s.quantité ELSE 0 END) AS total_recu,
            SUM(CASE WHEN s.type = 'commande' THEN s.quantité ELSE 0 END) AS total_sorti,
            SUM(CASE WHEN s.type = 'reception' THEN s.quantité ELSE 0 END) -
            SUM(CASE WHEN s.type = 'commande' THEN s.quantité ELSE 0 END) AS stock_actuel
        FROM
            StockEvenement s
        JOIN
            Produits p ON s.produit_id = p.produit_id
        GROUP BY
            p.produit_id, p.nom
        ORDER BY
            p.produit_id
        """
        df = pd.read_sql(query, conn)
        conn.close()

        date_str = datetime.now().strftime("%Y%m%d")
        output_file = f"{EXPORT_DIR}/etat_stock_{date_str}.csv"
        df.to_csv(output_file, index=False)
        logging.info("✅ Export CSV des stocks terminé : %s", output_file)
    except Exception as e:
        logging.error("❌ Erreur export CSV stock : %s", e)

# === MAIN ===

if __name__ == "__main__":
    logging.info("Démarrage du script ETL")

    # Étape 1 : Extraction
    donnees_sqlite = extraire_donnees_sqlite()
    commandes_csv = extraire_commandes_csv()

    # Étape 2 : Transformation + Chargement
    transformer_et_charger(donnees_sqlite, commandes_csv)

    # Étape 3 : Export SQL + CSV
    export_sql_complet()
    export_csv_stock()

    logging.info("Script ETL terminé avec succès")
