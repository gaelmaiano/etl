import os
import pandas as pd
import sqlite3
import mysql.connector
import logging
import subprocess
from datetime import datetime

# === CONFIGURATION ===
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
EXPORT_DIR = "./exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

MYSQL_CONFIG = {
    "host": "localhost",
    "port": "3307",
    "database": "distributech_db",
    "user": "etl_user",
    "password": "motdepasse123"
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
    
    # Convertir les données en gérant les valeurs NaN
    data = []
    for _, row in df.iterrows():
        row_data = []
        for val in row:
            if pd.isna(val):
                row_data.append(None)
            else:
                row_data.append(val)
        data.append(tuple(row_data))
    
    try:
        cursor.executemany(insert_sql, data)
        conn.commit()
        logging.info("✅ Chargement dans MySQL : '%s' → %d lignes", table_name, cursor.rowcount)
    except Exception as e:
        logging.error(f"❌ Erreur lors du chargement de {table_name}: {e}")
        conn.rollback()

def transformer_et_charger(donnees_sqlite, commandes_csv):
    conn = connexion_mysql()

    try:
        # === CHARGEMENT DES TABLES DE RÉFÉRENCE ===
        
        # Régions : mapper region_name → nom_region
        regions = donnees_sqlite["region"].copy()
        regions = regions.rename(columns={
            'region_name': 'nom_region'
        })
        charger_table_mysql(regions, "Regions", conn)
        
        # Revendeurs : mapper revendeur_name → nom_revendeur  
        revendeurs = donnees_sqlite["revendeur"].copy()
        revendeurs = revendeurs.rename(columns={
            'revendeur_name': 'nom_revendeur'
        })
        charger_table_mysql(revendeurs, "Revendeurs", conn)
        
        # Produits : mapper product_id → produit_id, product_name → nom_produit, cout_unitaire → prix_unitaire
        produits = donnees_sqlite["produit"].copy()
        produits = produits.rename(columns={
            'product_id': 'produit_id',
            'product_name': 'nom_produit', 
            'cout_unitaire': 'prix_unitaire'
        })
        charger_table_mysql(produits, "Produits", conn)
        
        # Productions : mapper quantity → quantite_produite, date_production → date
        productions = donnees_sqlite["production"].copy()
        productions = productions.rename(columns={
            'quantity': 'quantite_produite',
            'date_production': 'date'
        })
        charger_table_mysql(productions, "Productions", conn)

        # === TRAITEMENT DES COMMANDES CSV ===
        
        if not commandes_csv.empty:
            # Générer des IDs de commandes uniques basés sur numero_commande
            commandes_uniques = commandes_csv[['numero_commande', 'commande_date', 'revendeur_id']].drop_duplicates()
            commandes_uniques = commandes_uniques.reset_index(drop=True)
            commandes_uniques['commande_id'] = range(1, len(commandes_uniques) + 1)
            
            # Ajouter les IDs aux données originales
            df_avec_ids = commandes_csv.merge(
                commandes_uniques, 
                on=['numero_commande', 'commande_date', 'revendeur_id']
            )
            
            # Commandes principales : mapper commande_date → date_commande
            commandes_finales = commandes_uniques.copy()
            commandes_finales = commandes_finales.rename(columns={
                'commande_date': 'date_commande'
            })
            # Réorganiser les colonnes pour MySQL
            commandes_finales = commandes_finales[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']]
            charger_table_mysql(commandes_finales, "Commandes", conn)
            
            # Lignes de commandes : mapper product_id → produit_id, quantity → quantite, unit_price → prix_unitaire_vente
            lignes = df_avec_ids[['commande_id', 'product_id', 'quantity', 'unit_price']].copy()
            lignes['ligne_id'] = range(1, len(lignes) + 1)
            lignes = lignes.rename(columns={
                'product_id'