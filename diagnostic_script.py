import os
import pandas as pd
import sqlite3
import mysql.connector
import logging

# Configuration (copiée de votre script)
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'
SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'

MYSQL_CONFIG = {
    "host": MYSQL_HOST,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DB,
    "port": MYSQL_PORT
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def analyser_structure_sqlite():
    """Analyse la structure des tables SQLite"""
    logging.info("=== ANALYSE STRUCTURE SQLITE ===")
    conn = sqlite3.connect(SQLITE_DB_PATH)
    
    tables = ["region", "revendeur", "produit", "production"]
    
    for table in tables:
        logging.info(f"\n--- Table SQLite: {table} ---")
        
        # Structure de la table
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        logging.info("Colonnes:")
        for col in columns:
            logging.info(f"  - {col[1]} ({col[2]})")
        
        # Échantillon de données
        df = pd.read_sql(f"SELECT * FROM {table} LIMIT 3", conn)
        logging.info("Échantillon de données:")
        for index, row in df.iterrows():
            logging.info(f"  {dict(row)}")
    
    conn.close()

def analyser_structure_mysql():
    """Analyse la structure des tables MySQL"""
    logging.info("\n=== ANALYSE STRUCTURE MYSQL ===")
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        # Lister toutes les tables
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        logging.info(f"Tables MySQL disponibles: {tables}")
        
        for table in tables:
            logging.info(f"\n--- Table MySQL: {table} ---")
            
            # Structure de la table
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            logging.info("Colonnes:")
            for col in columns:
                logging.info(f"  - {col[0]} ({col[1]})")
            
            # Échantillon de données (si la table contient des données)
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            
            if count > 0:
                cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                logging.info("Échantillon de données:")
                for row in rows:
                    logging.info(f"  {dict(zip(column_names, row))}")
            else:
                logging.info("Table vide")
        
        conn.close()
        
    except Exception as e:
        logging.error(f"Erreur connexion MySQL: {e}")

def analyser_csv():
    """Analyse la structure du fichier CSV"""
    logging.info("\n=== ANALYSE FICHIER CSV ===")
    
    if not os.path.exists(CSV_PATH):
        logging.warning(f"Fichier CSV non trouvé: {CSV_PATH}")
        return
    
    df = pd.read_csv(CSV_PATH)
    logging.info(f"Nombre de lignes: {len(df)}")
    logging.info(f"Colonnes: {list(df.columns)}")
    
    logging.info("Échantillon de données:")
    for index, row in df.head(3).iterrows():
        logging.info(f"  {dict(row)}")
    
    logging.info("Types de données:")
    for col, dtype in df.dtypes.items():
        logging.info(f"  - {col}: {dtype}")

if __name__ == "__main__":
    logging.info("🔍 DIAGNOSTIC DES STRUCTURES DE DONNÉES")
    
    # Analyser SQLite
    if os.path.exists(SQLITE_DB_PATH):
        analyser_structure_sqlite()
    else:
        logging.warning(f"Base SQLite non trouvée: {SQLITE_DB_PATH}")
    
    # Analyser MySQL
    analyser_structure_mysql()
    
    # Analyser CSV
    analyser_csv()
    
    logging.info("\n✅ Diagnostic terminé")
